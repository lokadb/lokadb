package admin

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// The organization's other addresses (review A1). The proxy reaches an organization's database
// by more than its FDW server: the direct path's role (profile fdw+direct or direct) and the
// owner the DDL replay connects as, from the control plane (stage 9a) or else from customers_app
// and customers of its config. repoint that changed the server alone left them on the old
// database: the organization's writes went to two databases. Now each of them must reach the
// new database: the control plane's are rewritten under the pause, the config's are checked
// (repoint cannot edit every proxy's file) and refuse the repoint while they reach another one.

// otherAddr is one more way the proxy reaches the organization's database.
type otherAddr struct {
	kind   string // creds.Direct or creds.Owner
	source string // "control plane" or "proxy config (customers_app)" / "(customers)"
	old    string // the DSN stored now ("" none: a new one)
	dsn    string // the DSN that must reach the new database
	store  bool   // written to the control plane under the pause
}

// sameDB tells whether a DSN reaches the database repoint holds its mark in.
type sameDB func(ctx context.Context, dsn string) error

// databaseMark holds a random advisory lock on conn (the new database, as the cloud's role
// there) and returns how to tell another connection reaches the same database: it sees the
// lock in pg_locks of its own database. No row and no object of the customer's is written; the
// lock goes with conn. A copy of the database, a replica, another database of the same server
// does not see it.
func databaseMark(ctx context.Context, conn *pgx.Conn) (sameDB, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	k := int64(binary.BigEndian.Uint64(b[:]) &^ (1 << 63))
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", k); err != nil {
		return nil, fmt.Errorf("new customer database: %w", err)
	}
	probe := fmt.Sprintf(`SELECT EXISTS (SELECT FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		AND classid = %d AND objid = %d AND objsubid = 1)`, uint64(k)>>32, uint64(k)&0xffffffff)
	return func(ctx context.Context, dsn string) error {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		c, err := tunnel.PGXConnect(ctx, dsn)
		if err != nil {
			return fmt.Errorf("does not connect: %v", err)
		}
		defer c.Close(context.Background())
		var there bool
		if err := c.QueryRow(ctx, probe).Scan(&there); err != nil {
			return err
		}
		if !there {
			return fmt.Errorf("reaches another database (not the one -customer-app names)")
		}
		return nil
	}, nil
}

// otherAddresses finds the organization's direct and owner credentials, the control plane's
// first (they win in the proxy, credentials.go), the config's where the control plane has none,
// with the DSN each must have after repoint: the control plane's moved to the new address (or
// -direct / -owner given), the config's as they are.
func otherAddresses(ctx context.Context, o RepointOptions, inst, db string, app *pgconn.Config) (addrs []otherAddr, cp *pgx.Conn, key []byte, err error) {
	org := strconv.FormatInt(o.Org, 10)
	override := map[string]string{creds.Direct: o.DirectDSN, creds.Owner: o.OwnerDSN}
	stored := map[string]string{}
	if o.ControlPlane != "" {
		if key, err = creds.LoadKey(o.KeyFile); err != nil {
			return nil, nil, nil, err
		}
		keys := [][]byte{key}
		if o.KeyFileOld != "" {
			old, err := creds.LoadKey(o.KeyFileOld)
			if err != nil {
				return nil, nil, nil, err
			}
			keys = append(keys, old)
		}
		if cp, err = pgx.Connect(ctx, o.ControlPlane); err != nil {
			return nil, nil, nil, fmt.Errorf("control plane: %w", err)
		}
		set, errs, err := creds.Load(ctx, cp, inst, keys...)
		if err != nil {
			cp.Close(context.Background())
			return nil, nil, nil, fmt.Errorf("control plane: %w", err)
		}
		for _, e := range errs { // this organization's row unreadable: its address cannot be moved
			if strings.Contains(e.Error(), fmt.Sprintf("%s organization %s ", db, org)) {
				cp.Close(context.Background())
				return nil, nil, nil, fmt.Errorf("control plane: %w (repoint needs the key it is sealed with: -proxy-config's credentials_key or credentials_key_old)", e)
			}
		}
		for _, kind := range []string{creds.Direct, creds.Owner} {
			stored[kind] = set.DSN(db, org, kind)
		}
	} else if o.DirectDSN != "" || o.OwnerDSN != "" {
		return nil, nil, nil, fmt.Errorf("-direct and -owner are stored in the control plane, and the proxy's config has no control_plane")
	}
	config := map[string]string{creds.Direct: o.ConfigDirect, creds.Owner: o.ConfigOwner}
	configName := map[string]string{creds.Direct: "customers_app", creds.Owner: "customers"}
	for _, kind := range []string{creds.Direct, creds.Owner} {
		switch {
		case stored[kind] != "" || override[kind] != "":
			a := otherAddr{kind: kind, source: "control plane", old: stored[kind], dsn: override[kind], store: true}
			if a.dsn == "" {
				if a.dsn, err = withAddress(a.old, app.Host, app.Port, app.Database); err != nil {
					return nil, cp, nil, fmt.Errorf("control plane, %s credentials of organization %d: %w", kind, o.Org, err)
				}
			}
			addrs = append(addrs, a)
		case config[kind] != "":
			addrs = append(addrs, otherAddr{kind: kind, source: "proxy config (" + configName[kind] + ")", old: config[kind], dsn: config[kind]})
		}
	}
	return addrs, cp, key, nil
}

// checkOthers: every other address reaches the new database, or repoint refuses before it
// changes anything, naming each that does not.
func checkOthers(ctx context.Context, addrs []otherAddr, same sameDB, org int64) error {
	var bad []string
	for _, a := range addrs {
		if err := same(ctx, a.dsn); err != nil {
			bad = append(bad, fmt.Sprintf("%s credentials, %s, %s: %v", a.kind, a.source, describeDSN(a.dsn), err))
		}
	}
	if len(bad) == 0 {
		return nil
	}
	sort.Strings(bad)
	return fmt.Errorf("repoint changed nothing: the proxy reaches organization %d's database by these addresses too, and they do not reach the new database:\n  %s\n"+
		"  The control plane's are moved to the new host, port and database with their own role; give -direct DSN / -owner DSN where the proxy reaches it otherwise. "+
		"Point the proxy config's entries at the new database in every proxy's config (an organization on fdw+direct: org-profile fdw first, so that no statement goes to the new database before the cloud does), "+
		"or store them in the control plane with org-credentials (repoint rewrites those under its pause); then run repoint again",
		org, strings.Join(bad, "\n  "))
}

// storeOthers writes the control plane's credentials of the organization; undo puts back what
// was there.
func storeOthers(ctx context.Context, cp *pgx.Conn, key []byte, inst, db string, org int64, addrs []otherAddr) (undo func(), err error) {
	o := strconv.FormatInt(org, 10)
	var done []otherAddr
	undo = func() {
		for _, a := range done {
			var err error
			if a.old != "" {
				err = creds.Put(context.Background(), cp, key, inst, db, o, a.kind, a.old)
			} else {
				_, err = cp.Exec(context.Background(), `DELETE FROM lokadb_cp.org_credentials WHERE installation = $1 AND db = $2 AND org = $3 AND kind = $4`, inst, db, org, a.kind)
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "repoint: the %s credentials of organization %d in the control plane could not be put back (%v): they name the new database while the cloud's server names the old one; run repoint again\n", a.kind, org, err)
			}
		}
	}
	for _, a := range addrs {
		if !a.store {
			continue
		}
		if err := creds.Put(ctx, cp, key, inst, db, o, a.kind, a.dsn); err != nil {
			undo()
			return func() {}, fmt.Errorf("control plane, %s credentials: %w", a.kind, err)
		}
		done = append(done, a)
	}
	return undo, nil
}

// withAddress: dsn with its host, port and database replaced (its role, password and other
// settings kept), as a URL or as keyword=value (a later keyword wins in libpq and pgx).
func withAddress(dsn, host string, port uint16, dbname string) (string, error) {
	// hostaddr and service win over host (libpq, pgx): the DSN would stay on the old address
	// (review of block 10)
	if m := addrKeyRe.FindString(dsn); m != "" {
		return "", fmt.Errorf("it names %s, which a new host does not override: give the new credentials with -direct / -owner", strings.TrimRight(strings.TrimLeft(m, "?& "), "= "))
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", err
		}
		u.Host = net.JoinHostPort(host, strconv.Itoa(int(port)))
		u.Path = "/" + dbname
		u.RawPath = ""
		return u.String(), nil
	}
	if _, err := pgconn.ParseConfig(dsn); err != nil {
		return "", err
	}
	kv := func(k, v string) string {
		return " " + k + "='" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
	}
	return strings.TrimSpace(dsn) + kv("host", host) + kv("port", strconv.Itoa(int(port))) + kv("dbname", dbname), nil
}

// addrKeyRe: a hostaddr or service setting, keyword=value or in a URL's query.
var addrKeyRe = regexp.MustCompile(`(?:^|[\s?&])(hostaddr|service)\s*=`)

// describeDSN: where a DSN goes, without its password.
func describeDSN(dsn string) string {
	c, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "(a DSN that does not parse)"
	}
	return fmt.Sprintf("%s@%s:%d/%s", c.User, c.Host, c.Port, c.Database)
}
