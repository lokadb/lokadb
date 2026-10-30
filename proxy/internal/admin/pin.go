package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// Pin proves to the relays the address the SaaS database connects from (tunnel/pin.go, R01): a
// copy of the database restored elsewhere carries the organizations' FDW servers and the
// customers' passwords, and must not reach their databases. For each relay its organizations'
// servers lead to, it makes in a transaction it rolls back a postgres_fdw server with a one-time
// code from the registry, and connects once: the relay records the address the connection came
// from (behind a NAT, in a container or a pod: the database's own way out). Nothing of it stays
// in the database. With no organization moved there is nothing to pin.
type PinResult struct {
	Relays   []string          // host:port probed
	Failed   map[string]string // relay -> why its probe did not go through
	SystemID string            // the database's system identifier ("" when this role may not read it)
	Sources  []tunnel.Source   // the addresses the relays know now
}

func Pin(ctx context.Context, cloud *pgx.Conn, cp *tunnel.PGStore) (PinResult, error) {
	res := PinResult{Failed: map[string]string{}}
	_ = cloud.QueryRow(ctx, `SELECT system_identifier::text FROM pg_control_system()`).Scan(&res.SystemID)
	relays, err := pinRelays(ctx, cloud)
	if err != nil {
		return res, err
	}
	for _, addr := range relays {
		res.Relays = append(res.Relays, addr)
		if err := probe(ctx, cloud, cp, addr); err != nil {
			res.Failed[addr] = err.Error()
		}
	}
	res.Sources, err = cp.Sources(ctx)
	return res, err
}

// pinRelays: the host:port of the relays the organizations' FDW servers lead to.
func pinRelays(ctx context.Context, cloud *pgx.Conn) ([]string, error) {
	rows, err := cloud.Query(ctx, `SELECT coalesce(srvoptions, '{}') FROM pg_foreign_server s JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
		WHERE w.fdwname = 'postgres_fdw' AND s.srvname ~ '^lokadb_org[0-9]+$'`)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	var opts []string
	_, err = pgx.ForEachRow(rows, []any{&opts}, func() error {
		host, port := "", "5432"
		for _, o := range opts {
			k, v, _ := strings.Cut(o, "=")
			switch k {
			case "host":
				host = v
			case "port":
				port = v
			}
		}
		if host != "" && !strings.HasPrefix(host, "/") {
			if a := net.JoinHostPort(host, port); !seen[a] {
				seen[a] = true
				out = append(out, a)
			}
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

// errNotRelay: what answered the probe is not a relay of this version (a customer's database
// reached directly, an older relay): nothing to pin there.
var errNotRelay = errors.New("not a LokaDB relay that keeps the SaaS database's addresses (an older relay, or the customer's database reached directly)")

func probe(ctx context.Context, cloud *pgx.Conn, cp *tunnel.PGStore, addr string) error {
	host, port, _ := net.SplitHostPort(addr)
	code, err := cp.NewProbe(ctx, 30*time.Second)
	if err != nil {
		return fmt.Errorf("the registry: %w", err)
	}
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background()) // nothing of the probe stays: not in the catalog, not in a dump
	for _, q := range []string{
		"CREATE SCHEMA lokadb_pin_probe",
		fmt.Sprintf("CREATE SERVER lokadb_pin_probe FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host %s, port %s, dbname %s, sslmode 'disable', connect_timeout '10')",
			lit(host), lit(port), lit(tunnel.ProbeDatabase)),
		fmt.Sprintf("CREATE USER MAPPING FOR CURRENT_USER SERVER lokadb_pin_probe OPTIONS (user %s, password 'none')", lit(code)),
		"CREATE FOREIGN TABLE lokadb_pin_probe.t (x int) SERVER lokadb_pin_probe",
	} {
		if _, err := tx.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", strings.SplitN(q, " (", 2)[0], err)
		}
	}
	_, err = tx.Exec(ctx, "SELECT * FROM lokadb_pin_probe.t")
	said := fmt.Sprint(err) // postgres_fdw puts what the relay answered in the error's detail
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		said += " " + pe.Detail
	}
	switch {
	case err != nil && strings.Contains(said, tunnel.ProbeOK):
		return nil
	case err == nil, strings.Contains(said, tunnel.ProbeDatabase): // a database answered it as a database name
		return errNotRelay
	}
	return errors.New(strings.TrimSpace(said))
}

// pinAfterMove: the organization's FDW server leads to a relay now: the relays learn the SaaS
// database's address at once, so a copy of it made from now on does not reach the customer. A
// failure is only logged (the runner, or lokadb pin, proves it again).
func pinAfterMove(ctx context.Context, cloud *pgx.Conn, o MoveOptions, log *slog.Logger) {
	cp, err := tunnel.OpenPGStore(ctx, o.ControlPlaneDSN)
	if err != nil {
		log.Warn("the relays did not learn the SaaS database's address (run lokadb pin)", "err", err)
		return
	}
	defer cp.Close()
	res, err := Pin(ctx, cloud, cp)
	if err != nil || len(res.Failed) > 0 {
		log.Warn("the relays did not learn the SaaS database's address (run lokadb pin)", "err", err, "failed", res.Failed)
		return
	}
	log.Info("the relays take this organization's connections from this database only", "relays", res.Relays)
}

// Copy: another database of the SaaS database's cluster with organization servers that lead to
// the same relays: a copy restored next to it (a staging database on the production server). The
// relays cannot tell it by its address (the cluster's), so it is found here and said.
type Copy struct {
	Database string
	Servers  []string
}

// FindCopies looks, as the role of dsn, in the cluster's other databases it may connect to for
// organization servers that lead to relays (host:port). A database it cannot enter is skipped.
func FindCopies(ctx context.Context, dsn string, relays []string) ([]Copy, error) {
	if len(relays) == 0 {
		return nil, nil
	}
	want := map[string]bool{}
	for _, r := range relays {
		want[r] = true
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var names []string
	rows, err := conn.Query(ctx, `SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate AND datname <> current_database()
		AND has_database_privilege(datname, 'CONNECT') ORDER BY datname LIMIT 200`)
	if err == nil {
		names, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	conn.Close(context.Background())
	if err != nil {
		return nil, err
	}
	var out []Copy
	for _, n := range names {
		c := cfg.Copy()
		c.Database = n
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		other, err := pgx.ConnectConfig(cctx, c)
		if err != nil {
			cancel()
			continue
		}
		var servers []string
		if found, err := pinRelaysNamed(cctx, other); err == nil {
			for s, addr := range found {
				if want[addr] {
					servers = append(servers, s)
				}
			}
		}
		other.Close(context.Background())
		cancel()
		if len(servers) > 0 {
			sort.Strings(servers)
			out = append(out, Copy{Database: n, Servers: servers})
		}
	}
	return out, nil
}

// pinRelaysNamed: each organization server's relay (host:port), by server name.
func pinRelaysNamed(ctx context.Context, conn *pgx.Conn) (map[string]string, error) {
	rows, err := conn.Query(ctx, `SELECT s.srvname, coalesce(srvoptions, '{}') FROM pg_foreign_server s WHERE s.srvname ~ '^lokadb_org[0-9]+$'`)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	var name string
	var opts []string
	_, err = pgx.ForEachRow(rows, []any{&name, &opts}, func() error {
		host, port := "", "5432"
		for _, o := range opts {
			k, v, _ := strings.Cut(o, "=")
			switch k {
			case "host":
				host = v
			case "port":
				port = v
			}
		}
		if host != "" {
			out[name] = net.JoinHostPort(host, port)
		}
		return nil
	})
	return out, err
}
