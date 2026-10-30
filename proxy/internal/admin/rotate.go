package admin

import (
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// RotateCredentials gives a moved organization's roles (the owner's, lokadb_app_org<N>, and the
// direct path's, lokadb_dml_org<N>) new passwords (R01: a dump of the SaaS database carries the
// owner's in its user mappings; an hour later it opens nothing). Each role changes its own
// password at the customer (ALTER ROLE CURRENT_USER, sent as a SCRAM verifier: no password in the
// customer's logs), then the control plane gets it, then the SaaS database's user mappings of that
// role, with NOTIFY lokadb_meta in the same transaction: the proxies read the new credentials
// before their next statement. A postgres_fdw connection already open keeps its session; the next
// transaction connects with the new mapping. The new password is written first, sealed, to
// lokadb_cp.cred_rotations: a rotation cut in the middle is finished (or dropped) by the next one.
// An organization whose agent is not connected is not rotated: its old password stays.
type RotateOptions struct {
	CloudDSN, ControlPlane string
	KeyFile, KeyFileOld    string
	Org                    int64
	Log                    *slog.Logger
}

type RotateResult struct {
	Rotated []string // the roles that have a new password
	At      time.Time
}

const rotationsSQL = `CREATE TABLE IF NOT EXISTS lokadb_cp.cred_rotations (
  installation text NOT NULL,
  db text NOT NULL,
  org bigint NOT NULL,
  kind text NOT NULL,
  key_id text NOT NULL,
  next bytea,               -- the new DSN while it is being set (sealed for kind + ".next")
  started_at timestamptz,
  rotated_at timestamptz,   -- when the last rotation of this role ended
  PRIMARY KEY (installation, db, org, kind))`

func RotateCredentials(ctx context.Context, o RotateOptions) (RotateResult, error) {
	res := RotateResult{At: time.Now().UTC()}
	key, err := creds.LoadKey(o.KeyFile)
	if err != nil {
		return res, err
	}
	keys := [][]byte{key}
	if o.KeyFileOld != "" {
		if k, err := creds.LoadKey(o.KeyFileOld); err == nil {
			keys = append(keys, k)
		}
	}
	inst, err := CloudInstallation(ctx, o.CloudDSN)
	if err != nil {
		return res, err
	}
	db, err := dbName(o.CloudDSN)
	if err != nil {
		return res, err
	}
	cp, err := pgx.Connect(ctx, o.ControlPlane)
	if err != nil {
		return res, fmt.Errorf("control plane: %w", err)
	}
	defer cp.Close(context.Background())
	if _, err := cp.Exec(ctx, rotationsSQL); err != nil {
		return res, err
	}
	set, _, err := creds.Load(ctx, cp, inst, keys...)
	if err != nil {
		return res, err
	}
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return res, fmt.Errorf("cloud database: %w", err)
	}
	defer cloud.Close(context.Background())
	org := strconv.FormatInt(o.Org, 10)
	r := rotation{ctx: ctx, cp: cp, cloud: cloud, key: key, inst: inst, db: db, org: org, server: fmt.Sprintf("lokadb_org%d", o.Org)}
	for _, kind := range []string{creds.Owner, creds.Direct} {
		dsn := set.DSN(db, org, kind)
		if dsn == "" {
			continue
		}
		role, err := r.one(kind, dsn)
		if err != nil {
			return res, fmt.Errorf("%s: %w", kind, err)
		}
		res.Rotated = append(res.Rotated, role)
		if o.Log != nil {
			o.Log.Info("the organization's role has a new password", "org", o.Org, "role", role)
		}
	}
	return res, nil
}

type rotation struct {
	ctx                   context.Context
	cp, cloud             *pgx.Conn
	key                   []byte
	inst, db, org, server string
}

// one rotates the role of dsn: it returns the role.
func (r *rotation) one(kind, dsn string) (string, error) {
	pc, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", err
	}
	role := pc.User
	// a rotation cut in the middle: its new password may be the role's now
	var next []byte
	_ = r.cp.QueryRow(r.ctx, `SELECT next FROM lokadb_cp.cred_rotations WHERE installation = $1 AND db = $2 AND org = $3 AND kind = $4 AND next IS NOT NULL`,
		r.inst, r.db, r.org, kind).Scan(&next)
	cust, err := connect(r.ctx, dsn)
	if err != nil && next != nil {
		nd, oerr := creds.Open(r.key, r.inst, r.db, r.org, kind+".next", next)
		if oerr == nil {
			if c2, err2 := connect(r.ctx, nd); err2 == nil {
				c2.Close(context.Background())
				return role, r.finish(kind, role, nd) // the role has the new password: the rest is done now
			}
		}
	}
	if err != nil {
		return "", fmt.Errorf("the organization's database does not answer (is its agent connected?): its password stays: %w", err)
	}
	defer cust.Close(context.Background())
	pass := newPassword()
	nd, err := withCredentials(dsn, role, pass)
	if err != nil {
		return "", err
	}
	blob, err := creds.Seal(r.key, r.inst, r.db, r.org, kind+".next", nd)
	if err != nil {
		return "", err
	}
	if _, err := r.cp.Exec(r.ctx, `INSERT INTO lokadb_cp.cred_rotations (installation, db, org, kind, key_id, next, started_at) VALUES ($1, $2, $3, $4, $5, $6, now())
		ON CONFLICT (installation, db, org, kind) DO UPDATE SET key_id = excluded.key_id, next = excluded.next, started_at = now()`,
		r.inst, r.db, r.org, kind, creds.KeyID(r.key), blob); err != nil {
		return "", err
	}
	if _, err := cust.Exec(r.ctx, "ALTER ROLE CURRENT_USER PASSWORD "+lit(scramVerifier(pass))); err != nil {
		_, _ = r.cp.Exec(r.ctx, `UPDATE lokadb_cp.cred_rotations SET next = NULL WHERE installation = $1 AND db = $2 AND org = $3 AND kind = $4`, r.inst, r.db, r.org, kind)
		return "", fmt.Errorf("ALTER ROLE %s PASSWORD at the customer: %w", role, err)
	}
	return role, r.finish(kind, role, nd)
}

// finish: the control plane, then the SaaS database's user mappings of the role and NOTIFY
// lokadb_meta in one transaction (the proxies read the control plane again on it).
func (r *rotation) finish(kind, role, nd string) error {
	if err := creds.Put(r.ctx, r.cp, r.key, r.inst, r.db, r.org, kind, nd); err != nil {
		return fmt.Errorf("the control plane: %w", err)
	}
	pc, err := pgconn.ParseConfig(nd)
	if err != nil {
		return err
	}
	err = pgx.BeginFunc(r.ctx, r.cloud, func(tx pgx.Tx) error {
		rows, err := tx.Query(r.ctx, `SELECT usename::text FROM pg_user_mappings WHERE srvname = $1 AND $2 = ANY (umoptions)`, r.server, "user="+role)
		if err != nil {
			return err
		}
		who, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, w := range who {
			u := "PUBLIC"
			if w != "public" {
				u = q(w)
			}
			if _, err := tx.Exec(r.ctx, fmt.Sprintf("ALTER USER MAPPING FOR %s SERVER %s OPTIONS (SET password %s)", u, q(r.server), lit(pc.Password))); err != nil {
				return err
			}
		}
		_, err = tx.Exec(r.ctx, "NOTIFY lokadb_meta")
		return err
	})
	if err != nil {
		return fmt.Errorf("the SaaS database's user mappings: %w", err)
	}
	_, err = r.cp.Exec(r.ctx, `UPDATE lokadb_cp.cred_rotations SET next = NULL, rotated_at = now() WHERE installation = $1 AND db = $2 AND org = $3 AND kind = $4`, r.inst, r.db, r.org, kind)
	return err
}

// Rotated: when each organization's roles last got a new password (org -> the oldest of its roles).
func Rotated(ctx context.Context, controlPlane string) (map[string]time.Time, error) {
	cp, err := pgx.Connect(ctx, controlPlane)
	if err != nil {
		return nil, err
	}
	defer cp.Close(context.Background())
	var there bool
	if err := cp.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.cred_rotations') IS NOT NULL`).Scan(&there); err != nil || !there {
		return nil, err
	}
	rows, err := cp.Query(ctx, `SELECT org::text, min(rotated_at) FROM lokadb_cp.cred_rotations WHERE rotated_at IS NOT NULL GROUP BY org`)
	if err != nil {
		return nil, err
	}
	out := map[string]time.Time{}
	var org string
	var at time.Time
	_, err = pgx.ForEachRow(rows, []any{&org, &at}, func() error { out[org] = at; return nil })
	return out, err
}

func newPassword() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

// scramVerifier: what Postgres stores for password, SCRAM-SHA-256 (RFC 5802, 7677), as ALTER ROLE
// … PASSWORD takes it: the password itself never reaches the server.
func scramVerifier(password string) string {
	salt := make([]byte, 16)
	_, _ = rand.Read(salt)
	const iter = 4096
	salted, _ := pbkdf2.Key(sha256.New, password, salt, iter, 32)
	mac := func(k []byte, s string) []byte {
		h := hmac.New(sha256.New, k)
		h.Write([]byte(s))
		return h.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	serverKey := mac(salted, "Server Key")
	b64 := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("SCRAM-SHA-256$%d:%s$%s:%s", iter, b64(salt), b64(stored[:]), b64(serverKey))
}

// RotateReport: one organization's rotation by RotateDue.
type RotateReport struct {
	Org     string
	Rotated []string
	Err     error
}

// RotateDue rotates every organization with credentials in the control plane whose roles were
// rotated longer than every ago (or never): RotateCredentials for each, a failure (its agent not
// connected) leaving its old password and the others going on.
func RotateDue(ctx context.Context, o RotateOptions, every time.Duration) ([]RotateReport, error) {
	inst, err := CloudInstallation(ctx, o.CloudDSN)
	if err != nil {
		return nil, err
	}
	db, err := dbName(o.CloudDSN)
	if err != nil {
		return nil, err
	}
	cp, err := pgx.Connect(ctx, o.ControlPlane)
	if err != nil {
		return nil, fmt.Errorf("control plane: %w", err)
	}
	var there bool
	if err := cp.QueryRow(ctx, `SELECT to_regclass('lokadb_cp.org_credentials') IS NOT NULL`).Scan(&there); err != nil || !there {
		cp.Close(context.Background())
		return nil, err
	}
	if _, err := cp.Exec(ctx, rotationsSQL); err != nil {
		cp.Close(context.Background())
		return nil, err
	}
	rows, err := cp.Query(ctx, `SELECT DISTINCT c.org FROM lokadb_cp.org_credentials c
		WHERE c.installation = $1 AND c.db = $2 AND NOT EXISTS (SELECT FROM lokadb_cp.cred_rotations r
			WHERE r.installation = c.installation AND r.db = c.db AND r.org = c.org AND r.kind = c.kind
			AND r.next IS NULL AND r.rotated_at > now() - $3::interval)
		ORDER BY 1`, inst, db, fmt.Sprintf("%d seconds", int(every.Seconds())))
	var orgs []int64
	if err == nil {
		orgs, err = pgx.CollectRows(rows, pgx.RowTo[int64])
	}
	cp.Close(context.Background())
	if err != nil {
		return nil, err
	}
	var out []RotateReport
	for _, org := range orgs {
		oo := o
		oo.Org = org
		res, err := RotateCredentials(ctx, oo)
		out = append(out, RotateReport{Org: strconv.FormatInt(org, 10), Rotated: res.Rotated, Err: err})
	}
	return out, nil
}
