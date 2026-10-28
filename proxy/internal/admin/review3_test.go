package admin

// Second independent review of admin (tasks/review3/admin.md): proofs that need a Postgres,
// the ones of block 5 (R3-1, R3-3; R3-2 is block 6's).
// They run only with LOKADB_REVIEW_DSN, an admin DSN of a disposable server (superuser); each
// test makes databases of its own there. Without it they are skipped.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func review3Base(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("LOKADB_REVIEW_DSN")
	if dsn == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	return dsn
}

// review3DB makes a fresh database on the server of base and returns its DSN.
func review3DB(t *testing.T, base, name string) string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	_, _ = c.Exec(ctx, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s'`, name))
	if _, err := c.Exec(ctx, "DROP DATABASE IF EXISTS "+name); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	return u.String()
}

func review3Exec(t *testing.T, dsn string, stmts ...string) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, s := range stmts {
		if _, err := c.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func review3One(t *testing.T, dsn, sql string) string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	r, err := rowsOf(ctx, c, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(r) == 0 {
		return ""
	}
	return strings.Join(r[0], "|")
}

func review3Log() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// R3-3. The online split (splitTable) deletes the organization's rows from DEFAULT under
// session_replication_role = replica, which is meant to keep the application's triggers quiet.
// A trigger the application enabled ALWAYS (or REPLICA) fires all the same: here it writes an
// audit row "deleted" for every row of the organization, none of which the application deleted.
func TestReview3SplitFiresAlwaysTriggers(t *testing.T) {
	base := review3Base(t)
	dsn := review3DB(t, base, "rev3_split")
	review3Exec(t, dsn,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF; END $$`,
		`CREATE TABLE docs2 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`CREATE TABLE audit (id bigint, op text)`,
		`GRANT INSERT ON audit TO rev3_owner`,
		`CREATE FUNCTION audit_del() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.audit VALUES (OLD.id, TG_OP); RETURN OLD; END $$`,
		`CREATE TRIGGER docs2_audit AFTER DELETE ON docs2 FOR EACH ROW EXECUTE FUNCTION audit_del()`,
		`ALTER TABLE docs2 ENABLE ALWAYS TRIGGER docs2_audit`,
		`INSERT INTO docs2 VALUES (1, 7, 'a'), (2, 7, 'b'), (3, 7, 'c'), (4, 8, 'd')`)
	spec := Spec{Tables: []SpecTable{{Name: "docs2", TenantColumn: "org_id"}}}
	ctx := context.Background()
	if err := Enable(ctx, dsn, spec, "rev3_owner", review3Log()); err != nil {
		t.Fatal(err)
	}
	t.Logf("trigger on DEFAULT after enable (name, tgenabled): %s", review3One(t, dsn,
		`SELECT string_agg(tgname || ' ' || tgenabled::text, ', ') FROM pg_trigger WHERE tgrelid = 'docs2__cloud'::regclass AND NOT tgisinternal`))
	cloud, err := connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer cloud.Close(ctx)
	o := MoveOptions{CloudDSN: dsn, Spec: spec, Org: 7, LockTimeout: 5 * time.Second, Attempts: 3, Log: review3Log()}
	if err := splitOrg(ctx, cloud, o, review3Log()); err != nil {
		t.Fatal(err)
	}
	got := review3One(t, dsn, `SELECT (SELECT count(*) FROM audit)::text, (SELECT count(*) FROM docs2)::text,
		(SELECT count(*) FROM docs2__o7)::text`)
	t.Logf("after the split of organization 7: audit rows, rows of docs2, rows of docs2__o7: %s", got)
	// switched off for the move only: back to ALWAYS on DEFAULT after it
	if mode := review3One(t, dsn, `SELECT tgenabled::text FROM pg_trigger WHERE tgrelid = 'docs2__cloud'::regclass AND tgname = 'docs2_audit'`); mode != "A" {
		t.Errorf("the application's trigger on DEFAULT after the split: tgenabled %q, want A", mode)
	}
	if !strings.HasPrefix(got, "0|") {
		t.Errorf("FINDING: the split fired the application's ALWAYS trigger: audit holds %s 'DELETE' rows of organization 7, which nobody deleted (rows still there: %s)",
			strings.Split(got, "|")[0], strings.Split(got, "|")[1])
	}
}

// R3-1. applyLog's cursor (A5) re-reads late only the entries of the transactions in the
// snapshot's xip. A transaction whose xid is not below the snapshot's xmax (xmax is one past the
// last COMPLETED xid, not the next xid to assign) is running but absent from xip: its entry,
// with a seq below one the round applied, is never read again by the catch-up. The switch's
// drain reads the log from the start and applies it then, after newer images of the same row:
// the organization's copy keeps an old version of the row. Here: T writes row 1 (seq 1) with
// the highest xid, U (an older xid) writes row 2 (seq 2) and commits; round 1 applies seq 2;
// T commits, W updates row 1 (seq 3); round 2 applies seq 3, never seq 1; the drain applies seq
// 1 last: row 1 there is T's, the cloud's is W's.
func TestReview3ApplyLogSkipsTransactionAboveXmax(t *testing.T) {
	base := review3Base(t)
	cloudDSN := review3DB(t, base, "rev3_logc")
	custDSN := review3DB(t, base, "rev3_logk")
	review3Exec(t, cloudDSN, "CREATE SCHEMA lokadb", captureDDL(),
		`INSERT INTO lokadb.move_heartbeat VALUES (7, clock_timestamp(), false)`)
	review3Exec(t, custDSN, `CREATE TABLE rev3t__o7 (id bigint PRIMARY KEY, org_id bigint, v text)`)
	ctx := context.Background()
	conn := func(dsn string) *pgx.Conn {
		c, err := connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		return c
	}
	cloud, cust, cu, ct, cw := conn(cloudDSN), conn(custDSN), conn(cloudDSN), conn(cloudDSN), conn(cloudDSN)
	tbl := moveTable{name: "rev3t", tenant: "org_id", key: "id", org: 7, val: "7",
		cols: [][]string{{"id", "bigint", "true", "", "", ""}, {"org_id", "bigint", "false", "", "", ""}, {"v", "text", "false", "", "", ""}}}
	o := MoveOptions{Org: 7, Log: review3Log()}
	logRow := func(c *pgx.Conn, id int, v string) {
		if _, err := c.Exec(ctx, `INSERT INTO lokadb.move_log (org, tbl, op, row) VALUES (7, 'rev3t', 'I', jsonb_build_object('id', $1::bigint, 'org_id', 7, 'v', $2::text))`, id, v); err != nil {
			t.Fatal(err)
		}
	}
	mustExec := func(c *pgx.Conn, s string) {
		if _, err := c.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	var xidU, xidT string
	mustExec(cu, "BEGIN")
	if err := cu.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidU); err != nil { // U: the older xid
		t.Fatal(err)
	}
	mustExec(ct, "BEGIN")
	logRow(ct, 1, "T") // T: the newer xid, the smaller seq
	if err := ct.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xidT); err != nil {
		t.Fatal(err)
	}
	logRow(cu, 2, "U")
	mustExec(cu, "COMMIT")
	cur := &logCursor{}
	if _, err := applyLog(ctx, nil, cloud, cust, o, []moveTable{tbl}, 500, cur); err != nil {
		t.Fatal(err)
	}
	t.Logf("round 1: xid U %s, xid T %s (running); cursor last %d, snapshot %s", xidU, xidT, cur.last, cur.snap)
	mustExec(ct, "COMMIT")
	mustExec(cw, "BEGIN")
	logRow(cw, 1, "W") // W changes row 1 after T
	mustExec(cw, "COMMIT")
	if _, err := applyLog(ctx, nil, cloud, cust, o, []moveTable{tbl}, 500, cur); err != nil {
		t.Fatal(err)
	}
	afterCatchUp := review3One(t, custDSN, "SELECT coalesce((SELECT v FROM rev3t__o7 WHERE id = 1), '-')")
	left := review3One(t, cloudDSN, "SELECT coalesce(string_agg(seq::text || ':' || (row ->> 'v'), ','), '') FROM lokadb.move_log")
	// the switch: every writer done, the log drained from the start (drainLog)
	tx, err := cloud.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := drainLog(ctx, tx, cust, o, []moveTable{tbl}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	final := review3One(t, custDSN, "SELECT coalesce((SELECT v FROM rev3t__o7 WHERE id = 1), '-')")
	t.Logf("row 1 at the organization after the catch-up: %s; left in the log: %s; after the switch's drain: %s (the cloud's row 1 is W's)", afterCatchUp, left, final)
	if final != "W" {
		t.Errorf("FINDING: the organization's copy of row 1 is %q after the switch, the cloud's is \"W\": T's entry (seq 1) was skipped by the catch-up (its xid was not in the snapshot's xip) and applied by the drain after W's", final)
	}
}

// R3-6. Before the switch commits, verifyCounts compared the number of rows only: a row of the
// customer's copy with an old version (here a title the application changed after the copy)
// passed as long as the count matched. Now the sum of the rows' digests must match too.
func TestReview3VerifyCountsCatchesOldVersion(t *testing.T) {
	base := review3Base(t)
	ctx := context.Background()
	cloud := review3DB(t, base, "r36_cloud")
	cust := review3DB(t, base, "r36_cust")
	review3Exec(t, cloud, "CREATE TABLE docs__o1 (id bigint PRIMARY KEY, title text)",
		"INSERT INTO docs__o1 VALUES (1, 'a'), (2, 'b-new'), (3, 'c')")
	review3Exec(t, cust, "CREATE TABLE docs (id bigint PRIMARY KEY, title text)",
		"INSERT INTO docs VALUES (1, 'a'), (2, 'b-old'), (3, 'c')")
	tables := []moveTable{{name: "docs", org: 1, cols: [][]string{
		{"id", "bigint", "t", "", "", ""}, {"title", "text", "f", "", "", ""}}}}
	run := func() error {
		cc, err := pgx.Connect(ctx, cloud)
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Close(ctx)
		sc, err := pgx.Connect(ctx, cust)
		if err != nil {
			t.Fatal(err)
		}
		defer sc.Close(ctx)
		tx, err := cc.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		snap, err := sc.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer snap.Rollback(ctx)
		return verifyCounts(ctx, tx, snap, tables, review3Log(), "nothing switched")
	}
	err := run()
	t.Logf("different version, same count: %v", err)
	if err == nil {
		t.Fatal("verifyCounts let a row with an old version through")
	}
	review3Exec(t, cust, "UPDATE docs SET title = 'b-new' WHERE id = 2")
	if err := run(); err != nil {
		t.Fatalf("same rows: %v", err)
	}
}
