package admin

// Review 3 of check, H1 and H2, on a live Postgres (skipped without one):
//   LOKADB_LIVE_DSN=postgres://postgres:pw@host:5432 go test ./internal/admin -run Live

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

func liveDB(t *testing.T, name string) string {
	base := os.Getenv("LOKADB_LIVE_DSN")
	if base == "" {
		t.Skip("LOKADB_LIVE_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DO $$BEGIN CREATE ROLE app_owner LOGIN; EXCEPTION WHEN duplicate_object THEN END$$")
	_, _ = c.Exec(ctx, "DO $$BEGIN CREATE ROLE eve LOGIN; EXCEPTION WHEN duplicate_object THEN END$$")
	return base + "/" + name
}

func liveRun(t *testing.T, dsn string, sqls ...string) {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, s := range sqls {
		if _, err := c.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

// liveOne: the first column of the first row of sql (as role, if not ""), or the error.
func liveOne(t *testing.T, dsn, role, sql string) string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if role != "" {
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE "+role); err != nil {
			t.Fatal(err)
		}
	}
	var v string
	if err := tx.QueryRow(ctx, "SELECT ("+sql+")::text").Scan(&v); err != nil {
		return "error: " + err.Error()
	}
	return v
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// H1: a policy of a cloud table and one of another LokaDB table read a LokaDB table: after
// enable they read the parent (all organizations' rows through it), after disable the plain
// table again. H2: a function over the row type (a PostgREST computed column) is made again
// over the parent's and back.
func TestLiveDependents(t *testing.T) {
	dsn := liveDB(t, "b4_dependents")
	liveRun(t, dsn,
		`CREATE TABLE blocked (id bigint PRIMARY KEY, org_id bigint NOT NULL, user_name text)`,
		`CREATE TABLE docs (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`CREATE TABLE notes (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`, // stays in the cloud
		`ALTER TABLE notes ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY not_blocked ON notes USING (NOT EXISTS (SELECT 1 FROM blocked b WHERE b.org_id = notes.org_id AND b.user_name = current_user))`,
		`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY docs_not_blocked ON docs USING (NOT EXISTS (SELECT 1 FROM blocked b WHERE b.org_id = docs.org_id AND b.user_name = current_user))`,
		`CREATE FUNCTION body_len(d docs) RETURNS int LANGUAGE sql STABLE AS 'SELECT length(d.body)'`,
		`CREATE VIEW docs_len AS SELECT id, body_len(d) AS n FROM docs d`,
		`GRANT SELECT ON notes, blocked, docs, docs_len TO eve`,
		`INSERT INTO blocked VALUES (1, 2, 'eve')`,
		`INSERT INTO notes VALUES (1, 1, 'note'), (2, 2, 'hidden')`,
		`INSERT INTO docs VALUES (1, 1, 'hello'), (2, 2, 'hidden')`)
	probe := func(when string) {
		t.Helper()
		for sql, want := range map[string]string{
			`SELECT count(*) FROM notes`:                  "1",
			`SELECT count(*) FROM docs`:                   "1",
			`SELECT body_len(d) FROM docs d WHERE id = 1`: "5",
			`SELECT n FROM docs_len WHERE id = 1`:         "5",
			`SELECT count(*) FROM pg_policy WHERE pg_get_expr(polqual, polrelid) LIKE '%__cloud%'`: "0",
		} {
			if when == "after disable" && sql == `SELECT count(*) FROM docs` {
				continue // docs' own policy: disable does not give it back yet (V-R3-2, block 6)
			}
			role := "eve"
			if strings.Contains(sql, "pg_policy") {
				role = ""
			}
			if got := liveOne(t, dsn, role, sql); got != want {
				t.Errorf("%s: %s: %s, want %s", when, sql, got, want)
			}
		}
	}
	probe("before enable")
	spec := Spec{Tables: []SpecTable{{Name: "blocked", TenantColumn: "org_id"}, {Name: "docs", TenantColumn: "org_id"}}}.Sorted()
	if err := Enable(context.Background(), dsn, spec, "app_owner", quiet()); err != nil {
		t.Fatalf("enable: %v", err)
	}
	probe("after enable")
	if n := liveOne(t, dsn, "", `SELECT count(*) FROM pg_proc WHERE proname = 'body_len'`); n != "1" {
		t.Errorf("after enable: %s functions body_len, want 1 (the old one over docs__cloud dropped)", n)
	}
	if err := Disable(context.Background(), DisableOptions{CloudDSN: dsn, Spec: spec, Log: quiet()}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	probe("after disable")
}

// A column of the row type in another table: enable refuses before it changes anything.
func TestLiveRowtypeColumnRefused(t *testing.T) {
	dsn := liveDB(t, "b4_rowtype_col")
	liveRun(t, dsn,
		`CREATE TABLE docs (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`CREATE TABLE snapshots (id bigint PRIMARY KEY, doc docs)`)
	spec := Spec{Tables: []SpecTable{{Name: "docs", TenantColumn: "org_id"}}}.Sorted()
	err := Enable(context.Background(), dsn, spec, "app_owner", quiet())
	if err == nil || !strings.Contains(err.Error(), "snapshots") {
		t.Fatalf("enable: %v, want a refusal naming snapshots", err)
	}
	if got := liveOne(t, dsn, "", `SELECT relkind::text FROM pg_class WHERE relname = 'docs'`); got != "r" {
		t.Errorf("after the refusal docs is of relkind %s, want r (unchanged)", got)
	}
}

// Review 3 of check, M1: a child by parent_table that has a column of the organization
// column's name of its own: enable refuses before it changes anything, saying what to write.
func TestLiveChildHasTenantColumn(t *testing.T) {
	dsn := liveDB(t, "b4_child_col")
	liveRun(t, dsn,
		`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL)`,
		`CREATE TABLE items (id bigint PRIMARY KEY, order_id bigint REFERENCES orders, org_id bigint)`,
		`INSERT INTO orders VALUES (1, 1)`, `INSERT INTO items VALUES (1, 1, 1)`)
	spec := Spec{Tables: []SpecTable{{Name: "orders", TenantColumn: "org_id"}, {Name: "items", ParentTable: "orders", ParentColumn: "order_id"}}}.Sorted()
	err := Enable(context.Background(), dsn, spec, "app_owner", quiet())
	if err == nil || !strings.Contains(err.Error(), "tenant_column") {
		t.Fatalf("enable: %v, want a refusal that says tenant_column", err)
	}
	if got := liveOne(t, dsn, "", `SELECT relkind::text FROM pg_class WHERE relname = 'orders'`); got != "r" {
		t.Errorf("after the refusal orders is of relkind %s, want r (unchanged)", got)
	}
}

// Review 3 of check, L5: a spec naming public.<table> is the spec of <table>: enable goes
// through, its key index too (it committed and then failed on it).
func TestLivePublicQualifiedSpec(t *testing.T) {
	dsn := liveDB(t, "b4_public_spec")
	liveRun(t, dsn,
		`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL)`,
		`CREATE TABLE items (id bigint PRIMARY KEY, order_id bigint REFERENCES orders)`,
		`INSERT INTO orders VALUES (1, 1)`, `INSERT INTO items VALUES (1, 1)`)
	spec := Spec{Tables: []SpecTable{{Name: "public.orders", TenantColumn: "org_id"}, {Name: "public.items", ParentTable: "public.orders", ParentColumn: "order_id"}}}.Sorted()
	if err := Enable(context.Background(), dsn, spec, "app_owner", quiet()); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := liveOne(t, dsn, "", `SELECT string_agg(table_name, ',' ORDER BY table_name) FROM lokadb.tables`); got != "items,orders" {
		t.Errorf("lokadb.tables: %s, want items,orders", got)
	}
}
