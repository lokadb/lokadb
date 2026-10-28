package admin

// Review 3 of admin, R3-8 and R3-16, on a live Postgres (skipped without one):
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

func liveConn(t *testing.T, name string, sqls ...string) *pgx.Conn {
	t.Helper()
	base := os.Getenv("LOKADB_LIVE_DSN")
	if base == "" {
		t.Skip("LOKADB_LIVE_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DO $$BEGIN CREATE ROLE app_owner LOGIN; EXCEPTION WHEN duplicate_object THEN END$$")
	c.Close(ctx)
	d, err := pgx.Connect(ctx, base+"/"+name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close(ctx) })
	for _, s := range sqls {
		if _, err := d.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return d
}

func liveVal(t *testing.T, c *pgx.Conn, sql string) string {
	t.Helper()
	var v string
	if err := c.QueryRow(context.Background(), "SELECT coalesce(("+sql+")::text, '-')").Scan(&v); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return v
}

// An enum label the organization's database lacks goes where the cloud has it (it went to the
// end: < and ORDER BY differed); a function of a CHECK is created with its owner in one
// transaction (an ALTER OWNER that fails leaves no function behind owned by the administrator).
func TestLiveCustomerDeps(t *testing.T) {
	ctx := context.Background()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := liveConn(t, "b4_deps_cloud",
		`CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy', 'glad')`,
		`CREATE FUNCTION valid_note(n text) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN RETURN length(n) < 100; END $$`,
		`CREATE TABLE t (id bigint PRIMARY KEY, org bigint, m mood, note text CHECK (valid_note(note)))`)
	cust := liveConn(t, "b4_deps_cust", `CREATE TYPE mood AS ENUM ('sad', 'happy')`)
	tables := []moveTable{{name: "t", tenant: "org", key: "id"}}
	if err := customerDeps(ctx, cloud, cust, tables, "app_owner", "public", quiet); err != nil {
		t.Fatalf("customerDeps: %v", err)
	}
	if got := liveVal(t, cust, `SELECT string_agg(enumlabel, ',' ORDER BY enumsortorder) FROM pg_enum WHERE enumtypid = 'mood'::regtype`); got != "sad,ok,happy,glad" {
		t.Errorf("the organization's mood: %s, want sad,ok,happy,glad", got)
	}
	if got := liveVal(t, cust, `SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE proname = 'valid_note'`); got != "app_owner" {
		t.Errorf("valid_note there is owned by %s, want app_owner", got)
	}

	cust2 := liveConn(t, "b4_deps_cust2")
	err := customerDeps(ctx, cloud, cust2, tables, "no_such_role_b4", "public", quiet)
	if err == nil || !strings.Contains(err.Error(), "no_such_role_b4") {
		t.Fatalf("customerDeps with an owner that does not exist: %v", err)
	}
	if got := liveVal(t, cust2, `SELECT count(*) FROM pg_proc WHERE proname = 'valid_note'`); got != "0" {
		t.Errorf("after the failed ALTER OWNER: %s functions valid_note (owned by the administrator), want 0", got)
	}
}
