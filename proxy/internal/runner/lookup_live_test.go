package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

// A customer picked in the panel by its value (a ULID, a uuid, a text key) gets its number at its first
// move: nobody runs lokadb org-add for it. The same customer keeps its number; a value that is no
// customer's gets a plain error and no number.
func TestLookupNumbersACustomer(t *testing.T) {
	base := os.Getenv("INIT_TEST_DSN")
	if base == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	for _, c := range []struct{ name, typ, value, other string }{
		{"ulid", "text", "01m37m8w3kq2v5x9z0abcdefgh", "01m37m8w3kq2v5x9z0zzzzzzzz"},
		{"uuid", "uuid", "7d1f0b5e-3c2a-4b8e-9f10-1a2b3c4d5e6f", "00000000-0000-4000-8000-000000000001"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
			u, _ := url.Parse(base)
			u.Path = "/postgres"
			root, err := pgx.Connect(ctx, u.String())
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close(ctx)
			db := "lk_" + c.name
			_, _ = root.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)")
			if _, err := root.Exec(ctx, "CREATE DATABASE "+db); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = root.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)") })
			_, _ = root.Exec(ctx, "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'lk_owner') THEN CREATE ROLE lk_owner; END IF; END $$")
			u.Path = "/" + db
			dsn := u.String()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			for _, q := range []string{
				"CREATE TABLE users (id " + c.typ + " PRIMARY KEY, name text)",
				"CREATE TABLE builds (id bigint PRIMARY KEY, user_id " + c.typ + " NOT NULL REFERENCES users(id), slug text)",
				"INSERT INTO users VALUES ('" + c.value + "', 'Evgeny')",
				"INSERT INTO builds VALUES (1, '" + c.value + "', 'a')",
			} {
				if _, err := conn.Exec(ctx, q); err != nil {
					t.Fatal(err)
				}
			}
			spec := admin.Spec{Tables: []admin.SpecTable{{Name: "builds", TenantColumn: "user_id"}}}
			if err := admin.Enable(ctx, dsn, spec, "lk_owner", quiet); err != nil {
				t.Fatalf("enable: %v", err)
			}
			cfg := &Config{DB: dsn, Log: quiet, spec: spec.Sorted(), settings: &cloudproto.Settings{OrgTable: "users", OrgKey: "id"}}
			o := &Ops{Cfg: cfg, Log: quiet}
			first, err := o.Lookup(ctx, cloudproto.LookupArgs{Tenant: c.value})
			if err != nil || first.Org <= 0 {
				t.Fatalf("lookup: %+v %v", first, err)
			}
			again, err := o.Lookup(ctx, cloudproto.LookupArgs{Tenant: c.value})
			if err != nil || again.Org != first.Org {
				t.Fatalf("the second lookup: %+v %v, want %d", again, err, first.Org)
			}
			_, err = o.Lookup(ctx, cloudproto.LookupArgs{Tenant: c.other})
			var je *cloudproto.JobError
			if !errors.As(err, &je) || je.Code != "no_customer" {
				t.Fatalf("an unknown customer: %v", err)
			}
			var n int
			if err := conn.QueryRow(ctx, "SELECT count(*) FROM lokadb.tenants").Scan(&n); err != nil || n != 1 {
				t.Fatalf("lokadb.tenants has %d rows (%v), want only the real customer", n, err)
			}
		})
	}
}
