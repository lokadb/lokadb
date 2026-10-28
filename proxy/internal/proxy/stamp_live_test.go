package proxy

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// BACKLOG block 9, item 10, on a real Postgres (LOKADB_STAMP_DSN, a superuser of a throwaway
// database): the stamp reads lokadb.meta_changes once the event trigger records DDL; a GRANT, a
// row of lokadb.orgs, a new partition move it, a temporary table does not; without the event
// trigger it hashes the catalog as before.
func TestStampLive(t *testing.T) {
	dsn := os.Getenv("LOKADB_STAMP_DSN")
	if dsn == "" {
		t.Skip("LOKADB_STAMP_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	exec := func(sqls ...string) {
		t.Helper()
		for _, q := range sqls {
			if _, err := c.Exec(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	stamp := func() string {
		t.Helper()
		var s, role string
		if err := c.QueryRow(ctx, metaStampSQL).Scan(&s, &role); err != nil {
			t.Fatal(err)
		}
		return s
	}
	exec(`DROP SCHEMA IF EXISTS lokadb CASCADE`, `DROP TABLE IF EXISTS docs CASCADE`, `DROP EVENT TRIGGER IF EXISTS lokadb_meta_ddl`,
		`CREATE SCHEMA lokadb`,
		`CREATE TABLE lokadb.tables (table_name text PRIMARY KEY, tenant_column text, refs_table text, cloud_partition text)`,
		`CREATE TABLE lokadb.lookups (table_name text, column_name text, refs_table text)`,
		`CREATE TABLE lokadb.orgs (org bigint PRIMARY KEY, server text)`,
		`CREATE TABLE docs (id bigint, organization_id bigint) PARTITION BY LIST (organization_id)`,
		`CREATE TABLE docs__cloud PARTITION OF docs DEFAULT`,
		`INSERT INTO lokadb.tables VALUES ('docs', 'organization_id', 'docs__refs', 'docs__cloud')`)
	if s := stamp(); len(s) != 32 {
		t.Fatalf("before the change log: %q, want the catalog's hash", s)
	}
	exec(admin.MetaChangesSQL)
	v0 := stamp()
	if v0[:2] != "v|" {
		t.Fatalf("with the change log: %q, want its version", v0)
	}
	exec(`CREATE TEMP TABLE scratch (x int)`, `CREATE INDEX ON scratch (x)`)
	if s := stamp(); s != v0 {
		t.Errorf("a temporary table moved the stamp: %q -> %q", v0, s)
	}
	for _, q := range []string{`GRANT SELECT ON docs__cloud TO PUBLIC`, `INSERT INTO lokadb.orgs VALUES (1, 'x')`,
		`CREATE TABLE docs__o5 PARTITION OF docs FOR VALUES IN (5)`} {
		before := stamp()
		exec(q)
		if s := stamp(); s == before {
			t.Errorf("%s left the stamp as it was (%q)", q, s)
		}
	}
	exec(`BEGIN`, `CREATE TABLE docs__o6 PARTITION OF docs FOR VALUES IN (6)`)
	inTx := stamp()
	exec(`ROLLBACK`)
	if s := stamp(); s != inTx {
		t.Logf("in the transaction %q, after its rollback %q", inTx, s)
	}
	exec(`ALTER EVENT TRIGGER lokadb_meta_ddl DISABLE`)
	if s := stamp(); len(s) != 32 {
		t.Errorf("the event trigger disabled: %q, want the catalog's hash", s)
	}
}

// The stamp's cost at a thousand partitions, hashed and read from the change log (median of 20).
func TestStampLiveCost(t *testing.T) {
	dsn := os.Getenv("LOKADB_STAMP_DSN")
	if dsn == "" {
		t.Skip("LOKADB_STAMP_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, q := range []string{`DROP SCHEMA IF EXISTS lokadb CASCADE`, `DROP TABLE IF EXISTS docs CASCADE`, `DROP EVENT TRIGGER IF EXISTS lokadb_meta_ddl`,
		`CREATE SCHEMA lokadb`,
		`CREATE TABLE lokadb.tables (table_name text PRIMARY KEY, tenant_column text, refs_table text, cloud_partition text)`,
		`CREATE TABLE lokadb.orgs (org bigint PRIMARY KEY, server text)`,
		`CREATE TABLE docs (id bigint, organization_id bigint) PARTITION BY LIST (organization_id)`,
		`DO $$ BEGIN FOR i IN 1..1000 LOOP EXECUTE format('CREATE TABLE docs__o%s PARTITION OF docs FOR VALUES IN (%s)', i, i); EXECUTE format('GRANT SELECT ON docs__o%s TO PUBLIC', i); END LOOP; END $$`,
		`INSERT INTO lokadb.tables VALUES ('docs', 'organization_id', 'docs__refs', 'docs__cloud')`,
		admin.MetaChangesSQL} {
		if _, err := c.Exec(ctx, q); err != nil {
			t.Fatalf("%.80s: %v", q, err)
		}
	}
	median := func() time.Duration {
		var ds []time.Duration
		for i := 0; i < 20; i++ {
			t0 := time.Now()
			var s, role string
			if err := c.QueryRow(ctx, metaStampSQL).Scan(&s, &role); err != nil {
				t.Fatal(err)
			}
			ds = append(ds, time.Since(t0))
		}
		slices.Sort(ds)
		return ds[len(ds)/2]
	}
	logged := median()
	if _, err := c.Exec(ctx, `ALTER EVENT TRIGGER lokadb_meta_ddl DISABLE`); err != nil {
		t.Fatal(err)
	}
	hashed := median()
	t.Logf("1000 partitions: the stamp hashed %v, read from the change log %v", hashed, logged)
}
