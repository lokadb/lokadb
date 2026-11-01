package admin_test

// review3 R4-4: a routing key declared on a live database must not lose a value written by a
// transaction that is open while the map is filled. Needs LOKADB_REVIEW_DSN.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestKeyDeclaredWhileAWriterIsOpen(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dsn := rev3DB(t, base, "r44_key")
	rev3Exec(t, dsn,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF; END $$`,
		`CREATE TABLE docs (id bigserial PRIMARY KEY, org_id bigint NOT NULL, slug text UNIQUE, title text)`,
		`INSERT INTO docs (org_id, slug, title) SELECT 1 + (g % 2), 's' || g, 't' || g FROM generate_series(1, 10) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, dsn, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	// a writer whose transaction is open while the key is declared
	w, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)
	tx, err := w.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO docs (org_id, slug, title) VALUES (1, 'race', 'x')`); err != nil {
		t.Fatal(err)
	}
	spec.Routing = &admin.Routing{Keys: []admin.Key{{Table: "docs", Column: "slug"}}}
	done := make(chan error, 1)
	go func() { done <- admin.ApplyRouting(ctx, dsn, spec, log) }()
	time.Sleep(2 * time.Second) // the declaration is under way, waiting for the writer
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var n int
	if err := c.QueryRow(ctx, `SELECT count(*) FROM lokadb.`+admin.KeyTable("docs", "slug")+` WHERE id IN ('s1', 'race')`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("the key map has %d of the 2 values (s1, race): the value written while it was declared is lost", n)
	}
}
