package admin_test

// review3 R3-9: disable waits for the tables' lock at most LockTimeout per attempt; a long
// reader makes it give up with nothing changed instead of queueing every query behind it.
// Needs LOKADB_REVIEW_DSN.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestDisableGivesUpOnABusyTable(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3l_cloud")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF; END $$`,
		`CREATE TABLE docs5 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs5 SELECT g, g % 2, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs5", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	// a long report: an open transaction that read the table
	report, err := pgx.Connect(ctx, cloud)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close(context.Background())
	tx, err := report.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "SELECT count(*) FROM docs5"); err != nil {
		t.Fatal(err)
	}
	o := admin.DisableOptions{CloudDSN: cloud, Spec: spec, LockTimeout: 300 * time.Millisecond, Attempts: 2, Log: log}
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- admin.Disable(ctx, o) }()
	select {
	case err = <-done:
	case <-time.After(30 * time.Second):
		_ = tx.Rollback(context.Background())
		t.Fatal("FINDING: disable still waits for the report's lock after 30s")
	}
	t.Logf("disable after %s: %v", time.Since(start).Round(time.Millisecond), err)
	if err == nil || !strings.Contains(err.Error(), "changed nothing") {
		t.Fatalf("want disable to give up with nothing changed, got %v", err)
	}
	if got := rev3One(t, cloud, `SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`); got != "true" {
		t.Fatalf("after the refusal the database must stay enabled, lokadb.tables: %s", got)
	}
	_ = tx.Rollback(ctx)
	if err := admin.Disable(ctx, o); err != nil {
		t.Fatal(err)
	}
	if got := rev3One(t, cloud, `SELECT count(*)::text FROM docs5`); got != "8" {
		t.Fatalf("after disable: %s rows, want 8", got)
	}
}
