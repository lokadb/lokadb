package admin_test

// Second independent review of admin (tasks/review3/admin.md), R3-4: move-back brings the rows
// the organization wrote past the cloud (straight into its database, which lokadb supports and
// captures) without their __refs rows, and keeps the __refs rows of rows deleted there. After it
// refs-diff cannot see them: it compares only the organizations in lokadb.orgs, which move-back
// leaves. Needs LOKADB_REVIEW_DSN (a disposable server, superuser), skipped without it.

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

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/migrate"
)

func rev3DB(t *testing.T, base, name string) string {
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
	u, _ := url.Parse(base)
	u.Path = "/" + name
	return u.String()
}

func rev3Exec(t *testing.T, dsn string, stmts ...string) {
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

func rev3One(t *testing.T, dsn, sql string) string {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	var s string
	if err := c.QueryRow(ctx, sql).Scan(&s); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return s
}

func TestReview3MoveBackLosesRefsOfRowsWrittenPastTheCloud(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("LOKADB_REVIEW_VERBOSE") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	cloud := rev3DB(t, base, "rev3_cloud")
	cust := rev3DB(t, base, "rev3_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs3 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs3 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs3", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("rev3_app", "apppw")
	err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
		Owner: "rev3_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	opts := rev3One(t, cloud, `SELECT array_to_string(srvoptions, ' ') FROM pg_foreign_server WHERE srvname = 'lokadb_org7'`)
	t.Logf("the organization's FDW server: %s (the customer DSN had sslmode=%s)", opts, u.Query().Get("sslmode"))
	where := rev3One(t, cust, `SELECT to_regclass('lokadb.docs3')::text`)
	// The organization writes past the cloud, as its own database allows: a new row, a deleted one.
	rev3Exec(t, cust,
		fmt.Sprintf(`INSERT INTO %s VALUES (1000, 7, 'written past the cloud')`, where),
		fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, where))
	before, err := migrate.RefsCompare(ctx, migrate.RefsOptions{CloudDSN: cloud, Orgs: []string{"7"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("refs-diff while moved: %s", strings.TrimSpace(migrate.RefsText(before, false)))
	if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 7,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
		t.Fatal(err)
	}
	got := rev3One(t, cloud, `SELECT concat_ws('|',
		(SELECT count(*) FROM docs3 WHERE id = 1000), (SELECT count(*) FROM docs3__refs WHERE id = 1000),
		(SELECT count(*) FROM docs3 WHERE id = 1), (SELECT count(*) FROM docs3__refs WHERE id = 1))`)
	after, err := migrate.RefsCompare(ctx, migrate.RefsOptions{CloudDSN: cloud, Orgs: []string{"7"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after move-back: row 1000, its refs row, row 1, its refs row: %s; refs-diff -org 7 reports %d table(s)", got, len(after))
	if got != "1|1|0|0" {
		t.Errorf("FINDING: after move-back the cloud has row 1000 without its __refs row and the __refs row of the deleted row 1 (%s, want 1|1|0|0); refs-diff -org 7 now reports %d tables, so nothing shows it",
			got, len(after))
	}
}
