package admin_test

// review3 R3-13: move-back could not mark the copy it leaves in the organization's database
// (here an event trigger refuses COMMENT, as for a role that does not own the tables); a later
// move-org into the same database must not drop that copy. Needs LOKADB_REVIEW_DSN.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestMoveOrgKeepsTheCopyMoveBackCouldNotMark(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3m_cloud")
	cust := rev3DB(t, base, "rev3m_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs6 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs6 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs6", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("rev3_app", "apppw")
	move := admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
		Owner: "rev3_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log}
	if err := admin.MoveOrg(ctx, move); err != nil {
		t.Fatal(err)
	}
	where := rev3One(t, cust, `SELECT to_regclass('lokadb.docs6')::text`)
	// the customer's own object on its copy, and nobody may comment on the tables
	rev3Exec(t, cust,
		fmt.Sprintf(`CREATE VIEW public.customer_report AS SELECT count(*) FROM %s`, where),
		`CREATE FUNCTION public.no_comment() RETURNS event_trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'no comments here'; END $$`,
		`CREATE EVENT TRIGGER no_comment ON ddl_command_start WHEN TAG IN ('COMMENT') EXECUTE FUNCTION public.no_comment()`)
	if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 7,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
		t.Fatal(err)
	}
	t.Logf("after move-back: the copy's mark %q", rev3One(t, cust, fmt.Sprintf(`SELECT coalesce(obj_description(to_regclass('%s'), 'pg_class'), '')`, where)))
	rev3Exec(t, cust, `DROP EVENT TRIGGER no_comment`)
	err := admin.MoveOrg(ctx, move)
	t.Logf("move-org into the same database: %v", err)
	left := rev3One(t, cust, `SELECT (to_regclass('public.customer_report') IS NOT NULL)::text`)
	if left != "true" {
		t.Fatalf("FINDING: move-org dropped the copy move-back left with CASCADE, the customer's view on it is gone (move-org: %v)", err)
	}
	if err == nil {
		t.Fatal("want move-org to refuse the copy move-back left")
	}
	// the customer drops it: move-org goes ahead
	rev3Exec(t, cust, `DROP VIEW public.customer_report`, fmt.Sprintf(`DROP TABLE %s`, where))
	if err := admin.MoveOrg(ctx, move); err != nil {
		t.Fatal(err)
	}
}
