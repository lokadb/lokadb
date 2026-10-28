package admin_test

// review3 R3-4, the other side of the fix: a row the organization wrote past the cloud with an
// id another organization holds stops move-back before the switch; the organization stays in
// its database, and move-back passes once the row is gone. Needs LOKADB_REVIEW_DSN.

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

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestMoveBackRefusesAnIDAnotherOrganizationHolds(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3r_cloud")
	cust := rev3DB(t, base, "rev3r_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs4 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs4 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs4", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "rev3_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("rev3_app", "apppw")
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
		Owner: "rev3_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log}); err != nil {
		t.Fatal(err)
	}
	where := rev3One(t, cust, `SELECT to_regclass('lokadb.docs4')::text`)
	// id 6 is organization 8's in the cloud
	rev3Exec(t, cust, fmt.Sprintf(`INSERT INTO %s VALUES (6, 7, 'written past the cloud')`, where))
	back := admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 7,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}
	err := admin.MoveBack(ctx, back)
	t.Logf("move-back: %v", err)
	if err == nil || !strings.Contains(err.Error(), "another organization holds") {
		t.Fatalf("want a refusal naming the id another organization holds, got %v", err)
	}
	if got := rev3One(t, cloud, `SELECT (SELECT count(*) FROM lokadb.orgs WHERE org = 7)::text || '|' || (SELECT tenant FROM docs4__refs WHERE id = 6)::text`); got != "1|8" {
		t.Fatalf("after the refusal: organization 7 moved, id 6 is organization 8's: want 1|8, got %s", got)
	}
	rev3Exec(t, cust, fmt.Sprintf(`DELETE FROM %s WHERE id = 6`, where))
	if err := admin.MoveBack(ctx, back); err != nil {
		t.Fatal(err)
	}
	if got := rev3One(t, cloud, `SELECT (SELECT count(*) FROM lokadb.orgs)::text || '|' || (SELECT count(*) FROM docs4__refs WHERE tenant = 7)::text`); got != "0|5" {
		t.Fatalf("after move-back: want 0|5 (no organization moved, 5 refs rows of organization 7), got %s", got)
	}
}
