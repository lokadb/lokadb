package admin_test

// review 3, R3-3 (disable's part): disable pours the rows of the other partitions into DEFAULT
// under session_replication_role replica; the application's triggers enabled ALWAYS fire there
// all the same, and an audit trigger logged inserts nobody made. Needs LOKADB_REVIEW_DSN.

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func TestDisableFiresNoAlwaysTrigger(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloud := rev3DB(t, base, "rev3t_cloud")
	cust := rev3DB(t, base, "rev3t_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_app') THEN CREATE ROLE rev3_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs9 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`CREATE TABLE audit9 (op text, id bigint)`,
		`CREATE FUNCTION audit9() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit9 VALUES (TG_OP, NEW.id); RETURN NULL; END $$`,
		`CREATE TRIGGER docs9_audit AFTER INSERT ON docs9 FOR EACH ROW EXECUTE FUNCTION audit9()`,
		`ALTER TABLE docs9 ENABLE ALWAYS TRIGGER docs9_audit`,
		`INSERT INTO docs9 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs9", TenantColumn: "org_id"}}}
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
	if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 7,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
		t.Fatal(err)
	}
	before := rev3One(t, cloud, `SELECT count(*)::text FROM audit9`)
	if err := admin.Disable(ctx, admin.DisableOptions{CloudDSN: cloud, Spec: spec, Log: log}); err != nil {
		t.Fatal(err)
	}
	after := rev3One(t, cloud, `SELECT count(*)::text FROM audit9`)
	mode := rev3One(t, cloud, `SELECT tgenabled::text FROM pg_trigger WHERE tgrelid = 'docs9'::regclass AND tgname = 'docs9_audit'`)
	t.Logf("audit rows before disable %s, after %s; the trigger's mode after it: %s", before, after, mode)
	if after != before {
		t.Errorf("FINDING: disable fired the application's ALWAYS trigger: audit rows %s before, %s after", before, after)
	}
	if mode != "A" {
		t.Errorf("the trigger's mode after disable: %s, want A (ALWAYS)", mode)
	}
}
