package admin_test

// Review 4, R4-2. move-back took "the organization's partitions are in the cloud and lokadb.orgs
// has no row of it" for a move-back of its own that had switched, and finished it with the
// customer schema "" (public). A move-org that fails after its split leaves the same: move-back
// then marked the customer's OWN public tables of those names as the returned copy ("the
// customer may drop it"), and the next move-org moved them aside or dropped them. A move-back
// interrupted after its switch was finished the same way: its copy in schema lokadb kept the
// capture triggers and no mark. Needs LOKADB_REVIEW_DSN.

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
)

// r4Stand: a cloud with documents of organizations 1 and 2, enabled; the customer's database
// has its own public.documents (two rows), so the organization's tables go to schema lokadb.
func r4Stand(t *testing.T, name string) (cloud, cust, app string, spec admin.Spec) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	cloud = rev3DB(t, base, name+"_cloud")
	cust = rev3DB(t, base, name+"_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'r4_owner') THEN CREATE ROLE r4_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'r4_app') THEN CREATE ROLE r4_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, name text)`,
		`INSERT INTO organizations (name) VALUES ('a'), ('b')`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations(id), title text)`,
		`INSERT INTO documents (organization_id, title) SELECT 1 + g % 2, 't' || g FROM generate_series(1, 2000) g`)
	rev3Exec(t, cust,
		fmt.Sprintf(`GRANT CONNECT, CREATE ON DATABASE %s TO r4_app`, name+"_cust"),
		`GRANT CREATE ON SCHEMA public TO r4_app`,
		`CREATE TABLE public.documents (id int PRIMARY KEY, note text)`,
		`INSERT INTO public.documents VALUES (1, 'customer own row 1'), (2, 'customer own row 2')`,
		`COMMENT ON TABLE public.documents IS 'the customer''s own table'`)
	spec = admin.Spec{Tables: []admin.SpecTable{{Name: "documents", TenantColumn: "organization_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "r4_owner", slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("r4_app", "apppw")
	return cloud, cust, u.String(), spec
}

const customerOwn = `SELECT coalesce(obj_description('public.documents'::regclass, 'pg_class'), '') || '|' || (SELECT count(*) FROM public.documents)`

func TestMoveBackAfterAFailedMoveOrgLeavesTheCustomersTables(t *testing.T) {
	cloud, cust, app, spec := r4Stand(t, "r4a")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// a long report holds documents: move-org's switch does not get its lock, after the split
	report, err := pgx.Connect(ctx, cloud)
	if err != nil {
		t.Fatal(err)
	}
	defer report.Close(context.Background())
	tx, err := report.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "LOCK TABLE ONLY documents IN ACCESS SHARE MODE"); err != nil {
		t.Fatal(err)
	}
	moveErr := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: app, Spec: spec, Org: 1,
		Owner: "r4_owner", LockTimeout: 300 * time.Millisecond, MaxPause: 10 * time.Second, Attempts: 1, Mode: admin.ModeTrigger, FetchSize: 100, Log: log})
	_ = tx.Rollback(ctx)
	state := rev3One(t, cloud, `SELECT (SELECT count(*) FROM lokadb.orgs)::text || '|' || (SELECT relispartition::text FROM pg_class WHERE oid = to_regclass('documents__o1'))`)
	t.Logf("move-org: %v; lokadb.orgs rows | documents__o1 attached: %s", moveErr, state)
	if moveErr == nil || state != "0|true" {
		t.Fatalf("the stand did not fail move-org after its split (err %v, state %s)", moveErr, state)
	}
	backErr := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 1,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 1, Bridge: time.Second, Log: log})
	own := rev3One(t, cust, customerOwn)
	t.Logf("move-back: %v; the customer's public.documents: %s", backErr, own)
	if own != "the customer's own table|2" {
		t.Errorf("FINDING: move-back marked the customer's own public.documents (%s), want it untouched", own)
	}
	if backErr == nil || !strings.Contains(backErr.Error(), "No move-back switched it") {
		t.Errorf("want move-back to refuse: nothing of its own to finish, got %v", backErr)
	}
	// move-org again moves the organization, the customer's table stays where it is
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: app, Spec: spec, Org: 1,
		Owner: "r4_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log}); err != nil {
		t.Fatal(err)
	}
	if own := rev3One(t, cust, customerOwn); own != "the customer's own table|2" {
		t.Errorf("after move-org again the customer's public.documents: %s, want it untouched", own)
	}
}

// cancelOn cancels the context when a message is logged: the run stops right after that step.
type cancelOn struct {
	slog.Handler
	msg    string
	cancel context.CancelFunc
}

func (h cancelOn) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == h.msg {
		h.cancel()
	}
	return h.Handler.Handle(ctx, r)
}

func (h cancelOn) WithAttrs(a []slog.Attr) slog.Handler {
	return cancelOn{h.Handler.WithAttrs(a), h.msg, h.cancel}
}

func TestMoveBackInterruptedAfterItsSwitchIsFinishedWithItsSchema(t *testing.T) {
	cloud, cust, app, spec := r4Stand(t, "r4b")
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: app, Spec: spec, Org: 1,
		Owner: "r4_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: quiet}); err != nil {
		t.Fatal(err)
	}
	where := rev3One(t, cust, `SELECT to_regclass('lokadb.documents')::text`)
	// the first move-back stops right after its switch (a kill, a dropped connection)
	stopCtx, stop := context.WithCancel(ctx)
	defer stop()
	stopping := slog.New(cancelOn{slog.NewTextHandler(io.Discard, nil), "switched", stop})
	back := admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 1,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 1, Bridge: time.Second, Log: stopping}
	firstErr := admin.MoveBack(stopCtx, back)
	mid := rev3One(t, cloud, `SELECT (SELECT count(*) FROM lokadb.orgs)::text || '|' || (SELECT count(*) FROM pg_class WHERE relname = 'documents__o1' AND relispartition)`)
	t.Logf("first move-back (stopped after its switch): %v; lokadb.orgs rows | documents__o1 attached: %s", firstErr, mid)
	if firstErr == nil || mid != "0|1" {
		t.Fatalf("the first move-back did not stop between its switch and its end: %v, %s", firstErr, mid)
	}
	back.Log = quiet
	if err := admin.MoveBack(ctx, back); err != nil {
		t.Fatal(err)
	}
	copyMark := rev3One(t, cust, fmt.Sprintf(`SELECT coalesce(obj_description('%s'::regclass, 'pg_class'), '')`, where))
	capture := rev3One(t, cust, fmt.Sprintf(`SELECT count(*)::text FROM pg_trigger WHERE tgrelid = '%s'::regclass AND NOT tgisinternal`, where))
	own := rev3One(t, cust, customerOwn)
	left := "0"
	if rev3One(t, cloud, `SELECT (to_regclass('lokadb.moveback') IS NOT NULL)::text`) == "true" {
		left = rev3One(t, cloud, `SELECT count(*)::text FROM lokadb.moveback`)
	}
	t.Logf("after the second move-back: %s mark %q, triggers on it %s; the customer's public.documents %s; move-backs to finish %s", where, copyMark, capture, own, left)
	if !strings.HasPrefix(copyMark, "returned to the cloud by lokadb") {
		t.Errorf("FINDING: the copy %s is not marked as returned: %q", where, copyMark)
	}
	if capture != "0" {
		t.Errorf("FINDING: the capture triggers stay on the copy %s: %s", where, capture)
	}
	if own != "the customer's own table|2" {
		t.Errorf("FINDING: the customer's own public.documents was marked: %s", own)
	}
	if left != "0" {
		t.Errorf("the move-back's record stays after it finished: %s", left)
	}
}
