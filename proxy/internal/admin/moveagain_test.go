package admin_test

// enable -> move-org -> move-back -> move-org of the same organization into the same database.
// move-back leaves the organization's tables there, marked "returned to the cloud by lokadb ..."
// (they are not dropped by it), and the second move-org refused them as tables that lokadb did not
// create (blocker customer_privileges: self-serve session, 27.10). It now moves its own stale
// copy aside to a schema of its own and says how to drop it, or drops it with -drop-returned (no
// CASCADE); a view the customer built on it stops the move either way, with the command; a table
// of the customer's is still left alone. Needs LOKADB_REVIEW_DSN, skipped without it.

import (
	"bytes"
	"context"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

type lockedBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *lockedBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func TestMoveAgainAfterMoveBack(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	var out lockedBuf
	log := slog.New(slog.NewTextHandler(&out, nil))
	cloud := rev3DB(t, base, "again_cloud")
	cust := rev3DB(t, base, "again_cust")
	rev3Exec(t, cloud,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'again_owner') THEN CREATE ROLE again_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'again_app') THEN CREATE ROLE again_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`CREATE TABLE docs5 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs5 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs5", TenantColumn: "org_id"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "again_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("again_app", "apppw")
	moveWith := func(drop bool) error {
		return admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 7,
			Owner: "again_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100,
			DropReturned: drop, Log: log})
	}
	move := func() error { return moveWith(false) }
	back := func() {
		t.Helper()
		if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 7,
			LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
			t.Fatal(err)
		}
	}
	if err := move(); err != nil {
		t.Fatal(err)
	}
	back()
	mark := rev3One(t, cust, `SELECT coalesce(obj_description(c.oid, 'pg_class'), '') FROM pg_class c WHERE c.oid = to_regclass('lokadb.docs5')`)
	t.Logf("after move-back, the copy in the organization's database: %q", mark)
	if !strings.HasPrefix(mark, "returned to the cloud by lokadb at ") {
		t.Fatalf("move-back did not mark the copy as returned: %q", mark)
	}

	// 1. a view of the customer's on the copy: the move stops, naming the command, whatever the flag
	rev3Exec(t, cust, `CREATE VIEW customer_report AS SELECT id FROM lokadb.docs5`)
	for _, drop := range []bool{false, true} {
		err := moveWith(drop)
		t.Logf("with a view of the customer's on the copy (-drop-returned %v): %v", drop, err)
		if err == nil || !strings.Contains(err.Error(), "DROP TABLE") || !strings.Contains(err.Error(), "CASCADE") || !strings.Contains(err.Error(), "customer_report") {
			t.Fatalf("the move should stop on the customer's view, naming the command: %v", err)
		}
		if got := rev3One(t, cust, `SELECT count(*) FROM lokadb.docs5`); got != "5" {
			t.Fatalf("the copy changed: %s rows", got)
		}
		if got := rev3One(t, cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "0" {
			t.Fatalf("the organization moved all the same: %s", got)
		}
	}
	rev3Exec(t, cust, `DROP VIEW customer_report`)

	// 2. by default the second move-org moves the old copy aside and moves again, saying so
	if err := move(); err != nil {
		t.Fatalf("the second move-org: %v", err)
	}
	aside := rev3One(t, cust, `SELECT coalesce(string_agg(nspname, ','), '') FROM pg_namespace WHERE nspname LIKE 'lokadb\_returned\_7\_%'`)
	oldRows := rev3One(t, cust, `SELECT count(*)::text || '|' || coalesce(max(obj_description(c.oid, 'pg_class')), '') FROM pg_class c
		WHERE c.relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'lokadb\_returned\_7\_%') AND c.relname = 'docs5'`)
	rows := rev3One(t, cust, `SELECT count(*) || '|' || coalesce(max(obj_description('lokadb.docs5'::regclass, 'pg_class')), '') FROM lokadb.docs5`)
	t.Logf("after the second move-org: the old copy in %s (%s); the moved table: %s", aside, oldRows, rows)
	if aside == "" || strings.Contains(aside, ",") || !strings.HasPrefix(oldRows, "1|returned to the cloud by lokadb") {
		t.Fatalf("the old copy is not moved aside to a schema of its own: %q, %q", aside, oldRows)
	}
	if got := rev3One(t, cust, `SELECT count(*) FROM "`+aside+`".docs5`); got != "5" {
		t.Fatalf("the old copy lost rows: %s", got)
	}
	if !strings.HasPrefix(rows, "5|lokadb:") {
		t.Fatalf("the moved table is not lokadb's fresh one: %s", rows)
	}
	if !strings.Contains(out.String(), "DROP SCHEMA") || !strings.Contains(out.String(), aside) || !strings.Contains(out.String(), "moved aside") {
		t.Fatalf("the move did not say where the old copy went and how to drop it: %s", out.String())
	}
	if got := rev3One(t, cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "1" {
		t.Fatalf("organization 7 not moved: %s", got)
	}
	back()

	// 2b. with -drop-returned it drops the old copy: nothing is left aside by this move
	rev3Exec(t, cust, `DROP SCHEMA `+aside+` CASCADE`)
	if err := moveWith(true); err != nil {
		t.Fatalf("the third move-org with -drop-returned: %v", err)
	}
	if left := rev3One(t, cust, `SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'lokadb\_returned\_7\_%'`); left != "0" {
		t.Fatalf("-drop-returned left a schema aside: %s", left)
	}
	if !strings.Contains(out.String(), "dropped the copy an earlier move-back left") {
		t.Fatalf("the drop is not in the log")
	}
	if got := rev3One(t, cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "1" {
		t.Fatalf("organization 7 not moved: %s", got)
	}
	back()

	// 3. a table of the customer's of that name is still left alone
	rev3Exec(t, cust, `DROP TABLE lokadb.docs5`, `CREATE TABLE lokadb.docs5 (x int)`)
	err := move()
	t.Logf("with a table of the customer's of that name: %v", err)
	if err == nil || !strings.Contains(err.Error(), "docs5") {
		t.Fatalf("the customer's table should stop the move: %v", err)
	}
	if got := rev3One(t, cust, `SELECT count(*) FROM pg_class WHERE oid = 'lokadb.docs5'::regclass AND relkind = 'r'`); got != "1" {
		t.Fatal("the customer's table is gone")
	}
}

// The same cycle as the self-serve flow runs it: -customer = -customer-app = a role with CONNECT
// and CREATE on the database only, which owns the tables it made (no superuser, no CREATE on
// public). The copy move-back left is moved aside (schema made by that role, tables moved by it)
// or, with -drop-returned, dropped by it.
func TestMoveAgainAsSelfServeRole(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	for _, drop := range []bool{false, true} {
		name := "aside"
		if drop {
			name = "drop"
		}
		t.Run(name, func(t *testing.T) {
			var out lockedBuf
			log := slog.New(slog.NewTextHandler(&out, nil))
			cloud := rev3DB(t, base, "ss_cloud_"+name)
			cust := rev3DB(t, base, "ss_cust_"+name)
			rev3Exec(t, cloud,
				`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ss_owner') THEN CREATE ROLE ss_owner; END IF; END $$`,
				`CREATE TABLE docs6 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
				`INSERT INTO docs6 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'row ' || g FROM generate_series(1, 8) g`)
			rev3Exec(t, base,
				`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ss_app') THEN CREATE ROLE ss_app LOGIN PASSWORD 'sspw'; END IF; END $$`)
			rev3Exec(t, cust,
				`REVOKE ALL ON DATABASE ss_cust_`+name+` FROM PUBLIC`,
				`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
				`GRANT CONNECT, CREATE ON DATABASE ss_cust_`+name+` TO ss_app`)
			spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs6", TenantColumn: "org_id"}}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := admin.Enable(ctx, cloud, spec, "ss_owner", log); err != nil {
				t.Fatal(err)
			}
			u, _ := url.Parse(cust)
			u.User = url.UserPassword("ss_app", "sspw")
			move := func(drop bool) error {
				return admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: u.String(), CustomerAppDSN: u.String(), Spec: spec, Org: 7,
					Owner: "ss_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100,
					DropReturned: drop, Log: log})
			}
			if err := move(false); err != nil {
				t.Fatalf("the first move-org: %v\n%s", err, out.String())
			}
			if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: u.String(), Spec: spec, Org: 7,
				LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
				t.Fatalf("move-back: %v", err)
			}
			where := rev3One(t, cust, `SELECT c.relnamespace::regnamespace::text || '.' || c.relname FROM pg_class c WHERE c.relname = 'docs6' AND c.relkind = 'r'`)
			owner := rev3One(t, cust, `SELECT pg_get_userbyid(c.relowner) FROM pg_class c WHERE c.relname = 'docs6' AND c.relkind = 'r'`)
			t.Logf("after move-back: the copy is %s, owned by %s", where, owner)
			if err := move(drop); err != nil {
				t.Fatalf("the second move-org (-drop-returned %v) as a role with CREATE on the database only: %v\n%s", drop, err, out.String())
			}
			left := rev3One(t, cust, `SELECT coalesce(string_agg(nspname, ','), '') FROM pg_namespace WHERE nspname LIKE 'lokadb\_returned\_7\_%'`)
			fresh := rev3One(t, cust, `SELECT count(*) || '|' || coalesce(max(obj_description(c.oid, 'pg_class')), '') FROM pg_class c WHERE c.relname = 'docs6' AND c.relkind = 'r'
				AND c.relnamespace NOT IN (SELECT oid FROM pg_namespace WHERE nspname LIKE 'lokadb\_returned\_%')`)
			t.Logf("after the second move-org (-drop-returned %v): old copy aside in %q; the moved table: %s", drop, left, fresh)
			if !strings.HasPrefix(fresh, "1|lokadb:") {
				t.Fatalf("the moved table is not lokadb's fresh one: %s", fresh)
			}
			if drop && left != "" || !drop && (left == "" || strings.Contains(left, ",")) {
				t.Fatalf("schemas aside: %q", left)
			}
			if got := rev3One(t, cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "1" {
				t.Fatalf("organization 7 not moved: %s", got)
			}
		})
	}
}
