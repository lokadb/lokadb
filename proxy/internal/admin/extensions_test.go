package admin_test

// The extensions the moved tables use (extensions.go): installed in the organization's database
// at the cloud's version by a role with CREATE on the database only (a trusted extension), and a
// refusal before anything changes, with the command for the administrator, when the server lacks
// the package or the version or the role may not install it. Needs LOKADB_REVIEW_DSN (a
// disposable server, superuser), skipped without it.

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

type extStand struct {
	t           *testing.T
	cloud, cust string
	app         string // the DSN of the organization's role: CONNECT and CREATE on the database only
	spec        admin.Spec
}

func newExtStand(t *testing.T, name, cloudDDL string) *extStand {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	s := &extStand{t: t}
	s.cloud = rev3DB(t, base, "ext_cloud_"+name)
	s.cust = rev3DB(t, base, "ext_cust_"+name)
	rev3Exec(t, base,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ext_owner') THEN CREATE ROLE ext_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'ext_app') THEN CREATE ROLE ext_app LOGIN PASSWORD 'extpw'; END IF; END $$`)
	rev3Exec(t, s.cust,
		`REVOKE ALL ON DATABASE ext_cust_`+name+` FROM PUBLIC`,
		`REVOKE CREATE ON SCHEMA public FROM PUBLIC`,
		`GRANT CONNECT, CREATE ON DATABASE ext_cust_`+name+` TO ext_app`)
	rev3Exec(t, s.cloud, strings.Split(cloudDDL, ";;")...)
	u, _ := url.Parse(s.cust)
	u.User = url.UserPassword("ext_app", "extpw")
	s.app = u.String()
	s.spec = admin.Spec{Tables: []admin.SpecTable{{Name: "docs7", TenantColumn: "org_id"}}}
	return s
}

func (s *extStand) enable(log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, s.cloud, s.spec, "ext_owner", log); err != nil {
		s.t.Fatal(err)
	}
}

func (s *extStand) move(log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	return admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: s.cloud, CustomerDSN: s.app, CustomerAppDSN: s.app, Spec: s.spec, Org: 7,
		Owner: "ext_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Mode: admin.ModeTrigger, FetchSize: 100, Log: log})
}

const docs7DDL = `CREATE TABLE docs7 (id bigint PRIMARY KEY, org_id bigint NOT NULL, name citext, v earth);;
	INSERT INTO docs7 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'Name' || g, NULL FROM generate_series(1, 8) g`

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

// a trusted extension (citext) is installed by a role with CREATE on the database, at the
// version the cloud runs, and move-back leaves it
func TestExtensionInstalledAtTheCloudsVersion(t *testing.T) {
	s := newExtStand(t, "ok", `CREATE EXTENSION citext VERSION '1.5';;
		CREATE TABLE docs7 (id bigint PRIMARY KEY, org_id bigint NOT NULL, name citext);;
		INSERT INTO docs7 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'Name' || g FROM generate_series(1, 8) g`)
	s.enable(discard)
	if err := s.move(discard); err != nil {
		t.Fatal(err)
	}
	got := rev3One(t, s.cust, `SELECT e.extversion || '|' || n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'citext'`)
	t.Logf("citext at the organization: version|schema = %s (the cloud runs 1.5)", got)
	if !strings.HasPrefix(got, "1.5|") {
		t.Fatalf("the extension is not at the cloud's version: %s", got)
	}
	if n := rev3One(t, s.cloud, `SELECT count(*) FROM docs7 WHERE name = 'name3'`); n != "1" {
		t.Fatalf("a citext lookup through the cloud: %s rows, want 1", n)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: s.cloud, CustomerDSN: s.app, Spec: s.spec, Org: 7,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: discard}); err != nil {
		t.Fatal(err)
	}
	if left := rev3One(t, s.cust, `SELECT count(*) FROM pg_extension WHERE extname = 'citext'`); left != "1" {
		t.Fatalf("move-back removed the extension from the organization's database")
	}
}

// Each refusal comes before the first change: the organization stays in DEFAULT, nothing is
// made in the organization's database, and the message says what its administrator runs.
func TestExtensionRefusals(t *testing.T) {
	for _, tc := range []struct {
		name, hack string
		want       []string
	}{
		{"privilege", ``, []string{"extension earthdistance cannot be installed", "as a superuser", "CREATE EXTENSION IF NOT EXISTS", "run move-org again"}},
		{"version", `UPDATE pg_extension SET extversion = '9.9' WHERE extname = 'citext'`, []string{"extension citext has no such version", "versions 1.4, 1.5, 1.6", "9.9", "CREATE EXTENSION IF NOT EXISTS"}},
		{"package", `UPDATE pg_extension SET extname = 'nopackage_ext' WHERE extname = 'citext'`, []string{"extension nopackage_ext is not available", "Install its package", "CREATE EXTENSION IF NOT EXISTS"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ddl := `CREATE EXTENSION citext;; CREATE EXTENSION cube;; CREATE EXTENSION earthdistance;; ` + docs7DDL
			if tc.name == "privilege" {
				// citext and cube are trusted: only earthdistance (superuser) is refused
			} else {
				ddl = `CREATE EXTENSION citext;; CREATE TABLE docs7 (id bigint PRIMARY KEY, org_id bigint NOT NULL, name citext);;
					INSERT INTO docs7 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'n' || g FROM generate_series(1, 8) g;; ` + tc.hack
			}
			s := newExtStand(t, tc.name, ddl)
			s.enable(discard)
			err := s.move(discard)
			t.Logf("move-org: %v", err)
			if err == nil {
				t.Fatal("move-org went through")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("the message lacks %q", w)
				}
			}
			if !strings.Contains(err.Error(), "nothing was changed") {
				t.Errorf("the message does not say that nothing was changed")
			}
			if got := rev3One(t, s.cloud, `SELECT coalesce(to_regclass('docs7__o7')::text, 'none')`); got != "none" {
				t.Errorf("the organization was split out of DEFAULT before the refusal: %s", got)
			}
			if got := rev3One(t, s.cust, `SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)`); got != "0" {
				t.Errorf("tables were made in the organization's database before the refusal: %s", got)
			}
			if got := rev3One(t, s.cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "0" {
				t.Errorf("the organization moved: %s", got)
			}
		})
	}
}

// an extension the organization's database has in another version than the cloud is left as it
// is: the move goes through (check warns, customer_extension_version, it does not block)
func TestExtensionOtherVersionIsLeftAlone(t *testing.T) {
	s := newExtStand(t, "other", `CREATE EXTENSION citext VERSION '1.5';;
		CREATE TABLE docs7 (id bigint PRIMARY KEY, org_id bigint NOT NULL, name citext);;
		INSERT INTO docs7 SELECT g, CASE WHEN g <= 5 THEN 7 ELSE 8 END, 'Name' || g FROM generate_series(1, 8) g`)
	rev3Exec(t, s.cust, `CREATE EXTENSION citext VERSION '1.4'`)
	s.enable(discard)
	if err := s.move(discard); err != nil {
		t.Fatalf("move-org with citext 1.4 there and 1.5 in the cloud: %v", err)
	}
	if got := rev3One(t, s.cust, `SELECT extversion FROM pg_extension WHERE extname = 'citext'`); got != "1.4" {
		t.Fatalf("the organization's extension changed: %s", got)
	}
	if got := rev3One(t, s.cloud, `SELECT count(*) FROM lokadb.orgs WHERE org = 7`); got != "1" {
		t.Fatalf("organization 7 not moved: %s", got)
	}
}
