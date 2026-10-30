package admin_test

// disable on a managed Postgres: the tables' owner runs everything, with no superuser and no
// right to set session_replication_role (RDS, Cloud SQL, Azure, Yandex give it differently or
// not at all). enable, move-org and move-back passed there; disable refused, it set that role
// to pour the rows of the moved-back organization into DEFAULT without firing triggers. Needs
// LOKADB_REVIEW_DSN (a superuser makes the roles and the extension, as a provider does).

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

func TestDisableAsOwnerWithoutSuperuser(t *testing.T) {
	base := os.Getenv("LOKADB_REVIEW_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW_DSN not set: needs a disposable Postgres")
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cloudSU := rev3DB(t, base, "nosu_cloud")
	cust := rev3DB(t, base, "nosu_cust")
	// the provider's setup: the master user owns the database, may create roles, uses the FDW
	rev3Exec(t, cloudSU,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nosu_owner') THEN CREATE ROLE nosu_owner LOGIN PASSWORD 'ownpw' CREATEROLE; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'nosu_app') THEN CREATE ROLE nosu_app LOGIN PASSWORD 'apppw'; END IF; END $$`,
		`ALTER DATABASE nosu_cloud OWNER TO nosu_owner`,
		`ALTER SCHEMA public OWNER TO nosu_owner`,
		`CREATE EXTENSION IF NOT EXISTS postgres_fdw`,
		`GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO nosu_owner`)
	owner, _ := url.Parse(cloudSU)
	owner.User = url.UserPassword("nosu_owner", "ownpw")
	cloud := owner.String()
	if got := rev3One(t, cloud, `SELECT rolsuper::text FROM pg_roles WHERE rolname = current_user`); got != "false" {
		t.Fatalf("the owner must not be a superuser: %s", got)
	}
	// the application's tables, its own trigger on them, as the owner made them
	rev3Exec(t, cloud,
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, name text NOT NULL)`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations(id) DEFERRABLE INITIALLY DEFERRED, title text NOT NULL)`,
		`CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint NOT NULL REFERENCES documents(id) ON DELETE CASCADE, body text)`,
		`CREATE TABLE audit (op text, id bigint)`,
		`CREATE FUNCTION audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES (TG_OP, NEW.id); RETURN NULL; END $$`,
		`CREATE TRIGGER documents_audit AFTER INSERT ON documents FOR EACH ROW EXECUTE FUNCTION audit()`,
		`INSERT INTO organizations (name) VALUES ('a'), ('b')`,
		`INSERT INTO documents (organization_id, title) SELECT 1 + g % 2, 'd' || g FROM generate_series(1, 200) g`,
		`INSERT INTO document_versions (document_id, body) SELECT 1 + g % 200, 'v' || g FROM generate_series(1, 600) g`)
	before := rev3One(t, cloud, `SELECT (SELECT count(*) FROM documents) || '|' || (SELECT count(*) FROM document_versions) || '|' || (SELECT count(*) FROM audit)`)
	spec := admin.Spec{Tables: []admin.SpecTable{
		{Name: "documents", TenantColumn: "organization_id"},
		{Name: "document_versions", ParentTable: "documents", ParentColumn: "document_id"},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := admin.Enable(ctx, cloud, spec, "nosu_owner", log); err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(cust)
	u.User = url.UserPassword("nosu_app", "apppw")
	rev3Exec(t, cust, `GRANT CREATE ON DATABASE nosu_cust TO nosu_app`)
	if err := admin.MoveOrg(ctx, admin.MoveOptions{CloudDSN: cloud, CustomerDSN: cust, CustomerAppDSN: u.String(), Spec: spec, Org: 1,
		Owner: "nosu_owner", LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, FetchSize: 100, Log: log}); err != nil {
		t.Fatal(err)
	}
	if err := admin.MoveBack(ctx, admin.MoveBackOptions{CloudDSN: cloud, CustomerDSN: cust, Spec: spec, Org: 1,
		LockTimeout: 5 * time.Second, MaxPause: 10 * time.Second, Attempts: 3, Log: log}); err != nil {
		t.Fatal(err)
	}
	outside := rev3One(t, cloud, `SELECT count(*)::text FROM documents WHERE tableoid <> 'documents__cloud'::regclass`)
	if err := admin.Disable(ctx, admin.DisableOptions{CloudDSN: cloud, Spec: spec, Log: log}); err != nil {
		t.Fatalf("disable as the tables' owner without superuser: %v", err)
	}
	after := rev3One(t, cloud, `SELECT (SELECT count(*) FROM documents) || '|' || (SELECT count(*) FROM document_versions) || '|' || (SELECT count(*) FROM audit)`)
	state := rev3One(t, cloud, `SELECT (SELECT relkind::text FROM pg_class WHERE oid = 'documents'::regclass) || '|' ||
		(SELECT tgenabled::text FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND tgname = 'documents_audit') || '|' ||
		(SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('documents'::regclass, 'document_versions'::regclass) AND tgname LIKE 'lokadb%') || '|' ||
		(to_regnamespace('lokadb') IS NULL)::text`)
	t.Logf("rows moved back outside DEFAULT before disable: %s; documents|versions|audit before enable %s, after disable %s; relkind|audit trigger|lokadb triggers|schema gone: %s",
		outside, before, after, state)
	if outside == "0" {
		t.Error("the organization's rows were not outside DEFAULT before disable: the test does not reach the INSERT")
	}
	if after != before {
		t.Errorf("rows after disable %s, want %s (an audit row more: the application's trigger saw the rows poured into DEFAULT)", after, before)
	}
	if state[:4] != "r|O|" || state[len(state)-1:] == "" {
		t.Errorf("after disable: %s, want a plain table (r) whose trigger is enabled (O) and no lokadb trigger", state)
	}
	if got := rev3One(t, cloud, `SELECT count(*)::text FROM pg_trigger WHERE tgname LIKE 'lokadb%' AND tgrelid IN ('documents'::regclass, 'document_versions'::regclass)`); got != "0" {
		t.Errorf("lokadb triggers left on the tables: %s", got)
	}
}
