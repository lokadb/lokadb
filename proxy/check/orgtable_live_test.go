package check

// Discovery by the organizations table (orgtable.go), on a live Postgres (skipped without one):
//   LOKADB_LIVE_DSN=postgres://postgres:pw@host:5432 go test ./check -run OrgTable

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

func orgDB(t *testing.T, name string, stmts ...string) (string, *pgx.Conn) {
	t.Helper()
	base := os.Getenv("LOKADB_LIVE_DSN")
	if base == "" {
		t.Skip("LOKADB_LIVE_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	c.Close(ctx)
	dsn := base + "/" + name
	d, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close(ctx) })
	for _, s := range stmts {
		if _, err := d.Exec(ctx, s); err != nil {
			t.Fatalf("%v\n  in: %s", err, s)
		}
	}
	return dsn, d
}

func orgDiscover(t *testing.T, conn *pgx.Conn, table string) *Discovery {
	t.Helper()
	d, err := DiscoverByOrgTable(context.Background(), conn, table)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func roleOf(t *testing.T, d *Discovery, table string) TableRole {
	t.Helper()
	for _, r := range d.Tables {
		if r.Table == table {
			return r
		}
	}
	t.Fatalf("%s is not explained", table)
	return TableRole{}
}

// specLines: the spec as "table tenant_column" / "table -> parent.column", sorted.
func specLines(s admin.Spec) []string {
	var out []string
	for _, st := range s.Tables {
		if st.TenantColumn != "" {
			out = append(out, st.Name+" "+st.TenantColumn)
		} else {
			out = append(out, st.Name+" -> "+st.ParentTable+"."+st.ParentColumn)
		}
	}
	sort.Strings(out)
	return out
}

func wantSpec(t *testing.T, d *Discovery, want ...string) {
	t.Helper()
	sort.Strings(want)
	if got := specLines(d.Spec); !reflect.DeepEqual(got, want) {
		t.Errorf("spec\n got %q\nwant %q", got, want)
	}
	// every table of the database is explained once, the spec's tables as root or child
	in := map[string]bool{}
	for _, st := range d.Spec.Tables {
		in[st.Name] = true
	}
	for _, r := range d.Tables {
		if in[r.Table] != (r.Role == RoleRoot || r.Role == RoleChild) {
			t.Errorf("%s: role %s, in the spec: %v", r.Table, r.Role, in[r.Table])
		}
		if (r.Role == RoleCloud || r.Role == RoleAmbiguous) && (r.Reason == "" || r.Code == "") {
			t.Errorf("%s stays out of the spec without a reason: %+v", r.Table, r)
		}
	}
}

func wantReason(t *testing.T, r TableRole, role, code string, in ...string) {
	t.Helper()
	if r.Role != role || r.Code != code {
		t.Errorf("%s: role %s code %s, want %s %s (%s)", r.Table, r.Role, r.Code, role, code, r.Reason)
	}
	for _, s := range in {
		if !strings.Contains(r.Reason, s) {
			t.Errorf("%s: reason %q lacks %q", r.Table, r.Reason, s)
		}
	}
}

// The shape of a Laravel application whose organizations are tenants of users: users ->
// tenant_users -> tenants, tenant-owned tables, and personal_access_tokens with a
// polymorphic tokenable_id (the column the old guess by name proposed).
const laravelShape1 = `
CREATE TABLE users (id bigserial PRIMARY KEY, name text, email text, password text);
CREATE TABLE tenants (id bigserial PRIMARY KEY, name text);
CREATE TABLE tenant_users (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants, user_id bigint NOT NULL REFERENCES users,
  role text, created_at timestamp, updated_at timestamp);
CREATE TABLE documents (id bigserial PRIMARY KEY, tenant_id bigint NOT NULL REFERENCES tenants, title text);
CREATE TABLE comments (id bigserial PRIMARY KEY, document_id bigint NOT NULL REFERENCES documents, user_id bigint REFERENCES users, body text);
CREATE TABLE personal_access_tokens (id bigserial PRIMARY KEY, tokenable_type text NOT NULL, tokenable_id bigint NOT NULL, name text, token text);
CREATE TABLE sessions (id text PRIMARY KEY, user_id bigint, payload text);
INSERT INTO tenants (name) VALUES ('a'), ('b');
INSERT INTO users (name, email, password) VALUES ('u1', 'u1@x', 'p'), ('u2', 'u2@x', 'p');
INSERT INTO tenant_users (tenant_id, user_id, role) VALUES (1, 1, 'owner'), (2, 1, 'member'), (2, 2, 'owner');
INSERT INTO documents (tenant_id, title) VALUES (1, 'd1'), (2, 'd2');
INSERT INTO comments (document_id, user_id, body) VALUES (1, 1, 'c');
INSERT INTO personal_access_tokens (tokenable_type, tokenable_id, token) VALUES ('App\User', 1, 't');`

func TestOrgTableLaravelShape(t *testing.T) {
	_, conn := orgDB(t, "org_laravel", laravelShape1)
	d := orgDiscover(t, conn, "tenants")
	wantSpec(t, d, "documents tenant_id", "comments -> documents.document_id")
	if d.Key != "id" || d.KeyKind != admin.TenantInt {
		t.Errorf("key %s %s", d.Key, d.KeyKind)
	}
	wantReason(t, roleOf(t, d, "tenants"), RoleOrganization, "")
	wantReason(t, roleOf(t, d, "tenant_users"), RoleCloud, CodeLink, "routing.cloud_tables", "user")
	wantReason(t, roleOf(t, d, "users"), RoleCloud, CodeNoPath)
	wantReason(t, roleOf(t, d, "sessions"), RoleCloud, CodeNoPath)
	wantReason(t, roleOf(t, d, "personal_access_tokens"), RoleCloud, CodePolymorphic, "tokenable_id/tokenable_type")
	ch := roleOf(t, d, "comments")
	if ch.Role != RoleChild || ch.Parent != "documents" || ch.Column != "document_id" || ch.Basis != "fk" ||
		!reflect.DeepEqual(ch.Path, []string{"comments", "documents", "tenants"}) {
		t.Errorf("comments: %+v", ch)
	}
	if r := roleOf(t, d, "documents"); r.Role != RoleRoot || r.Column != "tenant_id" || !reflect.DeepEqual(r.Path, []string{"documents", "tenants"}) {
		t.Errorf("documents: %+v", r)
	}
	if rt := d.Spec.Routing; rt == nil || !reflect.DeepEqual(rt.CloudTables, []admin.CloudTable{{Table: "tenant_users", TenantColumn: "tenant_id"}}) {
		t.Errorf("routing: %+v", d.Spec.Routing)
	}

	cands, err := OrgTableCandidates(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) == 0 || cands[0].Table != "tenants" {
		t.Fatalf("candidates: %+v", cands)
	}
	if cands[0].ByFK != 3 || !strings.Contains(cands[0].Evidence, "3 tables reach it, 3 by FK") {
		t.Errorf("tenants: %+v", cands[0])
	}
	for _, oc := range cands[1:] {
		if oc.Table == "users" && oc.Excluded == "" {
			t.Errorf("users is on the many side of tenant_users and is not excluded: %+v", oc)
		}
	}
}

// The same through check: the spec is the report's, with the reasons, and a check of it runs.
func TestOrgTableRun(t *testing.T) {
	dsn, _ := orgDB(t, "org_run", laravelShape1)
	rep, err := Run(context.Background(), Options{DSN: dsn, OrgTable: "public.tenants", CountLimit: -1})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Discovery == nil || rep.Spec == nil {
		t.Fatalf("no discovery in the report: %+v", rep)
	}
	wantSpec(t, rep.Discovery, "documents tenant_id", "comments -> documents.document_id")
	if got := specLines(*rep.Spec); len(got) != 2 {
		t.Errorf("suggested_spec: %q", got)
	}
	if rep.Spec.Routing == nil || len(rep.Spec.Routing.CloudTables) != 1 {
		t.Errorf("the link table is not a cloud table in the spec: %+v", rep.Spec.Routing)
	}
	var found bool
	for _, f := range rep.Findings {
		found = found || f.Code == "discovered"
	}
	if !found {
		t.Error("no discovered finding")
	}
	var text strings.Builder
	rep.WriteText(&text)
	for _, want := range []string{"Tables by the organizations table tenants", "tenant_users", "link_table"[:0] + "membership table"} {
		if !strings.Contains(text.String(), want) {
			t.Errorf("text report lacks %q", want)
		}
	}
	// both -org-table and -tenant-column, or none: refused, with the candidates
	if _, err := Run(context.Background(), Options{DSN: dsn, OrgTable: "tenants", TenantColumn: "tenant_id"}); err == nil {
		t.Error("both flags accepted")
	}
	_, err = Run(context.Background(), Options{DSN: dsn})
	if err == nil || !strings.Contains(err.Error(), "-org-table") || !strings.Contains(err.Error(), "tenants:") {
		t.Errorf("none: %v", err)
	}
}

func TestOrgTableClassic(t *testing.T) {
	_, conn := orgDB(t, "org_classic",
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE users (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations, email text, password_digest text)`,
		`CREATE TABLE projects (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations, name text)`,
		`CREATE TABLE tasks (id bigserial PRIMARY KEY, project_id bigint NOT NULL REFERENCES projects, title text)`,
		`CREATE TABLE countries (code text PRIMARY KEY, name text)`)
	d := orgDiscover(t, conn, "organizations")
	wantSpec(t, d, "users organization_id", "projects organization_id", "tasks -> projects.project_id")
	wantReason(t, roleOf(t, d, "countries"), RoleCloud, CodeNoPath)
	if d.Spec.Routing != nil {
		t.Errorf("routing without need: %+v", d.Spec.Routing)
	}
}

func TestOrgTableColumnNamesDiffer(t *testing.T) {
	_, conn := orgDB(t, "org_names",
		`CREATE TABLE companies (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE docs (id bigserial PRIMARY KEY, owner_company_id bigint NOT NULL REFERENCES companies, title text)`,
		`CREATE TABLE invoices (id bigserial PRIMARY KEY, billing_company_id bigint REFERENCES companies, total int)`,
		`CREATE TABLE contacts (id bigserial PRIMARY KEY, company_id bigint REFERENCES companies, name text)`,
		`CREATE TABLE doc_tags (id bigserial PRIMARY KEY, doc_id bigint REFERENCES docs, tag text)`)
	d := orgDiscover(t, conn, "companies")
	wantSpec(t, d, "docs owner_company_id", "invoices billing_company_id", "contacts company_id", "doc_tags -> docs.doc_id")
}

func TestOrgTableTwoForeignKeys(t *testing.T) {
	_, conn := orgDB(t, "org_two",
		`CREATE TABLE companies (id bigserial PRIMARY KEY)`,
		`CREATE TABLE docs (id bigserial PRIMARY KEY, company_id bigint NOT NULL REFERENCES companies)`,
		`CREATE TABLE transfers (id bigserial PRIMARY KEY, from_company_id bigint NOT NULL REFERENCES companies, to_company_id bigint NOT NULL REFERENCES companies)`,
		`CREATE TABLE transfer_lines (id bigserial PRIMARY KEY, transfer_id bigint NOT NULL REFERENCES transfers)`)
	d := orgDiscover(t, conn, "companies")
	wantSpec(t, d, "docs company_id")
	tr := roleOf(t, d, "transfers")
	wantReason(t, tr, RoleAmbiguous, CodeTwoFKs, "from_company_id", "to_company_id")
	if !reflect.DeepEqual(tr.Options, []OrgOption{{"from_company_id", "companies"}, {"to_company_id", "companies"}}) {
		t.Errorf("options: %+v", tr.Options)
	}
	wantReason(t, roleOf(t, d, "transfer_lines"), RoleCloud, CodeVia, "transfers (ambiguous)")
	rep, err := Run(context.Background(), Options{DSN: dsnOf(conn), OrgTable: "companies"})
	if err != nil {
		t.Fatal(err)
	}
	var warned bool
	for _, f := range rep.Findings {
		warned = warned || f.Code == "discovery_ambiguous" && f.Severity == Warning && len(f.Examples) == 1
	}
	if !warned {
		t.Error("no discovery_ambiguous warning")
	}
}

func dsnOf(conn *pgx.Conn) string {
	cc := conn.Config()
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cc.User, cc.Password, cc.Host, cc.Port, cc.Database)
}

func TestOrgTableChain(t *testing.T) {
	_, conn := orgDB(t, "org_chain",
		`CREATE TABLE orgs (id bigserial PRIMARY KEY)`,
		`CREATE TABLE a (id bigserial PRIMARY KEY, org_id bigint NOT NULL REFERENCES orgs)`,
		`CREATE TABLE b (id bigserial PRIMARY KEY, a_id bigint NOT NULL REFERENCES a)`,
		`CREATE TABLE c (id bigserial PRIMARY KEY, b_id bigint NOT NULL REFERENCES b)`,
		`CREATE TABLE d (id bigserial PRIMARY KEY, c_id bigint NOT NULL REFERENCES c)`)
	d := orgDiscover(t, conn, "orgs")
	wantSpec(t, d, "a org_id", "b -> a.a_id", "c -> b.b_id", "d -> c.c_id")
	if r := roleOf(t, d, "d"); !reflect.DeepEqual(r.Path, []string{"d", "c", "b", "a", "orgs"}) {
		t.Errorf("path: %v", r.Path)
	}
	// a parent comes before its children
	pos := map[string]int{}
	for i, st := range d.Spec.Tables {
		pos[st.Name] = i
	}
	if !(pos["a"] < pos["b"] && pos["b"] < pos["c"] && pos["c"] < pos["d"]) {
		t.Errorf("order %v", pos)
	}
}

// No foreign keys at all (Laravel without foreign(), Rails): the names, verified on the data.
func TestOrgTableNoForeignKeys(t *testing.T) {
	_, conn := orgDB(t, "org_nofk",
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint, title text)`,
		`CREATE TABLE versions (id bigserial PRIMARY KEY, document_id bigint, body text)`,
		`CREATE TABLE version_notes (id bigserial PRIMARY KEY, version_id bigint, body text)`,
		`CREATE TABLE logs (id bigserial PRIMARY KEY, organization_id bigint, line text)`,
		`CREATE TABLE empties (id bigserial PRIMARY KEY, organization_id bigint)`,
		`CREATE TABLE tokens (id bigserial PRIMARY KEY, organization_id bigint, owner_id bigint, owner_type text)`,
		`CREATE TABLE odd (id bigserial PRIMARY KEY, organizations_id bigint)`,
		`CREATE TABLE notes (id bigserial PRIMARY KEY, document_id bigint, document_type text)`,
		`CREATE TABLE doc_files (id bigserial PRIMARY KEY, version_id bigint REFERENCES versions, name text)`,
		`INSERT INTO organizations (name) VALUES ('a'), ('b')`,
		`INSERT INTO documents (organization_id, title) VALUES (1, 'x'), (2, 'y'), (NULL, 'z')`,
		`INSERT INTO versions (document_id, body) VALUES (1, 'x'), (2, 'y')`,
		`INSERT INTO version_notes (version_id, body) VALUES (1, 'x')`,
		`INSERT INTO logs (organization_id, line) VALUES (1, 'x'), (99, 'orphan')`,
		`INSERT INTO tokens (organization_id, owner_id, owner_type) VALUES (1, 5, 'User')`,
		`INSERT INTO odd (organizations_id) VALUES (1)`,
		`INSERT INTO notes (document_id, document_type) VALUES (1, 'Document')`)
	d := orgDiscover(t, conn, "organizations")
	wantSpec(t, d, "documents organization_id", "versions -> documents.document_id", "version_notes -> versions.version_id", "tokens organization_id", "doc_files -> versions.version_id", "empties organization_id")
	wantReason(t, roleOf(t, d, "odd"), RoleCloud, CodeNoPath) // organizations_id is not the convention's organization_id
	wantReason(t, roleOf(t, d, "notes"), RoleCloud, CodePolymorphic, "document_id/document_type")
	r := roleOf(t, d, "versions")
	if r.Basis != "name" || r.Parent != "documents" || !strings.Contains(r.Reason, "verified on the data") {
		t.Errorf("versions: %+v", r)
	}
	if r := roleOf(t, d, "documents"); r.Basis != "name" {
		t.Errorf("documents: %+v", r)
	}
	wantReason(t, roleOf(t, d, "logs"), RoleCloud, CodeNoPath, "1 of 2 values are not rows of organizations")
	if r := roleOf(t, d, "empties"); r.Basis != "name" || !strings.Contains(r.Reason, "NOT verified") {
		t.Errorf("empties: %+v", r)
	}
	rt := d.Spec.Routing
	if rt == nil || !reflect.DeepEqual(rt.Relations, []admin.Relation{
		{From: "versions.document_id", To: "documents.id"}, {From: "version_notes.version_id", To: "versions.id"}}) {
		t.Errorf("relations: %+v", rt)
	}
	// tokens: organization_id is a plain column of a table with a polymorphic owner: still its organization
	// column, while owner_id/owner_type is never followed
	cands, err := OrgTableCandidates(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) == 0 || cands[0].Table != "organizations" || cands[0].ByFK != 0 || cands[0].Reach == 0 || !strings.Contains(cands[0].Evidence, "0 by FK") {
		t.Errorf("candidates: %+v", cands)
	}
}

func TestOrgTableUUID(t *testing.T) {
	_, conn := orgDB(t, "org_uuid",
		`CREATE TABLE accounts (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text)`,
		`CREATE TABLE projects (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), account_id uuid NOT NULL REFERENCES accounts)`,
		`CREATE TABLE tasks (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), project_id uuid NOT NULL REFERENCES projects)`)
	d := orgDiscover(t, conn, "accounts")
	wantSpec(t, d, "projects account_id", "tasks -> projects.project_id")
	if d.KeyKind != admin.TenantUUID {
		t.Errorf("kind %s", d.KeyKind)
	}
}

// A table that reaches the organization by two parents takes the shorter path when the data
// says both give the same organization, else it is ambiguous.
func TestOrgTableTwoParents(t *testing.T) {
	schema := []string{
		`CREATE TABLE orgs (id bigserial PRIMARY KEY)`,
		`CREATE TABLE a (id bigserial PRIMARY KEY, org_id bigint NOT NULL REFERENCES orgs)`,
		`CREATE TABLE b (id bigserial PRIMARY KEY, org_id bigint NOT NULL REFERENCES orgs)`,
		`CREATE TABLE c (id bigserial PRIMARY KEY, a_id bigint REFERENCES a, b_id bigint REFERENCES b)`,
		// deeper: e reaches by d (deeper) and by a (direct)
		`CREATE TABLE d (id bigserial PRIMARY KEY, a_id bigint REFERENCES a)`,
		`CREATE TABLE e (id bigserial PRIMARY KEY, a_id bigint REFERENCES a, d_id bigint REFERENCES d)`,
		`CREATE TABLE f (id bigserial PRIMARY KEY, c_id bigint REFERENCES c)`,
		`INSERT INTO orgs SELECT FROM generate_series(1, 2)`,
		`INSERT INTO a (org_id) VALUES (1), (2)`,
		`INSERT INTO b (org_id) VALUES (1), (2)`,
		`INSERT INTO d (a_id) VALUES (1), (2)`,
	}
	_, conn := orgDB(t, "org_agree", append(append([]string{}, schema...),
		`INSERT INTO c (a_id, b_id) VALUES (1, 1), (2, 2), (NULL, 2)`,
		`INSERT INTO e (a_id, d_id) VALUES (1, 1), (2, 2)`)...)
	d := orgDiscover(t, conn, "orgs")
	wantSpec(t, d, "a org_id", "b org_id", "c -> a.a_id", "d -> a.a_id", "e -> a.a_id", "f -> c.c_id")
	if r := roleOf(t, d, "c"); !strings.Contains(r.Reason, "b_id -> b") || !strings.Contains(r.Reason, "same organization") {
		t.Errorf("c: %+v", r)
	}
	if r := roleOf(t, d, "e"); !strings.Contains(r.Reason, "d_id -> d") {
		t.Errorf("e: %+v", r)
	}

	_, conn2 := orgDB(t, "org_disagree", append(append([]string{}, schema...),
		`INSERT INTO c (a_id, b_id) VALUES (1, 1), (1, 2)`,
		`INSERT INTO e (a_id, d_id) VALUES (1, 1), (1, 2)`)...)
	d = orgDiscover(t, conn2, "orgs")
	wantSpec(t, d, "a org_id", "b org_id", "d -> a.a_id")
	c := roleOf(t, d, "c")
	wantReason(t, c, RoleAmbiguous, CodeConflict, "1 of 2 rows")
	if !reflect.DeepEqual(c.Options, []OrgOption{{"a_id", "a"}, {"b_id", "b"}}) {
		t.Errorf("options: %+v", c.Options)
	}
	wantReason(t, roleOf(t, d, "e"), RoleAmbiguous, CodeConflict, "d_id")
	wantReason(t, roleOf(t, d, "f"), RoleAmbiguous, CodeParent, "parent c")
}

func TestOrgTableLinkWithoutID(t *testing.T) {
	_, conn := orgDB(t, "org_link",
		`CREATE TABLE users (id bigserial PRIMARY KEY, email text)`,
		`CREATE TABLE teams (id bigserial PRIMARY KEY)`,
		`CREATE TABLE team_user (team_id bigint NOT NULL REFERENCES teams, user_id bigint NOT NULL REFERENCES users, PRIMARY KEY (team_id, user_id))`,
		`CREATE TABLE posts (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams, user_id bigint REFERENCES users, body text)`)
	d := orgDiscover(t, conn, "teams")
	// posts holds data of its own: not a link table, though it points at a user too
	wantSpec(t, d, "posts team_id")
	wantReason(t, roleOf(t, d, "team_user"), RoleCloud, CodeLink, "cannot be declared as a cloud table")
	if d.Spec.Routing != nil {
		t.Errorf("routing: %+v", d.Spec.Routing)
	}
}

func TestOrgTableSchemaQualified(t *testing.T) {
	_, conn := orgDB(t, "org_schema",
		`CREATE SCHEMA app`,
		`CREATE TABLE app.orgs (id bigserial PRIMARY KEY)`,
		`CREATE TABLE app.docs (id bigserial PRIMARY KEY, org_id bigint NOT NULL REFERENCES app.orgs)`,
		`CREATE TABLE app.pages (id bigserial PRIMARY KEY, doc_id bigint NOT NULL REFERENCES app.docs)`)
	for _, name := range []string{"app.orgs", "orgs"} {
		d := orgDiscover(t, conn, name)
		wantSpec(t, d, "app.docs org_id", "app.pages -> app.docs.doc_id")
	}
}

func TestOrgTableErrors(t *testing.T) {
	_, conn := orgDB(t, "org_errors",
		`CREATE TABLE codes (code text PRIMARY KEY)`,
		`CREATE TABLE loose (a int, b int)`,
		`CREATE TABLE pair (a int, b int, PRIMARY KEY (a, b))`)
	for table, want := range map[string]string{"nope": "no table nope", "codes": "numbered by", "loose": "no primary key", "pair": "no primary key"} {
		if _, err := DiscoverByOrgTable(context.Background(), conn, table); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", table, err, want)
		}
	}
}

// What -tenant-column proposes on a schema whose tables all have a column of that name, the
// organizations table proposes too: the demo apps as they are (e2e/goapp/migrations, and the
// badapp of e2e/initdb/cloud.sql: a table with the column and no foreign key, a child with orphans).
func TestOrgTableSameAsTenantColumn(t *testing.T) {
	badapp := []string{
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, name text NOT NULL)`,
		`CREATE TABLE users (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations, email text NOT NULL UNIQUE)`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL REFERENCES organizations, slug text NOT NULL, UNIQUE (organization_id, slug), UNIQUE (organization_id, id))`,
		`CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint, content text)`,
		`CREATE TABLE attachments (id char(26) PRIMARY KEY, organization_id bigint NOT NULL,
			document_org bigint NOT NULL, document_id bigint NOT NULL, FOREIGN KEY (document_org, document_id) REFERENCES documents (organization_id, id))`,
		`INSERT INTO organizations (name) VALUES ('A'), ('B')`,
		`INSERT INTO documents (organization_id, slug) VALUES (1, 'a'), (2, 'b')`,
		`INSERT INTO attachments VALUES ('a', 1, 1, 1)`,
		`INSERT INTO document_versions (document_id, content) VALUES (1, 'ok'), (99, 'lost'), (NULL, 'lost')`,
	}
	_, bad := orgDB(t, "org_badapp", badapp...)
	var goapp []string
	for _, f := range []string{"001_init.sql", "002_document_status.sql"} {
		b, err := os.ReadFile(filepath.Join("..", "..", "e2e", "goapp", "migrations", f))
		if err != nil {
			t.Skip("e2e/goapp/migrations is not here")
		}
		goapp = append(goapp, string(b))
	}
	goapp = append(goapp, `INSERT INTO organizations (name) VALUES ('A')`, `INSERT INTO projects (organization_id, name) VALUES (1, 'p')`)
	_, goConn := orgDB(t, "org_goapp", goapp...)
	for name, conn := range map[string]*pgx.Conn{"badapp": bad, "goapp": goConn} {
		d := orgDiscover(t, conn, "organizations")
		old, err := (&checker{ctx: context.Background()}).oldDiscover(conn, "organization_id")
		if err != nil {
			t.Fatal(err)
		}
		if got, want := specLines(d.Spec), specLines(old); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: org-table\n got %q\n old %q", name, got, want)
		}
		t.Logf("%s: %q", name, specLines(d.Spec))
	}
}

func (c *checker) oldDiscover(conn *pgx.Conn, tc string) (admin.Spec, error) {
	tx, err := beginCheck(context.Background(), conn)
	if err != nil {
		return admin.Spec{}, err
	}
	defer tx.Rollback(context.Background())
	c.tx = tx
	c.rep = &Report{}
	return c.discover(tc)
}

// Candidates are ranked by the tables that reach them; a known name only breaks a tie.
func TestOrgTableRanking(t *testing.T) {
	_, conn := orgDB(t, "org_rank",
		`CREATE TABLE accounts (id bigserial PRIMARY KEY)`,
		`CREATE TABLE zones (id bigserial PRIMARY KEY)`,
		`CREATE TABLE a1 (id bigserial PRIMARY KEY, account_id bigint REFERENCES accounts)`,
		`CREATE TABLE z1 (id bigserial PRIMARY KEY, zone_id bigint REFERENCES zones)`,
		`CREATE TABLE z2 (id bigserial PRIMARY KEY, zone_id bigint REFERENCES zones)`,
		`CREATE TABLE z3 (id bigserial PRIMARY KEY, zone_id bigint REFERENCES zones)`,
		`CREATE TABLE b1 (id bigserial PRIMARY KEY, account_id bigint REFERENCES accounts)`,
		`CREATE TABLE tie_x (id bigserial PRIMARY KEY)`, `CREATE TABLE tie_y (id bigserial PRIMARY KEY, tie_x_id bigint REFERENCES tie_x)`)
	cands, err := OrgTableCandidates(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, c := range cands {
		got = append(got, fmt.Sprintf("%s:%d", c.Table, c.Reach))
	}
	// more tables first, though accounts is a known name
	if want := []string{"zones:3", "accounts:2", "tie_x:1"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// A table only the organizations point at (plans) is reached by everything that reaches the
// organizations, but it is a lookup of them, not the organizations table (playground's saas demo).
func TestOrgTableLookupOfCandidate(t *testing.T) {
	_, conn := orgDB(t, "org_lookup",
		`CREATE TABLE plans (id bigserial PRIMARY KEY, name text)`,
		`CREATE TABLE organizations (id bigserial PRIMARY KEY, plan_id bigint REFERENCES plans)`,
		`CREATE TABLE users (id bigserial PRIMARY KEY, organization_id bigint REFERENCES organizations, email text)`,
		`CREATE TABLE projects (id bigserial PRIMARY KEY, organization_id bigint REFERENCES organizations)`,
		`CREATE TABLE documents (id bigserial PRIMARY KEY, project_id bigint REFERENCES projects)`,
		`CREATE TABLE comments (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents)`)
	cands, err := OrgTableCandidates(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	if len(cands) == 0 || cands[0].Table != "organizations" {
		t.Fatalf("candidates: %+v", cands)
	}
	for _, c := range cands {
		if c.Table == "plans" && !strings.Contains(c.Excluded, "lookup of organizations") {
			t.Errorf("plans: %+v", c)
		}
		if c.Table != "plans" && c.Excluded != "" {
			t.Errorf("%s excluded: %s", c.Table, c.Excluded)
		}
	}
}

func orgConn(t *testing.T, dsn string) (string, *pgx.Conn) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return dsn, conn
}

// The schema of a real install (KeepPlain): commits point at builds by a nullable build_id and at
// repositories by a mandatory repository_id; items reach their proposal only by a nullable column.
const keepPlainShape = `
CREATE TABLE teams (id bigserial PRIMARY KEY, name text);
CREATE TABLE repositories (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams);
CREATE TABLE builds (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams, repository_id bigint REFERENCES repositories);
CREATE TABLE commits (id bigserial PRIMARY KEY, build_id bigint REFERENCES builds, repository_id bigint NOT NULL REFERENCES repositories);
CREATE TABLE proposals (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams);
CREATE TABLE proposal_items (id bigserial PRIMARY KEY, proposal_id bigint REFERENCES proposals, body text);
CREATE TABLE notes (id bigserial PRIMARY KEY, build_id bigint REFERENCES builds, commit_id bigint NOT NULL REFERENCES commits);
INSERT INTO teams (name) VALUES ('t1'), ('t2');
INSERT INTO repositories (team_id) VALUES (1), (2);
INSERT INTO builds (team_id, repository_id) VALUES (1, 1), (2, 2);
INSERT INTO commits (build_id, repository_id) VALUES (1, 1), (NULL, 1), (NULL, 2);
INSERT INTO proposals (team_id) VALUES (1);
INSERT INTO proposal_items (proposal_id) VALUES (1), (NULL), (NULL);
INSERT INTO notes (build_id, commit_id) VALUES (1, 1);`

// A NOT NULL path to the organization wins over a nullable one, even a longer one; a table with
// only a nullable path is taken with the count of rows that would not move.
func TestOrgTableParentChoice(t *testing.T) {
	_, conn := orgDB(t, "org_parent", keepPlainShape)
	d := orgDiscover(t, conn, "teams")
	wantSpec(t, d, "repositories team_id", "builds team_id", "proposals team_id",
		"commits -> repositories.repository_id", "proposal_items -> proposals.proposal_id", "notes -> commits.commit_id")
	if r := roleOf(t, d, "commits"); r.Nullable || r.RowsWithoutParent != 0 {
		t.Errorf("commits has a mandatory path: %+v", r)
	}
	if r := roleOf(t, d, "notes"); r.Nullable {
		t.Errorf("notes: %+v", r)
	}
	r := roleOf(t, d, "proposal_items")
	if !r.Nullable || r.RowsWithoutParent != 2 || !strings.Contains(r.Reason, "2 rows of proposal_items hold NULL there and would not move") {
		t.Errorf("proposal_items: %+v", r)
	}
	c := &checker{rep: &Report{}}
	c.orgDiscovered(d)
	var warn *Finding
	for i, f := range c.rep.Findings {
		if f.Code == "discovery_nullable_parent" {
			warn = &c.rep.Findings[i]
		}
	}
	if warn == nil || warn.Severity != Warning || len(warn.Examples) != 1 || !strings.Contains(warn.Examples[0], "proposal_items: 2 rows hold NULL in proposal_id") {
		t.Errorf("warning: %+v", warn)
	}
}

// With no NULL in the nullable column there is nothing to warn about.
func TestOrgTableNullableWithoutNulls(t *testing.T) {
	_, conn := orgDB(t, "org_nonull",
		`CREATE TABLE teams (id bigserial PRIMARY KEY)`,
		`CREATE TABLE proposals (id bigserial PRIMARY KEY, team_id bigint NOT NULL REFERENCES teams)`,
		`CREATE TABLE items (id bigserial PRIMARY KEY, proposal_id bigint REFERENCES proposals)`,
		`INSERT INTO teams DEFAULT VALUES`, `INSERT INTO proposals (team_id) VALUES (1)`, `INSERT INTO items (proposal_id) VALUES (1)`)
	d := orgDiscover(t, conn, "teams")
	wantSpec(t, d, "proposals team_id", "items -> proposals.proposal_id")
	if r := roleOf(t, d, "items"); !r.Nullable || r.RowsWithoutParent != 0 || strings.Contains(r.Reason, "NULL") {
		t.Errorf("items: %+v", r)
	}
	c := &checker{rep: &Report{}}
	c.orgDiscovered(d)
	for _, f := range c.rep.Findings {
		if f.Code == "discovery_nullable_parent" {
			t.Errorf("warned with no NULL: %+v", f)
		}
	}
}

// The real organizations table with a key of a type lokadb does not take (character(26)) is listed
// by -org-candidates with the reason and the column to use, and -org-table says the same.
func TestOrgTableUnsupportedKey(t *testing.T) {
	_, conn := orgDB(t, "org_charkey",
		`CREATE TABLE teams (id char(26) PRIMARY KEY, name text)`,
		`CREATE TABLE repositories (id bigserial PRIMARY KEY, team_id char(26) NOT NULL REFERENCES teams)`,
		`CREATE TABLE builds (id bigserial PRIMARY KEY, team_id char(26) NOT NULL REFERENCES teams)`,
		`CREATE TABLE github_installations (id bigserial PRIMARY KEY, account text)`,
		`CREATE TABLE rule_items (id bigserial PRIMARY KEY, installation_id bigint REFERENCES github_installations)`)
	cands, err := OrgTableCandidates(context.Background(), conn)
	if err != nil {
		t.Fatal(err)
	}
	var teams *OrgCandidate
	for i, c := range cands {
		if c.Table == "teams" {
			teams = &cands[i]
		} else if c.Unsupported != "" {
			t.Errorf("%s is marked unsupported: %+v", c.Table, c)
		}
	}
	if teams == nil {
		t.Fatalf("teams is not listed: %+v", cands)
	}
	if teams.Reach != 2 || teams.TenantColumn != "team_id" || teams.KeyKind != "" ||
		!strings.Contains(teams.Unsupported, "id is character") || !strings.Contains(teams.Evidence, "-tenant-column team_id") {
		t.Errorf("teams: %+v", teams)
	}
	if cands[0].Table != "teams" {
		t.Errorf("teams reached by 2 tables is not first: %+v", cands)
	}
	_, err = DiscoverByOrgTable(context.Background(), conn, "teams")
	if err == nil || !strings.Contains(err.Error(), "-tenant-column team_id") {
		t.Errorf("error: %v", err)
	}
	// no column to suggest: the error still names the flag
	_, conn2 := orgDB(t, "org_charkey2", `CREATE TABLE lone (id text PRIMARY KEY)`)
	if _, err := DiscoverByOrgTable(context.Background(), conn2, "lone"); err == nil || !strings.Contains(err.Error(), "-tenant-column COLUMN") {
		t.Errorf("lone: %v", err)
	}
}

// RLSRoles (the runner reads the application's role from its environment): a superuser role is a
// blocker with the way out in it, an ordinary one is not, and the other roles are left alone.
func TestAppRoleFromSettings(t *testing.T) {
	dsn, d := orgDB(t, "org_rls_roles", laravelShape1)
	ctx := context.Background()
	for _, q := range []string{"DROP ROLE IF EXISTS rls_super", "DROP ROLE IF EXISTS rls_plain", "CREATE ROLE rls_super LOGIN SUPERUSER", "CREATE ROLE rls_plain LOGIN"} {
		if _, err := d.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = d.Exec(ctx, "DROP ROLE IF EXISTS rls_super"); _, _ = d.Exec(ctx, "DROP ROLE IF EXISTS rls_plain") })
	blockers := func(role string) []Finding {
		rep, err := Run(ctx, Options{DSN: dsn, OrgTable: "public.tenants", CountLimit: -1, RLSRoles: []string{role}})
		if err != nil {
			t.Fatal(err)
		}
		var out []Finding
		for _, f := range rep.Findings {
			if f.Code == "app_role_bypasses_rls" {
				out = append(out, f)
			}
		}
		return out
	}
	got := blockers("rls_super")
	if len(got) != 1 || got[0].Severity != Blocker || !strings.Contains(got[0].Message, "rls_super") || !strings.Contains(got[0].Message, "ALTER ROLE rls_super NOSUPERUSER NOBYPASSRLS NOREPLICATION") {
		t.Errorf("superuser: %d findings, first message: %q", len(got), func() string { if len(got) > 0 { return got[0].Message }; return "" }())
	}
	if got := blockers("rls_plain"); len(got) != 0 {
		t.Errorf("ordinary role: %+v", got)
	}
}
