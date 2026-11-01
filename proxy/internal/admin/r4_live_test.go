package admin

// Third code review (R4), on a live Postgres (skipped without one):
//   LOKADB_LIVE_DSN=postgres://postgres:pw@host:5432 go test ./internal/admin -run LiveR4

import (
	"context"
	"strings"
	"testing"
)

// R4-5. The organization column of a child table is filled by an UPDATE of every row: the
// application's triggers (moddatetime, a version, an audit) must not fire for it, with or
// without enable -prepare.
func TestLiveR4ChildFillKeepsTriggersQuiet(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := map[bool]string{false: "plain", true: "prepared"}[prepared]
		dsn := liveDB(t, "b4_r4_fill_"+name)
		liveRun(t, dsn,
			`CREATE TABLE docs (id bigserial PRIMARY KEY, org_id bigint NOT NULL, title text)`,
			`CREATE TABLE comments (id bigserial PRIMARY KEY, doc_id bigint REFERENCES docs(id) ON DELETE CASCADE, body text,
				updated_at timestamptz NOT NULL DEFAULT '2020-01-01', version int NOT NULL DEFAULT 1)`,
			`CREATE TABLE audit (id bigserial PRIMARY KEY, tbl text, op text, row_id bigint)`,
			`CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.updated_at := now(); NEW.version := OLD.version + 1; RETURN NEW; END $$`,
			`CREATE FUNCTION audit_it() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit (tbl, op, row_id) VALUES (TG_TABLE_NAME, TG_OP, NEW.id); RETURN NULL; END $$`,
			`CREATE TRIGGER comments_touch BEFORE UPDATE ON comments FOR EACH ROW EXECUTE FUNCTION touch()`,
			`CREATE TRIGGER comments_audit AFTER UPDATE ON comments FOR EACH ROW EXECUTE FUNCTION audit_it()`,
			// one trigger the application keeps off: it stays off
			`CREATE TRIGGER comments_off AFTER UPDATE ON comments FOR EACH ROW EXECUTE FUNCTION audit_it()`,
			`ALTER TABLE comments DISABLE TRIGGER comments_off`,
			`INSERT INTO docs (org_id, title) SELECT 1 + g % 2, 't' || g FROM generate_series(1, 20) g`,
			`INSERT INTO comments (doc_id, body) SELECT 1 + g % 20, 'b' || g FROM generate_series(1, 40) g`)
		spec := Spec{Tables: []SpecTable{{Name: "docs", TenantColumn: "org_id"}, {Name: "comments", ParentTable: "docs", ParentColumn: "doc_id"}}}.Sorted()
		ctx := context.Background()
		if prepared {
			if err := Prepare(ctx, EnableOptions{DSN: dsn, Spec: spec, Owner: "app_owner", Log: quiet()}); err != nil {
				t.Fatalf("%s: enable -prepare: %v", name, err)
			}
		}
		if err := Enable(ctx, dsn, spec, "app_owner", quiet()); err != nil {
			t.Fatalf("%s: enable: %v", name, err)
		}
		if got := liveOne(t, dsn, "", `SELECT count(*) FILTER (WHERE updated_at <> '2020-01-01') || '|' || max(version) || '|' || (SELECT count(*) FROM audit) FROM comments`); got != "0|1|0" {
			t.Errorf("%s: after enable, rows with a changed updated_at | max version | audit rows: %s, want 0|1|0", name, got)
		}
		if got := liveOne(t, dsn, "", `SELECT string_agg(tgname || ':' || tgenabled::text, ',' ORDER BY tgname) FROM pg_trigger WHERE tgrelid = 'comments'::regclass AND NOT tgisinternal`); !strings.Contains(got, "comments_off:D") || !strings.Contains(got, "comments_touch:O") {
			t.Errorf("%s: triggers of comments after enable: %s, want comments_off disabled and the others on", name, got)
		}
	}
}

// R4-12. A function over the row type is made again by enable (another object): it keeps the
// old one's owner, its privileges (PUBLIC may not call it) and its comment, after enable and
// after disable.
func TestLiveR4RowtypeFunctionKeepsOwnerAndPrivileges(t *testing.T) {
	dsn := liveDB(t, "b4_r4_rowtype_fn")
	liveRun(t, dsn,
		`DO $$ BEGIN CREATE ROLE r4_other LOGIN; EXCEPTION WHEN duplicate_object THEN END $$`,
		`DO $$ BEGIN CREATE ROLE r4_reporter LOGIN; EXCEPTION WHEN duplicate_object THEN END $$`,
		`DO $$ BEGIN CREATE ROLE r4_app LOGIN; EXCEPTION WHEN duplicate_object THEN END $$`,
		`GRANT ALL ON SCHEMA public TO r4_app`,
		`CREATE TABLE docs (id bigserial PRIMARY KEY, org_id bigint NOT NULL, secret text)`,
		`INSERT INTO docs (org_id, secret) VALUES (1, 'one'), (2, 'two')`,
		`GRANT SELECT ON docs TO r4_reporter, r4_other`,
		`CREATE FUNCTION doc_secret(d docs) RETURNS text LANGUAGE sql STABLE SECURITY DEFINER SET search_path = public AS $$ SELECT d.secret $$`,
		`ALTER FUNCTION doc_secret(docs) OWNER TO r4_app`,
		`REVOKE EXECUTE ON FUNCTION doc_secret(docs) FROM PUBLIC`,
		`GRANT EXECUTE ON FUNCTION doc_secret(docs) TO r4_reporter`,
		`COMMENT ON FUNCTION doc_secret(docs) IS 'computed column'`)
	state := func() string {
		return liveOne(t, dsn, "", `SELECT pg_get_userbyid(proowner) || '|' || prosecdef || '|' || coalesce(proacl::text, '-') || '|' || coalesce(obj_description(oid, 'pg_proc'), '-')
			FROM pg_proc WHERE proname = 'doc_secret'`)
	}
	want := state()
	if !strings.HasPrefix(want, "r4_app|true|{") {
		t.Fatalf("the setup: %s", want)
	}
	spec := Spec{Tables: []SpecTable{{Name: "docs", TenantColumn: "org_id"}}}.Sorted()
	ctx := context.Background()
	if err := Enable(ctx, dsn, spec, "r4_app", quiet()); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := state(); got != want {
		t.Errorf("after enable: owner|secdef|acl|comment %s, want %s", got, want)
	}
	if got := liveOne(t, dsn, "r4_other", `SELECT doc_secret(d) FROM docs d LIMIT 1`); !strings.Contains(got, "permission denied") {
		t.Errorf("after enable, a role without EXECUTE calls doc_secret: %s", got)
	}
	if err := Disable(ctx, DisableOptions{CloudDSN: dsn, Spec: spec, Log: quiet()}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := state(); got != want {
		t.Errorf("after disable: owner|secdef|acl|comment %s, want %s", got, want)
	}
}
