package admin

// Second independent review of admin (tasks/review3/admin.md), R3-2: disable and the
// application's row security. Runs only with LOKADB_REVIEW_DSN; the helpers are in review3_test.go.

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// R3-2. disable gives the table back without the application's own row security: enable moved
// its policies to the new parent (carryOver) and switched row security off on DEFAULT; disable
// renames DEFAULT back and drops the old parent, with the policies on it. A role that saw one
// organization's rows by the application's policy sees every organization's after disable.
func TestReview3DisableDropsApplicationRLS(t *testing.T) {
	base := review3Base(t)
	dsn := review3DB(t, base, "rev3_rls")
	review3Exec(t, dsn,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_owner') THEN CREATE ROLE rev3_owner; END IF;
		 IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_reader') THEN CREATE ROLE rev3_reader; END IF; END $$`,
		`CREATE TABLE docs (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`ALTER TABLE docs ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY docs_by_org ON docs USING (org_id = nullif(current_setting('app.org', true), '')::bigint)`,
		`GRANT SELECT ON docs TO rev3_reader`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'rev3_colreader') THEN CREATE ROLE rev3_colreader; END IF; END $$`,
		`GRANT SELECT (id, body) ON docs TO rev3_colreader`,
		`CREATE TABLE docs_log (id bigint)`,
		`CREATE RULE docs_log AS ON INSERT TO docs DO ALSO INSERT INTO docs_log VALUES (NEW.id)`,
		`INSERT INTO docs VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'secret of org 2')`)
	seen := func() string {
		ctx := context.Background()
		c, err := pgx.Connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close(ctx)
		var n string
		if _, err := c.Exec(ctx, "SET app.org = '1'"); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Exec(ctx, "SET ROLE rev3_reader"); err != nil {
			t.Fatal(err)
		}
		if err := c.QueryRow(ctx, "SELECT count(*)::text FROM docs").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return "|" + n
	}
	before := seen()
	spec := Spec{Tables: []SpecTable{{Name: "docs", TenantColumn: "org_id"}}}
	ctx := context.Background()
	if err := Enable(ctx, dsn, spec, "rev3_owner", review3Log()); err != nil {
		t.Fatal(err)
	}
	if err := Disable(ctx, DisableOptions{CloudDSN: dsn, Spec: spec, Log: review3Log()}); err != nil {
		t.Fatal(err)
	}
	rls := review3One(t, dsn, `SELECT relrowsecurity::text, (SELECT count(*) FROM pg_policy WHERE polrelid = 'docs'::regclass)::text
		FROM pg_class WHERE oid = 'docs'::regclass`)
	after := seen()
	// the same table's rule and a column privilege, made before enable (R3-2, the rest of it)
	other := review3One(t, dsn, `SELECT (SELECT count(*) FROM pg_rewrite WHERE ev_class = 'docs'::regclass AND rulename = 'docs_log')::text,
		has_column_privilege('rev3_colreader', 'docs', 'body', 'SELECT')::text`)
	t.Logf("after disable: rule docs_log on docs, rev3_colreader's SELECT (body): %s (before enable: 1|true)", other)
	if other != "1|true" {
		t.Errorf("FINDING: disable also lost the application's rule and/or column privilege on docs: %s, want 1|true", other)
	}
	t.Logf("before enable: rev3_reader with app.org=1 sees %s; after disable: %s; row security, policies after disable: %s", before, after, rls)
	if rls != "true|1" || after != before {
		t.Errorf("FINDING: disable lost the application's row security (row security, policies: %s, want true|1); the reader sees %s rows instead of %s",
			rls, after[strings.LastIndex(after, "|")+1:], before[strings.LastIndex(before, "|")+1:])
	}
}

// R3-3. The online split (splitTable) deletes the organization's rows from DEFAULT under
// session_replication_role = replica, which is meant to keep the application's triggers quiet.
// A trigger the application enabled ALWAYS (or REPLICA) fires all the same: here it writes an
// audit row "deleted" for every row of the organization, none of which the application deleted.
