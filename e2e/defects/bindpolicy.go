package main

// A7. Rows an organization's database returns are trusted only for its own organization.
// Each move added a restrictive policy lokadb_bind_org<N>: with 300 organizations moved every
// statement on the table planned 300 policies (seconds), and the organization condition,
// now behind them, could no longer use the index on DEFAULT. One condition in lokadb_tenant
// covers every organization.

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "A7", name: "one policy per table binds every moved organization's rows to it", run: checkBindPolicy},
		check{id: "F7C2", name: "the policy reads the foreign partitions from the catalog's foreign tables, not every partition: an older policy is made again, a moved organization's rows are still bound to it, move-back leaves no foreign partition", run: checkForeignParts},
	)
}

func checkBindPolicy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// One organization: the stand gives every organization the same customer database, and
	// organization 2's partition would read the row below as its own, rightly.
	if _, err := e.move(1); err != nil {
		return false, "move-org 1 failed: " + firstErrLine(err), nil
	}
	// organization 1's database claims a row of organization 2: the customer controls it, and
	// drops the CHECK move-org left there
	if err := execSQL(e.ctx, e.customer(),
		"ALTER TABLE documents DROP CONSTRAINT IF EXISTS lokadb_owner",
		"INSERT INTO documents (id, organization_id, title) VALUES (990001, 2, 'a7-forged'), (990002, 1, 'a7-own')"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	expect := func(what, d, sql, want string) {
		got, err := one(e.ctx, d, sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("%s: %s", what, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	app := dsn(e.cloudHost, e.db, "app")
	expect("policies on documents", e.cloud(), "SELECT count(*) FROM pg_policy WHERE polrelid = 'documents'::regclass", "1")
	expect("policies on document_versions", e.cloud(), "SELECT count(*) FROM pg_policy WHERE polrelid = 'document_versions'::regclass", "1")
	expect("the forged row, all organizations", app, "SELECT count(*) FROM documents WHERE title = 'a7-forged'", "0")
	expect("organization 1's own new row", app, "SELECT count(*) FROM documents WHERE title = 'a7-own'", "1")
	expect("organization 2's rows", app, "SELECT count(*) FROM documents WHERE organization_id = 2", "100")
	return ok, strings.Join(notes, "; "), nil
}

// F7 of the third performance review, C2: the policy's InitPlan read every partition
// (local_parts: 5 ms a statement at a thousand organizations, 15 at three thousand). It reads
// the foreign tables now (lokadb.foreign_parts: pg_foreign_table, then pg_inherits by inhrelid),
// as many as the organizations moved. No list of lokadb's own: the catalog. A database enabled
// before gets the policy again from baseSQL at its next lokadb command (here move-org); the rows
// a moved organization's database returns stay bound to it (A7's forged row); after move-back
// no partition is foreign and the organization's rows pass as local ones.
func checkForeignParts(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// the policy as an older lokadb made it (local_parts), as on a database enabled before
	if err := execSQL(e.ctx, e.cloud(),
		"DROP POLICY lokadb_tenant ON documents",
		`CREATE POLICY lokadb_tenant ON documents AS PERMISSIVE USING (coalesce(organization_id, -2) >= coalesce(lokadb.org(), -9223372036854775808)
		  AND coalesce(organization_id, -2) <= coalesce(lokadb.org(), 9223372036854775807)
		  AND (tableoid = ANY ((SELECT lokadb.local_parts('documents'::regclass))::oid[])
		    OR ((SELECT lokadb.bound_orgs('documents'::regclass)) ->> tableoid::text)::bigint = coalesce(organization_id, -2)))`); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "move-org 1 failed: " + firstErrLine(err), nil
	}
	if err := execSQL(e.ctx, e.customer(),
		"ALTER TABLE documents DROP CONSTRAINT IF EXISTS lokadb_owner",
		"INSERT INTO documents (id, organization_id, title) VALUES (990101, 2, 'f7-forged'), (990102, 1, 'f7-own')"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	expect := func(what, d, sql, want string) {
		got, err := one(e.ctx, d, sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("%s: %s", what, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	app := dsn(e.cloudHost, e.db, "app")
	policy := "SELECT (pg_get_expr(polqual, polrelid) LIKE '%foreign_parts%' AND pg_get_expr(polqual, polrelid) NOT LIKE '%local_parts%')::text FROM pg_policy WHERE polname = 'lokadb_tenant' AND polrelid = "
	expect("documents' policy made again by move-org's baseSQL", e.cloud(), policy+"'documents'::regclass", "true")
	expect("document_versions' policy", e.cloud(), policy+"'document_versions'::regclass", "true")
	expect("foreign partitions of documents", e.cloud(), "SELECT array_length(lokadb.foreign_parts('documents'::regclass), 1)::text", "1")
	expect("the forged row, all organizations", app, "SELECT count(*) FROM documents WHERE title = 'f7-forged'", "0")
	expect("organization 1's own new row", app, "SELECT count(*) FROM documents WHERE title = 'f7-own'", "1")
	expect("organization 2's rows (local)", app, "SELECT count(*) FROM documents WHERE organization_id = 2", "100")
	if err := execSQL(e.ctx, e.customer(), "DELETE FROM documents WHERE id IN (990101, 990102)"); err != nil {
		return false, "", err
	}
	if out, err := e.lokadb(e.moveBackArgs(1)...); err != nil {
		return false, "move-back 1 failed: " + lastErr(out), nil
	}
	// the planner's view of the policy (the feed of block 14's paths: 5 rows expected of 20 000,
	// the rest read and sorted): the estimate of one organization's rows is near the real count
	// (100 of 200), and DEFAULT holds the statistics of the organization column with the key
	expect("extended statistics on documents__cloud", e.cloud(), "SELECT count(*) FROM pg_statistic_ext WHERE stxrelid = 'documents__cloud'::regclass", "1")
	est := "error"
	if c, err := pgx.Connect(e.ctx, app); err == nil {
		if _, err := c.Exec(e.ctx, "SELECT set_config('lokadb.org', '2', false)"); err == nil {
			var plan string
			if err := c.QueryRow(e.ctx, "EXPLAIN SELECT id FROM documents WHERE organization_id = 2").Scan(&plan); err == nil {
				est = plan
				if m := regexp.MustCompile(`rows=([0-9]+)`).FindStringSubmatch(plan); m != nil {
					est = m[1]
					if n, _ := strconv.Atoi(m[1]); n < 30 {
						ok = false
						notes = append(notes, "WRONG the policy hides the organization's rows from the planner: "+plan)
					}
				}
			}
		}
		c.Close(e.ctx)
	}
	notes = append(notes, "estimated rows of organization 2's 100: "+est)
	expect("foreign partitions after move-back", e.cloud(), "SELECT coalesce(array_length(lokadb.foreign_parts('documents'::regclass), 1), 0)::text", "0")
	expect("organization 1's rows after move-back (local again)", app, "SELECT count(*) FROM documents WHERE organization_id = 1", "100")
	return ok, strings.Join(notes, "; "), nil
}
