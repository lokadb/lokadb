package main

// Block 4, item 3: row security, rules and materialized views of the application's table
// are carried to the new parent by enable, and keep working after a move.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "X8", name: "enable carries row security, a rule and a materialized view; they work for cloud and moved organizations", run: checkCarried},
	)
}

// asRole runs sql as role (SET ROLE in a transaction of the superuser) with lokadb.org set to
// org ("" = unset), after the setup statements, and returns the first column of every row, joined.
func (e *env) asRole(role, org, sql string, setup ...string) (string, error) {
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return "", err
	}
	defer c.Close(e.ctx)
	tx, err := c.Begin(e.ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(e.ctx)
	if _, err := tx.Exec(e.ctx, "SET LOCAL ROLE "+role); err != nil {
		return "", err
	}
	if org != "" {
		if _, err := tx.Exec(e.ctx, "SELECT set_config('lokadb.org', $1, true)", org); err != nil {
			return "", err
		}
	}
	for _, s := range setup {
		if _, err := tx.Exec(e.ctx, s); err != nil {
			return "", err
		}
	}
	rows, err := tx.Query(e.ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		v, err := rows.Values()
		if err != nil {
			return "", err
		}
		out = append(out, fmt.Sprint(v[0]))
	}
	return strings.Join(out, "\n"), rows.Err()
}

// X8. documents: row security enabled, not forced, a policy that hides 'hidden%' titles from
// everyone but the owner; a rule that keeps rows titled 'keep'; a materialized view of the
// documents per organization with an index, read by reader.
func checkCarried(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"UPDATE documents SET title = 'hidden' || id WHERE id % 10 = 0",
		"UPDATE documents SET title = 'keep' WHERE id IN (1, 2)",
		"ALTER TABLE documents ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY documents_visible ON documents USING (title NOT LIKE 'hidden%')",
		"CREATE RULE documents_keep AS ON DELETE TO documents WHERE old.title = 'keep' DO INSTEAD NOTHING",
		"CREATE MATERIALIZED VIEW doc_counts AS SELECT organization_id, count(*) AS n FROM documents GROUP BY 1",
		"CREATE UNIQUE INDEX doc_counts_org ON doc_counts (organization_id)",
		"ALTER MATERIALIZED VIEW doc_counts OWNER TO app",
		"GRANT SELECT ON documents, doc_counts TO reader"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	var notes []string
	val := func(role, org, sql string, setup ...string) string {
		v, err := e.asRole(role, org, sql, setup...)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	// the owner, whom the policy never applied to, sees every row, the moved ones too
	owner := val("app", "", "SELECT count(*) FROM documents")
	// reader: hidden rows of neither organization
	reader := val("reader", "", "SELECT count(*) || ' visible, ' || count(*) FILTER (WHERE title LIKE 'hidden%') || ' hidden' FROM documents")
	readerOrg := val("reader", "1", "SELECT count(*) FROM documents")
	// the organization condition stays an index condition on DEFAULT next to the policy
	plan := val("reader", "2", "EXPLAIN (COSTS OFF) SELECT * FROM documents", "SET LOCAL enable_seqscan = off")
	keyIdx := strings.Contains(plan, "documents__key")
	// the rule keeps 'keep' rows in the cloud (id 1, organization 2) and moved (id 2, organization 1)
	delErr := execSQL(e.ctx, e.cloud(), "DELETE FROM documents WHERE id IN (1, 2)")
	kept, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE id IN (1, 2)")
	refresh := execSQL(e.ctx, e.cloud(), "REFRESH MATERIALIZED VIEW doc_counts")
	mv := val("reader", "", "SELECT string_agg(organization_id || ':' || n, ' ' ORDER BY organization_id) FROM doc_counts")
	mvIdx, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_indexes WHERE tablename = 'doc_counts' AND indexname = 'doc_counts_org'")
	notes = append(notes, fmt.Sprintf("owner sees %s; reader sees %s, of organization 1: %s; reader's plan uses documents__key: %v; DELETE of the 'keep' rows: %v, kept %s; REFRESH: %v, doc_counts %q, its index %s",
		owner, reader, readerOrg, keyIdx, delErr, kept, refresh, mv, mvIdx))
	ok := owner == "200" && reader == "180 visible, 0 hidden" && readerOrg == "80" && keyIdx &&
		delErr == nil && kept == "2" && refresh == nil && mv == "1:100 2:100" && mvIdx == "1"
	return ok, strings.Join(notes, "; "), nil
}
