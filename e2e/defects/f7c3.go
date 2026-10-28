package main

// F7, C1 with thousands of organizations (block 1's analysis of block 14's orgs): the proxy
// prepares a statement with the partition key's condition per statement of the client and
// organization on the cloud connection. Up to 256 per connection stayed for good and every pair
// past them was Parsed and planned at each execution (84 % of the executions at 1000
// organizations, 98.6 % at 3000). Now up to keyed_statements, the least recently used one
// closed for a new one. Checked with keyed_statements 2: a prepared read of organizations 1,
// 2, 1, 3 leaves the statements of 1 and 3 on the cloud connection (pg_prepared_statements of
// the same session), not 2's, and nothing Parsed for one execution (lokadb_alt); each read
// answers as in the cloud.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "F7C3", name: "past keyed_statements the proxy closes the least recently used statement with the partition key's condition and keeps the recent ones prepared", run: checkKeyedEviction},
	)
}

func checkKeyedEviction(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"SET lokadb.org = '3'",
		"INSERT INTO documents (organization_id, user_id, title) SELECT 3, 1, 'd3-' || g FROM generate_series(1, 5) g",
		"RESET lokadb.org",
		"SELECT lokadb.partition_org('documents', 1)", "SELECT lokadb.partition_org('documents', 2)",
		"SELECT lokadb.partition_org('documents', 3)", "ANALYZE"); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{
		"fanout":           map[string]any{e.db: map[string]string{"mode": "reject"}},
		"keyed_statements": 2,
	})
	if err != nil {
		return false, "", err
	}
	defer stop()
	p, err := e.viaProxyPgx("f7c3")
	if err != nil {
		return false, "", err
	}
	defer p.Close(e.ctx)
	const read = "SELECT title FROM documents WHERE organization_id = $1 AND id = $2"
	var notes []string
	ok := true
	for _, org := range []string{"1", "2", "1", "3"} {
		id, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = "+org)
		want, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE id = "+id)
		got := strings.Join(rowsOnly(rowsText(e, p, read, org, id)), "; ")
		if got != want {
			ok = false
			notes = append(notes, fmt.Sprintf("WRONG read of organization %s: %q, the cloud has %q", org, got, want))
		}
	}
	stmts := rowsOnly(rowsText(e, p, "SELECT name || ' ' || regexp_replace(statement, '.*COALESCE\\(documents.organization_id, -2\\) = ', '') FROM pg_prepared_statements WHERE name LIKE 'lokadb_k%' OR name = 'lokadb_alt' ORDER BY 1"))
	of := map[string]bool{}
	alt := false
	for _, s := range stmts {
		f := strings.Fields(s)
		if len(f) == 2 {
			of[f[1]] = true
			alt = alt || f[0] == "lokadb_alt"
		}
	}
	if len(stmts) != 2 || !of["1"] || !of["3"] || of["2"] || alt {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG statements with the key's condition on the cloud connection after organizations 1, 2, 1, 3 with keyed_statements 2: %q (want those of 1 and 3, not 2's, no lokadb_alt)", stmts))
	} else {
		notes = append(notes, fmt.Sprintf("after organizations 1, 2, 1, 3: %q, organization 2's (the least recently used) closed", stmts))
	}
	return ok, strings.Join(notes, "; "), nil
}

// rowsOnly: rowsText's rows without its line of the command tag.
func rowsOnly(rows []string) []string {
	var out []string
	for _, r := range rows {
		if !strings.HasPrefix(r, "tag: ") {
			out = append(out, r)
		}
	}
	return out
}
