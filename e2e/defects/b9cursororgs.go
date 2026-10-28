package main

// Second review of routing: a FETCH string over cursors of two organizations ran under
// lokadb.org = "" (every organization), the cooperative mode included, and a cursor declared
// without a condition on the organization read every organization's rows; cursors WITH HOLD of
// two organizations outside the cooperative mode were filled at the COMMIT under "".

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "CU5", name: "cursors of two organizations: one FETCH string over both refused (cooperative mode: it read every organization's rows), cursors WITH HOLD of two refused, each FETCH on its own reads its organization", run: checkCursorsOfTwoOrgs},
	)
}

// checkCursorsOfTwoOrgs: organization 1 moved (fdw), organization 2 in the cloud, fanout reject.
func checkCursorsOfTwoOrgs(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	want2, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// run: statements in order; the row counts of the last string's results, or the first error
	run := func(sqls ...string) string {
		var out []string
		for _, sql := range sqls {
			rs, err := simple(e.ctx, c, sql)
			if err != nil {
				_, _ = simple(e.ctx, c, "ROLLBACK")
				return "error: " + firstErrLine(err)
			}
			out = out[:0]
			for _, r := range rs {
				out = append(out, fmt.Sprint(len(r.Rows)))
			}
		}
		return strings.Join(out, "+")
	}
	tenant := func(org string) string { return "SELECT set_config('lokadb.tenant', '" + org + "', true)" }

	// the cooperative mode: two cursors without a condition on the organization, one string fetching both
	declare := []string{"BEGIN", tenant("1"), "DECLARE a CURSOR FOR SELECT id FROM documents", tenant("2"), "DECLARE b CURSOR FOR SELECT id FROM documents"}
	both := run(append(declare, "FETCH ALL FROM a; FETCH ALL FROM b")...)
	_, _ = simple(e.ctx, c, "ROLLBACK")
	note(strings.Contains(both, "0A000") || strings.Contains(both, "two organizations"),
		fmt.Sprintf("cooperative: FETCH ALL FROM a; FETCH ALL FROM b over cursors of organizations 1 and 2: %s (want refused; organization 1 has 100 documents, 2 has %s)", both, want2))
	a := run(append(declare, "FETCH ALL FROM a")...)
	b := run("FETCH ALL FROM b")
	_, _ = simple(e.ctx, c, "ROLLBACK")
	note(a == "100" && b == want2, fmt.Sprintf("cooperative: each FETCH on its own: %s and %s (want 100 and %s)", a, b, want2))

	// by the statement: cursors WITH HOLD of organizations 1 and 2 in one transaction
	hold := run("BEGIN", "DECLARE ha CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 1",
		"DECLARE hb CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 2", "COMMIT")
	if !strings.HasPrefix(hold, "error") {
		_, _ = simple(e.ctx, c, "CLOSE ha")
		_, _ = simple(e.ctx, c, "CLOSE hb")
	}
	note(strings.Contains(hold, "0A000") || strings.Contains(hold, "two organizations"),
		"cursors WITH HOLD of organizations 1 and 2 in one transaction, filled at the COMMIT under one lokadb.org: "+hold+" (want refused)")
	return ok, strings.Join(notes, "; "), nil
}
