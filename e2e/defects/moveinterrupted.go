package main

// Block 5 (moving an organization), review 3 (R4-21): a move-org cut off after its switch
// committed left the organization's rows in the cloud (<table>__o<N>_source), and nothing found
// them: a second move-org said "already in its own database", move-cleanup and check were silent.
// The state is made with -retain-source (the same tables).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV49", name: "the rows a move-org cut off after its switch left in the cloud (<table>__o1_source) are compared with the organization's database and dropped by the next move-org; when they differ they are kept", run: checkInterruptedSource},
	)
}

func checkInterruptedSource(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if out, err := e.move(1, "-retain-source"); err != nil {
		return false, "moving organization 1 failed: " + lastErr(out), nil
	}
	left := func() string {
		n, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_class WHERE relname LIKE '%\_\_o1\_source' AND relkind = 'r'`)
		return n
	}
	before := left()
	// a difference: the organization has written since; kept, and told
	if err := execSQL(e.ctx, e.customer(), "INSERT INTO documents (id, organization_id, user_id, title) VALUES (900001, 1, 1, 'after the move')"); err != nil {
		return false, "", err
	}
	out1, err1 := e.move(1)
	kept := left()
	if err := execSQL(e.ctx, e.customer(), "DELETE FROM documents WHERE id = 900001"); err != nil {
		return false, "", err
	}
	out2, err2 := e.move(1)
	after := left()
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	ok := before != "0" && err1 != nil && strings.Contains(out1, "differ") && kept == before && err2 == nil && after == "0" && n != "0"
	return ok, fmt.Sprintf("source tables in the cloud after the move: %s; the organization has written: %s, kept %s (want %s); equal again: %s, left %s (want 0); organization 1 at its database: %s rows",
		before, errOr(err1, true)+" "+lastLine(out1), kept, before, errOr(err2, true)+" "+lastLine(out2), after, n), nil
}
