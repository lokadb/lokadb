package main

// A3. The refs table (<table>__refs: id -> organization) was kept by INSERT and DELETE only.
// Postgres turns an UPDATE that moves a row to another partition into DELETE + INSERT, but
// an UPDATE inside one partition is an UPDATE: a new id, or a new organization while both
// organizations are still in DEFAULT, left the refs row stale, and the proxy routed the
// row's children by it.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "A3", name: "the refs table follows an UPDATE of the id and of the organization", run: checkRefsUpdate},
	)
}

func checkRefsUpdate(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// documents without versions: an FK onto the refs id (NO ACTION) refuses a new id that
	// leaves versions behind, as the original FK did
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (id, organization_id, user_id, title) VALUES (900001, 1, 2, 'a3-id'), (900002, 1, 2, 'a3-org'), (900003, 1, 2, 'a3-moved')"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	refs := func(want, id string) {
		got, err := one(e.ctx, e.cloud(), "SELECT coalesce((SELECT tenant::text FROM documents__refs WHERE id = "+id+"), 'missing')")
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("refs %s: %s", id, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	run := func(what, sql string) bool {
		if err := execSQL(e.ctx, e.cloud(), sql); err != nil {
			ok = false
			notes = append(notes, "WRONG "+what+" failed: "+firstErrLine(err))
			return false
		}
		return true
	}

	// a new id in DEFAULT
	if run("UPDATE id", "UPDATE documents SET id = 910001 WHERE id = 900001") {
		refs("missing", "900001")
		refs("1", "910001")
	}
	// organizations 1 and 2 both in DEFAULT: the row stays in DEFAULT
	if run("UPDATE organization", "UPDATE documents SET organization_id = 2 WHERE id = 900002") {
		refs("2", "900002")
	}
	// organization 1 moved: the UPDATE goes to its foreign partition, the trigger fires in the cloud
	if _, err := e.move(1); err != nil {
		notes = append(notes, "WRONG move-org 1 failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if run("UPDATE id of a moved organization", "UPDATE documents SET id = 910003 WHERE id = 900003") {
		refs("missing", "900003")
		refs("1", "910003")
	}
	return ok, strings.Join(notes, "; "), nil
}
