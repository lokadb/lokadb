package main

// A11. enable could not run again: a run whose key index failed after COMMIT (CREATE INDEX
// CONCURRENTLY interrupted, invalid index left) could not be finished, and a table could not
// be added to the spec later (both failed renaming the enabled table again).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "A11", name: "enable runs again: it finishes the key index and adds a table to the spec", run: checkEnableAgain},
	)
}

func checkEnableAgain(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE comments (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"ALTER TABLE comments OWNER TO app",
		"INSERT INTO comments (organization_id, body) SELECT 1 + g % 2, 'c' || g FROM generate_series(1, 20) g"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	expect := func(what, sql, want string) {
		got, err := one(e.ctx, e.cloud(), sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		note(got == want, fmt.Sprintf("%s: %s", what, got)+map[bool]string{true: "", false: " (want " + want + ")"}[got == want])
	}
	run := func(what, body string) bool {
		err := e.enableSpec(body)
		if err != nil {
			note(false, what+" failed: "+oneLine(err.Error()))
		}
		return err == nil
	}
	docs := `{"tables": [{"name": "documents", "tenant_column": "organization_id"}]}`
	if !run("enable documents", docs) {
		return false, strings.Join(notes, "; "), nil
	}
	// what an interrupted CREATE INDEX CONCURRENTLY leaves
	if err := execSQL(e.ctx, e.cloud(), "UPDATE pg_index SET indisvalid = false WHERE indexrelid = 'documents__key'::regclass"); err != nil {
		return false, "", err
	}
	if run("enable again", docs) {
		expect("documents__key valid", "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'documents__key'::regclass", "true")
		expect("documents rows, policies", "SELECT (SELECT count(*) FROM documents)::text || ', ' || (SELECT count(*) FROM pg_policy WHERE polrelid = 'documents'::regclass)", "200, 1")
	}
	// document_versions (a child: its organization from documents__refs) added later
	if run("enable with document_versions added", spec) {
		expect("document_versions", "SELECT (SELECT relkind::text FROM pg_class WHERE oid = 'document_versions'::regclass) || ', organization 1: ' || (SELECT count(*) FROM document_versions WHERE organization_id = 1)", "p, organization 1: 100")
		expect("its lookup", "SELECT refs_table FROM lokadb.lookups WHERE table_name = 'document_versions' AND column_name = 'document_id'", "documents__refs")
		expect("its FK", "SELECT confrelid::regclass::text FROM pg_constraint WHERE conrelid = 'document_versions__cloud'::regclass AND contype = 'f'", "documents__refs")
	}
	// once an organization is moved, a table added would keep its rows in the cloud
	if _, err := e.move(1); err != nil {
		note(false, "move-org 1 failed: "+firstErrLine(err))
		return ok, strings.Join(notes, "; "), nil
	}
	err := e.enableSpec(strings.Replace(spec, "]}", `, {"name": "comments", "tenant_column": "organization_id"}]}`, 1))
	if err == nil {
		note(false, "comments added after a move: accepted")
	} else {
		note(strings.Contains(err.Error(), "organizations are moved"), "comments added after a move: refused: "+oneLine(err.Error()))
	}
	return ok, strings.Join(notes, "; "), nil
}
