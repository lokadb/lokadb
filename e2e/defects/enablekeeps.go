package main

// A11. What enable left behind or changed besides the rows: column privileges stayed on
// <table>__cloud (read past the organization policy) and never reached the parent; an
// application index left invalid by a failed CREATE INDEX CONCURRENTLY became a valid index
// of every new partition (a unique one refused the organization's own rows).

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "A11", name: "enable moves column privileges to the parent, and a writer may change a row's id", run: checkColumnPrivileges},
		check{id: "A11", name: "an invalid index of the application stays out of a new partition", run: checkInvalidIndex},
	)
}

func checkColumnPrivileges(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT SELECT (id, organization_id, title), UPDATE (title) ON documents TO reader",
		// a table-wide writer: its UPDATE of an id goes to the refs table (A3)
		"GRANT SELECT, UPDATE ON documents TO intruder"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	expect := func(what, sql, want string) {
		got, err := one(e.ctx, e.cloud(), sql)
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
	expect("reader on documents (select title, update title, select user_id)",
		"SELECT has_column_privilege('reader', 'documents', 'title', 'SELECT')::text || ' ' || has_column_privilege('reader', 'documents', 'title', 'UPDATE')::text || ' ' || has_column_privilege('reader', 'documents', 'user_id', 'SELECT')::text",
		"true true false")
	expect("reader on documents__cloud", "SELECT has_column_privilege('reader', 'documents__cloud', 'title', 'SELECT')::text", "false")
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	doc, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM documents WHERE organization_id = 2 AND id NOT IN (SELECT document_id FROM document_versions WHERE document_id IS NOT NULL)")
	if err != nil || doc == "NULL" {
		if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (id, organization_id, title) VALUES (980001, 2, 'a11')"); err != nil {
			return false, "", err
		}
		doc = "980001"
	}
	got := "updated"
	for _, s := range []string{"SET ROLE reader", "SELECT title FROM documents WHERE id = " + doc, "UPDATE documents SET title = 'a11-reader' WHERE id = " + doc,
		"SET ROLE intruder", "UPDATE documents SET id = 970001 WHERE id = " + doc, "RESET ROLE"} {
		if _, err := c.Exec(e.ctx, s); err != nil {
			got = s + ": " + firstErrLine(err)
			_, _ = c.Exec(e.ctx, "RESET ROLE")
			break
		}
	}
	notes = append(notes, "as reader, then a writer of ids: "+got)
	if got != "updated" {
		ok = false
		notes[len(notes)-1] = "WRONG " + notes[len(notes)-1]
	}
	expect("refs follows the new id", "SELECT count(*) FROM documents__refs WHERE id = 970001", "1")
	return ok, strings.Join(notes, "; "), nil
}

func checkInvalidIndex(e *env) (bool, string, error) {
	// a failed CREATE UNIQUE INDEX CONCURRENTLY leaves the index invalid: not used, not enforced
	if err := execSQL(e.ctx, e.cloud(), "UPDATE documents SET title = 'dup' WHERE id IN (SELECT id FROM documents WHERE organization_id = 1 ORDER BY id LIMIT 2)"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE UNIQUE INDEX CONCURRENTLY documents_title ON documents (title)"); err == nil {
		return false, "the unique index was built in spite of duplicates", nil
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// move-org's first step without session_replication_role: the organization's own partition
	part, err := one(e.ctx, e.cloud(), "SELECT lokadb.partition_org('documents', 1)")
	if err != nil {
		return false, "partition_org failed: " + firstErrLine(err), nil
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE i.indrelid = '"+part+"'::regclass AND pg_get_indexdef(c.oid) LIKE '%(title)%'")
	if err != nil {
		return false, "", err
	}
	rows, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM "+part)
	if err != nil {
		return false, "", err
	}
	return n == "0" && rows == "100", fmt.Sprintf("%s: %s rows, %s indexes on title", part, rows, n), nil
}
