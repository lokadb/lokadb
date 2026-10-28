package main

// The second review of the admin (tasks/review/admin.md), A9 and A18.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "AD9", name: "move-back -abandon of an organization numbered by uuid deletes its rows of the maps of unique columns (lokadb.unique_refs) by its value, not its number", run: checkAbandonUniqueUUID},
		check{id: "AD18", name: "move-org copies a text key with a backslash and a quote when the database has standard_conforming_strings off: the tool's connections turn it on", run: checkConformingStringsOff},
	)
}

// AD9. abandon deleted the organization's rows of the unique columns' maps with
// "tenant = 1" (its number): operator does not exist: uuid = integer, and nothing changed.
func checkAbandonUniqueUUID(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE projects ADD COLUMN slug text UNIQUE",
		"UPDATE projects SET slug = 's-' || name",
		"CREATE TABLE notes (id bigserial PRIMARY KEY, account_id uuid NOT NULL, project_slug text REFERENCES projects (slug) ON DELETE CASCADE, body text)",
		"ALTER TABLE notes OWNER TO app",
		"INSERT INTO notes (account_id, project_slug, body) SELECT account_id, slug, 'n' FROM projects WHERE account_id IS NOT NULL"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [
  {"name": "projects", "tenant_column": "account_id"},
  {"name": "tasks", "parent_table": "projects", "parent_column": "project_id"},
  {"name": "notes", "tenant_column": "account_id"}
]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.lokadb("org-add", "-db", e.cloud(), "-tenant", orgA); err != nil {
		return false, "org-add failed: " + lastErr(out), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	x := &expects{ok: true}
	x.eq("the map of projects.slug holds A's projects", query(e, e.cloud(), "SELECT count(*) FROM projects__refs__slug WHERE tenant = '"+orgA+"'"), "10")
	out, err := e.run6(e.moveBackArgs(1, "-abandon", "-confirm-org", "1")...)
	x.eq("move-back -abandon", errText(err), errText(nil))
	if err != nil {
		x.notes = append(x.notes, lastLine(out))
	}
	x.eq("A's rows in the map, B's rows in it", query(e, e.cloud(), "SELECT (SELECT count(*) FROM projects__refs__slug WHERE tenant = '"+orgA+"')::text || ', ' || (SELECT count(*) FROM projects__refs__slug WHERE tenant = '"+orgB+"')"), "0, 10")
	x.eq("A's projects, lokadb.orgs", query(e, e.cloud(), "SELECT (SELECT count(*) FROM projects WHERE account_id = '"+orgA+"')::text || ', ' || (SELECT count(*) FROM lokadb.orgs)"), "0, 0")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// AD18. keyLiteral, lit() and the batches of the copy make a literal of a key that doubles its
// quote only; with standard_conforming_strings off a backslash before a quote is an escape:
// the next batch's "WHERE key > 'b\”q'" is a syntax error. The tool's connections turn the
// setting on.
func checkConformingStringsOff(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE tkeys (code varchar(30) PRIMARY KEY, org_id bigint NOT NULL, note text)",
		"ALTER TABLE tkeys OWNER TO app",
		// a, b\'q, c, d: the key at the end of the first batch of two has a backslash and a quote
		"INSERT INTO tkeys VALUES ('a', 1, 'n'), ('b\\''q', 1, 'n'), ('c', 1, 'n'), ('d', 1, 'n'), ('x', 2, 'n')"); err != nil {
		return false, "", err
	}
	// the admin's sessions in this database from now on, as a database of an old application has it
	if err := execSQL(e.ctx, e.cloud(), "ALTER ROLE postgres IN DATABASE "+e.db+" SET standard_conforming_strings = off"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "tkeys", "tenant_column": "org_id"}]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	x := &expects{ok: true}
	x.eq("the setting a new session of the admin gets", query(e, e.cloud(), "SHOW standard_conforming_strings"), "off")
	out, err := e.move(1, "-mode", "trigger", "-batch-rows", "2")
	x.eq("move-org by triggers in batches of two", errText(err), errText(nil))
	if err != nil {
		x.notes = append(x.notes, lastErr(out))
	}
	x.eq("the keys of organization 1 there", fmt.Sprint(query(e, e.customer(), "SELECT string_agg(code, ' | ' ORDER BY code) FROM tkeys")), `a | b\'q | c | d`)
	return x.ok, strings.Join(x.notes, "; "), nil
}
