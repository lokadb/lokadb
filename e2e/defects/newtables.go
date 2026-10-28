package main

// Review of check (finding 5, R5): after enable, check -spec runs the schema checks for the
// tables of the spec that enable has not taken yet, not only a warning that they were skipped.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X28", name: "a table added to the spec after enable (GENERATED ALWAYS id, UNIQUE email): check reports identity_always and unique_not_tenant_scoped for it, and nothing for the tables that are LokaDB already", run: checkNewTables},
	)
}

const newTablesSpecBefore = `{"tables": [{"name": "bookings", "tenant_column": "org"}]}`
const newTablesSpecAfter = `{"tables": [{"name": "bookings", "tenant_column": "org"}, {"name": "notes", "tenant_column": "org"}]}`

// X28.
func checkNewTables(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE bookings (id bigserial PRIMARY KEY, org bigint NOT NULL, room int NOT NULL)`,
		`ALTER TABLE bookings OWNER TO app`,
		`INSERT INTO bookings (org, room) VALUES (1, 1), (2, 2)`); err != nil {
		return false, "", err
	}
	e.spec = newTablesSpecBefore
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE notes (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, org bigint NOT NULL, email text UNIQUE)`,
		`ALTER TABLE notes OWNER TO app`,
		`INSERT INTO notes (org, email) VALUES (1, 'a@x'), (2, 'b@x')`); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(newTablesSpecAfter)
	idf := finding(out, "identity_always")
	uqf := finding(out, "unique_not_tenant_scoped")
	good := strings.Contains(idf, "notes") && strings.Contains(uqf, "notes") && !strings.Contains(out, "bookings_") && !reported(out, "spec_tables_not_checked")
	return good, fmt.Sprintf("check after enable (exit %d): %s; %s", code, idf, uqf), nil
}
