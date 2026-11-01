package main

// Block 5 (moving an organization), review 3 (R4-1): the same organization number from another
// LokaDB database (a second install, this database restored from a backup taken before the move,
// a staging copy) into a customer database that holds that organization's live tables. The mark
// on the tables names the organization and the database id; customerSchema took the same number
// for the leftovers of its own failed attempt and dropped the tables with the only copy of the rows.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV46", name: "move-org into a database that holds the same organization's tables made by another cloud database is refused before anything changes: the tables and their rows stay", run: checkSameOrgOtherCloud},
	)
}

func checkSameOrgOtherCloud(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// what another cloud's move-org left at the customer: a table of organization 1 with its own database id
	if err := execSQL(e.ctx, e.customer(),
		"CREATE TABLE documents (id bigint PRIMARY KEY, organization_id bigint NOT NULL, title text)",
		"INSERT INTO documents SELECT g, 1, 'another cloud ' || g FROM generate_series(1, 50) g",
		"COMMENT ON TABLE documents IS 'lokadb: organization 1 database deadbeefdeadbeef'"); err != nil {
		return false, "", err
	}
	out, err := e.move(1)
	rows, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title LIKE 'another cloud %'")
	part, _ := one(e.ctx, e.cloud(), "SELECT (to_regclass('documents__o1') IS NOT NULL)::text")
	refused := err != nil && strings.Contains(out, "did not make") && strings.Contains(out, "deadbeefdeadbeef")
	ok := refused && rows == "50" && part == "false"
	return ok, fmt.Sprintf("move-org: %s; the other cloud's rows at the customer: %s (want 50); partition of organization 1 made in the cloud: %s (want false)",
		errOr(err, true)+" "+lastLine(out), rows, part), nil
}
