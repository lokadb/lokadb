package main

// Block 5 (moving an organization), 2d: the comparison after the switch read every row as
// text (t::text) on both sides, so a side that prints a type differently made equal data
// "differ", and the move ended with an error, the source kept.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV7", name: "the comparison after the switch does not depend on how each side prints its types", run: checkCompareText},
	)
}

// MV7. The organization's database prints intervals and bytea its own way (IntervalStyle,
// bytea_output set for the database, as a DBA may): the copy is equal, the text is not.
func checkCompareText(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents ADD COLUMN took interval DEFAULT '1 day 02:03:04', ADD COLUMN blob bytea DEFAULT '\\x00ff10'",
		"UPDATE documents SET took = id * interval '1 minute', blob = decode(md5(id::text), 'hex')"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"ALTER DATABASE "+e.db+" SET IntervalStyle = 'iso_8601'",
		"ALTER DATABASE "+e.db+" SET bytea_output = 'escape'"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	out, err := e.move(1)
	if err != nil {
		return false, "move-org failed: " + lastLine(strings.TrimSpace(out+" "+err.Error())), nil
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE took = id * interval '1 minute' AND blob = decode(md5(id::text), 'hex')")
	if err != nil {
		return false, "", err
	}
	ok := strings.Contains(out, "verified") && n == "100"
	return ok, fmt.Sprintf("move-org: %s; rows equal at the organization: %s", lastLine(strings.TrimSpace(out)), n), nil
}
