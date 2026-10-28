package main

// Review of check (finding 11): an EXCLUDE constraint of a LokaDB table refuses overlaps across
// all organizations today and within one organization only after the move. check says so.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X23", name: "EXCLUDE constraints: check warns (exclude_not_tenant_scoped) for one that does not compare the organization column with =, not for one that does; the constraint is global before the move and per organization after it", run: checkExclude},
	)
}

const excludeSpec = `{"tables": [{"name": "bookings", "tenant_column": "org"}]}`

// X23.
func checkExclude(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE EXTENSION IF NOT EXISTS btree_gist`,
		`CREATE TABLE bookings (id bigserial PRIMARY KEY, org bigint NOT NULL, room int NOT NULL, during int4range NOT NULL,
			CONSTRAINT bookings_global_excl EXCLUDE USING gist (room WITH =, during WITH &&),
			CONSTRAINT bookings_scoped_excl EXCLUDE USING gist (org WITH =, during WITH &&))`,
		`ALTER TABLE bookings OWNER TO app`,
		`INSERT INTO bookings (org, room, during) VALUES (1, 1, '[1,5)'), (2, 2, '[10,15)')`); err != nil {
		return false, "", err
	}
	e.spec = excludeSpec
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	exec := func(d, sql string) string {
		if err := execSQL(e.ctx, d, sql); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "accepted"
	}
	out, code := e.checkWith(excludeSpec)
	f := finding(out, "exclude_not_tenant_scoped")
	note(strings.Contains(f, "bookings_global_excl") && !strings.Contains(out, "exclude_not_tenant_scoped] bookings_scoped_excl") && code != 3 && code != 5,
		fmt.Sprintf("check (exit %d): %s", code, f))
	before := exec(e.cloud(), `INSERT INTO bookings (org, room, during) VALUES (2, 1, '[3,4)')`) // room 1, overlaps organization 1's booking
	note(strings.HasPrefix(before, "refused"), "before the move, a booking of organization 2 overlapping one of organization 1 in the same room: "+before)
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if mv, err := e.move(1); err != nil {
		note(false, "move-org failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	after := exec(e.cloud(), `INSERT INTO bookings (org, room, during) VALUES (2, 1, '[3,4)')`)
	within := exec(e.cloud(), `INSERT INTO bookings (org, room, during) VALUES (1, 1, '[2,3)')`)
	note(after == "accepted" && strings.HasPrefix(within, "refused"),
		fmt.Sprintf("after the move: organization 2 in room 1 over organization 1's booking %s (what the warning says), organization 1 over its own %s", after, within))
	return ok, strings.Join(notes, "; "), nil
}
