package main

// Block 5 (moving an organization): the CHECK constraints of a table go onto the foreign table
// the switch attaches with the parent's own expressions. They were written out by
// pg_get_constraintdef and parsed again: a Laravel enum ($table->enum(): varchar(255) with
// CHECK (col IN ('a', 'b')), printed as (col)::text = ANY ((ARRAY['a'::character varying,
// ...])::text[])) came back with other casts, and ATTACH, which compares the stored
// expressions, refused the switch: "child table ... has different definition for check
// constraint" (the first move of a real organization, a Postgres 16 cloud and an 18
// organization's database).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV32", name: "CHECK constraints of a Laravel enum (varchar and text) and a domain's: the organization moves and comes back, the constraints hold on both sides", run: checkEnumChecksMove},
		check{cloudHost: "cloud16", id: "MV33", name: "the same from a Postgres 16 cloud to an 18 organization's database", run: checkEnumChecksMove},
	)
}

// MV32, MV33. builds has the CHECKs Laravel's migrations make: $table->enum() on varchar(255),
// an IN list on text, a CHECK of two columns, and a column of a domain with a CHECK of its own.
func checkEnumChecksMove(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE DOMAIN build_size AS varchar(10) CHECK (VALUE IN ('s', 'm', 'l'))`,
		`CREATE TABLE builds (id bigserial PRIMARY KEY, team_id bigint NOT NULL,
			"playbook_status" varchar(255) check ("playbook_status" in ('pending', 'running', 'done')),
			kind text NOT NULL DEFAULT 'web' check (kind in ('web', 'cli')),
			size build_size, tries int NOT NULL DEFAULT 0, max_tries int NOT NULL DEFAULT 3,
			CONSTRAINT builds_tries_check CHECK (tries <= max_tries))`,
		"ALTER TABLE builds OWNER TO app",
		`INSERT INTO builds (team_id, playbook_status, kind, size)
			SELECT 1 + g % 2, (ARRAY['pending', 'running', 'done'])[1 + g % 3], (ARRAY['web', 'cli'])[1 + g % 2], (ARRAY['s', 'm', 'l'])[1 + g % 3]
			FROM generate_series(1, 40) g`); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "builds", "tenant_column": "team_id"}]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	ver, _ := one(e.ctx, e.cloud(), "SELECT current_setting('server_version_num')::int / 10000")
	if out, err := e.move(1); err != nil {
		return false, fmt.Sprintf("move-org from Postgres %s failed: %s", ver, lastErr(out)), nil
	}
	moved, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM builds")
	held := refusals(e, e.cloud())
	if out, err := e.lokadb(e.moveBackArgs(1)...); err != nil {
		return false, fmt.Sprintf("moved (%s rows at the organization), move-back failed: %s", moved, lastErr(out)), nil
	}
	back, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM builds WHERE team_id = 1")
	heldBack := refusals(e, e.cloud())
	want := "playbook_status refused, kind refused, size refused, tries refused, a good row written"
	ok := moved == "20" && held == want && heldBack == want && back == "21"
	return ok, fmt.Sprintf("Postgres %s cloud: %s rows of team 1 at the organization (want 20); written through the cloud while moved: %s; after move-back: %s rows in the cloud (want 21, with the good row written while moved), written there: %s",
		ver, moved, held, back, heldBack), nil
}

// refusals writes, for team 1, a row breaking each CHECK and then a good one: what became of each.
func refusals(e *env, d string) string {
	var out []string
	for _, w := range []struct{ name, row string }{
		{"playbook_status", "'bogus', 'web', 's', 0"},
		{"kind", "'done', 'gui', 's', 0"},
		{"size", "'done', 'web', 'xl', 0"},
		{"tries", "'done', 'web', 's', 9"},
	} {
		err := execSQL(e.ctx, d, "INSERT INTO builds (team_id, playbook_status, kind, size, tries) VALUES (1, "+w.row+")")
		switch {
		case err == nil:
			out = append(out, w.name+" accepted")
		case strings.Contains(err.Error(), "23514"):
			out = append(out, w.name+" refused")
		default:
			out = append(out, w.name+": "+firstErrLine(err))
		}
	}
	if err := execSQL(e.ctx, d, "INSERT INTO builds (team_id, playbook_status, kind, size) VALUES (1, 'running', 'cli', 'm')"); err != nil {
		return strings.Join(out, ", ") + ", the good row failed: " + firstErrLine(err)
	}
	return strings.Join(out, ", ") + ", a good row written"
}
