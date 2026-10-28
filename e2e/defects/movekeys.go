package main

// Block 5 (moving an organization): the trigger mode by the table's key (block 4: one integer
// column, whatever its name), not by a column id.

import (
	"fmt"
	"strings"
	"sync"
)

func init() {
	checks = append(checks,
		check{id: "MV14", name: "-mode trigger: tables keyed by ticket_no (integer) and note_key (smallint) move, changes during the move (a key updated too) arrive", run: checkTriggerKeys},
	)
}

// MV14. Neither table has a column id. The copy goes in batches of 3 (each batch starts past
// the key the last one ended with). While the log is applied the application inserts,
// updates, deletes and changes a key. Before: "column "id" does not exist".
func checkTriggerKeys(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE tickets (ticket_no integer PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"CREATE TABLE notes (note_key smallint PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"ALTER TABLE tickets OWNER TO app", "ALTER TABLE notes OWNER TO app",
		"INSERT INTO tickets SELECT g, 1 + g % 2, 'b' || g FROM generate_series(1, 20) g",
		"INSERT INTO notes SELECT g, 1 + g % 2, 'n' || g FROM generate_series(1, 20) g"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "tickets", "tenant_column": "organization_id"}, {"name": "notes", "tenant_column": "organization_id"}]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	var once sync.Once
	var werr error
	out, _, merr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "step 5/6") {
			once.Do(func() {
				// organization 1 has the even keys
				werr = execSQL(e.ctx, e.cloud(),
					"INSERT INTO tickets VALUES (101, 1, 'inserted')",
					"UPDATE tickets SET body = 'updated' WHERE ticket_no = 2",
					"DELETE FROM tickets WHERE ticket_no = 4",
					"UPDATE tickets SET ticket_no = 106 WHERE ticket_no = 6",
					"INSERT INTO notes VALUES (101, 1, 'inserted')",
					"DELETE FROM notes WHERE note_key = 8")
			})
		}
	}, "-mode", "trigger", "-batch-rows", "3")
	if werr != nil {
		return false, "the application's writes during the move failed: " + firstErrLine(werr), nil
	}
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	tickets, err := one(e.ctx, e.customer(), `SELECT count(*)::text || ' tickets; ' || coalesce(string_agg(ticket_no || '=' || body, ', ' ORDER BY ticket_no)
		FILTER (WHERE ticket_no IN (2, 4, 6, 101, 106)), '') FROM tickets`)
	if err != nil {
		return false, "", err
	}
	notes, err := one(e.ctx, e.customer(), "SELECT count(*)::text || ' notes, 101 ' || count(*) FILTER (WHERE note_key = 101) || ', 8 ' || count(*) FILTER (WHERE note_key = 8) FROM notes")
	if err != nil {
		return false, "", err
	}
	// 10 even keys, + 1 inserted - 1 deleted
	ok := tickets == "10 tickets; 2=updated, 101=inserted, 106=b6" && notes == "10 notes, 101 1, 8 0"
	return ok, fmt.Sprintf("at the organization: %s; %s", tickets, notes), nil
}
