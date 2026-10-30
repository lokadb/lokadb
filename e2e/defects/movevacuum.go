package main

// Block 5 (moving an organization): an autovacuum worker on the DEFAULT partition, woken by the
// rows the split just deleted from it, must not stop the move (risk hunt R11).

import (
	"fmt"
	"strings"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MV34", name: "organization 1 with 480 000 rows in DEFAULT while autovacuum works on DEFAULT after the split: the switch asks it to stop and move-org succeeds, the pause stays short", run: checkAutovacuumOnDefault},
	)
}

// MV34. 600 000 documents, 480 000 of organization 1. Autovacuum wakes every second and is made
// slow on the DEFAULT partition (it holds the table for minutes, as it does on a big one): the
// split deletes the organization's rows from DEFAULT, autovacuum starts on them at once, and the
// switch asks for ACCESS EXCLUSIVE on DEFAULT in tries shorter than deadlock_timeout, which
// Postgres never answers by cancelling the worker. Before: five tries of 250 ms, then "switch not
// possible right now" (55P03); the split's last transaction waited as long for the same worker.
// Now the split holds DEFAULT against vacuum before it deletes, the switch too, and a worker that
// runs is cancelled (the check runs as a superuser): move-org goes on.
func checkAutovacuumOnDefault(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER SYSTEM SET autovacuum_naptime = '1s'",
		"SELECT pg_reload_conf()",
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + (g % 5 = 0)::int, 1, repeat('x', 200) || g FROM generate_series(1, 600000) g"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// slow and early: a vacuum that works for minutes once a few thousand rows are dead
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents__cloud SET (autovacuum_vacuum_threshold = 1000, autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_cost_delay = 20, autovacuum_vacuum_cost_limit = 10)"); err != nil {
		return false, "", err
	}
	out, _, moveErr := e.moveLines(1, nil, "-mode", "trigger")
	paused := durationIn(out, `msg="organization resumed".* paused=([0-9.]+[a-zµ]+)`)
	cancelled := strings.Count(out, "autovacuum cancelled")
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	inCloud, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if moveErr != nil {
		lines := strings.Split(strings.TrimSpace(out), "\n")
		return false, fmt.Sprintf("move-org failed (%d autovacuum workers cancelled): %s | log: %s", cancelled, lastErr(out),
			strings.Join(strings.Fields(strings.Join(lines[max(0, len(lines)-12):], " / ")), " ")), nil
	}
	ok := paused > 0 && paused <= 15*time.Second && n == "480100" && inCloud == "120100"
	return ok, fmt.Sprintf("autovacuum workers the move cancelled: %d (none when the split held DEFAULT before one started); organization paused %s (want at most 15s); organization 1 at its database: %s rows (want 480100: the fixture's 100 rows too); organization 2 in the cloud: %s (want 120100)",
		cancelled, paused.Round(time.Millisecond), n, inCloud), nil
}
