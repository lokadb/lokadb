package main

// Block 5 (moving an organization): the split holds DEFAULT and the organization's partition
// against vacuum (SHARE UPDATE EXCLUSIVE) from the start of its last transaction; the
// application's migrations that run meanwhile wait for it, they do not fail.

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MV37", name: "a CREATE INDEX of the application while the split runs waits for the split and succeeds, move-org succeeds too", run: checkDDLDuringSplit},
	)
}

// MV37. 300 000 documents, 200 100 of organization 1. As soon as the organization is paused for the
// split, a session runs CREATE INDEX on documents (SHARE on the table and its partitions,
// DEFAULT too, which the split holds against vacuum) with no lock_timeout, as a migration does.
// (An ADD COLUMN at that moment is not for the split: its partition, made LIKE DEFAULT before,
// lacks the column and ATTACH refuses it; move-org stops cleaning up after itself, the
// organization stays in the cloud. The same on any version, and a migration is not to run
// during a move.)
func checkDDLDuringSplit(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + (g % 3 = 0)::int, 1, repeat('x', 100) || g FROM generate_series(1, 300000) g"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var once sync.Once
	var ddlErr error
	var ddlTook time.Duration
	done := make(chan struct{})
	out, _, moveErr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "organization paused") {
			once.Do(func() {
				go func() {
					defer close(done)
					t0 := time.Now()
					ddlErr = execSQL(e.ctx, e.cloud(), "CREATE INDEX documents_mv37 ON documents (title)")
					ddlTook = time.Since(t0)
				}()
			})
		}
	}, "-mode", "trigger")
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		return false, "the application's CREATE INDEX is still waiting a minute after the move ended: " + lastLine(out), nil
	}
	if moveErr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if ddlErr != nil {
		return false, "the application's CREATE INDEX failed: " + firstErrLine(ddlErr), nil
	}
	col, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_indexes WHERE indexname = 'documents_mv37'")
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	ok := col == "1" && n == "200100"
	return ok, fmt.Sprintf("the CREATE INDEX ran for %s and succeeded (its index in the cloud: %s); organization 1 at its database: %s rows (want 200100)", ddlTook.Round(time.Millisecond), col, n), nil
}
