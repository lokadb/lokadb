package main

// Block 5 (moving an organization): the switch yields to the application in a lock cycle
// (block 4's D4 probe, at the coordinator's request).

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "MV15", name: "a writer holding the organization's partition, then cascading into a parent the switch holds: the writer commits, the switch waits its turn", run: checkSwitchYields},
	)
}

// MV15. A transaction holds ONLY documents__o1 when the switch begins. Once the switch has
// waited for it 1.3 s, the transaction deletes user 1, whose cascade needs document_versions,
// which the switch already holds: a lock cycle. Before: the switch waited -lock-timeout (5 s)
// in one go, longer than deadlock_timeout (1 s), and Postgres cancelled the application
// (40P01). Now each try of the switch waits under deadlock_timeout and gives its locks back:
// the application commits, the switch locks after it.
func checkSwitchYields(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "SELECT lokadb.partition_org(table_name, 1) FROM lokadb.tables"); err != nil {
		return false, "", err
	}
	dt, _ := one(e.ctx, e.cloud(), "SHOW deadlock_timeout")
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	// ACCESS SHARE from the start, in every mode: the switch's ACCESS EXCLUSIVE waits for it as it
	// does for the cascade's ROW EXCLUSIVE, and the capture's CREATE TRIGGER (SHARE ROW EXCLUSIVE)
	// installs past it. Taken after the copy it raced the move, which on a fast machine was over
	// before the lock was asked for ("relation documents__o1 does not exist").
	if _, err := tx.Exec(e.ctx, "LOCK TABLE ONLY documents__o1 IN ACCESS SHARE MODE"); err != nil {
		return false, "", err
	}
	var once6 sync.Once
	var txErr error
	var took time.Duration
	waited := "never seen"
	committed := make(chan struct{})
	out, _, moveErr := e.moveLines(1, func(line string) {
		if strings.Contains(line, "step 6/6") {
			once6.Do(func() {
				go func() {
					defer close(committed)
					// the switch's exclusive lock waiting for the holder, 1.3 s after it is first seen
					var seen time.Time
					for i := 0; i < 250; i++ {
						w, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE NOT granted AND mode = 'AccessExclusiveLock' AND relation = 'documents__o1'::regclass")
						if w != "0" && seen.IsZero() {
							seen = time.Now()
						}
						if !seen.IsZero() && time.Since(seen) >= 1300*time.Millisecond {
							waited = "1.3 s"
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					if _, txErr = tx.Exec(e.ctx, "SET LOCAL lokadb.fanout = 'log'"); txErr != nil {
						return
					}
					start := time.Now()
					_, txErr = tx.Exec(e.ctx, "DELETE FROM users WHERE id = 1")
					took = time.Since(start)
					if txErr != nil {
						return
					}
					time.Sleep(1500 * time.Millisecond)
					txErr = tx.Commit(e.ctx)
				}()
			})
		}
	})
	select {
	case <-committed:
	case <-time.After(20 * time.Second):
	}
	app := "committed"
	deadlock := false
	if txErr != nil {
		app = "failed: " + oneLine(txErr.Error())
		var pe *pgconn.PgError
		deadlock = errors.As(txErr, &pe) && pe.Code == "40P01"
	}
	mv := "done"
	if moveErr != nil {
		mv = "failed: " + lastErr(out)
	}
	tries := strings.Count(out, "switch attempt failed")
	user, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM users WHERE id = 1")
	left, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE user_id = 1")
	cl, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE user_id = 1")
	ok := txErr == nil && !deadlock && moveErr == nil && user == "0" && left == "0" && cl == "0"
	return ok, fmt.Sprintf("deadlock_timeout %s; the switch waited %s before the delete; DELETE took %s; the application's transaction %s; move-org %s (%d switch attempts failed); user 1 in the cloud: %s; its documents in the cloud: %s, in the customer database: %s",
		dt, waited, took.Round(time.Millisecond), app, mv, tries, user, cl, left), nil
}
