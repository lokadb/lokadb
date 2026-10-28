package main

// Block 5 (moving an organization): the split out of DEFAULT yields to the application in a
// lock cycle (T1 in block 10's full run on 608bc9d: "deadlock detected" in the split).

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
		check{id: "MV17", name: "a cascade from a cloud table into DEFAULT while the split waits for the parent: the application commits, the split runs again after it", run: checkSplitYields},
	)
}

// MV17. Organization 1 lies in DEFAULT. Once the split's CHECK is on DEFAULT, a transaction
// of the application holds DEFAULT (ROW EXCLUSIVE, as a cascade from users does). The split's
// last transaction deletes the organization's rows from DEFAULT and then waits for the parent
// (LOCK … EXCLUSIVE takes the partitions too). Then the application deletes user 1, whose
// cascade waits for those deleted rows: a lock cycle. Before: the split waited -lock-timeout
// (5 s) per try and kept its rows' locks between tries (ROLLBACK TO SAVEPOINT), Postgres
// cancelled it five times and move-org failed, or cancelled the application (40P01). Now each
// try waits under deadlock_timeout and gives the rows back: the application commits, the
// split runs again after it.
func checkSplitYields(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	// The gate: a session holds every user FOR UPDATE, so the split's last transaction (it locks
	// the users its rows reference FOR KEY SHARE before it touches DEFAULT) waits at its first
	// statement. The split is quick now (it copies before it pauses the organization: 8 ms from
	// the CHECK on DEFAULT to the ATTACH), and the holder below could not be sure to be in before
	// it. The gate opens once the holder has DEFAULT.
	gate, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer gate.Close(e.ctx)
	gtx, err := gate.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer gtx.Rollback(e.ctx)
	if _, err := gtx.Exec(e.ctx, "SELECT id FROM users FOR UPDATE"); err != nil {
		return false, "", err
	}
	var once sync.Once
	var txErr error
	var waited, took time.Duration
	held, deleted := "no", "no"
	done := make(chan struct{})
	start := func() {
		go func() {
			defer close(done)
			// the CHECK on DEFAULT is there: the split copies, then its last transaction
			for i := 0; i < 1500; i++ {
				n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_constraint WHERE conrelid = 'documents__cloud'::regclass AND conname = 'lokadb_not_org1'")
				if n == "1" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			tx, err := holder.Begin(e.ctx)
			if err != nil {
				txErr = err
				return
			}
			defer tx.Rollback(e.ctx)
			if _, txErr = tx.Exec(e.ctx, "LOCK TABLE ONLY documents__cloud IN ROW EXCLUSIVE MODE"); txErr != nil {
				return
			}
			held = "yes"
			if txErr = gtx.Commit(e.ctx); txErr != nil { // the gate opens: the split's last transaction goes on
				return
			}
			// the split waits for the parent (its partitions: DEFAULT, which this holds)
			w0 := time.Now()
			for i := 0; i < 1500; i++ {
				n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE NOT granted AND mode = 'ExclusiveLock' AND relation = 'documents__cloud'::regclass")
				if n != "0" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			waited = time.Since(w0)
			if _, txErr = tx.Exec(e.ctx, "SET LOCAL lokadb.fanout = 'log'"); txErr != nil {
				return
			}
			t0 := time.Now()
			_, txErr = tx.Exec(e.ctx, "DELETE FROM users WHERE id = 1")
			took = time.Since(t0)
			if txErr != nil {
				return
			}
			deleted = "yes"
			time.Sleep(500 * time.Millisecond)
			txErr = tx.Commit(e.ctx)
		}()
	}
	// -lock-timeout 30s: the split's tries may add up to 30 s x 5 on a loaded machine (a real
	// deadlock ends at once: the application commits, or the split would not go on)
	out, _, moveErr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "step 1/6") {
			once.Do(start)
		}
	}, "-lock-timeout", "30s")
	select {
	case <-done:
	case <-time.After(30 * time.Second):
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
		// the end of its log too: what happened to the connection before the error is there
		lines := strings.Split(strings.TrimSpace(out), "\n")
		mv = "failed: " + lastErr(out) + " | log: " + strings.Join(strings.Fields(strings.Join(lines[max(0, len(lines)-14):], " / ")), " ")
	}
	retries := strings.Count(out, "the last transaction of the split again") + strings.Count(out, "table busy, retrying")
	user, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM users WHERE id = 1")
	left, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE user_id = 1")
	moved, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	ok := held == "yes" && deleted == "yes" && txErr == nil && !deadlock && moveErr == nil && user == "0" && left == "0" && moved == "80"
	return ok, fmt.Sprintf("DEFAULT held: %s; the split waited for it after %s; DELETE of user 1 took %s; the application's transaction %s; move-org %s (%d retry lines); user 1 in the cloud: %s; at the organization: %s documents, %s of user 1",
		held, waited.Round(10*time.Millisecond), took.Round(time.Millisecond), app, mv, retries, user, moved, left), nil
}
