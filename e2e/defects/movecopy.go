package main

// Block 5 (moving an organization), A4, 2b, 2c, U3: the copy into the organization's database
// of a large organization — batches, going on after a broken connection, limits, progress.

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV3", name: "trigger mode: the initial copy commits in batches and reports its progress", run: checkTriggerCopyBatches},
		check{id: "MV3", name: "trigger mode: a broken connection to the organization's database does not restart the copy", run: checkTriggerCopyResume},
		check{id: "MV4", name: "the replication role lives as long as the move's limits, not 48 h", run: checkReplRoleLife},
		check{id: "MV4", name: "trigger mode: -max-pause bounds the switch's pause", run: checkTriggerMaxPause},
		check{id: "MV5", name: "logical mode: the initial copy reports its progress table by table", run: checkLogicalCopyProgress},
	)
}

// countProgress counts the progress lines of a step.
func countProgress(out, step string) int {
	n := 0
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "msg=progress") && strings.Contains(l, "step="+step+" ") {
			n++
		}
	}
	return n
}

// MV3 (2b, U3). The trigger mode copied every table in one transaction of the organization's
// database, silent until the end. Now -batch-rows rows per transaction (their xmin there).
func checkTriggerCopyBatches(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	out, took, err := e.moveLines(bigOrg, nil, "-mode", "trigger", "-batch-rows", fmt.Sprint(bigRows/6))
	if err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	txs, err := one(e.ctx, e.customer(), "SELECT count(DISTINCT xmin::text) FROM documents")
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	nt, _ := strconv.Atoi(txs)
	lines := countProgress(out, "copy")
	ok := nt >= 6 && lines >= 2 && n == fmt.Sprint(bigRows)
	return ok, fmt.Sprintf("%d rows arrived in %s transaction(s) (want 6 of %d); copy progress lines: %d; rows there: %s; move took %s",
		bigRows, txs, bigRows/6, lines, n, took.Round(time.Second)), nil
}

// MV3 (A4). The connection that writes the copy into the organization's database breaks
// halfway (a WAN): the move must go on with the batch it was sending, not start over or fail.
func checkTriggerCopyResume(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	var started, killed atomic.Bool
	stop := make(chan struct{})
	defer close(stop)
	out, took, err := e.moveLines(bigOrg, func(l string) {
		if !started.Load() && strings.Contains(l, "step 4/6") {
			started.Store(true)
			go func() {
				// once a few batches are there, end the backend that writes the copy
				for i := 0; i < 600; i++ {
					select {
					case <-stop:
						return
					case <-time.After(50 * time.Millisecond):
					}
					n, err := one(e.ctx, e.customer(), `SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity
						WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'COPY %FROM STDIN%'
						AND (SELECT count(*) FROM documents__o3) >= `+fmt.Sprint(bigRows/3))
					if err == nil && n != "0" {
						killed.Store(true)
						return
					}
				}
			}()
		}
	}, "-mode", "trigger", "-batch-rows", fmt.Sprint(bigRows/12))
	if !killed.Load() {
		return false, fmt.Sprintf("the copy's connection was not broken (copy seen: %v): %s", started.Load(), lastLine(out)), nil
	}
	if err != nil {
		return false, "move-org failed after the connection broke: " + lastLine(out), nil
	}
	n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	again := strings.Contains(out, "sending the batch again")
	ok := n == fmt.Sprint(bigRows) && again
	return ok, fmt.Sprintf("connection broken after %d rows; batch sent again: %v; rows there: %s; move took %s", bigRows/3, again, n, took.Round(time.Second)), nil
}

// MV4 (2c). The subscription's role was valid for 48 h whatever the move's limits.
func checkReplRoleLife(e *env) (bool, string, error) {
	if moveMode == "trigger" {
		return true, "trigger mode: no replication role", nil
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	life := ""
	out, _, err := e.moveLines(1, func(l string) {
		if life == "" && strings.Contains(l, "step 4/6") {
			// this database's role: roles belong to the cluster, other checks may have left theirs
			life, _ = one(e.ctx, e.cloud(), `SELECT coalesce(round(extract(epoch FROM rolvaliduntil - now()) / 3600, 1)::text, 'none')
				FROM pg_roles WHERE rolname = 'lokadb_repl_' || (SELECT oid FROM pg_database WHERE datname = current_database()) || '_org1'`)
		}
	}, "-mode", "logical", "-copy-timeout", "10m", "-catchup-timeout", "10m")
	if err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	h, perr := strconv.ParseFloat(life, 64)
	ok := perr == nil && h <= 3
	return ok, fmt.Sprintf("with -copy-timeout 10m -catchup-timeout 10m the replication role was valid for %s h more", life), nil
}

// MV4 (2c). -max-pause was kept only by the logical mode: the trigger mode's switch applied
// the rest of the log under its lock however long it took. The switch must find a log that
// takes it more than one round to apply, whatever the machine's speed: a transaction of the
// application changes organization 1's rows 1500 times (the switch applies 1000 entries a
// round and checks the limit before each) and commits only once the switch waits for its
// lock, so none of it was applied in the catch-up.
func checkTriggerMaxPause(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	var once sync.Once
	var werr error
	held := "no"
	done := make(chan struct{})
	write := func() {
		defer close(done)
		tx, err := w.Begin(e.ctx)
		if err != nil {
			werr = err
			return
		}
		defer tx.Rollback(e.ctx)
		for i := 0; i < 15; i++ {
			if _, werr = tx.Exec(e.ctx, "UPDATE documents SET title = title || '.' WHERE organization_id = 1"); werr != nil {
				return
			}
		}
		// the switch waits for the lock this transaction holds
		for i := 0; i < 3000; i++ {
			n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE NOT granted AND mode = 'AccessExclusiveLock' AND relation = 'documents'::regclass")
			if n != "0" {
				held = "yes"
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		werr = tx.Commit(e.ctx)
	}
	out, _, merr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "step 5/6") {
			once.Do(func() { go write() })
		}
	}, "-mode", "trigger", "-max-pause", "1ms")
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
	if werr != nil {
		return false, "the application's transaction failed: " + firstErrLine(werr), nil
	}
	moved, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	ok := held == "yes" && merr != nil && strings.Contains(out, "max-pause") && moved == "0" && n == "100"
	return ok, fmt.Sprintf("1500 changes committed while the switch waited for them: %s; move-org with -max-pause 1ms: %s; organization 1 moved: %s, its rows in the cloud: %s",
		held, errOr(merr, true)+" / "+lastLine(out), moved, n), nil
}

// MV5 (U3). The logical mode's copy said "initial copy in progress" with a count of tables:
// no rows, no bytes, no table names.
func checkLogicalCopyProgress(e *env) (bool, string, error) {
	if moveMode == "trigger" {
		return true, "trigger mode: no subscription (MV3 checks its progress)", nil
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	out, took, err := e.moveLines(bigOrg, nil, "-mode", "logical")
	if err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	var sample string
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "msg=progress") && strings.Contains(l, "step=copy ") && strings.Contains(l, "table=documents ") {
			sample = l[strings.Index(l, "step="):]
		}
	}
	ok := sample != "" && strings.Contains(sample, "state=done") && strings.Contains(sample, "rows_total=")
	return ok, fmt.Sprintf("copy progress lines: %d, last for documents: %q; move took %s", countProgress(out, "copy"), sample, took.Round(time.Second)), nil
}
