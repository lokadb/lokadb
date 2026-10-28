package main

// move-back's check before the point of no return (the admin review, A7).

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "RB10", name: "move-back stops before the switch when the copy's row count differs, and the organization stays in its own database", run: checkMoveBackPrecheck},
	)
}

// RB10. A row of the organization that neither the copy nor the log has: once move-back's
// capture is on the table, a transaction at the organization inserts it with
// session_replication_role = replica (no trigger runs, so the capture does not log it) and stays
// open (the copy does not see it); it commits when the switch waits for its lock on the table,
// so the row is there when the switch counts. move-back must refuse before its switch commits:
// the organization stays in its own database (lokadb.orgs, the foreign partition), not served
// by the cloud from a copy that lacks the row, which the verification after the switch found
// only once it was so.
func checkMoveBackPrecheck(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	watch, err := pgx.Connect(e.ctx, e.customer())
	if err != nil {
		return false, "", err
	}
	defer watch.Close(e.ctx)
	writer, err := pgx.Connect(e.ctx, e.customer())
	if err != nil {
		return false, "", err
	}
	defer writer.Close(e.ctx)
	type result struct {
		step string
		err  error
	}
	done := make(chan result, 1)
	go func() {
		poll := func(sql string) error {
			for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
				var yes bool
				if err := watch.QueryRow(e.ctx, sql).Scan(&yes); err != nil {
					return err
				}
				if yes {
					return nil
				}
			}
			return fmt.Errorf("timed out")
		}
		// move-back's capture is on the table
		if err := poll(`SELECT EXISTS (SELECT FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND tgname LIKE 'lokadb\_move\_org%')`); err != nil {
			done <- result{"waiting for the capture", err}
			return
		}
		// the row past the capture, uncommitted while the copy reads
		tx, err := writer.Begin(e.ctx)
		if err == nil {
			_, err = tx.Exec(e.ctx, "SET LOCAL session_replication_role = replica")
		}
		if err == nil {
			_, err = tx.Exec(e.ctx, "INSERT INTO documents (id, organization_id, user_id, title) VALUES (900000001, 1, 2, 'past the capture')")
		}
		if err != nil {
			done <- result{"the row past the capture", err}
			return
		}
		// the switch waits for the table: the row commits now
		if err := poll(`SELECT EXISTS (SELECT FROM pg_locks WHERE relation = 'documents'::regclass AND NOT granted)`); err != nil {
			_ = tx.Rollback(e.ctx)
			done <- result{"waiting for the switch's lock", err}
			return
		}
		done <- result{"", tx.Commit(e.ctx)}
	}()
	out, err := e.run6(e.moveBackArgs(1)...)
	r := <-done
	if r.err != nil {
		return false, fmt.Sprintf("%s: %v (the check proves nothing); move-back: %v", r.step, r.err, err), nil
	}
	why := "accepted"
	if err != nil {
		why = errLine(err)
	}
	where, qerr := one(e.ctx, e.cloud(), "SELECT (SELECT count(*) FROM lokadb.orgs WHERE org = 1)::text || ' ' || coalesce((SELECT relkind::text FROM pg_class WHERE relname = 'documents__org1'), '-')")
	if qerr != nil {
		return false, "", qerr
	}
	before := err != nil && strings.Contains(out, "nothing switched") && !strings.Contains(out, "verify after switch")
	return before && where == "1 f",
		fmt.Sprintf("a row past the capture, committed while the switch waited; move-back: %s; organization 1 in lokadb.orgs, its foreign partition: %q (want \"1 f\": still in its own database)", why, where), nil
}
