package main

// The pause of one organization: move-org holds lokadb.org_lock(N) exclusively while it splits
// the organization out of DEFAULT; through the proxy that organization's statements wait and
// every other one's run.

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "L1", name: "while a move holds organization 1's lock, its statements wait and organization 2's run", run: checkPauseOne},
	)
}

func checkPauseOne(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	has, err := one(e.ctx, e.cloud(), "SELECT (to_regprocedure('lokadb.org_lock(bigint)') IS NOT NULL)::text")
	if err != nil || has != "true" {
		return false, "lokadb.org_lock is not there after enable", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	conn := func() *pgconn.PgConn {
		c, err := e.viaProxy()
		if err != nil {
			panic(err)
		}
		return c
	}
	simpleQ := func(c *pgconn.PgConn, q string) error { _, err := simple(e.ctx, c, q); return err }
	extended := func(c *pgconn.PgConn, q string, args ...string) error {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		return c.ExecParams(e.ctx, q, ps, nil, nil, nil).Read().Err
	}
	// timed runs f and reports how long it took, or "still waiting" after limit
	timed := func(limit time.Duration, f func() error) (time.Duration, bool, error) {
		start, done := time.Now(), make(chan error, 1)
		go func() { done <- f() }()
		select {
		case err := <-done:
			return time.Since(start), true, err
		case <-time.After(limit):
			return limit, false, nil
		}
	}

	mover, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer mover.Close(e.ctx)
	if _, err := mover.Exec(e.ctx, "SELECT pg_advisory_lock(lokadb.org_lock(1))"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// organization 2 runs, simple and extended
	c2 := conn()
	defer c2.Close(e.ctx)
	d, fin, err := timed(3*time.Second, func() error {
		return simpleQ(c2, "INSERT INTO documents (organization_id, user_id, title) VALUES (2, 2, 'l1-2')")
	})
	note(fin && err == nil, fmt.Sprintf("organization 2, simple: %v in %v", errOr(err, fin), d.Round(time.Millisecond)))
	d, fin, err = timed(3*time.Second, func() error {
		return extended(c2, "UPDATE documents SET title = 'l1-2b' WHERE organization_id = $1 AND title = 'l1-2'", "2")
	})
	note(fin && err == nil, fmt.Sprintf("organization 2, extended: %v in %v", errOr(err, fin), d.Round(time.Millisecond)))
	// organization 1 waits, in both protocols
	c1, c1x := conn(), conn()
	defer c1.Close(e.ctx)
	defer c1x.Close(e.ctx)
	r1 := make(chan error, 1)
	r1x := make(chan error, 1)
	go func() {
		r1 <- simpleQ(c1, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'l1-1')")
	}()
	go func() {
		r1x <- extended(c1x, "UPDATE documents SET title = 'l1-1x' WHERE organization_id = $1 AND id = (SELECT min(id) FROM documents WHERE organization_id = 1)", "1")
	}()
	time.Sleep(1500 * time.Millisecond)
	waiting := len(r1) == 0 && len(r1x) == 0
	note(waiting, fmt.Sprintf("organization 1 still waiting after 1.5 s: %v", waiting))
	// a statement of every organization waits too (the move holds that lock as well)
	if _, err := mover.Exec(e.ctx, "SELECT pg_advisory_lock(lokadb.org_lock(NULL))"); err != nil {
		return false, "", err
	}
	ca := conn()
	defer ca.Close(e.ctx)
	_, fin, _ = timed(1500*time.Millisecond, func() error { return simpleQ(ca, "SELECT count(*) FROM documents") })
	note(!fin, fmt.Sprintf("a statement over every organization waits: %v", !fin))
	start := time.Now()
	if _, err := mover.Exec(e.ctx, "SELECT pg_advisory_unlock_all()"); err != nil {
		return false, "", err
	}
	got := func(ch chan error) string {
		select {
		case err := <-ch:
			return errOr(err, true)
		case <-time.After(5 * time.Second):
			return "still waiting after the unlock"
		}
	}
	a, b := got(r1), got(r1x)
	note(a == "ran" && b == "ran", fmt.Sprintf("after the unlock organization 1: simple %s, extended %s (%v)", a, b, time.Since(start).Round(time.Millisecond)))
	// no lock is left behind by the proxy's statements
	left, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted")
	if err != nil {
		return false, "", err
	}
	// ca's statement over every organization may still be finishing: wait a little
	for i := 0; i < 20 && left != "0"; i++ {
		time.Sleep(100 * time.Millisecond)
		left, _ = one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted")
	}
	note(left == "0", "advisory locks left once everything is done: "+left)
	// a move cannot take the lock while an organization's transaction holds it: it gives way
	tx := conn()
	defer tx.Close(e.ctx)
	if err := simpleQ(tx, "BEGIN"); err != nil {
		return false, "", err
	}
	if err := simpleQ(tx, "UPDATE documents SET title = 'l1-tx' WHERE organization_id = 1 AND title = 'l1-1'"); err != nil {
		return false, "", err
	}
	_, err = mover.Exec(e.ctx, "SET lock_timeout = '300ms'; SELECT pg_advisory_lock(lokadb.org_lock(1))")
	note(err != nil && strings.Contains(err.Error(), "lock timeout"), "a move while organization 1's transaction is open: "+errOr(err, true))
	_ = simpleQ(tx, "COMMIT")
	return ok, strings.Join(notes, "; "), nil
}

func errOr(err error, finished bool) string {
	switch {
	case !finished:
		return "still waiting"
	case err != nil:
		return "error: " + firstErrLine(err)
	}
	return "ran"
}
