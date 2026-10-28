package main

// Block 14 (load and scale): statement kinds and operations no other check exercised before:
// LISTEN/NOTIFY, cursors and portals, several move-org at once (COPY: copy.go). A check passes when
// every case either works correctly or is refused with an error; a functional gap that is
// refused cleanly is reported in the detail, silently wrong results or rows fail it.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "LD2", name: "LISTEN/NOTIFY through the proxy: notifications reach an idle and a busy listener", run: checkNotify},
		check{id: "LD3", name: "cursors and suspended portals keep their organization when other statements run in between", run: checkCursors},
		check{id: "LD4", name: "move-org of several organizations at once: one runs, the others are refused, nothing is lost", run: checkParallelMoves},
	)
}

// outcome of one statement: "ok", "refused: <lokadb message>" or "error: <server message>".
func outcome(err error) string {
	if err == nil {
		return "ok"
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
		return "refused: " + pe.Message
	}
	return "error: " + firstErrLine(err)
}

// LD2. Notifications are asynchronous messages from the server: the proxy must pass them on
// when no query is in flight, and in the middle of a response.
func checkNotify(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	got := make(chan string, 16)
	lc, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	lc.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) { got <- n.Payload }
	l, err := pgconn.ConnectConfig(e.ctx, lc)
	if err != nil {
		return false, "", err
	}
	defer l.Close(e.ctx)
	s, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer s.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, msg string) {
		if !good {
			ok = false
			msg = "WRONG " + msg
		}
		notes = append(notes, msg)
	}
	// a listener whose last query set an organization
	if _, err := simple(e.ctx, l, "SELECT count(*) FROM documents WHERE organization_id = 1; LISTEN k2"); err != nil {
		return false, "LISTEN failed: " + outcome(err), nil
	}
	wait := func(d time.Duration) string { // the next notification the listener got
		select {
		case p := <-got:
			return p
		default:
		}
		ctx, cancel := context.WithTimeout(e.ctx, d)
		defer cancel()
		if err := l.WaitForNotification(ctx); err != nil {
			return "none"
		}
		select {
		case p := <-got:
			return p
		default:
			return "?"
		}
	}

	// 1. idle listener, plain NOTIFY
	if _, err := simple(e.ctx, s, "NOTIFY k2, 'plain'"); err != nil {
		note(false, "NOTIFY: "+outcome(err))
	}
	p := wait(3 * time.Second)
	note(p == "plain", "idle listener, NOTIFY: got "+p)

	// 2. pg_notify in a transaction that writes the moved organization
	_, err = simple(e.ctx, s, "BEGIN; UPDATE documents SET title = 'k2' WHERE organization_id = 1 AND id = (SELECT min(id) FROM documents WHERE organization_id = 1); SELECT pg_notify('k2', 'in-tx'); COMMIT")
	if err != nil {
		note(false, "write + pg_notify: "+outcome(err))
	}
	p = wait(3 * time.Second)
	note(p == "in-tx", "pg_notify committed with a write of organization 1: got "+p)

	// 3. a notification arriving while the listener's query streams a large result
	done := make(chan error, 1)
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := simple(e.ctx, s, "NOTIFY k2, 'busy'")
		done <- err
	}()
	_, err = simple(e.ctx, l, "SELECT d.id, pg_sleep(0.0005) FROM documents d, generate_series(1, 3) g WHERE d.organization_id = 2")
	<-done
	inBand := wait(3 * time.Second)
	note(err == nil && inBand == "busy", "listener busy with a query of organization 2 ("+outcome(err)+"): got "+inBand)

	// 4. UNLISTEN stops them
	if _, err := simple(e.ctx, l, "UNLISTEN k2"); err != nil {
		note(false, "UNLISTEN: "+outcome(err))
	}
	_, _ = simple(e.ctx, s, "NOTIFY k2, 'after'")
	after := wait(time.Second)
	note(after == "none", "after UNLISTEN: got "+after)
	return ok, strings.Join(notes, "; "), nil
}

// LD3. A cursor (and a portal suspended by a row limit) is fetched by statements that name no
// table; in between, the transaction's other statements set lokadb.org to other organizations.
func checkCursors(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, msg string) {
		if !good {
			ok = false
			msg = "WRONG " + msg
		}
		notes = append(notes, msg)
	}
	rowsOf := func(sql string) (int, string, error) { // rows of the last result, and its first value
		res, err := simple(e.ctx, c, sql)
		if err != nil {
			return 0, "", err
		}
		last := res[len(res)-1]
		first := ""
		if len(last.Rows) > 0 {
			first = string(last.Rows[0][0])
		}
		return len(last.Rows), first, nil
	}
	for _, org := range []string{"1", "2"} {
		other := map[string]string{"1": "2", "2": "1"}[org]
		total := 0
		_, _, err := rowsOf("BEGIN; DECLARE k3 CURSOR FOR SELECT id, organization_id FROM documents WHERE organization_id = " + org + " ORDER BY id")
		if err != nil {
			note(false, "organization "+org+": DECLARE: "+outcome(err))
			_, _ = simple(e.ctx, c, "ROLLBACK")
			continue
		}
		n, _, err1 := rowsOf("FETCH 10 FROM k3")
		total += n
		_, cnt, err2 := rowsOf("SELECT count(*) FROM documents WHERE organization_id = " + other)
		n, _, err3 := rowsOf("FETCH ALL FROM k3")
		total += n
		_, _, err4 := rowsOf("CLOSE k3; COMMIT")
		errs := []string{}
		for _, er := range []error{err1, err2, err3, err4} {
			if er != nil {
				errs = append(errs, outcome(er))
			}
		}
		note(total == 100 && len(errs) == 0, fmt.Sprintf("organization %s: cursor FETCH 10, a query of organization %s (%s), FETCH ALL: %d rows (want 100); errors %v", org, other, cnt, total, errs))
	}
	// WITH HOLD over the moved organization: materialized at COMMIT, read after it
	n1, _, err := rowsOf("BEGIN; DECLARE k3h CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 1; COMMIT; SELECT count(*) FROM documents WHERE organization_id = 2; FETCH ALL FROM k3h")
	_, _ = simple(e.ctx, c, "CLOSE k3h")
	note(err == nil && n1 == 100, fmt.Sprintf("WITH HOLD cursor of organization 1 read after COMMIT and another organization's query: %d rows (%s)", n1, outcome(err)))

	// extended protocol: a named portal executed 10 rows at a time inside a transaction
	if _, err := simple(e.ctx, c, "BEGIN"); err != nil {
		return false, "", err
	}
	fe := c.Frontend()
	rowsTill := func() (int, string) { // reads to ReadyForQuery
		n, last := 0, ""
		for {
			m, err := fe.Receive()
			if err != nil {
				return n, "error: " + err.Error()
			}
			switch m := m.(type) {
			case *pgproto3.DataRow:
				n++
			case *pgproto3.PortalSuspended:
				last = "suspended"
			case *pgproto3.CommandComplete:
				last = string(m.CommandTag)
			case *pgproto3.ErrorResponse:
				last = "error: " + m.Message
			case *pgproto3.ReadyForQuery:
				return n, last
			}
		}
	}
	fe.Send(&pgproto3.Parse{Name: "k3s", Query: "SELECT id FROM documents WHERE organization_id = $1 ORDER BY id"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "k3p", PreparedStatement: "k3s", Parameters: [][]byte{[]byte("1")}})
	fe.Send(&pgproto3.Execute{Portal: "k3p", MaxRows: 10})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		return false, "", err
	}
	a, lastA := rowsTill()
	_, mid, errMid := rowsOf("SELECT count(*) FROM documents WHERE organization_id = 2")
	fe.Send(&pgproto3.Execute{Portal: "k3p"})
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		return false, "", err
	}
	b, lastB := rowsTill()
	_, _ = simple(e.ctx, c, "COMMIT")
	note(a+b == 100 && errMid == nil, fmt.Sprintf("portal of organization 1, Execute 10 rows (%d, %s), a query of organization 2 (%s), Execute the rest (%d, %s): %d rows (want 100)", a, lastA, mid, b, lastB, a+b))
	return ok, strings.Join(notes, "; "), nil
}

// LD4. Moves of different organizations into their own databases, started together while the
// application keeps writing each of them.
func checkParallelMoves(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 3, 2, 'k4-' || g FROM generate_series(1, 100) g"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	orgs := []int{1, 3} // a move takes about 45 s here: two of them fit the check's 3 minutes
	cust := map[int]string{}
	for _, o := range orgs {
		db := fmt.Sprintf("%s_c%d", e.db, o)
		if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
			"DROP DATABASE IF EXISTS "+db+" WITH (FORCE)", "CREATE DATABASE "+db+" OWNER app"); err != nil {
			return false, "", err
		}
		cust[o] = dsn("customer", db, "postgres")
	}
	moveTo := func(org int) error {
		args := e.moveArgs(org)
		for i := range args {
			if args[i] == "-customer" {
				args[i+1] = cust[org]
			}
		}
		_, err := e.lokadb(args...)
		return err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// the application writes every organization during the moves
	var (
		mu       sync.Mutex
		inserted = map[int]int{}
		failed   = map[int]int{}
		firstErr = map[int]string{}
		quit     = make(chan struct{})
		wg       sync.WaitGroup
	)
	for _, o := range orgs {
		wg.Add(1)
		go func(o int) {
			defer wg.Done()
			c, err := e.viaProxy()
			if err != nil {
				return
			}
			defer c.Close(e.ctx)
			for i := 0; ; i++ {
				select {
				case <-quit:
					return
				default:
				}
				_, err := simple(e.ctx, c, fmt.Sprintf("INSERT INTO documents (organization_id, user_id, title) VALUES (%d, 2, 'k4w-%d')", o, i))
				mu.Lock()
				if err == nil {
					inserted[o]++
				} else {
					failed[o]++
					if firstErr[o] == "" {
						firstErr[o] = outcome(err)
					}
				}
				mu.Unlock()
				time.Sleep(5 * time.Millisecond)
			}
		}(o)
	}
	time.Sleep(200 * time.Millisecond)
	errs := make([]error, len(orgs))
	var mw sync.WaitGroup
	t0 := time.Now()
	for i, o := range orgs {
		mw.Add(1)
		go func(i, o int) { defer mw.Done(); errs[i] = moveTo(o) }(i, o)
	}
	mw.Wait()
	together := time.Since(t0)
	var notes []string
	ran, refused, other := 0, 0, 0
	var pending []int
	for i, err := range errs {
		switch {
		case err == nil:
			ran++
			notes = append(notes, fmt.Sprintf("org %d: done", orgs[i]))
		case strings.Contains(err.Error(), "another move-org is running"):
			refused++
			pending = append(pending, orgs[i])
			notes = append(notes, fmt.Sprintf("org %d: refused (another move-org is running)", orgs[i]))
		default:
			other++
			pending = append(pending, orgs[i])
			notes = append(notes, fmt.Sprintf("org %d: failed: %s", orgs[i], oneLine(err.Error())))
		}
	}
	notes = append(notes, fmt.Sprintf("started together: %d ran, %d refused, %d failed in %.1f s", ran, refused, other, together.Seconds()))
	// the refused ones, one after another
	t1 := time.Now()
	for _, o := range pending {
		if err := moveTo(o); err != nil {
			notes = append(notes, fmt.Sprintf("org %d again: failed: %s", o, oneLine(err.Error())))
			other++
		}
	}
	notes = append(notes, fmt.Sprintf("the rest one after another: %.1f s", time.Since(t1).Seconds()))
	time.Sleep(200 * time.Millisecond)
	close(quit)
	wg.Wait()
	ok := ran >= 1 && other == 0
	for _, o := range orgs {
		at, err := one(e.ctx, cust[o], "SELECT count(*) FROM documents")
		if err != nil {
			at = "error: " + firstErrLine(err)
		}
		left, _ := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE d.organization_id = %d AND c.relkind = 'r'", o))
		want := 100 + inserted[o]
		good := at == fmt.Sprint(want) && left == "0"
		ok = ok && good
		mark := ""
		if !good {
			mark = "WRONG "
		}
		notes = append(notes, fmt.Sprintf("%sorg %d: at its database %s (want %d: 100 + %d written during the moves, %d writes failed: %s), left in the cloud %s", mark, o, at, want, inserted[o], failed[o], firstErr[o], left))
	}
	return ok, strings.Join(notes, "; "), nil
}
