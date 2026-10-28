package main

// Block 9, item 7: cursors and LISTEN/NOTIFY on LokaDB tables of a moved organization, on fdw,
// on fdw+direct and in the cooperative mode (B9.7 ran them on the cloud's rows alone).

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "CU1", name: "cursors and NOTIFY on a moved organization, fdw: FETCH, WITH HOLD after COMMIT, a cursor without the organization under reject, NOTIFY from the application's trigger", run: func(e *env) (bool, string, error) { return checkCursorsMoved(e, "fdw") }},
		check{id: "CU2", name: "cursors and NOTIFY on a moved organization, fdw+direct: the same, and a cursor in a direct transaction", run: func(e *env) (bool, string, error) { return checkCursorsMoved(e, "fdw+direct") }},
		check{id: "CU3", name: "cursors and NOTIFY in the cooperative mode: a cursor under lokadb.tenant reads that organization alone, also at a FETCH after COMMIT", run: func(e *env) (bool, string, error) { return checkCursorsMoved(e, "coop") }},
		check{id: "CU4", name: "cursors and NOTIFY in the cooperative mode on fdw+direct: the same, and a cursor in a direct cooperative transaction never reads without its organization", run: func(e *env) (bool, string, error) { return checkCursorsMoved(e, "coop+direct") }},
	)
}

// checkCursorsMoved: organization 1 moved (fdw; fdw+direct; fdw named by the application in the
// cooperative mode, also on fdw+direct), fanout reject. Organization 1 has 100 documents.
func checkCursorsMoved(e *env, mode string) (bool, string, error) {
	var stop func()
	var err error
	coop, direct := strings.HasPrefix(mode, "coop"), strings.HasSuffix(mode, "direct")
	if direct {
		stop, _, err = e.directWriteSetup()
	} else {
		stop, err = e.directSetup(nil)
	}
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	user, err := one(e.ctx, e.cloud(), "SELECT min(user_id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	// the application's trigger: every new document notifies its title; the proxy hears of it at once
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION cu_note() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_notify('cu', NEW.title); RETURN NEW; END $$`,
		"CREATE TRIGGER cu_note AFTER INSERT ON documents FOR EACH ROW EXECUTE FUNCTION cu_note()",
		"NOTIFY lokadb_meta"); err != nil {
		return false, "", err
	}
	time.Sleep(500 * time.Millisecond)
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	lcfg, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	var heard []string
	lcfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) { heard = append(heard, n.Channel+":"+n.Payload) }
	l, err := pgconn.ConnectConfig(e.ctx, lcfg)
	if err != nil {
		return false, "", err
	}
	defer l.Close(e.ctx)
	if _, err := simple(e.ctx, l, "LISTEN cu"); err != nil {
		return false, "", err
	}
	// hear: what the listener got since the last call
	hear := func() string {
		time.Sleep(700 * time.Millisecond)
		_, _ = simple(e.ctx, l, "SELECT 1")
		got := strings.Join(heard, ",")
		heard = nil
		if got == "" {
			return "nothing"
		}
		return got
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
	begin := func(org string) error {
		if _, err := simple(e.ctx, c, "BEGIN"); err != nil {
			return err
		}
		if coop && direct { // SET LOCAL: what a direct cooperative transaction starts with (block 10)
			_, err := simple(e.ctx, c, "SET LOCAL lokadb.tenant = '"+org+"'")
			return err
		}
		if coop {
			_, err := simple(e.ctx, c, "SELECT set_config('lokadb.tenant', '"+org+"', true)")
			return err
		}
		return nil
	}
	// run: statements in order, the last one's row count or the first error
	run := func(sqls ...string) string {
		n := 0
		for _, sql := range sqls {
			rs, err := simple(e.ctx, c, sql)
			if err != nil {
				_, _ = simple(e.ctx, c, "ROLLBACK")
				return "error: " + firstErrLine(err)
			}
			n = len(rs[len(rs)-1].Rows)
		}
		return fmt.Sprint(n)
	}
	inTx := func(org string, sqls ...string) string {
		if err := begin(org); err != nil {
			return "error: " + firstErrLine(err)
		}
		r := run(sqls...)
		if !strings.HasPrefix(r, "error") {
			if _, err := simple(e.ctx, c, "COMMIT"); err != nil {
				return "error at COMMIT: " + firstErrLine(err)
			}
		}
		return r
	}
	sum := func(parts ...string) string {
		t := 0
		for _, p := range parts {
			var n int
			if _, err := fmt.Sscan(p, &n); err != nil {
				return strings.Join(parts, "+")
			}
			t += n
		}
		return fmt.Sprint(t)
	}
	org1 := "SELECT id FROM documents WHERE organization_id = 1"
	all := "SELECT id FROM documents"
	if coop {
		org1 = all // lokadb.tenant names the organization
	}

	// a cursor of organization 1, fetched in two pieces
	if err := begin("1"); err != nil {
		return false, "", err
	}
	a := run("DECLARE a CURSOR FOR "+org1, "FETCH 30 FROM a")
	b := run("FETCH ALL FROM a")
	_, _ = simple(e.ctx, c, "COMMIT")
	note(sum(a, b) == "100", fmt.Sprintf("a cursor of organization 1, FETCH 30 then FETCH ALL: %s + %s rows (want 100)", a, b))

	// WITH HOLD: filled at COMMIT, fetched outside the transaction and after a transaction of another organization
	other := "INSERT INTO documents (organization_id, user_id, title) VALUES (2, 2, 'cu-2')"
	if coop {
		other = "SELECT count(*) FROM documents"
	}
	h := inTx("1", "DECLARE h CURSOR WITH HOLD FOR "+org1, "FETCH 10 FROM h")
	o := inTx("2", other)
	h2 := run("FETCH ALL FROM h")
	_, _ = simple(e.ctx, c, "CLOSE h")
	note(sum(h, h2) == "100", fmt.Sprintf("a cursor WITH HOLD of organization 1: FETCH 10 in its transaction %s, a transaction of organization 2 (%s), FETCH ALL after both: %s (want 100 in all)", h, o, h2))

	if !coop {
		// a cursor of organization 1 fetched after a statement of organization 2 in its transaction
		r := inTx("1", "DECLARE c CURSOR FOR "+org1, other, "FETCH ALL FROM c")
		note(r == "100", "a cursor of organization 1 fetched after an INSERT of organization 2: "+r+" (want 100)")
		// WITH HOLD declared outside a transaction (it is filled at once)
		r = run("DECLARE ha CURSOR WITH HOLD FOR "+org1, "FETCH ALL FROM ha")
		_, _ = simple(e.ctx, c, "CLOSE ha")
		note(r == "100", "a cursor WITH HOLD of organization 1 declared outside a transaction: "+r+" (want 100)")
		// no organization: under reject a cursor is refused as the SELECT is
		plain := run(all)
		r = inTx("1", "DECLARE f CURSOR FOR "+all, "FETCH ALL FROM f")
		note(strings.HasPrefix(plain, "error") && strings.HasPrefix(r, "error"), fmt.Sprintf("no organization under reject: the SELECT %s; the cursor %s (want refused too)", plain, r))
	} else {
		// lokadb.tenant = 1 and a cursor of organization 2's rows: none (never widened)
		r := inTx("1", "DECLARE w CURSOR FOR SELECT id FROM documents WHERE organization_id = 2", "FETCH ALL FROM w")
		note(r == "0" || strings.Contains(r, "42501"), "a cursor of organization 2 under lokadb.tenant = 1: "+r+" (want 0 rows or 42501)")
	}

	if direct {
		// a cursor in a direct transaction: its first statement's form learnt (two through the cloud)
		first := fmt.Sprintf("SELECT title FROM documents WHERE organization_id = 1 AND id = %s", doc)
		if coop {
			first = fmt.Sprintf("SELECT title FROM documents WHERE id = %s", doc)
		}
		for i := 0; i < 2; i++ {
			_ = inTx("1", first)
		}
		n := directCount(e.proxyRoutes(), "1")
		r := inTx("1", first, "DECLARE d CURSOR FOR "+org1, "FETCH ALL FROM d")
		onDirect := directCount(e.proxyRoutes(), "1") - n
		again := inTx("1", first, "DECLARE d CURSOR FOR "+org1, "FETCH ALL FROM d")
		note((r == "100" || strings.Contains(r, "cursor")) && again == "100",
			fmt.Sprintf("a cursor in a direct transaction: %s (on the direct path %d; want 100 or a refusal naming the cursor); the same transaction again: %s (want 100)", r, onDirect, again))
	}
	if coop && direct {
		// the direct cooperative transaction of block 10 through pgx (SET LOCAL lokadb.tenant, its
		// first read learnt); then a cursor without a condition on the organization in it
		px, err := e.viaProxyPgx("cu4")
		if err != nil {
			return false, "", err
		}
		defer px.Close(e.ctx)
		var id int64
		if _, err := fmt.Sscan(doc, &id); err != nil {
			return false, "", err
		}
		ptx := func(cursor bool) (string, int) {
			n := directCount(e.proxyRoutes(), "1")
			moved := func() int { return directCount(e.proxyRoutes(), "1") - n }
			t, err := px.Begin(e.ctx)
			if err != nil {
				return "begin: " + firstErrLine(err), moved()
			}
			defer t.Rollback(e.ctx) //nolint:errcheck // a no-op after COMMIT
			if _, err := t.Exec(e.ctx, "SET LOCAL lokadb.tenant = '1'"); err != nil {
				return "SET LOCAL: " + firstErrLine(err), moved()
			}
			var user int64 // a form of its own: the simple one above now goes the cloud's way (its cursor was refused)
			if err := t.QueryRow(e.ctx, "SELECT user_id FROM documents WHERE id = $1", id).Scan(&user); err != nil {
				return "read: " + firstErrLine(err), moved()
			}
			res := "read"
			if cursor {
				if _, err := t.Exec(e.ctx, "DECLARE pd CURSOR FOR SELECT id FROM documents"); err != nil {
					return "error: " + firstErrLine(err), moved()
				}
				rows, err := t.Query(e.ctx, "FETCH ALL FROM pd")
				if err != nil {
					return "error: " + firstErrLine(err), moved()
				}
				k := 0
				for rows.Next() {
					k++
				}
				if err := rows.Err(); err != nil {
					return "error: " + firstErrLine(err), moved()
				}
				res = fmt.Sprint(k)
			}
			if err := t.Commit(e.ctx); err != nil {
				return "commit: " + firstErrLine(err), moved()
			}
			return res, moved()
		}
		_, _ = ptx(false)
		_, _ = ptx(false)
		r, d := ptx(true)
		again, _ := ptx(true)
		note(d > 0 && (r == "100" || strings.Contains(r, "cursor")) && again == "100",
			fmt.Sprintf("a cursor without a condition on the organization in a direct cooperative transaction (pgx): %s (on the direct path %d, want more than 0; want 100 or a refusal naming the cursor, never more than 100); the same transaction again: %s (want 100)", r, d, again))
	}

	// NOTIFY from the application's trigger on organization 1's insert
	_ = hear() // the trigger's notes of organization 2's inserts above
	ins := fmt.Sprintf("INSERT INTO documents (organization_id, user_id, title) VALUES (1, %s, 'cu-trigger')", user)
	var r string
	if coop {
		r = inTx("1", ins)
	} else {
		r = run(ins)
	}
	got := hear()
	note(got == "cu:cu-trigger", fmt.Sprintf("NOTIFY from the application's trigger on organization 1's INSERT (%s): heard %s (want cu:cu-trigger once)", r, got))

	// NOTIFY in a transaction with organization 1's UPDATE: heard at COMMIT only, three times (on
	// fdw+direct the third one direct)
	upd := fmt.Sprintf("UPDATE documents SET title = title WHERE organization_id = 1 AND id = %s", doc)
	var rs []string
	for i := 1; i <= 3; i++ {
		rs = append(rs, inTx("1", upd, fmt.Sprintf("NOTIFY cu, 'tx%d'", i), fmt.Sprintf("SELECT pg_notify('cu', 'fn%d')", i)))
	}
	got = hear()
	note(got == "cu:tx1,cu:fn1,cu:tx2,cu:fn2,cu:tx3,cu:fn3", fmt.Sprintf("NOTIFY and pg_notify in three transactions with organization 1's UPDATE (%s): heard %s", strings.Join(rs, " "), got))
	if err := begin("1"); err != nil {
		return false, "", err
	}
	r = run(upd, "NOTIFY cu, 'rolled-back'")
	_, _ = simple(e.ctx, c, "ROLLBACK")
	got = hear()
	note(got == "nothing", fmt.Sprintf("NOTIFY in a rolled-back transaction (%s): heard %s (want nothing)", r, got))
	return ok, strings.Join(notes, "; "), nil
}
