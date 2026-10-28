package main

// Block 9, item 5: the cache of cloud connections (upstream_pool; tasks/proxy-pooling.md,
// project of 25.10). Nothing one client leaves on a connection may reach the next client that
// gets it; the checks find the same server process (pg_backend_pid) under both, so that they
// test the cache, not two connections.

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "PL1", name: "a cached cloud connection: nothing of the client before (settings, temp table, prepared statements, locks, LISTEN, cursor, lokadb.org) reaches the next", run: checkPoolState},
		check{id: "PL3", name: "a cached cloud connection goes to the same role, password and startup parameters only", run: checkPoolKey},
		check{id: "PL4", name: "a changed password: the old one gets no cached connection past max_age", run: checkPoolPassword},
		check{id: "PL5", name: "the cancel key of a client gone away does not cancel the next client's statement on the same connection", run: checkPoolCancel},
		check{id: "PL6", name: "a connection left inside a transaction is not cached: its rows and locks are gone", run: checkPoolDirty},
		check{id: "PL8", name: "a client per request (PHP): new connections to the cloud and the organization's database, without and with the cache", run: checkPoolCount},
		check{id: "PL7", name: "a pooled direct connection: the setting the session before repeated on it does not reach the next session on the same server process", run: checkDirectPoolState},
		check{id: "PL9", name: "a client per request on the direct path (fdw+direct): new connections to the organization's database, without and with the pools", run: checkDirectPoolCount},
		check{id: "PL10", name: "the cache of cloud connections is on by default, nothing moved out: a client per request, new connections to the cloud by default and with upstream_pool false", run: checkPoolDefault},
	)
}

// checkPoolDefault: the user's decision of 27.10 (block 14: 16 clients, a connection per read
// of a cloud organization, 874 ops/s through the proxy without the cache, 2795 with it, 1321
// straight to Postgres). Nothing moved out: the config without upstream_pool caches the cloud's
// connections, "upstream_pool": false gives every client one of its own.
func checkPoolDefault(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	sessions := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "SELECT sessions FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	const clients = 20
	run := func(extra map[string]any) (int64, error) {
		stop, err := e.poolProxy(extra)
		if err != nil {
			return 0, err
		}
		defer stop()
		c0, err := sessions()
		if err != nil {
			return 0, err
		}
		for i := 0; i < clients; i++ { // connect, one read of a cloud organization, go
			c, err := e.poolConn("app", "app", "")
			if err != nil {
				return 0, err
			}
			var n int
			if err := c.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = 2").Scan(&n); err != nil {
				c.Close(e.ctx)
				return 0, err
			}
			c.Close(e.ctx)
			time.Sleep(20 * time.Millisecond)
		}
		c1, err := sessions()
		return c1 - c0, err
	}
	def, err := run(nil) // the default: no upstream_pool in the config
	if err != nil {
		return false, "", err
	}
	off, err := run(map[string]any{"upstream_pool": false})
	if err != nil {
		return false, "", err
	}
	ok := def <= 3 && off >= clients
	return ok, fmt.Sprintf("%d clients, one read each, nothing moved out: new connections to the cloud %d by default, %d with \"upstream_pool\": false", clients, def, off), nil
}

func (e *env) poolProxy(extra map[string]any) (func(), error) {
	cfg := map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}}
	for k, v := range extra {
		cfg[k] = v
	}
	return e.proxyCfg(map[string]string{}, cfg)
}

func (e *env) poolConn(user, password, extra string) (*pgx.Conn, error) {
	return pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable %s", e.port, e.db, user, password, extra))
}

func backendPID(ctx context.Context, c *pgx.Conn) int {
	var pid int
	_ = c.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid)
	return pid
}

func checkPoolState(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	pidA := backendPID(e.ctx, a)
	for _, sql := range []string{
		"SET statement_timeout = '4321'",
		"SET search_path = pg_catalog, public",
		"SELECT set_config('app.x', 'leak', false)",
		"CREATE TEMP TABLE pl1_tmp (x int)",
		"PREPARE pl1_p AS SELECT 1",
		"SELECT pg_advisory_lock(424242)",
		"LISTEN pl1_chan",
		"BEGIN", "DECLARE pl1_c CURSOR WITH HOLD FOR SELECT 1", "COMMIT",
		"SELECT count(*) FROM documents WHERE organization_id = 1", // lokadb.org = 1 on the server
	} {
		if _, err := a.Exec(e.ctx, sql); err != nil {
			return false, "", fmt.Errorf("client A, %s: %w", sql, err)
		}
	}
	if _, err := a.Prepare(e.ctx, "pl1_named", "SELECT 2"); err != nil {
		return false, "", err
	}
	a.Close(e.ctx)
	time.Sleep(500 * time.Millisecond)
	b, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	ok := true
	var notes []string
	if pidB := backendPID(e.ctx, b); pidB != pidA {
		return false, fmt.Sprintf("the cache was not used: client A on process %d, client B on %d", pidA, pidB), nil
	}
	for _, c := range []struct{ what, sql, want string }{
		{"statement_timeout", "SELECT current_setting('statement_timeout')", "0"},
		{"search_path", "SELECT current_setting('search_path')", `"$user", public`},
		{"set_config app.x", "SELECT coalesce(current_setting('app.x', true), '')", ""},
		{"temp table", "SELECT (to_regclass('pg_temp.pl1_tmp') IS NULL)::text", "true"},
		{"prepared statements", "SELECT count(*)::text FROM pg_prepared_statements WHERE name IN ('pl1_p', 'pl1_named')", "0"},
		{"advisory lock", "SELECT count(*)::text FROM pg_locks WHERE locktype = 'advisory' AND objid = 424242", "0"},
		{"LISTEN", "SELECT count(*)::text FROM pg_listening_channels() c WHERE c = 'pl1_chan'", "0"},
		{"cursor WITH HOLD", "SELECT count(*)::text FROM pg_cursors WHERE name = 'pl1_c'", "0"},
		// not client A's 1: the proxy's own -1 for B's statement of no LokaDB table (closed by
		// default, review of routing R1)
		{"lokadb.org", "SELECT coalesce(current_setting('lokadb.org', true), '')", "-1"},
	} {
		var got string
		if err := b.QueryRow(e.ctx, c.sql).Scan(&got); err != nil {
			got = "error: " + firstErrLine(err)
		}
		if got != c.want {
			ok = false
			notes = append(notes, fmt.Sprintf("WRONG %s seen by client B: %q", c.what, got))
		}
	}
	// the proxy's own state too: client B names no organization, the policy refuses it
	var n int
	err = b.QueryRow(e.ctx, "SELECT count(*) FROM documents").Scan(&n)
	if err == nil || !strings.Contains(err.Error(), "fanout") {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG client B's count naming no organization: %d rows (err %v)", n, err))
	}
	if ok {
		notes = append(notes, fmt.Sprintf("same process %d for both clients; client B sees none of client A's settings, temp table, prepared statements, advisory lock, LISTEN, cursor, lokadb.org; its count naming no organization is refused", pidA))
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkPoolKey(e *env) (bool, string, error) {
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	pidA := backendPID(e.ctx, a)
	a.Close(e.ctx)
	time.Sleep(300 * time.Millisecond)
	ok := true
	var notes []string
	if c, err := e.poolConn("app", "wrong", ""); err == nil {
		c.Close(e.ctx)
		ok = false
		notes = append(notes, "WRONG a wrong password got a connection")
	} else {
		notes = append(notes, "wrong password: "+firstErrLine(err))
	}
	c, err := e.poolConn("app", "app", "application_name=pl3")
	if err != nil {
		return false, "", err
	}
	if pid := backendPID(e.ctx, c); pid == pidA {
		ok = false
		notes = append(notes, "WRONG another application_name got the cached connection")
	}
	c.Close(e.ctx)
	time.Sleep(300 * time.Millisecond)
	d, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	pidD := backendPID(e.ctx, d)
	d.Close(e.ctx)
	if pidD != pidA {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG the same key got a new connection (%d, %d): the cache was dropped by another client's failure", pidA, pidD))
	} else {
		notes = append(notes, "another application_name: a connection of its own; the same key again: the cached one")
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkPoolPassword(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE b9pl LOGIN PASSWORD 'b9pl'; EXCEPTION WHEN duplicate_object THEN ALTER ROLE b9pl PASSWORD 'b9pl'; END $$`); err != nil {
		return false, "", err
	}
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{"max_age": "3s"}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("b9pl", "b9pl", "")
	if err != nil {
		return false, "", err
	}
	a.Close(e.ctx)
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "ALTER ROLE b9pl PASSWORD 'b9pl2'"); err != nil {
		return false, "", err
	}
	var notes []string
	if b, err := e.poolConn("b9pl", "b9pl", ""); err == nil {
		b.Close(e.ctx)
		notes = append(notes, "old password right after the change: the cached connection (the window max_age leaves, README)")
	} else {
		notes = append(notes, "old password right after the change: refused already")
	}
	time.Sleep(3500 * time.Millisecond)
	ok := true
	if c, err := e.poolConn("b9pl", "b9pl", ""); err == nil {
		c.Close(e.ctx)
		ok = false
		notes = append(notes, "WRONG old password past max_age: let in")
	} else {
		notes = append(notes, "old password past max_age: "+firstErrLine(err))
	}
	if d, err := e.poolConn("b9pl", "b9pl2", ""); err != nil {
		ok = false
		notes = append(notes, "WRONG new password: "+firstErrLine(err))
	} else {
		d.Close(e.ctx)
		notes = append(notes, "new password: let in")
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkPoolCancel(e *env) (bool, string, error) {
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	pidA := backendPID(e.ctx, a)
	keyPID, keySecret := a.PgConn().PID(), append([]byte{}, a.PgConn().SecretKey()...)
	a.Close(e.ctx)
	time.Sleep(300 * time.Millisecond)
	b, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	if pid := backendPID(e.ctx, b); pid != pidA {
		return false, fmt.Sprintf("the cache was not used: %d, then %d", pidA, pid), nil
	}
	done := make(chan error, 1)
	go func() { _, err := b.Exec(e.ctx, "SELECT pg_sleep(2)"); done <- err }()
	time.Sleep(400 * time.Millisecond)
	c, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", e.port))
	if err != nil {
		return false, "", err
	}
	buf, _ := (&pgproto3.CancelRequest{ProcessID: keyPID, SecretKey: keySecret}).Encode(nil)
	_, _ = c.Write(buf)
	c.Close()
	ok := true
	var notes []string
	if err := <-done; err != nil {
		ok = false
		notes = append(notes, "WRONG client A's old key cancelled client B's statement: "+firstErrLine(err))
	} else {
		notes = append(notes, "client A's old key: client B's pg_sleep ran to its end")
	}
	// client B's own key still cancels its own statement
	go func() { _, err := b.Exec(e.ctx, "SELECT pg_sleep(5)"); done <- err }()
	time.Sleep(400 * time.Millisecond)
	_ = b.PgConn().CancelRequest(e.ctx)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "57014") {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG client B's own cancel: %v", err))
	} else {
		notes = append(notes, "client B's own key: cancelled (57014)")
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkPoolDirty(e *env) (bool, string, error) {
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	pidA := backendPID(e.ctx, a)
	for _, sql := range []string{"BEGIN", "INSERT INTO users (name) VALUES ('pl6')", "SELECT pg_advisory_xact_lock(777)"} {
		if _, err := a.Exec(e.ctx, sql); err != nil {
			return false, "", fmt.Errorf("%s: %w", sql, err)
		}
	}
	a.Close(e.ctx) // inside the transaction
	time.Sleep(300 * time.Millisecond)
	b, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	pidB := backendPID(e.ctx, b)
	var rows, locks string
	_ = b.QueryRow(e.ctx, "SELECT count(*)::text FROM users WHERE name = 'pl6'").Scan(&rows)
	_ = b.QueryRow(e.ctx, "SELECT count(*)::text FROM pg_locks WHERE locktype = 'advisory' AND objid = 777").Scan(&locks)
	ok := pidB != pidA && rows == "0" && locks == "0"
	return ok, fmt.Sprintf("client A left inside a transaction on process %d; client B on process %d; A's row seen: %s, its lock held: %s", pidA, pidB, rows, locks), nil
}

func checkPoolCount(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	sessions := func(host string) (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, dsn(host, "postgres", "postgres"), "SELECT sessions FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	const clients = 40
	run := func(pool bool) (cloud, org int64, err error) {
		extra := map[string]any{"upstream_pool": false} // on by default (27.10)
		if pool {
			extra["upstream_pool"] = map[string]any{}
		}
		stop, err := e.poolProxy(extra)
		if err != nil {
			return 0, 0, err
		}
		defer stop()
		c0, err := sessions(e.cloudHost)
		if err != nil {
			return 0, 0, err
		}
		o0, err := sessions("customer")
		if err != nil {
			return 0, 0, err
		}
		for i := 0; i < clients; i++ { // connect, one query of the moved organization (FDW), go
			c, err := e.poolConn("app", "app", "")
			if err != nil {
				return 0, 0, err
			}
			var n int
			if err := c.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = 1").Scan(&n); err != nil {
				c.Close(e.ctx)
				return 0, 0, err
			}
			c.Close(e.ctx)
			time.Sleep(20 * time.Millisecond)
		}
		c1, err := sessions(e.cloudHost)
		if err != nil {
			return 0, 0, err
		}
		o1, err := sessions("customer")
		if err != nil {
			return 0, 0, err
		}
		return c1 - c0, o1 - o0, nil
	}
	cOff, oOff, err := run(false)
	if err != nil {
		return false, "", err
	}
	cOn, oOn, err := run(true)
	if err != nil {
		return false, "", err
	}
	ok := cOff >= clients && cOn <= 3 && oOn <= 3
	return ok, fmt.Sprintf("%d clients, one statement each: new connections to the cloud %d without the cache, %d with; to the organization's database %d without, %d with", clients, cOff, cOn, oOff, oOn), nil
}

// poolDirectSetup: organization 1 moved and in fdw+direct (as directSetup), the proxy with
// both caches when pools is set.
func (e *env) poolDirectSetup(pools bool) (func(), error) {
	if err := e.enable(); err != nil {
		return nil, err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return nil, err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return nil, fmt.Errorf("org-profile fdw+direct: %s", lastErr(out))
	}
	return e.poolDirectProxy(pools)
}

func (e *env) poolDirectProxy(pools bool) (func(), error) {
	extra := map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}},
		"upstream_pool": false} // on by default (27.10)
	if pools {
		extra["upstream_pool"] = map[string]any{}
		extra["direct_pool"] = map[string]any{}
	}
	return e.proxyCfg(map[string]string{"1": e.appCustomer()}, extra)
}

// directBackends: the server processes of the direct path's role in the organization's database.
func (e *env) directBackends() string {
	rows, err := e.customerRows("SELECT pid::text FROM pg_stat_activity WHERE datname = current_database() AND usename = 'lokadb_dml' ORDER BY pid")
	if err != nil {
		return "error: " + err.Error()
	}
	return strings.Join(rows, ",")
}

// PL7. The pool of direct connections (block 10, item 7): client A's session repeats its
// statement_timeout of 300 ms on its direct connection; client B's session gets the same
// connection (the same process in the organization's database) after it, and its direct
// statement of about two seconds runs to its end. On a pool without the reset it is cancelled.
func checkDirectPoolState(e *env) (bool, string, error) {
	stop, err := e.poolDirectSetup(true)
	if err != nil {
		return false, "", err
	}
	defer stop()
	const read = "SELECT count(*) FROM documents WHERE organization_id = 1"
	const heavy = "SELECT (SELECT count(*) FROM generate_series(1, 20000000)), count(*) FROM documents WHERE organization_id = 1"
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	for _, sql := range []string{"SET statement_timeout = '300ms'", read} {
		if _, err := a.PgConn().Exec(e.ctx, sql).ReadAll(); err != nil {
			return false, "", fmt.Errorf("client A, %s: %w", sql, err)
		}
	}
	a.Close(e.ctx)
	time.Sleep(500 * time.Millisecond)
	before := e.directBackends()
	b, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	_, herr := b.PgConn().Exec(e.ctx, heavy).ReadAll()
	b.Close(e.ctx)
	time.Sleep(500 * time.Millisecond)
	after := e.directBackends()
	direct := directCount(e.proxyRoutes(), "1")
	ok := herr == nil && before != "" && !strings.Contains(before, ",") && before == after && direct >= 2
	verdict := "ran to its end"
	if herr != nil {
		verdict = "WRONG " + firstErrLine(herr)
	}
	return ok, fmt.Sprintf("direct statements of organization 1: %d; the direct path's process in the organization's database after client A: %s, after client B: %s; client B's statement of ~2 s under A's statement_timeout of 300 ms: %s", direct, before, after, verdict), nil
}

// PL9. 40 clients, one direct read each (fdw+direct): new connections to the organization's
// database and the cloud without the pools and with both.
func checkDirectPoolCount(e *env) (bool, string, error) {
	stop, err := e.poolDirectSetup(false)
	if err != nil {
		return false, "", err
	}
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	sessions := func(host string) (int64, error) {
		time.Sleep(1500 * time.Millisecond)
		v, err := one(e.ctx, dsn(host, "postgres", "postgres"), "SELECT sessions FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	const clients = 40
	run := func() (cloud, org int64, direct int, err error) {
		c0, err := sessions(e.cloudHost)
		if err != nil {
			return 0, 0, 0, err
		}
		o0, err := sessions("customer")
		if err != nil {
			return 0, 0, 0, err
		}
		d0 := directCount(e.proxyRoutes(), "1")
		for i := 0; i < clients; i++ {
			c, err := e.poolConn("app", "app", "")
			if err != nil {
				return 0, 0, 0, err
			}
			if _, err := c.PgConn().Exec(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = 1").ReadAll(); err != nil {
				c.Close(e.ctx)
				return 0, 0, 0, err
			}
			c.Close(e.ctx)
			time.Sleep(20 * time.Millisecond)
		}
		c1, err := sessions(e.cloudHost)
		if err != nil {
			return 0, 0, 0, err
		}
		o1, err := sessions("customer")
		if err != nil {
			return 0, 0, 0, err
		}
		return c1 - c0, o1 - o0, directCount(e.proxyRoutes(), "1") - d0, nil
	}
	cOff, oOff, dOff, err := run()
	if err != nil {
		return false, "", err
	}
	stop()
	stopped = true
	stop2, err := e.poolDirectProxy(true)
	if err != nil {
		return false, "", err
	}
	defer stop2()
	cOn, oOn, dOn, err := run()
	if err != nil {
		return false, "", err
	}
	ok := dOff >= clients && dOn >= clients && oOff >= clients && oOn <= 3 && cOn <= 3
	return ok, fmt.Sprintf("%d clients, one direct read each (direct: %d without the pools, %d with): new connections to the organization's database %d without, %d with; to the cloud %d without, %d with", clients, dOff, dOn, oOff, oOn, cOff, cOn), nil
}
