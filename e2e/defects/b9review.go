package main

// Block 9, findings of the protocol review (tasks/review/protocol.md, PR-n).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "PR7", name: "the cloud connection cache: after a password change SIGHUP drops the cached connections, and the old password is refused", run: checkPoolSighup},
		check{id: "PR9", name: "a full proxy (max_connections) still takes a cancel request: the runaway statement stops", run: checkCancelWhenFull},
		check{id: "PR10", name: "a NOTICE of a running statement reaches the client at once, not with the statement's end", run: checkNoticeAtOnce},
		check{id: "PF1", name: "the metadata are shared by default: a connection costs the cloud a handful of transactions, and a hundred sessions' reload every metaTTL is one read, not one each", run: checkMetaSharedByDefault},
		check{id: "PF3", name: "the proxy's own statements (set_config of lokadb.org, the pause lock) are prepared once on the connection: the server's counter of their uses grows with the client's statements", run: checkServiceStatementsOnce},
		check{id: "PR14", name: "the unnamed statement a direct batch took is used again by a Bind with no Parse of its own: it runs (it was 26000)", run: checkUnnamedUsedAgain},
		check{id: "CL1", name: "connect latency: a new connection and its first query through the proxy against straight to Postgres, and where the proxy's time goes (phases from its Debug log)", run: checkConnectLatency},
		check{id: "PR15", name: "the metadata cache is keyed by the role the metadata is read under: a login behind options=-c role=x does not hand its metadata to the sessions that log in as it plainly", run: checkMetaCacheCurrentRole},
		check{id: "PR11", name: "the proxy refuses a session that sees past row level security behind its login: a superuser behind options=-c role=app, a member of a BYPASSRLS role, a REPLICATION role", run: checkRoleBehindLogin},
	)
}

// PR-11. The role check looked at current_user: a superuser logging in with options=-c role=app
// passed (SET ROLE postgres: back to the login, a superuser; RESET ROLE would stay in app, the
// startup value being its default), so did a login role that may SET ROLE into a BYPASSRLS role,
// and a REPLICATION role (pg_logical_slot_get_changes streams every organization's changes).
func checkRoleBehindLogin(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	admin := dsn(e.cloudHost, "postgres", "postgres")
	for _, s := range []string{
		`DO $$ BEGIN CREATE ROLE b911bypass NOLOGIN BYPASSRLS; EXCEPTION WHEN duplicate_object THEN END $$`,
		`DO $$ BEGIN CREATE ROLE b911member LOGIN PASSWORD 'b911' NOSUPERUSER NOBYPASSRLS; EXCEPTION WHEN duplicate_object THEN END $$`,
		`DO $$ BEGIN CREATE ROLE b911repl LOGIN REPLICATION PASSWORD 'b911'; EXCEPTION WHEN duplicate_object THEN END $$`,
		`GRANT b911bypass TO b911member`,
	} {
		if err := execSQL(e.ctx, admin, s); err != nil {
			return false, "", err
		}
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO b911member, b911repl", "GRANT USAGE ON SCHEMA lokadb TO b911member, b911repl"); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var notes []string
	ok := true
	for _, c := range []struct{ name, user, pw, opts, want, become string }{
		{"a superuser behind -c role=app", "postgres", "pw", " options='-c role=app'", "was logged in as postgres", "SET ROLE postgres"},
		{"a member of a BYPASSRLS role", "b911member", "b911", "", "can SET ROLE to b911bypass", "SET ROLE b911bypass"},
		{"a REPLICATION role", "b911repl", "b911", "", "REPLICATION", "SELECT 1"},
	} {
		d := fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable%s", e.port, e.db, c.user, c.pw, c.opts)
		conn, err := pgconn.Connect(e.ctx, d)
		if err == nil {
			// what the session is after it: a role that sees past the policies
			_, _ = conn.Exec(e.ctx, c.become).ReadAll()
			past := "?"
			if rs, err := conn.Exec(e.ctx, "SELECT (rolsuper OR rolbypassrls OR rolreplication)::text FROM pg_roles WHERE rolname = current_user").ReadAll(); err == nil && len(rs) == 1 && len(rs[0].Rows) == 1 {
				past = string(rs[0].Rows[0][0])
			}
			conn.Close(e.ctx)
			ok = false
			notes = append(notes, fmt.Sprintf("WRONG %s let in: after %q the session's role sees past row level security: %s", c.name, c.become, past))
			continue
		}
		if strings.Contains(err.Error(), c.want) && strings.Contains(err.Error(), "bypasses row level security") {
			notes = append(notes, c.name+" refused: "+firstErrLine(err))
		} else {
			ok = false
			notes = append(notes, "WRONG "+c.name+": "+firstErrLine(err))
		}
	}
	// the application role, plain, still gets in
	if c, err := e.viaProxy(); err != nil {
		ok = false
		notes = append(notes, "WRONG app refused: "+firstErrLine(err))
	} else {
		c.Close(e.ctx)
	}
	return ok, strings.Join(notes, "; "), nil
}

// PR-15. The cache's key was the login role. b9mc_part (rights on the parent alone, not on
// organization 1's partition) logs in with options=-c role=b9mc_full (it may SET ROLE to it,
// granted below, NOINHERIT: it has no right of b9mc_full's of its own): the metadata is read under b9mc_full, which reads the partition, so a read of
// organization 1 is aimed at it; the entry was kept under b9mc_part, and b9mc_part's next plain
// session got it: its read went to the partition it has no right on (42501, as MC1's).
func checkMetaCacheCurrentRole(e *env) (bool, string, error) {
	part, err := e.metaCacheRoles()
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON "+part+" FROM b9mc_part"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "ALTER ROLE b9mc_part NOINHERIT", "GRANT b9mc_full TO b9mc_part"); err != nil {
		return false, "", err
	}
	stop, err := e.metaCacheProxy(map[string]any{"meta_cache": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	const q = "SELECT count(*)::text FROM documents WHERE organization_id = 1"
	ask := func(extra string) string {
		c, err := e.poolConn("b9mc_part", "b9mc_part", extra)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		defer c.Close(e.ctx)
		var v string
		if err := c.QueryRow(e.ctx, q).Scan(&v); err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	behind := ask("options='-c role=b9mc_full'")
	plain := ask("")
	ok := behind == "100" && plain == "100"
	return ok, fmt.Sprintf("organization 1's documents by b9mc_part (the parent alone) behind -c role=b9mc_full: %s (want 100); then by b9mc_part plainly: %s (want 100, not 42501 from the other role's metadata)", behind, plain), nil
}

// PR-7. The cache of cloud connections keeps a connection by the HMAC of its password: after
// ALTER ROLE … PASSWORD the old password still matched a cached connection and let in, until
// max_age (5 minutes). SIGHUP drops the cache; the old password then meets Postgres.
func checkPoolSighup(e *env) (bool, string, error) {
	admin := dsn(e.cloudHost, "postgres", "postgres")
	if err := execSQL(e.ctx, admin, `DO $$ BEGIN CREATE ROLE b97 LOGIN PASSWORD 'old'; EXCEPTION WHEN duplicate_object THEN END $$`, "ALTER ROLE b97 PASSWORD 'old'"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO b97", "GRANT USAGE ON SCHEMA lokadb TO b97"); err != nil {
		return false, "", err
	}
	stop, err := e.poolProxy(map[string]any{"upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	use := func(pw string) string {
		c, err := e.poolConn("b97", pw, "")
		if err != nil {
			return "refused: " + firstErrLine(err)
		}
		defer c.Close(e.ctx)
		var n string
		if err := c.QueryRow(e.ctx, "SELECT 1::text").Scan(&n); err != nil {
			return "error: " + firstErrLine(err)
		}
		return "let in"
	}
	first := use("old") // its connection goes to the cache
	time.Sleep(300 * time.Millisecond)
	if err := execSQL(e.ctx, admin, "ALTER ROLE b97 PASSWORD 'new'"); err != nil {
		return false, "", err
	}
	window := use("old") // the cached connection of the old password: the documented window
	time.Sleep(300 * time.Millisecond)
	if e.proxyProc == nil {
		return false, "", fmt.Errorf("the proxy's process is not known")
	}
	if err := e.proxyProc.Signal(syscall.SIGHUP); err != nil {
		return false, "", err
	}
	time.Sleep(500 * time.Millisecond)
	oldAfter := use("old")
	newAfter := use("new")
	ok := first == "let in" && strings.HasPrefix(oldAfter, "refused") && strings.Contains(oldAfter, "28P01") && newAfter == "let in"
	return ok, fmt.Sprintf("the old password before the change: %s; right after ALTER ROLE (cached connection, the documented window): %s; after SIGHUP the old password: %s (want refused, 28P01), the new one: %s (want let in)", first, window, oldAfter, newAfter), nil
}

// PR-9. A proxy at max_connections answered a cancel request with 53300 (it takes a slot like
// any connection) and the statement it was meant to stop ran on. The cancel of a session that
// holds the only slot but one now gets through.
func checkCancelWhenFull(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.poolProxy(map[string]any{"max_connections": 2})
	if err != nil {
		return false, "", err
	}
	defer stop()
	run, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer run.Close(e.ctx)
	hold, err := pgconn.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil { // the second slot, taken by an idle client: the proxy is full
		return false, "", err
	}
	defer hold.Close(e.ctx)
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		_, err := run.Exec(e.ctx, "SELECT pg_sleep(20)")
		done <- err
	}()
	time.Sleep(time.Second)
	cctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	cerr := run.PgConn().CancelRequest(cctx)
	var qerr error
	select {
	case qerr = <-done:
	case <-time.After(8 * time.Second):
		qerr = fmt.Errorf("still running after 8 s")
	}
	took := time.Since(start).Round(100 * time.Millisecond)
	ok := qerr != nil && strings.Contains(qerr.Error(), "57014")
	return ok, fmt.Sprintf("with the proxy full (2 of 2): the cancel request %v; the statement ended after %s with %s (want 57014, within the 8 s)", cerr, took, firstErrLineOr(qerr)), nil
}

// PR-10. A NOTICE raised by a statement that then runs for seconds was buffered by the proxy
// up to the statement's end; Postgres sends it at once (a migration's progress, RAISE NOTICE in a
// batch function).
func checkNoticeAtOnce(e *env) (bool, string, error) {
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var at time.Time
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
		if n.Message == "b910-progress" && at.IsZero() {
			at = time.Now()
		}
	}
	c, err := pgconn.ConnectConfig(e.ctx, cfg)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	start := time.Now()
	if _, err := c.Exec(e.ctx, "DO $$ BEGIN RAISE NOTICE 'b910-progress'; PERFORM pg_sleep(3); END $$").ReadAll(); err != nil {
		return false, "", err
	}
	total := time.Since(start)
	if at.IsZero() {
		return false, "no NOTICE came", nil
	}
	got := at.Sub(start)
	return got < total-time.Second, fmt.Sprintf("the NOTICE arrived %s after the statement began, the statement took %s (want the notice in well under that: the statement's last second not waited for)", got.Round(10*time.Millisecond), total.Round(10*time.Millisecond)), nil
}

// PF1. Every session read the lokadb metadata itself, at its connect (21 queries) and again every
// metaTTL, before a statement of its own; at a thousand organizations that is 0.3-1.8 s of the
// cloud's work each time. The proxy shares them now by default (meta_cache), and a session that
// is due a look uses what a session verified a moment ago. Counted in the cloud's transactions
// (every query of the proxy's own outside a transaction is one) for 40 clients that connect and
// ask once, and for 20 sessions that stay past metaTTL and ask again.
func checkMetaSharedByDefault(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	xacts := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "SELECT xact_commit::text FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	stop, err := e.metaCacheProxy(map[string]any{}) // no meta_cache section: the default
	if err != nil {
		return false, "", err
	}
	defer stop()
	const q = "SELECT count(*) FROM documents WHERE organization_id = 1"
	// 1. 40 clients, each connects, asks once, leaves
	x0, err := xacts()
	if err != nil {
		return false, "", err
	}
	for i := 0; i < 40; i++ {
		c, err := e.poolConn("app", "app", "")
		if err != nil {
			return false, "", err
		}
		if _, err := c.Exec(e.ctx, q); err != nil {
			c.Close(e.ctx)
			return false, "", err
		}
		c.Close(e.ctx)
	}
	x1, err := xacts()
	if err != nil {
		return false, "", err
	}
	perConnect := float64(x1-x0) / 40
	// 2. 20 sessions that stay past metaTTL (30 s) and ask again
	var conns []*pgx.Conn
	for i := 0; i < 20; i++ {
		c, err := e.poolConn("app", "app", "")
		if err != nil {
			return false, "", err
		}
		conns = append(conns, c)
		if _, err := c.Exec(e.ctx, q); err != nil {
			return false, "", err
		}
	}
	time.Sleep(33 * time.Second)
	for _, c := range conns {
		if _, err := c.Exec(e.ctx, q); err != nil {
			return false, "", err
		}
	}
	for _, c := range conns {
		c.Close(e.ctx)
	}
	x2, err := xacts()
	if err != nil {
		return false, "", err
	}
	perSession := float64(x2-x1) / 20 // connect, a statement, past metaTTL a statement again
	ok := perConnect < 12 && perSession < 30
	return ok, fmt.Sprintf("40 clients that connect and ask once: %.1f of the cloud's transactions a client (want under 12; 28 when every connection reads the metadata); 20 sessions held past metaTTL: %.1f a session over connect and two statements (want under 30; about 60 when every session reads again at metaTTL)", perConnect, perSession), nil
}

// PF3. set_config of lokadb.org and the pause lock were Closed and Parsed again with every Bind
// of every client statement: the cloud parsed and planned them each time. Now one Parse per
// connection: pg_prepared_statements counts the plans each use took, and the count grows with
// the client's statements (it was 1: the Parse of the last use).
func checkServiceStatementsOnce(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	const runs = 20
	for i := 0; i < runs; i++ {
		var n int64
		// organizations alternate: the proxy sets lokadb.org and takes the pause lock for each
		if err := c.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = $1", 1+i%2).Scan(&n); err != nil {
			return false, "", err
		}
	}
	rows, err := c.Query(e.ctx, "SELECT name, (generic_plans + custom_plans)::int FROM pg_prepared_statements WHERE name IN ('lokadb_s', 'lokadb_ls') ORDER BY name")
	if err != nil {
		return false, "", err
	}
	got := map[string]int{}
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			return false, "", err
		}
		got[name] = n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return false, "", err
	}
	ok := got["lokadb_s"] >= runs && got["lokadb_ls"] >= runs
	return ok, fmt.Sprintf("after %d statements of alternating organizations: lokadb_s used %d times (set_config), lokadb_ls %d times (the pause lock) (want at least %d each: prepared once; 1 when Parsed every time)", runs, got["lokadb_s"], got["lokadb_ls"], runs), nil
}

// PR-14. Parse "", Bind, Execute, Sync of a read of organization 1 goes to its database (the
// cloud never has the statement); Bind "", Execute, Sync with no Parse after it is legal, and
// the proxy answered 26000. It Parses the remembered text in the cloud first now.
func checkUnnamedUsedAgain(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	conn, err := pgconn.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	defer conn.Close(e.ctx)
	const q = "SELECT count(*)::text FROM document_versions WHERE organization_id = $1"
	n := directCount(e.proxyRoutes(), "1")
	r := conn.ExecParams(e.ctx, q, [][]byte{[]byte("1")}, nil, nil, nil).Read() // Parse "", Bind, Execute, Sync
	if r.Err != nil || len(r.Rows) != 1 {
		return false, "", fmt.Errorf("the first batch: %v", r.Err)
	}
	first, onDirect := string(r.Rows[0][0]), directCount(e.proxyRoutes(), "1")-n
	// Bind "", Execute, Sync: no Parse
	again := conn.ExecPrepared(e.ctx, "", [][]byte{[]byte("1")}, nil, nil).Read()
	got := "error: " + firstErrLineOr(again.Err)
	if again.Err == nil && len(again.Rows) == 1 {
		got = string(again.Rows[0][0])
	}
	want, _ := one(e.ctx, e.cloud(), "SELECT count(*)::text FROM document_versions WHERE organization_id = 1")
	ok := onDirect == 1 && got == want && first == want
	return ok, fmt.Sprintf("the batch with a Parse of its own: %s (on the direct path %d, want 1); the same statement bound again with no Parse: %s (want %s, not 26000)", first, onDirect, got, want), nil
}

// CL1. Block 14's trial: a new connection through the proxy 22-25 ms and its first query 8-31 ms,
// straight to Postgres 4.6 ms and 1.2 ms (PHP opens a connection for every request). This
// measures the same on the defects stand and splits the proxy's part by the phases of its own
// Debug line "connect" (server.go): the client's startup, the password, the connection to
// Postgres, the pooler and role checks, LISTEN, the metadata (the stamp and the read), the reply.
func checkConnectLatency(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	const rounds = 200
	median := func(d []time.Duration) time.Duration {
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		return d[len(d)/2]
	}
	// connect and ask: the time of each part
	measure := func(dsnStr string) (conn, first time.Duration, err error) {
		var cs, fs []time.Duration
		for i := 0; i < rounds; i++ {
			t0 := time.Now()
			c, err := pgx.Connect(e.ctx, dsnStr)
			if err != nil {
				return 0, 0, err
			}
			t1 := time.Now()
			var n int64
			if err := c.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = 1").Scan(&n); err != nil {
				c.Close(e.ctx)
				return 0, 0, err
			}
			t2 := time.Now()
			c.Close(e.ctx)
			cs, fs = append(cs, t1.Sub(t0)), append(fs, t2.Sub(t1))
		}
		return median(cs), median(fs), nil
	}
	appDSN := func(port int, db string) string {
		return fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", port, db)
	}
	dc, df, err := measure(dsn(e.cloudHost, e.db, "app"))
	if err != nil {
		return false, "", err
	}
	_ = os.Setenv("LOKADB_DEBUG", "1") // the proxy's Debug line "connect"
	defer os.Unsetenv("LOKADB_DEBUG")
	var notes []string
	notes = append(notes, fmt.Sprintf("straight to Postgres: connect %s, first query %s", dc.Round(10*time.Microsecond), df.Round(10*time.Microsecond)))
	var proxyTotal time.Duration
	for _, mode := range []struct {
		name  string
		extra map[string]any
	}{{"proxy, no caches (meta_cache off)", map[string]any{"meta_cache": map[string]any{"off": true}, "upstream_pool": false}}, {"proxy, defaults", map[string]any{}},
		{"proxy, defaults and upstream_pool", map[string]any{"upstream_pool": map[string]any{}}},
		{"proxy, upstream_pool and meta_cache.verify 1s", map[string]any{"upstream_pool": map[string]any{}, "meta_cache": map[string]any{"verify": "1s"}}}} {
		stop, err := e.metaCacheProxy(mode.extra)
		if err != nil {
			return false, "", err
		}
		conn, first, err := measure(appDSN(e.port, e.db))
		if err != nil {
			stop()
			return false, "", err
		}
		stop()
		if mode.name == "proxy, defaults" {
			proxyTotal = conn + first
		}
		phases := connectPhases(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
		notes = append(notes, fmt.Sprintf("%s: connect %s, first query %s; the proxy's phases (median µs): %s", mode.name, conn.Round(10*time.Microsecond), first.Round(10*time.Microsecond), phases))
	}
	ok := proxyTotal < 40*time.Millisecond // a bound, not a goal: the numbers are the result
	return ok, strings.Join(notes, "; "), nil
}

var phaseRe = regexp.MustCompile(`(\w+)_us=(\d+)`)

// connectPhases: the median of each phase over the "connect" lines of the proxy's log.
func connectPhases(logFile string) string {
	b, err := os.ReadFile(logFile)
	if err != nil {
		return "no log"
	}
	byPhase := map[string][]int{}
	var order []string
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.Contains(line, "msg=connect ") {
			continue
		}
		for _, m := range phaseRe.FindAllStringSubmatch(line, -1) {
			v, _ := strconv.Atoi(m[2])
			if _, seen := byPhase[m[1]]; !seen {
				order = append(order, m[1])
			}
			byPhase[m[1]] = append(byPhase[m[1]], v)
		}
	}
	var out []string
	for _, k := range order {
		v := byPhase[k]
		sort.Ints(v)
		out = append(out, fmt.Sprintf("%s %d", k, v[len(v)/2]))
	}
	if len(out) == 0 {
		return "no connect lines"
	}
	return strings.Join(out, ", ")
}
