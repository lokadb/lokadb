package main

// Block 9 (tasks/BACKLOG.md): the proxy's protocol handling, against real Postgres.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "B9.1", name: "a connection opened before enable routes right after it, not 30 s later", run: checkRightAfterEnable},
		check{id: "B9.9", name: "a client that cuts its pipelines with Flush learns of enable", run: checkFlushClientAfterEnable},
		check{id: "B9.2", name: "a pipeline cut by Flush: child rows of two organizations in one implicit transaction", run: checkLookupInPipeline},
		check{id: "B9.3", name: "a pgx batch of 600 child rows (1200 messages, one Sync) routes every row", run: checkLongBatch},
		check{id: "B9.7", name: "LISTEN/NOTIFY and cursors (FETCH, WITH HOLD, portal row limit) through the proxy", run: checkListenCursors},
		check{id: "B9.8", name: "a query string that sets lokadb.org itself cannot place a child row in another organization", run: checkAppSetsOrg},
		check{id: "B9.10", name: "enable forces RLS on the owner; the proxy refuses a role that bypasses it and lokadb.* in the startup parameters", run: checkBypassRLS},
	)
}

// B9.8. The proxy sets lokadb.org right before a statement on LokaDB tables; a child row
// (document_versions) takes its organization from it. A string with SET lokadb.org got no
// set_config from the proxy at all, and a set_config('lokadb.org', …) in the string or in the
// statement overrode the proxy's: the version of organization 1's document landed in
// organization 2.
func checkAppSetsOrg(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
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
	ins := fmt.Sprintf("INSERT INTO document_versions (document_id, content) VALUES (%s, 'B98-%%s')", doc)
	var notes []string
	refused := 0
	try := func(name string, run func() error) {
		if err := run(); err != nil {
			refused++
			notes = append(notes, name+": refused ("+firstErrLine(err)+")")
		} else {
			notes = append(notes, name+": ran")
		}
	}
	try("SET; INSERT", func() error {
		_, err := simple(e.ctx, c, "SET lokadb.org = '2'; "+fmt.Sprintf(ins, "set"))
		return err
	})
	try("set_config; INSERT", func() error {
		_, err := simple(e.ctx, c, "SELECT set_config('lokadb.org', '2', false); "+fmt.Sprintf(ins, "fn"))
		return err
	})
	try("INSERT … set_config (extended)", func() error {
		return c.ExecParams(e.ctx, "INSERT INTO document_versions (document_id, content) SELECT $1::bigint, 'B98-ext' || set_config('lokadb.org', '2', false)",
			[][]byte{[]byte(doc)}, nil, nil, nil).Read().Err
	})
	// alone, the application's SET still runs, and the proxy's own value holds after it
	try("SET alone, then INSERT", func() error {
		if _, err := simple(e.ctx, c, "SET lokadb.org = '2'"); err != nil {
			return err
		}
		_, err := simple(e.ctx, c, fmt.Sprintf(ins, "after"))
		return err
	})
	misplaced, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content LIKE 'B98-%' AND organization_id IS DISTINCT FROM 1")
	if err != nil {
		return false, "", err
	}
	after, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'B98-after' AND organization_id = 1")
	if err != nil {
		return false, "", err
	}
	notes = append(notes, "rows in another organization: "+misplaced, "row inserted after SET alone, in organization 1: "+after)
	return misplaced == "0" && refused == 3 && after == "1", strings.Join(notes, "; "), nil
}

// B9.1. A session learnt of enable only when it read the metadata again, up to 30 s (metaTTL)
// later: meanwhile its statements went through unanalysed. A child row got no organization
// (refused by the database), and a statement naming none ran in every organization's database
// despite the fanout policy. enable notifies lokadb_meta now; the proxy listens to it.
func checkRightAfterEnable(e *env) (bool, string, error) {
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents"); err != nil {
		return false, "", fmt.Errorf("before enable: %w", err)
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var notes []string
	// Until enable notifies lokadb_meta itself (block 3), the check sends what it will send at
	// its commit: this checks the proxy's side. B9_ENABLE_NOTIFIES=1 checks enable's own.
	if os.Getenv("B9_ENABLE_NOTIFIES") == "" {
		if err := execSQL(e.ctx, e.cloud(), "NOTIFY lokadb_meta"); err != nil {
			return false, "", err
		}
		notes = append(notes, "NOTIFY lokadb_meta sent by the check after enable")
	}
	time.Sleep(300 * time.Millisecond) // the notification's way, far below metaTTL
	ok := true
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents"); err != nil && strings.Contains(err.Error(), "fanout") {
		notes = append(notes, "statement naming no organization: refused by the policy")
	} else {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG statement naming no organization: not refused (err %v)", err))
	}
	if _, err := simple(e.ctx, c, fmt.Sprintf("INSERT INTO document_versions (document_id, content) VALUES (%s, 'B91')", doc)); err != nil {
		ok = false
		notes = append(notes, "WRONG child row: "+firstErrLine(err))
	}
	org, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ','), 'none') FROM document_versions WHERE content = 'B91'")
	if err != nil {
		return false, "", err
	}
	if org != "1" {
		ok = false
	}
	notes = append(notes, "child row of organization 1's document stored under: "+org)
	return ok, strings.Join(notes, "; "), nil
}

// B9.9. The metadata was read again only at a batch ending with Sync after a synced one. A
// client that cuts each pipeline with Flush and sends its Sync alone never got there: opened
// before enable, it went on unrouted for good (a child row got no organization), not only for
// metaTTL. The check waits past metaTTL, so a notification from enable does not matter here.
func checkFlushClientAfterEnable(e *env) (bool, string, error) {
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
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
	hj, err := c.Hijack()
	if err != nil {
		return false, "", err
	}
	defer hj.Conn.Close()
	pipeline := func(sql string, args ...string) (string, error) {
		r, err := flushPiece(hj, sql, args...)
		if err != nil {
			return "", err
		}
		return r, syncNow(hj)
	}
	if r, err := pipeline("SELECT 1"); err != nil || strings.HasPrefix(r, "ERROR") {
		return false, "", fmt.Errorf("before enable: %v %v", r, err)
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	time.Sleep(32 * time.Second) // past metaTTL
	var last string
	for i := 0; i < 3; i++ { // a few pipelines: the old proxy never reads the metadata again
		if last, err = pipeline("INSERT INTO document_versions (document_id, content) VALUES ($1, 'B99')", doc); err != nil {
			return false, "", err
		}
	}
	org, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ','), 'none') FROM document_versions WHERE content = 'B99'")
	if err != nil {
		return false, "", err
	}
	return org == "1,1,1", fmt.Sprintf("3 pipelines after enable, last: %s; child rows of organization 1's document stored under: %s", last, org), nil
}

// B9.3 (P3). The proxy held 1000 messages of a pipeline and sent them on with a Flush: in
// those pieces it could not read the refs tables, and a child row went under lokadb.org = ”
// (the database refused it for want of an organization; a read or a change ran in every
// organization's database, past the fanout policy). A pgx batch of 600 inserts is 1200
// messages and one Sync.
func checkLongBatch(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	b := &pgx.Batch{}
	for i := 0; i < 600; i++ {
		b.Queue("INSERT INTO document_versions (document_id, content) VALUES ($1, 'B93')", doc)
	}
	batchErr := c.SendBatch(e.ctx, b).Close()
	org, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(DISTINCT organization_id::text, ','), 'none') || ' / ' || count(*) FROM document_versions WHERE content = 'B93'")
	if err != nil {
		return false, "", err
	}
	detail := "rows stored (organizations / count): " + org
	if batchErr != nil {
		detail = "batch failed: " + firstErrLine(batchErr) + "; " + detail
	}
	return batchErr == nil && org == "1 / 600", detail, nil
}

// flushPiece sends Parse, Bind, Execute and Flush, and reads up to the Execute's result
// (its command tag, or "ERROR: …"): one piece of a pipeline, its implicit transaction left open.
func flushPiece(hj *pgconn.HijackedConn, sql string, args ...string) (string, error) {
	var params [][]byte
	for _, a := range args {
		params = append(params, []byte(a))
	}
	fe := hj.Frontend
	_ = hj.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	fe.Send(&pgproto3.Parse{Query: sql})
	fe.Send(&pgproto3.Bind{Parameters: params})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Flush{})
	if err := fe.Flush(); err != nil {
		return "", err
	}
	for {
		m, err := fe.Receive()
		if err != nil {
			return "", err
		}
		switch m := m.(type) {
		case *pgproto3.CommandComplete:
			return string(m.CommandTag), nil
		case *pgproto3.ErrorResponse:
			return "ERROR: " + m.Message, nil
		}
	}
}

// syncNow sends Sync alone and reads up to ReadyForQuery.
func syncNow(hj *pgconn.HijackedConn) error {
	fe := hj.Frontend
	fe.Send(&pgproto3.Sync{})
	if err := fe.Flush(); err != nil {
		return err
	}
	for {
		m, err := fe.Receive()
		if err != nil {
			return err
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			return nil
		}
	}
}

// B9.2 (1b). Inside a pipeline cut by Flush (an implicit transaction open upstream) the proxy
// could not run its own queries: a simple query would have committed the client's
// transaction. A child row whose organization the refs tables tell went under an empty
// lokadb.org there (the database refused it). The lookup goes through the extended protocol now.
func checkLookupInPipeline(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	d1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	d2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	if err != nil {
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
	hj, err := c.Hijack()
	if err != nil {
		return false, "", err
	}
	defer hj.Conn.Close()
	ins := "INSERT INTO document_versions (document_id, content) VALUES ($1, 'B92')"
	r1, err := flushPiece(hj, ins, d1)
	if err != nil {
		return false, "", err
	}
	r2 := "skipped: the pipeline had failed"
	if !strings.HasPrefix(r1, "ERROR") { // after an error the server answers nothing until Sync
		if r2, err = flushPiece(hj, ins, d2); err != nil {
			return false, "", err
		}
	}
	if err := syncNow(hj); err != nil {
		return false, "", err
	}
	got, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ',' ORDER BY organization_id), 'none') FROM document_versions WHERE content = 'B92'")
	if err != nil {
		return false, "", err
	}
	return got == "1,2", fmt.Sprintf("first piece: %s; second piece: %s; rows stored under organizations: %s", r1, r2, got), nil
}

// B9.7. LISTEN/NOTIFY and cursors had never been run through the proxy. A notification of
// the application's channel reaches it, the proxy's own (lokadb_meta) does not. A cursor of
// organization 1 fetched after a statement of organization 2: FETCH gets lokadb.org cleared, as
// the cursor's own WHERE keeps to its rows. A cursor WITH HOLD is filled at the COMMIT, under
// the lokadb.org then in force: the proxy clears it right before. A portal read with a row
// limit gets its organization back before each Execute.
func checkListenCursors(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	var heard []*pgconn.Notification
	cfg.OnNotification = func(_ *pgconn.PgConn, n *pgconn.Notification) { heard = append(heard, n) }
	c, err := pgconn.ConnectConfig(e.ctx, cfg)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// LISTEN / NOTIFY
	if _, err := simple(e.ctx, c, "LISTEN b97"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "NOTIFY lokadb_meta", "NOTIFY b97, 'hello'"); err != nil {
		return false, "", err
	}
	wctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	err = c.WaitForNotification(wctx)
	cancel()
	switch {
	case err != nil || len(heard) == 0:
		note(false, fmt.Sprintf("no notification: %v", err))
	default:
		note(len(heard) == 1 && heard[0].Channel == "b97" && heard[0].Payload == "hello", fmt.Sprintf("first notification: %s %q (of %d)", heard[0].Channel, heard[0].Payload, len(heard)))
	}
	count := func(sql string) string {
		rs, err := simple(e.ctx, c, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return fmt.Sprint(len(rs[len(rs)-1].Rows))
	}
	ins2 := "INSERT INTO documents (organization_id, user_id, title) VALUES (2, 2, 'B97')"
	// a cursor fetched after a statement of another organization
	if _, err := simple(e.ctx, c, "BEGIN"); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, fmt.Sprintf("DECLARE c1 CURSOR FOR SELECT id FROM document_versions WHERE document_id = %s", doc)); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, ins2); err != nil {
		return false, "", err
	}
	got := count("FETCH ALL FROM c1")
	note(got == "1", "cursor of organization 1 fetched after a statement of organization 2: "+got+" row(s), want 1")
	if _, err := simple(e.ctx, c, "COMMIT"); err != nil {
		return false, "", err
	}
	// WITH HOLD: filled at the COMMIT
	if _, err := simple(e.ctx, c, "BEGIN"); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, fmt.Sprintf("DECLARE h1 CURSOR WITH HOLD FOR SELECT id FROM document_versions WHERE document_id = %s", doc)); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, ins2); err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, "COMMIT"); err != nil {
		return false, "", err
	}
	got = count("FETCH ALL FROM h1")
	note(got == "1", "cursor WITH HOLD of organization 1, a statement of organization 2 before COMMIT: "+got+" row(s), want 1")
	_, _ = simple(e.ctx, c, "CLOSE h1")
	// a portal read with a row limit, another organization's statement in between
	hj, err := c.Hijack()
	if err != nil {
		return false, "", err
	}
	defer hj.Conn.Close()
	fe := hj.Frontend
	_ = hj.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	rows := 0
	read := func(until func(pgproto3.BackendMessage) bool) error {
		for {
			m, err := fe.Receive()
			if err != nil {
				return err
			}
			if _, isRow := m.(*pgproto3.DataRow); isRow {
				rows++
			}
			if er, isErr := m.(*pgproto3.ErrorResponse); isErr {
				return fmt.Errorf("%s", er.Message)
			}
			if until(m) {
				return nil
			}
		}
	}
	isRFQ := func(m pgproto3.BackendMessage) bool { _, y := m.(*pgproto3.ReadyForQuery); return y }
	fe.Send(&pgproto3.Query{String: "BEGIN"})
	_ = fe.Flush()
	if err := read(isRFQ); err != nil {
		return false, "", err
	}
	fe.Send(&pgproto3.Parse{Name: "v", Query: "SELECT id FROM documents WHERE organization_id = $1"})
	fe.Send(&pgproto3.Bind{DestinationPortal: "pv", PreparedStatement: "v", Parameters: [][]byte{[]byte("1")}})
	fe.Send(&pgproto3.Execute{Portal: "pv", MaxRows: 10})
	fe.Send(&pgproto3.Sync{})
	_ = fe.Flush()
	if err := read(isRFQ); err != nil {
		return false, "", err
	}
	fe.Send(&pgproto3.Parse{Query: ins2})
	fe.Send(&pgproto3.Bind{})
	fe.Send(&pgproto3.Execute{})
	fe.Send(&pgproto3.Execute{Portal: "pv", MaxRows: 10})
	fe.Send(&pgproto3.Sync{})
	_ = fe.Flush()
	if err := read(isRFQ); err != nil {
		note(false, "portal with a row limit: "+err.Error())
	} else {
		note(rows == 20, fmt.Sprintf("portal of organization 1 read 10 + 10 rows with a statement of organization 2 between: %d", rows))
	}
	fe.Send(&pgproto3.Query{String: "ROLLBACK"})
	_ = fe.Flush()
	_ = read(isRFQ)
	return ok, strings.Join(notes, "; "), nil
}

// B9.10. The policies lokadb_tenant are the wall behind the proxy's routing. enable sets FORCE
// ROW LEVEL SECURITY, so the tables' owner (app) is held by them too. A superuser or a BYPASSRLS
// role is not: the proxy let it in, and it saw every organization's rows. It refuses it now.
func checkBypassRLS(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	ok := true
	var notes []string
	flags, err := one(e.ctx, e.cloud(), "SELECT string_agg(relname || ' ' || relrowsecurity || '/' || relforcerowsecurity, ', ' ORDER BY relname) FROM pg_class WHERE relname IN ('documents', 'document_versions') AND relkind IN ('r', 'p')")
	if err != nil {
		return false, "", err
	}
	if strings.Contains(flags, "false") || flags == "" {
		ok = false
	}
	notes = append(notes, "row security enabled/forced: "+flags)
	// the owner, straight to the cloud under organization 1: organization 2's rows stay hidden
	// (set before the statement: a set_config in its select list runs after the scan)
	oc, err := pgx.Connect(e.ctx, dsn(e.cloudHost, e.db, "app"))
	if err != nil {
		return false, "", err
	}
	var seen string
	if _, err = oc.Exec(e.ctx, "SELECT set_config('lokadb.org', '1', false)"); err == nil {
		err = oc.QueryRow(e.ctx, "SELECT count(*) FILTER (WHERE organization_id = 2) || '/' || count(*) FROM documents").Scan(&seen)
	}
	oc.Close(e.ctx)
	if err != nil {
		return false, "", err
	}
	seen = "1:" + seen
	if !strings.HasPrefix(seen, "1:0/") {
		ok = false
	}
	notes = append(notes, "owner app under organization 1, rows of organization 2/all: "+strings.TrimPrefix(seen, "1:"))
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE b910 LOGIN BYPASSRLS PASSWORD 'b910'; EXCEPTION WHEN duplicate_object THEN END $$`); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO b910", "GRANT USAGE ON SCHEMA lokadb TO b910"); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	for user, pw := range map[string]string{"b910": "b910", "postgres": "pw"} {
		d := fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable", e.port, e.db, user, pw)
		c, err := pgconn.Connect(e.ctx, d)
		if err == nil {
			c.Close(e.ctx)
			r, qerr := one(e.ctx, d, "SELECT count(DISTINCT organization_id) FROM documents")
			ok = false
			notes = append(notes, fmt.Sprintf("WRONG %s let in, organizations seen: %s %v", user, r, qerr))
			continue
		}
		if strings.Contains(err.Error(), "bypasses row level security") {
			notes = append(notes, user+" refused: "+firstErrLine(err))
		} else {
			ok = false
			notes = append(notes, "WRONG "+user+": "+firstErrLine(err))
		}
	}
	c, err := e.viaProxy()
	if err != nil {
		ok = false
		notes = append(notes, "WRONG app refused: "+firstErrLine(err))
	} else {
		c.Close(e.ctx)
	}
	// lokadb.org in the startup parameters went upstream as it was: the session started under
	// organization 2 while the proxy took it for empty, and what it did not set itself ran so
	d := fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable options='-c lokadb.org=2'", e.port, e.db)
	if c, err := pgconn.Connect(e.ctx, d); err == nil {
		r, qerr := simple(e.ctx, c, "SELECT current_setting('lokadb.org', true)")
		val := ""
		if qerr == nil && len(r) > 0 && len(r[0].Rows) > 0 {
			val = string(r[0].Rows[0][0])
		}
		c.Close(e.ctx)
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG options=-c lokadb.org=2 let in, lokadb.org of the session: %q %v", val, qerr))
	} else if strings.Contains(err.Error(), "startup parameters") {
		notes = append(notes, "options=-c lokadb.org=2 refused")
	} else {
		ok = false
		notes = append(notes, "WRONG options=-c lokadb.org=2: "+firstErrLine(err))
	}
	return ok, strings.Join(notes, "; "), nil
}
