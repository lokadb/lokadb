package proxy

// Protocol tests: a real session between a test client and a fake upstream that answers
// like Postgres does (skips to Sync after an error, ignores Sync in copy-in mode, may fail
// at Sync when the implicit transaction commits).

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type fakeUpstream struct {
	failBegin     bool         // a BEGIN fails (withBegin: the statement after it must not run)
	rfqs          atomic.Int64 // ReadyForQuery sent: the exchanges the server answered
	be            *pgproto3.Backend
	failExec      map[int]bool          // Execute number (1-based, the client's and the proxy's) that fails
	failSync      map[int]bool          // Sync number whose processing (the implicit COMMIT) fails
	delaySync     map[int]time.Duration // wait before answering this Sync
	stmts         map[string]string
	parseOIDs     map[string][]uint32
	portals       map[string]string
	execN, sync   int
	mu            sync.Mutex
	sendMu        sync.Mutex    // the backend is written by run and by notify
	seen          []string      // what ran upstream, in order: the query of each Execute and Query
	simple        []string      // the simple queries alone
	describeTypes []uint32      // Describe of a statement answers ParameterDescription with these (nil: NoData only)
	profile       string        // what lokadb.orgs.profile reads (the direct path's gate)
	denied        bool          // the gate's check of privileges, partitions and policies fails
	delayQuery    time.Duration // wait before answering a simple query
	noticeFirst   bool          // a simple query is answered with a NOTICE first, at once (RAISE NOTICE of a long procedure)
	parsed        []string      // the name of every Parse the server took, in order
	lockErr       string        // SQLSTATE the gate's pause lock fails with (a cancel, lock_timeout)
	dropNext      bool          // close the connection on the next statement, unanswered (an idle drop)
	roleWhy       string        // what the direct role check finds beyond DML ("": nothing)
	marks         []string      // the lokadb comments of its tables (nil: organization 1's, direct_identity.go)
	unresolved    bool          // the gate does not find the tables by their names (another schema)
	extOp         string        // an operator of an extension in the cloud (the gate's operator check fails)
	// onQuery: sees each simple query first; true: the connection breaks there, unanswered
	// (the test closes its side: a COMMIT on its way when it broke)
	onQuery func(q string) bool
	// answer: the rows (or the error) of an Execute whose query it knows (ok); nil or !ok:
	// the answers below
	answer func(q string) (rows [][]string, err string, ok bool)
	// answerSimple: answer also answers simple queries (an error: 23505; rows: a DELETE's)
	answerSimple bool
	shared       *sharedLog // also logs there, as name: query
	name         string
}

func (f *fakeUpstream) ran(q string) {
	f.mu.Lock()
	f.seen = append(f.seen, q)
	f.mu.Unlock()
	if f.shared != nil {
		f.shared.add(f.name + ": " + q)
	}
}

// sharedLog: what several fakes ran, in one order (a direct transaction's two sides).
type sharedLog struct {
	mu  sync.Mutex
	all []string
}

func (l *sharedLog) add(s string) {
	l.mu.Lock()
	l.all = append(l.all, s)
	l.mu.Unlock()
}

func (l *sharedLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.all...)
}

func (f *fakeUpstream) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.seen...)
}

// syncs: the Syncs the fake has had (each an exchange the proxy waited on).
func (f *fakeUpstream) syncs() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sync
}

func (f *fakeUpstream) run() {
	f.sendMu.Lock()
	// this connection's: a new one may replace them while it ends (setBackend)
	be, stmts, portals, parseOIDs := f.be, f.stmts, f.portals, f.parseOIDs
	f.sendMu.Unlock()
	skipping, copyIn, inTx := false, false, false // inTx: a BEGIN by simple query, up to COMMIT/ROLLBACK
	aborted := false                              // a simple query's error in inTx: 25P02 up to ROLLBACK
	copySimple, copyRows := false, 0              // a COPY FROM STDIN by simple query: its rows (lines of CopyData)
	send := func(ms ...pgproto3.BackendMessage) {
		f.sendMu.Lock()
		defer f.sendMu.Unlock()
		for _, m := range ms {
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				f.rfqs.Add(1) // an exchange's end (item 9: round trips counted)
			}
			be.Send(m)
		}
		_ = be.Flush()
	}
	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		switch msg.(type) {
		case *pgproto3.Sync:
			if copyIn {
				continue // ignored in copy-in mode
			}
			f.mu.Lock()
			f.sync++
			f.mu.Unlock()
			if d := f.delaySync[f.sync]; d > 0 {
				time.Sleep(d)
			}
			if !skipping && f.failSync[f.sync] {
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23503", Message: "commit failed"})
			}
			skipping = false
			status := byte('I') // a transaction opened by an Execute of BEGIN (the proxy's cloudRun) stays open
			if inTx {
				status = 'T'
			}
			send(&pgproto3.ReadyForQuery{TxStatus: status})
			continue
		case *pgproto3.Terminate:
			return
		}
		if skipping {
			continue
		}
		switch m := msg.(type) {
		case *pgproto3.Parse:
			stmts[m.Name] = m.Query
			parseOIDs[m.Name] = m.ParameterOIDs
			f.mu.Lock()
			f.parsed = append(f.parsed, m.Name)
			f.mu.Unlock()
			send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			q, ok := stmts[m.PreparedStatement]
			if !ok {
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "26000", Message: fmt.Sprintf("prepared statement %q does not exist", m.PreparedStatement)})
				skipping = true
				continue
			}
			for i, p := range m.Parameters { // the parameters, as the query would read
				v := "NULL"
				if p != nil {
					v = string(p)
				}
				q = strings.Replace(q, fmt.Sprintf("$%d", i+1), v, 1)
			}
			portals[m.DestinationPortal] = q
			send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if m.ObjectType == 'S' && f.describeTypes != nil {
				send(&pgproto3.ParameterDescription{ParameterOIDs: f.describeTypes})
			}
			send(&pgproto3.NoData{})
		case *pgproto3.Close:
			send(&pgproto3.CloseComplete{})
		case *pgproto3.Execute:
			f.execN++
			q := portals[m.Portal]
			f.ran(q)
			if up := strings.ToUpper(q); strings.HasPrefix(up, "DISCARD ALL") || strings.HasPrefix(up, "DEALLOCATE ALL") {
				for name := range stmts { // as the server: every prepared statement goes
					delete(stmts, name)
				}
			}
			switch up := strings.ToUpper(q); {
			case strings.HasPrefix(up, "BEGIN"), strings.HasPrefix(up, "START TRANSACTION"):
				inTx = true
			case strings.HasPrefix(up, "COMMIT"), strings.HasPrefix(up, "ROLLBACK") && !strings.HasPrefix(up, "ROLLBACK TO"):
				inTx = false
			}
			f.mu.Lock()
			answer := f.answer
			f.mu.Unlock()
			if answer != nil {
				if rows, msg, ok := answer(q); ok {
					if msg != "" {
						code := "XX000"
						if len(msg) > 6 && msg[5] == ':' { // "42501:…": an error of that SQLSTATE
							code, msg = msg[:5], msg[6:]
						}
						send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: msg})
						skipping = true
						continue
					}
					for _, r := range rows {
						vals := make([][]byte, len(r))
						for i, v := range r {
							vals[i] = []byte(v)
						}
						send(&pgproto3.DataRow{Values: vals})
					}
					send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(rows)))})
					continue
				}
			}
			switch {
			case f.failExec[f.execN]:
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "execute failed"})
				skipping = true
			case strings.HasPrefix(q, "COPY"):
				send(&pgproto3.CopyInResponse{})
				copyIn = true
			case strings.Contains(q, "RAISE EXCEPTION"): // the proxy's refusal (raiseSQLCode)
				code := "0A000"
				if i := strings.Index(q, "ERRCODE = '"); i >= 0 {
					code = q[i+11 : i+16]
				}
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: "refused"})
				skipping = true
			case strings.Contains(q, "pg_prepared_statements"): // the client's statement takes text parameters
				send(&pgproto3.DataRow{Values: [][]byte{[]byte("{20,20,25}")}}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case gateLock(q) && f.lockErr != "":
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: f.lockErr, Message: "canceling statement due to user request"})
				skipping = true
			case gateLock(q): // the direct path's gate (a write's: for its transaction)
				send(&pgproto3.DataRow{}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.Contains(q, "SELECT coalesce((SELECT profile FROM lokadb.orgs WHERE org = "): // the direct path's gate
				allowed := "true"
				if f.denied || f.extOp != "" && strings.Contains(q, "oprname IN (") && strings.Contains(q, "'"+f.extOp+"'") {
					allowed = "false"
				}
				found := "true"
				if f.unresolved {
					found, allowed = "false", "false"
				}
				send(&pgproto3.DataRow{Values: [][]byte{[]byte(f.profile), []byte(allowed), []byte(found)}}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.Contains(q, "current_setting("): // a set_config of the client's, repeated on a direct connection
				send(&pgproto3.DataRow{Values: [][]byte{[]byte("Asia/Tokyo")}}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.Contains(q, "set_config"):
				send(&pgproto3.DataRow{Values: [][]byte{[]byte("1")}}, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case strings.HasPrefix(strings.ToUpper(q), "BEGIN") && f.failBegin:
				inTx = false
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25001", Message: "BEGIN failed"})
				skipping = true // up to the Sync: the statement after it does not run
			case strings.HasPrefix(strings.ToUpper(q), "BEGIN"): // as Postgres tags it (withBegin reads up to it)
				send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			default:
				send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
			}
		case *pgproto3.FunctionCall: // fastpath: answered with an empty result
			f.ran(fmt.Sprintf("FunctionCall %d", msg.(*pgproto3.FunctionCall).Function))
			status := byte('I')
			if inTx {
				status = 'T'
			}
			send(&pgproto3.FunctionCallResponse{Result: []byte{0, 0, 0, 0}}, &pgproto3.ReadyForQuery{TxStatus: status})
		case *pgproto3.CopyData:
			copyRows += strings.Count(string(msg.(*pgproto3.CopyData).Data), "\n")
		case *pgproto3.CopyFail:
			copyIn = false
			if copySimple {
				copySimple = false
				status := byte('I')
				if inTx {
					status = 'E'
				}
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "COPY from stdin failed: " + msg.(*pgproto3.CopyFail).Message}, &pgproto3.ReadyForQuery{TxStatus: status})
			}
		case *pgproto3.CopyDone:
			copyIn = false
			if copySimple {
				copySimple = false
				status := byte('I')
				if inTx {
					status = 'T'
				}
				send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("COPY %d", copyRows))}, &pgproto3.ReadyForQuery{TxStatus: status})
				continue
			}
			send(&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")})
		case *pgproto3.Query:
			if strings.Contains(m.String, "/* lokadb: identity */") { // the direct path's check of the database
				marks := f.marks
				if marks == nil {
					marks = []string{identityMark("1")}
				}
				for _, mk := range marks {
					send(&pgproto3.DataRow{Values: [][]byte{[]byte(mk)}})
				}
				send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("SELECT %d", len(marks)))}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
				continue
			}
			if strings.Contains(m.String, "/* lokadb: role */") { // the direct path's role check
				if f.roleWhy != "" {
					send(&pgproto3.DataRow{Values: [][]byte{[]byte(f.roleWhy)}})
				}
				send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
				continue
			}
			if f.onQuery != nil && f.onQuery(m.String) {
				f.ran(m.String)
				return // f.be stays: the proxy may connect again at once (a new run sets it)
			}
			if f.dropNext && !strings.HasPrefix(m.String, "SET") {
				f.dropNext = false
				f.sendMu.Lock()
				if f.be == be {
					f.be = nil
				}
				f.sendMu.Unlock()
				return
			}
			if f.noticeFirst && !strings.HasPrefix(m.String, "SET") {
				send(&pgproto3.NoticeResponse{Severity: "NOTICE", Message: "progress"})
			}
			// the client's statements are slow, not the settings (nor the proxy's lokadb.org before
			// one of no LokaDB table, closed by default: sent with it, never waited for)
			if f.delayQuery > 0 && !strings.HasPrefix(m.String, "SET") && !strings.HasPrefix(m.String, "SELECT set_config('lokadb.org'") {
				time.Sleep(f.delayQuery)
			}
			delete(stmts, "") // a simple query destroys the unnamed statement
			if up := strings.ToUpper(m.String); strings.HasPrefix(up, "DISCARD ALL") || strings.HasPrefix(up, "DEALLOCATE ALL") {
				for name := range stmts {
					delete(stmts, name)
				}
			}
			if up := strings.ToUpper(m.String); strings.HasPrefix(up, "BEGIN") && strings.Contains(m.String, "; ") {
				// "BEGIN; <statement>" (withBegin): the BEGIN answered, then the rest as a query of its own
				i := strings.Index(m.String, "; ")
				f.ran(m.String[:i])
				if f.failBegin { // the string stops at its first error
					send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25001", Message: "BEGIN failed"}, &pgproto3.ReadyForQuery{TxStatus: 'I'})
					continue
				}
				inTx = true
				send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
				m = &pgproto3.Query{String: m.String[i+2:]}
			}
			f.ran(m.String)
			f.mu.Lock()
			f.simple = append(f.simple, m.String)
			answer := f.answer
			f.mu.Unlock()
			if answer != nil && f.answerSimple && !strings.HasPrefix(m.String, "BEGIN") && !strings.HasPrefix(m.String, "COMMIT") && !strings.HasPrefix(m.String, "ROLLBACK") {
				if rows, msg, ok := answer(m.String); ok { // a simple query the test answers (the direct path's writes)
					status := byte('I')
					if inTx {
						status = 'T'
					}
					if msg != "" {
						if inTx {
							status = 'E'
						}
						send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: msg}, &pgproto3.ReadyForQuery{TxStatus: status})
						continue
					}
					tag := "DELETE"
					if strings.HasPrefix(m.String, "INSERT") {
						tag = "INSERT 0"
					}
					if strings.Contains(m.String, "RETURNING") { // as Postgres: the rows' description first
						fields := []pgproto3.FieldDescription{{Name: []byte("id"), DataTypeOID: 20, DataTypeSize: 8, TypeModifier: -1}}
						n := 1
						if len(rows) > 0 {
							n = len(rows[0])
						} else if strings.Contains(m.String, "xmax::text = '0'") {
							n = 2 // an upsert that returned no row
						}
						for i := 1; i < n; i++ { // an upsert's added columns (direct_write.go)
							fields = append(fields, pgproto3.FieldDescription{Name: []byte(fmt.Sprintf("c%d", i)), DataTypeOID: 25, TypeModifier: -1})
						}
						send(&pgproto3.RowDescription{Fields: fields})
					}
					for _, r := range rows {
						vals := make([][]byte, len(r))
						for i, v := range r {
							vals[i] = []byte(v)
						}
						send(&pgproto3.DataRow{Values: vals})
					}
					send(&pgproto3.CommandComplete{CommandTag: []byte(fmt.Sprintf("%s %d", tag, len(rows)))}, &pgproto3.ReadyForQuery{TxStatus: status})
					continue
				}
			}
			if strings.HasPrefix(m.String, "COPY") && strings.Contains(m.String, "FROM STDIN") { // copy-in by simple query (the direct path's COPY)
				copyIn, copySimple, copyRows = true, true, 0
				send(&pgproto3.CopyInResponse{ColumnFormatCodes: []uint16{0, 0}})
				continue
			}
			if strings.Contains(m.String, "pg_prepared_statements") { // the client's statement takes text parameters
				send(&pgproto3.DataRow{Values: [][]byte{[]byte("{20,20,25}")}})
			}
			if up := strings.ToUpper(m.String); aborted && !strings.HasPrefix(up, "ROLLBACK") && !strings.HasPrefix(up, "COMMIT") { // as Postgres's aborted transaction
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02", Message: "current transaction is aborted"}, &pgproto3.ReadyForQuery{TxStatus: 'E'})
				continue
			}
			if strings.Contains(m.String, "RAISE EXCEPTION") { // the proxy's refusal by simple query (raiseSQLCode)
				status := byte('I')
				if inTx {
					status, aborted = 'E', true
				}
				send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000", Message: "refused"}, &pgproto3.ReadyForQuery{TxStatus: status})
				continue
			}
			switch up := strings.ToUpper(m.String); {
			case strings.HasPrefix(up, "BEGIN"):
				inTx = true
			case strings.HasPrefix(up, "COMMIT"), strings.HasPrefix(up, "ROLLBACK") && !strings.HasPrefix(up, "ROLLBACK TO"):
				inTx, aborted = false, false
			}
			status := byte('I')
			if inTx {
				status = 'T'
			}
			tag := "SELECT 1"
			if m.String == "DISCARD ALL" { // the pools' reset reads its tag (direct_pool.go)
				tag = "DISCARD ALL"
			}
			send(&pgproto3.CommandComplete{CommandTag: []byte(tag)}, &pgproto3.ReadyForQuery{TxStatus: status})
		}
	}
}

// gateLock: the direct path's pause lock, a read's or a write's (org_lock_v: an organization
// numbered by uuid, block 18).
func gateLock(q string) bool {
	return strings.HasPrefix(q, "SELECT FROM pg_advisory_lock_shared(lokadb.org_lock") || strings.HasPrefix(q, "SELECT FROM pg_advisory_xact_lock_shared(lokadb.org_lock")
}

// notify sends an asynchronous notification, as Postgres does to an idle listening session.
func (f *fakeUpstream) notify(channel string) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()
	f.be.Send(&pgproto3.NotificationResponse{PID: 1, Channel: channel})
	_ = f.be.Flush()
}

func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := l.Accept()
		ch <- c
	}()
	a, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b := <-ch
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

// protoSession starts a session between a test client and f; meta nil = no LokaDB tables.
func protoSession(t *testing.T, f *fakeUpstream, meta *Meta) (*pgproto3.Frontend, net.Conn) {
	fe, c, _ := protoSessionS(t, f, meta)
	return fe, c
}

// protoSessionS also returns the session, whose metadata a test may replace between batches
// (as a reload after enable or a schema change would).
func protoSessionS(t *testing.T, f *fakeUpstream, meta *Meta) (*pgproto3.Frontend, net.Conn, *session) {
	return protoSessionCfg(t, f, meta, Config{})
}

// protoSessionCfg: protoSessionS with the proxy's config cfg.
func protoSessionCfg(t *testing.T, f *fakeUpstream, meta *Meta, cfg Config) (*pgproto3.Frontend, net.Conn, *session) {
	cli, cliSrv := tcpPair(t)
	up, upSrv := tcpPair(t)
	f.be = pgproto3.NewBackend(upSrv, upSrv)
	f.stmts, f.portals, f.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
	go f.run()
	srv := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil)), cfg: cfg}
	hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
	s := newSession(srv, "db", cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, meta, nil)
	go s.run()
	return pgproto3.NewFrontend(cli, cli), cli, s
}

// expectMsgs reads until n ReadyForQuery messages and returns the message types.
func expectMsgs(t *testing.T, fe *pgproto3.Frontend, c net.Conn, n int) []string {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var got []string
	for rfq := 0; rfq < n; {
		m, err := fe.Receive()
		if err != nil {
			t.Fatalf("after %v: %v", got, err)
		}
		name := strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3.")
		got = append(got, name)
		if name == "ReadyForQuery" {
			rfq++
		}
	}
	return got
}

func sendAll(t *testing.T, fe *pgproto3.Frontend, ms ...pgproto3.FrontendMessage) {
	for _, m := range ms {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
}

// R1: the client going away while a response is on its way must not panic the proxy.
func TestQueuePopAfterClose(t *testing.T) {
	q := newQueue()
	q.push([]*expect{{kind: kSync}})
	q.close()
	q.pop() // panicked: slice bounds out of range
	if !q.empty() {
		t.Fatal("queue not empty")
	}
}

// P1: an error at Sync (the implicit COMMIT fails) is followed by ReadyForQuery only. The
// proxy took it for an error inside the batch and skipped to the next Sync: the next
// batch, already queued, lost its expectations and the responses of the proxy's own
// set_config reached the client.
func TestErrorAtSync(t *testing.T) {
	f := &fakeUpstream{failSync: map[int]bool{1: true}, delaySync: map[int]time.Duration{1: 200 * time.Millisecond}}
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO documents (organization_id, title) VALUES (1, 'a')"
	sendAll(t, fe,
		&pgproto3.Parse{Query: ins}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{},
		&pgproto3.Parse{Query: ins}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	got := expectMsgs(t, fe, c, 2)
	want := []string{"ParseComplete", "BindComplete", "CommandComplete", "ErrorResponse", "ReadyForQuery",
		"ParseComplete", "BindComplete", "CommandComplete", "ReadyForQuery"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v\nwant %v", got, want)
	}
}

// P2: after an error in a batch that ended with Flush, the server answers nothing until a
// Sync. The next batch needed idle work (COMMIT) and waited for those answers while
// holding back the very Sync that would bring them: both goroutines hung.
func TestErrorAfterFlush(t *testing.T) {
	f := &fakeUpstream{failExec: map[int]bool{2: true}} // 1 is the proxy's set_config
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO documents (organization_id, title) VALUES (1, 'a')"
	sendAll(t, fe,
		&pgproto3.Parse{Name: "a", Query: ins}, &pgproto3.Bind{PreparedStatement: "a"}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "b", Query: ins}, &pgproto3.Bind{PreparedStatement: "b"}, &pgproto3.Execute{},
		&pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var first []string
	for len(first) < 3 {
		m, err := fe.Receive()
		if err != nil {
			t.Fatalf("after %v: %v", first, err)
		}
		first = append(first, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
	}
	if want := []string{"ParseComplete", "BindComplete", "ErrorResponse"}; !reflect.DeepEqual(first, want) {
		t.Fatalf("first batch: %v, want %v", first, want)
	}
	sendAll(t, fe, &pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got := expectMsgs(t, fe, c, 1); !reflect.DeepEqual(got, []string{"ReadyForQuery"}) {
		t.Fatalf("second batch: %v, want only ReadyForQuery (the server skipped to Sync)", got)
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	if got := expectMsgs(t, fe, c, 1); !reflect.DeepEqual(got, []string{"CommandComplete", "ReadyForQuery"}) {
		t.Fatalf("afterwards: %v", got)
	}
}

// P6: COPY FROM STDIN through Execute: the server ignores the Sync it reads in copy-in
// mode and the client sends another after CopyDone. The ignored Sync stayed queued: the
// next query waited for it forever.
func TestCopyInExtended(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "COPY notes FROM STDIN"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var got []string
	for len(got) < 3 {
		m, err := fe.Receive()
		if err != nil {
			t.Fatalf("after %v: %v", got, err)
		}
		got = append(got, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
	}
	if want := []string{"ParseComplete", "BindComplete", "CopyInResponse"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("copy start: %v", got)
	}
	sendAll(t, fe, &pgproto3.CopyData{Data: []byte("1\tx\n")}, &pgproto3.CopyDone{}, &pgproto3.Sync{})
	if got := expectMsgs(t, fe, c, 1); !reflect.DeepEqual(got, []string{"CommandComplete", "ReadyForQuery"}) {
		t.Fatalf("copy end: %v", got)
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	if got := expectMsgs(t, fe, c, 1); !reflect.DeepEqual(got, []string{"CommandComplete", "ReadyForQuery"}) {
		t.Fatalf("query after COPY: %v", got)
	}
}

// R1: a client that disconnects while its responses stream in must not take the proxy down.
func TestClientGoneMidResponse(t *testing.T) {
	f := &fakeUpstream{delaySync: map[int]time.Duration{1: 100 * time.Millisecond}}
	fe, c := protoSession(t, f, nil)
	for i := 0; i < 200; i++ {
		fe.Send(&pgproto3.Parse{Query: "INSERT INTO t VALUES (1)"})
		fe.Send(&pgproto3.Bind{})
		fe.Send(&pgproto3.Execute{})
	}
	sendAll(t, fe, &pgproto3.Sync{})
	c.Close() // gone before the answers arrive
	time.Sleep(400 * time.Millisecond)
	// reaching this point means no goroutine of the session panicked the test binary
}

// The pause of a move (lokadb.org_lock): every statement on LokaDB tables takes its
// organization's lock in its own transaction, which the client never sees.
func TestPauseLock(t *testing.T) {
	f := &fakeUpstream{}
	m := testMeta()
	m.OrgLock = true
	fe, c := protoSession(t, f, m)
	locks := func() []string {
		var out []string
		for _, q := range f.log() {
			if strings.Contains(q, "advisory") {
				out = append(out, q)
			}
		}
		return out
	}
	// extended: right before the client's statement, up to Sync one transaction
	sendAll(t, fe, &pgproto3.Parse{Query: "INSERT INTO documents (organization_id, title) VALUES ($1, 'a')"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ParseComplete", "BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	log := f.log()
	if want := "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(7::bigint))"; len(log) < 2 || log[len(log)-2] != want {
		t.Fatalf("upstream ran %q, want %q right before the INSERT", log, want)
	}
	// moving rows to another organization: the lock of every organization
	sendAll(t, fe, &pgproto3.Parse{Query: "UPDATE documents SET organization_id = 2 WHERE organization_id = 1"},
		&pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if l := locks(); l[len(l)-1] != "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(NULL::bigint))" {
		t.Fatalf("UPDATE of the organization column took %q", l[len(l)-1])
	}
	// a query string in autocommit: a session lock around it
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Query{String: "INSERT INTO documents (organization_id, title) VALUES (3, 'b')"})
	if got, want := expectMsgs(t, fe, c, 1), []string{"CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	// the unlock goes right behind the statement, and the client's answer does not wait for it: the
	// fake has logged it a moment later (a run under -race read the log before it)
	// set_config and the lock in one statement (third review of performance, perf3-12)
	waitFor(func() bool { return len(f.log()) >= n+3 })
	if got, want := f.log()[n:], []string{
		"SELECT set_config('lokadb.org', '3', false), pg_advisory_lock_shared(lokadb.org_lock(3))",
		"INSERT INTO documents (organization_id, title) VALUES (3, 'b')",
		"SELECT pg_advisory_unlock_shared(lokadb.org_lock(3))",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("upstream ran %q\nwant %q", got, want)
	}
	// no LokaDB table, no lock
	n = len(locks())
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	expectMsgs(t, fe, c, 1)
	if len(locks()) != n {
		t.Fatal("SELECT 1 took a lock")
	}
}

// Block 9, item 8: a query string that sets lokadb.org itself (SET, set_config, DISCARD ALL)
// and works on LokaDB tables. The proxy set nothing before a string with SET lokadb.org, and its
// set_config before a string with set_config('lokadb.org', …) was overridden: the statements ran
// under the application's organization (a child row landed in it). Refused now.
func TestAppSetsOrg(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	for _, q := range []string{
		"SET lokadb.org = '2'; INSERT INTO document_versions (document_id, content) VALUES (1, 'x')",
		"SELECT set_config('lokadb.org', '2', false); INSERT INTO document_versions (document_id, content) VALUES (1, 'x')",
		"INSERT INTO document_versions (document_id, content) SELECT 1, set_config('lokadb.org', '2', false)",
		"SELECT title FROM documents WHERE organization_id = 1; SET LOCAL lokadb.org = ''",
		"DISCARD ALL; SELECT title FROM documents WHERE organization_id = 1",
		"UPDATE pg_settings SET setting = '2' WHERE name = 'lokadb.org'; SELECT title FROM documents WHERE organization_id = 1",
	} {
		n := len(f.log())
		sendAll(t, fe, &pgproto3.Query{String: q})
		if got, want := expectMsgs(t, fe, c, 1), []string{"ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: client got %v, want %v", q, got, want)
		}
		for _, r := range f.log()[n:] {
			if r != "LISTEN "+metaChannel { // after DISCARD ALL, refused or not: harmless
				t.Errorf("%s: upstream ran %q", q, r)
			}
		}
	}
	// extended: one statement that sets lokadb.org and reads a LokaDB table
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Parse{Query: "SELECT set_config('lokadb.org', $1, false), title FROM documents WHERE organization_id = 1"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ParseComplete", "ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("extended: client got %v, want %v", got, want)
	}
	for _, q := range f.log()[n:] {
		if strings.Contains(q, "documents") {
			t.Errorf("extended: upstream ran %q", q)
		}
	}
	// alone, SET lokadb.org still runs
	sendAll(t, fe, &pgproto3.Query{String: "SET lokadb.org = '2'"})
	if got, want := expectMsgs(t, fe, c, 1), []string{"CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("SET alone: client got %v, want %v", got, want)
	}
}

// Block 9, item 8: SET lokadb.org executed between two portals of one organization bound
// earlier in the batch: the second Execute ran under the application's value, since the proxy
// took lokadb.org for still being its own.
func TestAppSetsOrgBetweenPortals(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sel := "SELECT title FROM documents WHERE organization_id = $1"
	sendAll(t, fe,
		&pgproto3.Parse{Name: "a", Query: sel}, &pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "a", Parameters: [][]byte{[]byte("7")}},
		&pgproto3.Parse{Name: "b", Query: "SET lokadb.org = '5'"}, &pgproto3.Bind{DestinationPortal: "p2", PreparedStatement: "b"},
		&pgproto3.Bind{DestinationPortal: "p3", PreparedStatement: "a", Parameters: [][]byte{[]byte("7")}},
		&pgproto3.Execute{Portal: "p1"}, &pgproto3.Execute{Portal: "p2"}, &pgproto3.Execute{Portal: "p3"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	log := f.log()
	if len(log) < 2 || log[len(log)-1] != "SELECT title FROM documents WHERE organization_id = 7" ||
		log[len(log)-2] != "SELECT set_config('lokadb.org', 7, false)" {
		t.Fatalf("upstream ran %q: organization 7 not set again after the client's SET", log)
	}
}

// Block 9, item 1: for up to metaTTL (30 s) after enable the proxy did not know of it and let
// statements through unanalysed. enable (and the other commands that change the metadata)
// notify lokadb_meta; the session reloads the metadata before its next statement. The
// notification is the proxy's: the client does not get it (its own channels it does).
func TestMetaNotify(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, nil)
	reads := func() int {
		n := 0
		for _, q := range f.log() {
			if strings.Contains(q, "lokadb.tables") {
				n++
			}
		}
		return n
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	expectMsgs(t, fe, c, 1)
	if n := reads(); n != 0 {
		t.Fatalf("metadata read %d times before any change", n)
	}
	f.notify(metaChannel)
	f.notify("jobs")
	time.Sleep(100 * time.Millisecond)
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 2"})
	got := expectMsgs(t, fe, c, 1)
	if want := []string{"NotificationResponse", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("client got %v, want %v (only its own channel)", got, want)
	}
	log := f.log()
	if reads() != 1 || log[len(log)-1] != "SELECT 2" {
		t.Errorf("upstream ran %q: the metadata was not read again before SELECT 2", log)
	}
	// the client's UNLISTEN * and DISCARD ALL end the proxy's LISTEN too: it listens again
	sendAll(t, fe, &pgproto3.Query{String: "UNLISTEN *"})
	expectMsgs(t, fe, c, 1)
	time.Sleep(100 * time.Millisecond) // the LISTEN goes after the client's answer
	if log := f.log(); log[len(log)-1] != "LISTEN "+metaChannel {
		t.Errorf("after UNLISTEN * upstream ran %q", log)
	}
	sendAll(t, fe, &pgproto3.Parse{Query: "DISCARD ALL"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ParseComplete", "BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("DISCARD ALL: client got %v, want %v", got, want)
	}
	if log := f.log(); log[len(log)-2] != "DISCARD ALL" || log[len(log)-1] != "LISTEN "+metaChannel {
		t.Errorf("after DISCARD ALL upstream ran %q", log)
	}
}

// Block 9, item 9: the metadata was read again only at a batch ending with Sync that followed
// a synced one. A client that cuts every pipeline with Flush and sends its Sync alone never
// got there: it kept the old metadata (foreign partitions, columns) for good.
func TestRefreshWithFlush(t *testing.T) {
	f := &fakeUpstream{}
	fe, c, s := protoSessionS(t, f, testMeta())
	s.srv.schemaVersion("db").Add(1) // a schema change replayed by another session of the database
	sel := "SELECT title FROM documents WHERE organization_id = $1"
	for i := 0; i < 2; i++ {
		sendAll(t, fe, &pgproto3.Parse{Query: sel}, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Flush{})
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		for n := 0; n < 3; n++ {
			if _, err := fe.Receive(); err != nil {
				t.Fatal(err)
			}
		}
		sendAll(t, fe, &pgproto3.Sync{})
		expectMsgs(t, fe, c, 1)
	}
	log := f.log()
	reads := 0
	for _, q := range log {
		if strings.Contains(q, "lokadb.tables") {
			reads++
		}
	}
	if reads != 1 || !strings.Contains(log[0], "lokadb.tables") {
		t.Fatalf("upstream ran %q: the metadata should be read once, before the first pipeline", log)
	}
}

// Block 9, item 3 (P3): a pipeline longer than the proxy held (1000 messages: pgx SendBatch,
// JDBC executeBatch) went on in pieces cut by Flush, and in them a statement whose
// organization the refs tables tell ran under an empty lokadb.org (every organization's
// database) without the fanout policy. Now the proxy holds far more and looks up in any
// piece (1b); where it cannot (the pipeline already failed) the policy applies.
func TestLongPipeline(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO document_versions (document_id, content) VALUES ($1, 'x')"
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: ins})
	for i := 0; i < 600; i++ { // 1201 messages
		fe.Send(&pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("5")}})
		fe.Send(&pgproto3.Execute{})
	}
	sendAll(t, fe, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	for _, q := range f.log() {
		if q == "SELECT set_config('lokadb.org', , false)" {
			t.Fatalf("a statement of the pipeline ran in every organization's database: %q", q)
		}
	}
	// a client's own Flush: the second piece runs inside the implicit transaction; its lookup
	// goes through the extended protocol (item 2, 1b), not a simple query that would commit it
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("6")}}, &pgproto3.Execute{}, &pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for n := 0; n < 2; n++ {
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	n, simple := len(f.log()), len(f.simple)
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("second piece: client got %v, want %v; upstream ran %q", got, want, f.log()[n:])
	}
	// lokadb.org is -1 upstream since the first piece (it ran in this implicit transaction): not set again
	want := []string{
		`SELECT id::text, coalesce(tenant::text, '-2') FROM "documents__refs" WHERE id IN (7)`,
		"INSERT INTO document_versions (document_id, content) VALUES (7, 'x')",
	}
	if got := f.log()[n:]; !reflect.DeepEqual(got, want) || len(f.simple) != simple {
		t.Fatalf("second piece: upstream ran %q (simple queries %q)\nwant %q, none of them a simple query", got, f.simple[simple:], want)
	}
}

// Block 9, item 2 (1b): a lookup inside a pipeline that fails ends the client's pipeline with
// the server's error, as its own statement's would; nothing hangs.
func TestLookupFailsInPipeline(t *testing.T) {
	f := &fakeUpstream{failExec: map[int]bool{4: true}} // 1-3: lookup, set_config and INSERT of the first piece
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO document_versions (document_id, content) VALUES ($1, 'x')"
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: ins}, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("5")}},
		&pgproto3.Execute{}, &pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for n := 0; n < 3; n++ {
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("6")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	for _, q := range f.log() {
		if strings.Contains(q, "(6, 'x')") {
			t.Fatalf("the statement ran after its lookup failed: %q", f.log())
		}
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	if got := expectMsgs(t, fe, c, 1); !reflect.DeepEqual(got, []string{"CommandComplete", "ReadyForQuery"}) {
		t.Fatalf("afterwards: %v", got)
	}
}

// Block 9, item 7: a cursor WITH HOLD is filled at the COMMIT, under the lokadb.org then in
// force: another statement's organization, whose policy hid the cursor's rows. The proxy sets
// the cursor's own organization right before that COMMIT.
func TestHoldCursorCommit(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	for _, q := range []string{
		"BEGIN",
		"DECLARE h CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 1",
		"INSERT INTO documents (organization_id, title) VALUES (2, 'a')",
		"COMMIT",
	} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	log := f.log()
	if log[len(log)-1] != "COMMIT" || log[len(log)-2] != "SELECT set_config('lokadb.org', '1', false)" {
		t.Fatalf("simple: upstream ran %q, want the cursor's organization set right before COMMIT", log)
	}
	// the same through the extended protocol, in one batch
	n := len(f.log())
	var ms []pgproto3.FrontendMessage
	for _, q := range []string{
		"BEGIN",
		"DECLARE h2 CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 1",
		"INSERT INTO documents (organization_id, title) VALUES (2, 'a')",
		"COMMIT",
	} {
		ms = append(ms, &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{})
	}
	sendAll(t, fe, append(ms, &pgproto3.Sync{})...)
	expectMsgs(t, fe, c, 1)
	log = f.log()[n:]
	if log[len(log)-1] != "COMMIT" || log[len(log)-2] != "SELECT set_config('lokadb.org', 1, false)" {
		t.Fatalf("extended: upstream ran %q, want the cursor's organization set right before COMMIT", log)
	}
	// without a cursor WITH HOLD nothing is added before COMMIT
	n = len(f.log())
	for _, q := range []string{"BEGIN", "INSERT INTO documents (organization_id, title) VALUES (2, 'a')", "COMMIT"} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	if log := f.log()[n:]; strings.HasPrefix(log[len(log)-2], "SELECT set_config('lokadb.org'") {
		t.Fatalf("lokadb.org set before a COMMIT with no cursor: %q", log)
	}
}

// Block 9, item 7: a cursor's query runs at each FETCH, under the lokadb.org then in force. In
// the cooperative mode the query need not name the organization (lokadb.tenant does): FETCH
// under an empty lokadb.org read every organization's rows (CU3). Each FETCH or MOVE runs
// under the organization its cursor was declared under, as does the COMMIT that fills a
// cursor WITH HOLD; two such cursors of two named organizations in one transaction are refused.
func TestCursorOrg(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	// before: the nearest set_config of lokadb.org before sql ran (one is not repeated when
	// the organization is the same upstream: PF3); "<not run>" for none
	before := func(log []string, sql string) string {
		for i, q := range log {
			if q == sql {
				for j := i - 1; j >= 0; j-- {
					if strings.HasPrefix(log[j], "SELECT set_config('lokadb.org'") {
						return log[j]
					}
				}
			}
		}
		return "<not run>"
	}
	for _, q := range []string{
		"BEGIN",
		"SELECT set_config('lokadb.tenant', '7', true)",
		"DECLARE c CURSOR FOR SELECT id FROM documents",
		"SELECT count(*) FROM users",
		"FETCH 10 FROM c",
		"MOVE BACKWARD 5 IN c",
		"COMMIT",
	} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	log := f.log()
	for _, q := range []string{"FETCH 10 FROM c", "MOVE BACKWARD 5 IN c"} {
		if got := before(log, q); got != "SELECT set_config('lokadb.org', '7', false)" {
			t.Errorf("simple: before %q upstream ran %q, want the cursor's organization 7 (all: %q)", q, got, log)
		}
	}
	// the extended protocol, the cursor WITH HOLD filled at the COMMIT
	n := len(f.log())
	var ms []pgproto3.FrontendMessage
	for _, q := range []string{
		"BEGIN",
		"SELECT set_config('lokadb.tenant', '7', true)",
		"DECLARE h CURSOR WITH HOLD FOR SELECT id FROM documents",
		"SELECT count(*) FROM users",
		"FETCH 10 FROM h",
		"SELECT set_config('lokadb.org', '', false)", // not the proxy's: stands for another organization's statement
	} {
		ms = append(ms, &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{})
	}
	sendAll(t, fe, append(ms, &pgproto3.Sync{})...)
	expectMsgs(t, fe, c, 1)
	log = f.log()[n:]
	if got, _ := orgsBefore(log, "FETCH 10 FROM h"); got != "SELECT set_config('lokadb.org', 7, false)" { // the statement between ran under 7 too (openOrg)
		t.Errorf("extended: before FETCH upstream ran %q, want organization 7 (all: %q)", got, log)
	}
	n = len(f.log())
	sendAll(t, fe, &pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if log = f.log()[n:]; before(log, "COMMIT") != "SELECT set_config('lokadb.org', 7, false)" {
		t.Errorf("extended: upstream ran %q, want organization 7 right before the COMMIT that fills the cursor", log)
	}
	// two cursors WITH HOLD of two named organizations
	for _, q := range []string{"BEGIN", "SELECT set_config('lokadb.tenant', '7', true)", "DECLARE a CURSOR WITH HOLD FOR SELECT id FROM documents",
		"SELECT set_config('lokadb.tenant', '8', true)"} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	sendAll(t, fe, &pgproto3.Query{String: "DECLARE b CURSOR WITH HOLD FOR SELECT id FROM documents"})
	if code := errorCode(t, fe); code != "0A000" {
		t.Errorf("a second cursor WITH HOLD of organization 8 after one of 7: code %q, want 0A000 (upstream ran %q)", code, f.log())
	}
}

// Block 7, item 9 (taken by block 9): CREATE UNIQUE INDEX on a LokaDB table runs as SELECT
// lokadb.create_index(…); the client got its tag (SELECT 1), not CREATE INDEX.
func TestUniqueIndexTag(t *testing.T) {
	f := &fakeUpstream{answer: metaKept}
	fe, c := protoSession(t, f, testMeta())
	tags := func() []string {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		var out []string
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			switch m := m.(type) {
			case *pgproto3.CommandComplete:
				out = append(out, string(m.CommandTag))
			case *pgproto3.ErrorResponse:
				out = append(out, "ERROR "+m.Message)
			case *pgproto3.ReadyForQuery:
				return out
			}
		}
	}
	idx := "CREATE UNIQUE INDEX documents_slug ON documents (slug)"
	sendAll(t, fe, &pgproto3.Query{String: idx})
	if got := tags(); !reflect.DeepEqual(got, []string{"CREATE INDEX"}) {
		t.Errorf("simple: tags %q, want CREATE INDEX (upstream ran %q)", got, f.log())
	}
	sendAll(t, fe, &pgproto3.Query{String: "BEGIN"})
	tags()
	sendAll(t, fe, &pgproto3.Parse{Query: idx}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got := tags(); !reflect.DeepEqual(got, []string{"CREATE INDEX"}) {
		t.Errorf("extended: tags %q, want CREATE INDEX", got)
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1"})
	if got := tags(); !reflect.DeepEqual(got, []string{"SELECT 1"}) {
		t.Errorf("another statement: tags %q", got)
	}
}

// Block 9, B9.12: a session whose metadata the role cannot read refuses statements on LokaDB
// tables, simple and extended, instead of passing them unrouted; others go on.
func TestMetaDenied(t *testing.T) {
	f := &fakeUpstream{}
	m := &Meta{Tables: map[string]*Table{"documents": {Name: "documents", ColType: map[string]string{}}}, Lookups: map[string]map[string]string{}, Denied: metaDeniedMsg}
	fe, c := protoSession(t, f, m)
	sendAll(t, fe, &pgproto3.Query{String: "SELECT count(*) FROM documents"})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("simple: client got %v, want %v", got, want)
	}
	sendAll(t, fe, &pgproto3.Parse{Query: "SELECT title FROM documents WHERE id = $1"}, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ParseComplete", "ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("extended: client got %v, want %v", got, want)
	}
	for _, q := range f.log() {
		if strings.Contains(q, "documents") && !strings.HasPrefix(q, "Parse") {
			t.Errorf("upstream ran %q", q)
		}
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	if got, want := expectMsgs(t, fe, c, 1), []string{"CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Errorf("not a LokaDB table: client got %v, want %v", got, want)
	}
}

// metaAnswers answers loadMeta's queries for a database where documents is enabled
// (failing: the query that contains it fails).
func metaAnswers(failing string) func(q string) ([][]string, string, bool) {
	return func(q string) ([][]string, string, bool) {
		if !strings.Contains(q, "lokadb.") {
			return nil, "", false
		}
		if failing != "" && strings.Contains(q, failing) {
			return nil, "canceling statement due to statement timeout", true
		}
		switch {
		case strings.Contains(q, "to_regclass('lokadb.tables')"):
			return [][]string{{"true"}}, "", true
		case strings.Contains(q, "FROM lokadb.tables t") && strings.Contains(q, "key_column"):
			return [][]string{{"documents", "organization_id", "documents__refs", "documents__cloud", "id"}}, "", true
		case strings.Contains(q, "FROM lokadb.lookups"):
			return [][]string{{"documents", "organization_id", ""}, {"documents", "id", "documents__refs"}}, "", true
		case strings.Contains(q, "format_type(a.atttypid"):
			// the columns, their types and whether the type is the application's (block 10)
			return [][]string{{"documents", "id", "bigint", "false"}, {"documents", "organization_id", "bigint", "false"}, {"documents", "title", "text", "false"}}, "", true
		case strings.Contains(q, "to_regprocedure"), strings.Contains(q, "attname = 'schema'"):
			return [][]string{{"false"}}, "", true
		}
		return nil, "", true // partitions, triggers, indexes, dependants: none
	}
}

// metaKept: the fake fails the metadata read, so the session keeps the metadata the test gave
// it (a read that fails says nothing of the database; one that finds no table means disable).
func metaKept(q string) ([][]string, string, bool) {
	if strings.Contains(q, "to_regclass('lokadb.tables')") {
		return nil, "metadata not served by this fake", true
	}
	return nil, "", false
}

// routed: upstream set lokadb.org for one of qs (the proxy routed it).
func routed(log []string) bool {
	for _, q := range log {
		if strings.Contains(q, "set_config('lokadb.org'") {
			return true
		}
	}
	return false
}

// Block 6: after disable (lokadb.tables gone or empty) a live session kept the metadata it had
// for good: it routed through refs tables and lokadb.org_lock that disable then dropped, and
// every statement failed (schema "lokadb" does not exist). A read that goes through and finds
// no LokaDB table now leaves the session without metadata; enabled again, it routes again.
// A statement prepared while enabled goes as the client prepared it.
func TestMetaGone(t *testing.T) {
	f := &fakeUpstream{}
	f.answer = metaAnswers("")
	fe, c, s := protoSessionS(t, f, nil)
	sel := "SELECT title FROM documents WHERE organization_id = 1"
	run := func(what string, wantRouted bool) {
		t.Helper()
		f.notify(metaChannel)
		time.Sleep(100 * time.Millisecond)
		n := len(f.log())
		sendAll(t, fe, &pgproto3.Query{String: sel})
		expectMsgs(t, fe, c, 1)
		log := f.log()[n:]
		if routed(log) != wantRouted || log[len(log)-1] != sel {
			t.Errorf("%s: upstream ran %q (routed: want %v)", what, log, wantRouted)
		}
	}
	run("enabled", true)
	// prepared while enabled
	prep := "SELECT title FROM documents WHERE organization_id = $1"
	sendAll(t, fe, &pgproto3.Parse{Name: "s1", Query: prep}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)

	f.mu.Lock()
	f.answer = func(q string) ([][]string, string, bool) { // disable: lokadb.tables empty, then gone
		if strings.Contains(q, "to_regclass('lokadb.tables')") {
			return [][]string{{"false"}}, "", true
		}
		return nil, "", false
	}
	f.mu.Unlock()
	run("after disable", false)
	if s.meta != nil {
		t.Errorf("after disable the session still has metadata for %d table(s)", len(s.meta.Tables))
	}
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if log := f.log()[n:]; routed(log) || len(log) != 1 || log[0] != "SELECT title FROM documents WHERE organization_id = 1" {
		t.Errorf("a statement prepared while enabled, bound after disable: upstream ran %q", log)
	}

	f.mu.Lock()
	f.answer = metaAnswers("")
	f.mu.Unlock()
	run("enabled again", true)
}

// Block 6: a metadata read that fails part way (a timeout on one of loadMeta's queries) says
// nothing of the database: the session keeps routing with what it had, and reads again in a
// second, then 2, 4 … up to metaTTL while it keeps failing.
func TestMetaReadFails(t *testing.T) {
	f := &fakeUpstream{}
	f.answer = metaAnswers("lokadb.lookups")
	fe, c, s := protoSessionS(t, f, testMeta())
	s.orgCache["documents__refs"] = map[string]string{"5": "1"}
	f.notify(metaChannel)
	time.Sleep(100 * time.Millisecond)
	sel := "SELECT title FROM documents WHERE organization_id = 1"
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Query{String: sel})
	expectMsgs(t, fe, c, 1)
	log := f.log()[n:]
	if !routed(log) || log[len(log)-1] != sel {
		t.Errorf("upstream ran %q: the session stopped routing after a failed read", log)
	}
	if s.meta == nil || s.meta.Tables["document_versions"] == nil {
		t.Errorf("the failed read replaced the metadata")
	}
	if len(s.orgCache) != 0 {
		t.Errorf("after lokadb_meta the refs cache is kept: %v", s.orgCache)
	}
	if d := time.Until(s.metaAt.Add(metaTTL)); d > 2*metaRetry {
		t.Errorf("the next read is %s away, want about %s", d, metaRetry)
	}
	// failing again: the wait doubles; a read that goes through starts over
	for i, want := range []time.Duration{2 * metaRetry, 4 * metaRetry} {
		s.metaAt = time.Now().Add(-metaTTL - time.Millisecond)
		sendAll(t, fe, &pgproto3.Query{String: sel})
		expectMsgs(t, fe, c, 1)
		if d := time.Until(s.metaAt.Add(metaTTL)); d < want-time.Second/2 || d > want {
			t.Errorf("failure %d in a row: the next read is %s away, want about %s", i+2, d, want)
		}
	}
	f.mu.Lock()
	f.answer = metaAnswers("")
	f.mu.Unlock()
	s.metaAt = time.Now().Add(-metaTTL - time.Millisecond)
	sendAll(t, fe, &pgproto3.Query{String: sel})
	expectMsgs(t, fe, c, 1)
	if s.metaFails != 0 || time.Until(s.metaAt.Add(metaTTL)) < metaTTL-metaTTL/6-time.Second { // metaLook: up to a sixth earlier
		t.Errorf("after a read that went through: %d failures counted, next read in %s", s.metaFails, time.Until(s.metaAt.Add(metaTTL)))
	}
}

// Block 9, review PR-10: a NOTICE of a long statement (RAISE NOTICE of a migration, a batch
// function) reached the client with the statement's end: it was buffered until a ReadyForQuery.
// Postgres sends it at once.
func TestNoticeIsNotHeldBack(t *testing.T) {
	f := &fakeUpstream{noticeFirst: true, delayQuery: 1500 * time.Millisecond}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	_ = c.SetReadDeadline(time.Now().Add(700 * time.Millisecond))
	m, err := fe.Receive()
	if err != nil {
		t.Fatalf("no NOTICE within 700 ms while the statement runs for 1.5 s: %v", err)
	}
	if _, ok := m.(*pgproto3.NoticeResponse); !ok {
		t.Fatalf("first message %T, want the NoticeResponse", m)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	expectMsgs(t, fe, c, 1)
}

func (f *fakeUpstream) parsedCount(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, p := range f.parsed {
		if p == name {
			n++
		}
	}
	return n
}

// Block 9, review PF3: set_config of lokadb.org and the pause lock were Closed, Parsed, Bound and
// Executed for every Bind of every client statement (parsed and planned by the cloud each time).
// They are prepared once on the connection; the Parse goes again only when the server has not
// answered one, and after the client's DISCARD ALL or DEALLOCATE ALL (which drop them), also
// inside one batch.
func TestServiceStatementsPreparedOnce(t *testing.T) {
	m := testMeta()
	m.OrgLock = true
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, m)
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	run := func(orgs ...string) {
		t.Helper()
		var ms []pgproto3.FrontendMessage
		for _, o := range orgs {
			ms = append(ms, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte(o)}}, &pgproto3.Execute{})
		}
		sendAll(t, fe, append(ms, &pgproto3.Sync{})...)
		if code := errorCode(t, fe); code != "" {
			t.Fatalf("binds of %v: error %s; upstream ran %q", orgs, code, f.log())
		}
	}
	run("1")
	run("2", "1", "2") // three binds in one batch, alternating organizations
	run("1")
	if s, l := f.parsedCount("lokadb_s"), f.parsedCount("lokadb_ls"); s != 1 || l != 1 {
		t.Fatalf("Parses of the service statements after five binds: set_config %d, lock %d, want 1 and 1", s, l)
	}
	// the client's DISCARD ALL drops them: prepared again from the next use
	sendAll(t, fe, &pgproto3.Query{String: "DISCARD ALL"})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{}) // the client's own statement went too
	expectMsgs(t, fe, c, 1)
	run("1")
	if s, l := f.parsedCount("lokadb_s"), f.parsedCount("lokadb_ls"); s != 2 || l != 2 {
		t.Fatalf("after DISCARD ALL: Parses set_config %d, lock %d, want 2 and 2", s, l)
	}
	// DISCARD ALL inside the batch, a bind after it: the Parse comes before that bind, not after
	// (the statement the proxy believed prepared is gone by then: 26000 otherwise)
	run("1")
	sendAll(t, fe, &pgproto3.Parse{Name: "d", Query: "DISCARD ALL"}, &pgproto3.Bind{PreparedStatement: "d"}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "v2", Query: "SELECT title FROM documents WHERE organization_id = $1"},
		&pgproto3.Bind{PreparedStatement: "v2", Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code := errorCode(t, fe); code != "" {
		t.Fatalf("a bind after DISCARD ALL in one batch: %s; upstream ran %q", code, f.log())
	}
	if s := f.parsedCount("lokadb_s"); s != 3 {
		t.Fatalf("after DISCARD ALL in a batch: Parses of set_config %d, want 3", s)
	}
}

// lokadb.org already the organization upstream: not set again, in the batch and for a simple
// query (the review's measure: 4 statements of the proxy's own for each of the client's).
func TestOrgNotSetAgain(t *testing.T) {
	m := testMeta()
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, m)
	count := func() int { return countContaining(f.log(), "set_config('lokadb.org'") }
	for i := 0; i < 3; i++ {
		sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1"})
		expectMsgs(t, fe, c, 1)
	}
	if n := count(); n != 1 {
		t.Fatalf("set_config of lokadb.org for three simple statements of one organization: %d, want 1; upstream ran %q", n, f.log())
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 2"})
	expectMsgs(t, fe, c, 1)
	if n := count(); n != 2 {
		t.Fatalf("another organization: %d set_config, want 2", n)
	}
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	n0 := count()
	for i := 0; i < 3; i++ {
		sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "v", Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
		expectMsgs(t, fe, c, 1)
	}
	if n := count() - n0; n != 0 {
		t.Fatalf("three binds of the organization lokadb.org is already set to: %d set_config, want none; upstream ran %q", n, f.log())
	}
}

func countContaining(log []string, sub string) int {
	n := 0
	for _, q := range log {
		if strings.Contains(q, sub) {
			n++
		}
	}
	return n
}

// Block 9 on R1 of block 1: a statement that reads no LokaDB row runs under lokadb.org = -1 (noRows).
// Like any organization it is set once and not again while it is the value upstream (PF3), and
// again after the client's DISCARD ALL, which resets it.
func TestNoRowsOrgNotSetAgain(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	count := func() int {
		return countContaining(f.log(), "set_config('lokadb.org', '-1'") + countContaining(f.log(), "set_config('lokadb.org', -1")
	}
	simple := func(q string) {
		t.Helper()
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	for i := 0; i < 3; i++ {
		simple("SELECT 1")
	}
	if n := count(); n != 1 {
		t.Fatalf("three statements that read no LokaDB row: %d set_config of -1, want 1; upstream ran %q", n, f.log())
	}
	simple("DISCARD ALL")
	simple("SELECT 1")
	simple("SELECT 1")
	if n := count(); n != 2 {
		t.Fatalf("after DISCARD ALL: %d set_config of -1 in all, want 2 (set again once); upstream ran %q", n, f.log())
	}
	// the extended protocol: binds in batches, then a DISCARD ALL in a batch and a bind after it
	sendAll(t, fe, &pgproto3.Parse{Name: "n", Query: "SELECT 1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	n0 := count()
	for i := 0; i < 2; i++ {
		sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "n"}, &pgproto3.Execute{}, &pgproto3.Bind{PreparedStatement: "n"}, &pgproto3.Execute{}, &pgproto3.Sync{})
		expectMsgs(t, fe, c, 1)
	}
	if n := count() - n0; n != 0 {
		t.Fatalf("four binds of statements that read no LokaDB row after -1 was set: %d more set_config of -1, want none; upstream ran %q", n, f.log())
	}
	sendAll(t, fe, &pgproto3.Parse{Name: "d", Query: "DISCARD ALL"}, &pgproto3.Bind{PreparedStatement: "d"}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "n2", Query: "SELECT 2"}, &pgproto3.Bind{PreparedStatement: "n2"}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code := errorCode(t, fe); code != "" {
		t.Fatalf("a bind after DISCARD ALL: %s", code)
	}
	if n := count() - n0; n != 1 {
		t.Fatalf("a bind after DISCARD ALL in the batch: %d set_config of -1 since, want 1; upstream ran %q", n, f.log())
	}
}

// setBackend: the backend and the statements of a new connection to the fake (the old one's
// run keeps its own).
func (f *fakeUpstream) setBackend(be *pgproto3.Backend) {
	f.sendMu.Lock()
	f.be, f.stmts, f.portals, f.parseOIDs = be, map[string]string{}, map[string]string{}, map[string][]uint32{}
	f.sendMu.Unlock()
}
