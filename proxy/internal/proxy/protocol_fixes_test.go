package proxy

// Findings of the review of the protocol (tasks/review/protocol.md, PR-2, PR-3, PR-4): what
// Postgres (or a safe proxy) does, each test failing on 4af18c4 (PR-1: direct_commit_test.go).

import (
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// answerTag: like answerAll, with the first CommandComplete's tag.
func (d *directTest) answerTag(t *testing.T) (code, tag string, status byte) {
	t.Helper()
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			if code == "" {
				code = m.Code
			}
		case *pgproto3.CommandComplete:
			if tag == "" {
				tag = string(m.CommandTag)
			}
		case *pgproto3.ReadyForQuery:
			return code, tag, m.TxStatus
		}
	}
}

// PR-4. An error the cloud raises while a BEGIN is deferred (a batch without Execute: a Bind of
// a name that does not exist, a Parse that does not parse) does not abort the client's
// transaction: the cloud is outside one ('I'), the client is told 'T', and what follows runs and
// commits. Postgres: the transaction is aborted ('E'), what follows gets 25P02, COMMIT answers
// ROLLBACK.
func TestDirectDeferredTxError(t *testing.T) {
	d, _ := txSession(t)
	_, code, st := d.exchange(t, "BEGIN")
	expectTx(t, "BEGIN", code, st, "", 'T')
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "nope"}, &pgproto3.Sync{})
	code, _, st = d.answerTag(t)
	if code != "26000" {
		t.Fatalf("Bind of an unknown statement: error %q, want 26000", code)
	}
	if st != 'E' {
		t.Errorf("an error inside the transaction block: the client is told %q, Postgres says 'E' (aborted)", st)
	}
	_, code, _ = d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	if code != "25P02" {
		t.Errorf("a statement after the error: error %q, want 25P02 (it ran: org1 %q)", code, d.org1.log())
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "COMMIT"})
	_, tag, _ := d.answerTag(t)
	if tag != "ROLLBACK" {
		t.Errorf("COMMIT of an aborted transaction answered %q, Postgres answers ROLLBACK", tag)
	}
}

// PR-3. The proxy's own unlock of the pause after a direct read (ungate) is sent without waiting
// and its answers are dropped, but an error of it is relayed to the client: the client, idle
// after its ReadyForQuery, gets it as the error of its NEXT statement (pgx, JDBC read it so),
// although that statement ran. And the session's shared org_lock stays held: move-org,
// move-back, org-profile of the organization wait for it until the session ends.
func TestDirectUngateOwnAnswer(t *testing.T) {
	d := directSession(t, false)
	failed := atomic.Bool{}
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "pg_advisory_unlock_shared") && failed.CompareAndSwap(false, true) { // a CancelRequest that reached the cloud late, once
			return nil, "57014:canceling statement due to user request", true
		}
		return nil, "", false
	}
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || !d.direct("SELECT title") {
		t.Fatalf("direct read: error %q, org1 %q", code, d.org1.log())
	}
	time.Sleep(100 * time.Millisecond) // the ungate's answer is in
	_, code, _ := d.exchange(t, "SELECT count(*) FROM projects")
	if code != "" {
		t.Errorf("the next statement (on cloud tables, it ran: %v) got the error %s of the proxy's own unlock", ranAny(d.cloud.log(), "FROM projects"), code)
	}
	unlocks := 0
	for _, q := range d.cloud.log() {
		if strings.Contains(q, "pg_advisory_unlock_shared") {
			unlocks++
		}
	}
	if unlocks != 2 {
		t.Errorf("the unlock that failed is sent again: %d unlocks, want 2 (cloud %q)", unlocks, d.cloud.log())
	}
}

// PR-3: a CancelRequest while the session's statement runs on the direct path goes to the
// organization's server alone: the cloud runs nothing of it, and a cancel arriving there late
// would hit the proxy's own release of the pause.
func TestDirectCancelNotCloud(t *testing.T) {
	org, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer org.Close()
	cloud, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer cloud.Close()
	d := directSession(t, false)
	d.cancelAddr = org.Addr().String()
	d.s.srv.cfg.Upstream = "host=127.0.0.1 port=" + strings.Split(cloud.Addr().String(), ":")[1]
	d.org1.delayQuery = 700 * time.Millisecond
	done := make(chan string, 1)
	go func() { done <- d.query(t, "SELECT title FROM documents WHERE organization_id = 1") }()
	time.Sleep(200 * time.Millisecond)
	go d.s.srv.forwardCancel(&pgproto3.CancelRequest{ProcessID: 0, SecretKey: nil})
	_ = org.(*net.TCPListener).SetDeadline(time.Now().Add(3 * time.Second))
	c, err := org.Accept()
	if err != nil {
		t.Fatalf("no CancelRequest reached the organization's server: %v", err)
	}
	c.Close()
	_ = cloud.(*net.TCPListener).SetDeadline(time.Now().Add(300 * time.Millisecond))
	if c, err := cloud.Accept(); err == nil {
		c.Close()
		t.Errorf("the cancel of a statement on the direct path reached the cloud too")
	}
	<-done
}

// PR-2. A FunctionCall (fastpath: pgjdbc's large objects lo_creat/lo_open/lowrite, Hibernate
// @Lob) inside a deferred transaction goes to the cloud outside any transaction: it commits on
// its own, a client's ROLLBACK does not undo it, and a large-object descriptor dies with its
// own implicit transaction. Postgres runs it in the client's transaction; the deferred BEGIN
// must reach the cloud first.
func TestDirectFunctionCallDeferred(t *testing.T) {
	d, _ := txSession(t)
	_, code, st := d.exchange(t, "BEGIN")
	expectTx(t, "BEGIN", code, st, "", 'T')
	sendAll(t, d.fe, &pgproto3.FunctionCall{Function: 952, ResultFormatCode: 1, Arguments: [][]byte{{0, 0, 0, 0}}}) // lo_open
	time.Sleep(300 * time.Millisecond)                                                                              // the fake does not answer a FunctionCall: what reached the cloud is enough
	if !ranAny(d.cloud.log(), "BEGIN") {
		t.Errorf("the FunctionCall reached the cloud without the client's BEGIN: it runs outside the transaction (cloud ran %q)", d.cloud.log())
	}
}

// PR-5: the cloud's transaction of a direct transaction and of a direct write waits idle while
// the organization runs the statements: it sets idle_in_transaction_session_timeout = 0 for
// itself, right after its BEGIN.
func TestDirectCloudBusy(t *testing.T) {
	d, l := txSession(t, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	for _, q := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1", "COMMIT",
		"INSERT INTO documents (organization_id, title) VALUES (1, 'z')"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	all, begins := l.get(), 0
	for i, q := range all {
		if q == "cloud: BEGIN" {
			begins++
			if i+1 >= len(all) || all[i+1] != "cloud: "+cloudBusy {
				t.Errorf("the cloud's BEGIN of the direct path not followed by %q: %q", cloudBusy, all)
			}
		}
	}
	if begins != 2 || !d.direct("INSERT INTO documents (") {
		t.Fatalf("a direct transaction and a direct INSERT: %d cloud BEGINs in %q", begins, all)
	}
}

// PR-13: in a direct transaction a SELECT that calls set_config and reads a cloud table is a
// statement, not a setting: it goes to the cloud's transaction, not whole to the organization's
// database (which has no projects).
func TestDirectSetConfigNotSetting(t *testing.T) {
	for sql, setting := range map[string]bool{
		"SELECT set_config('statement_timeout', '5s', false)":                            true,
		"SELECT pg_catalog.set_config('statement_timeout', '5s'::text, false)":           true,
		"SELECT set_config('statement_timeout', '5s', false), count(*) FROM projects":    false,
		"SELECT set_config('statement_timeout', current_setting('lock_timeout'), false)": false,
		"SELECT set_config('statement_timeout', '5s', false), now()":                     false,
	} {
		tree, err := pg.Parse(sql)
		if err != nil {
			t.Fatal(err)
		}
		if got := onlySetConfig(tree.Stmts[0].Stmt.GetSelectStmt()); got != setting {
			t.Errorf("%s: a setting %v, want %v", sql, got, setting)
		}
	}
	d, _ := txSession(t)
	q := "SELECT set_config('statement_timeout', '5s', false), count(*) FROM projects"
	for _, s := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1", q, "COMMIT"} {
		if _, code, _ := d.exchange(t, s); code != "" {
			t.Fatalf("%s: %q", s, code)
		}
	}
	if !d.clouded(q) {
		t.Fatalf("the SELECT with set_config and a cloud table: cloud %q, org1 %q", d.cloud.log(), d.org1.log())
	}
}

// Review of routing, R7: in a database with LokaDB tables a fastpath call runs a large object
// function alone, under lokadb.org = '-1' (no LokaDB row); any other oid (set_config by its oid,
// a function of the application's) is refused by the server, so a transaction is aborted.
func TestDirectFunctionCallRouted(t *testing.T) {
	d := directSession(t, false)
	sendAll(t, d.fe, &pgproto3.FunctionCall{Function: 2078, Arguments: [][]byte{[]byte("lokadb.org"), []byte(""), []byte("f")}}) // set_config
	if code, _ := errorCodeMsg(t, d.fe); code != "0A000" || ranAny(d.cloud.log(), "FunctionCall") {
		t.Fatalf("a fastpath set_config: %q, cloud %q", code, d.cloud.log())
	}
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 2"); code != "" { // organization 2 (fdw): lokadb.org = 2
		t.Fatal(code)
	}
	n := len(d.cloud.log())
	sendAll(t, d.fe, &pgproto3.FunctionCall{Function: 952, ResultFormatCode: 1, Arguments: [][]byte{{0, 0, 0, 0}}}) // lo_open
	if code, _ := errorCodeMsg(t, d.fe); code != "" {
		t.Fatalf("lo_open: %q", code)
	}
	log := d.cloud.log()[n:]
	if i := first(log, "SELECT set_config('lokadb.org', -1, false)"); i == len(log) || first(log, "FunctionCall 952") < i {
		t.Fatalf("lo_open without lokadb.org set again before it (organization 2's before): cloud %q", log)
	}
}

// Third review of the protocol, 6: lokadb.org = '-1' before a fastpath call goes by the extended
// protocol: a simple query would destroy the client's unnamed statement, which fastpath does not.
func TestDirectFunctionCallKeepsUnnamed(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "SELECT 1"}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.FunctionCall{Function: 952, Arguments: [][]byte{{0, 0, 0, 1}, {0, 2, 0, 0}}, ArgFormatCodes: []uint16{1}, ResultFormatCode: 1})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if code := errorCode(t, fe); code != "" {
		t.Fatalf("the client's unnamed statement, bound after a fastpath call: %s; upstream ran %q", code, f.log())
	}
}

// Third review of performance, perf3-11: the direct path's statements of its own in the cloud (the
// gate's lock and check, the release) are prepared once on the connection, the organization a
// parameter: the first direct read Parses them, the next ones only Bind and Execute (3 Parses
// a statement before). The client's DISCARD ALL drops them, and they are prepared again.
func TestDirectGatePreparedOnce(t *testing.T) {
	d := directSession(t, false)
	q := "SELECT title FROM documents WHERE organization_id = 1"
	parses := func() int {
		d.cloud.mu.Lock()
		defer d.cloud.mu.Unlock()
		n := 0
		for _, name := range d.cloud.parsed {
			if strings.HasPrefix(name, "lokadb_q") {
				n++
			}
		}
		return n
	}
	read := func() {
		t.Helper()
		if code := d.query(t, q); code != "" || !d.direct(q) {
			t.Fatalf("direct read: %q", code)
		}
		time.Sleep(50 * time.Millisecond) // the release is not waited for
	}
	read()
	first := parses()
	for i := 0; i < 3; i++ {
		read()
	}
	if first != 3 || parses() != first {
		t.Fatalf("Parses of the gate's statements: %d for the first read, %d after four (want 3, then none more)", first, parses())
	}
	if code := d.query(t, "DISCARD ALL"); code != "" {
		t.Fatal(code)
	}
	read()
	if parses() != 2*first {
		t.Fatalf("after the client's DISCARD ALL: %d Parses, want %d (prepared again)", parses(), 2*first)
	}
}
