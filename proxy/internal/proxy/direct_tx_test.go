package proxy

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// txSession: a direct session whose two fakes log into one order.
func txSession(t *testing.T, edit ...func(*Meta)) (*directTest, *sharedLog) {
	d := directSession(t, false, edit...)
	l := &sharedLog{}
	d.cloud.shared, d.cloud.name = l, "cloud"
	d.org1.shared, d.org1.name = l, "org1"
	// the forms the tests start their transactions with, learnt already (TestDirectTxLearn learns)
	for _, q := range []string{"SELECT title FROM documents WHERE organization_id = 1", "UPDATE documents SET title = 'y' WHERE organization_id = 1",
		"SELECT title FROM documents WHERE organization_id = $1"} {
		learnt(d, q)
	}
	return d, l
}

// learnt: the form starting with sql is learnt for the direct path.
func learnt(d *directTest, sql string) {
	for i := 0; i < learnRuns; i++ {
		d.s.srv.directOK.result("db/"+fingerprint(sql), true)
	}
}

// exchange sends sql and reads the answer: the message types, the error code, the status.
func (d *directTest) exchange(t *testing.T, sql string) ([]string, string, byte) {
	t.Helper()
	sendAll(t, d.fe, &pgproto3.Query{String: sql})
	return d.answerAll(t)
}

func (d *directTest) answerAll(t *testing.T) ([]string, string, byte) {
	t.Helper()
	var types []string
	code := ""
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			if code == "" {
				code = m.Code
			}
		case *pgproto3.ReadyForQuery:
			return types, code, m.TxStatus
		}
	}
}

// first: the position of the first line of log that starts with sub, len(log) for none.
func first(log []string, sub string) int {
	for i, l := range log {
		if strings.HasPrefix(l, sub) {
			return i
		}
	}
	return len(log)
}

func expectTx(t *testing.T, what, code string, status byte, wantCode string, wantStatus byte) {
	t.Helper()
	if code != wantCode || status != wantStatus {
		t.Fatalf("%s: error %q, status %q; want %q, %q", what, code, status, wantCode, wantStatus)
	}
}

// 8a.2: a BEGIN of a session that may go direct is held back; BEGIN; COMMIT with nothing in
// between never reaches the cloud; a second BEGIN warns as the server does.
func TestDirectTxDeferredEmpty(t *testing.T) {
	d, l := txSession(t)
	_, code, st := d.exchange(t, "BEGIN")
	expectTx(t, "BEGIN", code, st, "", 'T')
	types, code, st := d.exchange(t, "BEGIN")
	expectTx(t, "a second BEGIN", code, st, "", 'T')
	if types[0] != "NoticeResponse" {
		t.Fatalf("a second BEGIN: %v, want the 25001 notice first", types)
	}
	_, code, st = d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT", code, st, "", 'I')
	if all := l.get(); len(all) != 0 {
		t.Fatalf("nothing should have run anywhere: %q", all)
	}
}

// A direct transaction: its first statement opens both sides (the cloud: BEGIN, the pause for
// the transaction, the gate; the organization: BEGIN, the statement); later statements of the
// organization run there, a statement on cloud tables alone in the cloud's open transaction;
// COMMIT commits the organization first, then the cloud.
func TestDirectTx(t *testing.T) {
	d, l := txSession(t, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	for _, q := range []string{"BEGIN ISOLATION LEVEL REPEATABLE READ", "SELECT title FROM documents WHERE organization_id = 1",
		"UPDATE documents SET title = 'y' WHERE organization_id = 1", "SELECT count(*) FROM projects",
		"INSERT INTO documents (organization_id, title) VALUES (1, 'z')"} {
		_, code, st := d.exchange(t, q)
		expectTx(t, q, code, st, "", 'T')
	}
	_, code, st := d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT", code, st, "", 'I')
	all := l.get()
	if !inOrder(all, "cloud: BEGIN ISOLATION LEVEL REPEATABLE READ", "cloud: SELECT FROM pg_advisory_xact_lock_shared(lokadb.org_lock(1))",
		"org1: BEGIN ISOLATION LEVEL REPEATABLE READ", "org1: SELECT title", "org1: UPDATE documents", "cloud: SELECT count(*) FROM projects",
		"cloud: WITH lokadb_k AS", "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, 'z', '41')", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("the two sides ran %q", all)
	}
	if first(all, "cloud: COMMIT") < first(all, "org1: COMMIT") {
		t.Fatalf("the cloud committed before the organization: %q", all)
	}
	if ranAny(all, "pg_advisory_lock_shared") || ranAny(all, "pg_advisory_unlock_shared") {
		t.Fatalf("a direct transaction holds the pause for the transaction, not per statement: %q", all)
	}
}

// A transaction whose first statement goes the cloud's way sends the held BEGIN to the cloud
// first and stays there; a statement of the organization in it goes through the cloud too.
func TestDirectTxCloudFirst(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	_, code, st := d.exchange(t, "SELECT count(*) FROM projects")
	expectTx(t, "a cloud statement first", code, st, "", 'T')
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	_, code, st = d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT", code, st, "", 'I')
	if all := l.get(); ranAny(all, "org1:") || !inOrder(all, "cloud: BEGIN", "cloud: SELECT count(*) FROM projects", "cloud: COMMIT") {
		t.Fatalf("ran %q", all)
	}
}

// A statement of another organization in a direct transaction would need the FDW, which does
// not see the transaction's rows there: 0A000, the transaction is aborted (25P02 up to
// ROLLBACK), both sides roll back, and the next transaction that starts the same way goes
// through the cloud; the shape is written down for every proxy.
func TestDirectTxRefused(t *testing.T) {
	d, l := txSession(t, func(m *Meta) { m.CloudFirstTable = true })
	d.exchange(t, "BEGIN")
	first := "SELECT title FROM documents WHERE organization_id = 1"
	d.exchange(t, first)
	_, code, st := d.exchange(t, "SELECT title FROM documents WHERE organization_id = 2")
	expectTx(t, "another organization", code, st, "0A000", 'E')
	_, code, st = d.exchange(t, "SELECT 1")
	expectTx(t, "after the refusal", code, st, "25P02", 'E')
	_, code, st = d.exchange(t, "ROLLBACK")
	expectTx(t, "ROLLBACK", code, st, "", 'I')
	all := l.get()
	if !inOrder(all, "org1: ROLLBACK", "cloud: ROLLBACK", "cloud: INSERT INTO lokadb.direct_cloud_first") {
		t.Fatalf("ran %q", all)
	}
	n := len(d.org1.log())
	d.exchange(t, "BEGIN")
	d.exchange(t, first)
	d.exchange(t, "COMMIT")
	if len(d.org1.log()) != n {
		t.Fatalf("the same shape went direct again: %q", d.org1.log()[n:])
	}
}

// An error in the organization's database aborts the direct transaction; its COMMIT rolls both
// sides back and answers ROLLBACK.
func TestDirectTxOrgError(t *testing.T) {
	d, l := txSession(t)
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "UPDATE") {
			return nil, "duplicate key", true
		}
		return nil, "", false
	}
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	_, code, st := d.exchange(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1")
	expectTx(t, "the failing UPDATE", code, st, "23505", 'E')
	types, code, st := d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT of the aborted transaction", code, st, "", 'I')
	if strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("COMMIT answered %v", types)
	}
	if all := l.get(); ranAny(all, "org1: COMMIT") || ranAny(all, "cloud: COMMIT") || !inOrder(all, "org1: ROLLBACK", "cloud: ROLLBACK") {
		t.Fatalf("ran %q", all)
	}
}

// SAVEPOINT, RELEASE, ROLLBACK TO run on both sides, the organization first; one the cloud
// refuses aborts the transaction.
func TestDirectTxSavepoint(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	for _, q := range []string{"SAVEPOINT a", "ROLLBACK TO a", "RELEASE a"} {
		_, code, st := d.exchange(t, q)
		expectTx(t, q, code, st, "", 'T')
	}
	if all := l.get(); !inOrder(all, "org1: SAVEPOINT a", "cloud: SAVEPOINT a", "org1: ROLLBACK TO a", "cloud: ROLLBACK TO a", "org1: RELEASE a", "cloud: RELEASE a") {
		t.Fatalf("ran %q", all)
	}
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "SAVEPOINT") {
			return nil, "savepoint refused", true
		}
		return nil, "", false
	}
	_, code, st := d.exchange(t, "SAVEPOINT b")
	expectTx(t, "a SAVEPOINT the cloud refuses", code, st, "XX000", 'E')
}

// The cloud's COMMIT fails after the organization's: 40003, the organization's side is
// committed (refs-diff finds it).
func TestDirectTxCloudCommitFails(t *testing.T) {
	d, l := txSession(t)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if q == "COMMIT" {
			return nil, "deferred constraint", true
		}
		return nil, "", false
	}
	d.exchange(t, "BEGIN")
	d.exchange(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1")
	d.exchange(t, "SELECT count(*) FROM projects") // work of the transaction's own in the cloud
	_, code, st := d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT", code, st, "40003", 'I')
	if all := l.get(); !inOrder(all, "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("ran %q", all)
	}
	// without work in the cloud (item 9): both commit at once, the cloud's failure loses nothing
	d.exchange(t, "BEGIN")
	d.exchange(t, "UPDATE documents SET title = 'w' WHERE organization_id = 1")
	_, code, st = d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT without work in the cloud", code, st, "", 'I')
}

// The organization's connection lost in a direct transaction: 08006, the transaction is
// aborted; ROLLBACK rolls the cloud back.
func TestDirectTxOrgLost(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	d.conns[0].Close() // the organization's side is gone
	_, code, st := d.exchange(t, "SELECT slug FROM documents WHERE organization_id = 1")
	expectTx(t, "a lost connection", code, st, "08006", 'E')
	_, code, st = d.exchange(t, "ROLLBACK")
	expectTx(t, "ROLLBACK", code, st, "", 'I')
	if all := l.get(); !inOrder(all, "cloud: ROLLBACK") {
		t.Fatalf("ran %q", all)
	}
}

// The extended protocol inside a deferred and a direct transaction: a statement prepared in the
// cloud (Parse, Describe, Sync) leaves the client's status 'T' although the cloud is outside a
// transaction; its Bind and Execute open the direct transaction; an aborted one answers 25P02
// up to the Sync.
func TestDirectTxExtended(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	sendAll(t, d.fe, &pgproto3.Parse{Name: "s", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Describe{ObjectType: 'S', Name: "s"}, &pgproto3.Sync{})
	_, code, st := d.answerAll(t)
	expectTx(t, "a statement prepared in a deferred transaction", code, st, "", 'T')
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_, code, st = d.answerAll(t)
	expectTx(t, "its Bind and Execute", code, st, "", 'T')
	if all := l.get(); !inOrder(all, "cloud: BEGIN", "org1: BEGIN", "org1: SELECT title FROM documents WHERE organization_id = 1") {
		t.Fatalf("ran %q", all)
	}
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 2") // refused: aborted
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_, code, st = d.answerAll(t)
	expectTx(t, "a batch in an aborted transaction", code, st, "25P02", 'E')
	d.exchange(t, "ROLLBACK")
	// the same shape again, by the extended protocol: through the cloud from its first statement
	n := len(d.org1.log())
	d.exchange(t, "BEGIN")
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_, code, st = d.answerAll(t)
	expectTx(t, "the refused shape again", code, st, "", 'T')
	d.exchange(t, "COMMIT")
	if org := d.org1.log(); len(org) != n {
		t.Fatalf("the refused shape went direct again: %q", org[n:])
	}
}

// A whitelisted SET in a direct transaction runs once on each side and the client gets one
// answer; one off the whitelist is refused once and never reaches the cloud (block 9's review
// of 8a.2, 1).
func TestDirectTxSettings(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	types, code, st := d.exchange(t, "SET LOCAL TimeZone = 'UTC'")
	expectTx(t, "a whitelisted SET LOCAL", code, st, "", 'T')
	if strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("a whitelisted SET LOCAL answered %v", types)
	}
	all := l.get()
	if n := strings.Count(strings.Join(all, "\n"), "cloud: SET LOCAL TimeZone"); n != 1 || !inOrder(all, "org1: SET LOCAL TimeZone", "cloud: SET LOCAL TimeZone") {
		t.Fatalf("the SET ran in the cloud %d times: %q", n, all)
	}
	types, code, st = d.exchange(t, "SET LOCAL row_security = off")
	expectTx(t, "a SET off the whitelist", code, st, "0A000", 'E')
	if strings.Join(types, ",") != "ErrorResponse,ReadyForQuery" || ranAny(l.get(), "row_security") {
		t.Fatalf("a SET off the whitelist answered %v, ran %q", types, l.get())
	}
}

// Session settings set after a savepoint go with it at ROLLBACK TO: at COMMIT only the first is
// kept for the session's later direct connections (block 9's review of 8a.2, 2).
func TestDirectTxSettingsSavepoint(t *testing.T) {
	d, _ := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	for _, q := range []string{"SET TimeZone = 'UTC'", "SAVEPOINT a", "SET DateStyle = 'ISO'", "ROLLBACK TO a", "COMMIT"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	var kept []string
	for _, x := range d.s.direct.sets {
		kept = append(kept, x.sql)
	}
	if strings.Join(kept, "|") != "SET TimeZone = 'UTC'" {
		t.Fatalf("kept for later connections: %q", kept)
	}
}

// The cloud's deferred constraints are checked before the organization commits: one that fails
// rolls both sides back, and the client gets the cloud's error (block 9's review of 8a.2, 3).
func TestDirectTxDeferredConstraint(t *testing.T) {
	d, l := txSession(t)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "SET CONSTRAINTS ALL IMMEDIATE") {
			return nil, "insert or update on table violates foreign key constraint", true
		}
		return nil, "", false
	}
	d.exchange(t, "BEGIN")
	d.exchange(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1")
	d.exchange(t, "SELECT count(*) FROM projects")
	_, code, st := d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT with a failing deferred constraint", code, st, "XX000", 'I')
	if all := l.get(); ranAny(all, "org1: COMMIT") || !inOrder(all, "cloud: SET CONSTRAINTS ALL IMMEDIATE", "org1: ROLLBACK", "cloud: ROLLBACK") {
		t.Fatalf("ran %q", all)
	}
}

// The client gone in the middle of a direct transaction: neither side commits (the session's
// connections close, the servers roll back and release the pause).
func TestDirectTxClientGone(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1")
	d.fe.Send(&pgproto3.Terminate{})
	_ = d.fe.Flush()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && d.conns[0] != nil {
		time.Sleep(20 * time.Millisecond)
		if _, err := d.conns[0].Read(make([]byte, 1)); err != nil {
			break
		}
	}
	if all := l.get(); ranAny(all, "COMMIT") {
		t.Fatalf("a side committed after the client left: %q", all)
	}
}

// The gate's check failing in the cloud inside an open direct transaction (a cancel while it
// ran, an error of the check): the client gets the cloud's error, the transaction is aborted,
// and ROLLBACK rolls both sides back (block 9's review of 8a.2).
func TestDirectTxGateErrorInTx(t *testing.T) {
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "SELECT coalesce((SELECT profile FROM lokadb.orgs WHERE org = ") {
			return nil, "canceling statement due to user request", true
		}
		return nil, "", false
	}
	// an UPDATE: a check of its own (a read of the same tables passed already: not checked again)
	_, code, st := d.exchange(t, "UPDATE documents SET slug = 'z' WHERE organization_id = 1")
	expectTx(t, "the gate failing in the cloud", code, st, "XX000", 'E')
	_, code, st = d.exchange(t, "ROLLBACK")
	expectTx(t, "ROLLBACK", code, st, "", 'I')
	if all := l.get(); ranAny(all, "org1: UPDATE documents SET slug") || !inOrder(all, "org1: ROLLBACK", "cloud: ROLLBACK") {
		t.Fatalf("ran %q", all)
	}
}

// Learn first (coordinator's decision of 25.10): a form not learnt yet goes the cloud's way
// and is watched; two transactions of it committed in a row that fit make it direct; a form
// that does not fit (a statement of another organization) never goes direct and is never
// refused; a metadata change makes the proxy learn again.
func TestDirectTxLearn(t *testing.T) {
	d, l := txSession(t)
	shape := []string{"BEGIN", "SELECT slug FROM documents WHERE organization_id = 1", "UPDATE documents SET title = 'y' WHERE organization_id = 1", "COMMIT"}
	run := func() {
		for _, q := range shape {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	run()
	run()
	if ranAny(l.get(), "org1:") {
		t.Fatalf("a form went direct before it was learnt: %q", l.get())
	}
	run()
	if all := l.get(); !inOrder(all, "org1: BEGIN", "org1: SELECT slug", "org1: UPDATE documents", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("the learnt form did not go direct: %q", all)
	}
	// a form that does not fit: a statement of organization 2 after organization 1's
	n := len(d.org1.log())
	for i := 0; i < 3; i++ {
		for _, q := range []string{"BEGIN", "SELECT slug FROM documents WHERE organization_id = 1 AND id > 0", "SELECT slug FROM documents WHERE organization_id = 2", "COMMIT"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("an unfit form, %s: %q (it must never be refused)", q, code)
			}
		}
	}
	if len(d.org1.log()) != n {
		t.Fatalf("an unfit form went direct: %q", d.org1.log()[n:])
	}
	// a metadata change: learnt again
	d.s.srv.directOK.reset("db")
	n = len(d.org1.log())
	run()
	if len(d.org1.log()) != n {
		t.Fatalf("a form went direct right after a metadata change: %q", d.org1.log()[n:])
	}
}

// The form DP8 runs, by the extended protocol as pgx sends it: an UPDATE, a read of a cloud
// table, an INSERT … RETURNING; learnt in two cloud transactions, the third goes direct.
func TestDirectTxLearnExtended(t *testing.T) {
	d, l := txSession(t, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	prep := func(name, sql string) {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Describe{ObjectType: 'S', Name: name}, &pgproto3.Sync{})
		if _, code, _ := d.answerAll(t); code != "" {
			t.Fatalf("prepare %s: %q", name, code)
		}
	}
	bind := func(name string, args ...string) {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if _, code, st := d.answerAll(t); code != "" || st != 'T' {
			t.Fatalf("%s: %q %q", name, code, st)
		}
	}
	for i := 0; i < 3; i++ {
		d.exchange(t, "BEGIN")
		if i == 0 {
			prep("u", "UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3")
		}
		bind("u", "x", "1", "5")
		if i == 0 {
			prep("p", "SELECT count(*) FROM projects")
		}
		bind("p")
		if i == 0 {
			prep("i", "INSERT INTO documents (organization_id, title) VALUES ($1, $2) RETURNING id")
		}
		bind("i", "1", "y")
		if _, code, st := d.exchange(t, "COMMIT"); code != "" || st != 'I' {
			t.Fatalf("COMMIT %d: %q %q", i, code, st)
		}
		if i < 2 && ranAny(l.get(), "org1:") {
			t.Fatalf("went direct before it was learnt (run %d): %q", i, l.get())
		}
	}
	if all := l.get(); !inOrder(all, "org1: BEGIN", "org1: UPDATE documents", "cloud: SELECT count(*) FROM projects", "org1: INSERT INTO documents", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("the learnt form did not go direct: %q", all)
	}
}

// Learning, block 9's review: a form whose transactions alternate fit and unfit never reaches
// learnRuns; a transaction of an organization the direct path does not serve teaches nothing;
// a reset and learning again do not grow the order.
func TestDirectTxLearnEdges(t *testing.T) {
	var o directOK
	key := "db/x"
	o.result(key, true)
	o.result(key, false)
	o.result(key, true)
	if o.ok(key) {
		t.Fatal("fit, unfit, fit: learnt")
	}
	for i := 0; i < 100; i++ {
		o.result(key, true)
		o.result(key, true)
		o.reset("db")
	}
	if len(o.order) != 0 || len(o.runs) != 0 {
		t.Fatalf("after resets: order %d, runs %d", len(o.order), len(o.runs))
	}
	// organization 2 is not in fdw+direct: its transactions of the form teach nothing
	d, l := txSession(t)
	shape := func(org string) {
		for _, q := range []string{"BEGIN", "SELECT slug FROM documents WHERE organization_id = " + org, "COMMIT"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	shape("1")
	shape("2")
	shape("1")
	shape("1")
	if all := l.get(); !inOrder(all, "org1: BEGIN", "org1: SELECT slug FROM documents WHERE organization_id = 1") {
		t.Fatalf("organization 2's transaction undid the learning: %q", all)
	}
}

// A DELETE by the extended protocol inside a direct transaction: its keys come back, their refs
// rows are deleted in the cloud's open transaction, the client gets its answer (DP18 hung: the
// answer was taken for a BEGIN's).
func TestDirectTxDeleteBatch(t *testing.T) {
	d, l := txSession(t, keyed)
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") && strings.HasSuffix(q, "RETURNING id") {
			return [][]string{{"5"}}, "", true
		}
		return nil, "", false
	}
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	sendAll(t, d.fe, &pgproto3.Parse{Name: "del", Query: "DELETE FROM documents WHERE organization_id = $1 AND slug = $2"}, &pgproto3.Sync{})
	if _, code, _ := d.answerAll(t); code != "" {
		t.Fatal(code)
	}
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "del", Parameters: [][]byte{[]byte("1"), []byte("a")}}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
	types, code, st := d.answerAll(t)
	if code != "" || st != 'T' || strings.Join(types, ",") != "BindComplete,NoData,CommandComplete,ReadyForQuery" {
		t.Fatalf("DELETE in a direct transaction: %v %q %q", types, code, st)
	}
	d.exchange(t, "COMMIT")
	// a write: the commits in turn, the cloud's deferred constraints first (item 9 keeps them so)
	if all := l.get(); !inOrder(all, "org1: DELETE FROM documents", "cloud: DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (5)", "cloud: SET CONSTRAINTS ALL IMMEDIATE", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("ran %q", all)
	}
}

// The cooperative mode in transactions (8a.2, the second step): SET LOCAL lokadb.tenant right
// after BEGIN is held with it and does not make the transaction's form (every transaction of
// such an application starts with it); a form learnt under the named organization goes
// direct with statements that have no conditions of their own, the SET LOCAL in the cloud's
// open transaction after its BEGIN.
func TestDirectTxTenant(t *testing.T) {
	d, l := txSession(t)
	d.s.srv.cfg.Tenant = &TenantConfig{} // local_only, as behind a pooler
	run := func(stmt string) {
		for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1", stmt, "UPDATE documents SET title = 'y'", "COMMIT"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	run("SELECT slug FROM documents")
	run("SELECT slug FROM documents")
	if ranAny(l.get(), "org1:") {
		t.Fatalf("went direct before it was learnt: %q", l.get())
	}
	if all := l.get(); !inOrder(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = 1", "cloud: SELECT slug FROM") {
		t.Fatalf("the held SET LOCAL did not reach the cloud after its BEGIN: %q", all)
	}
	n := len(l.get())
	run("SELECT slug FROM documents")
	if all := l.get()[n:]; !inOrder(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = 1", "org1: BEGIN", "org1: SELECT slug FROM documents",
		"org1: UPDATE documents", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("the learnt form did not go direct: %q", all)
	}
	// another form after the same SET LOCAL is not learnt by it
	n = len(d.org1.log())
	run("SELECT body FROM documents")
	if len(d.org1.log()) != n {
		t.Fatalf("a form went direct, learnt by its SET LOCAL alone: %q", d.org1.log()[n:])
	}
	// SET LOCAL in the middle: neither fit nor unfit for learning; in the direct transaction
	// it runs in the cloud's, and the statement after it goes direct under it
	mid := []string{"BEGIN", "SELECT slug FROM documents WHERE organization_id = 1 AND id > 0", "SET LOCAL lokadb.tenant = 1", "UPDATE documents SET title = 'm'", "COMMIT"}
	for i := 0; i < 3; i++ {
		n = len(l.get())
		for _, q := range mid {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	if all := l.get()[n:]; !inOrder(all, "org1: BEGIN", "org1: SELECT slug", "cloud: SET LOCAL lokadb.tenant = 1", "org1: UPDATE documents SET title = 'm'", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("a form with SET LOCAL in the middle, learnt: %q", all)
	}
	// the transaction's tenant ends with it: a statement after COMMIT names none (the fanout
	// policy refuses it in the cloud)
	n = len(d.org1.log())
	if _, code, _ := d.exchange(t, "SELECT slug FROM documents"); code != "0A000" || len(d.org1.log()) != n {
		t.Fatalf("after COMMIT: %q, org1 %q", code, d.org1.log()[n:])
	}
}

// DP19's form as pgx sends it: SET LOCAL by a simple query, the statements by the extended
// protocol (Parse, Describe, Sync first; then Bind, Execute, Sync).
func TestDirectTxTenantExtended(t *testing.T) {
	d, l := txSession(t)
	prep := func(name, sql string) {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Describe{ObjectType: 'S', Name: name}, &pgproto3.Sync{})
		if _, code, _ := d.answerAll(t); code != "" {
			t.Fatalf("prepare %s: %q", name, code)
		}
	}
	bind := func(name string, args ...string) {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if _, code, st := d.answerAll(t); code != "" || st != 'T' {
			t.Fatalf("%s: %q %q", name, code, st)
		}
	}
	for i := 0; i < 3; i++ {
		for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
		if i == 0 {
			prep("c", "SELECT count(*) FROM documents")
		}
		bind("c")
		if i == 0 {
			prep("u", "UPDATE documents SET title = $1 WHERE title LIKE 'dp%'")
		}
		bind("u", "x")
		if _, code, st := d.exchange(t, "COMMIT"); code != "" || st != 'I' {
			t.Fatalf("COMMIT %d: %q %q", i, code, st)
		}
		if i < 2 && ranAny(l.get(), "org1:") {
			t.Fatalf("went direct before it was learnt (run %d): %q", i, l.get())
		}
	}
	if all := l.get(); !inOrder(all, "org1: BEGIN", "org1: SELECT count(*) FROM documents", "org1: UPDATE documents", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("the learnt form did not go direct: %q", all)
	}
}

// A form learnt ahead (lokadb.direct_learnt, from check -profile fdw+direct -direct-forms):
// direct from its first run; under a named organization only the "#named" entry counts.
func TestDirectTxLearntAhead(t *testing.T) {
	d, l := txSession(t)
	first := "SELECT slug FROM documents WHERE organization_id = 1 AND id > 3"
	d.s.meta.Learnt = map[string]bool{fingerprint(first): true, fingerprint("SELECT body FROM documents") + "#named": true}
	for _, q := range []string{"BEGIN", first, "COMMIT"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	if all := l.get(); !inOrder(all, "org1: BEGIN", "org1: SELECT slug", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("a form learnt ahead did not go direct at once: %q", all)
	}
	n := len(d.org1.log())
	for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT body FROM documents", "COMMIT"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	if !ranAny(d.org1.log()[n:], "SELECT body FROM documents") {
		t.Fatalf("a named form learnt ahead did not go direct: %q", d.org1.log()[n:])
	}
}

// Block 9's review: inside organization 1's direct transaction SET LOCAL lokadb.tenant names
// another organization, or '*': the next statement on LokaDB tables goes neither to the
// transaction's connection under the other name nor anywhere direct; it is refused (0A000),
// and the form goes the cloud's way from then on.
func TestDirectTxTenantChanged(t *testing.T) {
	for _, v := range []string{"2", "'*'"} {
		d, l := txSession(t, func(m *Meta) {
			m.CloudFirstTable = true
			m.Profiles["2"] = ProfileFDWDirect // organization 2 could go direct too, on its own connection
		})
		d.s.srv.cfg.Tenant = &TenantConfig{}
		d.s.srv.cfg.CustomersApp["db"]["2"] = "host=org1" // reachable: only the transaction's organization keeps it off
		first := "SELECT title FROM documents WHERE organization_id = 1"
		for _, q := range []string{"BEGIN", first, "SET LOCAL lokadb.tenant = " + v} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %s: %q", v, q, code)
			}
		}
		n := len(d.org1.log())
		if _, code, st := d.exchange(t, "SELECT slug FROM documents"); code != "0A000" || st != 'E' {
			t.Fatalf("%s: after the change: %q %q, log %q", v, code, st, l.get())
		}
		if len(d.org1.log()) > n+1 { // the organization's ROLLBACK alone
			t.Fatalf("%s: went to the transaction's connection: %q", v, d.org1.log()[n:])
		}
		if !inOrder(l.get(), "cloud: SET LOCAL lokadb.tenant = "+v) {
			t.Fatalf("%s: the SET LOCAL did not run in the cloud's transaction: %q", v, l.get())
		}
		d.exchange(t, "ROLLBACK")
		if _, cloud := d.s.cloudFirstFor(first); !cloud {
			t.Fatalf("%s: the form does not go the cloud's way after the refusal", v)
		}
	}
}

// Block 18's probe: pgx in a transaction names the organization with set_config by the
// extended protocol (prepared first: Parse, Describe, Sync while the BEGIN is deferred), then
// a statement without conditions of its own: it runs under the named organization, never
// refused as a fanout.
func TestDirectTxSetConfigExtended(t *testing.T) {
	d, _ := txSession(t)
	prep := func(name, sql string) {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Describe{ObjectType: 'S', Name: name}, &pgproto3.Sync{})
		if _, code, _ := d.answerAll(t); code != "" {
			t.Fatalf("prepare %s: %q", name, code)
		}
	}
	bind := func(name string, args ...string) {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if _, code, st := d.answerAll(t); code != "" || st != 'T' {
			t.Fatalf("%s: %q %q", name, code, st)
		}
	}
	for i := 0; i < 3; i++ {
		d.exchange(t, "BEGIN")
		if i == 0 {
			prep("t", "SELECT set_config('lokadb.tenant', $1, true)")
		}
		bind("t", "1")
		if i == 0 {
			prep("c", "SELECT count(*) FROM documents WHERE title LIKE '%'")
		}
		bind("c")
		if _, code, _ := d.exchange(t, "COMMIT"); code != "" {
			t.Fatalf("COMMIT %d: %q", i, code)
		}
	}
}
