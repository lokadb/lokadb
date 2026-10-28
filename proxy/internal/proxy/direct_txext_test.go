package proxy

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// COMMIT and ROLLBACK by the extended protocol (Parse, Bind, Execute, Sync) in a direct
// transaction: both sides end, as with the simple query; never the cloud's alone.
func TestDirectTxExtendedEnd(t *testing.T) {
	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		d, l := txSession(t)
		for _, q := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
		sendAll(t, d.fe, &pgproto3.Parse{Query: end}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		types, code, st := d.answerAll(t)
		if code != "" || st != 'I' {
			t.Fatalf("%s: %q %q %q", end, types, code, st)
		}
		if !ranAny(l.get(), "org1: "+end) || !ranAny(l.get(), "cloud: "+end) { // a read alone: both at once (item 9)
			t.Fatalf("%s did not end both sides: %q", end, l.get())
		}
		if d.s.direct.tx != nil {
			t.Fatalf("%s: the direct transaction is still open", end)
		}
	}
}

// An aborted direct transaction ends with ROLLBACK by the extended protocol (it got 25P02 for
// ever before); SAVEPOINT and ROLLBACK TO by it run on both sides; a transaction statement
// among other statements of a pipeline is refused, never the cloud's alone.
func TestDirectTxExtendedAbortedAndSavepoint(t *testing.T) {
	ext := func(d *directTest, q string) (string, byte) {
		sendAll(t, d.fe, &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		_, code, st := d.answerAll(t)
		return code, st
	}
	d, l := txSession(t)
	d.exchange(t, "BEGIN")
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	if code, _ := ext(d, "SAVEPOINT a"); code != "" {
		t.Fatalf("SAVEPOINT: %q", code)
	}
	d.s.direct.tx.aborted = true // as after a failed statement
	if code, st := ext(d, "ROLLBACK TO a"); code != "" || st != 'T' {
		t.Fatalf("ROLLBACK TO: %q %q", code, st)
	}
	d.s.direct.tx.aborted = true
	if code, st := ext(d, "ROLLBACK"); code != "" || st != 'I' || d.s.direct.tx != nil {
		t.Fatalf("ROLLBACK of an aborted transaction: %q %q", code, st)
	}
	if !inOrder(l.get(), "org1: SAVEPOINT a", "cloud: SAVEPOINT a", "org1: ROLLBACK TO a", "org1: ROLLBACK", "cloud: ROLLBACK") {
		t.Fatalf("both sides: %q", l.get())
	}
	// COMMIT among other statements: refused
	d2, l2 := txSession(t)
	d2.exchange(t, "BEGIN")
	d2.exchange(t, "SELECT title FROM documents WHERE organization_id = 1")
	sendAll(t, d2.fe, &pgproto3.Parse{Query: "SELECT 1"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, _ := d2.answerAll(t); code != "0A000" {
		t.Fatalf("COMMIT in a pipeline: %q, %q", code, l2.get())
	}
	if ranAny(l2.get(), "cloud: COMMIT") {
		t.Fatalf("the cloud committed alone: %q", l2.get())
	}
}

// JDBC with autocommit off: BEGIN by the extended protocol at the head of the transaction's
// first batch, the statement (prepared, a named one past prepareThreshold) after it, COMMIT by
// the extended protocol in a batch of its own. Learnt through the cloud twice, then direct.
func TestDirectTxExtendedBegin(t *testing.T) {
	d, l := txSession(t)
	sendAll(t, d.fe, &pgproto3.Parse{Name: "S_1", Query: "UPDATE documents SET body = $1 WHERE organization_id = $2"}, &pgproto3.Sync{})
	if code := errorCode(t, d.fe); code != "" {
		t.Fatalf("prepare: %s", code)
	}
	for i := 0; i < 3; i++ {
		n := len(l.get())
		sendAll(t, d.fe, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
			&pgproto3.Bind{PreparedStatement: "S_1", Parameters: [][]byte{[]byte("j"), []byte("1")}}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		types, code, st := d.answerAll(t)
		if code != "" || st != 'T' || types[0] != "ParseComplete" || types[1] != "BindComplete" || types[2] != "CommandComplete" {
			t.Fatalf("run %d, BEGIN and the UPDATE: %q %q %q", i, types, code, st)
		}
		sendAll(t, d.fe, &pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if _, code, st := d.answerAll(t); code != "" || st != 'I' {
			t.Fatalf("run %d, COMMIT: %q %q", i, code, st)
		}
		all := l.get()[n:]
		if i < 2 && ranAny(all, "org1:") {
			t.Fatalf("run %d went direct before it was learnt: %q", i, all)
		}
		if i == 2 && !inOrder(all, "cloud: BEGIN", "org1: BEGIN", "org1: UPDATE documents SET body", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
			t.Fatalf("the learnt form did not go direct: %q", all)
		}
	}
	// BEGIN alone in its batch: answered, the transaction deferred
	sendAll(t, d.fe, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, st := d.answerAll(t); code != "" || st != 'T' || d.s.direct.tx == nil {
		t.Fatalf("BEGIN alone: %q %q", code, st)
	}
}

// Block 9's review: after an extended COMMIT the proxy answered itself, a Bind of the unnamed
// statement without a Parse does not run the older SELECT the cloud still has; BEGIN's mode
// (ISOLATION LEVEL) reaches the organization's database too.
func TestDirectTxExtendedUnnamedGone(t *testing.T) {
	d, l := txSession(t)
	sendAll(t, d.fe, &pgproto3.Parse{Query: "SELECT 42"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, _ := d.answerAll(t); code != "" {
		t.Fatalf("SELECT 42: %q", code)
	}
	sendAll(t, d.fe, &pgproto3.Parse{Query: "BEGIN ISOLATION LEVEL REPEATABLE READ"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, st := d.answerAll(t); code != "" || st != 'T' {
		t.Fatalf("BEGIN: %q %q", code, st)
	}
	if _, code, _ := d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" {
		t.Fatalf("the statement: %q", code)
	}
	if !inOrder(l.get(), "cloud: BEGIN ISOLATION LEVEL REPEATABLE READ", "org1: BEGIN ISOLATION LEVEL REPEATABLE READ") {
		t.Fatalf("BEGIN's mode: %q", l.get())
	}
	sendAll(t, d.fe, &pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, _ := d.answerAll(t); code != "" {
		t.Fatalf("COMMIT: %q", code)
	}
	n := len(d.cloud.log())
	sendAll(t, d.fe, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_, code, _ := d.answerAll(t)
	if code != "26000" || ranAny(d.cloud.log()[n:], "SELECT 42") {
		t.Fatalf("a Bind of the unnamed statement after COMMIT: %q, cloud %q", code, d.cloud.log()[n:])
	}
}
