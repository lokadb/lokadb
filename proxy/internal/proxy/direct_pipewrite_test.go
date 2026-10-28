package proxy

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 9 (from block 10's queue): a pipeline with writes of 8b (JDBC executeBatch of INSERTs,
// pgx.Batch) goes direct as one transaction up to its Sync: the pause, every check and every
// INSERT's refs rows in one round trip of the cloud's transaction, all the statements in one
// exchange inside the organization's, a DELETE's refs rows after it, then both commits, the
// organization's first; an error in the organization's database rolls both back. Before, a
// pipeline with a write went the cloud's way whole.
func TestDirectPipelineWrites(t *testing.T) {
	d, l := txSession(t, keyed)
	var key atomic.Int32
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{itoa(41 + int(key.Add(1)) - 1)}}, "", true
		}
		return nil, "", false
	}
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") {
			return [][]string{{"7"}}, "", true
		}
		return nil, "", false
	}
	for name, sql := range map[string]string{
		"i": "INSERT INTO documents (organization_id, title) VALUES ($1, $2)",
		"a": "SELECT title FROM documents WHERE organization_id = $1",
		"x": "DELETE FROM documents WHERE organization_id = $1 AND id = $2",
	} {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Sync{})
		if code := errorCode(t, d.fe); code != "" {
			t.Fatalf("prepare %s: %s", name, code)
		}
	}
	unit := func(name string, args ...string) []pgproto3.FrontendMessage {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Execute{}}
	}
	pipe := func(units ...[]pgproto3.FrontendMessage) ([]string, string, byte) {
		var ms []pgproto3.FrontendMessage
		for _, u := range units {
			ms = append(ms, u...)
		}
		sendAll(t, d.fe, append(ms, &pgproto3.Sync{})...)
		return d.answerAll(t)
	}
	n := len(l.get())
	types, code, st := pipe(unit("i", "1", "x"), unit("i", "1", "y"), unit("a", "1"), unit("x", "1", "7"))
	if code != "" || st != 'I' || strings.Count(strings.Join(types, ","), "CommandComplete") != 4 || strings.Contains(strings.Join(types, ","), "ParseComplete") {
		t.Fatalf("pipeline: %v %q %q; %q", types, code, st, l.get()[n:])
	}
	all := l.get()[n:]
	if !inOrder(all, "cloud: BEGIN", "cloud: SELECT FROM pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "cloud: WITH lokadb_k", "cloud: WITH lokadb_k",
		"org1: BEGIN", "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, x, '41')", "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, y, '42')",
		"org1: SELECT title FROM documents WHERE organization_id = 1", "org1: DELETE FROM documents", "cloud: DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (7)",
		"cloud: SET CONSTRAINTS ALL IMMEDIATE", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("the pipeline's transaction: %q", all)
	}
	for _, q := range all {
		if strings.HasPrefix(q, "cloud: INSERT INTO documents") || strings.HasPrefix(q, "cloud: SELECT title") || strings.Contains(q, "pg_advisory_lock_shared(") {
			t.Fatalf("in the cloud or outside the transaction: %q in %q", q, all)
		}
	}

	// an error in the organization's database: what came before, the error, both sides rolled back
	n = len(l.get())
	d.org1.failExec = map[int]bool{d.org1.execN + 3: true} // BEGIN (extended, ahead of them), the first INSERT, then the second fails
	types, code, st = pipe(unit("i", "1", "p"), unit("i", "1", "q"), unit("a", "1"))
	if code != "22012" || st != 'I' || strings.Join(types, ",") != "BindComplete,CommandComplete,BindComplete,ErrorResponse,ReadyForQuery" {
		t.Fatalf("failed pipeline: %v %q %q", types, code, st)
	}
	all = l.get()[n:]
	if !inOrder(all, "org1: ROLLBACK", "cloud: ROLLBACK") || ranAny(all, "COMMIT") {
		t.Fatalf("failed pipeline: %q", all)
	}
	d.org1.failExec = nil

	// inside the client's transaction (pgx tx.SendBatch): its gate opens the transaction, the
	// pipeline runs in it, COMMIT ends it
	n = len(l.get())
	d.exchange(t, "BEGIN")
	if types, code, st = pipe(unit("a", "1"), unit("i", "1", "t1")); code != "" || st != 'T' {
		t.Fatalf("pipeline in a transaction: %v %q %q", types, code, st)
	}
	if _, code, _ := d.exchange(t, "COMMIT"); code != "" {
		t.Fatalf("COMMIT: %q", code)
	}
	if all = l.get()[n:]; !inOrder(all, "cloud: BEGIN", "cloud: WITH lokadb_k", "org1: BEGIN", "org1: SELECT title", "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, t1,",
		"org1: COMMIT", "cloud: COMMIT") || strings.Count(strings.Join(all, "|"), "org1: BEGIN") != 1 {
		t.Fatalf("pipeline in a transaction: %q", all)
	}

	// JDBC before prepareThreshold: one unnamed Parse, its statement bound again
	n = len(l.get())
	d.cloud.describeTypes = []uint32{20, 25}
	sendAll(t, d.fe, &pgproto3.Parse{Query: "INSERT INTO documents (organization_id, title) VALUES ($1, $2)"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("1"), []byte("u1")}}, &pgproto3.Execute{},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("1"), []byte("u2")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	types, code, st = d.answerAll(t)
	if code != "" || st != 'I' || strings.Join(types, ",") != "ParseComplete,BindComplete,CommandComplete,BindComplete,CommandComplete,ReadyForQuery" {
		t.Fatalf("unnamed pipeline: %v %q %q; %q", types, code, st, l.get()[n:])
	}
	if all = l.get()[n:]; !inOrder(all, "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, u1,", "org1: INSERT INTO documents (organization_id, title, id) VALUES (1, u2,", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("unnamed pipeline: %q", all)
	}
}
