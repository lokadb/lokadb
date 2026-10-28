package proxy

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Second independent review of the protocol core (tasks/review3/protocol.md): the review's
// proofs of the findings block 9 closes (P2, P3, P5, P6), now passing.

// P3-1: the proxy's own statement lokadb_s (set_config of lokadb.org, PF3: prepared once per
// connection) is a name the client may take. The client closes it and Parses its own text
// under it; the proxy keeps Binding lokadb_s as if it were its set_config, so lokadb.org is
// not set for the next statement: it runs under the organization of the one before.
func TestReview3ServiceStatementHijacked(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Name: "q", Query: "SELECT title FROM documents WHERE organization_id = $1"},
		&pgproto3.Bind{PreparedStatement: "q", Parameters: [][]byte{[]byte("5")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	// the client's own statement under the proxy's name
	sendAll(t, fe, &pgproto3.Close{ObjectType: 'S', Name: "lokadb_s"},
		&pgproto3.Parse{Name: "lokadb_s", Query: "SELECT $1::text AS hijacked"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "q", Parameters: [][]byte{[]byte("7")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	log := f.log()
	if countContaining(log, "set_config('lokadb.org', 7") == 0 {
		t.Fatalf("organization 7's statement ran without lokadb.org = 7 (the proxy bound the client's lokadb_s: %d times); upstream ran %q",
			countContaining(log, "hijacked"), log)
	}
}

// P3-4: a FunctionCall (lo_open) in a database with LokaDB tables goes with the proxy's
// set_config of -1 as a simple Query in front of it. A simple query destroys the unnamed
// statement (exec_simple_query), a fastpath call does not: the client's Bind of its unnamed
// statement after the call fails with 26000 through the proxy, not on Postgres. The proxy's own
// queries elsewhere go through the extended protocol for this reason (captureExt).
func TestReview3FunctionCallDropsUnnamed(t *testing.T) {
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

// P3-6: the shared metadata cache: a session that reads an entry holds e.loading until its read
// returns; the others whose stamp differs wait on it with no timeout. A read that never returns
// (a panic in it, recovered by the session or by survive at connect; a cloud connection that
// hangs: captureExt has no deadline) leaves every later connection and reload of that database
// and role waiting for good.
func TestReview3MetaCacheWaitsForAStuckRead(t *testing.T) {
	c := newMetaCache()
	stamp := func(s string) queryFunc {
		return func(sql string) ([][]string, error) {
			if sql == metaStampSQL {
				return [][]string{{s, "app"}}, nil
			}
			panic("a bug in the read")
		}
	}
	func() {
		defer func() { _ = recover() }() // as session.run and Server.survive do
		_, _, _ = c.load("db", "app", 0, stamp("s1"))
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		_, _, _ = c.load("db", "app", 0, stamp("s2"))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the next connection of the role waits for a read that ended long ago (e.loading is never closed)")
	}
}

// P3-3: COPY FROM STDIN through Execute: the Sync after the Execute is ignored in copy-in mode
// (the proxy drops it from its queue), so after CopyDone the COPY's implicit transaction is
// still open upstream until the client's next Sync. The proxy takes the connection for synced
// (unsynced is false after the batch that ended with that Sync): a query of its own at the
// client's Sync (a metadata reload here; a lookup for a statement the client pipelines after
// CopyDone) goes with a Sync of its own, inside the COPY's transaction. On Postgres that Sync
// commits the COPY before the client's Sync, and an error of the proxy's query rolls it back
// while the client has CommandComplete "COPY n" and then a clean ReadyForQuery.
func TestReview3CopyExtendedSyncedTooEarly(t *testing.T) {
	f := &fakeUpstream{}
	fe, c, s := protoSessionS(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "COPY notes FROM STDIN"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for n := 0; n < 3; n++ { // ParseComplete, BindComplete, CopyInResponse
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	sendAll(t, fe, &pgproto3.CopyData{Data: []byte("1\tx\n")}, &pgproto3.CopyDone{})
	if m, err := fe.Receive(); err != nil {
		t.Fatal(err)
	} else if _, ok := m.(*pgproto3.CommandComplete); !ok {
		t.Fatalf("copy end: %T", m)
	}
	syncsBefore := f.syncs()
	s.srv.schemaVersion("db").Add(1) // a schema change replayed by another session: a reload is due
	sendAll(t, fe, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if n := f.syncs() - syncsBefore; n != 1 {
		t.Fatalf("between CopyDone and the client's Sync the proxy sent %d Sync(s) of its own: the COPY's implicit transaction ended before the client's Sync; upstream ran %q", n-1, f.log())
	}
}

// P3-3, the same after a pipeline cut by Flush: the client's INSERT ran in an implicit
// transaction that is open upstream; its next message is a simple query. handleQuery clears
// unsynced before its reload (and lookups), which then go with a Sync of their own inside that
// transaction: an error of theirs rolls the INSERT back, and the client, which has its
// CommandComplete, is never told.
func TestReview3FlushThenQuerySyncsInside(t *testing.T) {
	f := &fakeUpstream{}
	fe, c, s := protoSessionS(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "INSERT INTO notes VALUES (1)"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for n := 0; n < 3; n++ { // ParseComplete, BindComplete, CommandComplete
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	syncsBefore := f.syncs()
	s.srv.schemaVersion("db").Add(1) // a reload is due
	sendAll(t, fe, &pgproto3.Query{String: "SELECT 1"})
	expectMsgs(t, fe, c, 1)
	if n := f.syncs() - syncsBefore; n != 0 {
		t.Fatalf("the client sent no Sync after its INSERT, the proxy sent %d inside its implicit transaction before the client's query; upstream ran %q", n, f.log())
	}
}

// P2 by SQL: PREPARE, EXECUTE and DEALLOCATE of the proxy's names are refused; a client's
// Parse under such a name goes upstream under a name of its own (lokadb_u_*), never the proxy's.
func TestOwnStatementNames(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	for _, q := range []string{"DEALLOCATE lokadb_s", "PREPARE lokadb_ls AS SELECT 1", "EXECUTE lokadb_s('7')"} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		if code := errorCode(t, fe); code == "" {
			t.Errorf("%q: no error; upstream ran %q", q, f.log())
		}
	}
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Parse{Name: "lokadb_s", Query: "SELECT 1"}, &pgproto3.Bind{PreparedStatement: "lokadb_s", DestinationPortal: "lokadb_lsv"},
		&pgproto3.Execute{Portal: "lokadb_lsv"}, &pgproto3.Close{ObjectType: 'S', Name: "lokadb_s"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	f.mu.Lock()
	parsed := append([]string(nil), f.parsed...)
	f.mu.Unlock()
	if !slices.Contains(parsed, "lokadb_u_s") { // the proxy's own lokadb_s (its set_config) is there too
		t.Errorf("the client's Parse of lokadb_s went upstream as one of %q, want lokadb_u_s (upstream ran %q)", parsed, f.log()[n:])
	}
	if countContaining(f.log()[n:], "SELECT 1") == 0 {
		t.Errorf("the client's statement did not run: %q", f.log()[n:])
	}
}

// P3, a lookup after a pipeline cut by Flush: the simple query's organization is found in the
// refs table (a child row by its document) through captureExt with Flush, not with a Sync of
// the proxy's inside the client's implicit transaction.
func TestFlushThenQueryLooksUpInside(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "INSERT INTO notes VALUES (1)"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for n := 0; n < 3; n++ { // ParseComplete, BindComplete, CommandComplete
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	syncsBefore := f.syncs()
	sendAll(t, fe, &pgproto3.Query{String: "SELECT content FROM document_versions WHERE document_id = 5"})
	expectMsgs(t, fe, c, 1)
	if n := f.syncs() - syncsBefore; n != 0 {
		t.Fatalf("the client sent no Sync after its INSERT, the proxy sent %d with its lookup before the client's query; upstream ran %q", n, f.log())
	}
	if countContaining(f.log(), "documents__refs") == 0 {
		t.Fatalf("no lookup ran: %q", f.log())
	}
}
