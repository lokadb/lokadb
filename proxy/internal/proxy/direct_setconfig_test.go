package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 9 (from block 10's queue): SELECT set_config('lokadb.tenant', …, true) starts a direct
// cooperative transaction as SET LOCAL does: held with the BEGIN, sent to the cloud as SET LOCAL
// when the transaction opens, its row answered by the proxy (simple query and pgx's extended
// form with a parameter); the form after it learns and then goes direct. Before, set_config sent
// the transaction the cloud's way, never learnt (DP21, UT10 of block 18).
func TestDirectTxSetConfig(t *testing.T) {
	d, l := txSession(t)
	d.s.srv.cfg.Tenant = &TenantConfig{} // local_only, as behind a pooler
	// simple queries: the row of set_config, then two transactions learnt, the third direct
	run := func() {
		t.Helper()
		if _, code, _ := d.exchange(t, "BEGIN"); code != "" {
			t.Fatalf("BEGIN: %q", code)
		}
		types, code, st := d.exchange(t, "SELECT set_config('lokadb.tenant', '1', true)")
		if code != "" || st != 'T' || strings.Join(types, ",") != "RowDescription,DataRow,CommandComplete,ReadyForQuery" {
			t.Fatalf("set_config: %v %q %q", types, code, st)
		}
		for _, q := range []string{"SELECT slug FROM documents", "UPDATE documents SET title = 'y'", "COMMIT"} {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	run()
	run()
	if ranAny(l.get(), "org1:") {
		t.Fatalf("went direct before it was learnt: %q", l.get())
	}
	if all := l.get(); !inOrder(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = '1'", "cloud: SELECT slug FROM") || ranAny(all, "set_config('lokadb.tenant'") {
		t.Fatalf("the held set_config did not reach the cloud as SET LOCAL after its BEGIN: %q", all)
	}
	n := len(l.get())
	run()
	if all := l.get()[n:]; !commitsAfter(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = '1'", "org1: BEGIN", "org1: SELECT slug FROM documents",
		"org1: UPDATE documents") {
		t.Fatalf("the learnt form did not go direct: %q", all)
	}

	// pgx: set_config($1) prepared, then bound in the deferred transaction
	sendAll(t, d.fe, &pgproto3.Parse{Name: "t", Query: "SELECT set_config('lokadb.tenant', $1, true)"}, &pgproto3.Describe{ObjectType: 'S', Name: "t"}, &pgproto3.Sync{})
	if _, code, _ := d.answerAll(t); code != "" {
		t.Fatalf("prepare: %q", code)
	}
	n = len(l.get())
	d.exchange(t, "BEGIN")
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "t", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
	types, code, st := d.answerAll(t)
	if code != "" || st != 'T' || strings.Join(types, ",") != "BindComplete,RowDescription,DataRow,CommandComplete,ReadyForQuery" {
		t.Fatalf("pgx set_config: %v %q %q", types, code, st)
	}
	for _, q := range []string{"SELECT slug FROM documents", "UPDATE documents SET title = 'z'", "COMMIT"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	if all := l.get()[n:]; !commitsAfter(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = '1'", "org1: BEGIN", "org1: SELECT slug FROM documents") {
		t.Fatalf("pgx's set_config did not start a direct transaction: %q", all)
	}
	// a value the proxy refuses goes the cloud's way, which refuses it
	d.exchange(t, "BEGIN")
	if _, code, _ := d.exchange(t, "SELECT set_config('lokadb.tenant', 'acme', true)"); code != "22023" {
		t.Fatalf("set_config to 'acme': %q, want 22023", code)
	}
	d.exchange(t, "ROLLBACK")
}

// commitsAfter: log has subs in order, then both COMMITs (the organization's and the cloud's), in
// either order: a direct transaction with no work of its own in the cloud (reads and UPDATEs at
// the organization) commits both at once (block 10, item 9: commitBoth), so the stubs' log does
// not fix which comes first. With work in the cloud (refs rows) the order is the
// organization's first: those tests keep inOrder.
func commitsAfter(log []string, subs ...string) bool {
	return inOrder(log, append(append([]string{}, subs...), "org1: COMMIT")...) &&
		inOrder(log, append(append([]string{}, subs...), "cloud: COMMIT")...)
}
