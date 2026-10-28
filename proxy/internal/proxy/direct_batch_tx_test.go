package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 2 of 8b.2, step 4: the batch forms inside a direct transaction. The cloud's part runs
// in the transaction's open one, the organization's in its; the commits are the transaction's.

// An upsert: the keys the cloud gave rows that were updated leave __refs in the cloud's open
// transaction, before COMMIT; COMMIT is the organization's first, then the cloud's.
func TestDirectTxUpsert(t *testing.T) {
	d, l := txSession(t, keyed, slugUnique)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT nextval('documents__id_seq'::regclass) AS id FROM generate_series(1, 2))") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"41", "t"}, {"17", "f"}}, "", true
		}
		return nil, "", false
	}
	for _, q := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1",
		"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x'), (1, 'b', 'y') ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title"} {
		_, code, st := d.exchange(t, q)
		expectTx(t, q, code, st, "", 'T')
	}
	_, code, st := d.exchange(t, "COMMIT")
	expectTx(t, "COMMIT", code, st, "", 'I')
	if all := l.get(); !inOrder(all, "cloud: BEGIN", "org1: BEGIN", "org1: SELECT title", "cloud: WITH lokadb_k AS",
		"org1: INSERT INTO documents (organization_id, slug, title, id) VALUES (1, 'a', 'x', '41'), (1, 'b', 'y', '42') ON CONFLICT (slug) DO UPDATE SET title = excluded.title RETURNING id, xmax::text = '0'",
		"cloud: DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (42)", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("the two sides ran %q", l.get())
	}
}

// An INSERT … SELECT naming its keys: the organization's statement in its open transaction
// RETURNING them, their refs rows after it in the cloud's; a key of another organization ends
// the transaction (23505), the client's ROLLBACK undoes both sides.
func TestDirectTxInsertSelect(t *testing.T) {
	for _, other := range []string{"0", "1"} {
		d, l := txSession(t, keyed)
		d.cloud.answer = func(q string) ([][]string, string, bool) {
			if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (1001))") {
				return [][]string{{other}}, "", true
			}
			return nil, "", false
		}
		d.org1.answerSimple = true
		d.org1.answer = func(q string) ([][]string, string, bool) {
			if strings.HasPrefix(q, "INSERT") {
				return [][]string{{"1001"}}, "", true
			}
			return nil, "", false
		}
		for _, q := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1"} {
			_, code, st := d.exchange(t, q)
			expectTx(t, q, code, st, "", 'T')
		}
		_, code, st := d.exchange(t, "INSERT INTO document_versions (id, document_id, content) SELECT id + 1000, document_id, content FROM document_versions WHERE organization_id = 1 AND id = 1")
		if other == "0" {
			expectTx(t, "INSERT … SELECT", code, st, "", 'T')
			_, code, st = d.exchange(t, "COMMIT")
			expectTx(t, "COMMIT", code, st, "", 'I')
			if all := l.get(); !inOrder(all, "org1: INSERT INTO document_versions", "cloud: WITH lokadb_v(id) AS (VALUES (1001))", "org1: COMMIT", "cloud: COMMIT") {
				t.Fatalf("the two sides ran %q", all)
			}
			continue
		}
		expectTx(t, "INSERT … SELECT of another organization's key", code, st, "23505", 'E')
		_, code, st = d.exchange(t, "ROLLBACK")
		expectTx(t, "ROLLBACK", code, st, "", 'I')
		if all := l.get(); !inOrder(all, "cloud: WITH lokadb_v(id)", "org1: ROLLBACK", "cloud: ROLLBACK") || ranAny(all, "org1: COMMIT") {
			t.Fatalf("another organization's key: %q", all)
		}
	}
}

// learntNamed: the form starting with sql is learnt for the direct path under a named
// organization (learnKey).
func learntNamed(d *directTest, sql string) {
	for i := 0; i < learnRuns; i++ {
		d.s.srv.directOK.result("db/"+fingerprint(sql)+"#named", true)
	}
}

// COPY in the cooperative mode as the default configuration has it: BEGIN; SET LOCAL
// lokadb.tenant; COPY … FROM STDIN; COMMIT. Learnt first (two such transactions through the
// cloud: learnStmt takes a COPY of the organization named), then direct: the stage in the
// organization's open transaction (no BEGIN of its own), the keys from the cloud's, the commits
// at COMMIT.
func TestDirectTxCopy(t *testing.T) {
	d, l := txSession(t, keyed)
	d.s.srv.cfg.Tenant = &TenantConfig{} // local_only, the default
	cp := "COPY documents (organization_id, title) FROM STDIN"
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "generate_series(1, 2)") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return nil, "", true
		}
		return nil, "", false
	}
	run := func() {
		for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1"} {
			_, code, st := d.exchange(t, q)
			expectTx(t, q, code, st, "", 'T')
		}
		_, code, tag := d.copyClient(t, cp, []string{"1\tx\n", "1\ty\n"}, &pgproto3.CopyDone{})
		if code != "" || tag != "COPY 2" {
			t.Fatalf("COPY: %q %q; %q", code, tag, l.get())
		}
		_, code, st := d.exchange(t, "COMMIT")
		expectTx(t, "COMMIT", code, st, "", 'I')
	}
	run()
	run()
	if ranAny(l.get(), "org1:") {
		t.Fatalf("went direct before it was learnt: %q", l.get())
	}
	n := len(l.get())
	run()
	all := l.get()[n:]
	if !inOrder(all, "cloud: BEGIN", "cloud: SET LOCAL lokadb.tenant = 1", "cloud: SELECT FROM pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "org1: BEGIN",
		"org1: DROP TABLE IF EXISTS pg_temp.__lokadb_dcopy", "org1: COPY __lokadb_dcopy(organization_id, title) FROM STDIN", "cloud: WITH lokadb_k AS",
		"org1: INSERT INTO \"documents\"", "org1: COMMIT", "cloud: COMMIT") {
		t.Fatalf("the two sides ran %q", all)
	}
	if ranAny(all, "org1: BEGIN; DROP") {
		t.Fatalf("the stage opened a transaction of its own: %q", all)
	}
}

// A COPY that fails in a direct transaction (a key of another organization) aborts it: no
// rollback of the proxy's own, the client's ROLLBACK undoes both sides.
func TestDirectTxCopyFails(t *testing.T) {
	d, l := txSession(t, keyed)
	d.s.srv.cfg.Tenant = &TenantConfig{}
	cp := "COPY documents (id, organization_id, title) FROM STDIN"
	learntNamed(d, cp)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (7))") {
			return [][]string{{"1"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"7"}}, "", true
		}
		return nil, "", false
	}
	for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1"} {
		_, code, st := d.exchange(t, q)
		expectTx(t, q, code, st, "", 'T')
	}
	types, code, _ := d.copyClient(t, cp, []string{"7\t1\tx\n"}, &pgproto3.CopyDone{})
	if code != "23505" || types[len(types)-1] != "ReadyForQuery" {
		t.Fatalf("COPY: %v %q", types, code)
	}
	if ranAny(l.get(), "ROLLBACK") {
		t.Fatalf("the proxy rolled back on its own: %q", l.get())
	}
	_, code, st := d.exchange(t, "ROLLBACK")
	expectTx(t, "ROLLBACK", code, st, "", 'I')
	if all := l.get(); !inOrder(all, "org1: ROLLBACK", "cloud: ROLLBACK") || ranAny(all, "COMMIT\"") {
		t.Fatalf("the two sides ran %q", all)
	}
}
