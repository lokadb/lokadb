package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 9 (from block 10's queue): the client's unnamed statement Parsed in the batch (JDBC
// before prepareThreshold, pgx's exec mode) goes the direct path: a read with the client's Parse
// and Bind as it sent them, a write with the text the proxy rewrites (its ParseComplete owed),
// a pipeline with units of both kinds; the cloud describes each text once and never runs it.
// Before, any batch with a Parse went the cloud's way. (TestUnnamedStatementUsedAgain: a Bind "" with no
// Parse after such a batch.)
func TestDirectUnnamed(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.describeTypes = []uint32{20}
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	read := "SELECT title FROM documents WHERE organization_id = $1"
	unnamed := func(sql string, args ...string) []pgproto3.FrontendMessage {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		return []pgproto3.FrontendMessage{&pgproto3.Parse{Query: sql}, &pgproto3.Bind{Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}}
	}
	send := func(ms ...[]pgproto3.FrontendMessage) ([]string, string) {
		var all []pgproto3.FrontendMessage
		for _, m := range ms {
			all = append(all, m...)
		}
		sendAll(t, d.fe, append(all, &pgproto3.Sync{})...)
		return d.answerTypes(t)
	}
	cloudRan := func(n int, sub string) bool {
		for _, q := range d.cloud.log()[n:] {
			if strings.Contains(q, sub) && !strings.Contains(q, "lokadb.orgs") {
				return true
			}
		}
		return false
	}

	// a read, twice (the second from the session's description)
	for i := 0; i < 2; i++ {
		c0, o0 := len(d.cloud.log()), len(d.org1.log())
		types, code := send(unnamed(read, "1"))
		if code != "" || strings.Join(types, ",") != "ParseComplete,BindComplete,NoData,CommandComplete,ReadyForQuery" {
			t.Fatalf("read %d: client got %v (%q)", i, types, code)
		}
		if org := d.org1.log()[o0:]; len(org) != 1 || org[0] != "SELECT title FROM documents WHERE organization_id = 1" {
			t.Fatalf("read %d: organization 1 ran %q (want the client's statement)", i, org)
		}
		if cloudRan(c0, "SELECT title") {
			t.Fatalf("read %d: the cloud ran it: %q", i, d.cloud.log()[c0:])
		}
	}
	// the unnamed statement is not the cloud's, but the client's: a Bind of it without a Parse
	// of its own is Parsed there first and runs (block 9, review PR-14: it was refused, 26000)
	o0 := len(d.org1.log())
	sendAll(t, d.fe, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code := d.answerTypes(t); code != "" || len(d.org1.log()) != o0 {
		t.Fatalf("a Bind of the unnamed statement without its Parse: %q, organization 1 ran %q", code, d.org1.log()[o0:])
	}

	// a write: Parsed rewritten by the proxy, the client's ParseComplete first
	c0 := len(d.cloud.log())
	o0 = len(d.org1.log())
	types, code := send(unnamed("INSERT INTO documents (organization_id, title) VALUES ($1, $2)", "1", "x"))
	if code != "" || types[0] != "ParseComplete" || strings.Count(strings.Join(types, ","), "ParseComplete") != 1 || types[len(types)-1] != "ReadyForQuery" {
		t.Fatalf("write: client got %v (%q)", types, code)
	}
	if org := d.org1.log()[o0:]; len(org) != 1 || !strings.Contains(org[0], "(organization_id, title, id) VALUES (1, x, '41')") {
		t.Fatalf("write: organization 1 ran %q", org)
	}
	if !inOrder(d.cloud.log()[c0:], "INSERT INTO \"documents__refs\"", "COMMIT") {
		t.Fatalf("write: the cloud ran %q", d.cloud.log()[c0:])
	}

	// a pipeline: an unnamed unit, a Bind of it again, a named statement Parsed by the proxy
	// there, another unnamed unit; each ParseComplete of the client's in its place
	sendAll(t, d.fe, &pgproto3.Parse{Name: "n", Query: "SELECT slug FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	if code := errorCode(t, d.fe); code != "" {
		t.Fatalf("prepare: %s", code)
	}
	o0 = len(d.org1.log())
	again := []pgproto3.FrontendMessage{&pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}}
	named := []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: "n", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}}
	types, code = send(unnamed(read, "1"), again, named, unnamed("SELECT id FROM documents WHERE organization_id = $1", "1"))
	want := "ParseComplete,BindComplete,NoData,CommandComplete,BindComplete,CommandComplete,BindComplete,CommandComplete,ParseComplete,BindComplete,NoData,CommandComplete,ReadyForQuery"
	if code != "" || strings.Join(types, ",") != want {
		t.Fatalf("pipeline: client got %v (%q), want %s", types, code, want)
	}
	if org := d.org1.log()[o0:]; !inOrder(org, "SELECT title", "SELECT title", "SELECT slug", "SELECT id") || len(org) != 4 {
		t.Fatalf("pipeline: organization 1 ran %q", org)
	}
}

// JDBC with autocommit off before prepareThreshold: BEGIN by the extended protocol (deferred),
// then the transaction's statements by unnamed Parses: the first opens a direct transaction, the
// next (an UPDATE, described in the cloud's open transaction) runs in it too, COMMIT commits the
// organization, then the cloud.
func TestDirectTxUnnamed(t *testing.T) {
	d, l := txSession(t)
	d.cloud.describeTypes = []uint32{25, 20}
	ext := func(sql string, args ...string) {
		t.Helper()
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		sendAll(t, d.fe, &pgproto3.Parse{Query: sql}, &pgproto3.Bind{Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if types, code, st := d.answerAll(t); code != "" || st != 'T' || types[0] != "ParseComplete" {
			t.Fatalf("%s: %v %q %q", sql, types, code, st)
		}
	}
	n := len(l.get())
	sendAll(t, d.fe, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code, st := d.answerAll(t); code != "" || st != 'T' {
		t.Fatalf("BEGIN: %q %q", code, st)
	}
	ext("SELECT title FROM documents WHERE organization_id = $1", "1")
	ext("UPDATE documents SET title = $1 WHERE organization_id = $2", "u1", "1")
	if _, code, _ := d.exchange(t, "COMMIT"); code != "" {
		t.Fatalf("COMMIT: %q", code)
	}
	all := l.get()[n:]
	if !commitsAfter(all, "cloud: BEGIN", "org1: BEGIN", "org1: SELECT title", "org1: UPDATE documents SET title = u1") {
		t.Fatalf("the transaction: %q", all)
	}
	for _, q := range all {
		if strings.HasPrefix(q, "cloud: UPDATE") || strings.HasPrefix(q, "cloud: SELECT title") {
			t.Fatalf("the cloud ran the client's statement: %q in %q", q, all)
		}
	}
}

// Block 9, review PR-14: Parse "", Bind, Execute, Sync took the direct path (the cloud never had
// the statement); a Bind "" without a Parse after it is legal, and was answered 26000. The
// statement is Parsed in the cloud before that Bind now (the client's ParseComplete is not asked
// for twice); a Parse "" of its own, or a simple query, ends it as in Postgres.
func TestUnnamedStatementUsedAgain(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.describeTypes = []uint32{20}
	read := "SELECT title FROM documents WHERE organization_id = $1"
	sendAll(t, d.fe, &pgproto3.Parse{Query: read}, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if types, code := d.answerTypes(t); code != "" || types[0] != "ParseComplete" {
		t.Fatalf("the direct batch: %v %q", types, code)
	}
	if len(d.org1.log()) != 1 {
		t.Fatalf("the first batch did not go direct: org1 %q", d.org1.log())
	}
	// the statement again, no Parse
	sendAll(t, d.fe, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "BindComplete,CommandComplete,ReadyForQuery" {
		t.Fatalf("a Bind of the unnamed statement after a direct batch: %v %q (cloud %q)", types, code, d.cloud.log())
	}
	if !ranAny(d.cloud.log(), `FROM "documents__org1" documents WHERE organization_id = 1`) {
		t.Fatalf("the statement did not run in the cloud: %q", d.cloud.log())
	}
	// ... and once more: the cloud has it now, no second Parse of it
	n := d.cloud.parsedCount("")
	sendAll(t, d.fe, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code := d.answerTypes(t); code != "" {
		t.Fatalf("the Bind again: %q", code)
	}
	if got := d.cloud.parsedCount(""); got != n {
		t.Fatalf("a second Parse of the restored statement: %d Parses of the unnamed one before, %d after", n, got)
	}
	// a simple query destroys it: a Bind "" now is Postgres's 26000
	sendAll(t, d.fe, &pgproto3.Parse{Query: read}, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	d.answerTypes(t)
	d.query(t, "SELECT 1")
	sendAll(t, d.fe, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if _, code := d.answerTypes(t); code != "26000" {
		t.Fatalf("a Bind of the unnamed statement after a simple query: %q, want 26000", code)
	}
}
