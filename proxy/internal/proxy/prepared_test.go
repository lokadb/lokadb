package proxy

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// 1.1: a pool's statement cache outlives enable. A statement Parsed while the session had no
// metadata is analysed when it is bound afterwards: lokadb.org and the pause lock go before
// it, as for a statement prepared after enable.
func TestPreparedBeforeEnable(t *testing.T) {
	f := &fakeUpstream{}
	fe, c, s := protoSessionS(t, f, nil)
	ins := "INSERT INTO documents (organization_id, title) VALUES ($1, $2)"
	sendAll(t, fe, &pgproto3.Parse{Name: "s1", Query: ins}, &pgproto3.Describe{ObjectType: 'S', Name: "s1"}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ParseComplete", "NoData", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("prepare: %v, want %v", got, want)
	}
	// enable ran; the session reloaded its metadata (as refresh would)
	m := testMeta()
	m.OrgLock = true
	s.meta = m
	s.anCache.reset() // as refresh does
	sendAll(t, fe, &pgproto3.Parse{Query: "SELECT title FROM documents WHERE organization_id = $1"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1"), []byte("t")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("bind after enable: %v, want %v", got, want)
	}
	got := f.log()[n:]
	want := []string{
		"SELECT set_config('lokadb.org', 1, false)",
		"SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(1::bigint))",
		"INSERT INTO documents (organization_id, title) VALUES (1, t)",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("upstream ran %q\nwant %q (the INSERT ran under organization 2 before)", got, want)
	}
}

// A Bind of a statement the proxy never saw Parsed (SQL PREPARE) would run unanalysed: refused
// with the code Postgres gives an unknown statement.
func TestBindUnknownStatement(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "x", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	for _, q := range f.log() {
		if strings.Contains(q, "set_config") {
			t.Fatalf("ran %q for a statement it does not know", q)
		}
	}
}

// P8: an upsert rewritten with the column types of its time is rewritten again when the
// types change; the new text goes under a statement name of the proxy's own.
func TestPreparedAfterSchemaChange(t *testing.T) {
	f := &fakeUpstream{}
	m1 := testMeta()
	fe, c, s := protoSessionS(t, f, m1)
	ups := "INSERT INTO documents (id, organization_id, title) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET title = excluded.title"
	sendAll(t, fe, &pgproto3.Parse{Name: "u", Query: ups}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if sent := f.stmts["u"]; !strings.Contains(sent, "AS character varying)") {
		t.Fatalf("the first rewrite has no cast of its time: %q", sent)
	}
	m2 := testMeta()
	m2.Tables["documents"].ColType["title"] = "text"
	s.meta = m2
	s.anCache.reset() // as refresh does
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "u", Parameters: [][]byte{[]byte("1"), []byte("1"), []byte("t")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
	alias := f.stmts["lokadb_v1"]
	if alias == "" || strings.Contains(alias, "character varying") || !strings.Contains(alias, "AS text)") {
		t.Fatalf("the statement run is not rewritten for the new type: %q", alias)
	}
	if log := f.log(); !strings.Contains(log[len(log)-1], "AS text") {
		t.Fatalf("executed %q", log[len(log)-1])
	}
	// the alias keeps the parameter types the server gave the client's statement: the client
	// encodes its parameters for them
	if got := f.parseOIDs["lokadb_v1"]; !reflect.DeepEqual(got, []uint32{20, 20, 25}) {
		t.Fatalf("the alias was parsed with parameter types %v, want the client's statement's [20 20 25]", got)
	}
}

// DEALLOCATE drops the statement: a later Bind of the name is refused as Postgres would,
// not run through the proxy's alias.
func TestDeallocateForgets(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Name: "d", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Query{String: "DEALLOCATE d"})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "d", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"ErrorResponse", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v", got, want)
	}
}

// Parse + Describe + Flush leaves no transaction of the client's open: the next Sync batch
// may look its organization up (the refs lookup runs).
func TestLookupAfterDescribeFlush(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO document_versions (document_id, content) VALUES ($1, $2)"
	sendAll(t, fe, &pgproto3.Parse{Name: "q", Query: ins}, &pgproto3.Describe{ObjectType: 'S', Name: "q"}, &pgproto3.Flush{})
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for i := 0; i < 2; i++ { // ParseComplete, NoData
		if _, err := fe.Receive(); err != nil {
			t.Fatal(err)
		}
	}
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "q", Parameters: [][]byte{[]byte("42"), []byte("x")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	looked := false
	for _, q := range f.log() {
		looked = looked || strings.Contains(q, "documents__refs")
	}
	if !looked {
		t.Fatalf("no refs lookup for document 42: %q", f.log())
	}
}

// DEALLOCATE ALL drops only what was prepared before it: a statement Parsed after it in the
// same batch stays bindable.
func TestDeallocateAllThenParse(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Parse{Query: "DEALLOCATE ALL"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Name: "s1", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Bind s1 after DEALLOCATE ALL and Parse s1: %v, want %v", got, want)
	}
}

// After an error the server skips only up to the client's Sync: a batch sent right after
// that Sync is analysed (lokadb.org set) and its statement recorded.
func TestBatchRightAfterErrorSync(t *testing.T) {
	f := &fakeUpstream{failExec: map[int]bool{2: true}, delaySync: map[int]time.Duration{1: 300 * time.Millisecond}} // 1 is the proxy's set_config
	fe, c := protoSession(t, f, testMeta())
	ins := "INSERT INTO documents (organization_id, title) VALUES ($1, $2)"
	sendAll(t, fe, &pgproto3.Parse{Name: "a", Query: ins}, &pgproto3.Bind{PreparedStatement: "a", Parameters: [][]byte{[]byte("1"), []byte("x")}}, &pgproto3.Execute{}, &pgproto3.Flush{},
		&pgproto3.Sync{},
		&pgproto3.Parse{Name: "b", Query: ins}, &pgproto3.Bind{PreparedStatement: "b", Parameters: [][]byte{[]byte("2"), []byte("y")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 2)
	n := len(f.log())
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "b", Parameters: [][]byte{[]byte("2"), []byte("z")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Bind b later: %v, want %v (b was not recorded)", got, want)
	}
	if log := f.log()[n:]; len(log) < 2 || !strings.Contains(log[len(log)-2], "set_config") && !strings.Contains(log[len(log)-2], "advisory") {
		t.Logf("upstream: %q", log)
	}
}

// Block 9, item 4: the unnamed statement Parsed before a schema change and bound after it
// (pgx: Parse, Describe, Sync, then Bind, Execute, Sync) ran in its old form: its parameter
// types are not in pg_prepared_statements, and the proxy's own simple queries in between (the
// metadata read) destroyed it upstream, so the client's Bind failed. The proxy's queries go
// through the extended protocol now, and the unnamed statement's types come from Describe.
func TestUnnamedAfterSchemaChange(t *testing.T) {
	f := &fakeUpstream{describeTypes: []uint32{20, 20, 25}, answer: metaKept}
	fe, c, s := protoSessionS(t, f, testMeta())
	ups := "INSERT INTO documents (id, organization_id, title) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET title = excluded.title"
	sendAll(t, fe, &pgproto3.Parse{Query: ups}, &pgproto3.Describe{ObjectType: 'S'}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	m2 := testMeta()
	m2.Tables["documents"].ColType["title"] = "text"
	s.meta = m2
	s.anCache.reset()                // as refresh does
	s.srv.schemaVersion("db").Add(1) // and the metadata is read again before the next batch
	sendAll(t, fe, &pgproto3.Bind{Parameters: [][]byte{[]byte("1"), []byte("1"), []byte("t")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if got, want := expectMsgs(t, fe, c, 1), []string{"BindComplete", "CommandComplete", "ReadyForQuery"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("client got %v, want %v; upstream ran %q", got, want, f.log())
	}
	if alias := f.stmts["lokadb_v1"]; !strings.Contains(alias, "AS text)") {
		t.Fatalf("the statement run is not rewritten for the new type: %q", alias)
	}
	if got := f.parseOIDs["lokadb_v1"]; !reflect.DeepEqual(got, []uint32{20, 20, 25}) {
		t.Fatalf("the alias was parsed with parameter types %v, want the unnamed statement's [20 20 25]", got)
	}
	if log := f.log(); !strings.Contains(log[len(log)-1], "AS text") {
		t.Fatalf("executed %q", log[len(log)-1])
	}
}
