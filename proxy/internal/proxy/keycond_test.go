package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// partitioned: documents and document_versions with partitions of organizations of their own
// (move-org's first step), as F7's thousands of organizations have them.
func partitioned(m *Meta) {
	m.Local = map[string]map[string]string{
		"documents":         {"1": "documents__o1", "2": "documents__o2"},
		"document_versions": {"1": "document_versions__o1", "2": "document_versions__o2"},
	}
}

// F7, C1: the partition key's condition on every LokaDB table of every level, not on the side
// of an outer join that may be null, not on a CTE's name, not in WHERE CURRENT OF.
func TestKeyCond(t *testing.T) {
	m := testMeta()
	partitioned(m)
	const d, v = "COALESCE(documents.organization_id, -2) = 1", "COALESCE(document_versions.organization_id, -2) = 1"
	for _, c := range []struct {
		name, sql string
		want      []string // in the result
		not       []string // not in it
	}{
		{"point read", "SELECT title FROM documents WHERE organization_id = 1 AND id = 5", []string{"WHERE organization_id = 1 AND id = 5 AND " + d}, nil},
		{"no WHERE", "SELECT count(*) FROM document_versions", []string{"WHERE " + v}, nil},
		{"alias", "SELECT d.title FROM documents d WHERE d.id = $1", []string{"COALESCE(d.organization_id, -2) = 1"}, nil},
		{"inner join", "SELECT * FROM documents d JOIN document_versions x ON x.document_id = d.id WHERE d.id = 5",
			[]string{"COALESCE(d.organization_id, -2) = 1", "COALESCE(x.organization_id, -2) = 1"}, nil},
		{"left join: the side that may be null left as it is", "SELECT * FROM documents d LEFT JOIN document_versions x ON x.document_id = d.id WHERE d.id = 5",
			[]string{"COALESCE(d.organization_id, -2) = 1"}, []string{"x.organization_id"}},
		{"right join", "SELECT * FROM documents d RIGHT JOIN document_versions x ON x.document_id = d.id WHERE x.id = 5",
			[]string{"COALESCE(x.organization_id, -2) = 1"}, []string{"COALESCE(d.organization_id"}},
		{"a subquery: a level of its own", "SELECT title FROM documents WHERE id IN (SELECT document_id FROM document_versions WHERE content = 'a')",
			[]string{"content = 'a' AND " + v, d}, nil},
		{"a CTE and the table", "WITH x AS (SELECT * FROM document_versions WHERE id = 5) SELECT * FROM documents, x WHERE documents.id = x.document_id",
			[]string{v, d}, nil},
		{"union", "SELECT id FROM documents WHERE id = 5 UNION SELECT id FROM document_versions WHERE id = 6", []string{d, v}, nil},
		{"update", "UPDATE documents SET title = 'x' WHERE id = 5", []string{"WHERE id = 5 AND " + d}, nil},
		{"delete using", "DELETE FROM documents USING document_versions x WHERE x.document_id = documents.id AND x.id = 5",
			[]string{d, "COALESCE(x.organization_id, -2) = 1"}, nil},
		{"insert select", "INSERT INTO document_versions (document_id, content) SELECT id, 'c' FROM documents WHERE id = 5", []string{d}, nil},
		{"explain", "EXPLAIN SELECT * FROM documents WHERE id = 5", []string{d}, nil},
		{"a join's alias hides its tables; the one outside it is seen", "SELECT * FROM (documents d JOIN document_versions x ON x.document_id = d.id) j JOIN documents e ON e.id = j.document_id WHERE e.id = 5",
			[]string{"COALESCE(e.organization_id, -2) = 1"}, []string{"d.organization_id", "x.organization_id"}},
		{"a subquery in FROM with an alias: its own level sees its table", "SELECT * FROM (SELECT d.id FROM documents d WHERE d.id = 5) s",
			[]string{"COALESCE(d.organization_id, -2) = 1"}, nil},
		{"lateral", "SELECT * FROM documents d, LATERAL (SELECT x.id FROM document_versions x WHERE x.document_id = d.id) l WHERE d.id = 5",
			[]string{"COALESCE(d.organization_id, -2) = 1", "COALESCE(x.organization_id, -2) = 1"}, nil},
		{"using", "SELECT * FROM documents d JOIN document_versions x USING (organization_id) WHERE d.id = 5",
			[]string{"COALESCE(d.organization_id, -2) = 1", "COALESCE(x.organization_id, -2) = 1"}, nil},
		{"using with the join's own alias of its columns: the tables are seen", "SELECT * FROM documents d JOIN document_versions x USING (organization_id) AS k WHERE d.id = 5",
			[]string{"COALESCE(d.organization_id, -2) = 1", "COALESCE(x.organization_id, -2) = 1"}, nil},
	} {
		got, ok := keyCond(c.sql, m, "1", defaultPath)
		if !ok {
			t.Errorf("%s: nothing added", c.name)
			continue
		}
		for _, w := range c.want {
			if !strings.Contains(got, w) {
				t.Errorf("%s: %q lacks %q", c.name, got, w)
			}
		}
		for _, w := range c.not {
			if strings.Contains(got, w) {
				t.Errorf("%s: %q has %q", c.name, got, w)
			}
		}
	}
	for _, c := range []struct{ name, sql, org string }{
		{"full join: both sides may be null", "SELECT * FROM documents d FULL JOIN document_versions x ON x.document_id = d.id", "1"},
		{"a CTE named as the table", "WITH documents AS (SELECT 1 AS id) SELECT * FROM documents", "1"},
		{"WHERE CURRENT OF", "DELETE FROM documents WHERE CURRENT OF c", "1"},
		{"a join with an alias of its own", "SELECT j.title FROM (documents d JOIN document_versions x ON x.document_id = d.id) j WHERE j.id = 5", "1"},
		{"a join with an alias, using", "SELECT * FROM (documents d JOIN document_versions x USING (organization_id)) j", "1"},
		{"column aliases rename the organization column", "SELECT * FROM documents d(i, o) WHERE i = 5", "1"},
		{"insert values", "INSERT INTO documents (organization_id, title) VALUES (1, 'x')", "1"},
		{"a cloud table", "SELECT * FROM projects WHERE id = 1", "1"},
		{"two statements", "SELECT 1 FROM documents; SELECT 2 FROM documents", "1"},
		{"no organization", "SELECT * FROM documents", ""},
		{"none found", "SELECT * FROM documents", "-1"},
		{"rows of none", "SELECT * FROM documents", "-2"},
		{"not a number", "SELECT * FROM documents", "1; DROP TABLE x"},
	} {
		if got, ok := keyCond(c.sql, m, c.org, defaultPath); ok {
			t.Errorf("%s: %q", c.name, got)
		}
	}
	// no partition of an organization's own: as it was
	if got, ok := keyCond("SELECT * FROM documents WHERE id = 5", testMeta(), "1", defaultPath); ok {
		t.Errorf("no partitions: %q", got)
	}
	// organizations numbered by uuid: the nil uuid of rows of none, the value cast
	mu := testMeta()
	partitioned(mu)
	mu.TenantKind = kindUUID
	const u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	if got, ok := keyCond("SELECT * FROM documents WHERE id = 5", mu, u, defaultPath); !ok ||
		!strings.Contains(got, "COALESCE(documents.organization_id, '00000000-0000-0000-0000-000000000000'::uuid) = '"+u+"'::uuid") {
		t.Errorf("uuid: %q %v", got, ok)
	}
}

// F7, C1 in a session: the simple protocol sends the statement with the condition; the
// extended one Binds the client's portal to the statement Parsed with it under a name of the
// proxy's own, once per organization on the connection, again after the client's DISCARD ALL.
func TestKeyCondSession(t *testing.T) {
	m := testMeta()
	partitioned(m)
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, m)
	sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1"})
	expectMsgs(t, fe, c, 1)
	if !ranAny(f.log(), "WHERE organization_id = 1 AND COALESCE(documents.organization_id, -2) = 1") {
		t.Fatalf("simple: upstream ran %q", f.log())
	}
	sendAll(t, fe, &pgproto3.Parse{Name: "s", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	bind := func(org string) {
		sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte(org)}}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if got := expectMsgs(t, fe, c, 1); len(got) == 0 || got[len(got)-1] != "ReadyForQuery" {
			t.Fatalf("client got %v", got)
		}
	}
	bind("1")
	if f.stmts["lokadb_k1"] == "" || !strings.Contains(f.stmts["lokadb_k1"], "COALESCE(documents.organization_id, -2) = 1") {
		t.Fatalf("extended: no statement with the condition: %q", f.stmts)
	}
	n := parsedN(f, "lokadb_k1")
	bind("1")
	if parsedN(f, "lokadb_k1") != n {
		t.Errorf("the second execution of organization 1 Parsed its statement again")
	}
	if last := f.log()[len(f.log())-1]; !strings.Contains(last, "COALESCE(documents.organization_id, -2) = 1") {
		t.Errorf("ran %q", last)
	}
	bind("2")
	if !strings.Contains(f.stmts["lokadb_k2"], "COALESCE(documents.organization_id, -2) = 2") {
		t.Errorf("organization 2: %q", f.stmts)
	}
	sendAll(t, fe, &pgproto3.Query{String: "DISCARD ALL"})
	expectMsgs(t, fe, c, 1)
	sendAll(t, fe, &pgproto3.Parse{Name: "s", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1) // DISCARD ALL dropped the client's statement too
	n = parsedN(f, "lokadb_k1")
	bind("1")
	if parsedN(f, "lokadb_k1") == n {
		t.Errorf("after DISCARD ALL the statement of organization 1 was not Parsed again")
	}
}

// Past keyed_statements a connection's least recently used keyed statement is closed and its
// name takes the new one (block 1's analysis of block 14's orgs: with a bound of 256 and no
// eviction every pair past it was Parsed and planned at each execution). One bound to a portal
// that may still run, or by the batch being sent, stays.
func TestKeyedEviction(t *testing.T) {
	m := testMeta()
	m.Local = map[string]map[string]string{"documents": {"1": "documents__o1", "2": "documents__o2", "3": "documents__o3"}}
	f := &fakeUpstream{}
	fe, c, _ := protoSessionCfg(t, f, m, Config{KeyedStatements: 2})
	sendAll(t, fe, &pgproto3.Parse{Name: "s", Query: "SELECT title FROM documents WHERE organization_id = $1"}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	bind := func(org string) {
		t.Helper()
		sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte(org)}}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if got := expectMsgs(t, fe, c, 1); len(got) == 0 || got[len(got)-1] != "ReadyForQuery" {
			t.Fatalf("client got %v", got)
		}
	}
	has := func(name, org string) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return strings.Contains(f.stmts[name], "COALESCE(documents.organization_id, -2) = "+org)
	}
	bind("1")
	bind("2")
	bind("1") // organization 2's statement is now the least recently used
	k1 := parsedN(f, "lokadb_k1")
	bind("3")
	if !has("lokadb_k2", "3") || parsedN(f, "lokadb_alt") != 0 {
		t.Fatalf("organization 3 did not take the least recently used statement's name: %q, lokadb_alt Parsed %d times", f.stmts, parsedN(f, "lokadb_alt"))
	}
	bind("1")
	if parsedN(f, "lokadb_k1") != k1 {
		t.Errorf("organization 1's statement, used recently, was evicted")
	}
	// a named portal of organization 1 that may still run: its statement stays
	sendAll(t, fe, &pgproto3.Bind{DestinationPortal: "p", PreparedStatement: "s", Parameters: [][]byte{[]byte("1")}}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	bind("3")
	k1 = parsedN(f, "lokadb_k1")
	bind("2") // organization 1's is the least recently used, but portal p is bound to it
	if parsedN(f, "lokadb_k1") != k1 || !has("lokadb_k1", "1") || !has("lokadb_k2", "2") {
		t.Errorf("the statement of an open portal was evicted: %q", f.stmts)
	}
	// both bound (portal p, and organization 2's by this batch): Parsed for the one execution
	alt := parsedN(f, "lokadb_alt")
	sendAll(t, fe, &pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("2")}}, &pgproto3.Execute{},
		&pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("3")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if parsedN(f, "lokadb_alt") != alt+1 || !has("lokadb_k2", "2") {
		t.Errorf("a statement bound by the batch being sent was evicted: %q, lokadb_alt Parsed %d times", f.stmts, parsedN(f, "lokadb_alt")-alt)
	}
}

// parsedN: the Parses of statement name the fake has had.
func parsedN(f *fakeUpstream, name string) int {
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
