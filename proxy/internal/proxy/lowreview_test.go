package proxy

import (
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// Second review of the protocol, P8: lokadb.org upstream is unknown at the start of a session
// (ALTER ROLE … SET lokadb.org, a pooler) and after a statement that calls what may set it out
// of the proxy's sight (a function of the application's): the next statement of the same
// organization sets it again; after a plain function of the server's it is not.
func TestOrgSetAfterAFunction(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sel := "SELECT title FROM documents WHERE organization_id = 1"
	withFunc := "SELECT title, app_func() FROM documents WHERE organization_id = 1"
	withLower := "SELECT lower(title) FROM documents WHERE organization_id = 1"
	for _, q := range []string{sel, withFunc, withFunc, withLower, withLower} {
		sendAll(t, fe, &pgproto3.Query{String: q})
		expectMsgs(t, fe, c, 1)
	}
	// set before the first; not before the first with app_func (the same organization), but
	// after it; before the first with lower(), and not again
	if n := countContaining(f.log(), "set_config('lokadb.org', '1'"); n != 3 {
		t.Errorf("organization 1's statements around an application's function: lokadb.org = 1 set %d times, want 3 (upstream ran %q)", n, f.log())
	}
	if !callsOwn(mustParse(t, "SELECT myschema.f()")) || !callsOwn(mustParse(t, "CALL p()")) || callsOwn(mustParse(t, "SELECT count(*) FROM t")) {
		t.Error("callsOwn: a function of a schema, CALL: yes; count(): no")
	}
}

func mustParse(t *testing.T, sql string) *pg.Node {
	t.Helper()
	tree, err := pg.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	return tree.Stmts[0].Stmt
}

// P9: a cancel on its way keeps the session's connections out of the pools; once they are
// back, a cancel goes nowhere.
func TestCancelKeepsTheConnection(t *testing.T) {
	s := &session{}
	if !s.cancelBegin() {
		t.Fatal("a live session's cancel refused")
	}
	if s.releasing() {
		t.Error("a cancel on its way: the connection went back to the pool")
	}
	s.cancelEnd()
	if !s.releasing() {
		t.Error("no cancel on its way: the connection kept out of the pool")
	}
	if s.cancelBegin() {
		t.Error("the connections back in the pools: a cancel still goes")
	}
}

// P11: the role named by a startup parameter in any case.
func TestNamesRole(t *testing.T) {
	for _, c := range []struct {
		params map[string]string
		want   bool
	}{
		{map[string]string{"user": "app"}, false},
		{map[string]string{"user": "app", "role": "x"}, true},
		{map[string]string{"user": "app", "ROLE": "x"}, true},
		{map[string]string{"user": "app", "Options": "-c a"}, true},
		{map[string]string{"user": "app", "options": ""}, false},
	} {
		if got := namesRole(c.params); got != c.want {
			t.Errorf("%v: %v, want %v", c.params, got, c.want)
		}
	}
}
