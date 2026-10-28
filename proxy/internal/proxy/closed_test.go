package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Review of routing R1, coordinator's decision of 25.10: a statement the analyzer does not see
// touch LokaDB tables runs under lokadb.org = -1 (no LokaDB row): every organization (”) only
// where a fanout is allowed outright, the organization the application named where it named
// one. Before, ” always: SELECT reports.f() of a function the analyzer did not know read every
// organization's rows past the policy reject.
func TestClosedByDefault(t *testing.T) {
	const sel = "SELECT reports.mystery()" // a function the metadata does not know
	for _, c := range []struct {
		name  string
		setup func(*session)
		pre   []string
		want  string
	}{
		{"reject", nil, nil, "SELECT set_config('lokadb.org', '-1', false)"},
		{"allow", func(s *session) { s.fanout = &Fanout{Mode: FanoutAllow} }, nil, "SELECT set_config('lokadb.org', '', false)"},
		{"log", func(s *session) { s.fanout = &Fanout{Mode: FanoutLog} }, nil, "SELECT set_config('lokadb.org', '', false)"},
		{"reject-writes", func(s *session) { s.fanout = &Fanout{Mode: FanoutRejectWrites} }, nil, "SELECT set_config('lokadb.org', '-1', false)"},
		{"named", func(s *session) { s.srv.cfg.Tenant = sessionTenant() }, []string{"SET lokadb.tenant = 7"}, "SELECT set_config('lokadb.org', '7', false)"},
		{"'*' of a role of all_roles", func(s *session) {
			s.srv.cfg.Tenant = sessionTenant()
			s.srv.cfg.Tenant.AllRoles = []string{"report"}
			s.user = "report"
		}, []string{"SET lokadb.tenant = '*'"}, "SELECT set_config('lokadb.org', '', false)"},
		{"'*' of another role", func(s *session) {
			s.srv.cfg.Tenant = sessionTenant()
			s.srv.cfg.Tenant.AllRoles = []string{"report"}
			s.user = "app"
		}, []string{"SET lokadb.tenant = '*'"}, "SELECT set_config('lokadb.org', '-1', false)"},
	} {
		f := &fakeUpstream{}
		fe, _, s := protoSessionS(t, f, testMeta())
		if c.setup != nil {
			c.setup(s)
		}
		for _, q := range append(c.pre, "SELECT title FROM documents WHERE organization_id = 7", sel) {
			sendAll(t, fe, &pgproto3.Query{String: q})
			if code := errorCode(t, fe); code != "" {
				t.Fatalf("%s: %s: %s", c.name, q, code)
			}
		}
		if got, _ := orgsBefore(f.log(), sel); got != c.want {
			t.Errorf("%s: %s ran after %q, want %q; upstream ran %q", c.name, sel, got, c.want, f.log())
		}
	}
	// the extended protocol: the same, a statement of the cloud's tables after one of organization 3
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	var ms []pgproto3.FrontendMessage
	for _, q := range []string{"SELECT title FROM documents WHERE organization_id = 3", "SELECT count(*) FROM users"} {
		ms = append(ms, &pgproto3.Parse{Query: q}, &pgproto3.Bind{}, &pgproto3.Execute{})
	}
	sendAll(t, fe, append(ms, &pgproto3.Sync{})...)
	expectMsgs(t, fe, c, 1)
	if got, _ := orgsBefore(f.log(), "SELECT count(*) FROM users"); got != "SELECT set_config('lokadb.org', -1, false)" {
		t.Errorf("extended: SELECT count(*) FROM users ran after %q; upstream ran %q", got, f.log())
	}
}

// R1: EXECUTE of a statement PREPAREd with SQL is routed by that statement, its arguments its
// parameters; alone in its query string. Among other statements, or by Parse/Bind, refused.
func TestExecutePrepared(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	q := func(sql string) string {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		return errorCode(t, fe)
	}
	for _, sql := range []string{
		"PREPARE byorg(bigint) AS SELECT title FROM documents WHERE organization_id = $1",
		"PREPARE byslug(text) AS SELECT title FROM documents WHERE slug = $1",
		"PREPARE cloud AS SELECT count(*) FROM users",
	} {
		q(sql) // byslug names no organization: the policy refuses the PREPARE itself; prepared or not, its EXECUTE is refused below
	}
	if code := q("EXECUTE byorg(4)"); code != "" {
		t.Fatalf("EXECUTE byorg(4): %s; upstream ran %q", code, f.log())
	}
	if got, ran := orgsBefore(f.log(), "EXECUTE byorg(4)"); !ran || got != "SELECT set_config('lokadb.org', '4', false)" {
		t.Errorf("EXECUTE byorg(4) ran %v after %q, want organization 4; upstream ran %q", ran, got, f.log())
	}
	q("PREPARE byslug2(text) AS SELECT title FROM documents WHERE slug = $1")
	q("EXECUTE byslug2('a')")
	if ranAny(f.log(), "EXECUTE byslug2") {
		t.Errorf("EXECUTE of a statement naming no organization ran under the policy reject: %q", f.log())
	}
	if code := q("EXECUTE cloud"); code != "" {
		t.Fatalf("EXECUTE cloud: %s", code)
	}
	if got, _ := orgsBefore(f.log(), "EXECUTE cloud"); got != "SELECT set_config('lokadb.org', '-1', false)" {
		t.Errorf("EXECUTE of the cloud's statement ran after %q, want -1", got)
	}
	if code := q("EXECUTE byorg(4); SELECT 1"); code != "0A000" {
		t.Errorf("EXECUTE among other statements: code %q, want 0A000", code)
	}
	sendAll(t, fe, &pgproto3.Parse{Query: "EXECUTE byorg(4)"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if n := len(f.log()); ranAny(f.log()[n-1:], "EXECUTE byorg") || !ranAny(f.log(), "EXECUTE of a statement on LokaDB tables") {
		t.Errorf("EXECUTE by Parse/Bind: upstream ran %q", f.log())
	}
	// DEALLOCATE, then the name again for the cloud's statement
	q("DEALLOCATE byorg")
	q("PREPARE byorg AS SELECT count(*) FROM users")
	q("EXECUTE byorg")
	if got, _ := orgsBefore(f.log()[len(f.log())-3:], "EXECUTE byorg"); got != "" && got != "SELECT set_config('lokadb.org', '-1', false)" {
		t.Errorf("after DEALLOCATE: EXECUTE byorg ran after %q", got)
	}
}

// R1: REFRESH MATERIALIZED VIEW of a view over LokaDB tables reads every organization: the
// policy reject refuses it.
func TestRefreshMatViewRefused(t *testing.T) {
	m := testMeta()
	m.Views = map[string][]string{"docs_mv": {"documents"}}
	f := &fakeUpstream{}
	fe, _ := protoSession(t, f, m)
	sendAll(t, fe, &pgproto3.Query{String: "REFRESH MATERIALIZED VIEW docs_mv"})
	errorCode(t, fe)
	if !refused(f.log(), "REFRESH MATERIALIZED VIEW docs_mv") {
		t.Errorf("REFRESH under reject: upstream ran %q", f.log())
	}
	if a := analyze("REFRESH MATERIALIZED VIEW other_mv", m); a.Lokadb {
		t.Error("REFRESH of a view of the cloud's tables: Lokadb")
	}
}

// R1: a function or a view over LokaDB tables created through the proxy is known at once.
func TestCreatedOver(t *testing.T) {
	s := &session{db: "created", meta: testMeta(), srv: &Server{}}
	for _, c := range []struct{ create, use string }{
		{"CREATE FUNCTION api.titles() RETURNS SETOF text LANGUAGE sql AS $$ SELECT title FROM Documents $$", "SELECT * FROM api.titles()"},
		{"CREATE OR REPLACE FUNCTION api.n() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT count(*) FROM \"documents\"); END $$", "SELECT api.n()"},
		{"CREATE VIEW app.v AS SELECT d.* FROM documents d", "SELECT * FROM app.v"},
		{"CREATE MATERIALIZED VIEW app.mv AS SELECT * FROM app.v", "REFRESH MATERIALIZED VIEW app.mv"},
		{"CREATE FUNCTION api.via() RETURNS bigint LANGUAGE sql AS $$ SELECT api.n() $$", "SELECT api.via()"},
		{"CREATE FUNCTION api.std() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM document_versions; END", "SELECT api.std()"},
	} {
		if a := analyze(c.use, s.meta); a.Lokadb {
			t.Fatalf("%s: Lokadb before %s", c.use, c.create)
		}
		before := s.srv.schemaVersion(s.db).Load()
		s.noteCreated(c.create)
		if a := analyze(c.use, s.meta); !a.Lokadb {
			t.Errorf("%s: not Lokadb after %s", c.use, c.create)
		}
		if s.srv.schemaVersion(s.db).Load() == before || !s.metaDirty {
			t.Errorf("%s: the metadata is not read again", c.create)
		}
	}
	if s.noteCreated("CREATE FUNCTION api.none() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"); s.meta.Funcs["none"] {
		t.Error("a function of no LokaDB table: in Funcs")
	}
}

// Review of performance PF2: the organization column named (organization_id = 1), the ids of
// the statement are not looked up in __refs: under lokadb.org = 1 a key of another
// organization finds nothing anyway. Without it, or with several organizations, they are.
func TestTenantNamedNoLookup(t *testing.T) {
	f := &fakeUpstream{answer: refsAnswer}
	fe, c := protoSession(t, f, testMeta())
	for _, id := range []string{"5", "6", "7"} {
		sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1 AND id = " + id})
		expectMsgs(t, fe, c, 1)
	}
	if n := count(f.log(), "documents__refs"); n != 0 {
		t.Errorf("%d lookups with the organization named; upstream ran %q", n, f.log())
	}
	if got, _ := orgsBefore(f.log(), "AND id = 7"); got != "SELECT set_config('lokadb.org', '1', false)" {
		t.Errorf("ran after %q, want organization 1", got)
	}
	sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE id = 8"})
	expectMsgs(t, fe, c, 1)
	if n := count(f.log(), "documents__refs"); n != 1 {
		t.Errorf("by the id alone: %d lookups, want 1", n)
	}
	// a parameter of the organization column too (the extended protocol)
	sendAll(t, fe, &pgproto3.Parse{Name: "s", Query: "SELECT title FROM document_versions WHERE organization_id = $1 AND document_id = $2"},
		&pgproto3.Bind{PreparedStatement: "s", Parameters: [][]byte{[]byte("2"), []byte("9")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	if n := count(f.log(), "__refs"); n != 1 {
		t.Errorf("extended, organization named: %d lookups in all, want the 1 above; upstream ran %q", n, f.log())
	}
}

// Review of performance PF14: a lookup inside the client's transaction is one exchange with
// the cloud (the savepoint, the lookup, its release up to one Sync), not three.
func TestLookupInTxOneExchange(t *testing.T) {
	f := &fakeUpstream{answer: refsAnswer}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Query{String: "BEGIN"})
	expectMsgs(t, fe, c, 1)
	syncs := f.syncs()
	sendAll(t, fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE id = 5"})
	expectMsgs(t, fe, c, 1)
	if !inOrder(f.log(), "SAVEPOINT lokadb_lookup", "documents__refs", "RELEASE SAVEPOINT lokadb_lookup", "WHERE id = 5") {
		t.Fatalf("upstream ran %q", f.log())
	}
	if n := f.syncs() - syncs; n != 1 {
		t.Errorf("the lookup took %d exchanges, want 1", n)
	}
}

// refsAnswer: a refs table's lookup finds every id in organization 1 (as in the review of
// performance's proofs).
func refsAnswer(q string) ([][]string, string, bool) {
	if !strings.Contains(q, "__refs") {
		return nil, "", false
	}
	in := q[strings.LastIndex(q, "IN (")+4 : strings.LastIndex(q, ")")]
	var rows [][]string
	for _, id := range strings.Split(in, ",") {
		rows = append(rows, []string{strings.TrimSpace(id), "1"})
	}
	return rows, "", true
}

func count(log []string, sub string) int {
	n := 0
	for _, q := range log {
		if strings.Contains(q, sub) {
			n++
		}
	}
	return n
}

// Second review of routing: a query string with a transaction's statement (BEGIN, COMMIT…) or
// one that sets lokadb.org itself (RESET ALL, DISCARD ALL) did not set lokadb.org: a statement of
// no LokaDB table in it ran under the value left before, on a new connection the empty one
// (every organization). Closed by default on these ways too; after a ROLLBACK (TO) inside the
// client's transaction, where the value is the transaction's, a statement that reads rows is
// refused, as after a reset in the string.
func TestClosedOnEveryWay(t *testing.T) {
	const f = "SELECT reports.mystery()"
	for _, c := range []struct {
		name    string
		pre     []string
		sql     string
		want    string // the lokadb.org f runs under; "refused": not run
		refused bool
	}{
		{"BEGIN … COMMIT on a new connection", nil, "BEGIN; " + f + "; COMMIT", "SELECT set_config('lokadb.org', '-1', false)", false},
		{"COMMIT after it", []string{"BEGIN"}, f + "; COMMIT", "SELECT set_config('lokadb.org', '-1', false)", false},
		{"after a statement of organization 3", []string{"SELECT title FROM documents WHERE organization_id = 3"}, "BEGIN; " + f + "; COMMIT", "SELECT set_config('lokadb.org', '-1', false)", false},
		{"named", []string{"SET lokadb.tenant = 7"}, "BEGIN; " + f + "; COMMIT", "SELECT set_config('lokadb.org', '7', false)", false},
		{"before a reset", nil, f + "; RESET ALL", "SELECT set_config('lokadb.org', '-1', false)", false},
		{"after a reset", nil, "RESET ALL; " + f, "", true},
		{"after DISCARD ALL", nil, "DISCARD ALL; " + f, "", true},
		{"after ROLLBACK TO in the client's transaction", []string{"BEGIN", "SAVEPOINT a"}, "ROLLBACK TO SAVEPOINT a; " + f, "", true},
		{"after ROLLBACK in the client's transaction", []string{"BEGIN"}, "ROLLBACK; " + f, "", true},
	} {
		up := &fakeUpstream{}
		fe, _, s := protoSessionS(t, up, testMeta())
		if c.name == "named" {
			s.srv.cfg.Tenant = sessionTenant()
		}
		for _, q := range c.pre {
			sendAll(t, fe, &pgproto3.Query{String: q})
			if code := errorCode(t, fe); code != "" {
				t.Fatalf("%s: %s: %s", c.name, q, code)
			}
		}
		sendAll(t, fe, &pgproto3.Query{String: c.sql})
		code := errorCode(t, fe)
		got, ran := orgsBefore(up.log(), f)
		switch {
		case c.refused && (ran || code == ""):
			t.Errorf("%s: ran (code %q) after %q; upstream ran %q", c.name, code, got, up.log())
		case !c.refused && (!ran || got != c.want):
			t.Errorf("%s: ran %v after %q, want %q; upstream ran %q", c.name, ran, got, c.want, up.log())
		}
	}
}
