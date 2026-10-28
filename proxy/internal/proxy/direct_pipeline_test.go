package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Item 6: a pipeline of the client's prepared statements (pgx.Batch, JDBC executeBatch), every
// one of them direct for organization 1, goes to its database in one exchange, after one gate
// in the cloud (the pause and a check per statement); a pipeline with another organization's
// statement goes the cloud's way whole, as before (one with a write of 8b goes direct since block
// 9's pipeWrite: TestDirectPipelineWrites).
func TestDirectPipeline(t *testing.T) {
	// organization 2 could go direct too: only the pipeline's own rule keeps it off
	d := directSession(t, false, keyed, func(m *Meta) { m.Profiles["2"] = ProfileFDWDirect })
	d.s.srv.cfg.CustomersApp["db"]["2"] = "host=org1"
	for name, sql := range map[string]string{
		"a": "SELECT title FROM documents WHERE organization_id = $1",
		"b": "SELECT slug FROM documents WHERE organization_id = $1 AND id > $2",
		"u": "UPDATE documents SET title = $1 WHERE organization_id = $2",
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
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}}
	}
	pipe := func(units ...[]pgproto3.FrontendMessage) ([]string, string) {
		var ms []pgproto3.FrontendMessage
		for _, u := range units {
			ms = append(ms, u...)
		}
		sendAll(t, d.fe, append(ms, &pgproto3.Sync{})...)
		types, code := d.answerTypes(t)
		return types, code
	}
	cloud0, org0 := len(d.cloud.log()), len(d.org1.log())
	types, code := pipe(unit("a", "1"), unit("b", "1", "3"), unit("u", "z", "1"), unit("a", "1"))
	if code != "" {
		t.Fatalf("pipeline: %q", code)
	}
	if n := strings.Count(strings.Join(types, " "), "CommandComplete"); n != 4 || types[len(types)-1] != "ReadyForQuery" || strings.Contains(strings.Join(types, " "), "ParseComplete") {
		t.Fatalf("the client's answer: %q", types)
	}
	org := d.org1.log()[org0:]
	if !inOrder(org, "SELECT title FROM documents", "SELECT slug FROM documents", "UPDATE documents SET title", "SELECT title FROM documents") {
		t.Fatalf("organization 1: %q", org)
	}
	gates := 0
	for _, q := range d.cloud.log()[cloud0:] {
		if strings.Contains(q, "FROM lokadb.orgs WHERE org = 1)") {
			gates++
		}
		if strings.Contains(q, "documents") && !strings.Contains(q, "lokadb.orgs") && !strings.Contains(q, "to_regclass") {
			t.Errorf("the cloud ran a statement of the pipeline: %q", q)
		}
	}
	if gates != 3 { // one check per statement form (a, b, u), in one round trip with the pause
		t.Errorf("gate checks: %d, cloud %q", gates, d.cloud.log()[cloud0:])
	}
	// another organization's statement: the whole pipeline in the cloud
	for _, other := range [][]pgproto3.FrontendMessage{unit("a", "2")} {
		n := len(d.org1.log())
		if _, code := pipe(unit("a", "1"), other); code != "" {
			t.Fatalf("a pipeline the cloud's way: %q", code)
		}
		if len(d.org1.log()) != n {
			t.Fatalf("went direct: %q", d.org1.log()[n:])
		}
	}
}

// A pipeline inside a direct transaction (pgx's tx.SendBatch): the deferred transaction opens
// with the pipeline's checks (txGateAll), its statements run in the organization's transaction;
// a second pipeline in the open transaction too; COMMIT commits the organization, then the cloud.
func TestDirectTxPipeline(t *testing.T) {
	d, l := txSession(t)
	for name, sql := range map[string]string{
		"a": "SELECT title FROM documents WHERE organization_id = $1",
		"u": "UPDATE documents SET title = $1 WHERE organization_id = $2",
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
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}}
	}
	pipe := func(units ...[]pgproto3.FrontendMessage) {
		var ms []pgproto3.FrontendMessage
		for _, u := range units {
			ms = append(ms, u...)
		}
		sendAll(t, d.fe, append(ms, &pgproto3.Sync{})...)
		if _, code, st := d.answerAll(t); code != "" || st != 'T' {
			t.Fatalf("pipeline: %q %q", code, st)
		}
	}
	n := len(l.get())
	d.exchange(t, "BEGIN")
	pipe(unit("a", "1"), unit("u", "p1", "1"))
	pipe(unit("u", "p2", "1"), unit("a", "1"))
	if _, code, _ := d.exchange(t, "COMMIT"); code != "" {
		t.Fatalf("COMMIT: %q", code)
	}
	all := l.get()[n:]
	if !inOrder(all, "cloud: BEGIN", "org1: BEGIN", "org1: SELECT title", "org1: UPDATE documents SET title", "org1: UPDATE documents SET title", "org1: SELECT title", "org1: COMMIT") || !ranAny(l.get(), "cloud: COMMIT") {
		t.Fatalf("the transaction: %q", all)
	}
	for _, q := range all {
		if strings.Contains(q, "pg_advisory_lock_shared(") || strings.HasPrefix(q, "cloud: UPDATE") || strings.HasPrefix(q, "cloud: SELECT title") {
			t.Fatalf("outside the transaction's gate or in the cloud: %q in %q", q, all)
		}
	}
}

// Block 9's review: an error in the pipeline's second statement skips the third, whose Parse
// (its first time on the connection) the server skips too: the next pipeline with it Parses it
// again, never Binds a name the organization's database does not have (26000); a pipeline of
// one statement many times sends its gate check once.
func TestDirectPipelineSkippedParse(t *testing.T) {
	d := directSession(t, false)
	for name, sql := range map[string]string{
		"a": "SELECT title FROM documents WHERE organization_id = $1",
		"b": "SELECT slug FROM documents WHERE organization_id = $1",
		"c": "SELECT body FROM documents WHERE organization_id = $1",
	} {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Sync{})
		if code := errorCode(t, d.fe); code != "" {
			t.Fatalf("prepare %s: %s", name, code)
		}
	}
	unit := func(name string) []pgproto3.FrontendMessage {
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: [][]byte{[]byte("1")}}, &pgproto3.Execute{}}
	}
	pipe := func(units ...[]pgproto3.FrontendMessage) string {
		var ms []pgproto3.FrontendMessage
		for _, u := range units {
			ms = append(ms, u...)
		}
		sendAll(t, d.fe, append(ms, &pgproto3.Sync{})...)
		_, code := d.answerTypes(t)
		return code
	}
	d.org1.failExec = map[int]bool{2: true} // b fails on the organization's server
	if code := pipe(unit("a"), unit("b"), unit("c")); code != "22012" {
		t.Fatalf("the failed pipeline: %q", code)
	}
	if code := pipe(unit("c"), unit("a")); code != "" {
		t.Fatalf("the next pipeline: %q, org1 %q", code, d.org1.log())
	}
	if !ranAny(d.org1.log(), "SELECT body FROM documents") {
		t.Fatalf("c did not run: %q", d.org1.log())
	}
	cloud0 := len(d.cloud.log())
	if code := pipe(unit("a"), unit("a"), unit("a"), unit("a")); code != "" {
		t.Fatalf("one statement four times: %q", code)
	}
	gates := 0
	for _, q := range d.cloud.log()[cloud0:] {
		if strings.Contains(q, "FROM lokadb.orgs WHERE org = 1)") {
			gates++
		}
	}
	if gates != 1 {
		t.Fatalf("gate checks for one statement four times: %d", gates)
	}
}
