package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Code review of routing (tasks/review/routing.md on claude/review2-routing-176434, 25.10): the
// findings of block 1, R1 and R5. Each test states the behaviour the proxy should have and
// failed on main 4af18c4 (R2, R4, R6, R8 are other blocks').

// orgsBefore: the lokadb.org values the proxy set upstream before the first statement of the
// log that contains sql (the last one is what sql ran under).
func orgsBefore(log []string, sql string) (last string, found bool) {
	for _, q := range log {
		if strings.HasPrefix(q, "SELECT set_config('lokadb.org'") {
			last = q
		}
		if strings.Contains(q, sql) && !strings.HasPrefix(q, "SELECT set_config") {
			return last, true
		}
	}
	return last, false
}

// R1a: the cooperative mode, lokadb.tenant = 7. SQL EXECUTE of a statement PREPAREd on a LokaDB
// table is not analysed (no Lokadb), so it runs under lokadb.org = ” — every organization —
// though the application named organization 7 (README: a row of another organization is not
// found by its id).
func TestReviewExecuteUnderNamedTenant(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, _ := protoSessionS(t, f, testMeta())
	q := func(sql string) string {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		return errorCode(t, fe)
	}
	for _, sql := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 7", "PREPARE p(bigint) AS SELECT title FROM documents WHERE id = $1"} {
		if code := q(sql); code != "" {
			t.Fatalf("%s: error %s; upstream ran %q", sql, code, f.log())
		}
	}
	if code := q("EXECUTE p(5)"); code != "" {
		t.Fatalf("EXECUTE: error %s", code)
	}
	last, ran := orgsBefore(f.log(), "EXECUTE p(5)")
	if ran && last != "SELECT set_config('lokadb.org', '7', false)" {
		t.Errorf("EXECUTE p(5) under lokadb.tenant = 7 ran after %q: every organization's rows are visible; upstream ran %q", last, f.log())
	}
}

// R1b: the same for a function over LokaDB tables called with its schema (any but public):
// scan skips Funcname of two parts unless the first is public, so the statement is not Lokadb.
func TestReviewSchemaFunctionUnderNamedTenant(t *testing.T) {
	m := testMeta()
	m.Funcs = map[string]bool{"doc_count": true} // loadDeps: reports.doc_count() reads documents
	f := &fakeUpstream{}
	fe, _, _ := protoSessionS(t, f, m)
	q := func(sql string) string {
		sendAll(t, fe, &pgproto3.Query{String: sql})
		return errorCode(t, fe)
	}
	q("BEGIN")
	q("SET LOCAL lokadb.tenant = 7")
	q("SELECT reports.doc_count()")
	last, ran := orgsBefore(f.log(), "SELECT reports.doc_count()")
	if ran && last != "SELECT set_config('lokadb.org', '7', false)" {
		t.Errorf("reports.doc_count() under lokadb.tenant = 7 ran after %q; upstream ran %q", last, f.log())
	}
	// without the cooperative mode it is not even a fanout: the policy reject does not see it
	if a := analyze("SELECT reports.doc_count()", m); !a.Lokadb {
		t.Errorf("SELECT reports.doc_count(): Lokadb false, the fanout policy never refuses it")
	}
}

// R1c: a view over a LokaDB table in a schema other than public, named bare (search_path):
// Meta.Views is by key (app.docs_v), qualify knows only the names of LokaDB and cloud tables, so
// the bare name is no view of LokaDB rows.
func TestReviewViewOutsidePublicBare(t *testing.T) {
	m := testMeta()
	m.Views = map[string][]string{"app.docs_v": {"documents"}}
	for _, path := range [][]string{{"app", "public"}, nil} {
		if a := analyzeIn("SELECT * FROM docs_v", m, path); !a.Lokadb {
			t.Errorf("SELECT * FROM docs_v (search_path %v): Lokadb false; under lokadb.org = '' it reads every organization", path)
		}
	}
	if a := analyze("SELECT * FROM app.docs_v", m); !a.Lokadb {
		t.Errorf("qualified app.docs_v: Lokadb false")
	}
}

// R1d: REFRESH MATERIALIZED VIEW of a view over LokaDB tables runs its query under lokadb.org =
// ” (every organization's database), and no fanout policy sees it.
func TestReviewRefreshMatView(t *testing.T) {
	m := testMeta()
	m.Views = map[string][]string{"docs_mv": {"documents"}}
	if a := analyze("REFRESH MATERIALIZED VIEW docs_mv", m); !a.Lokadb {
		t.Errorf("REFRESH MATERIALIZED VIEW docs_mv: Lokadb false: it reads every organization's database past the fanout policy")
	}
}

// R5: a statement whose values name two organizations is not confined to one (fanout.go: what
// the policy is for), yet it runs under lokadb.org = ” — every organization's partition, every
// organization's database — and the policy reject is never asked.
func TestReviewTwoOrgsPastPolicy(t *testing.T) {
	f := &fakeUpstream{}
	fe, _, _ := protoSessionS(t, f, testMeta())
	const sel = "SELECT title FROM documents WHERE organization_id IN (1, 2)"
	sendAll(t, fe, &pgproto3.Query{String: sel})
	errorCode(t, fe)
	if !refused(f.log(), sel) {
		last, _ := orgsBefore(f.log(), sel)
		t.Errorf("%s under the policy reject ran after %q; upstream ran %q", sel, last, f.log())
	}
}
