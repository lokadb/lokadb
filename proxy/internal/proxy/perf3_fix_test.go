package proxy

import (
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// Third review of performance, perf3-13: the full parse of noteCreated and notePrepare is for a
// statement that begins with their words, not for every text that has them (created_at).
func TestStmtStart(t *testing.T) {
	for _, c := range []struct {
		sql                string
		create, preparesIt bool
	}{
		{`insert into "documents" ("title", "updated_at", "created_at") values ($1, $2, $3)`, false, false},
		{`CREATE FUNCTION f() RETURNS int LANGUAGE sql AS 'SELECT 1'`, true, false},
		{"  -- a comment\n  alter view v rename to w", true, false},
		{"/* x */ create view v as select 1", true, false},
		{"SELECT 1; CREATE VIEW v AS SELECT 2", true, false},
		{"select prepared_at, discarded from t", false, false},
		{"PREPARE p AS SELECT 1", false, true},
		{"DISCARD ALL", false, true},
		{"select 1; deallocate p", false, true},
	} {
		if got := createStart.MatchString(c.sql); got != c.create {
			t.Errorf("createStart %q: %v, want %v", c.sql, got, c.create)
		}
		if got := prepareStart.MatchString(c.sql); got != c.preparesIt {
			t.Errorf("prepareStart %q: %v, want %v", c.sql, got, c.preparesIt)
		}
	}
}

func BenchmarkNoteCreatedLaravel(b *testing.B) {
	s := &session{meta: testMeta(), srv: &Server{}}
	const sql = `insert into "documents" ("organization_id", "project_id", "slug", "title", "body", "updated_at", "created_at") values ($1, $2, $3, $4, $5, $6, $7) returning "id"`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.notePrepare(sql)
		s.noteCreated(sql)
	}
}

// perf3-23: has answers from an index made once per set of credentials, and a new set is seen.
func TestCredHas(t *testing.T) {
	var c credStore
	a := creds.Set{"db": {"1": {"direct": ""}, "2": {"direct": "host=x"}}}
	c.set.Store(&a)
	if !c.has("db", "direct") || c.has("db", "owner") || c.has("other", "direct") {
		t.Error("has over the first set")
	}
	b := creds.Set{"db": {"1": {"owner": "host=y"}}}
	c.set.Store(&b)
	if c.has("db", "direct") || !c.has("db", "owner") {
		t.Error("has after a new set: the old index kept")
	}
}

// perf3-24: viewKey gives the first matching view in sorted order, as it did by sorting them all.
func TestViewKeyFirst(t *testing.T) {
	m := &Meta{Views: map[string][]string{"b.v": {"documents"}, "a.v": {"documents"}, "c.w": {"documents"}}}
	if k := m.viewKey(&pg.RangeVar{Relname: "v"}); k != "a.v" {
		t.Errorf("viewKey v: %q, want a.v", k)
	}
}
