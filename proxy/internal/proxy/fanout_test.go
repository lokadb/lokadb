package proxy

import (
	"strings"
	"testing"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

func TestFanoutPolicyConfig(t *testing.T) {
	cfg := Config{Fanout: map[string]FanoutPolicy{
		"*":       {Mode: FanoutLog},
		"reports": {Mode: FanoutAllow},
		"billing": {Allow: []string{"select * from documents where slug = 'x'"}},
	}}
	for db, want := range map[string]string{"reports": FanoutAllow, "billing": FanoutReject, "other": FanoutLog} {
		f, err := cfg.FanoutFor(db)
		if err != nil || f.Mode != want {
			t.Errorf("%s: mode %v (%v), want %s", db, f, err, want)
		}
	}
	if f, err := (Config{}).FanoutFor("any"); err != nil || f.Mode != FanoutReject {
		t.Errorf("without config: %v %v, want reject", f, err)
	}
	if _, err := (Config{Fanout: map[string]FanoutPolicy{"x": {Mode: "warn"}}}).FanoutFor("x"); err == nil {
		t.Error("unknown mode accepted")
	}
	if _, err := (Config{Fanout: map[string]FanoutPolicy{"x": {Allow: []string{"selec 1"}}}}).FanoutFor("x"); err == nil {
		t.Error("unparseable allow entry accepted")
	}
}

func TestFanoutDecide(t *testing.T) {
	m := testMeta()
	read := `select * from documents where slug = $1`
	write := `update documents set title = $1 where slug = $2`
	routed := `select * from documents where organization_id = $1 and slug = $2`
	cloud := `select * from projects`
	policy := func(mode string, allow ...string) *Fanout {
		f, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: mode, Allow: allow}}}.FanoutFor("db")
		if err != nil {
			t.Fatal(err)
		}
		return f
	}
	cases := []struct {
		policy *Fanout
		sql    string
		want   FanoutVerdict
	}{
		{policy(""), read, FanoutRefused},
		{policy(""), write, FanoutRefused},
		{policy(""), routed, FanoutNone},
		{policy(""), cloud, FanoutNone},
		{policy(FanoutRejectWrites), read, FanoutRun},
		{policy(FanoutRejectWrites), write, FanoutRefused},
		{policy(FanoutLog), write, FanoutRun},
		{policy(FanoutAllow), write, FanoutRun},
		// the allow list matches by fingerprint: other constants, list lengths and quoting
		{policy("", `SELECT * FROM "documents" WHERE "slug" = 'report'`), read, FanoutAllowed},
		{policy("", `select * from documents where slug in ('a', 'b')`), `select * from documents where slug in ($1, $2, $3)`, FanoutAllowed},
		{policy("", `select * from documents where slug = 'a'`), `select * from documents where title = $1`, FanoutRefused},
		{policy(FanoutRejectWrites, write), write, FanoutAllowed},
	}
	for i, c := range cases {
		if got := c.policy.Decide(c.sql, analyze(c.sql, m)); got != c.want {
			t.Errorf("case %d (%s, %s): got %d, want %d", i, c.policy.Mode, c.sql, got, c.want)
		}
	}
}

func TestFanoutAnalysis(t *testing.T) {
	m := testMeta()
	for _, c := range []struct {
		sql   string
		table string
		write bool
	}{
		{`select * from projects p join documents d on d.project_id = p.id where p.id = $1`, "documents", false},
		{`delete from document_versions where content = $1`, "document_versions", true},
		{`insert into documents (organization_id, slug) select organization_id, slug from documents where slug = $1`, "documents", true},
		{`update projects set name = $1 where id in (select project_id from documents where slug = $2)`, "documents", false},
	} {
		a := analyze(c.sql, m)
		if !a.Unhinted || a.UnhintedTable != c.table || a.Write != c.write {
			t.Errorf("%s: unhinted %v table %q write %v, want %q %v", c.sql, a.Unhinted, a.UnhintedTable, a.Write, c.table, c.write)
		}
	}
}

func TestFanoutRaiseSQL(t *testing.T) {
	f, _ := Config{}.FanoutFor("db")
	sql := f.raiseSQL(&Analysis{UnhintedTable: "documents"})
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 || tree.Stmts[0].Stmt.GetDoStmt() == nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if !strings.Contains(sql, "organization of documents rows") || !strings.Contains(sql, `"allow"`) {
		t.Errorf("message: %s", sql)
	}
	sql = f.raiseSQL(analyze(`select * from documents where slug = $1`, testMeta()))
	if _, err := pg.Parse(sql); err != nil || !strings.Contains(sql, "DETAIL = 'why: unknown_column documents.slug (=)'") {
		t.Errorf("detail: %s %v", sql, err)
	}
}

// Statements that name no organization are counted by shape (literals do not matter), with the reason.
func TestUnroutedCollected(t *testing.T) {
	var u unroutedSet
	an := &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "builds", Gaps: []Gap{{Kind: "unrestricted", Table: "builds", Column: "slug", Form: "="}}}
	u.note("app", "SELECT * FROM builds WHERE slug = 'a'", an, true)
	an2 := &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "builds", Gaps: an.Gaps}
	u.note("app", "SELECT * FROM builds WHERE slug = 'b'", an2, false)
	other := &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "imports", Write: true}
	u.note("app", "DELETE FROM imports WHERE done", other, true)
	s := &Server{}
	s.unrouted.m = u.m
	list, over := s.Unrouted()
	if over != 0 || len(list) != 2 || list[0].Count != 2 || list[0].Table != "builds" || list[0].Refused || len(list[0].Gaps) != 1 || !list[1].Write {
		t.Fatalf("%+v %d", list, over)
	}
	if list[0].SQL != "SELECT * FROM builds WHERE slug = $1" {
		t.Errorf("literals not removed: %q", list[0].SQL)
	}
}

// The log tells a statement without a customer once per shape, a refused one once a minute with how
// many times since, and all of them in a summary every ten minutes: not a line per query.
func TestUnroutedLogOnce(t *testing.T) {
	var u unroutedSet
	an := func() *Analysis { return &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "builds"} }
	if l := u.note("app", "SELECT * FROM builds WHERE slug = 'a'", an(), false); !l.first {
		t.Fatal("the first time is not logged")
	}
	for i := 0; i < 50; i++ {
		if l := u.note("app", "SELECT * FROM builds WHERE slug = 'x'", an(), false); l.first || l.refused || l.summary {
			t.Fatalf("run %d is logged again: %+v", i, l)
		}
	}
	if l := u.note("app", "DELETE FROM builds WHERE done", an(), true); !l.first {
		t.Fatal("the first refusal is not logged")
	}
	for i := 0; i < 5; i++ {
		if l := u.note("app", "DELETE FROM builds WHERE done", an(), true); l.refused {
			t.Fatal("a refusal within the minute is logged")
		}
	}
	for _, x := range u.m {
		x.loggedAt = x.loggedAt.Add(-2 * time.Minute)
	}
	if l := u.note("app", "DELETE FROM builds WHERE done", an(), true); !l.refused || l.since != 5 {
		t.Fatalf("after the minute: %+v", l)
	}
	u.summary = u.summary.Add(-11 * time.Minute)
	if l := u.note("app", "SELECT * FROM builds WHERE slug = 'y'", an(), false); !l.summary || l.shapes != 2 || l.runs != 52 {
		t.Fatalf("summary: %+v", l)
	}
}

// A decision about a table covers every statement on it, of any shape, in any mode: "allow" runs them in
// every organization's database, "refuse" refuses them without counting them as open again.
func TestFanoutTableDecision(t *testing.T) {
	m := testMeta()
	f, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: FanoutReject, Tables: map[string]string{"documents": TableAllow, "document_versions": TableRefuse}}}}.FanoutFor("db")
	if err != nil {
		t.Fatal(err)
	}
	for sql, want := range map[string]FanoutVerdict{
		`select * from documents where slug = $1`:            FanoutAllowed,
		`select * from documents order by id desc limit 20`:  FanoutAllowed, // a shape never seen before
		`select * from document_versions where content = $1`: FanoutDenied,
		`select * from documents where organization_id = $1`: FanoutNone,
	} {
		if got := f.Decide(sql, analyze(sql, m)); got != want {
			t.Errorf("%s: %v, want %v", sql, got, want)
		}
	}
	if _, err := (Config{Fanout: map[string]FanoutPolicy{"x": {Tables: map[string]string{"documents": "maybe"}}}}).FanoutFor("x"); err == nil {
		t.Error("an unknown table decision accepted")
	}
}

// A condition the proxy does not read on a column that is not the customer's does not hide the customer
// condition next to it: only the customer condition decides.
func TestNoFalseGaps(t *testing.T) {
	m := testMeta()
	for _, sql := range []string{
		`select * from documents where organization_id = $1 and title is null`,
		`select * from documents where organization_id = $1 and slug <> $2`,
		`select * from documents where organization_id = $1 and (title = $2 or slug = $3)`,
		`select * from documents where organization_id = $1 and slug in (select slug from documents where organization_id = $1)`,
	} {
		if an := analyze(sql, m); an.Unhinted {
			t.Errorf("%s: unhinted, gaps %s", sql, an.GapText())
		}
	}
	// and a real gap stays one: the customer column itself in a form the proxy does not read
	if an := analyze(`select * from documents where organization_id = $1 or title = $2`, m); !an.Unhinted {
		t.Error("an OR around the customer condition is taken as routed")
	}
}

// "read" lets a table's reads run everywhere and nothing else: a write without an organization is the
// mode's to decide, as if nothing had been decided (a future UPDATE must not run in every database).
func TestFanoutTableRead(t *testing.T) {
	m := testMeta()
	read, write := `select * from documents where slug = $1`, `update documents set title = $1 where slug = $2`
	for mode, wantWrite := range map[string]FanoutVerdict{FanoutReject: FanoutRefused, FanoutRejectWrites: FanoutRefused, FanoutLog: FanoutRun} {
		f, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: mode, Tables: map[string]string{"documents": TableRead}}}}.FanoutFor("db")
		if err != nil {
			t.Fatal(err)
		}
		if got := f.Decide(read, analyze(read, m)); got != FanoutAllowed {
			t.Errorf("%s: a read: %v", mode, got)
		}
		if got := f.Decide(write, analyze(write, m)); got != wantWrite {
			t.Errorf("%s: a write: %v, want %v", mode, got, wantWrite)
		}
	}
}

// The same shape in two databases of one proxy is two entries, each logged the first time: the
// policy and the decisions are per database (e2e guardapp ran a shape goapp had shown already).
func TestUnroutedPerDatabase(t *testing.T) {
	s := &Server{}
	u := &s.unrouted
	an := func() *Analysis { return &Analysis{} }
	if l := u.note("goapp", "SELECT count(*) FROM documents WHERE title LIKE '%'", an(), true); !l.first {
		t.Fatal("goapp: not first")
	}
	if l := u.note("guardapp", "SELECT count(*) FROM documents WHERE title LIKE '%'", an(), false); !l.first {
		t.Fatal("guardapp: the shape of another database hid its first line")
	}
	if l := u.note("guardapp", "SELECT count(*) FROM documents WHERE title LIKE 'x'", an(), false); l.first {
		t.Fatal("guardapp: the same shape again was first")
	}
	list, _ := s.Unrouted()
	if len(list) != 2 || list[0].DB == list[1].DB {
		t.Fatalf("%+v", list)
	}
}
