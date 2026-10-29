package proxy

import (
	"strings"
	"testing"

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
	u.note("SELECT * FROM builds WHERE slug = 'a'", an, true)
	an2 := &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "builds", Gaps: an.Gaps}
	u.note("SELECT * FROM builds WHERE slug = 'b'", an2, false)
	other := &Analysis{Lokadb: true, Unhinted: true, UnhintedTable: "imports", Write: true}
	u.note("DELETE FROM imports WHERE done", other, true)
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
