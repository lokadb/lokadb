package proxy

import (
	"strings"
	"testing"
)

// A ctid is unique only inside one partition: `DELETE … WHERE ctid IN (SELECT ctid … LIMIT n)` on
// a table with a partition for each organization deletes the rows of every partition that have
// the ctid of a row the subquery picked. The proxy rewrites that shape to the key and refuses
// every other use of ctid of a LokaDB table, whatever the fanout policy.
func TestCtid(t *testing.T) {
	m := testMeta()
	for _, c := range []struct {
		name, sql string
		rewritten string // the statement the proxy sends; "" when it is sent as it is
		refused   bool
		ctid      bool
	}{
		// Laravel's chunked delete (MassPrunable, ->limit()->delete()) and its update
		{"laravel prune", `delete from "documents" where "ctid" in (select "documents"."ctid" from "documents" where "slug" < $1 limit $2)`,
			`DELETE FROM documents WHERE id IN (SELECT documents.id FROM documents WHERE slug < $1 LIMIT $2)`, false, true},
		{"laravel update with limit", `update "documents" set "title" = $1 where "ctid" in (select "documents"."ctid" from "documents" where "slug" = $2 limit 10)`,
			`UPDATE documents SET title = $1 WHERE id IN (SELECT documents.id FROM documents WHERE slug = $2 LIMIT 10)`, false, true},
		{"ordered, aliased", `delete from documents d where d.ctid in (select x.ctid from documents x where x.slug < $1 order by x.id limit 5)`,
			`DELETE FROM documents d WHERE d.id IN (SELECT x.id FROM documents x WHERE x.slug < $1 ORDER BY x.id LIMIT 5)`, false, true},
		{"and another condition", `delete from documents where slug = $1 and ctid in (select ctid from documents where title = $2 limit 3)`,
			`DELETE FROM documents WHERE slug = $1 AND id IN (SELECT id FROM documents WHERE title = $2 LIMIT 3)`, false, true},
		{"with the organization", `delete from documents where organization_id = 1 and ctid in (select ctid from documents where organization_id = 1 limit 3)`,
			`DELETE FROM documents WHERE organization_id = 1 AND id IN (SELECT id FROM documents WHERE organization_id = 1 LIMIT 3)`, false, true},
		// every other use is refused, with or without the organization
		{"a ctid value", `delete from documents where ctid = '(0,1)'`, "", true, true},
		{"a ctid value with the organization", `delete from documents where organization_id = 1 and ctid = $1`, "", true, true},
		{"select ctid", `select ctid, id from documents where organization_id = 1`, "", true, true},
		{"returning ctid", `update documents set title = 'x' where id = 1 returning ctid`, "", true, true},
		{"self join on ctid", `delete from documents a using documents b where a.slug = b.slug and a.ctid < b.ctid`, "", true, true},
		{"another table's subquery", `delete from documents where ctid in (select ctid from document_versions limit 3)`, "", true, true},
		{"returning after the shape", `delete from documents where ctid in (select ctid from documents limit 3) returning id`, "", true, true},
		{"two ctid conditions", `delete from documents where ctid in (select ctid from documents limit 3) or ctid = '(0,1)'`, "", true, true},
		{"select from a join", `select d.id from documents d join document_versions v on v.ctid = d.ctid`, "", true, true},
		// not ours: a table that is not a LokaDB table, no ctid
		{"cloud table", `delete from audit_log where ctid in (select ctid from audit_log limit 3)`, "", false, false},
		{"no ctid", `delete from documents where id in (select id from documents limit 3)`, "", false, false},
		{"a column that only contains the word", `select * from documents where title = 'ctid' and organization_id = 1`, "", false, false},
		{"another table's ctid next to a LokaDB table", `select a.id from documents d, audit_log a where d.organization_id = 1 and a.ctid = '(0,1)'`, "", false, false},
	} {
		a := analyze(c.sql, m)
		got := ""
		if a.Rewritten != "" {
			got = a.Rewritten
		}
		switch {
		case c.refused && !strings.Contains(a.Forbidden, "ctid"):
			t.Errorf("%s: not refused (Forbidden %q, rewritten %q)", c.name, a.Forbidden, got)
		case !c.refused && a.Forbidden != "":
			t.Errorf("%s: refused: %s", c.name, a.Forbidden)
		case !c.refused && got != c.rewritten:
			t.Errorf("%s:\n got %q\nwant %q", c.name, got, c.rewritten)
		case (len(a.Ctid) > 0) != c.ctid:
			t.Errorf("%s: Ctid %v, want %v", c.name, a.Ctid, c.ctid)
		}
		if c.refused && a.rejection() == "" {
			t.Errorf("%s: no rejection", c.name)
		}
	}
}

// A composite key has no single column to find the rows by: refused.
func TestCtidCompositeKey(t *testing.T) {
	m := testMeta()
	m.Tables["documents"].Key = `["organization_id","slug"]`
	a := analyze(`delete from documents where ctid in (select ctid from documents where slug < $1 limit 5)`, m)
	if !strings.Contains(a.Forbidden, "ctid") || a.Rewritten != "" {
		t.Errorf("Forbidden %q, rewritten %q", a.Forbidden, a.Rewritten)
	}
}

// The key the table has is the column it is rewritten to, not "id".
func TestCtidKeyColumn(t *testing.T) {
	m := testMeta()
	m.Tables["documents"].Key = "slug"
	a := analyze(`delete from documents where ctid in (select ctid from documents where title = $1 limit 5)`, m)
	if a.Forbidden != "" || !strings.Contains(a.Rewritten, "slug IN (SELECT slug FROM documents") {
		t.Errorf("Forbidden %q, rewritten %q", a.Forbidden, a.Rewritten)
	}
}

// The workload check asks which tables a text finds rows of by ctid.
func TestCtidUse(t *testing.T) {
	m := testMeta()
	got := CtidUse(`delete from "documents" where "ctid" in (select "documents"."ctid" from "documents" limit 5)`, m, defaultPath)
	if len(got) != 1 || got[0] != "documents" {
		t.Errorf("tables: %v", got)
	}
	if got := CtidUse(`select 1 from documents where id = 1`, m, defaultPath); len(got) != 0 {
		t.Errorf("no ctid: %v", got)
	}
}

// commits has no organization column of its own in the application: its team is its repository's
// (repository_id → repositories.team_id), and Laravel's Commit::firstOrNew(…)->save() lists
// repository_id last among the columns.
func commitsMeta(parentLookup bool, refs string) *Meta {
	cols := []string{"id", "team_id", "repository_id", "build_id", "sha", "author_email", "subject"}
	ct := map[string]string{}
	for _, c := range cols {
		ct[c] = "character(26)"
	}
	lk := map[string]string{"team_id": "", "id": "commits__refs"}
	if parentLookup {
		lk["repository_id"] = "repositories__refs"
	}
	return &Meta{
		Tables:  map[string]*Table{"commits": {Name: "commits", TenantCol: "team_id", Refs: refs, Cloud: "commits__cloud", Cols: cols, ColType: ct, Key: "id"}},
		Lookups: map[string]map[string]string{"commits": lk},
	}
}

func TestInsertThroughParentColumn(t *testing.T) {
	const ins = `insert into "commits" ("id", "sha", "author_email", "subject", "build_id", "repository_id") values ($1, $2, $3, $4, $5, $6) returning "id"`
	a := analyze(ins, commitsMeta(true, "commits__refs"))
	if a.Unhinted || len(a.Hints) != 1 || a.Hints[0].Column != "repository_id" || a.Hints[0].Values[0].Param != 6 {
		t.Errorf("repository_id last: unhinted %v hints %+v gaps %s", a.Unhinted, a.Hints, a.GapText())
	}
	// the lookup of the parent column is not there (a table attached by a column the INSERT does not name):
	// the reason names the columns it could not use, not "no condition"
	a = analyze(ins, commitsMeta(false, "commits__refs"))
	if !a.Unhinted || strings.Contains(a.GapText(), "no_condition") || !strings.Contains(a.GapText(), "unknown_column commits.repository_id") {
		t.Errorf("no parent lookup: unhinted %v gaps %s", a.Unhinted, a.GapText())
	}
	// a table the metadata knows without its refs table (Refs ""): its organization column, whose lookup is
	// "" too, is still the organization of the row, not "its own key"
	m := commitsMeta(true, "")
	delete(m.Lookups["commits"], "id")
	a = analyze(`insert into commits (id, team_id, sha) values ($1, $2, $3)`, m)
	if a.Unhinted || len(a.Hints) != 1 || a.Hints[0].Column != "team_id" {
		t.Errorf("no refs table, the organization named: unhinted %v hints %+v gaps %s", a.Unhinted, a.Hints, a.GapText())
	}
	a = analyze(ins, m)
	if a.Unhinted || len(a.Hints) != 1 || a.Hints[0].Column != "repository_id" {
		t.Errorf("no refs table, through the parent: unhinted %v hints %+v gaps %s", a.Unhinted, a.Hints, a.GapText())
	}
	// the key alone is not an organization (a plain INSERT: the row is new)
	a = analyze(`insert into commits (id, sha) values ($1, $2)`, commitsMeta(true, "commits__refs"))
	if !a.Unhinted {
		t.Errorf("only the new row's own key: %+v", a.Hints)
	}
}

func TestContainsFold(t *testing.T) {
	for _, c := range []struct {
		s    string
		want bool
	}{{"select ctid from t", true}, {"SELECT CTID FROM t", true}, {`delete from "t" where "CtId" = 1`, true}, {"select cti from t", false}, {"", false}, {"ctid", true}, {"ctidx", true}, {"select id from t", false}} {
		if got := containsFold(c.s, "ctid"); got != c.want {
			t.Errorf("%q: %v", c.s, got)
		}
	}
}
