package check

import (
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Review of check, finding 2: check -profile judged by DirectIn, the proxy by its direct
// path's forms too (proxy.DirectRefusal). A form the proxy refuses on profile direct (a column
// of an application type, an UPDATE of the organization column) is a blocker of the profile
// with its reason, not a pattern that goes direct; so is a statement after
// SET LOCAL lokadb.tenant = '*', and a transaction shape with such a statement.
func TestProfileDirectRefusedForms(t *testing.T) {
	c := testChecker()
	for _, tb := range c.meta.Tables {
		tb.Refs = "" // cloudRefs would ask the database
	}
	c.meta.Tables["documents"].UserTypes = true // e.g. status of an enum type of the application
	c.o = Options{Profile: "direct", Workload: []string{"recording"}}
	c.rep.Sources = []string{"recording"}
	read := `select * from documents where id = $1`
	move := `update document_versions set organization_id = $1 where id = $2`
	all := `select count(*) from document_versions where organization_id = $1`
	c.pats = map[string]int64{`SET LOCAL lokadb.tenant = '<organization>'`: 13, read: 10, move: 3, all: 2}
	c.txs = []*proxy.RecordedTx{
		{Statements: []string{`SET LOCAL lokadb.tenant = '<organization>'`, read}, Count: 10},
		{Statements: []string{`SET LOCAL lokadb.tenant = '<organization>'`, move}, Count: 3},
		{Statements: []string{`SET LOCAL lokadb.tenant = '*'`, all}, Count: 2},
	}
	for _, sql := range []string{read, move} {
		if !proxy.DirectIn(sql, c.meta, true, c.directPath()).OK || proxy.DirectRefusal(sql, c.meta, true, c.directPath()) == "" {
			t.Fatalf("%s: DirectIn takes it and the direct path does not: the case of the finding", sql)
		}
	}
	if err := c.profile(); err != nil {
		t.Fatal(err)
	}
	got := map[string]map[string]bool{}
	for _, f := range c.rep.Findings {
		if f.Severity == Blocker {
			if got[f.Code] == nil {
				got[f.Code] = map[string]bool{}
			}
			got[f.Code][f.Object] = true
		}
	}
	if !got["profile_direct_refused_form"][proxy.ShapeUserType] || !got["profile_direct_refused_form"][proxy.ShapeKeyUpdate] {
		t.Errorf("the forms the proxy refuses on direct: blockers %v, want profile_direct_refused_form for %s and %s", got, proxy.ShapeUserType, proxy.ShapeKeyUpdate)
	}
	if !got["profile_direct_all_organizations"][""] {
		t.Errorf("a statement after lokadb.tenant = '*': blockers %v, want profile_direct_all_organizations", got)
	}
	if !got["profile_direct_tx_refused"][""] {
		t.Errorf("the transaction shapes with a refused statement: blockers %v, want profile_direct_tx_refused", got)
	}
	if c.rep.Profile.PatternsDirect != 0 {
		t.Errorf("query_patterns_direct = %d of %d, want 0: none of them goes direct", c.rep.Profile.PatternsDirect, c.rep.Profile.Patterns)
	}
}

// Third review of check, H3: the recording keeps a transaction across organizations apart
// (proxy/record_tenant.go: SET LOCAL lokadb.tenant = '*', set_config('lokadb.tenant', '*', …)).
// check reads it so: the report run under '*' is refused by the fanout policy, and on profile
// direct it is a blocker.
func TestProfileStarFromRecording(t *testing.T) {
	for _, set := range []string{`SET LOCAL lokadb.tenant = '*'`, `SELECT set_config('lokadb.tenant', '*', $1)`} {
		c := testChecker()
		fan, err := proxy.Config{}.FanoutFor("app")
		if err != nil {
			t.Fatal(err)
		}
		c.fan = fan
		report := `select count(*) from documents`
		txs := []*proxy.RecordedTx{{Statements: []string{set, report}, Count: 5}}
		k, _ := c.classifyNamed(report, namedOnly(txs))
		if k == kFanout {
			k = c.fanoutKind(report)
		}
		if k != kRefused {
			t.Errorf("%s: a report after it classified %v, want kRefused (%v)", set, k, kRefused)
		}
		c = testChecker()
		for _, tb := range c.meta.Tables {
			tb.Refs = ""
		}
		c.o = Options{Profile: "direct", Workload: []string{"recording"}}
		c.rep.Sources = []string{"recording"}
		c.pats = map[string]int64{set: 5, report: 5}
		c.txs = txs
		if err := c.profile(); err != nil {
			t.Fatal(err)
		}
		blocked := false
		for _, f := range c.rep.Findings {
			blocked = blocked || f.Severity == Blocker && f.Code == "profile_direct_all_organizations"
		}
		if !blocked {
			t.Errorf("%s: profile direct gives no blocker (findings %v)", set, c.rep.Findings)
		}
	}
}
