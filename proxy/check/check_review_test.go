package check

// Review 2 of lokadb check (tasks/review/check.md). Each test states what check should report;
// it failed on main 4af18c4 (the proof of the finding) and passes with the fix.

import (
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// R1. A statement on a LokaDB table that the proxy's parser does not read (PostgreSQL 18
// syntax, the parser is 17's): check reports parse_error, a warning that it "would pass
// unanalyzed". The proxy does not let it pass: analyze.go unparsed() makes it Lokadb and
// unhinted, and the fanout policy (default reject) refuses it.
func TestReviewParseErrorIsRefusedByProxy(t *testing.T) {
	c := testChecker()
	fan, err := proxy.Config{}.FanoutFor("app")
	if err != nil {
		t.Fatal(err)
	}
	c.fan = fan
	sql := `update documents set title = $1 where id = $2 returning with (old as o) o.title` // RETURNING WITH: PostgreSQL 18
	k, _ := c.classify(sql)
	live := fan.Decide(sql, proxy.Analyze(sql, c.meta))
	if live != proxy.FanoutRefused {
		t.Skipf("the proxy does not refuse it (%v): no finding", live)
	}
	if k == kFanout {
		k = c.fanoutKind(sql) // as workload() does
	}
	if k != kRefused {
		t.Errorf("check classifies it as %v (kParse=%v: finding parse_error, a warning that it passes unanalyzed); the proxy refuses it (0A000, fanout policy reject): want query_refused, a blocker", k, kParse)
	}
}

// R2 (check -profile direct judges by DirectIn alone) is block 10's: profile.go, with the
// proxy's decision shared by both.

// R3. Findings made with an empty object have the bare code as their key (keyOf with no
// subject): once a baseline accepts fk_to_cloud (or cloud_references, cdc_publication,
// logical_slots, profile_direct_fk_cloud), -ci never reports a new FK, publication or slot.
func TestReviewCIKeyOfCodeOnlyFindings(t *testing.T) {
	before := &checker{rep: &Report{}}
	before.add(Change, "fk_to_cloud", "", "FKs from LokaDB tables to cloud tables", []string{"documents.project_id -> projects"})
	base := before.rep.MakeBaseline(nil)

	after := &checker{rep: &Report{}}
	after.add(Change, "fk_to_cloud", "", "FKs from LokaDB tables to cloud tables",
		[]string{"documents.project_id -> projects", "document_versions.author_id -> users (ON DELETE CASCADE)"}) // a migration added one
	res := after.rep.CompareBaseline(&base, "accepted.json")
	if len(res.New) == 0 {
		t.Errorf("a new FK onto a cloud table is accepted by the old baseline entry %q: -ci exits %d", base.Accepted[0].Key, res.ExitCode())
	}
}

func codes(fs []Finding) string {
	var out []string
	for _, f := range fs {
		out = append(out, string(f.Severity)+":"+f.Code)
	}
	return strings.Join(out, ", ")
}

// R5. After enable, tables of -spec that enable has not taken yet get the schema checks.
func TestReviewSpecTablesAfterEnable(t *testing.T) {
	c := testChecker()
	c.o.Spec = &admin.Spec{Tables: []admin.SpecTable{
		{Name: "documents", TenantColumn: "organization_id"},
		{Name: "invoices", TenantColumn: "organization_id"}, // added to the spec after enable
	}}
	if !c.newSpecTables() {
		t.Errorf("invoices is in the spec and not LokaDB: newSpecTables = false")
	}
	c.o.Spec.Tables = c.o.Spec.Tables[:1]
	if c.newSpecTables() {
		t.Errorf("every table of the spec is LokaDB: newSpecTables = true")
	}
}
