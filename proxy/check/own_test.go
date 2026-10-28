package check

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// An UPDATE that assigns the organization column is what the proxy keeps with lokadb.same_org,
// not an upsert; INSERT … ON CONFLICT stays one (found on a real database: Django writes every
// column on save()).
func TestClassifyAssignsOrganization(t *testing.T) {
	c := testChecker()
	c.meta.SameOrg = true // the database has lokadb.same_org: the proxy rewrites such an UPDATE
	for sql, want := range map[string]kind{
		`UPDATE documents SET organization_id = organization_id WHERE organization_id = $1 AND id = $2`:                                        kSameOrg,
		`UPDATE "documents" SET "organization_id" = $1, "title" = $2 WHERE "documents"."organization_id" = $3 AND "documents"."id" = $4`:       kSameOrg,
		`UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3`:                                                               kRouted,
		`insert into documents (organization_id, slug) values ($1, $2) on conflict (organization_id, slug) do update set slug = excluded.slug`: kUpsert,
	} {
		if got, _ := c.classify(sql); got != want {
			t.Errorf("%s: %s, want %s", sql, classNames[got], classNames[want])
		}
	}
}

// LokaDB's own statements (internal/ownsql's mark) are not the application's workload, from
// the proxy's recording as from pg_stat_statements: the proxy's metadata read of the
// organizations named by uuid was a query_forbidden blocker on a real database.
func TestOwnStatementsLeftOut(t *testing.T) {
	own := ownsql.Tag(`SELECT tenant, org::text FROM lokadb.tenants`)
	app := `SELECT * FROM documents WHERE organization_id = $1`
	w := proxy.Workload{Version: 1, Databases: map[string]*proxy.WorkloadDB{"app": {
		Statements: []*proxy.RecordedStatement{{SQL: own, Count: 3}, {SQL: app, Count: 5}},
		Transactions: []*proxy.RecordedTx{
			{Statements: []string{own}, Count: 3},
			{Statements: []string{app}, Count: 5},
		},
	}}}
	b, _ := json.Marshal(w)
	path := filepath.Join(t.TempDir(), "workload.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	c := testChecker()
	c.o.Workload = []string{path}
	c.rep.Database = "app"
	if err := c.workload(); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.pats[own]; ok || c.pats[app] != 5 {
		t.Errorf("statements %v: want the application's only", c.pats)
	}
	if len(c.txs) != 1 {
		t.Errorf("transaction shapes %d, want the application's one", len(c.txs))
	}
	for _, f := range c.rep.Findings {
		if f.Code == "query_forbidden" {
			t.Errorf("LokaDB's own statement reported: %s", f.Message)
		}
	}
	// a proxy before the mark: its metadata read is still known by its text
	if !c.proxyMade(`SELECT tenant, org::text FROM lokadb.tenants`) || !c.proxyMade(`SELECT (to_regclass('lokadb.tenants') IS NOT NULL)::text`) {
		t.Error("an unmarked metadata read of an older proxy is taken for the application's")
	}
}
