package check

import (
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// A spec without routing (the usual one) must not stop the check: cloudDeletes once
// read spec.Routing without looking whether it is there, and check panicked.
func TestCloudDeletesNoRouting(t *testing.T) {
	c := &checker{rep: &Report{Database: "app", Findings: []Finding{}}, spec: admin.Spec{}}
	if err := c.cloudDeletes(map[string][]string{"users": {"documents.user_id -> users (ON DELETE CASCADE)"}}); err != nil {
		t.Fatal(err)
	}
	if len(c.rep.Findings) != 1 || c.rep.Findings[0].Code != "fk_to_cloud_delete" || c.rep.Findings[0].Severity != Warning {
		t.Fatalf("findings: %+v", c.rep.Findings)
	}
	c = &checker{rep: &Report{Database: "app", Findings: []Finding{}},
		spec: admin.Spec{Routing: &admin.Routing{CloudTables: []admin.CloudTable{{Table: "users", TenantColumn: "organization_id"}}}}}
	if err := c.cloudDeletes(map[string][]string{"users": {"documents.user_id -> users"}}); err != nil {
		t.Fatal(err)
	}
	if len(c.rep.Findings) != 0 {
		t.Fatalf("declared in routing.cloud_tables, still: %+v", c.rep.Findings)
	}
}
