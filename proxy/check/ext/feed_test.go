package ext

import (
	"testing"

	"github.com/lokadb/lokadb/proxy/check"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// feedSession: documents (organization_id) is a LokaDB table, projects a cloud one.
func feedSession() *check.Session {
	return check.NewOfflineSession(check.Offline{Meta: &proxy.Meta{
		Tables: map[string]*proxy.Table{"documents": {Name: "documents", TenantCol: "organization_id", Refs: "documents__refs", Cloud: "documents__cloud",
			Cols: []string{"id", "organization_id", "project_id", "slug", "title"}, ColType: map[string]string{"id": "bigint", "organization_id": "bigint", "project_id": "bigint", "slug": "text", "title": "text"}}},
		Lookups: map[string]map[string]string{"documents": {"organization_id": "", "id": "documents__refs"}}}})
}

// A feed: one table, its organization column with =, ORDER BY, LIMIT. Other statements are not.
func TestFeedShape(t *testing.T) {
	s := feedSession()
	for sql, want := range map[string]string{
		`SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id DESC LIMIT 20`:                                      "documents.id",
		`select * from "documents" where "documents"."organization_id" = $1 and title like $2 order by "documents"."slug" limit $3`: "documents.slug",
		`SELECT id FROM documents WHERE organization_id = $1 ORDER BY id`:                                                           "", // no LIMIT
		`SELECT id FROM documents WHERE id = $1 ORDER BY id LIMIT 1`:                                                                "", // not by the organization
		`SELECT id FROM documents WHERE organization_id = $1 OR id = $2 ORDER BY id LIMIT 5`:                                        "", // OR
		`SELECT id FROM projects WHERE organization_id = $1 ORDER BY id LIMIT 5`:                                                    "", // a cloud table
		// the organization of the joined table, not of the feed's
		`SELECT d.id FROM documents d JOIN projects p ON p.id = d.project_id WHERE p.organization_id = $1 ORDER BY d.id LIMIT 5`: "",
		// Django's select_related: the feed's table joined with a cloud table
		`SELECT "documents"."id", "projects"."name" FROM "documents" INNER JOIN "projects" ON ("documents"."project_id" = "projects"."id") WHERE "documents"."organization_id" = $1 ORDER BY "documents"."id" DESC LIMIT 20`: "documents.id",
	} {
		f, ok := feedShape(s, sql)
		got := ""
		if ok {
			got = f.table + "." + f.order
		}
		if got != want {
			t.Errorf("%s: %q, want %q", sql, got, want)
		}
	}
	s.AddFinding(check.Finding{Severity: check.Info, Code: "feed_index", Subject: feedSubject(feed{table: "documents", tenant: "organization_id", order: "id"})})
	if k := s.Report().Findings[0].Key; k != "feed_index:public.documents:organization_id,id" {
		t.Errorf("key %s", k)
	}
}
