package proxy

import "testing"

// Second review of performance, F9: the direct path's shape of a text (two more parses of every
// new text) was made before the organization was known, for statements of organizations
// without a direct profile too. Now the organization first: organization 2 (fdw) leaves no
// shape; organization 1 (fdw+direct) gets one.
func TestShapeAfterTheOrganization(t *testing.T) {
	d := directSession(t, false)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 2 AND id = 5"); code != "" {
		t.Fatalf("organization 2: %s", code)
	}
	if n := d.s.direct.info.len(); n != 0 {
		t.Errorf("a statement of organization 2 (fdw): %d shapes made, want none", n)
	}
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1 AND id = 5"); code != "" {
		t.Fatalf("organization 1: %s", code)
	}
	if n := d.s.direct.info.len(); n != 1 {
		t.Errorf("a statement of organization 1 (fdw+direct): %d shapes made, want one", n)
	}
}

// directShape parses once: what it gives is what directInWith and a parse of its own gave.
func BenchmarkDirectShape(b *testing.B) {
	m := testMeta()
	m.Profiles = map[string]string{"1": ProfileFDWDirect}
	m.Foreign = map[string]map[string]string{"documents": {"1": "documents__o1"}}
	const sql = `SELECT "docs_document"."id", "docs_document"."title" FROM "documents" "docs_document" WHERE ("documents"."organization_id" = 1 AND "documents"."id" = 123456) LIMIT 21`
	an := analyze(sql, m)
	for i := 0; i < b.N; i++ {
		_ = directShape(sql, an, m, false, defaultPath)
	}
}
