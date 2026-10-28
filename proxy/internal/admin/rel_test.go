package admin

import "testing"

func TestKeys(t *testing.T) {
	for _, c := range []struct{ key, schema, name, sql string }{
		{"documents", "public", "documents", `"documents"`},
		{"documents__cloud", "public", "documents__cloud", `"documents__cloud"`},
		{"public.acct.docs", "public", "acct.docs", `"acct.docs"`}, // a dot in a public table's name
		{"public.acct.docs__o3", "public", "acct.docs__o3", `"acct.docs__o3"`},
		{"app.documents", "app", "documents", `"app"."documents"`},
		{"app.documents__o3", "app", "documents__o3", `"app"."documents__o3"`},
		{"app.x.y", "app", "x.y", `"app"."x.y"`},
		{`"a.b".t`, "a.b", "t", `"a.b"."t"`},
		{`"a""b".t`, `a"b`, "t", `"a""b"."t"`},
		{`"a.b".t.u`, "a.b", "t.u", `"a.b"."t.u"`},
		{`"odd`, "public", `"odd`, `"""odd"`},
	} {
		s, n := SplitKey(c.key)
		if s != c.schema || n != c.name {
			t.Errorf("SplitKey(%s) = %q, %q; want %q, %q", c.key, s, n, c.schema, c.name)
		}
		if got := qr(c.key); got != c.sql {
			t.Errorf("qr(%s) = %s; want %s", c.key, got, c.sql)
		}
		if c.key != `"odd` {
			if got := RelKey(c.schema, c.name); got != c.key {
				t.Errorf("RelKey(%q, %q) = %s; want %s", c.schema, c.name, got, c.key)
			}
		}
	}
	if got := RelKey("public", "documents"); got != "documents" {
		t.Errorf("public key: %s", got)
	}
	if got := inSchemaOf("app.documents", "documents__key"); got != "app.documents__key" {
		t.Errorf("inSchemaOf: %s", got)
	}
	if got := parentKey("public.documents"); got != "documents" {
		t.Errorf("parentKey: %s", got)
	}
}
