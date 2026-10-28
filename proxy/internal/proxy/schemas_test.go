package proxy

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Block 4, item 9: a bare table name resolves by the session's search path.
func TestQualifyByPath(t *testing.T) {
	app := &Meta{
		Tables:      map[string]*Table{"app.documents": {Name: "app.documents", TenantCol: "org_id", Key: "id", ColType: map[string]string{}}},
		Lookups:     map[string]map[string]string{"app.documents": {"org_id": ""}},
		Rels:        map[string][]string{"documents": {"app"}},
		SchemasUsed: true,
	}
	for _, c := range []struct {
		sql    string
		path   []string
		lokadb bool
	}{
		{"SELECT * FROM documents WHERE org_id = 1", []string{"app", "public"}, true},
		{"SELECT * FROM app.documents WHERE org_id = 1", []string{"public"}, true},
		{"SELECT * FROM documents WHERE org_id = 1", []string{"public"}, false}, // not on the path: Postgres refuses it
		{"WITH documents AS (SELECT 1 AS org_id) SELECT * FROM documents", []string{"app"}, false},
		{"SELECT * FROM documents WHERE org_id = 1", nil, true}, // unknown path, one schema has the name
	} {
		if a := analyzeIn(c.sql, app, c.path); a.Lokadb != c.lokadb || a.Forbidden != "" {
			t.Errorf("%s with search_path %v: lokadb %v (want %v), forbidden %q", c.sql, c.path, a.Lokadb, c.lokadb, a.Forbidden)
		}
	}

	// public.documents is the LokaDB table, app.documents a cloud table of the same name
	shadow := &Meta{
		Tables:      map[string]*Table{"documents": {Name: "documents", TenantCol: "org_id", Key: "id", ColType: map[string]string{}}},
		Lookups:     map[string]map[string]string{"documents": {"org_id": ""}},
		Rels:        map[string][]string{"documents": {"app", "public"}},
		SchemasUsed: true,
	}
	for _, c := range []struct {
		path   []string
		lokadb bool
	}{{[]string{"public"}, true}, {[]string{"app", "public"}, false}, {[]string{"public", "app"}, true}} {
		if a := analyzeIn("SELECT * FROM documents WHERE org_id = 1", shadow, c.path); a.Lokadb != c.lokadb {
			t.Errorf("shadowed documents, search_path %v: lokadb %v, want %v", c.path, a.Lokadb, c.lokadb)
		}
	}
	if a := analyzeIn("SELECT * FROM documents WHERE org_id = 1", shadow, nil); !strings.Contains(a.Forbidden, "several schemas") {
		t.Errorf("ambiguous name, unknown search_path: forbidden %q", a.Forbidden)
	}
	if a := analyzeIn("SELECT * FROM public.documents WHERE org_id = 1", shadow, nil); a.Forbidden != "" || !a.Lokadb {
		t.Errorf("qualified name, unknown search_path: lokadb %v, forbidden %q", a.Lokadb, a.Forbidden)
	}
	// a routed UPDATE aims at the partition in the table's schema
	app.Foreign = map[string]map[string]string{"app.documents": {"1": "app.documents__org1"}}
	a := analyzeIn("UPDATE documents SET title = 'x' WHERE org_id = 1 AND id = 5", app, []string{"app"})
	if a.Target != "app.documents" || !strings.Contains(a.targetSQL("app.documents__org1", "1"), `"app"."documents__org1"`) {
		t.Errorf("target %q, sql %q", a.Target, a.targetSQL("app.documents__org1", "1"))
	}
}

func TestPathEffects(t *testing.T) {
	for _, c := range []struct {
		sql  string
		want pathEffect
	}{
		{"SELECT 1", pathEffect{}},
		{"SET search_path TO app, public", pathEffect{sets: true, value: []string{"app", "public"}}},
		{`SET LOCAL search_path = "App", public`, pathEffect{sets: true, value: []string{"App", "public"}}},
		{`SELECT set_config('search_path', 'app, "X"', false)`, pathEffect{sets: true, value: []string{"app", "X"}}},
		{"SELECT set_config('search_path', $1, false)", pathEffect{unknown: true}},
		{"SELECT set_config($1, 'app', false)", pathEffect{unknown: true}},
		{"SELECT set_config('application_name', 'x', false)", pathEffect{}},
		{"RESET search_path", pathEffect{unknown: true}},
		{"RESET ALL", pathEffect{unknown: true}},
		{"DISCARD ALL", pathEffect{unknown: true}},
		{"SET ROLE app", pathEffect{unknown: true}},
		{"SET statement_timeout = 0", pathEffect{}},
		{"SET SCHEMA 'app'", pathEffect{sets: true, value: []string{"app"}}},
		{"set session schema 'app'", pathEffect{sets: true, value: []string{"app"}}},
		{"SET LOCAL SCHEMA 'app'", pathEffect{sets: true, value: []string{"app"}}},
		{"SET SESSION search_path = app, public", pathEffect{sets: true, value: []string{"app", "public"}}},
		{"SET search_path TO 'a,b'", pathEffect{sets: true, value: []string{"a,b"}}},
		{"SET search_path = DEFAULT", pathEffect{unknown: true}},
		{"SET search_path FROM CURRENT", pathEffect{unknown: true}},
		{"UPDATE pg_settings SET setting = 'app' WHERE name = 'search_path'", pathEffect{unknown: true}},
		{"UPDATE pg_catalog.pg_settings SET setting = 'app' WHERE name = 'search_path'", pathEffect{unknown: true}},
		{"SELECT pg_catalog.set_config('search_path', 'app', false)", pathEffect{sets: true, value: []string{"app"}}},
		{"SELECT 'schema' FROM information_schema.tables", pathEffect{}},
	} {
		if got := effectOnPath(c.sql); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: %+v, want %+v", c.sql, got, c.want)
		}
	}
	if got := ParsePath(`"$user", public, "My.Schema"`, "alice"); !reflect.DeepEqual(got, []string{"alice", "public", "My.Schema"}) {
		t.Errorf("parsePath: %v", got)
	}
}

// The session follows SET and SET LOCAL, and forgets a path changed in a transaction at its end.
func TestPathTracking(t *testing.T) {
	s := &session{}
	s.initPath(`"$user", public`, "alice")
	if got := s.sp.path; !reflect.DeepEqual(got, []string{"$user", "public"}) {
		t.Fatalf("initial: %v", got)
	}
	s.notePath("SET search_path TO app, public", false)()
	if !reflect.DeepEqual(s.sp.path, []string{"app", "public"}) || s.sp.txDirty {
		t.Errorf("after SET: %v dirty %v", s.sp.path, s.sp.txDirty)
	}
	s.notePath("SET LOCAL search_path TO other", true)()
	s.pathTxEnd()
	if s.sp.path != nil {
		t.Errorf("after a transaction that changed it: %v, want unknown", s.sp.path)
	}
	if f := s.notePath("SELECT 1", false); f != nil {
		t.Error("SELECT 1 changes the path")
	}
	s.initPath("app", "alice")
	s.pathTxEnd() // a transaction that did not change it
	if !reflect.DeepEqual(s.sp.path, []string{"app"}) {
		t.Errorf("a transaction without a change: %v", s.sp.path)
	}
}

func TestOrgSearchPath(t *testing.T) {
	s := &session{meta: &Meta{SchemasUsed: false}}
	if got := s.orgSearchPath("lokadb"); got != `"lokadb", "public"` {
		t.Errorf("no schemas: %s", got)
	}
	s.meta.SchemasUsed = true
	s.initPath("app, public", "alice")
	if got := s.orgSearchPath("lokadb"); got != `"app", "lokadb", "public"` {
		t.Errorf("app, public at an organization with schema lokadb: %s", got)
	}
}

// Block 4, item 6: a function that sets the session's search_path makes it unknown.
func TestPathFuncs(t *testing.T) {
	funcs := map[string]bool{"use_app": true}
	for _, c := range []struct {
		sql  string
		want bool
	}{
		{"SELECT use_app()", true},
		{"SELECT public.use_app()", true},
		{"CALL use_app()", true},
		{"SELECT 1; SELECT use_app(); SELECT 2", true},
		{"DO $$ BEGIN PERFORM use_app(); END $$", true},
		{"SELECT use_application FROM t", false},
		{"SELECT count(*) FROM documents", false},
	} {
		if got := callsPathFunc(c.sql, funcs); got != c.want {
			t.Errorf("%s: %v, want %v", c.sql, got, c.want)
		}
	}
	s := &session{meta: &Meta{SchemasUsed: true, PathFuncs: funcs}}
	s.initPath("public", "app")
	s.notePath("SELECT use_app()", false)()
	if s.sp.path != nil {
		t.Errorf("after use_app(): %v, want unknown", s.sp.path)
	}
	if e := effectOnPath("DO $$ BEGIN PERFORM set_config('search_path', 'app', false); END $$"); !e.unknown {
		t.Errorf("DO with search_path: %+v", e)
	}
}

// PF6: the cache of path effects does not grow without bound.
func TestPathEffectsBounded(t *testing.T) {
	for i := 0; i < 3*pathEffectsMax; i++ {
		effectOnPath(fmt.Sprintf("SET search_path TO s%d", i))
	}
	pathEffects.Lock()
	n := len(pathEffects.m)
	pathEffects.Unlock()
	if n > pathEffectsMax {
		t.Errorf("%d entries, want at most %d", n, pathEffectsMax)
	}
	if e := effectOnPath("SET search_path TO app"); !e.sets || len(e.value) != 1 || e.value[0] != "app" {
		t.Errorf("after the cache was emptied: %+v", e)
	}
}
