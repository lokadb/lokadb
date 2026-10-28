package proxy

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// loadMeta against canned catalog answers: every query gets the rows of its own shape.
func TestLoadMeta(t *testing.T) {
	answers := []struct {
		prefix string
		rows   [][]string
	}{
		{"SELECT (to_regclass('lokadb.tables')", [][]string{{"true"}}},
		{"SELECT (to_regprocedure('lokadb.org_lock", [][]string{{"true"}}},
		{"SELECT table_name, tenant_column", [][]string{{"documents", "organization_id", "documents__refs", "documents__cloud", "id"}}},
		{"SELECT table_name, column_name", [][]string{{"documents", "organization_id", ""}}},
		{"SELECT t.table_name, a.attname", [][]string{{"documents", "id", "bigint", "false", "true", "false", "false", "nextval('documents__id_seq'::regclass)"},
			{"documents", "organization_id", "bigint", "false", "false", "false", "false", ""}, {"documents", "status", "text", "false", "true", "false", "false", "'draft'::text"}}},
		{"SELECT DISTINCT t.table_name FROM lokadb.tables t", [][]string{{"documents"}}}, // a LokaDB child's FK onto documents__refs
		{"SELECT t.table_name, bit_or(", [][]string{{"documents", "8"}}},                 // a trigger on DELETE alone
		{"SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs')", [][]string{{"true"}}},
		{"SELECT org::text, schema FROM lokadb.orgs", [][]string{{"1", "lokadb"}}},
		{"SELECT org::text, profile FROM lokadb.orgs", [][]string{{"1", "fdw+direct"}}},
		{"SELECT t.table_name, (CASE", [][]string{
			{"documents", "documents__org1", "FOR VALUES IN ('1')", "f", "false", "true"},
			{"documents", "documents__org5", "FOR VALUES IN ('5')", "f", "false", "false"},
			{"documents", "documents__o3", "FOR VALUES IN ('3')", "r", "true", "true"},
			{"documents", "documents__cloud", "DEFAULT", "r", "true", "true"},
			{"documents", "documents__none", "FOR VALUES IN ('-2')", "r", "false", "true"},
		}},
		{"SELECT (to_regprocedure('lokadb.same_org", [][]string{{"true"}}},
		{"SELECT coalesce(has_function_privilege(to_regprocedure('lokadb.grant_sync", [][]string{{"true"}}},
		{"SELECT DISTINCT t.table_name FROM pg_trigger", [][]string{{"documents"}}},
		{"SELECT DISTINCT (CASE", [][]string{{"outer_view", "inner_view"}, {"inner_view", "documents"}, {"inner_view", "projects"}}},
		{"SELECT DISTINCT p.proname", [][]string{{"archive_documents"}}},
	}
	q := func(sql string) ([][]string, error) {
		for _, a := range answers {
			if strings.HasPrefix(strings.TrimSpace(sql), a.prefix) {
				return a.rows, nil
			}
		}
		return nil, nil
	}
	m, err := loadMeta(q)
	if err != nil {
		t.Fatal(err)
	}
	if m.Foreign["documents"]["1"] != "documents__org1" {
		t.Errorf("foreign partitions: %v", m.Foreign)
	}
	if m.Local["documents"]["3"] != "documents__o3" || len(m.Foreign["documents"]) != 2 || !m.Writable["documents__cloud"] || m.Writable["documents__none"] || m.Writable["documents__org1"] || !m.SameOrg || !m.StmtTriggers["documents"] {
		t.Errorf("local partitions %v, writable %v, same_org %v", m.Local, m.Writable, m.SameOrg)
	}
	// a foreign partition is addressed directly only with the parent's privileges there
	if !m.Direct["documents__org1"] || m.Direct["documents__org5"] || m.Direct["documents__o3"] || !m.GrantSync {
		t.Errorf("direct %v, grant_sync %v", m.Direct, m.GrantSync)
	}
	if m.Schemas["1"] != "lokadb" {
		t.Errorf("schemas: %v", m.Schemas)
	}
	if m.Profiles["1"] != "fdw+direct" || m.Tables["documents"].UserTypes {
		t.Errorf("profiles %v, user types %v", m.Profiles, m.Tables["documents"].UserTypes)
	}
	if m.Tables["documents"].Key != "id" {
		t.Errorf("key: %q", m.Tables["documents"].Key)
	}
	if !m.RefsFromLokadb["documents"] {
		t.Errorf("FKs of LokaDB tables onto refs: %v", m.RefsFromLokadb)
	}
	// stage 8b: the defaults an INSERT on the direct path names, the key's sequence, row triggers
	if d := m.Tables["documents"]; !reflect.DeepEqual(d.Defaults, map[string]bool{"id": true, "status": true}) ||
		d.KeyDefault != "nextval('documents__id_seq'::regclass)" || d.Identity || d.Generated || !m.rowTrigger("documents", trigDelete) || m.rowTrigger("documents", trigInsert) || m.rowTrigger("documents", trigUpdate) {
		t.Errorf("defaults %v, key's default %q, identity %v, generated %v, row triggers %v", d.Defaults, d.KeyDefault, d.Identity, d.Generated, m.RowTriggers)
	}
	if !reflect.DeepEqual(m.Views, map[string][]string{"outer_view": {"documents"}, "inner_view": {"documents"}}) || !m.Funcs["archive_documents"] {
		t.Errorf("views %v, functions %v", m.Views, m.Funcs)
	}
	if !m.OrgLock {
		t.Error("org_lock not seen")
	}
}

// Block 9, B9.12: the proxy reads the metadata under the client's role. A role without the
// right to read lokadb's tables got no metadata, and the session passed its statements on
// LokaDB tables unrouted, past the fanout policy. An enabled database (its tables carry the
// policy lokadb_tenant, which the catalog shows to any role) whose metadata the role cannot read
// now gets a Meta that refuses them; one the catalog shows not enabled still gets none.
func TestLoadMetaDenied(t *testing.T) {
	permission := &pgconn.PgError{Code: "42501", Message: "permission denied for table tables"}
	policies := [][]string{{"documents"}, {"document_versions"}, {"documents__org1"}}
	for _, c := range []struct {
		name     string
		failAt   string // the query that fails, by prefix
		err      error
		visible  string // to_regclass('lokadb.tables')
		policies [][]string
		denied   bool
		wantErr  bool
	}{
		{"no SELECT on lokadb.tables", "SELECT table_name, tenant_column", permission, "true", policies, true, false},
		{"no USAGE on schema lokadb", "SELECT (to_regclass('lokadb.tables')", permission, "", policies, true, false},
		{"lokadb.tables not visible, policies there", "", nil, "false", policies, true, false},
		{"not enabled", "", nil, "false", nil, false, false},
		{"a failed read, not a refusal", "SELECT table_name, tenant_column", errors.New("connection reset"), "true", policies, false, true},
	} {
		q := func(sql string) ([][]string, error) {
			sql = strings.TrimSpace(sql)
			if c.failAt != "" && strings.HasPrefix(sql, c.failAt) {
				return nil, c.err
			}
			switch {
			case strings.HasPrefix(sql, "SELECT (to_regclass('lokadb.tables')"):
				return [][]string{{c.visible}}, nil
			case strings.Contains(sql, "pg_catalog.pg_policy"):
				return c.policies, nil
			}
			return nil, nil
		}
		m, err := loadMeta(q)
		if c.wantErr != (err != nil) {
			t.Errorf("%s: err %v", c.name, err)
			continue
		}
		if got := m != nil && m.Denied != ""; got != c.denied {
			t.Errorf("%s: denied %v, want %v (meta %+v)", c.name, got, c.denied, m)
			continue
		}
		if !c.denied {
			continue
		}
		for _, sql := range []string{"SELECT * FROM documents WHERE id = 1", "INSERT INTO document_versions (document_id) VALUES (1)", "SELECT * FROM documents__org1"} {
			if r := Analyze(sql, m).rejection(); r != metaDeniedMsg {
				t.Errorf("%s: %s: rejection %q", c.name, sql, r)
			}
		}
		for _, sql := range []string{"SELECT 1", "SELECT * FROM users", "BEGIN"} {
			if r := Analyze(sql, m).rejection(); r != "" {
				t.Errorf("%s: %s: refused %q", c.name, sql, r)
			}
		}
	}
}
