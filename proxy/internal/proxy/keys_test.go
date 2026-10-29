package proxy

import (
	"encoding/binary"
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

const testUUID = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"

// uuidMeta: udocs keyed by a uuid, uversions under them through a uuid FK, utags keyed by a
// varchar (block 17).
func uuidMeta() *Meta {
	m := &Meta{
		Tables: map[string]*Table{
			"udocs": {Name: "udocs", TenantCol: "organization_id", Refs: "udocs__refs", Cloud: "udocs__cloud", Key: "id",
				Cols: []string{"id", "organization_id", "title"}, ColType: map[string]string{"id": "uuid", "organization_id": "bigint", "title": "text"}},
			"uversions": {Name: "uversions", TenantCol: "organization_id", Refs: "uversions__refs", Cloud: "uversions__cloud", Key: "id",
				Cols: []string{"id", "doc_id", "organization_id", "content"}, ColType: map[string]string{"id": "uuid", "doc_id": "uuid", "organization_id": "bigint", "content": "text"}},
			"utags": {Name: "utags", TenantCol: "organization_id", Refs: "utags__refs", Cloud: "utags__cloud", Key: "code",
				Cols: []string{"code", "organization_id", "label"}, ColType: map[string]string{"code": "character varying(20)", "organization_id": "bigint", "label": "text"}},
		},
		Lookups: map[string]map[string]string{
			"udocs":     {"organization_id": "", "id": "udocs__refs"},
			"uversions": {"organization_id": "", "id": "uversions__refs", "doc_id": "udocs__refs"},
			"utags":     {"organization_id": "", "code": "utags__refs"},
		},
	}
	if err := loadKeyKinds(nil, m); err != nil {
		panic(err)
	}
	return m
}

func TestKeyKinds(t *testing.T) {
	m := uuidMeta()
	want := map[string]string{"udocs__refs": kindUUID, "uversions__refs": kindUUID, "utags__refs": kindText}
	if !reflect.DeepEqual(m.KeyKinds, want) {
		t.Fatalf("KeyKinds = %v, want %v", m.KeyKinds, want)
	}
	if k := m.keyKind(""); k != kindInt {
		t.Fatalf("the organization column: %q", k)
	}
	if k := testMeta().keyKind("documents__refs"); k != kindInt {
		t.Fatalf("metadata without kinds: %q", k)
	}
}

func TestCanonicalUUID(t *testing.T) {
	for _, in := range []string{
		testUUID,
		strings.ToUpper(testUUID),
		"{" + testUUID + "}",
		"a0eebc999c0b4ef8bb6d6bb9bd380a11",
		"a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11",
		"{A0EEBC99-9C0B4EF8-BB6D6BB9-BD380A11}",
	} {
		got, ok := canonicalUUID(in)
		if !ok || got != testUUID {
			t.Errorf("canonicalUUID(%q) = %q, %v; want %q", in, got, ok, testUUID)
		}
	}
	for _, in := range []string{
		"", "a0eebc99", testUUID + "0", "{" + testUUID, testUUID + "}", "-" + testUUID,
		"a0eebc99--9c0b-4ef8-bb6d-6bb9bd380a11", "a0eebc9-99c0b-4ef8-bb6d-6bb9bd380a11", " " + testUUID,
		"g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "7",
	} {
		if got, ok := canonicalUUID(in); ok {
			t.Errorf("canonicalUUID(%q) = %q, want refused", in, got)
		}
	}
}

func TestValuesByKind(t *testing.T) {
	raw, _ := hex.DecodeString(strings.ReplaceAll(testUUID, "-", ""))
	text8 := []byte("abcdefgh") // 8 bytes: not an int8 for a text key
	// a binary uuid[] of two elements (array_send)
	arr := binary.BigEndian.AppendUint32(nil, 1)
	arr = binary.BigEndian.AppendUint32(arr, 0)
	arr = binary.BigEndian.AppendUint32(arr, 2950)
	arr = binary.BigEndian.AppendUint32(arr, 2)
	arr = binary.BigEndian.AppendUint32(arr, 1)
	for i := 0; i < 2; i++ {
		arr = binary.BigEndian.AppendUint32(arr, 16)
		arr = append(arr, raw...)
	}
	// the same with an element of 15 bytes: no uuid
	bad := append([]byte{}, arr[:len(arr)-16-4]...)
	bad = binary.BigEndian.AppendUint32(bad, 15)
	bad = append(bad, raw[:15]...)
	p := &portalInfo{params: [][]byte{raw, text8, []byte(strings.ToUpper(testUUID)), arr, []byte("{" + strings.ToUpper(testUUID) + ",not-a-uuid}"), []byte("007"), bad, []byte(testUUID)},
		formats: []int16{1, 1, 0, 1, 0, 0, 1, 1}}
	cases := []struct {
		v    ValueRef
		kind string
		want []string // nil: not readable
	}{
		{ValueRef{Param: 1}, kindUUID, []string{testUUID}},
		{ValueRef{Param: 1}, kindInt, nil}, // 16 bytes are no integer
		{ValueRef{Param: 2}, kindText, []string{"abcdefgh"}},
		{ValueRef{Param: 2}, kindUUID, nil},
		{ValueRef{Param: 3}, kindUUID, []string{testUUID}},
		{ValueRef{Param: 3}, kindText, []string{strings.ToUpper(testUUID)}},
		{ValueRef{Param: 4, Array: true}, kindUUID, []string{testUUID, testUUID}},
		{ValueRef{Param: 5, Array: true}, kindUUID, nil}, // one element is no uuid
		{ValueRef{Param: 6}, kindInt, []string{"7"}},
		{ValueRef{Param: 6}, kindText, []string{"007"}},
		{ValueRef{Param: 7, Array: true}, kindUUID, nil},
		{ValueRef{Param: 8}, kindUUID, nil}, // a text parameter sent binary to a uuid key: 36 bytes, not read
		{ValueRef{Const: "{" + testUUID + "}"}, kindUUID, []string{testUUID}},
		{ValueRef{Null: true}, kindUUID, []string{nullValue}},
	}
	for _, c := range cases {
		c.v.Kind = c.kind
		got, ok := p.values(c.v)
		if c.want == nil && ok || c.want != nil && (!ok || !reflect.DeepEqual(got, c.want)) {
			t.Errorf("values(%+v) = %q, %v; want %q", c.v, got, ok, c.want)
		}
	}
	if got, ok := constOnly(ValueRef{Const: strings.ToUpper(testUUID), Kind: kindUUID}); !ok || got[0] != testUUID {
		t.Errorf("constOnly: %q, %v", got, ok)
	}
	if _, ok := constOnly(ValueRef{Const: "abc", Kind: kindInt}); ok {
		t.Errorf("constOnly read abc as an integer")
	}
	if got, ok := constOnly(ValueRef{Const: "A0EE"}); !ok || got[0] != "A0EE" {
		t.Errorf("constOnly without a kind: %q, %v; want the literal as it is", got, ok)
	}
}

// A uuid key found in any form Postgres takes: the organization cache holds the canonical
// form the refs table gives back. Before block 17 a value that was no integer named no
// organization (a fanout).
func TestResolveOrgUUID(t *testing.T) {
	s := &session{db: "uuid-test", meta: uuidMeta(), orgCache: map[string]map[string]string{
		"udocs__refs": {testUUID: "1"}, "utags__refs": {"Tag-1": "2"}}}
	for _, c := range []struct{ sql, want string }{
		{"SELECT * FROM udocs WHERE id = '" + strings.ToUpper(testUUID) + "'", "1"},
		{"SELECT * FROM udocs WHERE id = '{" + testUUID + "}'::uuid", "1"},
		{"SELECT * FROM udocs WHERE id IN ('" + testUUID + "', 'A0EEBC999C0B4EF8BB6D6BB9BD380A11')", "1"},
		{"SELECT * FROM udocs WHERE id = ANY('{" + testUUID + "}'::uuid[])", "1"},
		{"SELECT * FROM uversions WHERE doc_id = '" + testUUID + "'", "1"}, // through the FK onto udocs
		{"UPDATE uversions SET content = 'x' WHERE doc_id = '" + testUUID + "'", "1"},
		{"INSERT INTO uversions (doc_id, content) VALUES ('" + testUUID + "', 'x')", "1"},
		{"SELECT * FROM utags WHERE code = 'Tag-1'", "2"}, // text as it is
	} {
		an := analyze(c.sql, s.meta)
		org, incomplete, unknown, err := s.resolveOrg(an, constOnly, false)
		if org != c.want || incomplete || unknown || err != nil {
			t.Errorf("%s: org %q incomplete %v unknown %v err %v; want %q", c.sql, org, incomplete, unknown, err, c.want)
		}
	}
	an := analyze("SELECT * FROM udocs WHERE id = 'not-a-uuid'", s.meta)
	if _, _, unknown, _ := s.resolveOrg(an, constOnly, false); !unknown {
		t.Errorf("a value that is no uuid: not unknown")
	}
}

// A binary uuid parameter (pgx sends uuid so) is looked up in the refs table as a literal in
// its canonical form, and the statement runs under its organization.
func TestLookupBinaryUUID(t *testing.T) {
	raw, _ := hex.DecodeString(strings.ReplaceAll(testUUID, "-", ""))
	lookup := `SELECT id::text, coalesce(tenant::text, '-2') FROM "udocs__refs" WHERE id IN ('` + testUUID + `')`
	f := &fakeUpstream{answer: func(q string) ([][]string, string, bool) {
		if q == lookup {
			return [][]string{{testUUID, "1"}}, "", true
		}
		return nil, "", false
	}}
	fe, c := protoSession(t, f, uuidMeta())
	sendAll(t, fe, &pgproto3.Parse{Name: "v", Query: "INSERT INTO uversions (doc_id, content) VALUES ($1, 'x')", ParameterOIDs: []uint32{2950}},
		&pgproto3.Bind{PreparedStatement: "v", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{raw}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	log := f.log()
	found, set := false, false
	for _, q := range log {
		found = found || q == lookup
		set = set || q == "SELECT set_config('lokadb.org', 1, false)"
	}
	if !found || !set {
		t.Fatalf("upstream ran %q: want the lookup %q and lokadb.org 1", log, lookup)
	}
}

// An integer key past int8 (intRe takes 19 digits) is looked up as a number: Postgres compares
// it as numeric and finds nothing, where a quoted literal would fail the lookup with 22003
// (block 9's review of block 17).
func TestLookupIntPastInt8(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Query{String: "SELECT * FROM documents WHERE id = 9223372036854775808"})
	expectMsgs(t, fe, c, 1)
	want := `SELECT id::text, coalesce(tenant::text, '-2') FROM "documents__refs" WHERE id IN (9223372036854775808)`
	for _, q := range f.log() {
		if q == want {
			return
		}
	}
	t.Fatalf("upstream ran %q, want the lookup %q", f.log(), want)
}

// uuidOrgMeta: projects of organizations numbered by uuid (block 18), tasks under them.
func uuidOrgMeta() *Meta {
	m := &Meta{
		OrgLock: true,
		Tables: map[string]*Table{
			"projects": {Name: "projects", TenantCol: "account_id", Refs: "projects__refs", Cloud: "projects__cloud", Key: "id",
				Cols: []string{"id", "account_id", "name"}, ColType: map[string]string{"id": "uuid", "account_id": "uuid", "name": "text"}},
			"tasks": {Name: "tasks", TenantCol: "account_id", Refs: "tasks__refs", Cloud: "tasks__cloud", Key: "id",
				Cols: []string{"id", "project_id", "account_id"}, ColType: map[string]string{"id": "bigint", "project_id": "uuid", "account_id": "uuid"}},
		},
		Lookups: map[string]map[string]string{
			"projects": {"account_id": "", "id": "projects__refs"},
			"tasks":    {"account_id": "", "id": "tasks__refs", "project_id": "projects__refs"},
		},
	}
	if err := loadKeyKinds(nil, m); err != nil {
		panic(err)
	}
	return m
}

// Block 18: an organization numbered by uuid is its value, canonical, whatever form the
// statement gives it in; lokadb.tenant takes it too.
func TestUUIDOrganization(t *testing.T) {
	m := uuidOrgMeta()
	if m.TenantKind != kindUUID || m.keyKind("") != kindUUID || !m.valueOrgs() || testMeta().valueOrgs() {
		t.Fatalf("TenantKind %q, keyKind(\"\") %q", m.TenantKind, m.keyKind(""))
	}
	s := &session{db: "uuid-org-test", meta: m, orgCache: map[string]map[string]string{"projects__refs": {testUUID: "7f000000-0000-4000-8000-000000000001"}}}
	for _, c := range []struct{ sql, want string }{
		{"SELECT * FROM projects WHERE account_id = '" + strings.ToUpper(testUUID) + "'", testUUID},
		{"SELECT * FROM projects WHERE account_id = '{" + testUUID + "}'::uuid AND name = 'x'", testUUID},
		{"INSERT INTO projects (account_id, name) VALUES ('" + testUUID + "', 'x')", testUUID},
		{"SELECT * FROM tasks WHERE project_id = '" + testUUID + "'", "7f000000-0000-4000-8000-000000000001"}, // by the FK, refs give the organization
		{"SELECT * FROM projects WHERE account_id IS NULL", ""},
	} {
		an := analyze(c.sql, m)
		org, _, unknown, err := s.resolveOrg(an, constOnly, false)
		if org != c.want || err != nil || unknown && c.want != "" {
			t.Errorf("%s: org %q unknown %v err %v; want %q", c.sql, org, unknown, err, c.want)
		}
	}
	if _, _, unknown, _ := s.resolveOrg(analyze("SELECT * FROM projects WHERE account_id = '42'", m), constOnly, false); !unknown {
		t.Errorf("an integer for a uuid organization: not unknown")
	}
	for in, want := range map[string]string{strings.ToUpper(testUUID): testUUID, "7": "7", "*": "*", "": ""} {
		if got, err := tenantValue(in, TenantSetting, ""); err != nil || got != want {
			t.Errorf("tenantValue(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := tenantValue("acme", TenantSetting, ""); err == nil {
		t.Errorf("tenantValue took acme")
	}
}

// The pause lock of an organization numbered by uuid takes its value as text (simple and
// extended protocol); lokadb.org gets the value as a literal.
func TestUUIDOrganizationLock(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, uuidOrgMeta())
	sendAll(t, fe, &pgproto3.Query{String: "SELECT * FROM projects WHERE account_id = '" + strings.ToUpper(testUUID) + "'"})
	expectMsgs(t, fe, c, 1)
	raw, _ := hex.DecodeString(strings.ReplaceAll(testUUID, "-", ""))
	sendAll(t, fe, &pgproto3.Parse{Name: "p", Query: "SELECT * FROM projects WHERE account_id = $1", ParameterOIDs: []uint32{2950}},
		&pgproto3.Bind{PreparedStatement: "p", ParameterFormatCodes: []int16{1}, Parameters: [][]byte{raw}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	expectMsgs(t, fe, c, 1)
	log := f.log()
	if !inOrder(log, "SELECT set_config('lokadb.org', '"+testUUID+"', false), pg_advisory_lock_shared(lokadb.org_lock_v('"+testUUID+"'))",
		"SELECT pg_advisory_xact_lock_shared(lokadb.org_lock_v("+testUUID+"::text))") {
		t.Fatalf("upstream ran %q", log)
	}
}

// Block 18: a partition of an organization numbered by uuid is found by its value, as the
// session names the organization (Meta.Foreign: an upsert of a moved one went to DEFAULT).
func TestUUIDOrganizationBound(t *testing.T) {
	for bound, want := range map[string]string{
		"FOR VALUES IN ('" + testUUID + "')": testUUID,
		"FOR VALUES IN ('5')":                "5",
		"FOR VALUES IN (5)":                  "5",
		"FOR VALUES IN ('-2')":               "-2",
		"DEFAULT":                            "",
		"FOR VALUES IN ('abc')":              "abc", // a string organization
		"FOR VALUES IN ('01JA'::text)":       "01JA",
		"FOR VALUES IN ('it''s'::text)":      "it's",
		"FOR VALUES IN ('')":                 "", // the rows of none of a table named by strings
		"FOR VALUES IN ('a', 'b')":           "",
	} {
		got, _ := boundOf(bound)
		if got != want {
			t.Errorf("%s: %q, want %q", bound, got, want)
		}
	}
}

// Block 18: a statement aimed at an organization's partition (a moved one's foreign
// partition; a cloud one's upsert the same way, UT6) keeps to the organization by its value: a uuid went into
// the SQL bare (column "a0eebc99" does not exist).
func TestUUIDOrganizationTarget(t *testing.T) {
	m := uuidOrgMeta()
	for _, c := range []struct{ sql, part, want string }{
		{"SELECT count(*) FROM projects WHERE account_id = '" + testUUID + "'", "projects__org1", "projects.account_id = '" + testUUID + "'"},
		{"UPDATE projects SET name = 'x' WHERE account_id = '" + testUUID + "'", "projects__org1", "projects.account_id = '" + testUUID + "'"},
	} {
		an := analyze(c.sql, m)
		if an.targetText() == "" {
			t.Errorf("%s: no TargetSQL", c.sql)
			continue
		}
		if got := an.targetSQL(c.part, testUUID, true); !strings.Contains(got, c.want) || !strings.Contains(got, c.part) {
			t.Errorf("%s:\n%s\nwant %s", c.sql, got, c.want)
		}
	}
	if got := analyze("SELECT count(*) FROM documents WHERE organization_id = 1", testMeta()).targetSQL("documents__org1", "1", false); !strings.Contains(got, "documents.organization_id = 1") {
		t.Errorf("an integer organization: %s", got)
	}
}

// uuidOrgDirect: organization 1 numbered by uuid (testUUID): the organization columns are
// uuids, its partitions go by its value, its profile and the config by its number.
func uuidOrgDirect(m *Meta) {
	m.TenantKind = kindUUID
	for _, t := range m.Tables {
		t.ColType[t.TenantCol] = "uuid"
	}
	m.OrgNum = map[string]string{testUUID: "1"}
	for _, parts := range m.Foreign {
		parts[testUUID] = parts["1"]
		delete(parts, "1")
	}
}

// Block 18, merge III: the direct path of an organization numbered by uuid. The session names
// it by its value; the gate finds its profile and its database by its number and takes the
// proxy's lock by the value; a direct INSERT's refs rows get the value. It went the cloud's
// way: the gate took integers only.
func TestDirectUUIDOrganization(t *testing.T) {
	d := directSession(t, false, keyed, uuidOrgDirect)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = '"+strings.ToUpper(testUUID)+"'"); code != "" || !d.direct("SELECT title FROM documents") {
		t.Fatalf("read: error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	if !inOrder(d.cloud.log(), "pg_advisory_lock_shared(lokadb.org_lock_v("+testUUID+"))", "FROM lokadb.orgs WHERE org = 1)") {
		t.Fatalf("gate: %q", d.cloud.log())
	}
	d = directSession(t, false, keyed, uuidOrgDirect)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) VALUES ('"+testUUID+"', 'x')"); code != "" {
		t.Fatalf("insert: error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	if !inOrder(d.cloud.log(), "pg_advisory_xact_lock_shared(lokadb.org_lock_v("+testUUID+"))", // the key a parameter (perf3-11): the fake substitutes it as it is
		"INSERT INTO \"documents__refs\" (id, tenant) SELECT id, '"+testUUID+"' FROM lokadb_k") || !ranAny(d.org1.log(), "'x'") {
		t.Fatalf("insert: the cloud ran %q, the organization %q", d.cloud.log(), d.org1.log())
	}
	// without a number (no org-add) it has no profile: the cloud's way
	d = directSession(t, false, keyed, uuidOrgDirect, func(m *Meta) { m.OrgNum = nil })
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = '"+testUUID+"'"); code != "" || !d.clouded("title FROM") {
		t.Fatalf("no number: error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
}
