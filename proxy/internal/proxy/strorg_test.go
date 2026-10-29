package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Organizations and keys of type character(n) (Laravel's ulid()): the organization is its
// string, character(n) pads it, so the blanks at its end are not part of it.
const (
	ulidA = "01JAAAAAAAAAAAAAAAAAAAAAAA"
	ulidB = "01JBBBBBBBBBBBBBBBBBBBBBBB"
)

// ulidOrgMeta: builds of teams named by character(26), build_files under them (their team
// column is the text enable adds).
func ulidOrgMeta() *Meta {
	m := &Meta{
		OrgLock: true,
		Tables: map[string]*Table{
			"builds": {Name: "builds", TenantCol: "team_id", Refs: "builds__refs", Cloud: "builds__cloud", Key: "id",
				Cols: []string{"id", "team_id", "title"}, ColType: map[string]string{"id": "character(26)", "team_id": "character(26)", "title": "text"}},
			"build_files": {Name: "build_files", TenantCol: "team_id", Refs: "build_files__refs", Cloud: "build_files__cloud", Key: "id",
				Cols: []string{"id", "build_id", "team_id"}, ColType: map[string]string{"id": "character(26)", "build_id": "character(26)", "team_id": "text"}},
		},
		Lookups: map[string]map[string]string{
			"builds":      {"team_id": "", "id": "builds__refs"},
			"build_files": {"team_id": "", "id": "build_files__refs", "build_id": "builds__refs"},
		},
		OrgNum: map[string]string{ulidA: "1"},
		Local:  map[string]map[string]string{"builds": {ulidA: "builds__o1"}},
	}
	if err := loadKeyKinds(nil, m); err != nil {
		panic(err)
	}
	return m
}

func TestStringOrganization(t *testing.T) {
	m := ulidOrgMeta()
	if m.TenantKind != kindText || m.keyKind("") != kindChar || !m.valueOrgs() || m.KeyKinds["builds__refs"] != kindChar {
		t.Fatalf("TenantKind %q, keyKind(\"\") %q, builds__refs %q", m.TenantKind, m.keyKind(""), m.KeyKinds["builds__refs"])
	}
	if !m.Tables["builds"].OrgValue || m.orgNum(ulidA) != "1" || m.orgNum(ulidB) != "" {
		t.Errorf("OrgValue %v, orgNum(A) %q, orgNum(B) %q", m.Tables["builds"].OrgValue, m.orgNum(ulidA), m.orgNum(ulidB))
	}
	// a number-looking organization is a string, not 7: quoted, never an integer
	if got := m.Tables["builds"].orgLit("123"); got != "'123'" {
		t.Errorf("orgLit(123) = %s", got)
	}
	if got := (*Table)(nil).orgLit("123"); got != "123" {
		t.Errorf("orgLit of an integer organization = %s", got)
	}
	s := &session{db: "ulid-org-test", meta: m, orgCache: map[string]map[string]string{"builds__refs": {ulidB: ulidA}}}
	for _, c := range []struct{ sql, want string }{
		{"SELECT * FROM builds WHERE team_id = '" + ulidA + "'", ulidA},
		{"SELECT * FROM builds WHERE team_id = '" + ulidA + "   '", ulidA}, // character(n) pads: blanks do not count
		{"SELECT * FROM builds WHERE team_id = '" + ulidA + "'::char(26) AND title = 'x'", ulidA},
		{"INSERT INTO builds (id, team_id, title) VALUES ('" + ulidB + "', '" + ulidA + "', 'x')", ulidA},
		{"SELECT * FROM builds WHERE id = '" + ulidB + "'", ulidA},            // the key's refs give the organization
		{"SELECT * FROM builds WHERE id = '" + ulidB + "    '", ulidA},        // a padded key is the same key
		{"SELECT * FROM build_files WHERE build_id = '" + ulidB + "'", ulidA}, // by the FK
		{"SELECT * FROM builds WHERE team_id IS NULL", ""},
	} {
		an := analyze(c.sql, m)
		org, _, unknown, err := s.resolveOrg(an, constOnly, false)
		if org != c.want || err != nil || unknown && c.want != "" {
			t.Errorf("%s: org %q unknown %v err %v; want %q", c.sql, org, unknown, err, c.want)
		}
	}
	// lokadb.tenant: the string as it is (no number or uuid read into it), without blanks at its end
	for in, want := range map[string]string{ulidA: ulidA, ulidA + "  ": ulidA, "007": "007", "12": "12", "*": "*", "": ""} {
		if got, err := tenantValue(in, TenantSetting, kindText); err != nil || got != want {
			t.Errorf("tenantValue(%q, text) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"-1", "-acme"} {
		if _, err := tenantValue(in, TenantSetting, kindText); err == nil {
			t.Errorf("tenantValue(%q, text) took a value that a minus starts", in)
		}
	}
	// binary and text parameters of a character(26) key read the same
	if got, _ := binaryKey(kindChar, []byte(ulidA+"  ")); got != ulidA {
		t.Errorf("binaryKey(char) = %q", got)
	}
	if got, ok := canonicalKey(kindChar, ulidA+"  "); !ok || got != ulidA {
		t.Errorf("canonicalKey(char) = %q, %v", got, ok)
	}
	if got, _ := canonicalKey(kindText, "a "); got != "a " { // varchar: as written
		t.Errorf("canonicalKey(text) = %q", got)
	}
	for typ, want := range map[string]string{"character(26)": kindChar, "character varying(36)": kindText, "text": kindText, "uuid": kindUUID} {
		if got := keyKindOf(typ); got != want {
			t.Errorf("keyKindOf(%s) = %q, want %q", typ, got, want)
		}
	}
}

// The pause lock of a string organization takes its value as text, and the partition key's
// condition is the key as enable partitions by it.
func TestStringOrganizationRouting(t *testing.T) {
	m := ulidOrgMeta()
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, m)
	sendAll(t, fe, &pgproto3.Query{String: "SELECT * FROM builds WHERE team_id = '" + ulidA + "'"})
	expectMsgs(t, fe, c, 1)
	log := f.log()
	if !inOrder(log, "SELECT set_config('lokadb.org', '"+ulidA+"', false), pg_advisory_lock_shared(lokadb.org_lock_v('"+ulidA+"'))") {
		t.Fatalf("upstream ran %q", log)
	}
	got, ok := keyCond("SELECT * FROM builds WHERE team_id = '"+ulidA+"'", m, ulidA, defaultPath)
	if !ok || !strings.Contains(strings.ToLower(got), `coalesce(builds.team_id::text, '') collate "c" = '`+strings.ToLower(ulidA)+`'`) {
		t.Errorf("keyCond: %q, %v", got, ok)
	}
	if got := analyze("SELECT count(*) FROM builds WHERE team_id = '"+ulidA+"'", m).targetSQL("builds__org1", ulidA, true); !strings.Contains(got, `'`+ulidA+`'`) || strings.Contains(got, "= "+ulidA) {
		t.Errorf("targetSQL: %s", got)
	}
}
