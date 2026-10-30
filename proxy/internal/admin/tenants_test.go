package admin

import (
	"strings"
	"testing"
)

// Block 18: what a move puts into SQL as the organization's value. An organization numbered by
// integers keeps N everywhere, as before; one numbered by uuid gets its value where the rows
// are named (CHECKs, bounds, the refs) and its number where things are (names, lokadb.orgs).
func TestTenantSQL(t *testing.T) {
	const v = "aaaaaaaa-0000-4000-8000-000000000001"
	n := MoveOptions{Org: 7}.org() // not resolved: N = V
	if n.Lit() != "7" || n.Key("org_id") != partKey("org_id") || n.LockCall() != "lokadb.org_lock(7)" {
		t.Errorf("integer: %q, %q, %q", n.Lit(), n.Key("org_id"), n.LockCall())
	}
	u := MoveOptions{Org: 3, tenant: Tenant{Org: 3, Kind: TenantUUID, Value: v}}.org()
	if u.Lit() != "'"+v+"'::uuid" || u.Key("account_id") != `(coalesce("account_id", '`+NilUUID+`'::uuid))` || u.LockCall() != "lokadb.org_lock_v('"+v+"')" {
		t.Errorf("uuid: %q, %q, %q", u.Lit(), u.Key("account_id"), u.LockCall())
	}
	mt := moveTable{name: "projects", tenant: "account_id", org: 3, val: u.Lit()}
	steps := strings.Join(swapSteps(mt, "app", true), "\n")
	for _, want := range []string{
		`ATTACH PARTITION "projects__o3" FOR VALUES IN ('` + v + `'::uuid)`,
		`WHERE "account_id" = '` + v + `'::uuid`,
		`DROP POLICY IF EXISTS "lokadb_bind_org3"`,
	} {
		if !strings.Contains(steps, want) {
			t.Errorf("move-back's swap lacks %q:\n%s", want, steps)
		}
	}
}

// Review A9: abandon's delete from the maps of unique columns names the organization by its
// value, inside the string the DO block formats (quotes doubled), not by its number.
func TestAbandonMaps(t *testing.T) {
	const v = "aaaaaaaa-0000-4000-8000-000000000001"
	u := Tenant{Org: 3, Kind: TenantUUID, Value: v}
	if got := abandonMaps([]string{"'projects'"}, u); !strings.Contains(got, "WHERE tenant = ''"+v+"''::uuid'") || strings.Contains(got, "tenant = 3") {
		t.Errorf("uuid organization: %s", got)
	}
	if got := abandonMaps([]string{"'docs'", "'notes'"}, Tenant{Org: 7, Kind: TenantInt, Value: "7"}); !strings.Contains(got, "WHERE tenant = 7'") || !strings.Contains(got, "IN ('docs', 'notes')") {
		t.Errorf("integer organization: %s", got)
	}
}

// Organizations named by a string (text, varchar, character(n)), keys of type character(n): a
// Laravel ulid() is character(26).
func TestStringTenant(t *testing.T) {
	const v = "01JAAAAAAAAAAAAAAAAAAAAAAA"
	for base, want := range map[string]string{"character": TenantText, "character varying": TenantText, "text": TenantText,
		"uuid": TenantUUID, "bigint": TenantInt, "numeric": "", "money": ""} {
		if got := TenantKind(base); got != want {
			t.Errorf("TenantKind(%s) = %q, want %q", base, got, want)
		}
	}
	for base, want := range map[string]string{"character": KindChar, "character varying": KindText, "text": KindText, "uuid": KindUUID, "integer": KindInt, "bytea": ""} {
		if got := KeyKind(base); got != want {
			t.Errorf("KeyKind(%s) = %q, want %q", base, got, want)
		}
	}
	for k, want := range map[string]string{KindChar: TenantText, KindText: TenantText, KindUUID: TenantUUID, KindInt: TenantInt, "": ""} {
		if got := TenantOfKey(k); got != want {
			t.Errorf("TenantOfKey(%q) = %q, want %q", k, got, want)
		}
	}
	str := MoveOptions{Org: 4, tenant: Tenant{Org: 4, Kind: TenantText, Value: v}}.org()
	if str.Lit() != "'"+v+"'" || str.Key("team_id") != `(coalesce("team_id"::text, '') COLLATE "C")` || str.LockCall() != "lokadb.org_lock_v('"+v+"')" {
		t.Errorf("string organization: %q, %q, %q", str.Lit(), str.Key("team_id"), str.LockCall())
	}
	if tenantType(TenantText) != "text" || tenantDefault(TenantText) != "lokadb.org_default_text()" || !ValueKind(TenantText) || ValueKind(TenantInt) {
		t.Errorf("tenantType %q, tenantDefault %q", tenantType(TenantText), tenantDefault(TenantText))
	}
	mt := moveTable{name: "builds", tenant: "team_id", org: 4, val: str.Lit()}
	steps := strings.Join(swapSteps(mt, "app", true), " ")
	for _, want := range []string{`ATTACH PARTITION "builds__o4" FOR VALUES IN ('` + v + `')`, `WHERE "team_id" = '` + v + `'`} {
		if !strings.Contains(steps, want) {
			t.Errorf("move-back's swap lacks %q: %s", want, steps)
		}
	}
	if got := abandonMaps([]string{"'builds'"}, str); !strings.Contains(got, "WHERE tenant = ''"+v+"''") {
		t.Errorf("abandonMaps: %s", got)
	}
	for in, want := range map[string]string{v: v, v + "   ": v, "007": "007", "acme": "acme"} {
		if got, err := CanonTenant(in); err != nil || got != want {
			t.Errorf("CanonTenant(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "   ", "-1", "-acme", " lead"} {
		if _, err := CanonTenant(in); err == nil {
			t.Errorf("CanonTenant(%q) took it", in)
		}
	}
}
