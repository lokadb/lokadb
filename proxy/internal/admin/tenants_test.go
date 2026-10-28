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
	steps := strings.Join(swapSteps(mt, "app"), "\n")
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
