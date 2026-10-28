package admin

import (
	"context"
	"strings"
	"testing"
)

// The commands that act on one organization refuse an organization that is no number: 0 is
// what a forgotten -org reads as (move-org of organization 0 moved the rows whose tenant is
// 0 into the customer database of the other flags), -1 and -2 are reserved. Each is refused
// before anything connects anywhere.
func TestOrgCommandsRefuseNoOrganization(t *testing.T) {
	ctx := context.Background()
	for _, org := range []int64{0, -1, -2} {
		t.Run("", func(t *testing.T) {
			if err := ValidOrg(org); err == nil {
				t.Errorf("ValidOrg(%d): no error", org)
			}
			if _, err := RequireOrg(ctx, "", org, ""); err == nil { // -org given, no -tenant: no connection needed
				t.Errorf("RequireOrg(%d): no error", org)
			}
			if n, err := LookupOrg(ctx, "", org, ""); err != nil || n != org { // check: no organization is fine
				t.Errorf("LookupOrg(%d) = %d, %v", org, n, err)
			}
			for name, err := range map[string]error{
				"move-org":  MoveOrg(ctx, MoveOptions{Org: org}),
				"move-back": MoveBack(ctx, MoveBackOptions{Org: org}),
				"repoint":   Repoint(ctx, RepointOptions{Org: org}),
				"org-add":   orgAddErr(ctx, org),
			} {
				if name == "org-add" && org == 0 {
					continue // 0: the next free number
				}
				if err == nil || !strings.Contains(err.Error(), "organization") {
					t.Errorf("%s with organization %d: %v", name, org, err)
				}
			}
			if _, err := SetOrgProfile(ctx, OrgProfileOptions{Org: org, Profile: "fdw"}); err == nil || !strings.Contains(err.Error(), "org-profile: ") {
				t.Errorf("org-profile with organization %d: %v", org, err)
			}
		})
	}
	if err := ValidOrg(1); err != nil {
		t.Errorf("ValidOrg(1): %v", err)
	}
}

func orgAddErr(ctx context.Context, org int64) error {
	_, err := OrgAdd(ctx, OrgAddOptions{Org: org, Tenant: "x"})
	return err
}
