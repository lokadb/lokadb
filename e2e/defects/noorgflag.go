package main

// Block 5, CLI (check review of 25.10, finding 4): a command that acts on one organization
// has no default organization.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV19", name: "move-org, move-back, repoint, schema-sync and refs-sync without -org (or -all) refuse, and nothing is moved", run: checkNoOrgFlag},
	)
}

// MV19. move-org without -org moved organization 0: an empty "moved" organization in
// lokadb.orgs, or, with rows of tenant 0 (a system or demo tenant), those rows in the customer
// database the other flags named. Each command refuses and says what to give; the sync
// commands also take every moved organization only with -all.
func checkNoOrgFlag(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// rows of tenant 0, the system or demo tenant
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) SELECT 0, 2, 'tenant0' FROM generate_series(1, 5)"); err != nil {
		return false, "", err
	}
	p, err := e.specFile(spec)
	if err != nil {
		return false, "", err
	}
	cases := []struct {
		name string
		args []string
	}{
		{"move-org", []string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-customer", e.customer(), "-publisher", e.cloud()}},
		{"move-back", []string{"move-back", "-db", e.cloud(), "-spec", p, "-customer", e.customer()}},
		{"repoint", []string{"repoint", "-db", e.cloud(), "-customer-app", e.appCustomer()}},
		{"schema-sync", []string{"schema-sync", "-db", e.cloud()}},
		{"refs-sync", []string{"refs-sync", "-db", e.cloud()}},
		{"org-profile", []string{"org-profile", "-db", e.cloud(), "fdw"}},
	}
	var notes []string
	ok := true
	for _, c := range cases {
		out, err := e.lokadb(c.args...)
		// org-profile asks for -org in its usage line; the others say what to give
		said := strings.Contains(out, "give -org") || c.name == "org-profile" && strings.Contains(out, "-org")
		if err == nil || !said {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: %s", c.name, map[bool]string{true: "refused", false: "RAN: " + oneLine(out)}[err != nil && said]))
	}
	moved, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs")
	if err != nil {
		return false, "", err
	}
	atCustomer, _ := one(e.ctx, e.customer(), "SELECT coalesce(to_regclass('documents')::text, 'no table')")
	if moved != "0" || atCustomer != "no table" {
		ok = false
	}
	return ok, fmt.Sprintf("%s; lokadb.orgs rows: %s; documents at the customer database: %s", strings.Join(notes, "; "), moved, atCustomer), nil
}
