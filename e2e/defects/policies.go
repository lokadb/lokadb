package main

// Row security of LokaDB tables through the proxy (review 2 of routing, block 7): the policies
// that keep the organizations apart are LokaDB's; the application's own policies of the table
// are its own, and lokadb_tenant narrows what they allow.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "RLS1", name: "through the proxy the application cannot drop or change lokadb_tenant or turn row security off; its own permissive policy does not open other organizations' rows", run: checkPolicyDDL},
	)
}

// RLS1. The role of the application owns the LokaDB tables (enable -owner): through the proxy
// it dropped lokadb_tenant, and a policy of its own USING (true), permissive like lokadb_tenant
// when the table had no other policy, let every organization's rows through.
func checkPolicyDDL(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if out, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	stop, err := e.proxyWith(map[string]string{"1": e.appCustomer()})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(q string) error { _, err := simple(e.ctx, c, q); return err }
	var notes []string
	ok := true
	// LokaDB's own: refused, and lokadb_tenant still there as it was
	for _, q := range []string{
		"DROP POLICY lokadb_tenant ON documents",
		"ALTER POLICY lokadb_tenant ON documents USING (true)",
		"ALTER POLICY lokadb_tenant ON documents RENAME TO gone",
		"ALTER TABLE documents NO FORCE ROW LEVEL SECURITY",
		"ALTER TABLE documents DISABLE ROW LEVEL SECURITY",
	} {
		if err := run(q); err == nil {
			ok = false
			notes = append(notes, q+": passed")
		}
	}
	state := func() string {
		v, _ := one(e.ctx, e.cloud(), `SELECT coalesce(string_agg(polname || ':' || CASE WHEN polpermissive THEN 'permissive' ELSE 'restrictive' END, ',' ORDER BY polname), '-')
			|| ' force=' || (SELECT relforcerowsecurity FROM pg_class WHERE oid = 'documents'::regclass)
			FROM pg_policy WHERE polrelid = 'documents'::regclass`)
		return v
	}
	count := func() string { // what the application reads with organization 2 named
		v, err := e.asRole("app", "2", "SELECT count(*) FROM documents")
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	before, beforeN := state(), count()
	// the application's own policy: allowed, and lokadb_tenant narrows it (restrictive)
	createErr := run("CREATE POLICY open_all ON documents USING (true)")
	withOwn, withOwnN := state(), count()
	dropErr := run("DROP POLICY open_all ON documents")
	after, afterN := state(), count()
	notes = append(notes, fmt.Sprintf("policies %s, organization 2 reads %s; after CREATE POLICY open_all (%s): %s, reads %s; after DROP POLICY open_all (%s): %s, reads %s",
		before, beforeN, errText(createErr), withOwn, withOwnN, errText(dropErr), after, afterN))
	ok = ok && before == "lokadb_tenant:permissive force=true" && beforeN == "100" &&
		createErr == nil && withOwn == "lokadb_tenant:restrictive,open_all:permissive force=true" && withOwnN == "100" &&
		dropErr == nil && after == before && afterN == "100"
	return ok, strings.Join(notes, "; "), nil
}
