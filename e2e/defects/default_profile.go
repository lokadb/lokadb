package main

// The profile a moved organization gets (decision of 26.10, after block 14's measurements on
// a real network: fdw about 5 exchanges with the organization's database a statement, fdw+direct
// about 1.5): move-org sets fdw+direct when the proxy's config it is given (-proxy-config) has
// the direct path's credentials of the organization (customers_app), and keeps fdw with a
// warning that says why and what it costs otherwise; check reports the organizations on fdw.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "DP30", name: "move-org sets fdw+direct when the proxy's config has the organization's direct path credentials, keeps fdw with a warning (why, what it costs) without them; check reports the organizations on fdw", run: checkDefaultProfile},
	)
}

func checkDefaultProfile(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// organization 1: no -proxy-config, so it stays on fdw and says why
	out1, err := e.move(1, "-customer-app", e.appCustomer())
	if err != nil {
		return false, "move-org 1: " + lastErr(out1), nil
	}
	profile := func(org int) string {
		p, _ := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT coalesce(profile, 'fdw') FROM lokadb.orgs WHERE org = %d", org))
		return p
	}
	warned := strings.Contains(out1, "stays on profile fdw") && strings.Contains(out1, "5 exchanges") && strings.Contains(out1, "-proxy-config")
	// organization 2, to a database of its own: the proxy's config gives its direct path's role
	db2 := e.db + "_c2"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+db2+" WITH (FORCE)", "CREATE DATABASE "+db2+" OWNER app"); err != nil {
		return false, "", err
	}
	defer execSQL(e.ctx, dsn("customer", "postgres", "postgres"), "DROP DATABASE IF EXISTS "+db2+" WITH (FORCE)")
	cust2, app2 := dsn("customer", db2, "postgres"), dsn("customer", db2, "lokadb_app")
	if err := execSQL(e.ctx, cust2, "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return false, "", err
	}
	cfg, _ := json.Marshal(map[string]any{"customers_app": map[string]any{e.db: map[string]string{"2": dsn("customer", db2, "lokadb_dml")}}})
	cfgPath := filepath.Join(os.TempDir(), e.db+"-dp30-proxy.json")
	if err := os.WriteFile(cfgPath, cfg, 0o644); err != nil {
		return false, "", err
	}
	defer os.Remove(cfgPath)
	out2, err := e.move(2, "-customer", cust2, "-customer-app", app2, "-proxy-config", cfgPath, "-direct-role", "lokadb_dml")
	if err != nil {
		return false, "move-org 2: " + lastErr(out2), nil
	}
	// the role of the direct path got DML on the moved tables (org-profile's grant)
	grant, _ := one(e.ctx, cust2, "SELECT has_table_privilege('lokadb_dml', 'documents', 'INSERT')::text")
	// check, after enable: organization 1 on fdw is reported, 2 is not
	chk, _ := e.checkWith(spec)
	onFDW := reported(chk, "org_profile_fdw") && strings.Contains(finding(chk, "org_profile_fdw"), "1 organization moved")
	ok := profile(1) == "fdw" && warned && profile(2) == "fdw+direct" && grant == "true" && onFDW
	return ok, fmt.Sprintf("without -proxy-config: profile %s, warning %v; with its customers_app: profile %s, DML of the direct role %s; check reports one organization on fdw %v (%s)",
		profile(1), warned, profile(2), grant, onFDW, finding(chk, "org_profile_fdw")), nil
}
