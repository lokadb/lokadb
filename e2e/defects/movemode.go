package main

// Block 5 (moving an organization), K-1 of the tunnel review (25.10): -mode auto never picks
// logical replication. Its subscription connects as a role with the REPLICATION attribute, which
// reads the change stream of the whole cloud database.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV11", name: "-mode auto moves by triggers whatever the size, and makes no role that can read the change stream of the cloud database", run: checkAutoIsTriggers},
		check{id: "MV23", name: "-mode logical is refused without -allow-customer-replication; with it the move warns loudly, and the role it makes is gone after the move", run: checkLogicalNeedsConsent},
	)
}

// moveModeOf: the mode move-org printed ("move mode" line), "?" if none.
func moveModeOf(out string) string {
	for _, l := range strings.Split(out, "\n") {
		if i := strings.Index(l, `msg="move mode" `); i >= 0 {
			for _, f := range strings.Fields(l[i+len(`msg="move mode" `):]) {
				if strings.HasPrefix(f, "mode=") {
					return strings.TrimPrefix(f, "mode=")
				}
			}
		}
	}
	return "?"
}

// replRoles: the roles with the REPLICATION attribute that a move makes (lokadb_repl_...).
func replRoles(e *env) (string, error) {
	return one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_roles WHERE rolreplication AND rolname LIKE 'lokadb\\_repl\\_%'")
}

// MV11. Organization 3 (20 000 rows, analyzed) with -mode auto, on a cloud that has
// wal_level = logical and a customer database that can subscribe: before, it went by
// logical replication (a role with REPLICATION for the customer database to connect with);
// now by triggers. While the move runs, no such role exists.
func checkAutoIsTriggers(e *env) (bool, string, error) {
	if moveMode != "" && moveMode != "auto" {
		return true, "DEFECTS_MODE=" + moveMode + ": every move has its mode given", nil
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 3, 2, 'x' || g FROM generate_series(1, 20000) g",
		"ANALYZE documents__cloud", "ANALYZE document_versions__cloud"); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	args := []string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "3",
		"-customer", e.customer(), "-publisher", e.cloud()}
	out, err := e.lokadb(args...)
	if err != nil {
		return false, "auto move of organization 3 failed: " + firstErrLine(err), nil
	}
	roles, err := replRoles(e)
	if err != nil {
		return false, "", err
	}
	mode := moveModeOf(out)
	ok := mode == "trigger" && !strings.Contains(out, "LOGICAL REPLICATION") && roles == "0"
	return ok, fmt.Sprintf("-mode auto, 20 000 rows: %s; the loud warning printed: %v; roles with REPLICATION left: %s", mode, strings.Contains(out, "LOGICAL REPLICATION"), roles), nil
}

// MV23. -mode logical alone is refused with the reason, before anything changes. With the flag
// it moves, prints the warning, and drops its role with the subscription afterwards.
func checkLogicalNeedsConsent(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	args := []string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1",
		"-customer", e.customer(), "-publisher", e.cloud(), "-mode", "logical", "-allow-customer-replication=false"}
	out, err := e.lokadb(args...)
	moved, merr := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	if merr != nil {
		return false, "", merr
	}
	roles, rerr := replRoles(e)
	if rerr != nil {
		return false, "", rerr
	}
	refused := err != nil && strings.Contains(out, "REPLICATION attribute") && strings.Contains(out, "-allow-customer-replication")
	if !refused || moved != "0" || roles != "0" {
		return false, fmt.Sprintf("without the flag: %s / %s; organization 1 moved: %s; roles with REPLICATION: %s", errOr(err, true), lastLine(out), moved, roles), nil
	}
	out, err = e.lokadb(append(args[:len(args)-1:len(args)-1], "-allow-customer-replication")...)
	if err != nil {
		return false, "with the flag the move failed: " + lastErr(out), nil
	}
	roles, err = replRoles(e)
	if err != nil {
		return false, "", err
	}
	warned := strings.Contains(out, "LOGICAL REPLICATION") && strings.Contains(out, "WHOLE cloud database")
	mode := moveModeOf(out)
	moved, _ = one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	ok := warned && mode == "logical" && moved == "1" && roles == "0"
	return ok, fmt.Sprintf("without the flag: refused; with it: mode %s, the loud warning: %v, organization 1 moved: %s, roles with REPLICATION left: %s", mode, warned, moved, roles), nil
}
