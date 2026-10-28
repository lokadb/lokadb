package main

// Block 5, the second admin review (tasks/review3/admin.md): R3-5, R3-3.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV25", name: "the organization's FDW server carries the TLS settings of its DSN (sslmode, sslsni)", run: checkFDWSSL},
	)
}

// MV25 (R3-5). postgres_fdw connects with the server's options only: a -customer-app with
// sslmode=verify-full gave the server nothing of it, and the cloud connected with libpq's
// default (prefer: no certificate check, plain text when the database offers no TLS). sslmode
// prefer here: the stand's databases have no TLS, and the option is what is looked at.
func checkFDWSSL(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	app := e.appCustomer() + " sslmode=prefer sslsni=0"
	if out, err := e.move(1, "-customer-app", app); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	opts, err := one(e.ctx, e.cloud(), "SELECT array_to_string(srvoptions, ' ') FROM pg_foreign_server WHERE srvname = 'lokadb_org1'")
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	ok := strings.Contains(opts, "sslmode=prefer") && strings.Contains(opts, "sslsni=0") && n == "100"
	return ok, fmt.Sprintf("the server's options: %s; organization 1 read through it: %s rows", opts, n), nil
}
