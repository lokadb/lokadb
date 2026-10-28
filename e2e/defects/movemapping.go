package main

// Block 5 (moving an organization): no user mapping FOR PUBLIC, also when the tables are
// granted to PUBLIC (block 2's lokadb.sync_mappings maps each role instead).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "MV16", name: "a table granted to PUBLIC: move-org makes no user mapping FOR PUBLIC, a role not mapped sees no credentials, a role reading through PUBLIC reads the organization", run: checkNoPublicMapping},
	)
}

// MV16. documents and document_versions are granted SELECT TO PUBLIC before the move.
// Before: mappedRoles turned the PUBLIC grantee into CREATE USER MAPPING FOR PUBLIC with the
// organization role's password: any role with USAGE on the server reads the organization's
// database as that role (Postgres shows the options themselves only to members of the
// server's owner). A role created after the move and given USAGE on the server (as a DBA
// might) has no mapping of its own: it must see no password and get nothing through the
// server (a foreign table of its own over the organization's database). A role created
// before the move that reads only through the PUBLIC grant must read organization 1's rows
// through the proxy.
func checkNoPublicMapping(e *env) (bool, string, error) {
	reader := "mv16r_" + e.db
	if err := role(e, "cloud", reader); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents, document_versions TO PUBLIC"); err != nil {
		return false, "", err
	}
	stop, err := e.moved()
	if err != nil {
		return false, "move-org failed: " + firstErrLine(err), nil
	}
	defer stop()
	public, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_user_mappings WHERE srvname = 'lokadb_org1' AND umuser = 0")
	if err != nil {
		return false, "", err
	}
	late := "mv16l_" + e.db
	if err := role(e, "cloud", late); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT USAGE ON FOREIGN SERVER lokadb_org1 TO "+late); err != nil {
		return false, "", err
	}
	defer func() { _ = execSQL(e.ctx, e.cloud(), "REVOKE USAGE ON FOREIGN SERVER lokadb_org1 FROM "+late) }()
	seen, err := one(e.ctx, dsn(e.cloudHost, e.db, late),
		"SELECT count(*) FROM pg_user_mappings m, unnest(m.umoptions) o WHERE m.srvname = 'lokadb_org1' AND o LIKE 'password=%'")
	if err != nil {
		return false, "", err
	}
	// the credentials used rather than read (Postgres shows a PUBLIC mapping's options only to
	// members of the server's owner): a foreign table of its own over the organization's
	// database runs as the organization's role through a mapping FOR PUBLIC
	used := "refused"
	if err := execSQL(e.ctx, e.cloud(), "GRANT CREATE ON SCHEMA public TO "+late); err != nil {
		return false, "", err
	}
	defer func() {
		_ = execSQL(e.ctx, e.cloud(), "DROP FOREIGN TABLE IF EXISTS mv16_leak", "REVOKE CREATE ON SCHEMA public FROM "+late)
	}()
	if err := execSQL(e.ctx, dsn(e.cloudHost, e.db, late),
		"CREATE FOREIGN TABLE mv16_leak (rolname text) SERVER lokadb_org1 OPTIONS (schema_name 'pg_catalog', table_name 'pg_roles')"); err != nil {
		used = "cannot create: " + short(err)
	} else if n, err := one(e.ctx, dsn(e.cloudHost, e.db, late), "SELECT count(*) FROM mv16_leak"); err != nil {
		used = "refused: " + short(err)
	} else {
		used = "read " + n + " roles of the organization's database"
	}
	// the proxy reads lokadb's metadata as the session's role
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON ALL TABLES IN SCHEMA lokadb TO "+reader); err != nil {
		return false, "", err
	}
	read := "?"
	c, err := e.viaProxyAs(reader)
	if err != nil {
		read = "connect: " + short(err)
	} else {
		defer c.Close(e.ctx)
		res, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1")
		switch {
		case err != nil:
			read = "error: " + short(err)
		case len(res) == 1 && len(res[0].Rows) == 1:
			read = string(res[0].Rows[0][0])
		}
	}
	ok := public == "0" && seen == "0" && !strings.HasPrefix(used, "read ") && read == "100"
	return ok, fmt.Sprintf("mappings FOR PUBLIC on lokadb_org1: %s; a role created after the move with USAGE on the server: passwords it sees %s, its own foreign table on the server %s; a role reading through the PUBLIC grant reads organization 1 through the proxy: %s",
		public, seen, used, read), nil
}
