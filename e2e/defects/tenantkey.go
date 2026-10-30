package main

// A table keyed by its organization column: one row per organization (its settings, its
// profile, the team's mail; Documenso's TeamEmail and TeamEmailVerification, Laravel's
// team_settings with team_id as the primary key). enable named the column twice in
// lokadb.lookups, as the organization's column and as the key looked up in the refs table,
// and failed: duplicate key value violates unique constraint "lookups_pkey". check said ready.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "TK1", name: "a table whose primary key is its organization column (one row per organization): enable, move-org, the proxy reads and writes it, move-back, disable", run: checkTenantIsKey})
}

func checkTenantIsKey(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE org_settings (organization_id bigint PRIMARY KEY, theme text NOT NULL, locale text)",
		"ALTER TABLE org_settings OWNER TO app",
		"INSERT INTO org_settings VALUES (1, 'dark', 'ru'), (2, 'dark', 'en'), (3, 'light', 'de')"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"},
  {"name": "org_settings", "tenant_column": "organization_id"}
]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	lookups := query(e, e.cloud(), "SELECT string_agg(column_name || ' ' || coalesce(refs_table, '-'), ', ') FROM lokadb.lookups WHERE table_name = 'org_settings'")
	if out, err := e.move(1); err != nil {
		return false, fmt.Sprintf("enabled (lookups %s), move-org failed: %s", lookups, lastErr(out)), nil
	}
	moved := query(e, e.customer(), "SELECT string_agg(organization_id || ' ' || theme, ', ') FROM org_settings")
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		stop()
		return false, "", err
	}
	via := []string{
		proxyValue(e, c, "SELECT theme FROM org_settings WHERE organization_id = 1"),
		proxyValue(e, c, "SELECT theme FROM org_settings WHERE organization_id = 2"),
		proxyValue(e, c, "UPDATE org_settings SET theme = 'light' WHERE organization_id = 1 RETURNING theme"),
		proxyValue(e, c, "INSERT INTO org_settings (organization_id, theme, locale) VALUES (4, 'blue', 'fr') RETURNING theme"),
		proxyValue(e, c, "INSERT INTO org_settings (organization_id, theme, locale) VALUES (1, 'twice', 'ru') RETURNING theme"),
	}
	c.Close(e.ctx)
	stop()
	there := query(e, e.customer(), "SELECT string_agg(organization_id || ' ' || theme, ', ') FROM org_settings")
	if out, err := e.lokadb(e.moveBackArgs(1)...); err != nil {
		return false, fmt.Sprintf("moved (%s), move-back failed: %s", moved, lastErr(out)), nil
	}
	back := query(e, e.cloud(), "SELECT string_agg(organization_id || ' ' || theme, ', ' ORDER BY organization_id) FROM org_settings")
	if out, err := e.lokadb(e.disableArgs()...); err != nil {
		return false, fmt.Sprintf("moved and back (%s), disable failed: %s", back, lastErr(out)), nil
	}
	after := query(e, e.cloud(), `SELECT (SELECT string_agg(organization_id || ' ' || theme, ', ' ORDER BY organization_id) FROM org_settings) || '; '
		|| (SELECT relkind::text FROM pg_class WHERE oid = 'org_settings'::regclass) || ' ' || (SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'org_settings'::regclass AND contype = 'p')`)
	dup := strings.HasPrefix(via[4], "error:") && strings.Contains(via[4], "23505")
	want := []string{"dark", "dark", "light", "blue"}
	ok := lookups == "organization_id -" && moved == "1 dark" && strings.Join(via[:4], "|") == strings.Join(want, "|") && dup &&
		there == "1 light" && back == "1 light, 2 dark, 3 light, 4 blue" && after == "1 light, 2 dark, 3 light, 4 blue; r PRIMARY KEY (organization_id)"
	return ok, fmt.Sprintf("lookups of org_settings: %s (want organization_id -); moved: %s; through the proxy: organization 1 (moved) %s, 2 %s, update %s, a new organization's row %s, organization 1's second row %s (want 23505); in its database: %s; after move-back: %s; after disable: %s",
		lookups, moved, via[0], via[1], via[2], via[3], via[4], there, back, after), nil
}
