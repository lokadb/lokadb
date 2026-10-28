package main

// Credentials of organizations in the control plane (block 10, stage 9a,
// tasks/direct-profile.md): lokadb org-credentials seals them with the proxy's key in
// lokadb_cp.org_credentials; a proxy with control_plane and credentials_key and no
// customers_app in its config takes the direct path with them.

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "DP27", name: "an organization's credentials sealed in the control plane (lokadb org-credentials): the proxy without customers_app goes direct with them; the table holds no password; a key rotation resealed (-reseal) leaves the old key unneeded; a table of before A14 gets the cloud's installation, the rows are sealed for it", run: checkDirectCredentials},
	)
}

func checkDirectCredentials(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	cpDB := e.db + "_cp"
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "DROP DATABASE IF EXISTS "+cpDB, "CREATE DATABASE "+cpDB); err != nil {
		return false, "", err
	}
	defer execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "DROP DATABASE IF EXISTS "+cpDB+" WITH (FORCE)")
	cp := dsn(e.cloudHost, cpDB, "postgres")
	// the table as a control plane of before A14 has it (no installation): org-credentials
	// makes it the new one (review of admin, A14)
	if err := execSQL(e.ctx, cp, "CREATE SCHEMA lokadb_cp", `CREATE TABLE lokadb_cp.org_credentials (db text NOT NULL, org bigint NOT NULL,
		kind text NOT NULL CHECK (kind IN ('direct', 'owner')), key_id text NOT NULL, ciphertext bytea NOT NULL,
		updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (db, org, kind))`); err != nil {
		return false, "", err
	}
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	keyFile := filepath.Join(os.TempDir(), e.db+"-credentials.key")
	if err := os.WriteFile(keyFile, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return false, "", err
	}
	defer os.Remove(keyFile)
	direct := dsn("customer", e.db, "lokadb_dml")
	for kind, d := range map[string]string{"direct": direct, "owner": e.appCustomer()} {
		if out, err := e.lokadb("org-credentials", "-db", e.cloud(), "-control-plane", cp, "-key", keyFile, "-org", "1", "-kind", kind, "-dsn", d); err != nil {
			return false, "org-credentials " + kind + ": " + lastErr(out), nil
		}
	}
	// a key file others may read: refused (the coordinator's condition for 9a); the key itself
	// is in no output
	_ = os.Chmod(keyFile, 0o644)
	out, err := e.lokadb("org-credentials", "-db", e.cloud(), "-control-plane", cp, "-key", keyFile, "-org", "1", "-kind", "direct", "-dsn", direct)
	_ = os.Chmod(keyFile, 0o600)
	if err == nil || !strings.Contains(out, "0600") || strings.Contains(out, hex.EncodeToString(raw)) {
		return false, "a key file readable by others: " + lastErr(out), nil
	}
	// a direct path role that may do more than DML is refused
	if _, err := e.lokadb("org-credentials", "-db", e.cloud(), "-control-plane", cp, "-key", keyFile, "-org", "1", "-kind", "direct", "-dsn", dsn("customer", e.db, "postgres")); err == nil {
		return false, "a superuser stored as the direct path's role", nil
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"control_plane": cp, "credentials_key": keyFile})
	if err != nil {
		return false, "", err
	}
	defer func() { stop() }()
	c, err := e.viaProxyPgx("dp27")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n := directCount(e.proxyRoutes(), "1")
	var count int64
	if err := c.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = $1", 1).Scan(&count); err != nil {
		return false, "a read of organization 1: " + firstErrLine(err), nil
	}
	got := directCount(e.proxyRoutes(), "1") - n
	plain, err := one(e.ctx, cp, "SELECT count(*) FROM lokadb_cp.org_credentials WHERE position('lokadb_dml' in encode(ciphertext, 'escape')) > 0 OR position('lokadb_app' in encode(ciphertext, 'escape')) > 0")
	if err != nil {
		return false, "", err
	}
	rows, err := one(e.ctx, cp, "SELECT string_agg(kind, ',' ORDER BY kind) FROM lokadb_cp.org_credentials WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	list, _ := e.lokadb("org-credentials", "-control-plane", cp, "-list")
	// sealed for the cloud's installation (review of admin, A14): its system_identifier
	sysid, err := one(e.ctx, e.cloud(), "SELECT system_identifier::text FROM pg_control_system()")
	if err != nil {
		return false, "", err
	}
	ofCloud, err := one(e.ctx, cp, "SELECT count(*) FROM lokadb_cp.org_credentials WHERE org = 1 AND installation = '"+sysid+"'")
	if err != nil {
		return false, "", err
	}
	ok := count > 0 && got == 1 && plain == "0" && rows == "direct,owner" && !strings.Contains(list, "lokadb_dml") && ofCloud == "2"
	note := fmt.Sprintf("a read through the proxy with no customers_app: %d rows, on the direct path %d (want 1); rows stored %q (want direct,owner); rows with a role's name in plain text %s (want 0); -list shows no secret %v; rows of the cloud's installation %s (want 2)",
		count, got, rows, plain, !strings.Contains(list, "lokadb_dml"), ofCloud)

	// key rotation (review of admin, A8): a new key, the rows resealed with it, the old key gone
	c.Close(e.ctx)
	stop()
	_, _ = rand.Read(raw)
	newKey := filepath.Join(os.TempDir(), e.db+"-credentials-new.key")
	if err := os.WriteFile(newKey, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return false, "", err
	}
	defer os.Remove(newKey)
	out, err = e.lokadb("org-credentials", "-reseal", "-db", e.cloud(), "-control-plane", cp, "-key", newKey, "-key-old", keyFile)
	if err != nil {
		return false, note + "; -reseal: " + lastErr(out), nil
	}
	ids, err := one(e.ctx, cp, "SELECT count(DISTINCT key_id) FROM lokadb_cp.org_credentials WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	if stop, err = e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"control_plane": cp, "credentials_key": newKey}); err != nil {
		return false, "", err
	}
	c2, err := e.viaProxyPgx("dp27b")
	if err != nil {
		return false, "", err
	}
	defer c2.Close(e.ctx)
	n = directCount(e.proxyRoutes(), "1")
	var count2 int64
	rerr := c2.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = $1", 1).Scan(&count2)
	got2 := directCount(e.proxyRoutes(), "1") - n
	ok = ok && strings.Contains(out, "2 credentials resealed") && ids == "1" && rerr == nil && count2 == count && got2 == 1
	return ok, note + fmt.Sprintf("; rotation: %q, key ids of the rows %s (want 1), the proxy with the new key alone: %d rows on the direct path %d (want %d, 1), error %v",
		strings.TrimSpace(out), ids, count2, got2, count, rerr), nil
}
