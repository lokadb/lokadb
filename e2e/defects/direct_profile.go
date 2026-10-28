package main

// The profile direct (block 10, stage 9b, tasks/direct-profile.md): an organization with no
// way through the FDW. org-profile direct needs its credentials in the control plane (stage
// 9a) and with -drop-fdw-credentials drops the cloud's user mappings of its server; its
// statements go to its database or are refused with their reason; going back to fdw makes the
// user mapping again from the owner's credentials.

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
		check{id: "DP28", name: "profile direct: no user mapping in the cloud, a named transaction direct, an unnamed read and a join with a cloud table refused with their reason, move-back refused, back to fdw only with the key, the user mapping made again", run: checkDirectProfile},
	)
}

func checkDirectProfile(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	stop() // the proxy again below, with the control plane
	cpDB := e.db + "_cp"
	admin := dsn(e.cloudHost, "postgres", "postgres")
	if err := execSQL(e.ctx, admin, "DROP DATABASE IF EXISTS "+cpDB, "CREATE DATABASE "+cpDB); err != nil {
		return false, "", err
	}
	defer execSQL(e.ctx, admin, "DROP DATABASE IF EXISTS "+cpDB+" WITH (FORCE)")
	cp := dsn(e.cloudHost, cpDB, "postgres")
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	keyFile := filepath.Join(os.TempDir(), e.db+"-dp28.key")
	if err := os.WriteFile(keyFile, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return false, "", err
	}
	defer os.Remove(keyFile)
	for kind, d := range map[string]string{"direct": dsn("customer", e.db, "lokadb_dml"), "owner": e.appCustomer()} {
		if out, err := e.lokadb("org-credentials", "-db", e.cloud(), "-control-plane", cp, "-key", keyFile, "-org", "1", "-kind", kind, "-dsn", d); err != nil {
			return false, "org-credentials " + kind + ": " + lastErr(out), nil
		}
	}
	mappings := func() string {
		n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_user_mappings WHERE srvname = (SELECT server FROM lokadb.orgs WHERE org = 1)")
		return n
	}
	before := mappings()
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-org", "1", "-control-plane", cp, "-key", keyFile, "-drop-fdw-credentials", "direct"); err != nil {
		return false, "org-profile direct: " + lastErr(out), nil
	}
	dropped := mappings()
	stop, err = e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"control_plane": cp, "credentials_key": keyFile})
	if err != nil {
		return false, "", err
	}
	defer func() { stop() }()
	c, err := e.viaProxyPgx("dp28")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := before != "0" && dropped == "0"
	notes = append(notes, fmt.Sprintf("user mappings of its server: %s before, %s after -drop-fdw-credentials (want >0, 0)", before, dropped))

	// a named transaction: direct from the first one, nothing to learn on
	n := directCount(e.proxyRoutes(), "1")
	tx, err := c.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	var count int64
	steps := []error{exec1(e, tx, "SET LOCAL lokadb.tenant = 1"), tx.QueryRow(e.ctx, "SELECT count(*) FROM document_versions").Scan(&count),
		exec1(e, tx, "UPDATE document_versions SET content = content WHERE organization_id = $1 AND id = (SELECT min(id) FROM document_versions)", 1)}
	failed := ""
	for _, s := range steps {
		if s != nil && failed == "" {
			failed = firstErrLine(s)
		}
	}
	if failed == "" {
		if err := tx.Commit(e.ctx); err != nil {
			failed = firstErrLine(err)
		}
	} else {
		_ = tx.Rollback(e.ctx)
	}
	direct := directCount(e.proxyRoutes(), "1") - n
	ok = ok && failed == "" && count > 0 && direct == 2
	notes = append(notes, fmt.Sprintf("a named transaction: error %q, %d rows, on the direct path %d (want 2)", failed, count, direct))

	refusal := func(q, want string) string {
		_, err := c.Exec(e.ctx, q, 1)
		if err == nil || pgCode(err) != "0A000" || !strings.Contains(err.Error(), want) {
			ok = false
			return fmt.Sprintf("%q: %v (want 0A000 %q)", q, err, want)
		}
		return fmt.Sprintf("%.60q refused", q)
	}
	notes = append(notes, refusal("SELECT count(*) FROM document_versions WHERE organization_id = $1", "must name the organization"))
	notes = append(notes, refusal("SELECT count(*) FROM document_versions v JOIN users u ON u.id = v.document_id WHERE v.organization_id = $1", "no copy"))

	// move-back first needs the FDW way back
	if out, err := e.lokadb(e.moveBackArgs(1)...); err == nil || !strings.Contains(out, "profile direct") {
		ok = false
		notes = append(notes, "move-back on profile direct: "+lastErr(out))
	}
	// leaving direct: without the key refused, with it the user mapping again
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-org", "1", "fdw"); err == nil || !strings.Contains(out, "-key") {
		ok = false
		notes = append(notes, "back to fdw without the key: "+lastErr(out))
	}
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-org", "1", "-control-plane", cp, "-key", keyFile, "fdw"); err != nil {
		return false, strings.Join(notes, "; ") + "; back to fdw: " + lastErr(out), nil
	}
	again := mappings()
	var cnt int64
	rerr := c.QueryRow(e.ctx, "SELECT count(*) FROM document_versions WHERE organization_id = $1", 1).Scan(&cnt)
	ok = ok && again != "0" && rerr == nil && cnt == count
	notes = append(notes, fmt.Sprintf("back on fdw: user mappings %s (want >0), a read through the cloud %d rows (want %d), error %v", again, cnt, count, rerr))
	return ok, strings.Join(notes, "; "), nil
}
