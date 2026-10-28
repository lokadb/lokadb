package main

// repoint and the organization's other addresses (review A1): the proxy reaches an
// organization's database by its direct path role and its owner too (control plane, or
// customers_app and customers of its config), not only by the FDW server.

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "RB8", name: "repoint of an organization on fdw+direct with its credentials in the control plane: a live session's direct path reads and writes the new database", run: checkRepointDirect},
		check{id: "RB9", name: "repoint is refused while the proxy's config sends the direct path to the old database, and changes nothing", run: checkRepointConfigOld},
	)
}

// repoint runs lokadb repoint with the proxy config cfg. A lokadb from before review A1 knows no
// -proxy-config: it runs without it, so that a check shows what that version does.
func (e *env) repoint(cfg map[string]any, args ...string) (string, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return "", err
	}
	p, err := e.specFile(string(b))
	if err != nil {
		return "", err
	}
	out, err := e.lokadb(append([]string{"repoint", "-db", e.cloud(), "-proxy-config", p}, args...)...)
	if err != nil && strings.Contains(out, "flag provided but not defined: -proxy-config") {
		return e.lokadb(append([]string{"repoint", "-db", e.cloud()}, args...)...)
	}
	return out, err
}

// controlPlane makes a control plane database and a key file; done drops them.
func (e *env) controlPlane() (cp, keyFile string, done func(), err error) {
	cpDB := e.db + "_cp"
	admin := dsn(e.cloudHost, "postgres", "postgres")
	if err := execSQL(e.ctx, admin, "DROP DATABASE IF EXISTS "+cpDB+" WITH (FORCE)", "CREATE DATABASE "+cpDB); err != nil {
		return "", "", nil, err
	}
	raw := make([]byte, 32)
	_, _ = rand.Read(raw)
	keyFile = filepath.Join(os.TempDir(), e.db+"-credentials.key")
	if err := os.WriteFile(keyFile, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return "", "", nil, err
	}
	return dsn(e.cloudHost, cpDB, "postgres"), keyFile, func() {
		os.Remove(keyFile)
		_ = execSQL(e.ctx, admin, "DROP DATABASE IF EXISTS "+cpDB+" WITH (FORCE)")
	}, nil
}

// RB8. Organization 1 on fdw+direct, its direct path role and owner in the control plane, a
// session through the proxy reading on the direct path. The customer restores the database
// elsewhere, where a row differs, and the old one still answers. After repoint the same
// session reads the copy's row on the direct path and its UPDATE lands in the copy, not in the
// old database. Before A1 repoint moved the FDW server alone: the session read and wrote the
// old database.
func checkRepointDirect(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	cp, keyFile, done, err := e.controlPlane()
	if err != nil {
		return false, "", err
	}
	defer done()
	for kind, d := range map[string]string{"direct": dsn("customer", e.db, "lokadb_dml"), "owner": e.appCustomer()} {
		if out, err := e.lokadb("org-credentials", "-db", e.cloud(), "-control-plane", cp, "-key", keyFile, "-org", "1", "-kind", kind, "-dsn", d); err != nil {
			return false, "org-credentials " + kind + ": " + lastErr(out), nil
		}
	}
	copyDSN, err := e.restoredCopy() // before the proxy: it ends the database's sessions
	if err != nil {
		return false, "", err
	}
	copyDB := e.db + "_copy"
	id, err := one(e.ctx, copyDSN, "SELECT min(id) FROM documents")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, copyDSN, "UPDATE documents SET title = 'only in the copy' WHERE id = "+id); err != nil {
		return false, "", err
	}
	cfg := map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}, "control_plane": cp, "credentials_key": keyFile}
	stop, err := e.proxyCfg(map[string]string{}, cfg)
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("rb8")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	read := func() (string, int, error) {
		n := directCount(e.proxyRoutes(), "1")
		var title string
		err := c.QueryRow(e.ctx, "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", 1, id).Scan(&title)
		return title, directCount(e.proxyRoutes(), "1") - n, err
	}
	before, d0, err := read()
	if err != nil {
		return false, "a read before repoint: " + firstErrLine(err), nil
	}
	out, err := e.repoint(cfg, "-org", "1", "-customer-app", dsn("customer", copyDB, "lokadb_app"))
	if err != nil {
		return false, "repoint failed: " + lastErr(out), nil
	}
	after, d1, err := read()
	if err != nil {
		return false, "a read after repoint: " + firstErrLine(err), nil
	}
	n := directCount(e.proxyRoutes(), "1")
	if _, err := c.Exec(e.ctx, "UPDATE documents SET title = 'after repoint' WHERE organization_id = $1 AND id = $2", 1, id); err != nil {
		return false, "an UPDATE after repoint: " + firstErrLine(err), nil
	}
	dUpd := directCount(e.proxyRoutes(), "1") - n
	inCopy, err := one(e.ctx, copyDSN, "SELECT count(*) FROM documents WHERE title = 'after repoint'")
	if err != nil {
		return false, "", err
	}
	inOld, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'after repoint'")
	if err != nil {
		return false, "", err
	}
	// the UPDATE goes the direct path once the proxy has learnt the form (8a), else through
	// the cloud: either way it must land in the copy
	ok := before != "only in the copy" && d0 == 1 && after == "only in the copy" && d1 == 1 && inCopy == "1" && inOld == "0"
	return ok, fmt.Sprintf("the session's direct read before repoint %q (direct %d), after %q (direct %d, want the copy's row); its UPDATE after (direct %d) in the copy %s, in the old database %s",
		before, d0, after, d1, dUpd, inCopy, inOld), nil
}

// RB9. Organization 1 on fdw+direct with its direct path role in the proxy's config
// (customers_app), which still names the old database. repoint to a restored copy is refused
// before it changes anything, and says which entry: else the direct path would stay on the old
// database and the cloud's writes go to the copy.
func checkRepointConfigOld(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	copyDSN, err := e.restoredCopy()
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, copyDSN, "UPDATE documents SET title = 'only in the copy' WHERE id = (SELECT min(id) FROM documents)"); err != nil {
		return false, "", err
	}
	cfg := map[string]any{"customers": map[string]any{e.db: map[string]string{"1": e.appCustomer()}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}}
	out, err := e.repoint(cfg, "-org", "1", "-customer-app", dsn("customer", e.db+"_copy", "lokadb_app"))
	refused := err != nil && strings.Contains(out, "customers_app") && strings.Contains(out, "changed nothing")
	seen, rerr := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1 AND title = 'only in the copy'")
	if rerr != nil {
		return false, "", rerr
	}
	why := "accepted"
	if err != nil {
		why = lastErr(out)
	}
	return refused && seen == "0", fmt.Sprintf("repoint with customers_app on the old database: %s; the cloud reads the copy's row: %s (want 0, still the old database)", why, seen), nil
}
