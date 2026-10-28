package main

// Block 5 (moving an organization): what a split out of DEFAULT that fails leaves behind.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV1", name: "a split out of DEFAULT that fails leaves no CHECK on DEFAULT: the organization's rows there stay writable", run: checkSplitFailCheck},
		check{id: "MV1", name: "move-cleanup removes the CHECK on DEFAULT a killed split left", run: checkSplitCheckCleanup},
	)
}

// notOrgCheck: the CHECK lokadb_not_org1 on documents' DEFAULT partition, "" when it is not there.
const notOrgCheck = `SELECT coalesce((SELECT CASE WHEN convalidated THEN 'valid' ELSE 'not valid' END FROM pg_constraint
	WHERE conrelid = 'documents__cloud'::regclass AND conname = 'lokadb_not_org1'), '')`

// MV1 (block 5, 2a). The split adds CHECK (key <> 1) NOT VALID to DEFAULT, then moves the
// rows and attaches the partition in one transaction. When that transaction fails (here the
// attach cannot get the parent's lock), the rows stay in DEFAULT and so did the CHECK: every
// UPDATE of them failed until move-org ran again.
func checkSplitFailCheck(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// An open transaction holds the parent (not DEFAULT): the attach's EXCLUSIVE lock waits.
	hold, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer hold.Close(e.ctx)
	if _, err := hold.Exec(e.ctx, "BEGIN; LOCK TABLE ONLY documents IN SHARE MODE"); err != nil {
		return false, "", err
	}
	out, merr := e.move(1, "-lock-timeout", "300ms")
	_, _ = hold.Exec(e.ctx, "ROLLBACK")
	if merr == nil {
		return false, "move-org succeeded although the parent was locked: the check proves nothing", nil
	}
	split, err := one(e.ctx, e.cloud(), "SELECT (to_regclass('documents__o1') IS NOT NULL AND EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('documents__o1')))::text")
	if err != nil {
		return false, "", err
	}
	chk, err := one(e.ctx, e.cloud(), notOrgCheck)
	if err != nil {
		return false, "", err
	}
	// the application updates a row of organization 1, still in DEFAULT
	_, uerr := one(e.ctx, e.cloud(), "WITH u AS (UPDATE documents SET title = 'mv1' WHERE organization_id = 1 AND id = (SELECT min(id) FROM documents WHERE organization_id = 1) RETURNING 1) SELECT count(*) FROM u")
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	ok := split == "false" && chk == "" && uerr == nil && n == "100"
	return ok, fmt.Sprintf("move-org failed (%s); partition attached: %s; CHECK on DEFAULT left: %q; UPDATE of an organization 1 row: %s; its rows in the cloud: %s",
		lastLine(strings.TrimSpace(out+merr.Error())), split, chk, errOr(uerr, true), n), nil
}

// MV1. A move killed between the CHECK and the split's commit leaves the CHECK NOT VALID on
// DEFAULT with the organization's rows still there (the state is made by hand: the window is
// a few statements wide). move-cleanup must remove it; a CHECK of an organization that has
// its own partition stays (it proves to ATTACH that DEFAULT holds none of its rows).
func checkSplitCheckCleanup(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// organization 2 moved the normal way: its CHECK on DEFAULT is valid and must stay
	if _, err := e.move(2); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents__cloud ADD CONSTRAINT lokadb_not_org1 CHECK (coalesce(organization_id, -2) <> 1) NOT VALID"); err != nil {
		return false, "", err
	}
	before, err := one(e.ctx, e.cloud(), notOrgCheck)
	if err != nil {
		return false, "", err
	}
	out, cerr := e.lokadb("move-cleanup", "-db", e.cloud())
	if cerr != nil {
		return false, "", cerr
	}
	after, err := one(e.ctx, e.cloud(), notOrgCheck)
	if err != nil {
		return false, "", err
	}
	_, uerr := one(e.ctx, e.cloud(), "WITH u AS (UPDATE documents SET title = 'mv1' WHERE organization_id = 1 RETURNING 1) SELECT count(*) FROM u")
	other, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_constraint WHERE conrelid = 'documents__cloud'::regclass AND conname = 'lokadb_not_org2' AND convalidated`)
	if err != nil {
		return false, "", err
	}
	ok := before == "not valid" && after == "" && uerr == nil && other == "1"
	return ok, fmt.Sprintf("CHECK before move-cleanup: %q, after: %q (%s); UPDATE of organization 1: %s; the valid CHECK of moved organization 2 kept: %s",
		before, after, strings.TrimSpace(lastLine(out)), errOr(uerr, true), other), nil
}
