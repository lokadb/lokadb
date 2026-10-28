package main

// Block 4, review points: what check says about FKs between cloud and LokaDB tables and about
// the application's role is what happens after enable and move-org.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X10", name: "an FK from a LokaDB table onto a cloud table holds for a moved organization: a missing row is refused, a delete of a referenced row is refused under fanout reject and cascades under log", run: checkFKToCloud},
		check{id: "X11", name: "an FK from a cloud table onto a LokaDB table keeps ON DELETE CASCADE and refuses a missing row for a moved organization", run: checkFKFromCloud},
		check{id: "X12", name: "an application role with BYPASSRLS: check blocks, since the organization policy does not apply to it", run: checkAppRoleBypass},
	)
}

// X10. documents.user_id -> users ON DELETE CASCADE (the base schema); users is not in
// routing.cloud_tables. Organization 1 is moved: lokadb checks the FK for it in the cloud. A
// missing user is refused for both organizations. Deleting a user is refused under the fanout
// policy reject (the default, and lokadb.fanout is unset outside the proxy); under log it
// cascades into organization 1's database as into the cloud's partition.
func checkFKToCloud(e *env) (bool, string, error) {
	out, code := e.checkWith(spec)
	change := strings.Contains(sectionOf(out, "Changes after the migration"), "[fk_to_cloud]") &&
		strings.Contains(out, "documents.user_id -> users (ON DELETE CASCADE)")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO users (id, name) VALUES (1001, 'x10')",
		"INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1001, 'x10-org1'), (2, 1001, 'x10-org2')"); err != nil {
		return false, "", err
	}
	del := func(fanout string) string {
		var stmts []string
		if fanout != "" {
			stmts = append(stmts, "SET lokadb.fanout = '"+fanout+"'")
		}
		if err := execSQL(e.ctx, e.cloud(), append(stmts, "DELETE FROM users WHERE id = 1001")...); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "deleted"
	}
	rejected, logged := del(""), del("log")
	org1, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'x10-org1'")
	org2, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE title = 'x10-org2'")
	missing := func(org int) string {
		err := execSQL(e.ctx, e.cloud(), fmt.Sprintf("INSERT INTO documents (organization_id, user_id, title) VALUES (%d, 999999, 'x10-missing')", org))
		if err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "accepted"
	}
	m1, m2 := missing(1), missing(2)
	ok := change && strings.Contains(rejected, "fanout policy (reject)") && logged == "deleted" && org1 == "0" && org2 == "0" &&
		strings.Contains(m1, "violates foreign key constraint") && strings.HasPrefix(m2, "refused")
	if !change {
		m1 += "; the check's changes: " + sectionOf(out, "Changes after the migration")
	}
	return ok, fmt.Sprintf("check (exit %d): fk_to_cloud as a change with its action %v; deleting the user: without lokadb.fanout %s, with log %s; then organization 1 keeps %s document(s), organization 2 %s; a missing user: organization 1 %s, organization 2 %s",
		code, change, rejected, logged, org1, org2, m1, m2), nil
}

// X11. audit (a cloud table) references documents ON DELETE CASCADE: enable moves the FK onto
// documents__refs, which the parent's trigger keeps, the moved organization's rows too.
func checkFKFromCloud(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE audit (id bigserial PRIMARY KEY, document_id bigint NOT NULL REFERENCES documents ON DELETE CASCADE, note text)",
		"ALTER TABLE audit OWNER TO app"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, title) VALUES (1, 'x11-org1'), (2, 'x11-org2')",
		"INSERT INTO audit (document_id, note) SELECT id, title FROM documents WHERE title LIKE 'x11-%'"); err != nil {
		return false, "", err
	}
	before, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM audit WHERE note LIKE 'x11-%'")
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM documents WHERE title LIKE 'x11-%'"); err != nil {
		return false, "", err
	}
	after, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM audit WHERE note LIKE 'x11-%'")
	missing := "accepted"
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO audit (document_id, note) VALUES (999999, 'x11-missing')"); err != nil {
		missing = "refused: " + firstErrLine(err)
	}
	fk, _ := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(pg_get_constraintdef(oid), '; '), '') FROM pg_constraint WHERE conrelid = 'audit'::regclass AND contype = 'f'")
	ok := before == "2" && after == "0" && strings.HasPrefix(missing, "refused") && strings.Contains(fk, "documents__refs")
	return ok, fmt.Sprintf("audit's FK: %s; audit rows of the two documents: %s, after deleting them (one of the moved organization): %s; a missing document: %s",
		fk, before, after, missing), nil
}

// X12. The application connects as appsu, which has BYPASSRLS: lokadb_tenant does not narrow
// its queries, so with lokadb.org = 1 it still sees both organizations.
func checkAppRoleBypass(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`DO $$ BEGIN CREATE ROLE appsu LOGIN PASSWORD 'appsu'; EXCEPTION WHEN duplicate_object THEN END $$`,
		"ALTER ROLE appsu BYPASSRLS"); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(spec, "-app-role", "appsu")
	blocked := code == 3 && strings.Contains(sectionOf(out, "Blockers"), "[app_role_bypasses_rls]")
	plain, _ := e.checkWith(spec, "-app-role", "app")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO appsu"); err != nil {
		return false, "", err
	}
	const q = "SELECT count(DISTINCT organization_id)::text FROM documents"
	su, err := e.asRole("appsu", "1", q)
	if err != nil {
		return false, "", err
	}
	app, err := e.asRole("app", "1", q)
	if err != nil {
		return false, "", err
	}
	ok := blocked && !reported(plain, "app_role_bypasses_rls") && su == "2" && app == "1"
	return ok, fmt.Sprintf("check -app-role appsu: exit %d, %s; -app-role app reports it: %v; organizations seen with lokadb.org = 1: appsu %s, app %s",
		code, finding(out, "app_role_bypasses_rls"), reported(plain, "app_role_bypasses_rls"), su, app), nil
}
