package main

// Review of admin (A16): the SECURITY DEFINER trigger functions of the FKs onto cloud tables
// (and the cascade's) have search_path = pg_catalog, pg_temp (pg_temp last), not pg_catalog, public: on
// Postgres 14 and older anyone can write to public and put an operator or a cast in front of
// lokadb's. Keys of a type from an extension (citext, whose = lives in public) are still
// compared right, by OPERATOR(schema.=).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X27", name: "an FK onto a cloud table keyed by citext: the triggers' search_path is pg_catalog, pg_temp, case-insensitive keys are still matched, RESTRICT holds", run: checkFKPath},
	)
}

const fkPathSpec = `{"tables": [{"name": "bills", "tenant_column": "org"}]}`

// X27.
func checkFKPath(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE EXTENSION IF NOT EXISTS citext`,
		`CREATE TABLE accounts (code citext PRIMARY KEY, name text)`,
		`CREATE TABLE bills (id bigserial PRIMARY KEY, org bigint NOT NULL, account citext REFERENCES accounts (code) ON DELETE RESTRICT ON UPDATE RESTRICT)`,
		`ALTER TABLE accounts OWNER TO app`, `ALTER TABLE bills OWNER TO app`,
		`INSERT INTO accounts VALUES ('acme', 'Acme'), ('beta', 'Beta')`,
		`INSERT INTO bills (org, account) SELECT 1 + g % 2, CASE WHEN g % 2 = 0 THEN 'ACME' ELSE 'beta' END FROM generate_series(1, 10) g`); err != nil {
		return false, "", err
	}
	e.spec = fkPathSpec
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	exec := func(sql string) string {
		// the fanout policy log: the action runs in the organizations' databases (the default
		// reject would refuse the delete before looking at the keys)
		if err := execSQL(e.ctx, e.cloud(), `SELECT set_config('lokadb.fanout', 'log', false)`, sql); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "accepted"
	}
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	paths := val(e.cloud(), `SELECT coalesce(string_agg(DISTINCT p.proconfig::text, ' '), '-') FROM pg_proc p
		WHERE p.pronamespace = 'lokadb'::regnamespace AND p.prosecdef AND p.proname ~ '^(fk_(check|act)__|fkcascade__)'`)
	n := val(e.cloud(), `SELECT count(*) FROM pg_proc p WHERE p.pronamespace = 'lokadb'::regnamespace AND p.prosecdef AND p.proname ~ '^(fk_(check|act)__|fkcascade__)'`)
	note(paths == `{"search_path=pg_catalog, pg_temp"}` && n != "0" && n != "error", fmt.Sprintf("the %s SECURITY DEFINER trigger functions of the FKs have search_path %s", n, paths))
	same := exec(`INSERT INTO bills (org, account) VALUES (1, 'BETA')`) // the key differs in case only
	missing := exec(`INSERT INTO bills (org, account) VALUES (1, 'nope')`)
	note(same == "accepted" && strings.HasPrefix(missing, "refused"), fmt.Sprintf("a bill of the moved organization: account 'BETA' (accounts has 'beta') %s, 'nope' %s", same, missing))
	del := exec(`DELETE FROM accounts WHERE code = 'ACME'`) // organization 1 has bills of 'ACME'
	upd := exec(`UPDATE accounts SET code = 'gamma' WHERE code = 'beta'`)
	note(strings.HasPrefix(del, "refused") && strings.HasPrefix(upd, "refused"), fmt.Sprintf("an account its bills reference: DELETE %s, key change %s", del, upd))
	return ok, strings.Join(notes, "; "), nil
}
