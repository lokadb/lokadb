package main

// Review 3 of check, H1 and H2: what holds a LokaDB table by its OID besides views, rules and
// SQL functions. A policy of a cloud table (and of another LokaDB table) reading a LokaDB table
// read its DEFAULT partition after enable: permission denied for the application's roles, and
// after a move the moved organization's rows gone from it. A function over the row type (a
// PostgREST computed column) did not exist for the parent's rows, and the view calling it
// failed enable. A column of the row type in another table: enable refuses before it changes
// anything.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "X31", name: "a policy of a cloud table and one of a LokaDB table read a LokaDB table, a function over its row type and a view calling it: after enable and a move they read the parent (the moved organization's rows too)", run: checkDependents},
		check{id: "X32", name: "a column of a LokaDB table's row type in another table: enable refuses, naming it, before it changes anything", run: checkRowtypeColumn},
	)
}

const dependentsSpec = `{"tables": [{"name": "blocked", "tenant_column": "org"}, {"name": "memos", "tenant_column": "org"}]}`

// asRole: the first column of sql, read as role on the cloud database ("error: …" if it fails).
func asRole(e *env, role, sql string) string {
	c, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	defer c.Close(e.ctx)
	tx, err := c.Begin(e.ctx)
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	defer tx.Rollback(e.ctx) //nolint:errcheck // read only
	if _, err := tx.Exec(e.ctx, "SET LOCAL ROLE "+role); err != nil {
		return "error: " + firstErrLine(err)
	}
	var v string
	if err := tx.QueryRow(e.ctx, "SELECT ("+sql+")::text").Scan(&v); err != nil {
		return "error: " + firstErrLine(err)
	}
	return v
}

// X31.
func checkDependents(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`DO $$ BEGIN CREATE ROLE x31_reader; EXCEPTION WHEN duplicate_object THEN END $$`,
		`CREATE TABLE blocked (id bigserial PRIMARY KEY, org bigint NOT NULL, user_name text)`,
		`CREATE TABLE memos (id bigserial PRIMARY KEY, org bigint NOT NULL, body text)`,
		`CREATE TABLE notices (id bigserial PRIMARY KEY, org bigint NOT NULL, body text)`, // stays in the cloud
		`ALTER TABLE notices ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY not_blocked ON notices USING (NOT EXISTS (SELECT 1 FROM blocked b WHERE b.org = notices.org AND b.user_name = current_user))`,
		`ALTER TABLE memos ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY memos_not_blocked ON memos USING (NOT EXISTS (SELECT 1 FROM blocked b WHERE b.org = memos.org AND b.user_name = current_user))`,
		`CREATE FUNCTION body_len(m memos) RETURNS int LANGUAGE sql STABLE AS 'SELECT length(m.body)'`,
		`CREATE VIEW memo_lens AS SELECT id, org, body_len(m) AS n FROM memos m`,
		`ALTER TABLE blocked OWNER TO app`, `ALTER TABLE memos OWNER TO app`, `ALTER TABLE notices OWNER TO app`,
		`ALTER VIEW memo_lens OWNER TO app`, `ALTER FUNCTION body_len(memos) OWNER TO app`,
		`GRANT SELECT ON blocked, memos, notices, memo_lens TO x31_reader`,
		// x31_reader is blocked in organization 2: it sees organization 1's rows only
		`INSERT INTO blocked (org, user_name) VALUES (2, 'x31_reader')`,
		`INSERT INTO notices (org, body) VALUES (1, 'n1'), (2, 'n2')`,
		`INSERT INTO memos (org, body) VALUES (1, 'hello'), (2, 'hidden')`); err != nil {
		return false, "", err
	}
	e.spec = dependentsSpec
	var notes []string
	ok := true
	probe := func(when string) {
		got := []string{
			asRole(e, "x31_reader", `SELECT count(*) FROM notices`),
			asRole(e, "x31_reader", `SELECT string_agg(n::text, ',' ORDER BY id) FROM memo_lens`),
			asRole(e, "x31_reader", `SELECT string_agg(body_len(m)::text, ',' ORDER BY id) FROM memos m`),
		}
		// the view reads as its owner (app, not blocked anywhere): both organizations' rows
		good := got[0] == "1" && got[1] == "5,6" && got[2] == "5"
		s := fmt.Sprintf("%s, as x31_reader: notices %s (want 1), memo_lens %s (want 5,6), body_len over memos %s (want 5)", when, got[0], got[1], got[2])
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	probe("before enable")
	if err := e.enable(); err != nil {
		return false, strings.Join(notes, "; ") + "; WRONG enable failed: " + firstErrLine(err), nil
	}
	probe("after enable")
	if mv, err := e.move(2); err != nil {
		return false, strings.Join(notes, "; ") + "; WRONG move-org 2 failed: " + lastErr(mv), nil
	}
	// organization 2's blocked row is in its database now: a policy on blocked__cloud would let
	// organization 2's notices through
	probe("after the move of organization 2")
	return ok, strings.Join(notes, "; "), nil
}

// X32.
func checkRowtypeColumn(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE memos (id bigserial PRIMARY KEY, org bigint NOT NULL, body text)`,
		`CREATE TABLE snapshots (id bigserial PRIMARY KEY, memo memos)`,
		`ALTER TABLE memos OWNER TO app`, `ALTER TABLE snapshots OWNER TO app`); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "memos", "tenant_column": "org"}]}`
	err := e.enable()
	kind, _ := one(e.ctx, e.cloud(), `SELECT relkind::text FROM pg_class WHERE oid = 'memos'::regclass`)
	lokadb, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_namespace WHERE nspname = 'lokadb'`)
	refused := err != nil && strings.Contains(err.Error(), "snapshots")
	msg := "accepted"
	if err != nil {
		msg = "refused: " + firstErrLine(err)
	}
	return refused && kind == "r", fmt.Sprintf("enable %s; memos is of relkind %s after it (want r), schema lokadb %s", msg, kind, lokadb), nil
}
