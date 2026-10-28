package main

// Block 4, item 9, stage B: the proxy routes tables outside public. A bare name resolves by
// the session's search_path, as in the cloud: public.documents (a cloud table here) and
// app.documents (a LokaDB table) share the name.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "X18", name: "a function, a DO block or a procedure that sets search_path for the session (set_config(..., false) inside): the proxy follows it, a bare documents is app's after it, no statement runs past the fanout policy over every organization", run: checkPathFuncs},
		check{id: "X26", name: "SET SCHEMA 'app' (a spelling of SET search_path), SET SESSION SCHEMA, UPDATE pg_settings: the proxy follows the path, a bare documents is app's after it and an unscoped count is refused", run: checkSetSchema},
		check{id: "X16", name: "app.documents moved, public.documents a cloud table of the same name: through the proxy a bare documents is app's or public's as the session's search_path says (qualified, $user, SET, startup parameter, options, ALTER ROLE, SET LOCAL, ROLLBACK, RESET, set_config)", run: checkSchemaRouting},
	)
}

// X16.
func checkSchemaRouting(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	// a row only the organization's database has: read through its foreign partition
	if err := execSQL(e.ctx, e.customer(), `INSERT INTO app.documents (id, org, title) VALUES (100000, 1, 'at-org')`); err != nil {
		return false, "", err
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	connect := func(params map[string]string) (*pgconn.PgConn, error) {
		cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
		if err != nil {
			return nil, err
		}
		for k, v := range params {
			cfg.RuntimeParams[k] = v
		}
		return pgconn.ConnectConfig(e.ctx, cfg)
	}
	// run: the first value of the last result, or "refused: …" / "error: …"
	run := func(c *pgconn.PgConn, sqls ...string) string {
		var got string
		for _, sql := range sqls {
			res, err := simple(e.ctx, c, sql)
			if err != nil {
				msg := firstErrLine(err)
				if strings.Contains(msg, "lokadb") {
					return "refused"
				}
				return "error: " + msg
			}
			got = ""
			if n := len(res); n > 0 && len(res[n-1].Rows) > 0 && len(res[n-1].Rows[0]) > 0 {
				got = string(res[n-1].Rows[0][0])
			}
		}
		return got
	}
	const routed = `SELECT title FROM documents WHERE org = 1 AND id = 100000`
	const all = `SELECT count(*) FROM documents` // no organization: refused for a LokaDB table (fanout reject)
	// app's: the routed read reaches the organization, the unscoped one is refused
	appCase := func(what string, c *pgconn.PgConn, pre ...string) {
		r, a := run(c, append(pre, routed)...), run(c, all)
		note(r == "at-org" && a == "refused", fmt.Sprintf("%s: organization 1's row %q, all rows %s", what, r, a))
	}
	// public's: the cloud table, not refused
	publicCase := func(what string, c *pgconn.PgConn, pre ...string) {
		a := run(c, append(pre, all)...)
		note(a == "200", fmt.Sprintf("%s: documents is public's, all rows %s", what, a))
	}

	c, err := connect(nil)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	q := run(c, `SELECT title FROM app.documents WHERE org = 1 AND id = 100000`)
	qa := run(c, `SELECT count(*) FROM app.documents`)
	note(q == "at-org" && qa == "refused", fmt.Sprintf("qualified app.documents: organization 1's row %q, all rows %s", q, qa))
	// the default "$user", public: the role is app, and schema app exists
	appCase(`default search_path ("$user", public: role app, schema app)`, c)
	publicCase("SET search_path TO public", c, "SET search_path TO public")
	appCase("SET search_path TO app, public", c, "SET search_path TO app, public")
	publicCase("SET search_path TO public again", c, "SET search_path TO public")
	appCase("set_config('search_path', 'app, public', false)", c, "SELECT set_config('search_path', 'app, public', false)")
	appCase(`RESET search_path (the default, "$user" first)`, c, "RESET search_path")
	run(c, "SET search_path TO public")
	r := run(c, "BEGIN", "SET LOCAL search_path TO app, public", routed)
	a := run(c, all)
	run(c, "ROLLBACK")
	note(r == "at-org" && a == "refused", fmt.Sprintf("SET LOCAL in a transaction: organization 1's row %q, all rows %s", r, a))
	publicCase("after that transaction (public again)", c)
	run(c, "BEGIN", "SET search_path TO app, public", "ROLLBACK")
	publicCase("after BEGIN; SET search_path TO app; ROLLBACK", c)
	run(c, "BEGIN", "SET search_path TO app, public", "COMMIT")
	appCase("after BEGIN; SET search_path TO app; COMMIT", c)

	for _, p := range []struct {
		what   string
		params map[string]string
		app    bool
	}{
		{"startup parameter search_path=public", map[string]string{"search_path": "public"}, false},
		{"startup parameter search_path=app, public", map[string]string{"search_path": "app, public"}, true},
		{"options -c search_path=public", map[string]string{"options": "-c search_path=public"}, false},
		{"options -c search_path=app,public", map[string]string{"options": "-c search_path=app,public"}, true},
	} {
		c2, err := connect(p.params)
		if err != nil {
			note(false, p.what+": "+firstErrLine(err))
			continue
		}
		if p.app {
			appCase(p.what, c2)
		} else {
			publicCase(p.what, c2)
		}
		c2.Close(e.ctx)
	}
	if err := execSQL(e.ctx, e.cloud(), fmt.Sprintf(`ALTER ROLE app IN DATABASE %s SET search_path = public`, e.db)); err != nil {
		return false, "", err
	}
	defer execSQL(e.ctx, e.cloud(), fmt.Sprintf(`ALTER ROLE app IN DATABASE %s RESET search_path`, e.db))
	c3, err := connect(nil)
	if err != nil {
		return false, "", err
	}
	publicCase("ALTER ROLE app SET search_path = public", c3)
	appCase("then SET search_path TO app, public", c3, "SET search_path TO app, public")
	publicCase("then RESET search_path (the role's default, public)", c3, "RESET search_path")
	// a write and a schema change by the bare name, app's by the path
	ins := run(c3, "SET search_path TO app, public", `INSERT INTO documents (org, title) VALUES (1, 'via-proxy')`,
		`UPDATE documents SET title = 'via-proxy-2' WHERE org = 1 AND title = 'via-proxy'`, `ALTER TABLE documents ADD COLUMN extra text`)
	atOrg := ""
	if v, err := one(e.ctx, e.customer(), `SELECT count(*) FROM app.documents WHERE title = 'via-proxy-2'`); err == nil {
		atOrg = v
	}
	cols := func(d, rel string) string {
		v, err := one(e.ctx, d, `SELECT count(*) FROM pg_attribute WHERE attrelid = '`+rel+`'::regclass AND attname = 'extra' AND NOT attisdropped`)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	orgCol, cloudCol, publicCol := cols(e.customer(), "app.documents"), cols(e.cloud(), "app.documents"), cols(e.cloud(), "public.documents")
	note(!strings.HasPrefix(ins, "error") && ins != "refused" && atOrg == "1" && orgCol == "1" && cloudCol == "1" && publicCol == "0",
		fmt.Sprintf("with search_path app, public: INSERT and UPDATE of documents %q, the row at the organization %s; ALTER TABLE documents ADD COLUMN: at the organization %s, in the cloud's app.documents %s, public.documents %s",
			ins, atOrg, orgCol, cloudCol, publicCol))
	c3.Close(e.ctx)
	return ok, strings.Join(notes, "; "), nil
}

// X18. Item 6 of block 4: the path a function changes. The proxy took documents for the cloud
// table public.documents and passed an unscoped count unrouted: it read app.documents of
// every organization past the fanout policy (lokadb_tenant lets every row through without an
// organization).
func checkPathFuncs(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION public.use_app() RETURNS void LANGUAGE plpgsql AS $$ BEGIN PERFORM set_config('search_path', 'app, public', false); END $$`,
		`CREATE PROCEDURE public.use_app_proc() LANGUAGE plpgsql AS $$ BEGIN EXECUTE 'SET search_path TO app, public'; END $$`,
		`CREATE FUNCTION public.pinned() RETURNS int LANGUAGE plpgsql SET search_path = app AS $$ BEGIN RETURN 1; END $$`,
		`GRANT EXECUTE ON FUNCTION public.use_app(), public.pinned() TO app`, `GRANT EXECUTE ON PROCEDURE public.use_app_proc() TO app`); err != nil {
		return false, "", err
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	run := func(sqls ...string) string {
		var got string
		for _, sql := range sqls {
			res, err := simple(e.ctx, c, sql)
			if err != nil {
				if msg := firstErrLine(err); strings.Contains(msg, "lokadb") {
					return "refused"
				} else {
					return "error: " + msg
				}
			}
			got = ""
			if n := len(res); n > 0 && len(res[n-1].Rows) > 0 && len(res[n-1].Rows[0]) > 0 {
				got = string(res[n-1].Rows[0][0])
			}
		}
		return got
	}
	const all = `SELECT count(*) FROM documents` // public's: 200 rows; app's: refused under reject
	for _, x := range []struct{ what, sets string }{
		{"SELECT use_app() (set_config inside)", "SELECT use_app()"},
		{"CALL use_app_proc() (SET inside)", "CALL use_app_proc()"},
		{"DO block with set_config", "DO $$ BEGIN PERFORM set_config('search_path', 'app, public', false); END $$"},
		{"SELECT use_app() among other statements", "SELECT 1; SELECT use_app(); SELECT 2"},
	} {
		before := run("SET search_path TO public", all)
		after := run(x.sets, all)
		note(before == "200" && after == "refused", fmt.Sprintf("%s: before, all rows of documents %s; after it %s", x.what, before, after))
	}
	// a function with its own SET search_path does not change the session's
	p := run("SET search_path TO public", "SELECT pinned()", all)
	note(p == "200", "a function with SET search_path of its own: documents stays public's, all rows "+p)

	// check judges a recorded bare name by the path the application's role starts with
	rec := filepath.Join(os.TempDir(), e.db+"-x18-workload.json")
	cfg := filepath.Join(os.TempDir(), e.db+"-x18-proxy.json")
	w := map[string]any{"version": 1, "updated": "2026-10-25T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{{"sql": all, "count": 10}}, "transactions": []any{}}}}
	for path, v := range map[string]any{rec: w, cfg: map[string]any{"fanout": map[string]any{"*": map[string]string{"mode": "reject"}}}} {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return false, "", err
		}
	}
	for _, x := range []struct {
		path    string
		refused bool
	}{{"public", false}, {"app, public", true}} {
		if err := execSQL(e.ctx, e.cloud(), fmt.Sprintf(`ALTER ROLE app IN DATABASE %s SET search_path = %s`, e.db, x.path)); err != nil {
			return false, "", err
		}
		out, _ := e.checkWith(schemaSpec, "-workload", rec, "-proxy-config", cfg, "-app-role", "app")
		note(reported(out, "query_refused") == x.refused, fmt.Sprintf("check -app-role app, the role's search_path %s: the recorded %q refused %v (want %v)", x.path, all, reported(out, "query_refused"), x.refused))
	}
	_ = execSQL(e.ctx, e.cloud(), fmt.Sprintf(`ALTER ROLE app IN DATABASE %s RESET search_path`, e.db))
	return ok, strings.Join(notes, "; "), nil
}

// X26. Review of routing (R4): SET SCHEMA 'app' did not change the path the proxy follows, so
// a bare documents stayed the cloud's public.documents and an unscoped count ran past reject.
func checkSetSchema(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	run := func(sqls ...string) string {
		var got string
		for _, sql := range sqls {
			res, err := simple(e.ctx, c, sql)
			if err != nil {
				if msg := firstErrLine(err); strings.Contains(msg, "lokadb") {
					return "refused"
				} else {
					return "error: " + msg
				}
			}
			got = ""
			if n := len(res); n > 0 && len(res[n-1].Rows) > 0 && len(res[n-1].Rows[0]) > 0 {
				got = string(res[n-1].Rows[0][0])
			}
		}
		return got
	}
	for _, x := range []struct{ what, set string }{
		{"SET SCHEMA 'app'", "SET SCHEMA 'app'"},
		{"SET SESSION SCHEMA 'app'", "SET SESSION SCHEMA 'app'"},
		{"SET LOCAL SCHEMA in a transaction", "BEGIN; SET LOCAL SCHEMA 'app'"},
		{"UPDATE pg_settings", "UPDATE pg_settings SET setting = 'app' WHERE name = 'search_path'"},
	} {
		run("ROLLBACK")
		run("SET search_path TO public")
		before := run(`SELECT count(*) FROM documents`)
		after := run(x.set, `SELECT count(*) FROM documents`)
		good := before == "200" && after == "refused"
		ok = ok && good
		if !good {
			x.what = "WRONG " + x.what
		}
		notes = append(notes, fmt.Sprintf("%s: before, all rows of documents %s; after it %s", x.what, before, after))
	}
	return ok, strings.Join(notes, "; "), nil
}
