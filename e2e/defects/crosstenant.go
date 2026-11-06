package main

// The cross-tenant suite: organization A's session tries to reach organization B's rows, on real
// Postgres through the real proxy, in the shapes where a statement hides the tenant: a join through
// a table without an organization column, CTEs, subqueries, views and functions, errors and side
// channels, prepared statements reused across organizations, and pooled connections handed from one
// client to the next. Run it alone with `bash e2e/defects/crosstenant.sh`.
//
// Every row of the two organizations carries a mark (SECRET-O<org>-…). A check fails when a session
// of one organization gets the other's mark anywhere: a result, a COPY, an error message. So that a
// refusal of everything could not pass, each read shape also runs under the organization that owns
// the row, and must return its mark there (the control).
//
// What the suite promises and what it does not: see docs/testing.md, "The cross-tenant suite". By
// design the application names the organization, and LokaDB has no per-tenant credentials: a session
// that names no organization and asks for B's row by its id gets it (XT5 and XT6 pin that too).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	named := func(both, direct bool) func(*env) (bool, string, error) {
		return func(e *env) (bool, string, error) { return xtRun(e, both, direct, false, (*xtStand).named) }
	}
	unnamed := func(both, direct bool) func(*env) (bool, string, error) {
		return func(e *env) (bool, string, error) { return xtRun(e, both, direct, false, (*xtStand).unnamed) }
	}
	prepared := func(both, direct bool) func(*env) (bool, string, error) {
		return func(e *env) (bool, string, error) { return xtRun(e, both, direct, false, (*xtStand).prepared) }
	}
	pooled := func(both, direct bool) func(*env) (bool, string, error) {
		return func(e *env) (bool, string, error) { return xtRun(e, both, direct, true, (*xtStand).pooled) }
	}
	stress := func(both, direct bool) func(*env) (bool, string, error) {
		return func(e *env) (bool, string, error) { return xtRun(e, both, direct, true, (*xtStand).stress) }
	}
	checks = append(checks,
		check{id: "XT1", name: "cross-tenant, organization named, fdw, B in the cloud: every read shape, write and tampering under A never reaches B's rows, in both directions", run: named(false, false)},
		check{id: "XT2", name: "cross-tenant, organization named, fdw+direct, B in the cloud: the same", run: named(false, true)},
		check{id: "XT3", name: "cross-tenant, organization named, fdw, both moved to databases of their own: the same", run: named(true, false)},
		check{id: "XT4", name: "cross-tenant, organization named, fdw+direct, both moved: the same", run: named(true, true)},
		check{id: "XT5", name: "cross-tenant, organization only in the statement, fdw, B in the cloud: a statement naming A never shows B's rows, a statement naming nobody is refused, by id it routes (not an authorization layer)", run: unnamed(false, false)},
		check{id: "XT6", name: "cross-tenant, organization only in the statement, fdw+direct, both moved: the same", run: unnamed(true, true)},
		check{id: "XT7", name: "cross-tenant, one prepared statement reused across organizations on one connection, custom and generic plans, fdw, B in the cloud", run: prepared(false, false)},
		check{id: "XT8", name: "cross-tenant, one prepared statement reused across organizations, fdw+direct, both moved", run: prepared(true, true)},
		check{id: "XT9", name: "cross-tenant, a pooled connection handed from a client of A to a client of B keeps nothing of A, fdw, B in the cloud", run: pooled(false, false)},
		check{id: "XT10", name: "cross-tenant, pooled connections on the cloud and the direct path, fdw+direct, both moved", run: pooled(true, true)},
		check{id: "XT11", name: "cross-tenant under load: clients of two organizations on the pool, connecting and disconnecting, each sees its own rows alone, fdw, B in the cloud", run: stress(false, false)},
		check{id: "XT12", name: "cross-tenant under load, fdw+direct, both moved", run: stress(true, true)},
	)
}

// xtStand: organization 1 moved, organization 2 in the cloud or moved to a database of its own, the
// proxy under fanout reject with its default connection pools.
type xtStand struct {
	e      *env
	direct bool
	pools  bool              // the direct path's pool on too
	db2    string            // organization 2's database, "" when it stays in the cloud
	doc    map[string]string // organization -> the id of one of its documents, owned by the shared user
	ver    map[string]string // organization -> the id of a version of that document
	ok     bool
	notes  []string
	tried  int
}

func (s *xtStand) wrong(format string, args ...any) {
	s.ok = false
	if len(s.notes) < 40 {
		s.notes = append(s.notes, "WRONG "+fmt.Sprintf(format, args...))
	}
}

func xtMark(org string) string { return "SECRET-O" + org + "-" }

func xtOther(org string) string {
	if org == "1" {
		return "2"
	}
	return "1"
}

// xtRun prepares the stand and runs one part of the suite on it.
func xtRun(e *env, both, direct, pools bool, part func(*xtStand) error) (bool, string, error) {
	s := &xtStand{e: e, direct: direct, pools: pools, ok: true, doc: map[string]string{}, ver: map[string]string{}}
	if err := execSQL(e.ctx, e.cloud(),
		"UPDATE documents SET title = 'SECRET-O' || organization_id || '-' || id",
		"UPDATE document_versions v SET content = 'SECRET-O' || d.organization_id || '-v' || v.id FROM documents d WHERE d.id = v.document_id",
		// users has no organization column: user 1 owns a document of each organization
		"UPDATE documents SET user_id = 1 WHERE id IN (SELECT min(id) FROM documents GROUP BY organization_id)"); err != nil {
		return false, "", err
	}
	extra := map[string]string(nil)
	if both {
		s.db2 = e.db + "_o2"
		if err := execSQL(e.ctx, dsn(e.customerHost, "postgres", "postgres"),
			"DROP DATABASE IF EXISTS "+s.db2+" WITH (FORCE)", "CREATE DATABASE "+s.db2+" OWNER app"); err != nil {
			return false, "", err
		}
		if err := execSQL(e.ctx, dsn(e.customerHost, s.db2, "postgres"), "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
			return false, "", err
		}
		extra = map[string]string{"2": s.db2}
	}
	stop, err := e.directSetup(extra)
	if err != nil {
		return false, "", err
	}
	if !pools {
		defer stop()
	}
	if both {
		if out, err := e.move(2, "-customer", dsn(e.customerHost, s.db2, "postgres"), "-customer-app", dsn(e.customerHost, s.db2, "lokadb_app")); err != nil {
			return false, "", fmt.Errorf("moving organization 2: %s", lastErr(out))
		}
	}
	if direct {
		if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
			return false, "", fmt.Errorf("org-profile 1: %s", lastErr(out))
		}
		if both {
			if out, err := e.orgProfile(2, "fdw+direct", s.db2); err != nil {
				return false, "", fmt.Errorf("org-profile 2: %s", lastErr(out))
			}
		}
	}
	// a view and a function the application owns: they hide the table from the statement
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE VIEW xt_docs AS SELECT id, organization_id, title FROM documents", "ALTER VIEW xt_docs OWNER TO app",
		"CREATE FUNCTION xt_title(bigint) RETURNS text LANGUAGE sql STABLE AS 'SELECT title FROM documents WHERE id = $1'", "ALTER FUNCTION xt_title(bigint) OWNER TO app",
		"NOTIFY lokadb_meta"); err != nil {
		return false, "", err
	}
	time.Sleep(300 * time.Millisecond)
	for _, org := range []string{"1", "2"} {
		if s.doc[org], err = one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = "+org); err != nil {
			return false, "", err
		}
		if s.ver[org], err = one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE document_id = "+s.doc[org]); err != nil {
			return false, "", err
		}
	}
	if err := s.placement(); err != nil {
		return false, "", err
	}
	if s.pools {
		// the direct path's pool is off by default; the checks of the pools need it on
		stop()
		customers, apps := map[string]string{"1": e.appCustomer()}, map[string]string{"1": dsn(e.customerHost, e.db, "lokadb_dml")}
		if both {
			customers["2"], apps["2"] = dsn(e.customerHost, s.db2, "lokadb_app"), dsn(e.customerHost, s.db2, "lokadb_dml")
		}
		restarted, err := e.proxyCfg(customers, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
			"customers_app": map[string]any{e.db: apps}, "direct_pool": map[string]any{}})
		if err != nil {
			return false, "", err
		}
		stop = restarted
		defer func() { stop() }()
	}
	if err := part(s); err != nil {
		return false, "", err
	}
	detail := fmt.Sprintf("%d attempts, nothing of the other organization reached", s.tried)
	if !s.ok {
		detail = fmt.Sprintf("%d attempts: %s", s.tried, strings.Join(s.notes, "; "))
	}
	return s.ok, detail, nil
}

// placement: a moved organization's rows are in its database alone, and no database of an
// organization holds another's.
func (s *xtStand) placement() error {
	count := func(d, org string) (string, error) {
		return one(s.e.ctx, d, fmt.Sprintf("SELECT (SELECT count(*) FROM documents WHERE title LIKE '%[1]s%%') + (SELECT count(*) FROM document_versions WHERE content LIKE '%[1]s%%')", xtMark(org)))
	}
	dbs := []struct {
		name, dsn, own, foreign string
	}{{"organization 1's database", s.e.customer(), "1", "2"}}
	if s.db2 != "" {
		dbs = append(dbs, struct{ name, dsn, own, foreign string }{"organization 2's database", dsn(s.e.customerHost, s.db2, "postgres"), "2", "1"})
	}
	for _, d := range dbs {
		own, err := count(d.dsn, d.own)
		if err != nil {
			return err
		}
		other, err := count(d.dsn, d.foreign)
		if err != nil {
			return err
		}
		s.tried++
		if own == "0" || other != "0" {
			s.wrong("%s holds %s rows of its own and %s of the other organization", d.name, own, other)
		}
	}
	inCloud, err := one(s.e.ctx, s.e.cloud(), orgInCloud)
	if err != nil {
		return err
	}
	s.tried++
	if inCloud != "0" {
		s.wrong("the cloud keeps %s rows of the moved organization 1", inCloud)
	}
	return nil
}

// xtAnswer: all that came back, the error message included, and the SQLSTATE.
type xtAnswer struct{ text, code string }

// exec runs statements on c, in a transaction under the organization when tenant is set: all the
// text that came back, whether as rows, as COPY data or in an error.
func (s *xtStand) exec(c *pgconn.PgConn, tenant string, extended bool, sqls ...string) xtAnswer {
	ctx := s.e.ctx
	var b strings.Builder
	fail := func(err error) xtAnswer {
		b.WriteString(" ERROR " + err.Error())
		if tenant != "" {
			_, _ = simple(ctx, c, "ROLLBACK")
		}
		return xtAnswer{b.String(), pgCode(err)}
	}
	if tenant != "" {
		if _, err := simple(ctx, c, "BEGIN"); err != nil {
			return fail(err)
		}
		if _, err := simple(ctx, c, "SET LOCAL lokadb.tenant = '"+tenant+"'"); err != nil {
			return fail(err)
		}
	}
	take := func(rows [][][]byte) {
		for _, r := range rows {
			for _, cell := range r {
				b.Write(cell)
				b.WriteByte('\n')
			}
		}
	}
	for _, sql := range sqls {
		switch {
		case strings.HasPrefix(sql, "COPY "):
			var out bytes.Buffer
			if _, err := c.CopyTo(ctx, &out, sql); err != nil {
				return fail(err)
			}
			b.Write(out.Bytes())
		case extended:
			r := c.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
			if r.Err != nil {
				return fail(r.Err)
			}
			take(r.Rows)
		default:
			rs, err := simple(ctx, c, sql)
			if err != nil {
				return fail(err)
			}
			for _, r := range rs {
				take(r.Rows)
			}
		}
	}
	if tenant != "" {
		if _, err := simple(ctx, c, "COMMIT"); err != nil {
			return fail(err)
		}
	}
	return xtAnswer{b.String(), ""}
}

// trace (DEFECTS_TRACE): what each shape came to, for reading the suite's run.
func (s *xtStand) trace(what string, a xtAnswer) {
	if os.Getenv("DEFECTS_TRACE") == "" {
		return
	}
	result := "rows"
	if a.code != "" {
		result = "refused " + a.code
	} else if a.text == "" {
		result = "nothing"
	}
	fmt.Printf("    %s: %s\n", what, result)
}

// session: a connection through the proxy; a statement that broke it gets a new one.
func (s *xtStand) session(c **pgconn.PgConn) error {
	if *c != nil && !(*c).IsClosed() {
		return nil
	}
	n, err := s.e.viaProxy()
	if err != nil {
		return err
	}
	*c = n
	return nil
}

func xtShort(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		return s[:160] + "…"
	}
	return s
}

// xtShape: {id} and {ver} are a document and a version of the target organization {org}, {me} the
// session's organization, {uid} the user both organizations have a document of. noControl: the shape
// reads nothing by itself even for the row's owner.
type xtShape struct {
	name, sql string
	noControl bool
}

var xtReads = []xtShape{
	{"by the organization", "SELECT title FROM documents WHERE organization_id = {org}", false},
	{"by id", "SELECT title FROM documents WHERE id = {id}", false},
	{"a child by id", "SELECT content FROM document_versions WHERE id = {ver}", false},
	{"a child by its parent", "SELECT content FROM document_versions WHERE document_id = {id}", false},
	{"no condition", "SELECT title FROM documents", false},
	{"a child, no condition", "SELECT content FROM document_versions", false},

	// a join through a table without an organization column
	{"join through users", "SELECT d.title FROM users u JOIN documents d ON d.user_id = u.id WHERE u.id = {uid}", false},
	{"join through users to the child", "SELECT v.content FROM users u JOIN documents d ON d.user_id = u.id JOIN document_versions v ON v.document_id = d.id WHERE u.id = {uid}", false},
	{"left join from users", "SELECT d.title FROM users u LEFT JOIN documents d ON d.user_id = u.id WHERE u.id = {uid}", false},
	{"lateral from users", "SELECT x.title FROM users u, LATERAL (SELECT title FROM documents d WHERE d.user_id = u.id) x WHERE u.id = {uid}", false},
	{"a self-join by the user", "SELECT o.title FROM documents d JOIN documents o ON o.user_id = d.user_id WHERE d.id = {id}", false},

	// a CTE or a subquery that hides the tenant column
	{"CTE of everything", "WITH x AS (SELECT * FROM documents) SELECT title FROM x", false},
	{"CTE by id", "WITH x AS (SELECT * FROM documents WHERE id = {id}) SELECT title FROM x", false},
	{"CTE that renames the columns", "WITH x(a, o, u, t) AS (SELECT id, organization_id, user_id, title FROM documents) SELECT t FROM x WHERE a = {id}", false},
	{"CTE without the tenant column", "WITH x AS (SELECT id, title FROM documents) SELECT title FROM x WHERE id = {id}", false},
	{"recursive CTE", "WITH RECURSIVE r(id, t) AS (SELECT id, title FROM documents WHERE id = {id} UNION ALL SELECT d.id, d.title FROM documents d JOIN r ON d.id = r.id + 1000000) SELECT t FROM r", false},
	{"data-modifying CTE", "WITH x AS (UPDATE documents SET title = title WHERE id = {id} RETURNING title) SELECT title FROM x", false},
	{"derived table", "SELECT t.title FROM (SELECT id, title FROM documents) t WHERE t.id = {id}", false},
	{"scalar subquery", "SELECT (SELECT title FROM documents WHERE id = {id})", false},
	{"IN subquery on the child", "SELECT content FROM document_versions WHERE document_id IN (SELECT id FROM documents WHERE title LIKE 'SECRET-O{org}-%')", false},
	{"UNION with the session's own", "SELECT title FROM documents WHERE organization_id = {me} UNION ALL SELECT title FROM documents WHERE id = {id}", false},
	{"an aggregate over everything", "SELECT string_agg(title, ',') FROM documents", false},
	{"a whole row as JSON", "SELECT to_jsonb(d)::text FROM documents d WHERE d.id = {id}", false},

	// a view and a function hide the table itself
	{"a view", "SELECT title FROM xt_docs WHERE id = {id}", false},
	{"a view, no condition", "SELECT title FROM xt_docs", false},
	{"a function", "SELECT xt_title({id})", false},

	// other ways out
	{"COPY of a query", "COPY (SELECT title FROM documents WHERE id = {id}) TO STDOUT", false},
	{"COPY of a table", "COPY documents TO STDOUT", false},
	{"the value in an error message", "SELECT title::int FROM documents WHERE id = {id}", false},
	{"a yes or no by EXISTS", "SELECT CASE WHEN EXISTS (SELECT 1 FROM documents WHERE id = {id}) THEN 'SECRET-O{org}-seen' END", false},
	{"a yes or no by count", "SELECT 'SECRET-O{org}-count' FROM documents WHERE id = {id} HAVING count(*) > 0", false},
}

// xtTamper: a session of the organization tries to change the organization it runs under. Nothing
// here may show the other's rows.
var xtTamper = [][]string{
	{"SET LOCAL lokadb.org = '{org}'", "SELECT title FROM documents"},
	{"SELECT set_config('lokadb.org', '{org}', true)", "SELECT title FROM documents WHERE id = {id}"},
	{"SELECT set_config('lokadb.org', '{org}', true); SELECT title FROM documents"},
	{"SELECT set_config('lokadb.org', '{org}', true), title FROM documents"},
	{"SELECT set_config('lokadb.org', '', true)", "SELECT title FROM documents"},
	{"RESET lokadb.org", "SELECT title FROM documents WHERE id = {id}"},
	{"SET LOCAL row_security = off", "SELECT title FROM documents WHERE id = {id}"},
}

// xtWrites: a session of the organization writes at another's rows; the rows must stay as they were.
var xtWrites = []string{
	"UPDATE documents SET title = 'HACKED' WHERE id = {id}",
	"UPDATE documents SET title = 'HACKED' WHERE organization_id = {org}",
	"UPDATE document_versions SET content = 'HACKED' WHERE document_id = {id}",
	"DELETE FROM document_versions WHERE id = {ver}",
	"DELETE FROM documents WHERE id = {id}",
	"INSERT INTO documents (organization_id, title) VALUES ({org}, 'HACKED')",
	"INSERT INTO document_versions (document_id, content) VALUES ({id}, 'HACKED')",
	"UPDATE documents SET organization_id = {me} WHERE id = {id}",
	"WITH x AS (DELETE FROM documents WHERE id = {id} RETURNING id) SELECT count(*) FROM x",
}

func (s *xtStand) fill(sql, target, me string) string {
	return strings.NewReplacer("{id}", s.doc[target], "{ver}", s.ver[target], "{org}", target, "{me}", me, "{uid}", "1").Replace(sql)
}

// relations: every relation of the cloud that holds a document's rows, by the name of its own
// partition, source copy or leftover: a statement can name those as well as the parent.
func (s *xtStand) relations() ([]string, error) {
	return s.e.cloudRows("SELECT quote_ident(relname) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p', 'f', 'm', 'v') AND relname LIKE 'document%' AND relname NOT LIKE '%refs%' ORDER BY 1")
}

// named: the organization named by the application (SET LOCAL lokadb.tenant).
func (s *xtStand) named() error {
	var c *pgconn.PgConn
	defer func() {
		if c != nil {
			c.Close(s.e.ctx)
		}
	}()
	rels, err := s.relations()
	if err != nil {
		return err
	}
	counts := func() (string, error) {
		return one(s.e.ctx, s.e.cloud(), `SELECT (SELECT count(*) FILTER (WHERE organization_id = 1) || '/' || count(*) FILTER (WHERE organization_id = 2) || '/' || count(*) FILTER (WHERE title LIKE 'HACKED%') FROM documents)
			|| ' ' || (SELECT count(*) || '/' || count(*) FILTER (WHERE content LIKE 'HACKED%') FROM document_versions)`)
	}
	before, err := counts()
	if err != nil {
		return err
	}
	for _, me := range []string{"1", "2"} {
		target := xtOther(me)
		mark := xtMark(target)
		for _, sh := range xtReads {
			attack := s.fill(sh.sql, target, me)
			for _, extended := range []bool{false, true} {
				if err := s.session(&c); err != nil {
					return err
				}
				a := s.exec(c, me, extended, attack)
				s.tried++
				s.trace(fmt.Sprintf("%s, under %s, extended %v", sh.name, me, extended), a)
				if strings.Contains(a.text, mark) {
					s.wrong("%s under organization %s: %s returned %q", sh.name, me, attack, xtShort(a.text))
				}
			}
			// the control: the same statement under the organization that owns the row
			if !sh.noControl {
				if err := s.session(&c); err != nil {
					return err
				}
				own := s.fill(sh.sql, target, target)
				if a := s.exec(c, target, false, own); !strings.Contains(a.text, mark) {
					s.wrong("control, %s under its owner %s: %s returned %q (code %s)", sh.name, target, own, xtShort(a.text), a.code)
				}
			}
		}
		for _, rel := range rels {
			attack := "SELECT to_jsonb(t)::text FROM " + rel + " t"
			if err := s.session(&c); err != nil {
				return err
			}
			a := s.exec(c, me, false, attack)
			s.tried++
			s.trace(fmt.Sprintf("the relation %s, under %s", rel, me), a)
			if strings.Contains(a.text, mark) {
				s.wrong("the relation %s by its name under organization %s returned %q", rel, me, xtShort(a.text))
			}
		}
		for _, steps := range xtTamper {
			filled := make([]string, len(steps))
			for i, st := range steps {
				filled[i] = s.fill(st, target, me)
			}
			if err := s.session(&c); err != nil {
				return err
			}
			a := s.exec(c, me, false, filled...)
			s.tried++
			if strings.Contains(a.text, mark) {
				s.wrong("tampering under organization %s: %q returned %q", me, filled, xtShort(a.text))
			}
		}
		for _, w := range xtWrites {
			attack := s.fill(w, target, me)
			if err := s.session(&c); err != nil {
				return err
			}
			s.exec(c, me, false, attack)
			s.tried++
			if now, err := counts(); err != nil {
				return err
			} else if now != before {
				s.wrong("a write under organization %s: %s changed the rows: %s, was %s", me, attack, now, before)
				before = now
			}
		}
		// '*' is for the roles in all_roles alone, and the application's role is not one
		if err := s.session(&c); err != nil {
			return err
		}
		a := s.exec(c, "*", false, "SELECT title FROM documents")
		s.tried++
		if strings.Contains(a.text, "SECRET-O") {
			s.wrong("lokadb.tenant = '*' for the application's role returned %q", xtShort(a.text))
		}
	}
	return nil
}

// unnamed: the organization only in the statement itself.
func (s *xtStand) unnamed() error {
	var c *pgconn.PgConn
	defer func() {
		if c != nil {
			c.Close(s.e.ctx)
		}
	}()
	for _, me := range []string{"1", "2"} {
		target := xtOther(me)
		mark := xtMark(target)
		// names the session's organization, and tries to read more than it: narrow, never widen
		for _, sql := range []string{
			"SELECT title FROM documents WHERE organization_id = {me} UNION ALL SELECT title FROM documents",
			"SELECT d.title, (SELECT string_agg(title, ',') FROM documents) FROM documents d WHERE d.organization_id = {me}",
			"WITH x AS (SELECT * FROM documents) SELECT d.title, x.title FROM documents d, x WHERE d.organization_id = {me}",
			"SELECT d.title, o.title FROM documents d CROSS JOIN documents o WHERE d.organization_id = {me}",
			"SELECT d.title, o.title FROM documents d JOIN documents o ON o.user_id = d.user_id WHERE d.organization_id = {me}",
			"SELECT d.title, v.content FROM documents d JOIN document_versions v ON true WHERE d.organization_id = {me}",
			"SELECT title FROM documents WHERE organization_id = {me} AND title LIKE 'SECRET-O{org}-%'",
			"SELECT title, xt_title({id}) FROM documents WHERE organization_id = {me}",
			"SELECT d.title, t.title FROM documents d, xt_docs t WHERE d.organization_id = {me}",
		} {
			attack := s.fill(sql, target, me)
			for _, extended := range []bool{false, true} {
				if err := s.session(&c); err != nil {
					return err
				}
				a := s.exec(c, "", extended, attack)
				s.tried++
				if strings.Contains(a.text, mark) {
					s.wrong("names organization %s: %s returned %q", me, attack, xtShort(a.text))
				}
			}
		}
	}
	// names no organization: the statement is refused, whatever it hides behind, or reads nothing
	for _, sql := range []string{
		"SELECT title FROM documents",
		"SELECT content FROM document_versions",
		"SELECT d.title FROM users u JOIN documents d ON d.user_id = u.id WHERE u.id = 1",
		"SELECT v.content FROM users u JOIN documents d ON d.user_id = u.id JOIN document_versions v ON v.document_id = d.id WHERE u.id = 1",
		"SELECT x.title FROM users u, LATERAL (SELECT title FROM documents d WHERE d.user_id = u.id) x WHERE u.id = 1",
		"WITH x AS (SELECT * FROM documents) SELECT title FROM x",
		"WITH x(a, o, u, t) AS (SELECT id, organization_id, user_id, title FROM documents) SELECT t FROM x",
		"SELECT t.title FROM (SELECT title FROM documents) t",
		"SELECT (SELECT string_agg(title, ',') FROM documents)",
		"SELECT title FROM documents WHERE organization_id > 0",
		"SELECT title FROM documents WHERE organization_id <> 0",
		"SELECT title FROM documents WHERE title LIKE 'SECRET-%'",
		"SELECT title FROM documents WHERE organization_id = 1 OR title LIKE 'SECRET-%'",
		"SELECT title FROM xt_docs",
		"SELECT xt_title(id) FROM users",
		"COPY documents TO STDOUT",
		"COPY (SELECT title FROM documents) TO STDOUT",
	} {
		for _, extended := range []bool{false, true} {
			if err := s.session(&c); err != nil {
				return err
			}
			a := s.exec(c, "", extended, sql)
			s.tried++
			s.trace(fmt.Sprintf("no organization: %s, extended %v", sql, extended), a)
			if strings.Contains(a.text, "SECRET-O") {
				s.wrong("names no organization: %s returned %q", sql, xtShort(a.text))
			}
		}
	}
	rels, err := s.relations()
	if err != nil {
		return err
	}
	for _, rel := range rels {
		if err := s.session(&c); err != nil {
			return err
		}
		sql := "SELECT to_jsonb(t)::text FROM " + rel + " t"
		a := s.exec(c, "", false, sql)
		s.tried++
		s.trace("no organization: the relation "+rel, a)
		if strings.Contains(a.text, "SECRET-O") {
			s.wrong("names no organization: the relation %s by its name returned %q", rel, xtShort(a.text))
		}
	}
	// what the proxy is not: a statement that names B's row by its id gets it (routing, not
	// authorization). The application decides who may ask. If this changes, the docs must too.
	for _, org := range []string{"1", "2"} {
		if err := s.session(&c); err != nil {
			return err
		}
		sql := s.fill("SELECT title FROM documents WHERE id = {id}", org, xtOther(org))
		a := s.exec(c, "", false, sql)
		s.tried++
		if !strings.Contains(a.text, xtMark(org)) {
			s.wrong("by id with no organization named: %s returned %q (the docs say the proxy routes it)", sql, xtShort(a.text))
		}
	}
	return nil
}

// prepared: one statement prepared once and reused across organizations on one connection, more
// times than Postgres needs to settle on a generic plan, and with the generic plan forced.
func (s *xtStand) prepared() error {
	ctx := s.e.ctx
	c, err := s.e.viaProxy()
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	text := func(name string, params ...string) (string, error) {
		var ps [][]byte
		for _, p := range params {
			ps = append(ps, []byte(p))
		}
		res := c.ExecPrepared(ctx, name, ps, nil, nil).Read()
		if res.Err != nil {
			return " ERROR " + res.Err.Error(), res.Err
		}
		var b strings.Builder
		for _, row := range res.Rows {
			for _, cell := range row {
				b.Write(cell)
				b.WriteByte('\n')
			}
		}
		return b.String(), nil
	}
	for i, mode := range []string{"auto", "force_generic_plan"} {
		if _, err := simple(ctx, c, "SET plan_cache_mode = "+mode); err != nil {
			return fmt.Errorf("plan_cache_mode: %w", err)
		}
		byID := fmt.Sprintf("xt_by_id_%d", i)
		child := fmt.Sprintf("xt_child_%d", i)
		byOrg := fmt.Sprintf("xt_by_org_%d", i)
		join := fmt.Sprintf("xt_join_%d", i)
		cte := fmt.Sprintf("xt_cte_%d", i)
		for name, sql := range map[string]string{
			byID:  "SELECT title FROM documents WHERE id = $1",
			child: "SELECT content FROM document_versions WHERE document_id = $1",
			byOrg: "SELECT title FROM documents WHERE organization_id = $1",
			join:  "SELECT d.title FROM users u JOIN documents d ON d.user_id = u.id WHERE u.id = $1",
			cte:   "WITH x AS (SELECT id, title FROM documents) SELECT title FROM x WHERE id = $1",
		} {
			if _, err := c.Prepare(ctx, name, sql, nil); err != nil {
				return fmt.Errorf("prepare %s: %w", name, err)
			}
		}
		for round := 0; round < 8; round++ {
			for _, me := range []string{"1", "2"} {
				other := xtOther(me)
				if _, err := simple(ctx, c, "BEGIN"); err != nil {
					return err
				}
				if _, err := simple(ctx, c, "SET LOCAL lokadb.tenant = '"+me+"'"); err != nil {
					return err
				}
				// the same statements, the session's own row, then the other's
				for _, call := range []struct {
					name, param string
					ownSees     bool
				}{{byID, s.doc[me], true}, {child, s.doc[me], true}, {join, "1", true}, {cte, s.doc[me], true}, {byID, s.doc[other], false}, {child, s.doc[other], false}, {cte, s.doc[other], false}} {
					got, err := text(call.name, call.param)
					s.tried++
					if err != nil {
						s.wrong("%s, plan %s, round %d, under %s: %v", call.name, mode, round, me, err)
						continue
					}
					if strings.Contains(got, xtMark(other)) {
						s.wrong("%s(%s), plan %s, round %d, under %s returned %q", call.name, call.param, mode, round, me, xtShort(got))
					}
					if call.ownSees && !strings.Contains(got, xtMark(me)) {
						s.wrong("control, %s(%s), plan %s, round %d, under %s returned %q", call.name, call.param, mode, round, me, xtShort(got))
					}
				}
				if _, err := simple(ctx, c, "ROLLBACK"); err != nil {
					return err
				}
				// no name, only the parameter: the proxy finds the organization at each Bind
				got, err := text(byOrg, me)
				s.tried++
				if err != nil {
					s.wrong("%s(%s), plan %s, round %d: %v", byOrg, me, mode, round, err)
				} else if strings.Contains(got, xtMark(other)) || !strings.Contains(got, xtMark(me)) {
					s.wrong("%s(%s), plan %s, round %d returned %q", byOrg, me, mode, round, xtShort(got))
				}
				got, err = text(byID, s.doc[me])
				s.tried++
				if err != nil {
					s.wrong("%s(%s), plan %s, round %d: %v", byID, s.doc[me], mode, round, err)
				} else if strings.Contains(got, xtMark(other)) || !strings.Contains(got, xtMark(me)) {
					s.wrong("%s(%s), plan %s, round %d returned %q", byID, s.doc[me], mode, round, xtShort(got))
				}
			}
		}
	}
	return nil
}

// pooled: the proxy's own pools hand a connection from one client to the next.
func (s *xtStand) pooled() error {
	ctx := s.e.ctx
	a, err := s.e.poolConn("app", "app", "")
	if err != nil {
		return err
	}
	pidA := backendPID(ctx, a)
	for _, sql := range []string{
		"SELECT set_config('app.xt', '" + xtMark("1") + "guc', false)",
		"CREATE TEMP TABLE xt_tmp AS SELECT '" + xtMark("1") + "tmp'::text AS t",
		"PREPARE xt_sql AS SELECT title FROM documents WHERE organization_id = 1",
		"SELECT title FROM documents WHERE organization_id = 1", // lokadb.org = 1 on the server
	} {
		if _, err := a.Exec(ctx, sql); err != nil {
			a.Close(ctx)
			return fmt.Errorf("client of A, %s: %w", sql, err)
		}
	}
	if _, err := a.Prepare(ctx, "xt_named", "SELECT title FROM documents WHERE organization_id = 1"); err != nil {
		a.Close(ctx)
		return err
	}
	a.Close(ctx)
	time.Sleep(500 * time.Millisecond)

	b, err := s.e.poolConn("app", "app", "")
	if err != nil {
		return err
	}
	defer b.Close(ctx)
	s.tried++
	if pidB := backendPID(ctx, b); pidB != pidA {
		s.wrong("the pool was not used: the client of A on process %d, the client of B on %d, so this check proved nothing", pidA, pidB)
	}
	for _, c := range []struct{ what, sql, want string }{
		{"a setting", "SELECT coalesce(current_setting('app.xt', true), '')", ""},
		{"a temp table", "SELECT (to_regclass('pg_temp.xt_tmp') IS NULL)::text", "true"},
		{"the prepared statements", "SELECT count(*)::text FROM pg_prepared_statements WHERE name IN ('xt_sql', 'xt_named')", "0"},
		{"lokadb.tenant", "SELECT coalesce(current_setting('lokadb.tenant', true), '')", ""},
		{"lokadb.org", "SELECT coalesce(current_setting('lokadb.org', true), '')", "-1"}, // the proxy's own "no organization"
	} {
		var got string
		if err := b.QueryRow(ctx, c.sql).Scan(&got); err != nil {
			got = "error: " + firstErrLine(err)
		}
		s.tried++
		if got != c.want {
			s.wrong("%s left to the next client: %q, want %q", c.what, got, c.want)
		}
	}
	// the statement A prepared is gone for B, under its name and in SQL
	var title string
	s.tried++
	if err := b.QueryRow(ctx, "EXECUTE xt_sql").Scan(&title); err == nil {
		s.wrong("EXECUTE of A's statement by the next client returned %q", title)
	}
	// the proxy's own state: B names nobody, so the policy refuses it
	s.tried++
	if err := b.QueryRow(ctx, "SELECT title FROM documents LIMIT 1").Scan(&title); err == nil {
		s.wrong("the next client, naming no organization, read %q", title)
	}
	b.Close(ctx)
	time.Sleep(200 * time.Millisecond)

	// a client gone inside a transaction under A: the connection is not handed on as it is
	open, err := s.e.poolConn("app", "app", "")
	if err != nil {
		return err
	}
	if _, err := open.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	if _, err := open.Exec(ctx, "SET LOCAL lokadb.tenant = '1'"); err != nil {
		return err
	}
	if _, err := open.Exec(ctx, "SELECT title FROM documents"); err != nil {
		return err
	}
	pidOpen := backendPID(ctx, open)
	open.Close(ctx)
	time.Sleep(500 * time.Millisecond)
	next, err := s.e.poolConn("app", "app", "")
	if err != nil {
		return err
	}
	defer next.Close(ctx)
	s.tried++
	if pid := backendPID(ctx, next); pid == pidOpen {
		s.wrong("a connection left inside a transaction went to the next client (process %d)", pid)
	}
	s.tried++
	if err := next.QueryRow(ctx, "SELECT title FROM documents LIMIT 1").Scan(&title); err == nil {
		s.wrong("after a client gone inside a transaction under A, the next client naming nobody read %q", title)
	}

	if s.direct {
		// a direct connection passes from one client to the next, of whichever organization's
		// database it is, and only that database's rows come back. The direct path and its pool must
		// really be in use, or this proves nothing.
		routed := directCount(s.e.proxyRoutes(), "1")
		backends := ""
		for i, org := range []string{"1", "2", "1", "2"} {
			c, err := s.e.poolConn("app", "app", "")
			if err != nil {
				return err
			}
			rows, err := c.Query(ctx, "SELECT title FROM documents WHERE organization_id = "+org)
			if err != nil {
				c.Close(ctx)
				return err
			}
			n := 0
			for rows.Next() {
				var t string
				if err := rows.Scan(&t); err != nil {
					rows.Close()
					c.Close(ctx)
					return err
				}
				n++
				s.tried++
				if !strings.HasPrefix(t, xtMark(org)) {
					s.wrong("the direct path, organization %s, returned %q", org, t)
				}
			}
			rows.Close()
			if n == 0 {
				s.wrong("control: the direct path, organization %s, returned no rows", org)
			}
			if i == 0 {
				backends = s.e.directBackends()
			}
			if i == 2 {
				s.tried++
				if now := s.e.directBackends(); backends == "" || now != backends {
					s.wrong("the direct pool was not reused: processes %q, then %q", backends, now)
				}
			}
			c.Close(ctx)
			time.Sleep(200 * time.Millisecond)
		}
		s.tried++
		if directCount(s.e.proxyRoutes(), "1") <= routed {
			s.wrong("organization 1's statements did not take the direct path, so this check proved nothing about it")
		}
	}
	return nil
}

// stress: clients of both organizations at once on the proxy's pools, connecting and disconnecting
// so that connections keep passing between them, in every way of naming the organization.
func (s *xtStand) stress() error {
	ctx, cancel := context.WithTimeout(s.e.ctx, 20*time.Second)
	defer cancel()
	var reads, leaks, errs atomic.Int64
	var mu sync.Mutex
	var first []string
	report := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		if len(first) < 5 {
			first = append(first, fmt.Sprintf(format, args...))
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < 16; w++ {
		me := "1"
		if w%2 == 1 {
			me = "2"
		}
		other := xtOther(me)
		wg.Add(1)
		go func(w int, me, other string) {
			defer wg.Done()
			for ctx.Err() == nil {
				c, err := s.e.viaProxy()
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
						report("connect: %v", err)
					}
					return
				}
				const name = "xt_stress"
				if _, err := c.Prepare(s.e.ctx, name, "SELECT title FROM documents WHERE id = $1", nil); err != nil {
					errs.Add(1)
					report("prepare: %v", err)
					c.Close(s.e.ctx)
					return
				}
				for i := 0; i < 5 && ctx.Err() == nil; i++ {
					var got string
					var err error
					switch i {
					case 0: // in the statement
						r := c.ExecParams(s.e.ctx, "SELECT title FROM documents WHERE organization_id = $1", [][]byte{[]byte(me)}, nil, nil, nil).Read()
						got, err = xtCells(r.Rows), r.Err
					case 1: // by the id, prepared
						r := c.ExecPrepared(s.e.ctx, name, [][]byte{[]byte(s.doc[me])}, nil, nil).Read()
						got, err = xtCells(r.Rows), r.Err
					case 2: // named, no condition
						a := s.exec(c, me, false, "SELECT title FROM documents")
						got = a.text
						if a.code != "" {
							err = fmt.Errorf("%s", xtShort(a.text))
						}
					case 3: // named, a join through users
						a := s.exec(c, me, false, "SELECT d.title FROM users u JOIN documents d ON d.user_id = u.id WHERE u.id = 1")
						got = a.text
						if a.code != "" {
							err = fmt.Errorf("%s", xtShort(a.text))
						}
					case 4: // named, the other's id: nothing
						a := s.exec(c, me, false, "WITH x AS (SELECT id, title FROM documents) SELECT title FROM x WHERE id = "+s.doc[other])
						if a.code != "" {
							err = fmt.Errorf("%s", xtShort(a.text))
						}
						got = a.text + xtMark(me) // reads nothing by design; only the other's mark counts
					}
					reads.Add(1)
					if err != nil {
						errs.Add(1)
						report("worker %d of organization %s, form %d: %v", w, me, i, err)
						continue
					}
					if strings.Contains(got, xtMark(other)) {
						leaks.Add(1)
						report("worker %d of organization %s, form %d got %q", w, me, i, xtShort(got))
					}
					if !strings.Contains(got, xtMark(me)) {
						errs.Add(1)
						report("control: worker %d of organization %s, form %d got %q", w, me, i, xtShort(got))
					}
				}
				c.Close(s.e.ctx)
			}
		}(w, me, other)
	}
	wg.Wait()
	s.tried += int(reads.Load())
	if leaks.Load() > 0 || errs.Load() > 0 || reads.Load() < 100 {
		s.wrong("%d reads, %d of the other organization, %d errors: %s", reads.Load(), leaks.Load(), errs.Load(), strings.Join(first, "; "))
	}
	return nil
}

func xtCells(rows [][][]byte) string {
	var b strings.Builder
	for _, r := range rows {
		for _, cell := range r {
			b.Write(cell)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
