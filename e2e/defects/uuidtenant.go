package main

// Block 18 (tasks/uuid-tenant.md): organizations numbered by uuid. The organization column
// holds the organization's value V, the organization for rows; lokadb's own number N (org-add)
// names its partitions and its database.

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "UT1", name: "check: an organization column of type uuid is taken (no tenant_column_type); kinds mixed are a spec error, rows of the nil uuid a blocker", run: checkUUIDTenantReport},
		check{id: "UT2", name: "enable with uuid organizations: partitions by value, the policy by lokadb.org (one organization, rows of none, nothing, all), a child under its parent's organization, org-add", run: checkUUIDTenantEnable},
		check{id: "UT3", name: "the proxy (fanout reject) routes by a uuid organization: literals in any form, binary and text parameters, uuid[], IN, a child by its FK; inserts land under their organization", run: checkUUIDTenantRouting},
		check{id: "UT4", name: "the cooperative mode with a uuid organization: SET LOCAL lokadb.tenant names it, a statement of another one is refused", run: checkUUIDTenantCoop},
		check{id: "UT5", name: "enable -prepare with uuid organizations: a child's organization filled in batches, then the switch", run: checkUUIDTenantPrepare},
		check{id: "UT6", name: "move-org of an organization numbered by uuid (org-add first) while both organizations write through the proxy; its rows there, the proxy reads, writes and upserts it there", run: checkUUIDTenantMove},
		check{id: "UT7", name: "repoint, move-back and disable of an organization numbered by uuid", run: checkUUIDTenantBack},
		check{id: "UT8", name: "refs-diff and refs-sync of an organization numbered by uuid: rows written and deleted past the cloud", run: checkUUIDTenantRefs},
		check{id: "UT10", name: "the cooperative mode by set_config in a transaction (pgx, the extended protocol) names an account by its uuid: the same rows with the profile fdw and fdw+direct, never refused as a fanout", run: checkUUIDTenantCoopDirect},
		check{id: "UT11", name: "the cooperative mode in a direct transaction of an account numbered by uuid: SET LOCAL lokadb.tenant after BEGIN, statements without a condition of their own; learnt through the cloud, then both statements in its database", run: checkUUIDTenantCoopTx},
		check{id: "UT9", name: "fdw+direct of an organization numbered by uuid: reads, an UPDATE, INSERTs (a key from the cloud's DEFAULT, a key given), a DELETE and a learnt transaction go straight to its database, __refs following", run: checkUUIDTenantDirect},
	)
}

// the organizations of the UT checks
const (
	orgA = "aaaaaaaa-0000-4000-8000-000000000001"
	orgB = "bbbbbbbb-0000-4000-8000-000000000002"
)

// uuidTenantSpec: projects of organizations numbered by uuid (account_id), tasks under them.
const uuidTenantSpec = `{"tables": [
  {"name": "projects", "tenant_column": "account_id"},
  {"name": "tasks", "parent_table": "projects", "parent_column": "project_id"}
]}`

// uuidTenantApp: accounts A and B (a cloud table), 10 projects each and one of no
// organization (account_id NULL), a task under every project.
func uuidTenantApp(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		"CREATE TABLE accounts (id uuid PRIMARY KEY, name text)",
		"CREATE TABLE projects (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), account_id uuid REFERENCES accounts, name text)",
		"CREATE TABLE tasks (id bigserial PRIMARY KEY, project_id uuid REFERENCES projects ON DELETE CASCADE, title text)",
		"ALTER TABLE accounts OWNER TO app", "ALTER TABLE projects OWNER TO app", "ALTER TABLE tasks OWNER TO app",
		"INSERT INTO accounts VALUES ('"+orgA+"', 'A'), ('"+orgB+"', 'B')",
		"INSERT INTO projects (account_id, name) SELECT CASE WHEN g % 2 = 0 THEN '"+orgA+"'::uuid ELSE '"+orgB+"'::uuid END, 'p' || g FROM generate_series(1, 20) g",
		"INSERT INTO projects (account_id, name) VALUES (NULL, 'nobody')",
		"INSERT INTO tasks (project_id, title) SELECT id, 't-' || name FROM projects")
}

// asApp: one value of sql in the cloud as the application's role (the policy applies) under
// lokadb.org = org ("" for none set).
func asApp(e *env, org, sql string) string {
	set := "SELECT set_config('lokadb.org', '" + org + "', false)"
	c, err := pgx.Connect(e.ctx, dsn(e.cloudHost, e.db, "app"))
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	defer c.Close(e.ctx)
	if _, err := c.Exec(e.ctx, set); err != nil {
		return "error: " + firstErrLine(err)
	}
	var v any
	if err := c.QueryRow(e.ctx, sql).Scan(&v); err != nil {
		return "error: " + firstErrLine(err)
	}
	return fmt.Sprint(v)
}

// UT1. check gave tenant_column_type ("lokadb supports integer organizations only so far").
func checkUUIDTenantReport(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	x := &expects{ok: true}
	out, _ := e.checkWith(uuidTenantSpec)
	x.eq("tenant_column_type", fmt.Sprint(reported(out, "tenant_column_type")), "false")
	x.eq("tenant_value_reserved without such rows", fmt.Sprint(reported(out, "tenant_value_reserved")), "false")
	// an integer organization column next to the uuid ones
	if err := execSQL(e.ctx, e.cloud(), "CREATE TABLE legacy (id bigserial PRIMARY KEY, org_id bigint NOT NULL)", "ALTER TABLE legacy OWNER TO app"); err != nil {
		return false, "", err
	}
	out, _ = e.checkWith(`{"tables": [{"name": "projects", "tenant_column": "account_id"}, {"name": "legacy", "tenant_column": "org_id"}]}`)
	x.eq("kinds mixed: tenant_kind_mixed", fmt.Sprint(reported(out, "tenant_kind_mixed")), "true")
	// rows under the nil uuid
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO accounts VALUES ('00000000-0000-0000-0000-000000000000', 'nil')",
		"INSERT INTO projects (account_id, name) VALUES ('00000000-0000-0000-0000-000000000000', 'nil')"); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(uuidTenantSpec)
	x.eq("rows of the nil uuid: tenant_value_reserved, exit", fmt.Sprint(reported(out, "tenant_value_reserved"), " ", code), "true 3")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT2. enable made LIST (coalesce(account_id, -2)) of a uuid column and failed.
func checkUUIDTenantEnable(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidTenantSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	x := &expects{ok: true}
	x.eq("the partition of rows of none", query(e, e.cloud(), "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'projects__none'"),
		"FOR VALUES IN ('00000000-0000-0000-0000-000000000000')")
	x.eq("the refs table's organization", query(e, e.cloud(), "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'projects__refs'::regclass AND attname = 'tenant'"), "uuid")
	x.eq("the child's organization column", query(e, e.cloud(), "SELECT format_type(atttypid, atttypmod) || ' ' || pg_get_expr(adbin, adrelid) FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE a.attrelid = 'tasks'::regclass AND a.attname = 'account_id'"),
		"uuid lokadb.org_default_uuid()")
	x.eq("tasks under their project's organization", query(e, e.cloud(), "SELECT count(*) FROM tasks t JOIN projects p ON p.id = t.project_id WHERE t.account_id IS DISTINCT FROM p.account_id"), "0")
	// the policy, as the application's role
	x.eq("lokadb.org = A (upper case): projects, tasks", asApp(e, strings.ToUpper(orgA), "SELECT (SELECT count(*) FROM projects)::text || ', ' || (SELECT count(*) FROM tasks)"), "10, 10")
	x.eq("lokadb.org = -2: rows of none", asApp(e, "-2", "SELECT (SELECT count(*) FROM projects)::text || ' ' || (SELECT string_agg(name, ',') FROM projects)"), "1 nobody")
	x.eq("lokadb.org = -1: nothing", asApp(e, "-1", "SELECT count(*) FROM projects"), "0")
	x.eq("lokadb.org unset: every one", asApp(e, "", "SELECT count(*) FROM projects"), "21")
	// a child under its parent's organization, a parent of another one refused
	pa, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p2'")
	if err != nil {
		return false, "", err
	}
	pb, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p1'")
	if err != nil {
		return false, "", err
	}
	x.eq("a task under A's project, under A", asApp(e, orgA, "WITH t AS (INSERT INTO tasks (project_id, title) VALUES ('"+pa+"', 'new') RETURNING account_id) SELECT account_id::text FROM t"), orgA)
	got := asApp(e, orgA, "WITH t AS (INSERT INTO tasks (project_id, title) VALUES ('"+pb+"', 'wrong') RETURNING 1) SELECT count(*) FROM t")
	x.eq("a task under B's project, under A: refused", fmt.Sprint(strings.Contains(got, "SQLSTATE 23514")), "true")
	got = asApp(e, orgA, "SELECT lokadb.same_org('"+orgB+"'::uuid, '"+orgA+"'::uuid)::text")
	x.eq("same_org(B, A): refused", fmt.Sprint(strings.Contains(got, "SQLSTATE 0A000")), "true")
	// org-add: numbers once, refuses the nil uuid
	a1, _ := e.lokadb("org-add", "-db", e.cloud(), "-tenant", strings.ToUpper(orgA))
	a2, _ := e.lokadb("org-add", "-db", e.cloud(), "-tenant", orgA)
	_, nilErr := e.lokadb("org-add", "-db", e.cloud(), "-tenant", "00000000-0000-0000-0000-000000000000")
	x.eq("org-add A twice", strings.TrimSpace(lastLine(a1))+" | "+strings.TrimSpace(lastLine(a2)), "organization 1: "+strings.ToUpper(orgA)+" | organization 1: "+orgA)
	x.eq("org-add of the nil uuid refused", fmt.Sprint(nilErr != nil), "true")
	x.eq("lokadb.tenants", query(e, e.cloud(), "SELECT string_agg(org || '=' || tenant, ',') FROM lokadb.tenants"), "1="+orgA)
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT3. The proxy read an organization column's value as an integer only.
func checkUUIDTenantRouting(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidTenantSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
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
	x := &expects{ok: true}
	lit := func(what, sql, want string) {
		res, err := simple(e.ctx, c, sql)
		v := ""
		switch {
		case err != nil:
			v = "error: " + firstErrLine(err)
		case len(res) > 0 && len(res[0].Rows) > 0:
			v = string(res[0].Rows[0][0])
		}
		x.eq(what, v, want)
	}
	param := func(what, sql string, p []byte, oid uint32, format int16, want string) {
		res := c.ExecParams(e.ctx, sql, [][]byte{p}, []uint32{oid}, []int16{format}, nil).Read()
		v := ""
		switch {
		case res.Err != nil:
			v = "error: " + firstErrLine(res.Err)
		case len(res.Rows) > 0:
			v = string(res.Rows[0][0])
		}
		x.eq(what, v, want)
	}
	// counts that name no organization but by account_id: a fanout would be refused (reject)
	lit("literal, upper case", "SELECT count(*) FROM projects WHERE account_id = '"+strings.ToUpper(orgA)+"' AND name LIKE 'p%'", "10")
	lit("literal without hyphens", "SELECT count(*) FROM projects WHERE account_id = '"+strings.ReplaceAll(orgA, "-", "")+"'", "10")
	lit("IN", "SELECT count(*) FROM projects WHERE account_id IN ('"+orgA+"')", "10")
	lit("= ANY of a uuid[] literal", "SELECT count(*) FROM projects WHERE account_id = ANY('{"+orgA+"}'::uuid[])", "10")
	param("binary parameter", "SELECT count(*) FROM projects WHERE account_id = $1", uuidBytes(orgB), 2950, 1, "10")
	param("text parameter", "SELECT count(*) FROM projects WHERE account_id = $1", []byte(strings.ToUpper(orgB)), 0, 0, "10")
	param("binary uuid[] parameter", "SELECT count(*) FROM projects WHERE account_id = ANY($1)", uuidArray(orgB), 2951, 1, "10")
	pa, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p2'")
	if err != nil {
		return false, "", err
	}
	// only the FK onto projects names the organization
	param("tasks by their project (the FK)", "SELECT count(*) FROM tasks WHERE project_id = $1", uuidBytes(pa), 2950, 1, "1")
	param("a task inserted under its project's organization", "INSERT INTO tasks (project_id, title) VALUES ($1, 'via proxy') RETURNING account_id::text", uuidBytes(pa), 2950, 1, orgA)
	lit("a project inserted by its account_id", "INSERT INTO projects (account_id, name) VALUES ('"+strings.ToUpper(orgB)+"', 'via proxy') RETURNING account_id::text", orgB)
	_, refErr := simple(e.ctx, c, "SELECT count(*) FROM projects WHERE name LIKE 'p%'")
	x.eq("a statement of no organization refused", fmt.Sprint(refErr != nil), "true")
	x.eq("in the cloud: A's tasks, B's projects", query(e, e.cloud(), "SELECT (SELECT count(*) FROM tasks WHERE account_id = '"+orgA+"')::text || ', ' || (SELECT count(*) FROM projects WHERE account_id = '"+orgB+"')"), "11, 11")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT4. lokadb.tenant took an integer only ("must be an organization id or '*'").
func checkUUIDTenantCoop(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidTenantSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
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
	x := &expects{ok: true}
	// run: each statement a query of its own, in one transaction; the value of the third
	run := func(sqls ...string) string {
		got := ""
		for i, sql := range sqls {
			res, err := simple(e.ctx, c, sql)
			if err != nil {
				_, _ = simple(e.ctx, c, "ROLLBACK")
				return "error: " + firstErrLine(err) + " (" + pgCode(err) + ")"
			}
			if i == 2 && len(res) > 0 && len(res[0].Rows) > 0 {
				got = string(res[0].Rows[0][0])
			}
		}
		return got
	}
	// a condition with no lookup (name LIKE): under the named organization only
	x.eq("SET LOCAL lokadb.tenant = A (upper case), then an unhinted count", run("BEGIN", "SET LOCAL lokadb.tenant = '"+strings.ToUpper(orgA)+"'", "SELECT count(*) FROM projects WHERE name LIKE 'p%'", "COMMIT"), "10")
	got := run("BEGIN", "SET LOCAL lokadb.tenant = '"+orgA+"'", "SELECT count(*) FROM projects WHERE account_id = '"+orgB+"'", "COMMIT")
	x.eq("a statement naming B under A refused", fmt.Sprint(strings.Contains(got, "42501")), "true")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT5. enable -prepare filled a child's organization column as bigint and the switch made
// LIST (coalesce(account_id, -2)).
func checkUUIDTenantPrepare(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO projects (account_id, name) SELECT CASE WHEN g % 2 = 0 THEN '"+orgA+"'::uuid ELSE '"+orgB+"'::uuid END, 'bulk' FROM generate_series(1, 12000) g",
		"INSERT INTO tasks (project_id, title) SELECT id, 'bulk' FROM projects WHERE name = 'bulk'"); err != nil {
		return false, "", err
	}
	p, err := e.specFile(uuidTenantSpec)
	if err != nil {
		return false, "", err
	}
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app", "-prepare"); err != nil {
		return false, "enable -prepare failed: " + lastErr(out), nil
	}
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app"); err != nil {
		return false, "the switch failed: " + lastErr(out), nil
	}
	x := &expects{ok: true}
	x.eq("tasks whose organization differs from their project's", query(e, e.cloud(), "SELECT count(*) FROM tasks t LEFT JOIN projects p ON p.id = t.project_id WHERE t.account_id IS DISTINCT FROM p.account_id"), "0")
	x.eq("projects__refs differs from projects", query(e, e.cloud(), "SELECT count(*) FROM projects d FULL JOIN projects__refs r ON r.id = d.id WHERE r.id IS NULL OR d.id IS NULL OR r.tenant IS DISTINCT FROM d.account_id"), "0")
	x.eq("A's projects under the policy", asApp(e, orgA, "SELECT count(*) FROM projects"), "6010")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// uuidTenantMoved: the UT app enabled, organization A numbered 1 (org-add) and moved (extra:
// move-org's further arguments); why it could not be, "" when it was.
func uuidTenantMoved(e *env, extra ...string) (string, error) {
	if err := uuidTenantApp(e); err != nil {
		return "", err
	}
	e.spec = uuidTenantSpec
	if err := e.enable(); err != nil {
		return "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.lokadb("org-add", "-db", e.cloud(), "-tenant", orgA); err != nil {
		return "org-add failed: " + lastErr(out), nil
	}
	if out, err := e.move(1, extra...); err != nil {
		return "move-org failed: " + lastErr(out), nil
	}
	return "", nil
}

// proxyValue: the first value of sql's answer through the proxy connection c.
func proxyValue(e *env, c *pgconn.PgConn, sql string) string {
	res, err := simple(e.ctx, c, sql)
	switch {
	case err != nil:
		return "error: " + firstErrLine(err)
	case len(res) > 0 && len(res[0].Rows) > 0:
		return string(res[0].Rows[0][0])
	}
	return ""
}

// UT6. move-org refused an organization numbered by uuid (block 18, merge I: "moving one is
// not supported by this version yet"); before block 18 enable failed on its column.
func checkUUIDTenantMove(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidTenantSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	x := &expects{ok: true}
	out, err := e.move(1)
	x.eq("move-org before org-add: refused, org-add named", fmt.Sprint(err != nil && strings.Contains(out, "org-add")), "true")
	out, err = e.lokadb(e.moveArgs(0, "-tenant", orgB)...)
	x.eq("move-org -tenant of an account without a number: refused, org-add named", fmt.Sprint(err != nil && strings.Contains(out, "org-add -tenant "+orgB)), "true")
	if out, err := e.lokadb("org-add", "-db", e.cloud(), "-tenant", orgA); err != nil {
		return false, "org-add failed: " + lastErr(out), nil
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// Both organizations write through the proxy while A moves: the pause holds A's writes
	// (move-org takes A's lock by its value, as the proxy does), B's go on. A writer of A
	// the pause misses meets the CHECK on DEFAULT during the split.
	done := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	wrote := map[string]int{}
	var writeErrs []string
	for _, org := range []string{orgA, orgB} {
		wg.Add(1)
		go func(org string) {
			defer wg.Done()
			c, err := e.viaProxy()
			if err != nil {
				mu.Lock()
				writeErrs = append(writeErrs, firstErrLine(err))
				mu.Unlock()
				return
			}
			defer c.Close(e.ctx)
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				_, err := simple(e.ctx, c, fmt.Sprintf("INSERT INTO projects (account_id, name) VALUES ('%s', 'w%d')", org, i))
				mu.Lock()
				if err != nil {
					writeErrs = append(writeErrs, firstErrLine(err))
				} else {
					wrote[org]++
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
			}
		}(org)
	}
	out, merr := e.move(1)
	close(done)
	wg.Wait()
	if merr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	x.eq("writes through the proxy during the move that failed", fmt.Sprint(len(writeErrs), " ", strings.Join(writeErrs[:min(len(writeErrs), 2)], " | ")), "0 ")
	x.eq("writes of both organizations during the move", fmt.Sprint(wrote[orgA] > 0, " ", wrote[orgB] > 0), "true true")
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	x.eq("A's projects there, nothing else", query(e, e.customer(), fmt.Sprintf("SELECT count(*)::text || ' ' || count(*) FILTER (WHERE account_id <> '%s') FROM %s.projects", orgA, sch)),
		fmt.Sprintf("%d 0", 10+wrote[orgA]))
	x.eq("A's tasks there", query(e, e.customer(), fmt.Sprintf("SELECT count(*)::text || ' ' || count(*) FILTER (WHERE account_id = '%s') FROM %s.tasks", orgA, sch)), "10 10")
	x.eq("the foreign partition by A's value", query(e, e.cloud(), "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'projects__org1'"), "FOR VALUES IN ('"+orgA+"')")
	x.eq("B's projects in the cloud", query(e, e.cloud(), "SELECT count(*) FROM ONLY projects__cloud WHERE account_id = '"+orgB+"'"), fmt.Sprint(10+wrote[orgB]))
	x.eq("the policy: lokadb.org = A reads there", asApp(e, orgA, "SELECT count(*) FROM projects"), fmt.Sprint(10+wrote[orgA]))
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x.eq("via the proxy: A's projects", proxyValue(e, c, "SELECT count(*) FROM projects WHERE account_id = '"+strings.ToUpper(orgA)+"'"), fmt.Sprint(10+wrote[orgA]))
	pa, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p2'")
	if err != nil {
		return false, "", err
	}
	x.eq("via the proxy: a task under A's project, there", proxyValue(e, c, "INSERT INTO tasks (project_id, title) VALUES ('"+pa+"', 'ut6') RETURNING account_id::text"), orgA)
	// an upsert of a moved organization keeps lokadb's rewrite (a foreign table has no ON
	// CONFLICT DO UPDATE); taken for one of the cloud it went to DEFAULT and was refused
	x.eq("via the proxy: an upsert of A's project", proxyValue(e, c, "INSERT INTO projects (id, account_id, name) VALUES ('"+pa+"', '"+orgA+"', 'ut6-upsert') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING name"), "ut6-upsert")
	x.eq("the upsert there", query(e, e.customer(), fmt.Sprintf("SELECT name FROM %s.projects WHERE id = '%s'", sch, pa)), "ut6-upsert")
	// one of B's, in the cloud: its own partition or DEFAULT, natively, kept to B by its value
	pb, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p1'")
	if err != nil {
		return false, "", err
	}
	x.eq("via the proxy: an upsert of B's project", proxyValue(e, c, "INSERT INTO projects (id, account_id, name) VALUES ('"+pb+"', '"+orgB+"', 'ut6-b') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING name"), "ut6-b")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT7. repoint, move-back and disable of an organization numbered by uuid: move-org refused
// to move one at all.
func checkUUIDTenantBack(e *env) (bool, string, error) {
	if why, err := uuidTenantMoved(e); err != nil || why != "" {
		return false, why, err
	}
	x := &expects{ok: true}
	// repoint (under A's pause, its lock by value): a restored copy with a change of its own
	cp, err := e.restoredCopy()
	if err != nil {
		return false, "", err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, cp, fmt.Sprintf("UPDATE %s.projects SET name = 'only in the copy' WHERE name = 'p2'", sch)); err != nil {
		return false, "", err
	}
	// by its uuid instead of its number (-tenant: a lookup in lokadb.tenants)
	if out, err := e.repoint(map[string]any{}, "-tenant", strings.ToUpper(orgA), "-customer-app", cp); err != nil {
		return false, "repoint failed: " + lastErr(out), nil
	}
	x.eq("repoint: the cloud reads the copy", query(e, e.cloud(), "SELECT count(*) FROM projects WHERE account_id = '"+orgA+"' AND name = 'only in the copy'"), "1")
	// move-back from the copy
	if out, err := e.lokadb(e.withCustomer(e.moveBackArgs(1), cp)...); err != nil {
		return false, "move-back failed: " + lastErr(out), nil
	}
	x.eq("move-back: A's rows in its partition of the cloud", query(e, e.cloud(), `SELECT count(*)::text || ' ' || pg_get_expr(c.relpartbound, c.oid) FROM projects p JOIN pg_class c ON c.oid = p.tableoid
		WHERE p.account_id = '`+orgA+`' GROUP BY c.oid`), "10 FOR VALUES IN ('"+orgA+"')")
	x.eq("move-back: the copy's change", query(e, e.cloud(), "SELECT count(*) FROM projects WHERE name = 'only in the copy'"), "1")
	x.eq("move-back: A's tasks under A", query(e, e.cloud(), "SELECT count(*) FROM tasks WHERE account_id = '"+orgA+"'"), "10")
	x.eq("move-back: lokadb.orgs, servers and foreign tables left", query(e, e.cloud(), `SELECT (SELECT count(*) FROM lokadb.orgs) + (SELECT count(*) FROM pg_foreign_server WHERE srvname LIKE 'lokadb\_org%')
		+ (SELECT count(*) FROM pg_class WHERE relkind = 'f' AND relname LIKE '%\_\_org1%')`), "0")
	x.eq("the policy after move-back: lokadb.org = A", asApp(e, orgA, "SELECT count(*) FROM projects"), "10")
	// disable: plain tables again, every row kept
	if out, err := e.lokadb(e.disableArgs()...); err != nil {
		return false, "disable failed: " + lastErr(out), nil
	}
	x.eq("disable: projects, tasks, the table's kind, schema lokadb gone, the child's added column", query(e, e.cloud(), `SELECT (SELECT count(*) FROM projects)::text || ' ' || (SELECT count(*) FROM tasks)
		|| ' ' || (SELECT relkind::text FROM pg_class WHERE oid = 'projects'::regclass) || ' ' || (to_regnamespace('lokadb') IS NULL)::text
		|| ' ' || (SELECT count(*) FROM pg_attribute WHERE attrelid = 'tasks'::regclass AND attname = 'account_id' AND NOT attisdropped)`), "21 21 r true 0")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT8. refs-diff and refs-sync of an organization numbered by uuid: they compared the refs
// with the organization's number (r.tenant = 1), and move-org refused it before.
func checkUUIDTenantRefs(e *env) (bool, string, error) {
	if why, err := uuidTenantMoved(e); err != nil || why != "" {
		return false, why, err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	const added = "cccccccc-0000-4000-8000-000000000003"
	gone, err := one(e.ctx, e.customer(), fmt.Sprintf("SELECT id::text FROM %s.projects WHERE name = 'p4'", sch))
	if err != nil {
		return false, "", err
	}
	// past the cloud, in A's database: a project added, another deleted (its task with it)
	if err := execSQL(e.ctx, e.customer(),
		fmt.Sprintf("INSERT INTO %s.projects (id, account_id, name) VALUES ('%s', '%s', 'past the cloud')", sch, added, orgA),
		fmt.Sprintf("DELETE FROM %s.projects WHERE id = '%s'", sch, gone)); err != nil {
		return false, "", err
	}
	x := &expects{ok: true}
	diff, dcode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	x.eq("refs-diff: exit, the row added, the row deleted", fmt.Sprint(dcode, " ", strings.Contains(diff, "1 row(s) written past the cloud, not in the refs table (ids "+added+")"),
		" ", strings.Contains(diff, "1 id(s) of rows the organization no longer has (ids "+gone+")")), "2 true true")
	if dcode != 2 {
		x.notes = append(x.notes, oneLine(diff))
	}
	sync, scode, err := e.schemaCmd("refs-sync", "-delete-stale")
	if err != nil {
		return false, "", err
	}
	after, acode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	x.eq("refs-sync -delete-stale, then refs-diff: exits", fmt.Sprint(scode, " ", acode), "0 0")
	if scode != 0 || acode != 0 {
		x.notes = append(x.notes, oneLine(sync+after))
	}
	x.eq("the added project's refs row under A", query(e, e.cloud(), "SELECT tenant::text FROM projects__refs WHERE id = '"+added+"'"), orgA)
	x.eq("the deleted project's refs row", query(e, e.cloud(), "SELECT count(*) FROM projects__refs WHERE id = '"+gone+"'"), "0")
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
	x.eq("via the proxy: the added project by its key (the refs)", proxyValue(e, c, "SELECT name FROM projects WHERE id = '"+added+"'"), "past the cloud")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT9. The direct path took an organization by an integer only (and org-profile refused
// fdw+direct for one numbered by uuid): its statements went the cloud's way. pgx sends the
// uuids binary.
func checkUUIDTenantDirect(e *env) (bool, string, error) {
	if why, err := uuidTenantMoved(e, "-customer-app", e.appCustomer()); err != nil || why != "" {
		return false, why, err
	}
	// the proxy's config by the organization's number, as for any organization
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout":        map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-tenant", orgA, "-customer-app", dsn("customer", e.db, "lokadb_app"),
		"-direct-role", "lokadb_dml", "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	pa, err := one(e.ctx, e.cloud(), "SELECT id::text FROM projects WHERE name = 'p2'")
	if err != nil {
		return false, "", err
	}
	bin := func(u string) [16]byte {
		var b [16]byte
		copy(b[:], uuidBytes(u))
		return b
	}
	a, p := bin(orgA), bin(pa)
	c, err := e.viaProxyPgx("ut9")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x := &expects{ok: true}
	step := func(what, want, sql string, args ...any) []string {
		n := directCount(e.proxyRoutes(), orgA)
		rows := rowsText(e, c, sql, args...)
		x.eq(what+" on the direct path", fmt.Sprint(directCount(e.proxyRoutes(), orgA)-n), want)
		return rows
	}
	x.eq("A's projects", strings.Join(step("A's projects by account_id", "1", "SELECT count(*) FROM projects WHERE account_id = $1", a), ","), "10,tag: SELECT 1")
	x.eq("a project's tasks", strings.Join(step("tasks by their project (the FK)", "1", "SELECT count(*) FROM tasks WHERE project_id = $1", p), ","), "1,tag: SELECT 1")
	// Writes go to tasks: a project's INSERT or UPDATE goes the cloud's way whatever its
	// organization (its FK onto accounts, a cloud table, is checked by lokadb_fk_* there).
	tk, err := one(e.ctx, e.cloud(), "SELECT t.id::text FROM tasks t JOIN projects p ON p.id = t.project_id WHERE p.name = 'p2'")
	if err != nil {
		return false, "", err
	}
	var task int64
	fmt.Sscan(tk, &task)
	step("UPDATE of a task by its key", "1", "UPDATE tasks SET title = 'ut9' WHERE id = $1", task)
	ins := step("INSERT without a key (the cloud's sequence)", "1", "INSERT INTO tasks (project_id, title) VALUES ($1, 'ut9-new') RETURNING id::text", p)
	step("INSERT with a key given", "1", "INSERT INTO tasks (id, project_id, title) VALUES (900009, $1, 'ut9-given')", p)
	x.eq("there: the update, the new tasks under A", query(e, e.customer(), fmt.Sprintf(
		"SELECT (SELECT title FROM %[1]s.tasks WHERE id = %[2]s) || ', ' || (SELECT count(*) FROM %[1]s.tasks WHERE title IN ('ut9-new', 'ut9-given') AND account_id = '%[3]s')",
		sch, tk, orgA)), "ut9, 2")
	if len(ins) == 2 {
		x.eq("the refs of the new tasks: under A", query(e, e.cloud(), "SELECT string_agg(tenant::text, ',' ORDER BY id) FROM tasks__refs WHERE id IN ("+ins[0]+", 900009)"), orgA+","+orgA)
	}
	step("DELETE of the new tasks", "1", "DELETE FROM tasks WHERE project_id = $1 AND title IN ('ut9-new', 'ut9-given')", p)
	x.eq("after the DELETE: there, in the refs", query(e, e.customer(), fmt.Sprintf("SELECT count(*) FROM %s.tasks WHERE title IN ('ut9-new', 'ut9-given')", sch))+", "+
		query(e, e.cloud(), "SELECT count(*) FROM tasks__refs WHERE id = 900009"), "0, 0")
	// a transaction: learnt through the cloud twice, then on the direct path
	tx := func(mark string) (int, error) {
		n := directCount(e.proxyRoutes(), orgA)
		t, err := c.Begin(e.ctx)
		if err != nil {
			return 0, err
		}
		if _, err := t.Exec(e.ctx, "UPDATE tasks SET title = $1 WHERE id = $2", mark, task); err != nil {
			_ = t.Rollback(e.ctx)
			return 0, err
		}
		if _, err := t.Exec(e.ctx, "INSERT INTO tasks (project_id, title) VALUES ($1, $2)", p, mark+"-i"); err != nil {
			_ = t.Rollback(e.ctx)
			return 0, err
		}
		err = t.Commit(e.ctx)
		return directCount(e.proxyRoutes(), orgA) - n, err
	}
	var direct []string
	for _, m := range []string{"ut9-t1", "ut9-t2", "ut9-t3"} {
		n, err := tx(m)
		if err != nil {
			direct = append(direct, "error: "+firstErrLine(err))
			continue
		}
		direct = append(direct, fmt.Sprint(n))
	}
	x.eq("a transaction three times: statements on the direct path", strings.Join(direct, " "), "0 0 2")
	x.eq("the transactions there", query(e, e.customer(), fmt.Sprintf("SELECT (SELECT title FROM %[1]s.tasks WHERE id = %[2]s) || ', ' || (SELECT count(*) FROM %[1]s.tasks WHERE title LIKE 'ut9-t_-i')", sch, tk)), "ut9-t3, 3")
	if !x.ok {
		x.notes = append(x.notes, e.proxyLines("msg=route"))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UT10. DP21 for an organization numbered by uuid: pgx Begin, set_config('lokadb.tenant', $1,
// true) with the account's uuid by the extended protocol (prepared while the BEGIN is
// deferred), a count of its projects without a condition on the account: with the profile
// fdw and with fdw+direct the same count, no 0A000. On main 0606ff7 the count was refused as
// a fanout with fdw+direct (the prepare's Sync forgot lokadb.tenant), whatever kind the
// organization was. Such a transaction begins with a statement the direct path does not take,
// so it runs through the cloud every time: the check guards lokadb.tenant on that way. The
// direct cooperative transaction (SET LOCAL after BEGIN) is UT11.
func checkUUIDTenantCoopDirect(e *env) (bool, string, error) {
	if why, err := uuidTenantMoved(e, "-customer-app", e.appCustomer()); err != nil || why != "" {
		return false, why, err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout":        map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	run := func() string {
		c, err := e.viaProxyPgx("ut10")
		if err != nil {
			return "connect: " + firstErrLine(err)
		}
		defer c.Close(e.ctx)
		t, err := c.Begin(e.ctx)
		if err != nil {
			return "begin: " + firstErrLine(err)
		}
		defer t.Rollback(e.ctx)
		if _, err := t.Exec(e.ctx, "SELECT set_config('lokadb.tenant', $1, true)", orgA); err != nil {
			return "set_config: " + firstErrLine(err)
		}
		var n int64
		if err := t.QueryRow(e.ctx, "SELECT count(*) FROM projects WHERE name LIKE 'p%'").Scan(&n); err != nil {
			return "count: " + firstErrLine(err)
		}
		return fmt.Sprint(n)
	}
	fdw := run()
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-tenant", orgA, "-customer-app", dsn("customer", e.db, "lokadb_app"),
		"-direct-role", "lokadb_dml", "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	var direct []string
	for i := 0; i < 3; i++ { // every one through the cloud, as DP21's
		direct = append(direct, run())
	}
	ok := fdw == "10"
	for _, d := range direct {
		ok = ok && d == fdw
	}
	return ok, fmt.Sprintf("fdw: %s (want 10); fdw+direct, three transactions: %v", fdw, direct), nil
}

// UT11. DP19 for an account numbered by uuid: pgx Begin, SET LOCAL lokadb.tenant = '<uuid>', a
// count of tasks and an UPDATE of a task by its title (neither has a condition on the
// account), Commit. Two such transactions go through the cloud and teach the form, the third
// goes direct: both statements in the account's database, the count equal to the cloud's for
// the account, the UPDATE in its database.
func checkUUIDTenantCoopTx(e *env) (bool, string, error) {
	if why, err := uuidTenantMoved(e, "-customer-app", e.appCustomer()); err != nil || why != "" {
		return false, why, err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout":        map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-tenant", orgA, "-customer-app", dsn("customer", e.db, "lokadb_app"),
		"-direct-role", "lokadb_dml", "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	tk, err := one(e.ctx, e.cloud(), "SELECT t.id::text FROM tasks t JOIN projects p ON p.id = t.project_id WHERE p.name = 'p2'")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE tasks SET title = 'ut11-0' WHERE id = "+tk); err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM tasks WHERE account_id = '"+orgA+"'")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("ut11")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(mark string) (int64, int, error) {
		n := directCount(e.proxyRoutes(), orgA)
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return 0, 0, err
		}
		var count int64
		steps := []error{
			exec1(e, tx, "SET LOCAL lokadb.tenant = '"+orgA+"'"),
			tx.QueryRow(e.ctx, "SELECT count(*) FROM tasks").Scan(&count),
			exec1(e, tx, "UPDATE tasks SET title = $1 WHERE title LIKE 'ut11-%'", mark),
		}
		for _, err := range steps {
			if err != nil {
				_ = tx.Rollback(e.ctx)
				return 0, 0, err
			}
		}
		err = tx.Commit(e.ctx)
		return count, directCount(e.proxyRoutes(), orgA) - n, err
	}
	warm := 0
	var counts []int64
	for _, m := range []string{"ut11-w1", "ut11-w2"} {
		n, d, err := run(m)
		if err != nil {
			return false, "a transaction learnt through the cloud: " + firstErrLine(err), nil
		}
		warm += d
		counts = append(counts, n)
	}
	n, direct, err := run("ut11-c")
	if err != nil {
		return false, "the learnt transaction: " + firstErrLine(err), nil
	}
	counts = append(counts, n)
	title, err := one(e.ctx, e.customer(), fmt.Sprintf("SELECT title FROM %s.tasks WHERE id = %s", sch, tk))
	if err != nil {
		return false, "", err
	}
	ok := warm == 0 && direct == 2 && title == "ut11-c" && fmt.Sprint(counts) == fmt.Sprintf("[%s %s %s]", want, want, want)
	return ok, fmt.Sprintf("two transactions learnt through the cloud: on the direct path %d (want 0); the third: on the direct path %d (want 2); counts %v (the cloud's for the account: %s); the task in its database %q (want ut11-c)",
		warm, direct, counts, want, title), nil
}
