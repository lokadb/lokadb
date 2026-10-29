package main

// Organizations and keys of type character(n) (tasks/char-keys.md): a Laravel application with
// $table->ulid() everywhere keeps its ids, its organization column (team_id) and every foreign
// key as character(26). LokaDB took none of it: check called 46 of 47 tables of a production
// schema "not supported yet".

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "UL1", name: "check takes a Laravel ulid() schema (character(26) ids, organization column and foreign keys): no id_type, tenant_column_type, parent_column_type, fk_not_to_id or routing_invalid; reserved organization values are a blocker", run: checkULIDReport},
		check{id: "UL2", name: "enable with character(26) organizations: partitions by value, the policy by lokadb.org (one team, rows of none, nothing, all), a child under its parent's team, org-add by the ulid", run: checkULIDEnable},
		check{id: "UL3", name: "the proxy (fanout reject) routes character(26) ids and organizations: literals (padded or not), text and binary parameters, char[] and text[], IN, a child by its FK; inserts land under their team", run: checkULIDRouting},
		check{id: "UL4", name: "move-org of a team named by a character(26) value (org-add first) while both teams write through the proxy; its rows there, the proxy reads, writes and upserts it there", run: checkULIDMove},
		check{id: "UL5", name: "repoint, move-back and disable of a team named by a character(26) value", run: checkULIDBack},
		check{id: "UL6", name: "fdw+direct of a team named by a character(26) value: reads, an UPDATE, an INSERT, a DELETE and a learnt transaction go straight to its database, __refs following", run: checkULIDDirect},
	)
}

// the teams of the ULID checks: ulids of 26 characters
const (
	teamA = "01JAAAAAAAAAAAAAAAAAAAAAAA"
	teamB = "01JBBBBBBBBBBBBBBBBBBBBBBB"
)

// ulidSpec: repos and builds of a team (team_id), build_files under builds (build_id); teams, the
// organizations table, is a cloud table whose id is the organization; members are shared.
const ulidSpec = `{"tables": [
  {"name": "repos", "tenant_column": "team_id"},
  {"name": "team_members", "tenant_column": "team_id"},
  {"name": "builds", "tenant_column": "team_id"},
  {"name": "build_files", "parent_table": "builds", "parent_column": "build_id"}
], "routing": {"cloud_tables": [{"table": "teams", "tenant_column": "id"}]}}`

// ulidApp: a Laravel schema as ulid() makes it. 10 repos and 10 builds (a file each) of each
// team, a build of team B authored by a user, a repo-less build, and rows of no team.
func ulidApp(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		"CREATE TABLE teams (id char(26) PRIMARY KEY, name text)",
		"CREATE TABLE members (id char(26) PRIMARY KEY, email text UNIQUE)",
		"CREATE TABLE repos (id char(26) PRIMARY KEY, team_id char(26) REFERENCES teams, name text)",
		"CREATE TABLE builds (id char(26) PRIMARY KEY, team_id char(26) REFERENCES teams, repo_id char(26) REFERENCES repos, author_id char(26) REFERENCES members, title text)",
		"CREATE TABLE build_files (id char(26) PRIMARY KEY, build_id char(26) NOT NULL REFERENCES builds ON DELETE CASCADE, path text)",
		"CREATE INDEX builds_team ON builds (team_id)",
		// Laravel's pivot: the primary key is the team and the member, both character(26)
		"CREATE TABLE team_members (team_id char(26) NOT NULL REFERENCES teams, member_id char(26) NOT NULL REFERENCES members, role text, PRIMARY KEY (team_id, member_id))",
		"ALTER TABLE team_members OWNER TO app",
		"ALTER TABLE teams OWNER TO app", "ALTER TABLE members OWNER TO app", "ALTER TABLE repos OWNER TO app",
		"ALTER TABLE builds OWNER TO app", "ALTER TABLE build_files OWNER TO app",
		"INSERT INTO teams VALUES ('"+teamA+"', 'A'), ('"+teamB+"', 'B')",
		"INSERT INTO members VALUES ('01JUUUUUUUUUUUUUUUUUUUUUU1', 'u1@example.com'), ('01JUUUUUUUUUUUUUUUUUUUUUU2', 'u2@example.com'), ('01JUUUUUUUUUUUUUUUUUUUUUU3', 'u3@example.com')",
		// the member u1 is in both teams: only the team and the member together tell the rows apart
		"INSERT INTO team_members VALUES ('"+teamA+"', '01JUUUUUUUUUUUUUUUUUUUUUU1', 'owner'), ('"+teamA+"', '01JUUUUUUUUUUUUUUUUUUUUUU2', 'member'), ('"+teamB+"', '01JUUUUUUUUUUUUUUUUUUUUUU1', 'member'), ('"+teamB+"', '01JUUUUUUUUUUUUUUUUUUUUUU3', 'owner')",
		"INSERT INTO repos SELECT '01JR' || lpad(g::text, 22, '0'), CASE WHEN g % 2 = 0 THEN '"+teamA+"' ELSE '"+teamB+"' END, 'r' || g FROM generate_series(1, 20) g",
		"INSERT INTO builds SELECT '01JB' || lpad(g::text, 22, '0'), CASE WHEN g % 2 = 0 THEN '"+teamA+"' ELSE '"+teamB+"' END, '01JR' || lpad(g::text, 22, '0'), NULL, 'b' || g FROM generate_series(1, 20) g",
		"INSERT INTO build_files SELECT '01JF' || lpad(g::text, 22, '0'), '01JB' || lpad(g::text, 22, '0'), 'f' || g FROM generate_series(1, 20) g",
		"INSERT INTO builds VALUES ('01JBNOTEAM0000000000000001', NULL, NULL, NULL, 'nobody')")
}

// ulidB: a build id of team A (even numbers) or B (odd) of ulidApp.
func ulidB(g int) string { return fmt.Sprintf("01JB%022d", g) }

// UL1. check called a character(26) id "not supported yet" for all of 46 tables.
func checkULIDReport(e *env) (bool, string, error) {
	if err := ulidApp(e); err != nil {
		return false, "", err
	}
	x := &expects{ok: true}
	out, code := e.checkWith(ulidSpec)
	for _, c := range []string{"id_type", "tenant_column_type", "parent_column_type", "fk_not_to_id", "routing_invalid"} {
		x.eq(c, fmt.Sprint(reported(out, c)), "false")
	}
	x.eq("tenant_value_reserved without such rows, exit", fmt.Sprint(reported(out, "tenant_value_reserved"), " ", code == 0 || code == 2), "false true")
	x.eq("the pivot with a composite key of character(26) is ready", fmt.Sprint(strings.Contains(out, "team_members") && !strings.Contains(out, "team_members: not supported")), "true")
	// values lokadb keeps for itself: an empty string, a minus, a number with a leading zero
	for i, v := range []string{"", "-5", "007"} {
		if err := execSQL(e.ctx, e.cloud(), "INSERT INTO teams VALUES ('"+v+"', 'reserved')",
			fmt.Sprintf("INSERT INTO repos VALUES ('01JRRESERVED%014d', '%s', 'reserved')", i, v)); err != nil {
			return false, "", err
		}
		out, code = e.checkWith(ulidSpec)
		x.eq(fmt.Sprintf("team_id %q: tenant_value_reserved, exit", v), fmt.Sprint(reported(out, "tenant_value_reserved"), " ", code), "true 3")
		if err := execSQL(e.ctx, e.cloud(), "DELETE FROM repos WHERE name = 'reserved'"); err != nil {
			return false, "", err
		}
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UL2. enable made LIST (coalesce(team_id, -2)) of a character(26) column and failed.
func checkULIDEnable(e *env) (bool, string, error) {
	if err := ulidApp(e); err != nil {
		return false, "", err
	}
	e.spec = ulidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	x := &expects{ok: true}
	x.eq("the partition of rows of none", query(e, e.cloud(), "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'builds__none'"), "FOR VALUES IN ('')")
	x.eq("the refs table's id and organization", query(e, e.cloud(), `SELECT (SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'builds__refs'::regclass AND attname = 'id')
		|| ' ' || (SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'builds__refs'::regclass AND attname = 'tenant')`), "character(26) text")
	x.eq("the child's organization column", query(e, e.cloud(), "SELECT format_type(atttypid, atttypmod) || ' ' || pg_get_expr(adbin, adrelid) FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE a.attrelid = 'build_files'::regclass AND a.attname = 'team_id'"),
		"text lokadb.org_default_text()")
	x.eq("files under their build's team", query(e, e.cloud(), "SELECT count(*) FROM build_files f JOIN builds b ON b.id = f.build_id WHERE f.team_id IS DISTINCT FROM b.team_id"), "0")
	x.eq("lokadb.org = A: builds, files", asApp(e, teamA, "SELECT (SELECT count(*) FROM builds)::text || ', ' || (SELECT count(*) FROM build_files)"), "10, 10")
	x.eq("lokadb.org = A: the pivot's rows", asApp(e, teamA, "SELECT count(*) FROM team_members"), "2")
	x.eq("the refs table of the pivot holds the pair", query(e, e.cloud(), "SELECT count(*) FROM team_members__refs"), "4")
	x.eq("lokadb.org = -2: rows of none", asApp(e, "-2", "SELECT (SELECT count(*) FROM builds)::text || ' ' || (SELECT string_agg(title, ',') FROM builds)"), "1 nobody")
	x.eq("lokadb.org = -1: nothing", asApp(e, "-1", "SELECT count(*) FROM builds"), "0")
	x.eq("lokadb.org unset: every one", asApp(e, "", "SELECT count(*) FROM builds"), "21")
	// a file under a build of team B, with team A named: refused
	x.eq("a file under B's build, under A: refused", fmt.Sprint(strings.Contains(asApp(e, teamA, "WITH f AS (INSERT INTO build_files (id, build_id, path) VALUES ('01JFNEW00000000000000000A1', '"+ulidB(1)+"', 'x') RETURNING 1) SELECT count(*) FROM f"), "23514")), "true")
	// a file under A's build gets A
	x.eq("a file under A's build, under A", asApp(e, teamA, "WITH f AS (INSERT INTO build_files (id, build_id, path) VALUES ('01JFNEW00000000000000000A2', '"+ulidB(2)+"', 'x') RETURNING team_id) SELECT team_id::text FROM f"), teamA)
	a1, _ := e.lokadb("org-add", "-db", e.cloud(), "-tenant", teamA+"  ")
	a2, _ := e.lokadb("org-add", "-db", e.cloud(), "-tenant", teamA)
	_, badErr := e.lokadb("org-add", "-db", e.cloud(), "-tenant", "-5")
	x.eq("org-add A twice (blanks at the end go)", strings.TrimSpace(lastLine(a1))+" | "+strings.TrimSpace(lastLine(a2)), "organization 1: "+teamA+" | organization 1: "+teamA)
	x.eq("org-add of a value that starts with a minus refused", fmt.Sprint(badErr != nil), "true")
	x.eq("lokadb.tenants", query(e, e.cloud(), "SELECT string_agg(org || '=' || tenant, ',') FROM lokadb.tenants"), "1="+teamA)
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UL3. The proxy read an organization column's value as an integer or a uuid only, and a key
// of type character(26) as nothing.
func checkULIDRouting(e *env) (bool, string, error) {
	if err := ulidApp(e); err != nil {
		return false, "", err
	}
	e.spec = ulidSpec
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
	lit := func(what, sql, want string) { x.eq(what, proxyValue(e, c, sql), want) }
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
	const bpchar, text = 1042, 25
	lit("literal", "SELECT count(*) FROM builds WHERE team_id = '"+teamA+"'", "10")
	lit("literal with blanks at its end", "SELECT count(*) FROM builds WHERE team_id = '"+teamA+"  '", "10")
	lit("cast to character(26)", "SELECT count(*) FROM builds WHERE team_id = '"+teamB+"'::char(26)", "10")
	lit("IN", "SELECT count(*) FROM builds WHERE team_id IN ('"+teamA+"')", "10")
	lit("= ANY of a char[] literal", "SELECT count(*) FROM builds WHERE team_id = ANY('{"+teamA+"}'::char(26)[])", "10")
	param("text parameter", "SELECT count(*) FROM builds WHERE team_id = $1", []byte(teamB), 0, 0, "10")
	param("bpchar parameter, binary", "SELECT count(*) FROM builds WHERE team_id = $1", []byte(teamB), bpchar, 1, "10")
	param("text parameter, binary", "SELECT count(*) FROM builds WHERE team_id = $1", []byte(teamB), text, 1, "10")
	param("text[] parameter", "SELECT count(*) FROM builds WHERE team_id = ANY($1)", []byte("{"+teamA+"}"), 1009, 0, "10")
	// only the key names the organization
	param("a pivot row by its team and member (text parameters)", "SELECT role FROM team_members WHERE team_id = $1 AND member_id = '01JUUUUUUUUUUUUUUUUUUUUUU1'", []byte(teamB), bpchar, 1, "member")
	lit("a pivot row by its pair", "SELECT role FROM team_members WHERE team_id = '"+teamA+"' AND member_id = '01JUUUUUUUUUUUUUUUUUUUUUU2'", "member")
	lit("a pivot row inserted", "INSERT INTO team_members (team_id, member_id, role) VALUES ('"+teamA+"', '01JUUUUUUUUUUUUUUUUUUUUUU3', 'new') RETURNING role", "new")
	param("a build by its id", "SELECT title FROM builds WHERE id = $1", []byte(ulidB(4)), bpchar, 1, "b4")
	param("a build by its id, padded", "SELECT title FROM builds WHERE id = $1", []byte(ulidB(4)+" "), 0, 0, "b4")
	param("its files by their FK", "SELECT count(*) FROM build_files WHERE build_id = $1", []byte(ulidB(4)), bpchar, 1, "1")
	param("an insert under the key's team", "INSERT INTO build_files (id, build_id, path) VALUES ('01JFNEW00000000000000000B1', $1, 'x') RETURNING team_id", []byte(ulidB(4)), bpchar, 1, teamA)
	lit("a build inserted by its team", "INSERT INTO builds (id, team_id, title) VALUES ('01JBNEW0000000000000000001', '"+teamB+"', 'via proxy') RETURNING team_id", teamB)
	_, refErr := simple(e.ctx, c, "SELECT count(*) FROM builds WHERE title LIKE 'b%'")
	x.eq("a statement of no organization refused", fmt.Sprint(refErr != nil), "true")
	x.eq("in the cloud: A's files, B's builds, A's pivot rows", query(e, e.cloud(), "SELECT (SELECT count(*) FROM build_files WHERE team_id = '"+teamA+"')::text || ', ' || (SELECT count(*) FROM builds WHERE team_id = '"+teamB+"') || ', ' || (SELECT count(*) FROM team_members WHERE team_id = '"+teamA+"')"), "11, 11, 3")
	// the cooperative mode: the team's ulid
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
	x.eq("SET LOCAL lokadb.tenant = A, then an unhinted count", run("BEGIN", "SET LOCAL lokadb.tenant = '"+teamA+"'", "SELECT count(*) FROM builds WHERE title LIKE 'b%'", "COMMIT"), "10")
	x.eq("a statement naming B under A refused (42501)", fmt.Sprint(strings.Contains(run("BEGIN", "SET LOCAL lokadb.tenant = '"+teamA+"'", "SELECT count(*) FROM builds WHERE team_id = '"+teamB+"'", "COMMIT"), "42501")), "true")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// ulidMoved: the ULID app enabled, team A numbered 1 (org-add) and moved (extra: move-org's
// further arguments); why it could not be, "" when it was.
func ulidMoved(e *env, extra ...string) (string, error) {
	if err := ulidApp(e); err != nil {
		return "", err
	}
	e.spec = ulidSpec
	if err := e.enable(); err != nil {
		return "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.lokadb("org-add", "-db", e.cloud(), "-tenant", teamA); err != nil {
		return "org-add failed: " + lastErr(out), nil
	}
	if out, err := e.move(1, extra...); err != nil {
		return "move-org failed: " + lastErr(out), nil
	}
	return "", nil
}

// UL4. move-org of a team named by a character(26) value, in both modes, while both teams write
// through the proxy; the proxy then reads, writes and upserts it in its own database.
func checkULIDMove(e *env) (bool, string, error) {
	if err := ulidApp(e); err != nil {
		return false, "", err
	}
	e.spec = ulidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	x := &expects{ok: true}
	out, err := e.move(1)
	x.eq("move-org before org-add: refused, org-add named", fmt.Sprint(err != nil && strings.Contains(out, "org-add")), "true")
	if out, err := e.lokadb("org-add", "-db", e.cloud(), "-tenant", teamA); err != nil {
		return false, "org-add failed: " + lastErr(out), nil
	}
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	done := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	wrote := map[string]int{}
	var writeErrs []string
	for i, team := range []string{teamA, teamB} {
		wg.Add(1)
		go func(i int, team string) {
			defer wg.Done()
			c, err := e.viaProxy()
			if err != nil {
				mu.Lock()
				writeErrs = append(writeErrs, firstErrLine(err))
				mu.Unlock()
				return
			}
			defer c.Close(e.ctx)
			for n := 0; ; n++ {
				select {
				case <-done:
					return
				default:
				}
				id := fmt.Sprintf("01JW%c%021d", 'A'+i, n)
				_, err := simple(e.ctx, c, fmt.Sprintf("INSERT INTO builds (id, team_id, title) VALUES ('%s', '%s', 'w%d')", id, team, n))
				if err == nil { // a file under it: the team comes from the build
					_, err = simple(e.ctx, c, fmt.Sprintf("INSERT INTO build_files (id, build_id, path) VALUES ('01JX%c%021d', '%s', 'x')", 'A'+i, n, id))
				}
				mu.Lock()
				if err != nil {
					writeErrs = append(writeErrs, firstErrLine(err))
				} else {
					wrote[team]++
				}
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
			}
		}(i, team)
	}
	out, merr := e.move(1)
	close(done)
	wg.Wait()
	if merr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	x.eq("writes through the proxy during the move that failed", fmt.Sprint(len(writeErrs), " ", strings.Join(writeErrs[:min(len(writeErrs), 2)], " | ")), "0 ")
	x.eq("writes of both teams during the move", fmt.Sprint(wrote[teamA] > 0, " ", wrote[teamB] > 0), "true true")
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	x.eq("A's builds there, nothing else", query(e, e.customer(), fmt.Sprintf("SELECT count(*)::text || ' ' || count(*) FILTER (WHERE team_id <> '%s') FROM %s.builds", teamA, sch)),
		fmt.Sprintf("%d 0", 10+wrote[teamA]))
	x.eq("A's files there, their team", query(e, e.customer(), fmt.Sprintf("SELECT count(*)::text || ' ' || count(*) FILTER (WHERE team_id = '%s') FROM %s.build_files", teamA, sch)),
		fmt.Sprintf("%d %d", 10+wrote[teamA], 10+wrote[teamA]))
	x.eq("A's pivot rows there, nothing else", query(e, e.customer(), fmt.Sprintf("SELECT count(*)::text || ' ' || count(*) FILTER (WHERE team_id <> '%s') FROM %s.team_members", teamA, sch)), "2 0")
	x.eq("the foreign partition by A's value", fmt.Sprint(strings.HasPrefix(query(e, e.cloud(), "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE relname = 'builds__org1'"), "FOR VALUES IN ('"+teamA+"'")), "true")
	x.eq("B's builds in the cloud", query(e, e.cloud(), "SELECT count(*) FROM ONLY builds__cloud WHERE team_id = '"+teamB+"'"), fmt.Sprint(10+wrote[teamB]))
	x.eq("the policy: lokadb.org = A reads there", asApp(e, teamA, "SELECT count(*) FROM builds"), fmt.Sprint(10+wrote[teamA]))
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x.eq("via the proxy: A's builds", proxyValue(e, c, "SELECT count(*) FROM builds WHERE team_id = '"+teamA+"'"), fmt.Sprint(10+wrote[teamA]))
	x.eq("via the proxy: a build of A by its id", proxyValue(e, c, "SELECT title FROM builds WHERE id = '"+ulidB(2)+"'"), "b2")
	x.eq("via the proxy: a file under it, there", proxyValue(e, c, "INSERT INTO build_files (id, build_id, path) VALUES ('01JFUL4000000000000000000A', '"+ulidB(2)+"', 'ul4') RETURNING team_id"), teamA)
	x.eq("the file there", query(e, e.customer(), fmt.Sprintf("SELECT path FROM %s.build_files WHERE id = '01JFUL4000000000000000000A'", sch)), "ul4")
	// an upsert of the moved team keeps lokadb's rewrite; one of B's runs in the cloud
	x.eq("via the proxy: an upsert of A's build", proxyValue(e, c, "INSERT INTO builds (id, team_id, title) VALUES ('"+ulidB(2)+"', '"+teamA+"', 'ul4-upsert') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title RETURNING title"), "ul4-upsert")
	x.eq("via the proxy: an upsert of B's build", proxyValue(e, c, "INSERT INTO builds (id, team_id, title) VALUES ('"+ulidB(1)+"', '"+teamB+"', 'ul4-b') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title RETURNING title"), "ul4-b")
	if !x.ok {
		x.notes = append(x.notes, e.proxyLines("ON CONFLICT"))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UL5. repoint, move-back and disable of a team named by a character(26) value.
func checkULIDBack(e *env) (bool, string, error) {
	if why, err := ulidMoved(e); err != nil || why != "" {
		return false, why, err
	}
	x := &expects{ok: true}
	cp, err := e.restoredCopy()
	if err != nil {
		return false, "", err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, cp, fmt.Sprintf("UPDATE %s.builds SET title = 'only in the copy' WHERE title = 'b2'", sch)); err != nil {
		return false, "", err
	}
	if out, err := e.repoint(map[string]any{}, "-tenant", teamA, "-customer-app", cp); err != nil {
		return false, "repoint failed: " + lastErr(out), nil
	}
	x.eq("repoint: the cloud reads the copy", query(e, e.cloud(), "SELECT count(*) FROM builds WHERE team_id = '"+teamA+"' AND title = 'only in the copy'"), "1")
	if out, err := e.lokadb(e.withCustomer(e.moveBackArgs(1), cp)...); err != nil {
		return false, "move-back failed: " + lastErr(out), nil
	}
	x.eq("move-back: A's builds in its partition of the cloud", query(e, e.cloud(), `SELECT count(*)::text || ' ' || (regexp_match(pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES IN \(''([^'']*)'''))[1] FROM builds p
		JOIN pg_class c ON c.oid = p.tableoid WHERE p.team_id = '`+teamA+`' GROUP BY c.oid`), "10 "+teamA)
	x.eq("move-back: the copy's change", query(e, e.cloud(), "SELECT count(*) FROM builds WHERE title = 'only in the copy'"), "1")
	x.eq("move-back: A's pivot rows", query(e, e.cloud(), "SELECT count(*) FROM team_members WHERE team_id = '"+teamA+"'"), "2")
	x.eq("move-back: A's files under A", query(e, e.cloud(), "SELECT count(*) FROM build_files WHERE team_id = '"+teamA+"'"), "10")
	x.eq("move-back: lokadb.orgs, servers and foreign tables left", query(e, e.cloud(), `SELECT (SELECT count(*) FROM lokadb.orgs) + (SELECT count(*) FROM pg_foreign_server WHERE srvname LIKE 'lokadb\_org%')
		+ (SELECT count(*) FROM pg_class WHERE relkind = 'f' AND relname LIKE '%\_\_org1%')`), "0")
	x.eq("the policy after move-back: lokadb.org = A", asApp(e, teamA, "SELECT count(*) FROM builds"), "10")
	if out, err := e.lokadb(e.disableArgs()...); err != nil {
		return false, "disable failed: " + lastErr(out), nil
	}
	x.eq("disable: builds, files, the table's kind, schema lokadb gone, the child's added column", query(e, e.cloud(), `SELECT (SELECT count(*) FROM builds)::text || ' ' || (SELECT count(*) FROM build_files)
		|| ' ' || (SELECT relkind::text FROM pg_class WHERE oid = 'builds'::regclass) || ' ' || (to_regnamespace('lokadb') IS NULL)::text
		|| ' ' || (SELECT count(*) FROM pg_attribute WHERE attrelid = 'build_files'::regclass AND attname = 'team_id' AND NOT attisdropped)`), "21 20 r true 0")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// UL6. The direct path (fdw+direct) of a team named by a character(26) value: reads, writes of
// build_files (a child of builds, not tied to the cloud's teams by an FK of its own), a
// transaction learnt through the cloud and then direct.
func checkULIDDirect(e *env) (bool, string, error) {
	if why, err := ulidMoved(e, "-customer-app", e.appCustomer()); err != nil || why != "" {
		return false, why, err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout":        map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.lokadb("org-profile", "-db", e.cloud(), "-tenant", teamA, "-customer-app", dsn("customer", e.db, "lokadb_app"),
		"-direct-role", "lokadb_dml", "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("ul6")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x := &expects{ok: true}
	step := func(what, want, sql string, args ...any) []string {
		n := directCount(e.proxyRoutes(), teamA)
		rows := rowsText(e, c, sql, args...)
		x.eq(what+" on the direct path", fmt.Sprint(directCount(e.proxyRoutes(), teamA)-n), want)
		return rows
	}
	x.eq("A's builds", strings.Join(step("A's builds by team_id", "1", "SELECT count(*) FROM builds WHERE team_id = $1", teamA), ","), "10,tag: SELECT 1")
	x.eq("a build's files", strings.Join(step("files by their build (the FK)", "1", "SELECT count(*) FROM build_files WHERE build_id = $1", ulidB(4)), ","), "1,tag: SELECT 1")
	file := "01JF" + fmt.Sprintf("%022d", 4)
	step("UPDATE of a file by its key", "1", "UPDATE build_files SET path = 'ul6' WHERE id = $1", file)
	step("INSERT of a file with its key", "1", "INSERT INTO build_files (id, build_id, path) VALUES ('01JFUL6000000000000000000A', $1, 'ul6-new')", ulidB(4))
	x.eq("there: the update, the new file under A", query(e, e.customer(), fmt.Sprintf(
		"SELECT (SELECT path FROM %[1]s.build_files WHERE id = '%[2]s') || ', ' || (SELECT count(*) FROM %[1]s.build_files WHERE path = 'ul6-new' AND team_id = '%[3]s')", sch, file, teamA)), "ul6, 1")
	x.eq("its refs row under A", query(e, e.cloud(), "SELECT tenant FROM build_files__refs WHERE id = '01JFUL6000000000000000000A'"), teamA)
	step("DELETE of the new file", "1", "DELETE FROM build_files WHERE id = '01JFUL6000000000000000000A'")
	x.eq("after the DELETE: there, in the refs", query(e, e.customer(), fmt.Sprintf("SELECT count(*) FROM %s.build_files WHERE path = 'ul6-new'", sch))+", "+
		query(e, e.cloud(), "SELECT count(*) FROM build_files__refs WHERE id = '01JFUL6000000000000000000A'"), "0, 0")
	// a transaction: learnt through the cloud twice, then on the direct path
	tx := func(mark string) (int, error) {
		n := directCount(e.proxyRoutes(), teamA)
		t, err := c.Begin(e.ctx)
		if err != nil {
			return 0, err
		}
		if _, err := t.Exec(e.ctx, "UPDATE build_files SET path = $1 WHERE id = $2", mark, file); err != nil {
			_ = t.Rollback(e.ctx)
			return 0, err
		}
		if _, err := t.Exec(e.ctx, "INSERT INTO build_files (id, build_id, path) VALUES ($1, $2, $3)", "01JFUL6T"+mark[len(mark)-1:]+"00000000000000000", ulidB(4), mark+"-i"); err != nil {
			_ = t.Rollback(e.ctx)
			return 0, err
		}
		err = t.Commit(e.ctx)
		return directCount(e.proxyRoutes(), teamA) - n, err
	}
	var direct []string
	for _, m := range []string{"ul6-t1", "ul6-t2", "ul6-t3"} {
		n, err := tx(m)
		if err != nil {
			direct = append(direct, "error: "+firstErrLine(err))
			continue
		}
		direct = append(direct, fmt.Sprint(n))
	}
	x.eq("a transaction three times: statements on the direct path", strings.Join(direct, " "), "0 0 2")
	x.eq("the transactions there", query(e, e.customer(), fmt.Sprintf("SELECT (SELECT path FROM %[1]s.build_files WHERE id = '%[2]s') || ', ' || (SELECT count(*) FROM %[1]s.build_files WHERE path LIKE 'ul6-t_-i')", sch, file)), "ul6-t3, 3")
	if !x.ok {
		x.notes = append(x.notes, e.proxyLines("msg=route"))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}
