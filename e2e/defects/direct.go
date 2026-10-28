package main

// The direct path, profile fdw+direct (block 10, stage 8a, tasks/direct-path.md): a read or
// an UPDATE of one moved organization goes from the proxy to the organization's database on a
// connection of its own, not through the cloud and postgres_fdw.

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "DP1", name: "fdw+direct answers as fdw does: two moved organizations with the same ids in their databases, no row of the other", run: checkDirectDifferential},
		check{id: "DP2", name: "a read by id reaches the organization's database as the application's own statement", run: checkDirectText},
		check{id: "DP3", name: "reads and UPDATEs of an organization in fdw+direct open no FDW connection to its database", run: checkDirectNoFDW},
		check{id: "DP9", name: "a profile switch under load on the direct path: no lost write, no direct statement after the switch", run: checkDirectSwitch},
		check{id: "DP10", name: "a role with SELECT alone reads on the direct path and may UPDATE or DELETE there no more than through the FDW", run: checkDirectPrivileges},
		check{id: "DP11", name: "current_user, current_database(), pg_backend_pid() and an extension's operator (pg_trgm's % in the cloud) answer as the cloud's: such statements stay off the direct path", run: checkDirectCloudFuncs},
	)
}

// directSetup: organization 1 moved (its application role given), the proxy with its DSNs
// (customers: the tables' owner, for DDL replay; customers_app: lokadb_dml, the direct
// path's role of DML alone) and fanout reject. extra: more organizations, org -> database.
// Returns the proxy's stop.
func (e *env) directSetup(extra map[string]string) (func(), error) {
	if err := e.enable(); err != nil {
		return nil, err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return nil, err
	}
	customers := map[string]string{"1": e.appCustomer()}
	apps := map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}
	for k, db := range extra {
		customers[k], apps[k] = dsn("customer", db, "lokadb_app"), dsn("customer", db, "lokadb_dml")
	}
	stop, err := e.proxyCfg(customers, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: apps}})
	if err != nil {
		return nil, err
	}
	return stop, nil
}

// orgProfile: lokadb org-profile; for fdw+direct it grants lokadb_dml DML on the tables in
// db (the check's own customer database if "") as their owner lokadb_app.
func (e *env) orgProfile(org int, profile string, db ...string) (string, error) {
	if profile != "fdw+direct" {
		return e.lokadb("org-profile", "-db", e.cloud(), "-org", fmt.Sprint(org), profile)
	}
	d := e.db
	if len(db) > 0 {
		d = db[0]
	}
	return e.lokadb("org-profile", "-db", e.cloud(), "-org", fmt.Sprint(org),
		"-customer-app", dsn("customer", d, "lokadb_app"), "-direct-role", "lokadb_dml", profile)
}

// viaProxyPgx: the application through the proxy with pgx (extended protocol, statements
// prepared under names: Parse once, then Bind and Execute).
func (e *env) viaProxyPgx(app string) (*pgx.Conn, error) {
	return pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable application_name=%s", e.port, e.db, app))
}

// proxyRoutes: the route lines of the check's proxy log: when, the organization, direct or not.
type routeLine struct {
	at     time.Time
	org    string
	direct bool
}

func (e *env) proxyRoutes() []routeLine {
	f, err := os.Open(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []routeLine
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		l := sc.Text()
		if !strings.Contains(l, "msg=route") {
			continue
		}
		r := routeLine{direct: strings.Contains(l, "path=direct")}
		if i := strings.Index(l, "time="); i >= 0 {
			ts := l[i+5:]
			if j := strings.IndexByte(ts, ' '); j >= 0 {
				ts = ts[:j]
			}
			r.at, _ = time.Parse(time.RFC3339Nano, ts)
		}
		if i := strings.Index(l, `to="tenant `); i >= 0 {
			r.org = strings.SplitN(l[i+11:], `"`, 2)[0]
		}
		out = append(out, r)
	}
	return out
}

// proxyLines: the proxy log's lines that contain sub, for a check's notes.
func (e *env) proxyLines(sub string) string {
	b, _ := os.ReadFile(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, sub) {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		return ""
	}
	return "; proxy: " + strings.Join(out, " | ")
}

func directCount(rs []routeLine, org string) int {
	n := 0
	for _, r := range rs {
		if r.direct && r.org == org {
			n++
		}
	}
	return n
}

// rowsText runs sql through c and returns its rows as text, or the error as a row.
func rowsText(e *env, c *pgx.Conn, sql string, args ...any) []string {
	rows, err := c.Query(e.ctx, sql, args...)
	if err != nil {
		return []string{"error: " + firstErrLine(err)}
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		vs, err := rows.Values()
		if err != nil {
			return []string{"error: " + firstErrLine(err)}
		}
		out = append(out, fmt.Sprint(vs...))
	}
	if err := rows.Err(); err != nil {
		return append(out, "error: "+firstErrLine(err))
	}
	return append(out, "tag: "+rows.CommandTag().String())
}

// DP1. Two organizations moved to two databases, both in fdw+direct; each database holds a
// marker row under the same id. Each statement form of a CRUD application, for each
// organization, gives the same rows in fdw and in fdw+direct, and no marker of the other.
func checkDirectDifferential(e *env) (bool, string, error) {
	db2 := e.db + "_c2"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+db2+" WITH (FORCE)", "CREATE DATABASE "+db2+" OWNER app"); err != nil {
		return false, "", err
	}
	cust2, app2 := dsn("customer", db2, "postgres"), dsn("customer", db2, "lokadb_app")
	// as the check's own database (main.go): the tables go to public, where the proxy's
	// statements name them, not to schema lokadb (block 7's default without this grant)
	if err := execSQL(e.ctx, cust2, "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return false, "", err
	}
	stop, err := e.directSetup(map[string]string{"2": db2})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if _, err := e.move(2, "-customer", cust2, "-customer-app", app2); err != nil {
		return false, "", err
	}
	for org, d := range map[int]string{1: e.customer(), 2: cust2} {
		if err := execSQL(e.ctx, d,
			fmt.Sprintf("INSERT INTO documents (id, organization_id, title) VALUES (990001, %d, 'mark-%d')", org, org),
			fmt.Sprintf("INSERT INTO document_versions (id, document_id, organization_id, content) VALUES (990001, 990001, %d, 'markv-%d')", org, org)); err != nil {
			return false, "", err
		}
	}
	type pattern struct {
		name, sql string
		args      func(org int) []any
		simple    bool
	}
	org := func(o int) []any { return []any{o} }
	patterns := []pattern{
		{"list", "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id", org, false},
		{"by id", "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", func(o int) []any { return []any{o, 990001} }, false},
		{"join", "SELECT d.id, v.content FROM documents d JOIN document_versions v ON v.document_id = d.id WHERE d.organization_id = $1 ORDER BY v.id", org, false},
		{"in", "SELECT title FROM documents WHERE organization_id = ANY($1) AND id < 20 ORDER BY id", func(o int) []any { return []any{[]int{o}} }, false},
		{"count", "SELECT count(*), max(title) FROM documents WHERE organization_id = %d", nil, true},
		{"update", "UPDATE documents SET title = title WHERE organization_id = $1 AND id = $2", func(o int) []any { return []any{o, 990001} }, false},
		{"versions", "SELECT content FROM document_versions WHERE organization_id = %d AND document_id = 990001", nil, true},
	}
	run := func() (map[string][]string, error) {
		c, err := e.viaProxyPgx("dp1")
		if err != nil {
			return nil, err
		}
		defer c.Close(e.ctx)
		out := map[string][]string{}
		for _, p := range patterns {
			for o := 1; o <= 2; o++ {
				key := fmt.Sprintf("%s/%d", p.name, o)
				if p.simple {
					out[key] = rowsText(e, c, fmt.Sprintf(p.sql, o), pgx.QueryExecModeSimpleProtocol)
				} else {
					out[key] = rowsText(e, c, p.sql, p.args(o)...)
					out[key+" again"] = rowsText(e, c, p.sql, p.args(o)...) // the prepared statement bound once more
				}
			}
		}
		return out, nil
	}
	fdw, err := run()
	if err != nil {
		return false, "", err
	}
	for o, db := range map[int]string{1: e.db, 2: db2} {
		if out, err := e.orgProfile(o, "fdw+direct", db); err != nil {
			return false, fmt.Sprintf("org-profile %d fdw+direct refused: %s", o, lastErr(out)), nil
		}
	}
	direct, err := run()
	if err != nil {
		return false, "", err
	}
	var diffs, foreign []string
	for k, want := range fdw {
		if !reflect.DeepEqual(want, direct[k]) {
			diffs = append(diffs, fmt.Sprintf("%s: fdw %q, fdw+direct %q", k, want, direct[k]))
		}
		o := k[strings.IndexByte(k, '/')+1:][:1]
		other := map[string]string{"1": "2", "2": "1"}[o]
		for _, r := range direct[k] {
			if strings.Contains(r, "mark-"+other) || strings.Contains(r, "markv-"+other) {
				foreign = append(foreign, k+": "+r)
			}
		}
	}
	routes := e.proxyRoutes()
	n1, n2 := directCount(routes, "1"), directCount(routes, "2")
	mark1 := strings.Join(direct["by id/1"], " ")
	ok := len(diffs) == 0 && len(foreign) == 0 && n1 > 0 && n2 > 0 && strings.Contains(mark1, "mark-1")
	return ok, fmt.Sprintf("%d results compared, differing %d %q; rows of the other organization %q; statements on the direct path: organization 1 %d, organization 2 %d; organization 1 by id 990001: %s",
		len(fdw), len(diffs), diffs, foreign, n1, n2, mark1), nil
}

// DP2. Through the FDW the organization's database sees postgres_fdw's cursor (DECLARE,
// FETCH); on the direct path it runs the application's statement under the application's
// name. The statement is slow by a subquery over generate_series (of the direct path's
// functions, unlike pg_sleep: review of block 9).
func checkDirectText(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp2")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	const q = "SELECT (SELECT count(*) FROM generate_series(1, 30000000)), title FROM documents WHERE organization_id = $1 AND id = $2"
	done := make(chan []string, 1)
	go func() { done <- rowsText(e, c, q, 1, doc) }()
	seen := "nothing"
	for i := 0; i < 50; i++ {
		time.Sleep(50 * time.Millisecond)
		rows, err := e.customerRows("SELECT application_name || ' | ' || query FROM pg_stat_activity WHERE datname = current_database() AND state = 'active' AND pid <> pg_backend_pid()")
		if err == nil && len(rows) > 0 {
			seen = strings.Join(rows, "; ")
			break
		}
	}
	got := <-done
	ok := seen == "dp2 | "+q && len(got) == 2
	return ok, fmt.Sprintf("the organization's database ran: %s; the client got %q", seen, got), nil
}

func (e *env) customerRows(sql string) ([]string, error) {
	c, err := pgx.Connect(e.ctx, e.customer())
	if err != nil {
		return nil, err
	}
	defer c.Close(e.ctx)
	rows, err := c.Query(e.ctx, sql)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// DP3. In fdw every statement of the organization reaches its database through postgres_fdw
// (a connection of the cloud's); in fdw+direct its reads and UPDATEs open none. The UPDATE is
// of document_versions: documents has an FK onto users, a cloud table, kept by a row trigger
// of the cloud (lokadb_fk_*), so since stage 8b its UPDATEs go the cloud's way (DP14).
func checkDirectNoFDW(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	if err := execSQL(e.ctx, e.customer(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp3")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	errs := 0
	for i := 0; i < 20; i++ {
		for _, r := range [][]string{
			rowsText(e, c, "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id LIMIT 5", 1),
			rowsText(e, c, "SELECT d.title, v.content FROM documents d JOIN document_versions v ON v.document_id = d.id WHERE d.organization_id = $1 AND d.id = $2", 1, 2+2*i),
			rowsText(e, c, "UPDATE document_versions SET content = content WHERE organization_id = $1 AND document_id = $2", 1, 2+2*i),
			rowsText(e, c, "SELECT count(*) FROM documents WHERE organization_id = 1", pgx.QueryExecModeSimpleProtocol),
		} {
			if len(r) > 0 && strings.HasPrefix(r[len(r)-1], "error") {
				errs++
			}
		}
	}
	others, err := e.customerRows("SELECT coalesce(nullif(application_name, ''), '-') || ' ' || usename FROM pg_stat_activity WHERE datname = current_database() AND backend_type = 'client backend' AND pid <> pg_backend_pid() AND application_name <> 'dp3'")
	if err != nil {
		return false, "", err
	}
	n := directCount(e.proxyRoutes(), "1")
	return len(others) == 0 && errs == 0 && n >= 80, fmt.Sprintf("80 statements of organization 1: errors %d, on the direct path %d; other connections to its database: %q", errs, n, others), nil
}

// DP9. org-profile fdw under a load of reads and counter UPDATEs on the direct path: the
// switch waits for the direct statements in flight (they hold the pause lock shared), every
// UPDATE is counted once, and no statement goes direct after the switch. Each worker counts
// in a row of its own: through the FDW concurrent UPDATEs of one row fail with 40001
// (postgres_fdw's REPEATABLE READ), which is not what the check is about. (move-back of block
// 6 takes the same pause: this check is its form until block 6 is on main.)
func checkDirectSwitch(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	ids, err := e.customerRows("SELECT id::text FROM documents WHERE organization_id = 1 ORDER BY id LIMIT 4")
	if err != nil || len(ids) < 4 {
		return false, "", fmt.Errorf("4 documents of organization 1: %v %v", ids, err)
	}
	if err := execSQL(e.ctx, e.customer(), "UPDATE documents SET title = '0' WHERE id IN ("+strings.Join(ids, ", ")+")"); err != nil {
		return false, "", err
	}
	var updates, reads, errs atomic.Int64
	var firstErr atomic.Value
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		c, err := e.viaProxyPgx("dp9")
		if err != nil {
			return false, "", err
		}
		defer c.Close(e.ctx)
		doc := ids[w]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopLoad:
					return
				default:
				}
				if _, err := c.Exec(e.ctx, "UPDATE documents SET title = (title::int + 1)::text WHERE organization_id = $1 AND id = $2", 1, doc); err != nil {
					errs.Add(1)
					firstErr.CompareAndSwap(nil, firstErrLine(err))
				} else {
					updates.Add(1)
				}
				if r := rowsText(e, c, "SELECT count(*) FROM documents WHERE organization_id = $1", 1); len(r) > 0 && strings.HasPrefix(r[len(r)-1], "error") {
					errs.Add(1)
				} else {
					reads.Add(1)
				}
			}
		}()
	}
	time.Sleep(1500 * time.Millisecond)
	out, swErr := e.orgProfile(1, "fdw")
	switched := time.Now()
	time.Sleep(1500 * time.Millisecond)
	close(stopLoad)
	wg.Wait()
	if swErr != nil {
		return false, "org-profile fdw under load: " + lastErr(out), nil
	}
	// the switch's own time: its log line "organization profile set"
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "organization profile set") {
			if i := strings.Index(l, "time="); i >= 0 {
				ts := strings.SplitN(l[i+5:], " ", 2)[0]
				if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
					switched = t
				}
			}
		}
	}
	final, err := one(e.ctx, e.customer(), "SELECT sum(title::int)::text FROM documents WHERE id IN ("+strings.Join(ids, ", ")+")")
	if err != nil {
		return false, "", err
	}
	// the proxy's and org-profile's logs keep the time to the nanosecond, on one clock (both
	// run here): a direct statement logs its route under the pause it holds, the switch logs
	// once its UPDATE committed under the exclusive pause, so every direct one is before it
	before, after, fdwAfter := 0, 0, 0
	var late []string
	for _, r := range e.proxyRoutes() {
		switch {
		case r.org != "1":
		case r.direct && r.at.Before(switched):
			before++
		case r.direct:
			after++
			late = append(late, r.at.Format(time.RFC3339Nano))
		case r.at.After(switched):
			fdwAfter++
		}
	}
	fe, _ := firstErr.Load().(string)
	ok := errs.Load() == 0 && final == fmt.Sprint(updates.Load()) && before > 0 && after == 0 && fdwAfter > 0
	return ok, fmt.Sprintf("%d UPDATEs and %d reads, %d errors %q; the counters hold %s (want %d); direct statements before the switch %d, after it %d (want 0) %v, the switch at %s; through the FDW after it %d",
		updates.Load(), reads.Load(), errs.Load(), fe, final, updates.Load(), before, after, late, switched.Format(time.RFC3339Nano), fdwAfter), nil
}

// DP10. The organization's database knows the proxy's -customer-app role alone, the FDW path
// checks the client's privileges on the cloud's table (review of block 9, A). A role with
// SELECT on documents reads on the direct path; its UPDATE fails with 42501 in fdw and in
// fdw+direct alike, and so does its DELETE; the row stays. The role reads lokadb's tables (the
// proxy loads a session's metadata under the session's role).
func checkDirectPrivileges(e *env) (bool, string, error) {
	ro := "dp10_" + e.db
	if err := role(e, e.cloudHost, ro); err != nil {
		return false, "", err
	}
	// before enable, which carries it to the partitions' parent and gives USAGE on lokadb
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO "+ro); err != nil {
		return false, "", err
	}
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	// the proxy loads a session's metadata under its role: without reading lokadb's tables it
	// routes nothing for it (and the direct path never opens); the grant gives no UPDATE
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON ALL TABLES IN SCHEMA lokadb TO "+ro); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	title, err := one(e.ctx, e.customer(), "SELECT title FROM documents WHERE id = "+doc)
	if err != nil {
		return false, "", err
	}
	try := func() (read, upd, del string, direct int, err error) {
		c, err := pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable application_name=dp10", e.port, e.db, ro, ro))
		if err != nil {
			return "", "", "", 0, err
		}
		defer c.Close(e.ctx)
		n := directCount(e.proxyRoutes(), "1")
		read = strings.Join(rowsText(e, c, "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", 1, doc), " ")
		direct = directCount(e.proxyRoutes(), "1") - n
		upd = strings.Join(rowsText(e, c, "UPDATE documents SET title = 'dp10' WHERE organization_id = $1 AND id = $2", 1, doc), " ")
		del = strings.Join(rowsText(e, c, "DELETE FROM documents WHERE organization_id = $1 AND id = $2", 1, doc), " ")
		return read, upd, del, direct, nil
	}
	fr, fu, fd, _, err := try()
	if err != nil {
		return false, "", err
	}
	notes := fmt.Sprintf("fdw: read %q, UPDATE %q, DELETE %q", fr, fu, fd)
	denied := func(r string) bool { return strings.Contains(r, "42501") || strings.Contains(r, "permission denied") }
	// fr is reported, not judged: the FDW path sends a read of one moved organization to its
	// foreign partition, which the role has no privilege on (42501 although it may read the
	// parent); that is the FDW path's, outside the direct path
	ok := denied(fu) && denied(fd)
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, notes + "; org-profile fdw+direct refused: " + lastErr(out), nil
	}
	dr, du, dd, n, err := try()
	if err != nil {
		return false, "", err
	}
	after, err := one(e.ctx, e.customer(), "SELECT coalesce((SELECT title FROM documents WHERE id = "+doc+"), 'deleted')")
	if err != nil {
		return false, "", err
	}
	ok = ok && strings.Contains(dr, title) && n == 1 && denied(du) && denied(dd) && after == title
	return ok, fmt.Sprintf("%s; fdw+direct: read %q (on the direct path %d of 1), UPDATE %q, DELETE %q; the row's title %q (was %q)%s",
		notes, dr, n, du, dd, after, title, e.proxyLines("direct path")+e.proxyLines("metadata not loaded")), nil
}

// DP11. The functions of pg_catalog that answer otherwise in the organization's database
// (review of block 9, B): with them the statement stays on the cloud's path and answers as
// in fdw (current_user the client's role, not the -customer-app one); a plain read next to
// it goes direct. So does an operator of an extension the cloud has and the organization's
// database has not (pg_trgm's %): the gate finds it outside pg_catalog.
func checkDirectCloudFuncs(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(), "CREATE EXTENSION IF NOT EXISTS pg_trgm"); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	qs := []string{
		"SELECT current_user, title FROM documents WHERE organization_id = $1 AND id = $2",
		"SELECT current_database() = current_database(), title FROM documents WHERE organization_id = $1 AND id = $2",
		"SELECT pg_backend_pid() > 0, title FROM documents WHERE organization_id = $1 AND id = $2",
		"SELECT title % title, title FROM documents WHERE organization_id = $1 AND id = $2",
		"SELECT title FROM documents WHERE organization_id = $1 AND id = $2",
	}
	run := func() ([]string, []int, error) {
		c, err := e.viaProxyPgx("dp11")
		if err != nil {
			return nil, nil, err
		}
		defer c.Close(e.ctx)
		var out []string
		var direct []int
		for _, q := range qs {
			n := directCount(e.proxyRoutes(), "1")
			out = append(out, strings.Join(rowsText(e, c, q, 1, doc), " "))
			direct = append(direct, directCount(e.proxyRoutes(), "1")-n)
		}
		return out, direct, nil
	}
	fdw, _, err := run()
	if err != nil {
		return false, "", err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, fmt.Sprintf("fdw: %q; org-profile fdw+direct refused: %s", fdw, lastErr(out)), nil
	}
	got, direct, err := run()
	if err != nil {
		return false, "", err
	}
	ok := reflect.DeepEqual(fdw, got) && reflect.DeepEqual(direct, []int{0, 0, 0, 0, 1}) && got[0] == "app"+got[4] // current_user of the cloud, then the title
	return ok, fmt.Sprintf("fdw %q; fdw+direct %q; on the direct path %v (want [0 0 0 0 1])", fdw, got, direct), nil
}
