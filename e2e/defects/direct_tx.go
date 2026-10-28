package main

// Transactions on the direct path, profile fdw+direct (block 10, stage 8a.2,
// tasks/direct-path.md, section 4): a transaction whose first statement goes direct runs in
// the organization's database, the cloud's transaction holding its pause and its refs rows;
// COMMIT commits the organization, then the cloud. pgx's Begin sends a simple "begin", its
// statements go by the extended protocol: the way most applications run.

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "DP8", name: "a transaction on the direct path: reads and writes of the organization in its database, a cloud statement in the cloud, COMMIT shows both, ROLLBACK neither", run: checkDirectTx},
		check{id: "DP16", name: "move-back under direct transactions: every committed transaction is in the cloud once, none on the direct path after the switch", run: checkDirectTxMoveBack},
		check{id: "DP17", name: "a form learnt for the direct path that branches (a LokaDB statement that needs the cloud) is refused once (0A000), rolls back whole, and then runs through the cloud", run: checkDirectTxRefused},
		check{id: "DP19", name: "the cooperative mode in a direct transaction: SET LOCAL lokadb.tenant after BEGIN, statements without conditions of their own; learnt through the cloud, then direct, the same rows", run: checkDirectTxTenant},
		check{id: "DP21", name: "the cooperative mode by set_config in a transaction (pgx, the extended protocol): the same rows on fdw and fdw+direct, never refused as a fanout", run: checkDirectTxSetConfig},
		check{id: "DP24", name: "COMMIT by the extended protocol ends a direct transaction on both sides (it committed the cloud's alone, the organization's UPDATE left uncommitted)", run: checkDirectTxExtendedCommit},
		check{id: "DP25", name: "JDBC with autocommit off: BEGIN by the extended protocol in one pipeline with the first statement, COMMIT by it: two transactions learnt through the cloud, the third direct", run: checkDirectTxJDBC},
		check{id: "DP26", name: "fewer exchanges in a direct transaction (item 9): one gate check per statement form, no SET CONSTRAINTS without work in the cloud (the cloud's pg_stat_statements)", run: checkDirectTxTrips},
		check{id: "DP18", name: "Django's delete with an ORM cascade on fdw+direct: never refused; two of its transactions go through the cloud, the third direct", run: checkDirectTxDjangoCascade},
	)
}

// DP8. pgx Begin; an UPDATE and an INSERT of organization 1's versions (direct), a read of
// users (the cloud's table, in the cloud's open transaction); Commit: both sides show it, the
// new key in __refs. Then the same with Rollback: nothing on either side.
func checkDirectTx(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("dp8")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	run := func(mark string, commit bool) (string, int, error) {
		n := directCount(e.proxyRoutes(), "1")
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return "", 0, err
		}
		var id int64
		steps := []error{
			exec1(e, tx, "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark+"-u", 1, ver),
			tx.QueryRow(e.ctx, "SELECT count(*) FROM users").Scan(new(int64)),
			tx.QueryRow(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, $2) RETURNING id", doc, mark+"-i").Scan(&id),
		}
		for _, err := range steps {
			if err != nil {
				_ = tx.Rollback(e.ctx)
				return "", 0, err
			}
		}
		if commit {
			err = tx.Commit(e.ctx)
		} else {
			err = tx.Rollback(e.ctx)
		}
		return fmt.Sprint(id), directCount(e.proxyRoutes(), "1") - n, err
	}
	// learn first: two committed transactions of the form go through the cloud, the third direct
	warm := 0
	for _, m := range []string{"dp8w1", "dp8w2"} {
		_, n, err := run(m, true)
		if err != nil {
			return false, "a transaction learnt through the cloud: " + firstErrLine(err), nil
		}
		warm += n
	}
	id, direct, err := run("dp8c", true)
	if err != nil {
		return false, "committed transaction: " + firstErrLine(err), nil
	}
	upd, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	ins, err := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'dp8c-i' AND id = "+id)
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf([]string{id})
	if err != nil {
		return false, "", err
	}
	ok := warm == 0 && direct == 2 && upd == "dp8c-u" && ins == "1" && refs == "1"
	notes := fmt.Sprintf("two transactions learnt through the cloud: on the direct path %d (want 0); then COMMIT: on the direct path %d (want 2), the UPDATE %q, the INSERT %s row(s) under key %s, __refs %s", warm, direct, upd, ins, id, refs)
	rid, rdirect, err := run("dp8r", false)
	if err != nil {
		return false, notes + "; rolled-back transaction: " + firstErrLine(err), nil
	}
	upd2, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	ins2, err := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'dp8r-i'")
	if err != nil {
		return false, "", err
	}
	refs2, err := e.refsOf([]string{rid})
	if err != nil {
		return false, "", err
	}
	ok = ok && rdirect == 2 && upd2 == "dp8c-u" && ins2 == "0" && refs2 == "0"
	return ok, notes + fmt.Sprintf("; ROLLBACK: on the direct path %d, the UPDATE left %q, the INSERT %s row(s), __refs of its key %s %s", rdirect, upd2, ins2, rid, refs2), nil
}

func exec1(e *env, tx pgx.Tx, sql string, args ...any) error {
	_, err := tx.Exec(e.ctx, sql, args...)
	return err
}

// DP16. move-back of organization 1 while four clients run transactions on the direct path
// (an INSERT and an UPDATE of a version each, then COMMIT): the switch waits for those in
// flight (the cloud's transaction holds the pause); after it every transaction a client saw
// committed is in the cloud exactly once, with its key in __refs, and no statement went the
// direct path after the switch.
func checkDirectTxMoveBack(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var mu sync.Mutex
	var seen []string
	failed := 0
	firstErr := ""
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		c, err := e.viaProxyPgx("dp16")
		if err != nil {
			return false, "", err
		}
		defer c.Close(e.ctx)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 1; ; i++ {
				select {
				case <-stopLoad:
					return
				default:
				}
				mark := fmt.Sprintf("dp16-%d-%d", w, i)
				err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
					var id int64
					if err := tx.QueryRow(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, $2) RETURNING id", doc, mark).Scan(&id); err != nil {
						return err
					}
					_, err := tx.Exec(e.ctx, "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark, 1, id)
					return err
				})
				mu.Lock()
				if err != nil {
					failed++
					if firstErr == "" {
						firstErr = firstErrLine(err)
					}
				} else {
					seen = append(seen, mark)
				}
				mu.Unlock()
			}
		}(w)
	}
	time.Sleep(1500 * time.Millisecond)
	out, backErr := e.run6(e.moveBackArgs(1)...)
	switched := time.Now()
	time.Sleep(1500 * time.Millisecond)
	close(stopLoad)
	wg.Wait()
	if backErr != nil {
		return false, "move-back under direct transactions failed: " + errLine(backErr), nil
	}
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "msg=switched") {
			if i := strings.Index(l, "time="); i >= 0 {
				if t, err := time.Parse(time.RFC3339Nano, strings.SplitN(l[i+5:], " ", 2)[0]); err == nil {
					switched = t
				}
			}
		}
	}
	rows, err := e.cloudRows("SELECT content FROM document_versions WHERE organization_id = 1 AND content LIKE 'dp16-%'")
	if err != nil {
		return false, "", err
	}
	inCloud := map[string]int{}
	for _, r := range rows {
		inCloud[r]++
	}
	missing, twice := 0, 0
	for _, m := range seen {
		if inCloud[m] == 0 {
			missing++
		}
	}
	for _, n := range inCloud {
		if n > 1 {
			twice++
		}
	}
	unknown := len(inCloud) - (len(seen) - missing)
	refsGap, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM document_versions v WHERE v.organization_id = 1
		AND NOT EXISTS (SELECT FROM document_versions__refs r WHERE r.id = v.id AND r.tenant = 1)`)
	if err != nil {
		return false, "", err
	}
	before, after := 0, 0
	for _, r := range e.proxyRoutes() {
		switch {
		case r.org != "1" || !r.direct:
		case r.at.Before(switched):
			before++
		default:
			after++
		}
	}
	ok := len(seen) > 0 && missing == 0 && twice == 0 && unknown == 0 && refsGap == "0" && before > 0 && after == 0
	return ok, fmt.Sprintf("%d transactions committed, %d failed (%s); in the cloud after move-back: %d missing, %d twice, %d not seen committed; versions without their key in __refs %s; direct statements before the switch %d, after it %d (want 0); %s",
		len(seen), failed, firstErr, missing, twice, unknown, refsGap, before, after, pauseOf(out)), nil
}

// DP17. A form learnt for the direct path that branches. Its transactions read organization
// 1's documents and insert a version; two of them commit through the cloud (learn first), the
// third goes direct. Then the same form takes its other branch: after the same first statement
// it reads organization 2's documents, which only the cloud has: the proxy refuses that
// statement with 0A000, the client rolls back, nothing of the transaction is left on either
// side. Run again, the branch goes through the cloud from its first statement and commits;
// lokadb.direct_cloud_first holds the form.
func checkDirectTxRefused(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("dp17")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(mark string, branch bool) (int, error) {
		n := directCount(e.proxyRoutes(), "1")
		err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
			if err := tx.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = $1", 1).Scan(new(int64)); err != nil {
				return err
			}
			if _, err := tx.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, $2)", doc, mark); err != nil {
				return err
			}
			if branch {
				return tx.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = $1 AND id > 0", 2).Scan(new(int64))
			}
			return nil
		})
		return directCount(e.proxyRoutes(), "1") - n, err
	}
	var learn []int
	for _, m := range []string{"dp17-l1", "dp17-l2", "dp17-l3"} {
		n, err := run(m, false)
		if err != nil {
			return false, "the form's transactions: " + firstErrLine(err), nil
		}
		learn = append(learn, n)
	}
	learnt := learn[0] == 0 && learn[1] == 0 && learn[2] == 2
	direct1, err1 := run("dp17-a", true)
	var pe *pgconn.PgError
	refused := errors.As(err1, &pe) && pe.Code == "0A000"
	left, err := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'dp17-a'")
	if err != nil {
		return false, "", err
	}
	direct2, err2 := run("dp17-b", true)
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'dp17-b' AND organization_id = 1")
	if err != nil {
		return false, "", err
	}
	shapes, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.direct_cloud_first")
	if err != nil {
		return false, "", err
	}
	ok := learnt && refused && left == "0" && err2 == nil && direct2 == 0 && inCloud == "1" && shapes == "1"
	return ok, fmt.Sprintf("the form's three transactions on the direct path %v (want [0 0 2]: learnt through the cloud first); its branch: %v (0A000 %v), on the direct path %d, its INSERT left %s row(s); the branch again: error %v, on the direct path %d (want 0), its row in the cloud %s; lokadb.direct_cloud_first holds %s form(s)%s",
		learn, short(err1), refused, direct1, left, err2, direct2, inCloud, shapes, e.proxyLines("direct transaction refused")), nil
}

// DP18. The form Django's delete with an ORM cascade takes (the e2e's django failed on it
// before learn first): in one transaction its versions are read and deleted, then the
// document. Three such transactions on fdw+direct: none is refused, every one commits, the
// documents and their versions are gone; the first two go through the cloud (learnt), the
// third direct, every statement of it.
func checkDirectTxDjangoCascade(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	docs, err := e.customerRows("SELECT id::text FROM documents WHERE organization_id = 1 ORDER BY id LIMIT 3")
	if err != nil || len(docs) < 3 {
		return false, "", fmt.Errorf("3 documents of organization 1: %v %v", docs, err)
	}
	c, err := e.viaProxyPgx("dp18")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var errs []string
	var direct []int
	for _, d := range docs {
		n := directCount(e.proxyRoutes(), "1")
		err := pgx.BeginFunc(e.ctx, c, func(tx pgx.Tx) error {
			if err := tx.QueryRow(e.ctx, "SELECT count(*) FROM document_versions WHERE organization_id = $1 AND document_id = $2", 1, d).Scan(new(int64)); err != nil {
				return err
			}
			if _, err := tx.Exec(e.ctx, "DELETE FROM document_versions WHERE organization_id = $1 AND document_id = $2", 1, d); err != nil {
				return err
			}
			_, err := tx.Exec(e.ctx, "DELETE FROM documents WHERE organization_id = $1 AND id = $2", 1, d)
			return err
		})
		if err != nil {
			errs = append(errs, firstErrLine(err))
		}
		direct = append(direct, directCount(e.proxyRoutes(), "1")-n)
	}
	left, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE id IN ("+strings.Join(docs, ", ")+")")
	if err != nil {
		return false, "", err
	}
	vleft, err := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE document_id IN ("+strings.Join(docs, ", ")+")")
	if err != nil {
		return false, "", err
	}
	ok := len(errs) == 0 && fmt.Sprint(direct) == "[0 0 3]" && left == "0" && vleft == "0"
	return ok, fmt.Sprintf("3 deletes with the cascade: errors %q; on the direct path %v (want [0 0 3]: learnt through the cloud, then direct); documents left %s, versions left %s", errs, direct, left, vleft), nil
}

// DP19. The cooperative mode (8a.2, the second step): pgx Begin, SET LOCAL lokadb.tenant = 1, a
// count of document_versions and an UPDATE of a version by its content (neither has a
// condition on the organization), Commit. Two such transactions go through the cloud and
// teach the form, the third goes direct: both statements in organization 1's database, the
// count equal to the cloud's for organization 1, the UPDATE in its database.
func checkDirectTxTenant(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE document_versions SET content = 'dp19-0' WHERE id = "+ver); err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp19")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(mark string) (int64, int, error) {
		n := directCount(e.proxyRoutes(), "1")
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return 0, 0, err
		}
		var count int64
		steps := []error{
			exec1(e, tx, "SET LOCAL lokadb.tenant = 1"),
			tx.QueryRow(e.ctx, "SELECT count(*) FROM document_versions").Scan(&count),
			exec1(e, tx, "UPDATE document_versions SET content = $1 WHERE content LIKE 'dp19-%'", mark),
		}
		for _, err := range steps {
			if err != nil {
				_ = tx.Rollback(e.ctx)
				return 0, 0, err
			}
		}
		err = tx.Commit(e.ctx)
		return count, directCount(e.proxyRoutes(), "1") - n, err
	}
	warm := 0
	var counts []int64
	for _, m := range []string{"dp19-w1", "dp19-w2"} {
		n, d, err := run(m)
		if err != nil {
			return false, "a transaction learnt through the cloud: " + firstErrLine(err), nil
		}
		warm += d
		counts = append(counts, n)
	}
	n, direct, err := run("dp19-c")
	if err != nil {
		return false, "the learnt transaction: " + firstErrLine(err), nil
	}
	counts = append(counts, n)
	content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	ok := warm == 0 && direct == 2 && content == "dp19-c" && fmt.Sprint(counts) == fmt.Sprintf("[%s %s %s]", want, want, want)
	return ok, fmt.Sprintf("two transactions learnt through the cloud: on the direct path %d (want 0); the third: on the direct path %d (want 2); counts %v (the cloud's for organization 1: %s); the version in the organization's database %q (want dp19-c)",
		warm, direct, counts, want, content), nil
}

// DP21 (block 18's probe). pgx Begin, set_config('lokadb.tenant', $1, true) with '1' by the
// extended protocol (prepared while the BEGIN is deferred), a count of documents without a
// condition on the organization: on fdw and on fdw+direct the same count, no 0A000 (the
// transaction goes through the cloud: set_config's form is not held with BEGIN, it answers with a
// row; a direct cooperative transaction starts with SET LOCAL lokadb.tenant, DP19). On main
// 0606ff7 the count on fdw+direct was refused as a fanout: the prepare's Sync took the
// deferred transaction for an implicit one and forgot lokadb.tenant.
func checkDirectTxSetConfig(e *env) (bool, string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	run := func() string {
		c, err := e.viaProxyPgx("dp21")
		if err != nil {
			return "connect: " + firstErrLine(err)
		}
		defer c.Close(e.ctx)
		t, err := c.Begin(e.ctx)
		if err != nil {
			return "begin: " + firstErrLine(err)
		}
		defer t.Rollback(e.ctx)
		if _, err := t.Exec(e.ctx, "SELECT set_config('lokadb.tenant', $1, true)", "1"); err != nil {
			return "set_config: " + firstErrLine(err)
		}
		var n int64
		if err := t.QueryRow(e.ctx, "SELECT count(*) FROM documents WHERE title LIKE '%'").Scan(&n); err != nil {
			return "count: " + firstErrLine(err)
		}
		return fmt.Sprint(n)
	}
	fdw := run()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	var direct []string
	for i := 0; i < 3; i++ { // set_config(…, true) in a deferred transaction: the cloud's way, never learnt (SET LOCAL is the direct one, DP19)
		direct = append(direct, run())
	}
	ok := !strings.Contains(fdw, ":") && fdw != "0"
	for _, d := range direct {
		ok = ok && d == fdw
	}
	return ok, fmt.Sprintf("fdw: %s; fdw+direct, three transactions: %v", fdw, direct), nil
}

// DP24. A direct transaction (simple BEGIN, the UPDATE of DP8's form learnt first) ended by a
// COMMIT sent by the extended protocol (pgconn ExecParams: Parse, Bind, Describe, Execute,
// Sync, as JDBC and other drivers send it): the organization's UPDATE is committed, seen from
// another connection, and no direct connection stays idle in a transaction. Before the fix the
// COMMIT went to the cloud alone.
func checkDirectTxExtendedCommit(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp24")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(mark string, extended bool) (int, error) {
		n := directCount(e.proxyRoutes(), "1")
		if _, err := c.Exec(e.ctx, "BEGIN"); err != nil {
			return 0, err
		}
		if _, err := c.Exec(e.ctx, "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark, 1, ver); err != nil {
			_, _ = c.Exec(e.ctx, "ROLLBACK")
			return 0, err
		}
		if extended {
			if _, err := c.PgConn().ExecParams(e.ctx, "COMMIT", nil, nil, nil, nil).Close(); err != nil {
				return 0, err
			}
		} else if _, err := c.Exec(e.ctx, "COMMIT"); err != nil {
			return 0, err
		}
		return directCount(e.proxyRoutes(), "1") - n, nil
	}
	for _, m := range []string{"dp24-w1", "dp24-w2"} { // the form learnt through the cloud
		if _, err := run(m, false); err != nil {
			return false, "learning: " + firstErrLine(err), nil
		}
	}
	direct, err := run("dp24-x", true)
	if err != nil {
		return false, "the transaction with an extended COMMIT: " + firstErrLine(err), nil
	}
	content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	idle, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_stat_activity WHERE usename = 'lokadb_dml' AND state LIKE 'idle in transaction%'")
	if err != nil {
		return false, "", err
	}
	ok := direct == 1 && content == "dp24-x" && idle == "0"
	return ok, fmt.Sprintf("on the direct path %d (want 1); the organization's row %q (want dp24-x, committed); direct connections idle in a transaction %s (want 0)", direct, content, idle), nil
}

// DP25. The messages JDBC sends with autocommit off (pgconn's pipeline): Parse/Bind/Describe/
// Execute of BEGIN and Bind/Describe/Execute of a prepared UPDATE in one pipeline up to its
// Sync, then COMMIT by the extended protocol. Twice through the cloud (the form is learnt),
// the third time the UPDATE runs in the organization's database, committed on both sides.
func checkDirectTxJDBC(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp25")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	pc := c.PgConn()
	if _, err := pc.Prepare(e.ctx, "S_1", "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", nil); err != nil {
		return false, "", err
	}
	run := func(mark string) (int, error) {
		n := directCount(e.proxyRoutes(), "1")
		p := pc.StartPipeline(e.ctx)
		p.SendQueryParams("BEGIN", nil, nil, nil, nil)
		p.SendQueryPrepared("S_1", [][]byte{[]byte(mark), []byte("1"), []byte(ver)}, nil, nil)
		if err := p.Sync(); err != nil {
			return 0, err
		}
		for {
			r, err := p.GetResults()
			if err != nil {
				_ = p.Close()
				return 0, err
			}
			if r == nil {
				break
			}
			if rr, ok := r.(*pgconn.ResultReader); ok {
				if _, err := rr.Close(); err != nil {
					_ = p.Close()
					return 0, err
				}
			}
			if _, ok := r.(*pgconn.PipelineSync); ok {
				break
			}
		}
		if err := p.Close(); err != nil {
			return 0, err
		}
		if _, err := pc.ExecParams(e.ctx, "COMMIT", nil, nil, nil, nil).Close(); err != nil {
			return 0, err
		}
		return directCount(e.proxyRoutes(), "1") - n, nil
	}
	var directs []int
	for _, m := range []string{"dp25-a", "dp25-b", "dp25-c"} {
		d, err := run(m)
		if err != nil {
			return false, fmt.Sprintf("transaction %s: %s", m, firstErrLine(err)), nil
		}
		directs = append(directs, d)
	}
	content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	idle, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_stat_activity WHERE usename = 'lokadb_dml' AND state LIKE 'idle in transaction%'")
	if err != nil {
		return false, "", err
	}
	ok := fmt.Sprint(directs) == "[0 0 1]" && content == "dp25-c" && idle == "0"
	return ok, fmt.Sprintf("on the direct path per transaction %v (want [0 0 1]); the organization's row %q (want dp25-c); direct connections idle in a transaction %s (want 0)", directs, content, idle), nil
}

// DP26 (item 9). Transactions of two reads of one form and an UPDATE of organization 1, learnt
// and then direct three times, and a read-only one: the organization's BEGIN goes with the
// first statement, the second read's gate check is not repeated, and without work of the
// transaction's own in the cloud both sides commit at once (TestDirectTrips counts the
// exchanges; the defects stand has no pg_stat_statements). On a live server: every statement
// direct, the UPDATE committed at the organization, no connection left in a transaction there
// or in the cloud.
func checkDirectTxTrips(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp26")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	read := "SELECT content FROM document_versions WHERE organization_id = $1 AND id = $2"
	run := func(mark string) error {
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return err
		}
		steps := []func() error{
			func() error { return tx.QueryRow(e.ctx, read, 1, ver).Scan(new(string)) },
			func() error { return tx.QueryRow(e.ctx, read, 1, ver).Scan(new(string)) },
		}
		if mark != "" {
			steps = append(steps, func() error {
				return exec1(e, tx, "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark, 1, ver)
			})
		}
		for _, q := range steps {
			if err := q(); err != nil {
				_ = tx.Rollback(e.ctx)
				return err
			}
		}
		return tx.Commit(e.ctx)
	}
	for _, m := range []string{"dp26-w1", "dp26-w2"} { // learnt through the cloud
		if err := run(m); err != nil {
			return false, "learning: " + firstErrLine(err), nil
		}
	}
	n0 := directCount(e.proxyRoutes(), "1")
	for _, m := range []string{"dp26-a", "dp26-b", "dp26-c"} {
		if err := run(m); err != nil {
			return false, "a direct transaction: " + firstErrLine(err), nil
		}
	}
	direct := directCount(e.proxyRoutes(), "1") - n0
	if err := run(""); err != nil { // read-only: learnt by its first statement already
		return false, "a read-only transaction: " + firstErrLine(err), nil
	}
	readOnly := directCount(e.proxyRoutes(), "1") - n0 - direct
	content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
	if err != nil {
		return false, "", err
	}
	idleOrg, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_stat_activity WHERE usename = 'lokadb_dml' AND state LIKE 'idle in transaction%'")
	if err != nil {
		return false, "", err
	}
	idleCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'dp26' AND state LIKE 'idle in transaction%'")
	if err != nil {
		return false, "", err
	}
	ok := direct == 9 && readOnly == 2 && content == "dp26-c" && idleOrg == "0" && idleCloud == "0"
	return ok, fmt.Sprintf("three transactions: on the direct path %d (want 9); a read-only one %d (want 2); the row %q (want dp26-c); idle in a transaction: the organization's %s, the cloud's %s (want 0, 0)", direct, readOnly, content, idleOrg, idleCloud), nil
}
