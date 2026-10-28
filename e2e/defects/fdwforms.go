package main

// Query forms on the FDW path (tasks/BACKLOG.md, block 2) for organization 1 in its own
// database.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "F1", name: "an upsert of one row of a moved organization reads and locks that row only (REALISTIC_REVIEW 1.2)", run: checkUpsertReach},
		check{id: "F2", name: "Prisma's UPDATE, DELETE and upsert with \"public\".\"t\".\"col\" references (REALISTIC_REVIEW P1, P2)", run: checkSchemaRefs},
		check{id: "F4", name: "MERGE into a LokaDB table once one organization has its own database (REALISTIC_REVIEW P5)", run: checkMerge},
		check{id: "F5", name: "an upsert DO UPDATE … WHERE of a new key inserts the row as given, not updated on top", run: checkUpsertNewKeyWhere},
		check{id: "F6", name: "GROUP BY and ORDER BY … LIMIT of one moved organization run in its database (REALISTIC_REVIEW 1.6)", run: checkAggregates},
		check{id: "F7", name: "an upsert with its own WITH, and RETURNING of rows with a NULL key, as Postgres does (P13)", run: checkUpsertP13},
		check{id: "F8", name: "COPY FROM into a paused organization waits for the pause and writes nothing until it ends", run: checkCopyPaused},
		check{id: "F9", name: "a cloud organization's upsert: atomic against a concurrent one, never another organization's row", run: checkLocalUpsert},
		check{id: "F10", name: "a moved organization's upsert waits for a concurrent one of the same key and updates the row, no unique violation (block 2, item 9 (b))", run: checkMovedUpsertLock},
		check{id: "F11", name: "an upsert of a cloud organization whose DO UPDATE assigns the organization column does not move the row to another organization (R6 of the routing review)", run: checkLocalUpsertSetsOrg},
		check{id: "F12", name: "a feed (ORDER BY … LIMIT) of a moved organization reaches its database with the LIMIT without an ANALYZE by hand: move-org gives the foreign partition its statistics (Z2 of block 14's trial)", run: checkFeedPushdown},
		check{id: "F3", name: "COPY documents TO gives every row; COPY FROM puts each row in its organization (REALISTIC_REVIEW 1.6, block 14 LD1)", run: checkCopyTable},
	)
}

// F3. Under FORCE ROW LEVEL SECURITY `COPY t TO` reads ONLY the partitioned parent: an empty
// copy, no error. COPY FROM Postgres refuses under RLS (pgx CopyFrom, psycopg copy, \copy).
func checkCopyTable(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	ok := true
	var notes []string
	for _, sql := range []string{"COPY documents TO STDOUT", "COPY public.documents (id, title) TO STDOUT WITH (FORMAT csv)"} {
		var buf strings.Builder
		tag, cpErr := c.CopyTo(e.ctx, &buf, sql)
		rows := strings.Count(buf.String(), "\n")
		if cpErr != nil || fmt.Sprint(rows) != want {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: %d of %s rows, tag %q (error %s)", sql, rows, want, tag.String(), short(cpErr)))
	}
	count := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + short(err)
		}
		return v
	}
	// rows of organization 1 (its own database) and 2 (the cloud) in one COPY
	tag, cpErr := c.CopyFrom(e.ctx, strings.NewReader("1\t2\tf3-org1\n2\t2\tf3-org2\n1\t3\tf3-org1\n"), "COPY documents (organization_id, user_id, title) FROM STDIN")
	at1 := count(e.customer(), "SELECT count(*) FROM documents WHERE title = 'f3-org1'")
	in2 := count(e.cloud(), "SELECT count(*) FROM documents WHERE title = 'f3-org2' AND organization_id = 2")
	cloud1 := count(e.cloud(), "SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE title = 'f3-org1' AND c.relkind = 'r'")
	if cpErr != nil || tag.String() != "COPY 3" || at1 != "2" || in2 != "1" || cloud1 != "0" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("COPY FROM, two organizations: tag %q, organization 1 at the customer %s (want 2), in the cloud %s, organization 2 in the cloud %s (error %s)", tag.String(), at1, cloud1, in2, short(cpErr)))
	// a child table: the organization comes from the parent document
	doc1 := count(e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	doc2 := count(e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	_, cpErr = c.CopyFrom(e.ctx, strings.NewReader(doc1+"\tf3-v1\n"+doc2+"\tf3-v2\n"), "COPY document_versions (document_id, content) FROM STDIN")
	v1 := count(e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'f3-v1'")
	v2 := count(e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'f3-v2' AND organization_id = 2")
	if cpErr != nil || v1 != "1" || v2 != "1" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("COPY FROM into the child table: organization 1's version at the customer %s, organization 2's in the cloud %s (error %s)", v1, v2, short(cpErr)))
	// a row that fails (no such user) in the cloud organization, where the FK to users is
	// checked (an organization's own database does not check FKs to cloud tables, README):
	// the whole COPY fails, the good row of organization 1 is not written either
	_, cpErr = c.CopyFrom(e.ctx, strings.NewReader("1\t2\tf3-bad\n2\t999999\tf3-bad\n"), "COPY documents (organization_id, user_id, title) FROM STDIN")
	bad := count(e.cloud(), "SELECT count(*) FROM documents WHERE title = 'f3-bad'")
	if cpErr == nil || bad != "0" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("COPY FROM with a bad row: %s rows written (error %s)", bad, short(cpErr)))
	// the connection goes on
	_, qErr := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 2")
	if qErr != nil {
		ok = false
	}
	notes = append(notes, "a query after: "+short(qErr))
	return ok, strings.Join(notes, "; "), nil
}

// F4. Postgres refuses MERGE on a partitioned table with a foreign partition, for every
// organization: "cannot execute MERGE on relation documents__org1". For the organization in
// its own database the rewritten statement must also keep MERGE's order: matched is decided
// before anything changes, and the UPDATE does not reach the rows the INSERT added.
func checkMerge(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		ids, err := one(e.ctx, e.cloud(), "SELECT min(id) || ',' || max(id) || ',' || (SELECT max(id) + 1000 FROM documents) FROM documents WHERE organization_id = "+org)
		if err != nil {
			return false, "", err
		}
		f := strings.Split(ids, ",")
		upd, del, fresh := f[0], f[1], f[2]
		before, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE id = "+upd)
		_, err1 := simple(e.ctx, c, `MERGE INTO documents d USING (VALUES (`+upd+`::bigint, `+org+`::bigint, '-m'), (`+fresh+`, `+org+`, 'new')) s(id, org, title)
			ON d.id = s.id AND d.organization_id = `+org+`
			WHEN MATCHED THEN UPDATE SET title = d.title || s.title
			WHEN NOT MATCHED THEN INSERT (id, organization_id, title) VALUES (s.id, s.org, s.title)`)
		_, err2 := simple(e.ctx, c, `MERGE INTO documents d USING (VALUES (`+del+`::bigint), (`+fresh+`+1)) s(id)
			ON d.id = s.id AND d.organization_id = `+org+`
			WHEN MATCHED THEN DELETE
			WHEN NOT MATCHED THEN INSERT (id, organization_id, title) VALUES (s.id, `+org+`, 'new2')`)
		got, _ := one(e.ctx, e.cloud(), fmt.Sprintf(`SELECT (SELECT title FROM documents WHERE id = %s) || ' / ' || (SELECT coalesce(string_agg(title, ','), '-') FROM documents WHERE id IN (%s, %s + 1)) || ' / deleted ' || (SELECT count(*) = 0 FROM documents WHERE id = %s)`, upd, fresh, fresh, del))
		want := before + "-m / new,new2 / deleted true"
		if err1 != nil || err2 != nil || got != want {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("organization %s: %s, want %s (errors %s; %s)", org, got, want, short(err1), short(err2)))
	}
	return ok, strings.Join(notes, "; "), nil
}

// F5. The rewritten DO UPDATE … WHERE ran its UPDATE after the INSERT. In the cloud one
// snapshot hides the inserted row from it; the organization's database (postgres_fdw's
// transaction sees its own writes) updated the new row too.
func checkUpsertNewKeyWhere(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		fresh, err := one(e.ctx, e.cloud(), "SELECT max(id) + 1000 FROM documents")
		if err != nil {
			return false, "", err
		}
		_, upErr := simple(e.ctx, c, "INSERT INTO documents (id, organization_id, title) VALUES ("+fresh+", "+org+", 'f5') ON CONFLICT (id) DO UPDATE SET title = documents.title || '!' WHERE documents.title IS NOT NULL")
		got, _ := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(title, ','), '-') FROM documents WHERE id = "+fresh)
		if upErr != nil || got != "f5" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("organization %s: stored %s (error %s)", org, got, short(upErr)))
	}
	return ok, strings.Join(notes, "; "), nil
}

// remoteSQL runs sql through the proxy while the organization's table is locked and returns
// what the organization's database was asked (the waiting FDW session's query) and the
// result's rows.
func (e *env) remoteSQL(c *pgconn.PgConn, sql string) (string, [][]string, error) {
	return e.remoteOf(func() ([]*pgconn.Result, error) { return simple(e.ctx, c, sql) })
}

// remoteOf is remoteSQL of what run sends.
func (e *env) remoteOf(run func() ([]*pgconn.Result, error)) (string, [][]string, error) {
	lk, err := pgconnTx(e, e.customer(), "BEGIN; LOCK TABLE documents IN ACCESS EXCLUSIVE MODE")
	if err != nil {
		return "", nil, err
	}
	type res struct {
		rows [][]string
		err  error
	}
	done := make(chan res, 1)
	go func() {
		r, err := run()
		var rows [][]string
		if err == nil && len(r) > 0 {
			for _, row := range r[len(r)-1].Rows {
				var cells []string
				for _, v := range row {
					cells = append(cells, string(v))
				}
				rows = append(rows, cells)
			}
		}
		done <- res{rows, err}
	}()
	var remote string
	for i := 0; i < 100 && remote == ""; i++ {
		remote, _ = one(e.ctx, e.customer(), "SELECT coalesce(max(query), '') FROM pg_stat_activity WHERE usename = 'lokadb_app' AND wait_event_type = 'Lock'")
		if remote == "" {
			time.Sleep(100 * time.Millisecond)
		}
	}
	_, _ = simple(e.ctx, lk, "COMMIT")
	lk.Close(e.ctx)
	r := <-done
	return oneLine(remote), r.rows, r.err
}

// F6. Through the parent the RLS filter stays in the cloud, and postgres_fdw sends nothing
// above the scan: a dashboard's count(*) … GROUP BY fetched every row of the organization.
// Addressed directly, the foreign partition gets the whole query.
func checkAggregates(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// Without statistics on the foreign table (autovacuum never analyzes one) the planner
	// takes the organization's table for a dozen rows and aggregates in the cloud.
	if err := execSQL(e.ctx, e.cloud(), "ANALYZE documents__org1"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, q := range []struct{ sql, remote, want string }{
		{"SELECT user_id, count(*) FROM documents WHERE organization_id = 1 GROUP BY user_id ORDER BY user_id",
			"GROUP BY", "SELECT user_id::text, count(*)::text FROM documents WHERE organization_id = 1 GROUP BY user_id ORDER BY user_id"},
		{"SELECT id, title FROM documents WHERE organization_id = 1 ORDER BY id DESC LIMIT 3",
			"LIMIT", "SELECT id::text, title FROM documents d WHERE organization_id = 1 ORDER BY d.id DESC LIMIT 3"},
	} {
		remote, rows, qErr := e.remoteSQL(c, q.sql)
		want, err := one(e.ctx, e.cloud(), "SELECT string_agg(r::text, ';' ORDER BY r::text) FROM ("+q.want+") r")
		if err != nil {
			return false, "", err
		}
		var got []string
		for _, r := range rows {
			got = append(got, "("+strings.Join(r, ",")+")")
		}
		sort.Strings(got)
		if qErr != nil || !strings.Contains(remote, q.remote) || strings.Join(got, ";") != want {
			ok = false
		}
		if len(remote) > 160 {
			remote = remote[:160] + "…"
		}
		notes = append(notes, fmt.Sprintf("%s → the organization's database got %q; rows %s, the cloud %s (error %s)", q.sql, remote, strings.Join(got, ";"), want, short(qErr)))
	}
	return ok, strings.Join(notes, "; "), nil
}

// F12 (Z2 of block 14's trial). The foreign partition of a moved organization had no statistics
// (autovacuum and a database-wide ANALYZE skip a foreign table), the planner took it for a
// handful of rows, and postgres_fdw sent neither ORDER BY nor LIMIT: a feed of the organization,
// addressed to its partition by the proxy, fetched every row of it (13–17 ms on 20 000 rows
// instead of 0.4, growing with the organization). move-org analyzes the partitions it attaches.
// Here no ANALYZE by the check: the feed, as a query string and with the organization as a
// parameter (an ORM's prepared statement), must reach the organization's database with its LIMIT.
func checkFeedPushdown(e *env) (bool, string, error) {
	// more rows than the LIMIT: a guess of a few rows makes the LIMIT cut nothing
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) SELECT 1, 2, 'feed ' || g FROM generate_series(1, 200) g"); err != nil {
		return false, "", err
	}
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	notes := []string{"statistics of documents__org1: " + func() string {
		v, err := one(e.ctx, e.cloud(), "SELECT reltuples::text FROM pg_class WHERE relname = 'documents__org1'")
		if err != nil {
			return firstErrLine(err)
		}
		return v + " rows (-1: none)"
	}()}
	want, err := one(e.ctx, e.cloud(), "SELECT string_agg(id::text, ',' ORDER BY id DESC) FROM (SELECT id FROM documents WHERE organization_id = 1 ORDER BY id DESC LIMIT 20) x")
	if err != nil {
		return false, "", err
	}
	feed := "SELECT id, title FROM documents WHERE organization_id = 1 ORDER BY id DESC LIMIT 20"
	param := "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id DESC LIMIT 20"
	for _, q := range []struct {
		form string
		run  func() ([]*pgconn.Result, error)
	}{
		{"query string", func() ([]*pgconn.Result, error) { return simple(e.ctx, c, feed) }},
		{"parameter $1", func() ([]*pgconn.Result, error) {
			r := c.ExecParams(e.ctx, param, [][]byte{[]byte("1")}, nil, nil, nil).Read()
			return []*pgconn.Result{r}, r.Err
		}},
	} {
		remote, rows, qErr := e.remoteOf(q.run)
		var ids []string
		for _, r := range rows {
			ids = append(ids, r[0])
		}
		got := strings.Join(ids, ",")
		s := fmt.Sprintf("%s: the organization's database got %q; ids %s (error %s)", q.form, remote, got, short(qErr))
		if qErr != nil || !strings.Contains(remote, "LIMIT") || got != want {
			ok = false
			s = "WRONG " + s + ", want ids " + want + " and a LIMIT"
		}
		notes = append(notes, s)
	}
	return ok, strings.Join(notes, "; "), nil
}

// F7 (research/ANALYSIS.md P13). The rewritten upsert dropped the statement's WITH, and its
// RETURNING matched inserted rows to the input by key: a row with a NULL key (it never
// conflicts) was inserted but not returned, and Django assigns the returned ids by position.
func checkUpsertP13(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// the key column and its unique index on both sides, as the operator would have them
	// (the DDL replay is not what this checks)
	if err := execSQL(e.ctx, e.customer(), "ALTER TABLE documents ADD COLUMN code text",
		"CREATE UNIQUE INDEX documents_org_code ON documents (organization_id, code)"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "ALTER TABLE documents ADD COLUMN code text",
		"SELECT lokadb.create_index('documents', 'CREATE UNIQUE INDEX __lokadb_idx__ ON __lokadb_part__ USING btree (organization_id, code)', 'documents_org_code')"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy() // connected after the DDL: the proxy reads the new column
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		if _, err := simple(e.ctx, c, "INSERT INTO documents (organization_id, code, title) VALUES ("+org+", 'a', 'a')"); err != nil {
			return false, "", err
		}
		res, retErr := simple(e.ctx, c, "INSERT INTO documents (organization_id, code, title) VALUES ("+org+", NULL, 'n1'), ("+org+", 'a', 'a2'), ("+org+", 'z', 'z'), ("+org+", NULL, 'n2') ON CONFLICT (organization_id, code) DO UPDATE SET title = EXCLUDED.title RETURNING title")
		var got []string
		if retErr == nil && len(res) > 0 {
			for _, r := range res[len(res)-1].Rows {
				got = append(got, string(r[0]))
			}
		}
		_, withErr := simple(e.ctx, c, "WITH src AS (SELECT "+org+"::bigint AS o, 'z'::text AS k) INSERT INTO documents (organization_id, code, title) SELECT o, k, 'z2' FROM src ON CONFLICT (organization_id, code) DO UPDATE SET title = EXCLUDED.title")
		z, _ := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(title, ','), '-') FROM documents WHERE organization_id = "+org+" AND code = 'z'")
		if retErr != nil || strings.Join(got, ",") != "n1,a2,z,n2" || withErr != nil || z != "z2" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("organization %s: RETURNING %s, want n1,a2,z,n2 (error %s); with WITH: %s (error %s)", org, strings.Join(got, ","), short(retErr), z, short(withErr)))
	}
	return ok, strings.Join(notes, "; "), nil
}

// F8. COPY FROM gives its rows their organizations inside the database (the staging table's
// trigger): the proxy cannot take one organization's pause lock for it. It takes the lock of
// statements not confined to one organization (org_lock(NULL)), which a move holds with the
// organization's own while it splits it out of DEFAULT (admin/split.go, pauseOrg).
func checkCopyPaused(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	mover, err := pgconnTx(e, e.cloud(), "SELECT pg_advisory_lock(lokadb.org_lock(1)), pg_advisory_lock(lokadb.org_lock(NULL))")
	if err != nil {
		return false, "", err
	}
	defer mover.Close(e.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := c.CopyFrom(e.ctx, strings.NewReader("1\t2\tf8\n"), "COPY documents (organization_id, user_id, title) FROM STDIN")
		done <- err
	}()
	var during string
	waited := false
	select {
	case err := <-done:
		during = "finished during the pause (error " + short(err) + ")"
	case <-time.After(3 * time.Second):
		waited = true
		during = "waited 3s"
	}
	written, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'f8'")
	if _, err := simple(e.ctx, mover, "SELECT pg_advisory_unlock(lokadb.org_lock(1)), pg_advisory_unlock(lokadb.org_lock(NULL))"); err != nil {
		return false, "", err
	}
	var after error
	if waited {
		after = <-done
	}
	final, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'f8'")
	ok := waited && written == "0" && after == nil && final == "1"
	return ok, fmt.Sprintf("during the pause: %s, %s rows at the customer; after it: %s rows (error %s)", during, written, final, short(after)), nil
}

// F9 (block 2, item 9). The rewritten upsert is NOT EXISTS + INSERT: a concurrent insert of
// the same key, not yet committed, is not seen, and the INSERT ends in a unique violation
// instead of DO UPDATE. A cloud organization's upsert now runs natively in its partition
// (DEFAULT). DEFAULT holds every cloud organization and has no policy: a key unique only
// within it must not let one organization's DO UPDATE reach another's row.
func checkLocalUpsert(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// the application's row trigger on the table (enable moves it to the parent)
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE audit (what text)", "GRANT INSERT ON audit TO app",
		"CREATE FUNCTION audit_row() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES ('row ' || coalesce(NEW.title, '-')); RETURN NULL; END $$",
		"CREATE FUNCTION audit_stmt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO audit VALUES ('statement'); RETURN NULL; END $$",
		"CREATE TRIGGER audit_row AFTER INSERT ON documents FOR EACH ROW EXECUTE FUNCTION audit_row()"); err != nil {
		return false, "", err
	}
	// the default policy: a statement the proxy cannot confine to one organization is refused
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	conn := func() (*pgconn.PgConn, error) { return e.viaProxy() }
	a, err := conn()
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	for _, q := range []string{
		"ALTER TABLE documents ADD COLUMN code text",
		"CREATE UNIQUE INDEX documents_code ON documents (code)",
		"INSERT INTO documents (organization_id, code, title) VALUES (3, 'x', 'org3')",
	} {
		if _, err := simple(e.ctx, a, q); err != nil {
			return false, "", fmt.Errorf("%s: %w", q, err)
		}
	}
	ok := true
	var notes []string
	// another organization's row with the same key
	_, err2 := simple(e.ctx, a, "INSERT INTO documents (organization_id, code, title) VALUES (2, 'x', 'org2') ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title")
	org3, _ := one(e.ctx, e.cloud(), "SELECT title || '/' || organization_id FROM documents WHERE code = 'x'")
	if org3 != "org3/3" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("organization 2's upsert of organization 3's key: the row is %s (error %s)", org3, short(err2)))
	// a concurrent upsert of a new key
	if _, err := simple(e.ctx, a, "BEGIN; INSERT INTO documents (organization_id, code, title) VALUES (2, 'k', 'a') ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title"); err != nil {
		return false, "", err
	}
	b, err := conn()
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	done := make(chan error, 1)
	go func() {
		_, err := simple(e.ctx, b, "INSERT INTO documents (organization_id, code, title) VALUES (2, 'k', 'b') ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title")
		done <- err
	}()
	time.Sleep(time.Second)
	if _, err := simple(e.ctx, a, "COMMIT"); err != nil {
		return false, "", err
	}
	errB := <-done
	k, _ := one(e.ctx, e.cloud(), "SELECT string_agg(title, ',') FROM documents WHERE code = 'k'")
	if errB != nil || k != "b" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("two upserts of one new key, the first not committed yet: the second %s, stored %s", short(errB), k))
	// what the parent does on a write, done on the partition too: the refs row (lokadb's
	// row trigger) and the application's row trigger. The same single-statement upsert as
	// the second one above, which only the native ON CONFLICT lets through.
	_, errD := simple(e.ctx, a, "INSERT INTO documents (organization_id, code, title) VALUES (2, 'd', 'd') ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title")
	refs, _ := one(e.ctx, e.cloud(), "SELECT coalesce((SELECT tenant::text FROM documents__refs WHERE id = (SELECT id FROM documents WHERE code = 'd')), 'none')")
	audit, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM audit WHERE what = 'row d'")
	if errD != nil || refs != "2" || audit != "1" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("the new row's refs: organization %s; the application's row trigger fired %s time(s)", refs, audit))
	// the organization from the parent's key, an expression the proxy cannot read in the
	// row's own column: never the partition (it does not check the row); under the default
	// policy such a statement is refused
	doc2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	_, errMis := simple(e.ctx, a, "INSERT INTO document_versions (id, document_id, organization_id, content) VALUES (900001, "+doc2+", 3 + 0, 'f9-mismatch') ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content")
	mis, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'f9-mismatch'")
	if errMis == nil || mis != "0" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("a row of organization 3 under organization 2's document: %s rows written (error %s)", mis, short(errMis)))
	// a statement trigger fires for statements on the parent only: with one, the upsert
	// stays there (a session connected after it sees it)
	if err := execSQL(e.ctx, e.cloud(), "CREATE TRIGGER audit_stmt AFTER INSERT ON documents FOR EACH STATEMENT EXECUTE FUNCTION audit_stmt()"); err != nil {
		return false, "", err
	}
	c3, err := conn()
	if err != nil {
		return false, "", err
	}
	defer c3.Close(e.ctx)
	_, errS := simple(e.ctx, c3, "INSERT INTO documents (organization_id, code, title) VALUES (2, 's', 'st') ON CONFLICT (code) DO UPDATE SET title = EXCLUDED.title")
	st, _ := one(e.ctx, e.cloud(), "SELECT string_agg(what, ',' ORDER BY what) FROM audit WHERE what IN ('row st', 'statement')")
	if errS != nil || st != "row st,statement" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("with the application's statement trigger: fired %s (error %s)", st, short(errS)))
	return ok, strings.Join(notes, "; "), nil
}

// F2. Prisma (and SQLAlchemy with a schema) name columns "public"."documents"."id". The
// UPDATE/DELETE sent to the foreign partition and the rewritten upsert name the table
// differently, and such a reference pointed to nothing: invalid reference to FROM-clause entry.
func checkSchemaRefs(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, org := range []string{"1", "2"} {
		ids, err := one(e.ctx, e.cloud(), "SELECT min(id) || ',' || max(id) FROM documents WHERE organization_id = "+org)
		if err != nil {
			return false, "", err
		}
		f := strings.Split(ids, ",")
		upd, del := f[0], f[1]
		var got []string
		for _, st := range []struct{ name, sql, want string }{
			{"UPDATE", `UPDATE "public"."documents" SET "title" = 'f2u' WHERE ("public"."documents"."id" = ` + upd + ` AND 1=1) RETURNING "public"."documents"."id", "public"."documents"."title"`,
				"SELECT title FROM documents WHERE id = " + upd},
			{"upsert", `INSERT INTO "public"."documents" ("id","organization_id","title") VALUES (` + upd + `,` + org + `,'f2i') ON CONFLICT ("id") DO UPDATE SET "title" = 'f2p' WHERE ("public"."documents"."id" = ` + upd + ` AND 1=1) RETURNING "public"."documents"."id", "public"."documents"."title"`,
				"SELECT title FROM documents WHERE id = " + upd},
			{"DELETE", `DELETE FROM "public"."documents" WHERE ("public"."documents"."id" IN (` + del + `) AND 1=1)`,
				"SELECT count(*) FROM documents WHERE id = " + del},
		} {
			_, stErr := simple(e.ctx, c, st.sql)
			v, _ := one(e.ctx, e.cloud(), st.want)
			if stErr != nil {
				ok = false
			}
			got = append(got, fmt.Sprintf("%s → %s (error %s)", st.name, v, short(stErr)))
		}
		if !strings.Contains(got[0], "f2u") || !strings.Contains(got[1], "f2p") || !strings.HasPrefix(got[2], "DELETE → 0 ") {
			ok = false
		}
		notes = append(notes, "organization "+org+": "+strings.Join(got, ", "))
	}
	return ok, strings.Join(notes, "; "), nil
}

// rowsRead is how many rows the organization's database has read from its documents so far.
// A backend reports its counters when it goes idle, at most once a second, and when it exits:
// every other session of that database has to end first (the cloud's FDW sessions end when
// their client closes; the check's own queries are sessions of their own too).
func (e *env) rowsRead() (int, error) {
	for i := 0; ; i++ {
		n, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND backend_type = 'client backend'")
		if err != nil {
			return 0, err
		}
		if n == "0" {
			break
		}
		if i > 150 {
			return 0, fmt.Errorf("the cloud's sessions to the organization's database do not end")
		}
		time.Sleep(100 * time.Millisecond)
	}
	s, err := one(e.ctx, e.customer(), "SELECT coalesce(sum(coalesce(seq_tup_read, 0) + coalesce(idx_tup_fetch, 0)), 0)::bigint FROM pg_stat_user_tables WHERE relname = 'documents'")
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(s)
}

// F1. The rewritten upsert (UPDATE … FROM v WHERE key = v.key) reached the foreign partition
// with no condition at all: postgres_fdw fetched every row of the organization FOR UPDATE and
// matched them in the cloud. One row upserted: every row read, every row locked.
func checkUpsertReach(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	// rowsRead waits for the cloud's sessions to the organization's database to end: with the
	// cache of cloud connections (on by default since 26.10 for a database with moved
	// organizations) a client's backend, and its postgres_fdw session, outlive the client
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{"upstream_pool": false})
	if err != nil {
		return false, "", err
	}
	defer stop()
	ids, err := one(e.ctx, e.cloud(), "SELECT min(id) || ',' || max(id) || ',' || count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	f := strings.Split(ids, ",")
	target, other := f[0], f[1]
	// Rows read in the organization's database are what its planner makes of the condition.
	// With 100 rows (2 pages) and statistics a seq scan is cheaper than the index, and the
	// count depended on whether autovacuum had analyzed the table yet (on a loaded stand it
	// had: 100 or 200 rows read, the condition there all the same). The organization gets
	// rows enough for the index to win, and fixed statistics; the condition missing still
	// reads every row.
	if err := execSQL(e.ctx, e.customer(),
		"INSERT INTO documents (id, organization_id, title) SELECT 50000000 + g, 1, 'f1 pad' FROM generate_series(1, 5000) g",
		"ANALYZE documents"); err != nil {
		return false, "", err
	}
	total, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
	plan, err := planOf(e.ctx, e.customer(), "EXPLAIN (COSTS OFF) SELECT id FROM public.documents WHERE id = ANY ('{"+target+"}'::bigint[]) FOR UPDATE")
	if err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	for _, form := range []struct{ name, sql string }{
		{"DO UPDATE", "INSERT INTO documents (id, organization_id, title) VALUES (" + target + ", 1, 'f1') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"},
		{"DO UPDATE, key with the organization", "INSERT INTO documents (id, organization_id, title) VALUES (" + target + ", 1, 'f1b') ON CONFLICT (id, organization_id) DO UPDATE SET title = EXCLUDED.title"},
		{"DO NOTHING", "INSERT INTO documents (id, organization_id, title) VALUES (" + target + ", 1, 'f1x') ON CONFLICT (id) DO NOTHING"},
		{"DO UPDATE from SELECT", "INSERT INTO documents (id, organization_id, title) SELECT * FROM unnest(ARRAY[" + target + "]::bigint[], ARRAY[1]::bigint[], ARRAY['f1c']) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"},
	} {
		before, err := e.rowsRead()
		if err != nil {
			return false, "", err
		}
		c, err := e.viaProxy()
		if err != nil {
			return false, "", err
		}
		_, upErr := simple(e.ctx, c, form.sql)
		c.Close(e.ctx)
		after, err := e.rowsRead()
		if err != nil {
			return false, "", err
		}
		read := after - before
		if upErr != nil || read > 2 {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: %d of %s rows read (error %s)", form.name, read, total, short(upErr)))
	}
	title, _ := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE id = "+target)
	if title != "f1c" {
		ok = false
	}
	// Another row of the organization is locked in its database: the upsert of the first
	// one must not wait for it.
	lk, err := pgconnTx(e, e.customer(), "BEGIN; SELECT 1 FROM documents WHERE id = "+other+" FOR UPDATE")
	if err != nil {
		return false, "", err
	}
	defer lk.Close(e.ctx)
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	start := time.Now()
	_, upErr := simple(e.ctx, c, "SET statement_timeout = '3s'; INSERT INTO documents (id, organization_id, title) VALUES ("+target+", 1, 'f1d') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title; RESET statement_timeout")
	took := time.Since(start).Round(100 * time.Millisecond)
	if upErr != nil {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("with another row locked: %s (error %s)", took, short(upErr)))
	return ok, strings.Join(notes, "; ") + "; stored " + title + "; the organization's plan for one key: " + plan, nil
}

// F10 (block 2, item 9 (b)). A moved organization keeps the rewritten upsert (no ON CONFLICT
// DO UPDATE for a foreign table): its NOT EXISTS did not see another transaction's insert of
// the same key, not committed yet, and its INSERT failed with a unique violation in the
// organization's database. The proxy now runs it under a lock of the table and organization.
// Both protocols: a simple query and a bound statement, each in autocommit.
func checkMovedUpsertLock(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	ok := true
	var notes []string
	upsert := "INSERT INTO documents (id, organization_id, user_id, title) VALUES (%s, 1, 1, '%s') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title"
	for _, c := range []struct {
		name, id string
		second   func(b *pgconn.PgConn) error
	}{
		{"simple query", "990001", func(b *pgconn.PgConn) error {
			_, err := simple(e.ctx, b, fmt.Sprintf(upsert, "990001", "b"))
			return err
		}},
		{"bound statement", "990002", func(b *pgconn.PgConn) error {
			return b.ExecParams(e.ctx, "INSERT INTO documents (id, organization_id, user_id, title) VALUES ($1, $2, 1, $3) ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title",
				[][]byte{[]byte("990002"), []byte("1"), []byte("b")}, nil, nil, nil).Read().Err
		}},
	} {
		a, err := e.viaProxy()
		if err != nil {
			return false, "", err
		}
		b, err := e.viaProxy()
		if err != nil {
			a.Close(e.ctx)
			return false, "", err
		}
		for _, q := range []string{"BEGIN", fmt.Sprintf(upsert, c.id, "a")} {
			if _, err := simple(e.ctx, a, q); err != nil {
				a.Close(e.ctx)
				b.Close(e.ctx)
				return false, "", fmt.Errorf("%s: %w", q, err)
			}
		}
		done := make(chan error, 1)
		go func() { done <- c.second(b) }()
		var early error
		select { // the second waits for the first
		case early = <-done:
		case <-time.After(time.Second):
		}
		_, errC := simple(e.ctx, a, "COMMIT")
		errB := early
		if early == nil {
			select {
			case errB = <-done:
			case <-time.After(30 * time.Second):
				errB = fmt.Errorf("still waiting 30 s after the first committed")
			}
		}
		got, _ := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(title, ','), 'none') FROM documents WHERE id = "+c.id)
		inCloud, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE d.id = "+c.id+" AND c.relkind = 'r'")
		if errC != nil || errB != nil || got != "b" || inCloud != "0" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: the second upsert %s, stored at the customer %s, in the cloud %s rows", c.name, short(errB), got, inCloud))
		a.Close(e.ctx)
		b.Close(e.ctx)
	}
	// nothing left locked: another upsert of the organization goes through at once
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := simple(e.ctx, c, "SET statement_timeout = '5s'"); err != nil {
		return false, "", err
	}
	start := time.Now()
	_, errN := simple(e.ctx, c, fmt.Sprintf(upsert, "990001", "c"))
	took := time.Since(start).Round(100 * time.Millisecond)
	locks, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory'")
	if errN != nil || locks != "0" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("an upsert after them: %s (error %s); advisory locks left in the cloud: %s", took, short(errN), locks))
	return ok, strings.Join(notes, "; "), nil
}

// planOf runs an EXPLAIN on d and gives its plan on one line.
func planOf(ctx context.Context, d, sql string) (string, error) {
	c, err := pgx.Connect(ctx, d)
	if err != nil {
		return "", err
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	var lines []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return "", err
		}
		lines = append(lines, strings.TrimSpace(l))
	}
	return strings.Join(lines, " / "), rows.Err()
}

// pgconnTx opens a connection to d and runs sql on it (a transaction it leaves open).
func pgconnTx(e *env, d, sql string) (*pgconn.PgConn, error) {
	c, err := pgconn.Connect(e.ctx, d)
	if err != nil {
		return nil, err
	}
	if _, err := simple(e.ctx, c, sql); err != nil {
		c.Close(e.ctx)
		return nil, err
	}
	return c, nil
}

// F11 (R6 of the routing review). A cloud organization's upsert goes natively into its local
// partition (DEFAULT, shared by the cloud organizations): there the parent's policy and
// lokadb.same_org are not, so DO UPDATE SET organization_id = 2 moved a row of organization 1 to
// organization 2 (its children stayed in 1). Through the parent the policy refuses it. The form
// Rails' upsert_all writes, SET organization_id = excluded.organization_id, is the same value
// and stays native.
func checkLocalUpsertSetsOrg(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	for _, q := range []string{
		"ALTER TABLE documents ADD COLUMN code text",
		"CREATE UNIQUE INDEX documents_code ON documents (code)",
		"INSERT INTO documents (organization_id, code, title) VALUES (1, 'r6', 'org1')",
	} {
		if _, err := simple(e.ctx, a, q); err != nil {
			return false, "", fmt.Errorf("%s: %w", q, err)
		}
	}
	ok := true
	var notes []string
	state := func() string {
		v, _ := one(e.ctx, e.cloud(), "SELECT organization_id || '/' || title FROM documents WHERE code = 'r6'")
		return v
	}
	_, errMove := simple(e.ctx, a, "INSERT INTO documents (organization_id, code, title) VALUES (1, 'r6', 'x') ON CONFLICT (code) DO UPDATE SET organization_id = 2, title = EXCLUDED.title")
	moved := state()
	if moved != "1/org1" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("SET organization_id = 2: the row is %s (error %s)", moved, short(errMove)))
	_, errSame := simple(e.ctx, a, "INSERT INTO documents (organization_id, code, title) VALUES (1, 'r6', 'same') ON CONFLICT (code) DO UPDATE SET organization_id = EXCLUDED.organization_id, title = EXCLUDED.title")
	same := state()
	if errSame != nil || same != "1/same" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("SET organization_id = excluded.organization_id: the row is %s (error %s)", same, short(errSame)))
	return ok, strings.Join(notes, "; "), nil
}
