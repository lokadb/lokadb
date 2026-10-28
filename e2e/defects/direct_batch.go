package main

// Batch writes on the direct path (block 2, BACKLOG block 10 item 8, 8b.2): an upsert, an
// INSERT … SELECT and a COPY of one moved organization in fdw+direct go to its database, not
// through the cloud's FDW, and keep __refs exact.

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "DU1", name: "an upsert of a moved organization in fdw+direct runs in its database (ON CONFLICT on the key and on a unique index): __refs holds exactly its new rows; one naming another organization's key goes the cloud's way", run: checkDirectUpsert},
		check{id: "DU4", name: "a COPY FROM STDIN of a moved organization in fdw+direct, the organization named for the session (tenant.local_only off), runs in its database: keys from the cloud, every one in __refs; a row of another organization fails the whole COPY, nothing stays", run: checkDirectCopy},
		check{id: "DU5", name: "the batch forms inside a direct transaction (step 4, default tenant.local_only): BEGIN; SET LOCAL lokadb.tenant; COPY FROM STDIN; COMMIT, and an upsert with an INSERT … SELECT, go direct once learnt; every row in __refs, nothing in the cloud", run: checkDirectBatchTx},
		check{id: "DU3", name: "Django 5.2's bulk_create (INSERT … SELECT * FROM unnest($1::bigint[], $2::text[]) without its key) of a moved organization in fdw+direct runs in its database: keys from the cloud, every one in __refs", run: checkDirectUnnest},
		check{id: "DU2", name: "an INSERT … SELECT naming its keys, of a moved organization in fdw+direct, runs in its database: its keys in __refs; a key of another organization refuses it whole", run: checkDirectInsertSelect},
	)
}

// DU1. Through the cloud an upsert of a moved organization is rewritten (NOT EXISTS + INSERT,
// the rows fetched over the FDW). Now each form below is one statement on the direct path, run
// natively in the organization's database; a key the cloud gave a row that was updated or
// skipped instead leaves __refs, so __refs maps to organization 1 exactly the rows its
// database holds. The unique index is created through the proxy (the DEFAULT partition, and
// the organization's by the replay), pgx asks binary formats.
func checkDirectUpsert(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	before, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM document_versions__refs")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("du1")
	if err != nil {
		return false, "", err
	}
	if _, err := c.Exec(e.ctx, "CREATE UNIQUE INDEX du1_content ON document_versions (content)"); err != nil {
		c.Close(e.ctx)
		return false, "", fmt.Errorf("the unique index: %w", err)
	}
	c.Close(e.ctx)
	if c, err = e.viaProxyPgx("du1"); err != nil { // a session with the index in its metadata
		return false, "", err
	}
	defer c.Close(e.ctx)
	base, err := strconv.ParseInt(before, 10, 64)
	if err != nil {
		return false, "", err
	}
	newKey := base + 1000000
	n0 := directCount(e.proxyRoutes(), "1")
	var got []string
	run := func(name, sql string, args ...any) {
		got = append(got, name+" "+strings.Join(rowsText(e, c, sql, args...), "|"))
	}
	run("insert both:", "INSERT INTO document_versions (document_id, content) VALUES ($1, 'du1-a'), ($1, 'du1-b') ON CONFLICT (content) DO NOTHING RETURNING id", doc)
	run("skip both:", "INSERT INTO document_versions (document_id, content) VALUES ($1, 'du1-a'), ($1, 'du1-b') ON CONFLICT (content) DO NOTHING RETURNING id", doc)
	run("update one, insert one:", "INSERT INTO document_versions (document_id, content) VALUES ($1, 'du1-a'), ($1, 'du1-c') ON CONFLICT (content) DO UPDATE SET content = EXCLUDED.content || '!'", doc)
	first, err := one(e.ctx, e.customer(), "SELECT min(id) FROM document_versions WHERE content = 'du1-b'")
	if err != nil {
		return false, "", err
	}
	firstKey, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return false, "", fmt.Errorf("the row 'du1-b': %q", first)
	}
	run("keys named:", "INSERT INTO document_versions (id, document_id, content) VALUES ($1, $2, 'du1-k'), ($3, $2, 'du1-n') ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content", firstKey, doc, newKey)
	direct := directCount(e.proxyRoutes(), "1") - n0
	rows, err := e.customerRows("SELECT id::text || ':' || content FROM document_versions WHERE organization_id = 1 AND (content LIKE 'du1-%' OR id > " + before + ") ORDER BY id")
	if err != nil {
		return false, "", err
	}
	var ids []string
	for _, r := range rows {
		ids = append(ids, strings.SplitN(r, ":", 2)[0])
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	newRefs, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs WHERE id > "+before)
	if err != nil {
		return false, "", err
	}
	newRows, err := e.customerRows("SELECT id::text FROM document_versions WHERE organization_id = 1 AND id > " + before)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE content LIKE 'du1-%'")
	if err != nil {
		return false, "", err
	}
	want := "du1-a!,du1-c,du1-k,du1-n"
	var contents []string
	for _, r := range rows {
		contents = append(contents, strings.SplitN(r, ":", 2)[1])
	}
	sort.Strings(contents)
	ok := direct == 4 && strings.Join(contents, ",") == want && refs == fmt.Sprint(len(ids)) && newRefs == fmt.Sprint(len(newRows)) && inCloud == "0"
	// a key __refs maps to organization 2: the organization's database has no conflict for it,
	// the row would go in and the key be in two organizations; the cloud's way instead
	k2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions__refs WHERE tenant = 2")
	if err != nil {
		return false, "", err
	}
	n1 := directCount(e.proxyRoutes(), "1")
	other := rowsText(e, c, "INSERT INTO document_versions (id, document_id, content) VALUES ($1, $2, 'du1-other') ON CONFLICT (id) DO NOTHING", k2, doc)
	otherDirect := directCount(e.proxyRoutes(), "1") - n1
	left, err := e.customerRows("SELECT id::text FROM document_versions WHERE content = 'du1-other'")
	if err != nil {
		return false, "", err
	}
	still, err := one(e.ctx, e.cloud(), "SELECT tenant::text FROM document_versions__refs WHERE id = "+k2)
	if err != nil {
		return false, "", err
	}
	ok = ok && otherDirect == 0 && len(left) == 0 && still == "2"
	// a role that may INSERT but not UPDATE: DO UPDATE refused (42501) as through the cloud's
	// parent, the row unchanged (the organization's database would let the direct role do it)
	ins := "du1i_" + e.db
	if err := role(e, "cloud", ins); err != nil {
		return false, "", err
	}
	// (SELECT on documents: the proxy finds a version's organization through documents__refs
	// under the client's role)
	for _, g := range []string{"GRANT SELECT, INSERT ON document_versions TO " + ins, "GRANT SELECT ON documents TO " + ins} {
		if _, err := c.Exec(e.ctx, g); err != nil {
			return false, "", fmt.Errorf("%s through the proxy: %w", g, err)
		}
	}
	ic, err := e.viaProxyAs(ins)
	if err != nil {
		return false, "", err
	}
	defer ic.Close(e.ctx)
	_, errU := simple(e.ctx, ic, "INSERT INTO document_versions (id, document_id, content) VALUES ("+fmt.Sprint(firstKey)+", "+doc+", 'du1-denied') ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content")
	denied := errU != nil && strings.Contains(errU.Error(), "SQLSTATE 42501")
	unchanged, err := e.customerRows("SELECT content FROM document_versions WHERE id = " + fmt.Sprint(firstKey))
	if err != nil {
		return false, "", err
	}
	ok = ok && denied && len(unchanged) == 1 && unchanged[0] == "du1-k"
	return ok, fmt.Sprintf("%s; on the direct path %d (want 4); the organization's database holds %v (want contents %s), __refs maps %s of them to organization 1; keys above %s: %s in __refs, %d rows; the cloud's partition holds %s; DO NOTHING with organization 2's key %s: %q, on the direct path %d (want 0), rows at organization 1 %v, __refs maps it to %s; DO UPDATE by a role without UPDATE: %s, the row holds %v%s",
		strings.Join(got, "; "), direct, rows, want, refs, before, newRefs, len(newRows), inCloud, k2, other, otherDirect, left, still, short(errU), unchanged, e.proxyLines("direct write")), nil
}

// DU2. INSERT … SELECT with the key named (a copy of rows under new keys): one statement on the
// direct path, in a transaction of the organization's database that returns the keys; their
// refs rows follow in the cloud, then both commit. A key __refs maps to organization 2 refuses
// the statement with 23505 and leaves nothing on either side.
func checkDirectInsertSelect(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxyPgx("du2")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n0 := directCount(e.proxyRoutes(), "1")
	ret := rowsText(e, c, "INSERT INTO document_versions (id, document_id, content) SELECT id + 5000000, document_id, content FROM document_versions WHERE organization_id = $1 ORDER BY id LIMIT 5 RETURNING id", 1)
	direct := directCount(e.proxyRoutes(), "1") - n0
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE organization_id = 1 AND id > 5000000 ORDER BY id")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE id > 5000000")
	if err != nil {
		return false, "", err
	}
	returned := strings.Join(ret[:max(0, len(ret)-1)], ",")
	ok := direct == 1 && len(ids) == 5 && refs == "5" && inCloud == "0" && returned == strings.Join(ids, ",")
	// a key of organization 2's
	k2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions__refs WHERE tenant = 2")
	if err != nil {
		return false, "", err
	}
	other := rowsText(e, c, "INSERT INTO document_versions (id, document_id, content) SELECT $1::bigint, document_id, 'du2-other' FROM document_versions WHERE organization_id = 1 ORDER BY id LIMIT 1", k2)
	left, err := e.customerRows("SELECT id::text FROM document_versions WHERE content = 'du2-other'")
	if err != nil {
		return false, "", err
	}
	still, err := one(e.ctx, e.cloud(), "SELECT tenant::text FROM document_versions__refs WHERE id = "+k2)
	if err != nil {
		return false, "", err
	}
	refused := len(other) == 1 && strings.Contains(other[0], "23505")
	ok = ok && refused && len(left) == 0 && still == "2"
	return ok, fmt.Sprintf("client got %q; on the direct path %d (want 1); the organization's database holds %v, __refs maps %s of them to organization 1, the cloud's partition holds %s; organization 2's key %s: %q, rows left %v, __refs still maps it to %s%s",
		ret, direct, ids, refs, inCloud, k2, other, left, still, e.proxyLines("direct write")), nil
}

// DU3. Django 5.2's bulk_create sends INSERT … SELECT * FROM unnest(…) of typed arrays, with no
// key: the rows are as many as the arrays' elements, so the cloud gives their keys first (as for
// VALUES rows) and they go in as one more array; one statement on the direct path, its
// RETURNING the keys __refs maps to organization 1. pgx sends the arrays in binary.
func checkDirectUnnest(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	before, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM document_versions__refs")
	if err != nil {
		return false, "", err
	}
	docID, err := strconv.ParseInt(doc, 10, 64)
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("du3")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n0 := directCount(e.proxyRoutes(), "1")
	ret := rowsText(e, c, "INSERT INTO document_versions (document_id, content) SELECT * FROM unnest($1::bigint[], $2::text[]) RETURNING id",
		[]int64{docID, docID, docID}, []string{"du3-a", "du3-b", "du3-c"})
	direct := directCount(e.proxyRoutes(), "1") - n0
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE 'du3-%' AND organization_id = 1 AND id > " + before + " ORDER BY id")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE content LIKE 'du3-%'")
	if err != nil {
		return false, "", err
	}
	returned := strings.Join(ret[:max(0, len(ret)-1)], ",")
	ok := direct == 1 && len(ids) == 3 && refs == "3" && inCloud == "0" && returned == strings.Join(ids, ",")
	return ok, fmt.Sprintf("client got %q; on the direct path %d (want 1); the organization's database holds %v (keys above %s), __refs maps %s of them to organization 1, the cloud's partition holds %s%s",
		ret, direct, ids, before, refs, inCloud, e.proxyLines("direct write")), nil
}

// DU4. COPY FROM STDIN goes direct when the application named the organization (lokadb.tenant;
// for the session here: tenant.local_only off, as an application without a pooler in
// transaction mode may run it). pgx's CopyFrom (binary) of three versions: one statement on the
// direct path, keys from the cloud, each in __refs. A text COPY with a row of organization 2
// fails whole (the organization's CHECK), no row and no refs row stay. Without the
// organization named, the same COPY goes the cloud's way.
func checkDirectCopy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"fanout":        map[string]any{e.db: map[string]string{"mode": "reject"}},
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}},
		"tenant":        map[string]any{"local_only": false},
	})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "", fmt.Errorf("org-profile fdw+direct refused: %s", lastErr(out))
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	docID, err := strconv.ParseInt(doc, 10, 64)
	if err != nil {
		return false, "", err
	}
	before, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM document_versions__refs")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("du4")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := c.Exec(e.ctx, "SET lokadb.tenant = '1'"); err != nil {
		return false, "", fmt.Errorf("SET lokadb.tenant: %w", err)
	}
	n0 := directCount(e.proxyRoutes(), "1")
	copied, errC := c.CopyFrom(e.ctx, pgx.Identifier{"document_versions"}, []string{"document_id", "content"},
		pgx.CopyFromRows([][]any{{docID, "du4-a"}, {docID, "du4-b"}, {docID, "du4-c"}}))
	direct := directCount(e.proxyRoutes(), "1") - n0
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE 'du4-%' AND organization_id = 1 AND id > " + before + " ORDER BY id")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE content LIKE 'du4-%'")
	if err != nil {
		return false, "", err
	}
	ok := errC == nil && copied == 3 && direct == 1 && len(ids) == 3 && refs == "3" && inCloud == "0"
	// a row of organization 2 among organization 1's: the whole COPY fails
	refsBefore, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents__refs")
	if err != nil {
		return false, "", err
	}
	_, errMixed := c.PgConn().CopyFrom(e.ctx, bytes.NewReader([]byte("1\t2\tdu4-mixed-1\n2\t2\tdu4-mixed-2\n")), "COPY documents (organization_id, user_id, title) FROM STDIN")
	mixed, err := e.customerRows("SELECT id::text FROM documents WHERE title LIKE 'du4-mixed-%'")
	if err != nil {
		return false, "", err
	}
	mixedCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE title LIKE 'du4-mixed-%'")
	if err != nil {
		return false, "", err
	}
	refsAfter, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents__refs")
	if err != nil {
		return false, "", err
	}
	ok = ok && errMixed != nil && len(mixed) == 0 && mixedCloud == "0" && refsAfter == refsBefore
	// the organization not named: the cloud's way
	if _, err := c.Exec(e.ctx, "RESET lokadb.tenant"); err != nil {
		return false, "", fmt.Errorf("RESET lokadb.tenant: %w", err)
	}
	n1 := directCount(e.proxyRoutes(), "1")
	_, errU := c.PgConn().CopyFrom(e.ctx, bytes.NewReader([]byte("1\t2\tdu4-cloud\n")), "COPY documents (organization_id, user_id, title) FROM STDIN")
	unnamed := directCount(e.proxyRoutes(), "1") - n1
	viaCloud, err := e.customerRows("SELECT id::text FROM documents WHERE title = 'du4-cloud'")
	if err != nil {
		return false, "", err
	}
	ok = ok && errU == nil && unnamed == 0 && len(viaCloud) == 1
	return ok, fmt.Sprintf("CopyFrom: %d rows (error %s), on the direct path %d (want 1); the organization's database holds %v (keys above %s), __refs maps %s of them to organization 1, the cloud's partition holds %s; a row of organization 2 among them: %s, rows %v at the organization, %s in the cloud, documents__refs %s -> %s; not named: %s, on the direct path %d (want 0), rows at the organization %v%s",
		copied, short(errC), direct, ids, before, refs, inCloud, short(errMixed), mixed, mixedCloud, refsBefore, refsAfter, short(errU), unnamed, viaCloud, e.proxyLines("direct")), nil
}

// DU5. Step 4 of 8b.2: the batch forms inside a direct transaction, with the default
// configuration (tenant.local_only on: the organization is named by SET LOCAL after BEGIN).
// Each transaction form runs three times: the first two go through the cloud and are learnt,
// the third goes direct (learn first, 8a.2). The COPY's form: BEGIN; SET LOCAL lokadb.tenant;
// pgx's CopyFrom; COMMIT. The writes' form: a read of organization 1, a DO UPDATE upsert of a
// key named, an INSERT … SELECT copying a version under a new key. Every row the organization's
// database holds is in __refs under organization 1, the cloud's partition holds none.
func checkDirectBatchTx(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	docID, err := strconv.ParseInt(doc, 10, 64)
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("du5")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	runs := func(name string, tx func(i int) error) {
		var direct []int
		for i := 0; i < 3; i++ {
			n0 := directCount(e.proxyRoutes(), "1")
			err := tx(i)
			direct = append(direct, directCount(e.proxyRoutes(), "1")-n0)
			if err != nil {
				ok = false
				notes = append(notes, fmt.Sprintf("%s, run %d: %s", name, i+1, short(err)))
				return
			}
		}
		if direct[0] != 0 || direct[1] != 0 || direct[2] == 0 {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s: on the direct path %v (want 0, 0, then all)", name, direct))
	}
	runs("COPY in BEGIN; SET LOCAL lokadb.tenant", func(i int) error {
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(e.ctx)
		if _, err := tx.Exec(e.ctx, "SET LOCAL lokadb.tenant = '1'"); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(e.ctx, pgx.Identifier{"document_versions"}, []string{"document_id", "content"},
			pgx.CopyFromRows([][]any{{docID, fmt.Sprintf("du5-copy-%d-a", i)}, {docID, fmt.Sprintf("du5-copy-%d-b", i)}})); err != nil {
			return err
		}
		return tx.Commit(e.ctx)
	})
	first, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	firstID, err := strconv.ParseInt(first, 10, 64)
	if err != nil {
		return false, "", err
	}
	runs("an upsert and an INSERT … SELECT", func(i int) error {
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(e.ctx)
		var n int
		if err := tx.QueryRow(e.ctx, "SELECT count(*) FROM document_versions WHERE organization_id = $1", 1).Scan(&n); err != nil {
			return err
		}
		if _, err := tx.Exec(e.ctx, "INSERT INTO document_versions (id, document_id, content) VALUES ($1, $2, $3) ON CONFLICT (id) DO UPDATE SET content = EXCLUDED.content",
			firstID, docID, fmt.Sprintf("du5-upsert-%d", i)); err != nil {
			return err
		}
		if _, err := tx.Exec(e.ctx, "INSERT INTO document_versions (id, document_id, content) SELECT id + $2::bigint, document_id, 'du5-copied-' || $3::text FROM document_versions WHERE organization_id = $1 AND id = $4",
			1, int64(7000000+i), fmt.Sprint(i), firstID); err != nil {
			return err
		}
		return tx.Commit(e.ctx)
	})
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE organization_id = 1 AND content LIKE 'du5-%'")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE content LIKE 'du5-%'")
	if err != nil {
		return false, "", err
	}
	upserted, err := e.customerRows("SELECT content FROM document_versions WHERE id = " + first)
	if err != nil {
		return false, "", err
	}
	// 6 copied rows, 3 copies of a version, the upserted one
	if len(ids) != 10 || refs != "10" || inCloud != "0" || len(upserted) != 1 || upserted[0] != "du5-upsert-2" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("the organization's database holds %d such rows (want 10), __refs maps %s of them to organization 1, the cloud's partition holds %s, the upserted row %v",
		len(ids), refs, inCloud, upserted))
	return ok, strings.Join(notes, "; ") + e.proxyLines("direct"), nil
}
