package main

// Writes on the direct path, profile fdw+direct (block 10, stage 8b, tasks/direct-path.md,
// section 5): an INSERT or a DELETE of one moved organization goes to its database straight;
// the proxy keeps <table>__refs in the cloud as the parent's triggers did through the FDW, in
// postgres_fdw's order (the cloud's transaction open under the pause, the organization's
// statement, the organization's commit, then the cloud's). The checks write document_versions:
// documents has an FK onto users, a cloud table, kept by a trigger of the cloud (lokadb_fk_*),
// so its writes stay on the cloud's way.

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "DP4", name: "a batch INSERT of a moved organization in fdw+direct goes to its database once, keys from the cloud's sequence, every key in __refs", run: checkDirectInsert},
		check{id: "DP7", name: "__refs after direct INSERTs and DELETEs: a failed INSERT leaves none, a cloud RESTRICT keeps the row, a cloud CASCADE acts", run: checkDirectRefs},
		check{id: "DP12", name: "a direct write whose cloud commit fails after the organization's: refs-diff finds the row, refs-sync adds its key", run: checkDirectCommitGap},
		check{id: "DP13", name: "move-back under direct INSERTs: every INSERT the client saw is in the cloud once, none on the direct path after the switch", run: checkDirectWritesMoveBack},
		check{id: "DP15", name: "a DELETE in fdw+direct of a parent whose LokaDB child cascades (block 5's lokadb_fkcascade_* on the foreign partition) goes through the cloud: the children leave the organization's database and __refs", run: checkDirectCascade},
		check{id: "DP14", name: "an UPDATE in fdw+direct of a table with a row trigger of the cloud fires it: the application's trigger runs, an FK onto a cloud table refuses a missing row", run: checkDirectTriggers},
	)
}

// DP14. Stage 8a sent an UPDATE of one organization straight to its database whatever the
// cloud's parent had: its row triggers, cloned onto the foreign partition, did not fire there.
// The application's AFTER UPDATE trigger (an audit row in the cloud) was skipped, and so was
// lokadb_fk_* of documents.user_id, an FK onto users, a cloud table: an UPDATE to a user that
// does not exist went through. Such a table now writes the cloud's way (Meta.RowTriggers).
func checkDirectTriggers(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE dp14_audit (version_id bigint, content text)", "GRANT ALL ON dp14_audit TO PUBLIC",
		"CREATE FUNCTION dp14_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO dp14_audit VALUES (NEW.id, NEW.content); RETURN NULL; END $$",
		"CREATE TRIGGER dp14_audit AFTER UPDATE ON document_versions FOR EACH ROW EXECUTE FUNCTION dp14_audit()"); err != nil {
		return false, "", err
	}
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp14") // after the trigger: the session's metadata has it
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n := directCount(e.proxyRoutes(), "1")
	upd := rowsText(e, c, "UPDATE document_versions SET content = 'dp14' WHERE organization_id = $1 AND id = $2", 1, ver)
	audit, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM dp14_audit WHERE content = 'dp14'")
	if err != nil {
		return false, "", err
	}
	fk := rowsText(e, c, "UPDATE documents SET user_id = 999999 WHERE organization_id = $1 AND id = $2", 1, doc)
	user, err := one(e.ctx, e.customer(), "SELECT coalesce(user_id::text, 'null') FROM documents WHERE id = "+doc)
	if err != nil {
		return false, "", err
	}
	direct := directCount(e.proxyRoutes(), "1") - n
	ok := audit == "1" && strings.Contains(strings.Join(fk, " "), "23503") && user != "999999"
	return ok, fmt.Sprintf("UPDATE of a version %q: the application's trigger wrote %s audit row(s) (want 1); UPDATE of a document to user 999999 %q (want 23503), its user_id now %s; on the direct path %d",
		upd, audit, fk, user, direct), nil
}

// directWriteSetup: directSetup, organization 1 in fdw+direct, one of its documents.
func (e *env) directWriteSetup() (func(), string, error) {
	stop, err := e.directSetup(nil)
	if err != nil {
		return nil, "", err
	}
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		stop()
		return nil, "", fmt.Errorf("org-profile fdw+direct refused: %s", lastErr(out))
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		stop()
		return nil, "", err
	}
	return stop, doc, nil
}

// refsOf: how many of ids __refs maps to organization 1.
func (e *env) refsOf(ids []string) (string, error) {
	if len(ids) == 0 {
		return "0", nil
	}
	return one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs WHERE tenant = 1 AND id IN ("+strings.Join(ids, ", ")+")")
}

// DP4. Three versions in one INSERT through the proxy, no key given: one statement on the
// direct path; the organization's database holds them under keys the cloud's sequence gave
// (above every key before), each in __refs under organization 1; the client's RETURNING is
// those keys.
func checkDirectInsert(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	before, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM document_versions__refs")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp4")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n := directCount(e.proxyRoutes(), "1")
	ret := rowsText(e, c, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'dp4-a'), ($1, 'dp4-b'), ($1, 'dp4-c') RETURNING id", doc)
	direct := directCount(e.proxyRoutes(), "1") - n
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE 'dp4-%' AND organization_id = 1 AND id > " + before + " ORDER BY id")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	inCloud, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM ONLY document_versions__cloud WHERE content LIKE 'dp4-%'")
	if err != nil {
		return false, "", err
	}
	returned := strings.Join(ret[:max(0, len(ret)-1)], ",")
	ok := direct == 1 && len(ids) == 3 && refs == "3" && inCloud == "0" && returned == strings.Join(ids, ",")
	return ok, fmt.Sprintf("client got %q; on the direct path %d (want 1); the organization's database holds %v (keys above %s), __refs maps %s of them to organization 1, the cloud's partition holds %s%s",
		ret, direct, ids, before, refs, inCloud, e.proxyLines("direct write")), nil
}

// DP7. __refs follows direct writes, and nothing of a write the other side refuses stays:
//   - INSERT and DELETE: the keys appear, then go;
//   - an INSERT the organization's database refuses (its FK: a document __refs maps to
//     organization 1 that its database does not have) leaves no key in __refs;
//   - a DELETE of a version a cloud table holds with ON DELETE RESTRICT onto __refs fails with
//     the cloud's 23001, the version stays in the organization's database and in __refs;
//   - one held with ON DELETE CASCADE goes, and the cloud's row with it.
func checkDirectRefs(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE dp7_pins (version_id bigint REFERENCES document_versions__refs (id) ON DELETE RESTRICT)",
		"CREATE TABLE dp7_notes (version_id bigint REFERENCES document_versions__refs (id) ON DELETE CASCADE)",
		"GRANT ALL ON dp7_pins, dp7_notes TO PUBLIC"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp7")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	n0 := directCount(e.proxyRoutes(), "1")
	// INSERT, DELETE
	ins := rowsText(e, c, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'dp7-a'), ($1, 'dp7-b'), ($1, 'dp7-c') RETURNING id", doc)
	ids := ins[:max(0, len(ins)-1)]
	if len(ids) != 3 {
		return false, fmt.Sprintf("the INSERT: %q", ins), nil
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	del := rowsText(e, c, "DELETE FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ids[0])
	left, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	rows, err := e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE 'dp7-%' ORDER BY id")
	if err != nil {
		return false, "", err
	}
	step := refs == "3" && strings.Join(del, " ") == "tag: DELETE 1" && left == "2" && strings.Join(rows, ",") == strings.Join(ids[1:], ",")
	ok = ok && step
	notes = append(notes, fmt.Sprintf("INSERT of 3: __refs %s; DELETE of one %q: __refs %s, the organization's rows %v (%v)", refs, del, left, rows, step))
	// an INSERT the organization's database refuses
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents__refs (id, tenant) VALUES (990777, 1)"); err != nil {
		return false, "", err
	}
	count := "SELECT count(*) FROM document_versions__refs WHERE tenant = 1"
	b, err := one(e.ctx, e.cloud(), count)
	if err != nil {
		return false, "", err
	}
	bad := rowsText(e, c, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'dp7-bad')", 990777)
	a, err := one(e.ctx, e.cloud(), count)
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM documents__refs WHERE id = 990777"); err != nil {
		return false, "", err
	}
	step = strings.Contains(strings.Join(bad, " "), "23503") && a == b
	ok = ok && step
	notes = append(notes, fmt.Sprintf("an INSERT the organization refuses: %q; organization 1's keys in __refs %s before, %s after (%v)", bad, b, a, step))
	// RESTRICT and CASCADE of cloud tables onto __refs
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO dp7_pins VALUES ("+ids[1]+")", "INSERT INTO dp7_notes VALUES ("+ids[2]+")"); err != nil {
		return false, "", err
	}
	pinned := rowsText(e, c, "DELETE FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ids[1])
	cascaded := rowsText(e, c, "DELETE FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ids[2])
	rows, err = e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE 'dp7-%' ORDER BY id")
	if err != nil {
		return false, "", err
	}
	left, err = e.refsOf(ids[1:])
	if err != nil {
		return false, "", err
	}
	noteRows, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM dp7_notes")
	if err != nil {
		return false, "", err
	}
	step = strings.Contains(strings.Join(pinned, " "), "23001") && strings.Join(cascaded, " ") == "tag: DELETE 1" && // restrict_violation, as through the FDW
		strings.Join(rows, ",") == ids[1] && left == "1" && noteRows == "0"
	ok = ok && step
	notes = append(notes, fmt.Sprintf("DELETE of a version a cloud row holds (RESTRICT): %q; one with CASCADE: %q; the organization's rows %v, __refs %s of 2, the cloud's notes left %s (%v)",
		pinned, cascaded, rows, left, noteRows, step))
	// the simple protocol (psql, PHP's pgsql, pgx in its simple mode): an INSERT … RETURNING and
	// a DELETE, after which the connection still answers (a NoData there would break it: review
	// of block 9 of 8b, 1)
	// the messages themselves: pgx's simple mode lets a stray NoData pass, libpq does not
	raw, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	hj, err := raw.Hijack()
	if err != nil {
		return false, "", err
	}
	defer hj.Conn.Close()
	sret, sid, err := simpleAnswer(hj, "INSERT INTO document_versions (document_id, content) VALUES ("+doc+", 'dp7-s') RETURNING id")
	if err != nil {
		return false, "", err
	}
	sin, err := e.refsOf([]string{sid})
	if err != nil {
		return false, "", err
	}
	sdel, _, err := simpleAnswer(hj, "DELETE FROM document_versions WHERE organization_id = 1 AND id = "+sid)
	if err != nil {
		return false, "", err
	}
	sleft, err := e.refsOf([]string{sid})
	if err != nil {
		return false, "", err
	}
	step = sret == "RowDescription DataRow CommandComplete ReadyForQuery" && sin == "1" && sdel == "CommandComplete ReadyForQuery" && sleft == "0"
	ok = ok && step
	notes = append(notes, fmt.Sprintf("simple protocol: INSERT … RETURNING answered %s (key %s, __refs %s), DELETE answered %s (want CommandComplete ReadyForQuery; __refs %s after) (%v)", sret, sid, sin, sdel, sleft, step))
	direct := directCount(e.proxyRoutes(), "1") - n0
	// the INSERT, the DELETE, the refused INSERT, the two DELETEs held by the cloud, the simple
	// protocol's INSERT and DELETE
	ok = ok && direct == 7
	notes = append(notes, fmt.Sprintf("on the direct path %d of 7", direct))
	return ok, strings.Join(notes, "; ") + e.proxyLines("direct write"), nil
}

// DP12. The cloud's commit fails after the organization's (a deferred trigger of the check on
// __refs stands for a crash between the two commits): the client gets an error, the row is in
// the organization's database and its key not in __refs; refs-diff finds it, refs-sync adds it,
// refs-diff is clean after.
func checkDirectCommitGap(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE dp12_fail (x int)", "GRANT SELECT ON dp12_fail TO PUBLIC", "INSERT INTO dp12_fail VALUES (1)",
		`CREATE FUNCTION dp12_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
			IF EXISTS (SELECT FROM dp12_fail) THEN RAISE EXCEPTION 'dp12: the cloud''s commit fails'; END IF; RETURN NULL; END $$`,
		"CREATE CONSTRAINT TRIGGER dp12_fail AFTER INSERT ON document_versions__refs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION dp12_fail()"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp12")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	got := rowsText(e, c, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'dp12')", doc)
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM dp12_fail"); err != nil {
		return false, "", err
	}
	ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE content = 'dp12'")
	if err != nil {
		return false, "", err
	}
	refs, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	diff, dcode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	sync, scode, err := e.schemaCmd("refs-sync")
	if err != nil {
		return false, "", err
	}
	after, acode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	fixed, err := e.refsOf(ids)
	if err != nil {
		return false, "", err
	}
	ok := strings.HasPrefix(strings.Join(got, " "), "error") && len(ids) == 1 && refs == "0" && dcode == 2 && strings.Contains(diff, "written past the cloud") && scode == 0 && acode == 0 && fixed == "1"
	return ok, fmt.Sprintf("client got %q; the organization's database holds %v, __refs %s of them; refs-diff exit %d: %s; refs-sync exit %d: %s; refs-diff after exit %d: %s; __refs then %s",
		got, ids, refs, dcode, oneLine(diff), scode, oneLine(sync), acode, oneLine(after), fixed), nil
}

// DP13. move-back of organization 1 while four clients insert its versions through the proxy
// on the direct path: the switch waits for the writes in flight (they hold the pause lock in
// their cloud transaction); after it every INSERT a client saw succeed is in the cloud exactly
// once, the refs agree, and no statement went the direct path after the switch.
func checkDirectWritesMoveBack(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var mu sync.Mutex
	var seen []string
	failed := 0
	var firstErr string
	stopLoad := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		c, err := e.viaProxyPgx("dp13")
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
				mark := fmt.Sprintf("dp13-%d-%d", w, i)
				if _, err := c.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, $2)", doc, mark); err != nil {
					mu.Lock()
					failed++
					if firstErr == "" {
						firstErr = firstErrLine(err)
					}
					mu.Unlock()
					continue
				}
				mu.Lock()
				seen = append(seen, mark)
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
		return false, "move-back under direct writes failed: " + errLine(backErr), nil
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
	inCloud := map[string]int{}
	rows, err := e.cloudRows("SELECT content FROM document_versions WHERE organization_id = 1 AND content LIKE 'dp13-%'")
	if err != nil {
		return false, "", err
	}
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
	unknown := len(inCloud) - (len(seen) - missing) // in the cloud, never seen as written by a client
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
	return ok, fmt.Sprintf("%d INSERTs seen, %d refused (%s); in the cloud after move-back: %d missing, %d twice, %d not seen by a client; versions without their key in __refs %s; direct statements before the switch %d, after it %d (want 0); %s",
		len(seen), failed, firstErr, missing, twice, unknown, refsGap, before, after, pauseOf(out)), nil
}

// simpleAnswer sends sql as a simple query on hj and returns the types of the messages
// answering it, up to ReadyForQuery, and the first DataRow's first value.
func simpleAnswer(hj *pgconn.HijackedConn, sql string) (string, string, error) {
	_ = hj.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	hj.Frontend.Send(&pgproto3.Query{String: sql})
	if err := hj.Frontend.Flush(); err != nil {
		return "", "", err
	}
	var types []string
	first := ""
	for {
		m, err := hj.Frontend.Receive()
		if err != nil {
			return strings.Join(types, " "), first, err
		}
		types = append(types, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
		switch m := m.(type) {
		case *pgproto3.DataRow:
			if first == "" && len(m.Values) > 0 {
				first = string(m.Values[0])
			}
		case *pgproto3.ReadyForQuery:
			return strings.Join(types, " "), first, nil
		}
	}
}

// cloudRows: the first column of sql's rows in the cloud database, as text.
func (e *env) cloudRows(sql string) ([]string, error) {
	c, err := pgx.Connect(e.ctx, e.cloud())
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

// DP15 (with block 5's MV18). document_versions references documents ON DELETE CASCADE; after
// the move the cloud keeps the cascade with a row trigger on documents' foreign partition
// (lokadb_fkcascade_*: it deletes the children through the cloud, else their rows stay in
// document_versions__refs). documents' FK onto users (lokadb_fk_*) is dropped first, so the
// cascade's trigger is the only one: on fdw+direct a role with SELECT and DELETE on documents
// alone deletes a document through the proxy; the DELETE goes the cloud's way (0 direct), its
// versions leave the organization's database and __refs; a read of documents still goes direct.
func checkDirectCascade(e *env) (bool, string, error) {
	fks, err := e.cloudRows("SELECT conname FROM pg_constraint WHERE conrelid IN ('document_versions'::regclass, 'documents'::regclass) AND contype = 'f'")
	if err != nil {
		return false, "", err
	}
	deleter := "dp15d_" + e.db
	if err := role(e, "cloud", deleter); err != nil {
		return false, "", err
	}
	var stmts []string
	for _, fk := range fks {
		stmts = append(stmts, "ALTER TABLE document_versions DROP CONSTRAINT IF EXISTS "+fk, "ALTER TABLE documents DROP CONSTRAINT IF EXISTS "+fk)
	}
	stmts = append(stmts, "ALTER TABLE document_versions ADD FOREIGN KEY (document_id) REFERENCES documents ON DELETE CASCADE",
		"GRANT SELECT, DELETE ON documents TO "+deleter)
	if err := execSQL(e.ctx, e.cloud(), stmts...); err != nil {
		return false, "", err
	}
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON ALL TABLES IN SCHEMA lokadb TO "+deleter); err != nil {
		return false, "", err
	}
	triggers, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'lokadb_fkcascade_%' AND NOT tgisinternal")
	if err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.customer(), "SELECT min(document_id)::text FROM document_versions")
	if err != nil {
		return false, "", err
	}
	versions, err := one(e.ctx, e.customer(), "SELECT string_agg(id::text, ',') FROM document_versions WHERE document_id = "+doc)
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyAs(deleter)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	n := directCount(e.proxyRoutes(), "1")
	read := "read"
	if _, err := simple(e.ctx, c, "SELECT title FROM documents WHERE organization_id = 1 AND id = "+doc); err != nil {
		read = "error: " + short(err)
	}
	readDirect := directCount(e.proxyRoutes(), "1") - n
	del := "deleted"
	res, err := simple(e.ctx, c, "DELETE FROM documents WHERE organization_id = 1 AND id = "+doc)
	switch {
	case err != nil:
		del = "error: " + short(err)
	case len(res) == 1:
		del = res[0].CommandTag.String()
	}
	delDirect := directCount(e.proxyRoutes(), "1") - n - readDirect
	there, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE document_id = "+doc)
	refs, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs WHERE id IN ("+versions+")")
	ok := triggers != "0" && read == "read" && readDirect == 1 && del == "DELETE 1" && delDirect == 0 && there == "0" && refs == "0"
	return ok, fmt.Sprintf("lokadb_fkcascade_* triggers %s; a read of document %s: %s, on the direct path %d (want 1); its DELETE by a role with no right on document_versions: %s, on the direct path %d (want 0); its versions (%s) at the organization %s, in document_versions__refs %s",
		triggers, doc, read, readDirect, del, delDirect, versions, there, refs), nil
}
