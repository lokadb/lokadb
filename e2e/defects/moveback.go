package main

// The way back (tasks/rollback.md): repoint, move-back, disable.

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "RB1", name: "repoint: the organization's database restored elsewhere is read and written through the cloud", run: checkRepoint},
		check{id: "RB2", name: "repoint to a database without the organization's tables is refused, the old one stays in use", run: checkRepointRefused},
		check{id: "RB3", name: "move-back under inserts, updates and deletes (cascades in the organization's database) loses nothing", run: checkMoveBackUnderLoad},
		check{id: "RB4", name: "move-back with a role that cannot put triggers there copies under a pause, -dry-run says so first", run: checkMoveBackPause},
		check{id: "RB5", name: "move-back from an unreachable organization's database is refused with the way out", run: checkMoveBackUnreachable},
		check{id: "RB6", name: "move-back -abandon needs the organization's id twice and brings it back empty", run: checkMoveBackAbandon},
		check{id: "RB7", name: "an UPDATE a proxy with the old metadata sends to the foreign partition lands in the cloud", run: checkMoveBackStaleProxy},
	)
}

// restoredCopy makes <db>_copy on the customer Postgres: the organization's database as a
// restore of it would be (CREATE DATABASE ... TEMPLATE needs the source idle).
func (e *env) restoredCopy() (string, error) {
	cp := e.db + "_copy"
	err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+cp+" WITH (FORCE)",
		fmt.Sprintf("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s'", e.db),
		fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", cp, e.db))
	return dsn("customer", cp, "postgres"), err
}

// RB1. The copy gets a row the old database does not have: the cloud must see it, and a
// write through the cloud must land in the copy only.
func checkRepoint(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	cp, err := e.restoredCopy()
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, cp, "UPDATE documents SET title = 'only in the copy' WHERE id = (SELECT min(id) FROM documents)"); err != nil {
		return false, "", err
	}
	out, err := e.repoint(map[string]any{}, "-org", "1", "-customer-app", cp)
	if err != nil {
		return false, "repoint failed: " + short(err), nil
	}
	seen, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1 AND title = 'only in the copy'")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'after repoint')"); err != nil {
		return false, "write through the cloud after repoint: " + short(err), nil
	}
	inCopy, err := one(e.ctx, cp, "SELECT count(*) FROM documents WHERE title = 'after repoint'")
	if err != nil {
		return false, "", err
	}
	inOld, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'after repoint'")
	if err != nil {
		return false, "", err
	}
	paused := ""
	for _, l := range strings.Split(out, "\n") {
		if i := strings.Index(l, "paused="); i >= 0 && strings.Contains(l, "server repointed") {
			paused = ", " + strings.Fields(l[i:])[0]
		}
	}
	// the database id (review A1, the proxy's check of the database): lokadb.orgs has the
	// copy's, the old database keeps the one move-org gave it
	mark := "SELECT coalesce(obj_description('documents'::regclass, 'pg_class'), '')"
	id, err := one(e.ctx, e.cloud(), "SELECT coalesce(db_id, '') FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	copyMark, err := one(e.ctx, cp, mark)
	if err != nil {
		return false, "", err
	}
	oldMark, err := one(e.ctx, e.customer(), mark)
	if err != nil {
		return false, "", err
	}
	ids := id != "" && strings.HasSuffix(copyMark, " database "+id) && strings.Contains(oldMark, " database ") && !strings.HasSuffix(oldMark, " database "+id)
	return seen == "1" && inCopy == "1" && inOld == "0" && ids,
		fmt.Sprintf("row only in the copy seen by the cloud: %s; a new row in the copy: %s, in the old database: %s%s; database id in the cloud %q, the copy's mark %q, the old one's %q",
			seen, inCopy, inOld, paused, id, copyMark, oldMark), nil
}

// RB2. An empty database is not the organization's: refused before anything changes.
func checkRepointRefused(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	empty := e.db + "_empty"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+empty+" WITH (FORCE)", "CREATE DATABASE "+empty); err != nil {
		return false, "", err
	}
	out, err := e.repoint(map[string]any{}, "-org", "1", "-customer-app", dsn("customer", empty, "postgres"))
	if err == nil {
		return false, "repoint to an empty database succeeded", nil
	}
	n, rerr := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if rerr != nil {
		return false, "the organization is unreadable after the refused repoint: " + short(rerr), nil
	}
	why := "?"
	for _, l := range strings.Split(out, "\n") {
		if strings.HasPrefix(l, "error:") {
			why = strings.TrimSpace(strings.TrimPrefix(l, "error:"))
		}
	}
	return n == "100", fmt.Sprintf("refused (%s); organization 1 still reads %s rows", why, n), nil
}

func (e *env) moveBackArgs(org int, extra ...string) []string {
	body := spec
	if e.spec != "" {
		body = e.spec
	}
	p, _ := e.specFile(body)
	return append([]string{"move-back", "-db", e.cloud(), "-spec", p, "-org", fmt.Sprint(org), "-customer", e.customer(), "-bridge", "1s"}, extra...)
}

// withCustomer replaces the -customer DSN of move-back's arguments.
func (e *env) withCustomer(args []string, d string) []string {
	for i := range args {
		if args[i] == e.customer() {
			args[i] = d
		}
	}
	return args
}

// backState: organization 1 in the cloud after a move-back. ok: in its local partition, not
// in lokadb.orgs, no server or foreign table left, its tables there marked as returned.
func (e *env) backState(wantMark bool) (bool, string, error) {
	local, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid
		WHERE d.organization_id = 1 AND c.relname = 'documents__o1'`)
	if err != nil {
		return false, "", err
	}
	all, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	orgs, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs")
	if err != nil {
		return false, "", err
	}
	left, err := one(e.ctx, e.cloud(), `SELECT (SELECT count(*) FROM pg_foreign_server WHERE srvname LIKE 'lokadb\_org%')
		+ (SELECT count(*) FROM pg_class WHERE relname LIKE '%\_\_org1%')`)
	if err != nil {
		return false, "", err
	}
	mark, err := one(e.ctx, e.customer(), "SELECT coalesce(obj_description('documents'::regclass, 'pg_class'), '')")
	if err != nil {
		return false, "", err
	}
	marked := strings.HasPrefix(mark, "returned to the cloud by lokadb")
	ok := local == all && orgs == "0" && left == "0" && (marked || !wantMark)
	return ok, fmt.Sprintf("%s of %s rows in documents__o1, lokadb.orgs %s, servers and foreign tables left %s, its table there marked %v",
		local, all, orgs, left, marked), nil
}

func pauseOf(out string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "msg=switched") {
			if i := strings.Index(l, "writes_paused="); i >= 0 {
				return strings.Fields(l[i:])[0]
			}
		}
	}
	return "writes_paused=?"
}

// RB3. The application keeps writing organization 1 through the cloud during the whole
// move-back; deleting a document sets its versions' document_id NULL in the organization's
// database (ON DELETE SET NULL there), which the cloud never sees as a statement. Three more
// writers only insert: they wait at the switch's lock and write the new partition the moment
// it commits, while move-back verifies the rows as switched.
func checkMoveBackUnderLoad(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	rows, err := w.Query(e.ctx, "SELECT id FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[int64])
	if err != nil {
		return false, "", err
	}
	state := map[int64]string{} // id -> last title written, "" = deleted
	var mu sync.Mutex
	stop, done := make(chan struct{}), make(chan struct{})
	ops, failed := 0, 0
	var firstErr error
	go func() {
		defer close(done)
		r := rand.New(rand.NewSource(3))
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			title := fmt.Sprint("rb3-", i)
			mu.Lock()
			var err error
			switch i % 3 {
			case 0:
				var id int64
				if err = w.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, $1) RETURNING id", title).Scan(&id); err == nil {
					ids = append(ids, id)
					state[id] = title
				}
			case 1:
				id := ids[r.Intn(len(ids))]
				if !deleted(state, id) {
					tag, uerr := w.Exec(e.ctx, "UPDATE documents SET title = $1 WHERE organization_id = 1 AND id = $2", title, id)
					if err = uerr; err == nil && tag.RowsAffected() == 1 {
						state[id] = title
					}
				}
			case 2:
				id := ids[r.Intn(len(ids))]
				tag, derr := w.Exec(e.ctx, "DELETE FROM documents WHERE organization_id = 1 AND id = $1", id)
				if err = derr; err == nil && tag.RowsAffected() == 1 {
					state[id] = ""
				}
			}
			if err != nil {
				failed++
				if firstErr == nil {
					firstErr = err
				}
			} else {
				ops++
			}
			mu.Unlock()
		}
	}()
	const inserters = 3
	inserted := make([][]int64, inserters)
	insErr := make([]error, inserters)
	var wg sync.WaitGroup
	for k := 0; k < inserters; k++ {
		c, err := pgx.Connect(e.ctx, e.cloud())
		if err != nil {
			return false, "", err
		}
		defer c.Close(e.ctx)
		wg.Add(1)
		go func(k int, c *pgx.Conn) {
			defer wg.Done()
			for i := 1; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				var id int64
				if err := c.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, $1) RETURNING id", fmt.Sprint("rb3-ins", k, "-", i)).Scan(&id); err != nil {
					if insErr[k] == nil {
						insErr[k] = err
					}
					continue
				}
				inserted[k] = append(inserted[k], id)
			}
		}(k, c)
	}
	time.Sleep(300 * time.Millisecond)
	// RB3 failed twice under load with one row behind (no logs kept): the log entries move-back
	// applies are traced (admin/movetrace.go) and, on a failure, shown for the keys that differ
	trace := filepath.Join(os.TempDir(), e.db+"-move-trace.log")
	os.Remove(trace)
	os.Setenv("LOKADB_MOVE_TRACE", trace)
	out, backErr := e.run6(e.moveBackArgs(1)...)
	os.Unsetenv("LOKADB_MOVE_TRACE")
	time.Sleep(300 * time.Millisecond)
	close(stop)
	<-done
	wg.Wait()
	nIns := 0
	for k := range inserted {
		nIns += len(inserted[k])
		if insErr[k] != nil {
			failed++
			if firstErr == nil {
				firstErr = insErr[k]
			}
		}
	}
	if backErr != nil {
		return false, "move-back failed: " + errLine(backErr) + moveEvidence(out, trace), nil
	}
	got := map[int64]string{}
	rs, err := w.Query(e.ctx, "SELECT id, title FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	for rs.Next() {
		var id int64
		var t string
		if err := rs.Scan(&id, &t); err != nil {
			return false, "", err
		}
		got[id] = t
	}
	if err := rs.Err(); err != nil {
		return false, "", err
	}
	wrong := 0
	for id, want := range state {
		t, present := got[id]
		if want == "" && present || want != "" && t != want {
			wrong++
		}
	}
	for k := range inserted {
		for _, id := range inserted[k] {
			if _, present := got[id]; !present {
				wrong++
			}
		}
	}
	dangling, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM document_versions v WHERE v.organization_id = 1
		AND v.document_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM documents d WHERE d.id = v.document_id)`)
	if err != nil {
		return false, "", err
	}
	ok, st, err := e.backState(true)
	if err != nil {
		return false, "", err
	}
	return ok && wrong == 0 && failed == 0 && dangling == "0",
		fmt.Sprintf("%d writes and %d inserts by %d more writers during move-back (%d failed: %s), %d rows differ, %s versions point at deleted documents; %s; %s",
			ops, nIns, inserters, failed, short(firstErr), wrong, dangling, st, pauseOf(out)), nil
}

// RB4. The organization's tables belong to its admin; the cloud's role there may read and
// lock them and create in the database, but not put triggers on them.
func checkMoveBackPause(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT SELECT, UPDATE ON documents, document_versions TO app"); err != nil {
		return false, "", err
	}
	appDSN := dsn("customer", e.db, "app")
	dry, err := e.run6(e.withCustomer(e.moveBackArgs(1, "-dry-run"), appDSN)...)
	if err != nil {
		return false, "dry run failed: " + short(err), nil
	}
	sayPause := strings.Contains(dry, "way=pause")
	stillMoved, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs")
	if err != nil {
		return false, "", err
	}
	digest := "SELECT count(*)::text || '/' || coalesce(sum(hashtext(d::text)::numeric), 0)::text FROM documents d"
	want, err := one(e.ctx, e.customer(), digest)
	if err != nil {
		return false, "", err
	}
	out, err := e.run6(e.withCustomer(e.moveBackArgs(1), appDSN)...)
	if err != nil {
		return false, "move-back failed: " + errLine(err), nil
	}
	got, err := one(e.ctx, e.cloud(), digest+" WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	// the role does not own the tables there: it cannot mark them, and says how the customer can
	ok, st, err := e.backState(false)
	if err != nil {
		return false, "", err
	}
	hint := strings.Contains(out, "the customer marks them with: COMMENT ON TABLE")
	return ok && sayPause && stillMoved == "1" && strings.Contains(out, "way=pause") && got == want && hint,
		fmt.Sprintf("dry run says pause: %v and changes nothing: %v; rows/hash %s in the cloud, %s there; %s; marking left to the customer with its SQL: %v; %s",
			sayPause, stillMoved == "1", got, want, st, hint, pauseOf(out)), nil
}

// RB5. lokadb holds no copy of a moved organization: an unreachable database stops move-back.
func checkMoveBackUnreachable(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	out, err := e.run6(e.withCustomer(e.moveBackArgs(1), dsn("customer", e.db+"_gone", "postgres"))...)
	if err == nil {
		return false, "move-back from a database that does not exist succeeded", nil
	}
	orgs, qerr := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs")
	if qerr != nil {
		return false, "", qerr
	}
	hint := strings.Contains(out, "lokadb repoint") && strings.Contains(out, "-abandon -confirm-org 1")
	return orgs == "1" && hint, fmt.Sprintf("refused; the organization stays moved: %v; the error names repoint and -abandon: %v", orgs == "1", hint), nil
}

// RB6. The database is lost: without the id repeated nothing happens; with it the
// organization is back, empty, its refs rows gone, the other organization untouched.
func checkMoveBackAbandon(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	other, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	if _, err := e.run6(e.moveBackArgs(1, "-abandon")...); err == nil {
		return false, "-abandon without -confirm-org succeeded", nil
	}
	if orgs, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs"); err != nil || orgs != "1" {
		return false, fmt.Sprintf("-abandon without -confirm-org changed lokadb.orgs (%s, %v)", orgs, err), nil
	}
	out, err := e.run6(e.moveBackArgs(1, "-abandon", "-confirm-org", "1")...)
	if err != nil {
		return false, "move-back -abandon failed: " + errLine(err), nil
	}
	n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	refs, err := one(e.ctx, e.cloud(), "SELECT (SELECT count(*) FROM documents__refs WHERE tenant = 1) + (SELECT count(*) FROM document_versions__refs WHERE tenant = 1)")
	if err != nil {
		return false, "", err
	}
	other2, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'after abandon')"); err != nil {
		return false, "a write of the organization after -abandon: " + short(err), nil
	}
	loud := strings.Contains(out, "ABANDON")
	return n == "0" && refs == "0" && other == other2 && loud,
		fmt.Sprintf("organization 1: %s rows, %s refs rows; organization 2: %s rows (was %s); warned: %v; writes again", n, refs, other2, other, loud), nil
}

// RB7. A proxy session that loaded its metadata before the switch sends an UPDATE of
// organization 1 to documents__org1 directly; after the switch that name is a view into the
// parent, so the row changes in the cloud, not in the database the cloud no longer reads.
func checkMoveBackStaleProxy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stopProxy, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stopProxy()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	id, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	upd := "UPDATE documents SET title = '%s' WHERE organization_id = 1 AND id = " + id
	if _, err := simple(e.ctx, c, fmt.Sprintf(upd, "before")); err != nil { // loads the metadata
		return false, "", err
	}
	cmd := exec.CommandContext(e.ctx, e.bin, append(e.moveBackArgs(1), "-bridge", "20s")...)
	pipe, err := cmd.StdoutPipe()
	if err != nil {
		return false, "", err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return false, "", err
	}
	var all strings.Builder
	updErr := fmt.Errorf("move-back printed no switch")
	sc := bufio.NewScanner(pipe)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if strings.Contains(sc.Text(), "msg=switched") {
			_, updErr = simple(e.ctx, c, fmt.Sprintf(upd, "stale"))
		}
	}
	if err := cmd.Wait(); err != nil {
		return false, "move-back failed: " + errLine(fmt.Errorf("%s", all.String())), nil
	}
	if updErr != nil {
		return false, "the stale proxy's UPDATE failed: " + short(updErr), nil
	}
	title, err := one(e.ctx, e.cloud(), "SELECT title FROM documents WHERE id = "+id)
	if err != nil {
		return false, "", err
	}
	there, err := one(e.ctx, e.customer(), "SELECT title FROM documents WHERE id = "+id)
	if err != nil {
		return false, "", err
	}
	return title == "stale", fmt.Sprintf("title in the cloud %q, in the organization's database %q", title, there), nil
}

// errLine: the "error:" line lokadb printed (what went wrong), else the error shortened.
func errLine(err error) string {
	lines := strings.Split(err.Error(), "\n")
	for i, l := range lines {
		if strings.HasPrefix(l, "error:") {
			msg := strings.TrimSpace(strings.TrimPrefix(l, "error:"))
			if i+1 < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i+1]), "in:") { // the statement it failed in
				msg += " " + strings.TrimSpace(lines[i+1])
			}
			return msg
		}
	}
	return short(err)
}

// run6 runs lokadb; on failure the error is what lokadb printed from its "error:" line on (the
// lokadb helper keeps only the output's last lines, where a long statement hides it).
func (e *env) run6(args ...string) (string, error) {
	out, err := e.lokadb(args...)
	if err != nil {
		if i := strings.Index(out, "\nerror:"); i >= 0 {
			return out, fmt.Errorf("%s", out[i+1:])
		}
	}
	return out, err
}

// moveEvidence: what a failed move-back leaves to examine. Its warnings and switch attempts, and
// the lines of its trace (every log entry applied, in order) about the keys its verification
// named.
func moveEvidence(out, trace string) string {
	var b strings.Builder
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "level=WARN") || strings.Contains(l, "switch") || strings.Contains(l, "caught up") {
			b.WriteString("\n      " + l)
		}
	}
	keys := map[string]bool{}
	for _, m := range regexp.MustCompile(`\[([^\]]*)\]`).FindAllStringSubmatch(out, -1) {
		for _, k := range strings.Split(m[1], ",") {
			if k = strings.TrimSpace(k); k != "" && !strings.HasPrefix(k, "…") {
				keys[k] = true
			}
		}
	}
	data, err := os.ReadFile(trace)
	if err != nil {
		return b.String() + "\n      (no trace: " + err.Error() + ")"
	}
	round := ""
	for _, l := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(l, "  ") {
			round = l
			continue
		}
		for k := range keys {
			if strings.Contains(l, " old="+k+" ") || strings.Contains(l, " new="+k+" ") {
				b.WriteString("\n      trace: " + round + " |" + l)
				break
			}
		}
	}
	return b.String()
}
