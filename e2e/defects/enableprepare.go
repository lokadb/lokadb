package main

// 1.4 / R7. enable held its tables locked while it filled the refs tables and the
// organization column of child tables: a downtime of the whole service, as long as the rows
// take (e2e/enabletime: 39 s on 2.1 million rows). enable -prepare does that online, under
// the application's writes; enable then only switches, after checking what -prepare made.

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "E14", name: "enable -prepare under writes, then the switch: refs, organizations and FKs as a one-step enable makes them", run: checkPrepareSwitch},
		check{id: "E14", name: "the switch refuses what -prepare did not finish or what no longer agrees", run: checkPrepareRefuses},
		check{id: "E17", name: "the log of parents moved after -prepare is pruned without moving its count: old rows go, recent ones stay, the application's role cannot prune (A3 of the admin review)", run: checkPrepareLogPrune},
		check{id: "E16", name: "a parent moved to another organization between the switch's check and its lock: the switch refuses, no child keeps the old organization (A3 of the admin review)", run: checkPrepareWindow},
		check{id: "E18", name: "enable -prepare creates its triggers in a lock order the application's writes (parent, then child) cannot deadlock", run: checkPrepareLockOrder},
	)
}

func (e *env) enableArgs(extra ...string) error {
	p, err := e.specFile(spec)
	if err != nil {
		return err
	}
	_, err = e.lokadb(append([]string{"enable", "-db", e.cloud(), "-spec", p, "-owner", "app"}, extra...)...)
	return err
}

func checkPrepareSwitch(e *env) (bool, string, error) {
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	if _, err := w.Exec(e.ctx, "INSERT INTO documents (organization_id, user_id, title) SELECT 1 + g % 2, 2, 'bulk' FROM generate_series(1, 50000) g"); err != nil {
		return false, "", err
	}
	if _, err := w.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) SELECT id, 'bulk' FROM documents WHERE title = 'bulk'"); err != nil {
		return false, "", err
	}
	// the application keeps writing both tables while -prepare runs
	var (
		stop   = make(chan struct{})
		wg     sync.WaitGroup
		writes int
		fails  []string
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var err error
			switch i % 4 {
			case 0:
				_, err = w.Exec(e.ctx, "WITH d AS (INSERT INTO documents (organization_id, user_id, title) VALUES (1 + $1::int % 2, 2, 'live') RETURNING id) INSERT INTO document_versions (document_id, content) SELECT id, 'live' FROM d", i)
			case 1:
				_, err = w.Exec(e.ctx, "UPDATE documents SET organization_id = 3 - organization_id WHERE id = (SELECT min(id) FROM documents WHERE title = 'bulk' AND id NOT IN (SELECT document_id FROM document_versions WHERE document_id IS NOT NULL))")
			case 2:
				_, err = w.Exec(e.ctx, "DELETE FROM document_versions WHERE id = (SELECT max(id) FROM document_versions WHERE content = 'bulk')")
			case 3:
				_, err = w.Exec(e.ctx, "UPDATE document_versions SET document_id = (SELECT max(id) FROM documents WHERE organization_id = 2) WHERE id = (SELECT min(id) FROM document_versions WHERE content = 'bulk')")
			}
			if err != nil && len(fails) < 3 {
				fails = append(fails, firstErrLine(err))
			}
			writes++
			time.Sleep(2 * time.Millisecond)
		}
	}()
	prepErr := e.enableArgs("-prepare")
	close(stop)
	wg.Wait()
	if prepErr != nil {
		return false, "enable -prepare failed: " + oneLine(prepErr.Error()), nil
	}
	if err := e.enableArgs(); err != nil {
		return false, "the switch failed: " + oneLine(err.Error()), nil
	}
	var notes []string
	ok := len(fails) == 0
	notes = append(notes, fmt.Sprintf("%d writes during -prepare, failed: %v", writes, fails))
	expect := func(what, sql, want string) {
		got, err := one(e.ctx, e.cloud(), sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("%s: %s", what, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	expect("documents__refs differs", "SELECT count(*) FROM documents d FULL JOIN documents__refs r ON r.id = d.id WHERE r.id IS NULL OR d.id IS NULL OR r.tenant IS DISTINCT FROM d.organization_id", "0")
	expect("versions whose organization differs from their document's", "SELECT count(*) FROM document_versions v LEFT JOIN documents d ON d.id = v.document_id WHERE v.organization_id IS DISTINCT FROM d.organization_id", "0")
	expect("FKs of document_versions__cloud", "SELECT string_agg(conname || '->' || confrelid::regclass::text, ',') FROM pg_constraint WHERE conrelid = 'document_versions__cloud'::regclass AND contype = 'f'", "document_versions_document_id_fkey->documents__refs")
	expect("FKs of documents__cloud", "SELECT string_agg(conname || '->' || confrelid::regclass::text, ',') FROM pg_constraint WHERE conrelid = 'documents__cloud'::regclass AND contype = 'f'", "documents_user_id_fkey->users")
	expect("triggers of -prepare left, rows of lokadb.prepare", "SELECT (SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'lokadb\\_prep%')::text || ', ' || (SELECT count(*) FROM lokadb.prepare)", "0, 0")
	expect("partitioned, policy", "SELECT (SELECT relkind::text FROM pg_class WHERE oid = 'documents'::regclass) || ', ' || (SELECT count(*) FROM pg_policy WHERE polrelid = 'documents'::regclass)", "p, 1")
	// and it moves
	if _, err := e.move(1); err != nil {
		ok = false
		notes = append(notes, "WRONG move-org 1 failed: "+firstErrLine(err))
	} else {
		expect("after move-org 1, its documents in the cloud", orgInCloud, "0")
	}
	return ok, strings.Join(notes, "; "), nil
}

func checkPrepareRefuses(e *env) (bool, string, error) {
	var notes []string
	ok := true
	refused := func(what string, err error, want string) {
		if err != nil && strings.Contains(err.Error(), want) {
			notes = append(notes, what+": refused")
			return
		}
		ok = false
		got := "accepted"
		if err != nil {
			got = oneLine(err.Error())
		}
		notes = append(notes, "WRONG "+what+": "+got)
	}
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare failed: " + oneLine(err.Error()), nil
	}
	// check sees the preparation, and still checks the schema (lokadb.tables exists, empty)
	out, _ := e.checkWith(spec)
	if reported(out, "enable_prepare_done") && !reported(out, "already_enabled") {
		notes = append(notes, "check after -prepare: enable_prepare_done")
	} else {
		ok = false
		notes = append(notes, fmt.Sprintf("WRONG check after -prepare: enable_prepare_done %v, already_enabled %v", reported(out, "enable_prepare_done"), reported(out, "already_enabled")))
	}
	// a write past the triggers (session_replication_role = replica skips them)
	if err := execSQL(e.ctx, e.cloud(), "SET session_replication_role = replica; INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'past the trigger'); RESET session_replication_role"); err != nil {
		return false, "", err
	}
	refused("the switch after a write the refs table missed", e.enableArgs(), "does not agree")
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare again failed: " + oneLine(err.Error()), nil
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE lokadb.prepare SET stage = 'refs' WHERE table_name = 'document_versions'"); err != nil {
		return false, "", err
	}
	refused("the switch while -prepare is not finished", e.enableArgs(), "has not finished")
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare a third time failed: " + oneLine(err.Error()), nil
	}
	if err := e.enableArgs(); err != nil {
		ok = false
		notes = append(notes, "WRONG the switch after -prepare repaired it: "+oneLine(err.Error()))
	} else {
		notes = append(notes, "after -prepare again the switch ran")
	}
	return ok, strings.Join(notes, "; "), nil
}

// E18. enable -prepare put lokadb_prep_moved on the parent in the transaction that adds the
// organization column to its child: it locked the child (ALTER TABLE), then waited for the parent,
// while the application, writing the parent, then the child (E14's WITH … INSERT), waited for the
// child: a deadlock, and the application's statement or -prepare failed with 40P01 (E14, once in
// a few runs). Here in that order on purpose: a prepared stand whose parent lacks the trigger (as a
// -prepare of an older version left it), session A writes documents and keeps its transaction,
// -prepare again waits to create the trigger, A writes document_versions and commits. Both must go
// through.
func checkPrepareLockOrder(e *env) (bool, string, error) {
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare failed: " + oneLine(err.Error()), nil
	}
	if err := execSQL(e.ctx, e.cloud(), "DROP TRIGGER lokadb_prep_moved ON documents"); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	a, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	// the lock of a write to documents, no row lock: -prepare's batches pass
	if _, err := a.Exec(e.ctx, "BEGIN"); err != nil {
		return false, "", err
	}
	if _, err := a.Exec(e.ctx, "UPDATE documents SET title = title WHERE false"); err != nil {
		return false, "", err
	}
	done := make(chan error, 1)
	go func() { done <- e.enableArgs("-prepare", "-lock-timeout", "60s") }()
	waiting := false
	for i := 0; i < 600 && !waiting; i++ {
		n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query ILIKE '%CREATE TRIGGER lokadb_prep_moved%'")
		if err != nil {
			return false, "", err
		}
		if waiting = n != "0"; !waiting {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !waiting {
		a.Exec(e.ctx, "ROLLBACK")
		<-done
		return false, "-prepare never waited to create lokadb_prep_moved", nil
	}
	var notes []string
	ok := true
	if _, err := a.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ("+doc+", 'order')"); err != nil {
		ok = false
		notes = append(notes, "WRONG the application's write of the child failed: "+firstErrLine(err))
		a.Exec(e.ctx, "ROLLBACK")
	} else if _, err := a.Exec(e.ctx, "COMMIT"); err != nil {
		ok = false
		notes = append(notes, "WRONG the application's commit failed: "+firstErrLine(err))
	} else {
		notes = append(notes, "the application wrote the parent, then the child, and committed")
	}
	select {
	case err := <-done:
		if err != nil {
			ok = false
			notes = append(notes, "WRONG enable -prepare failed: "+lastLine(strings.TrimSpace(err.Error())))
		} else {
			notes = append(notes, "enable -prepare ran")
		}
	case <-time.After(90 * time.Second):
		return false, "enable -prepare did not end", nil
	}
	if ok {
		if err := e.enableArgs(); err != nil {
			ok = false
			notes = append(notes, "WRONG the switch failed: "+oneLine(err.Error()))
		} else {
			notes = append(notes, "the switch ran")
		}
	}
	return ok, strings.Join(notes, "; "), nil
}

// E16 (A3 of the admin review). The switch counted the child tables' organizations before its
// lock and did not look again: a transaction of the application that moved a parent in between
// left the children behind (their column is a copy of the parent's organization), and after the
// switch the parent's children were another organization's rows. Session A moves a document and
// keeps its transaction open; the switch (a long lock timeout) passes its check, since A has not
// committed, and waits for its lock; A commits. The switch must refuse, then -prepare again
// repairs the children and the switch runs.
func checkPrepareWindow(e *env) (bool, string, error) {
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare failed: " + oneLine(err.Error()), nil
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(d.id) FROM documents d JOIN document_versions v ON v.document_id = d.id WHERE d.organization_id = 1")
	if err != nil {
		return false, "", err
	}
	a, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	if _, err := a.Exec(e.ctx, "BEGIN"); err != nil {
		return false, "", err
	}
	if _, err := a.Exec(e.ctx, "UPDATE documents SET organization_id = 2 WHERE id = "+doc); err != nil {
		return false, "", err
	}
	done := make(chan error, 1)
	go func() { done <- e.enableArgs("-lock-timeout", "60s") }()
	// the switch has passed its check when its LOCK waits behind A's row lock
	waiting := false
	for i := 0; i < 600 && !waiting; i++ {
		n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND query ILIKE '%LOCK TABLE%'")
		if err != nil {
			return false, "", err
		}
		if waiting = n != "0"; !waiting {
			time.Sleep(100 * time.Millisecond)
		}
	}
	if !waiting {
		return false, "the switch never waited for its lock", nil
	}
	if _, err := a.Exec(e.ctx, "COMMIT"); err != nil {
		return false, "", err
	}
	var switchErr error
	select {
	case switchErr = <-done:
	case <-time.After(90 * time.Second):
		return false, "the switch did not end", nil
	}
	stale := func() (string, error) {
		return one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions v JOIN documents d ON d.id = v.document_id WHERE v.organization_id IS DISTINCT FROM d.organization_id")
	}
	if switchErr == nil {
		// the switch ran: the children of the moved document are another organization's now
		n, _ := stale()
		return false, "WRONG the switch ran past the move; " + n + " child rows keep the old organization", nil
	}
	ok := strings.Contains(switchErr.Error(), "changed an organization")
	notes := []string{"the switch refused: " + oneLine(switchErr.Error())}
	// a refused switch leaves the preparation as it was: the trigger stays, the move is in the log
	kept, err := one(e.ctx, e.cloud(), "SELECT (SELECT count(*) FROM pg_trigger WHERE tgname = 'lokadb_prep_moved')::text || '/' || (SELECT lokadb.prep_moved_count())::text")
	if err != nil {
		return false, "", err
	}
	if kept != "1/1" {
		ok = false
	}
	notes = append(notes, "after the refusal triggers/moves "+kept+" (want 1/1)")
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare again failed: " + oneLine(err.Error()), nil
	}
	if err := e.enableArgs(); err != nil {
		return false, "the switch after -prepare again failed: " + oneLine(err.Error()), nil
	}
	n, err := stale()
	if err != nil {
		return false, "", err
	}
	if n != "0" {
		ok = false
	}
	notes = append(notes, n+" child rows disagree with their parent after the repair")
	// the switch took the trigger and the log with it
	gone, err := one(e.ctx, e.cloud(), "SELECT (SELECT count(*) FROM pg_trigger WHERE tgname = 'lokadb_prep_moved')::text || '/' || (SELECT count(*) FROM lokadb.prep_moved_log)::text")
	if err != nil {
		return false, "", err
	}
	if gone != "0/0" {
		ok = false
	}
	notes = append(notes, "after the switch triggers/log rows "+gone+" (want 0/0)")
	return ok, strings.Join(notes, "; "), nil
}

// E17 (A3 of the admin review). The log of moves must not grow for as long as enable is not run:
// lokadb.prep_moved_prune takes the old rows and counts them (lokadb.prep_moved_count = rows +
// pruned), so a row pruned while the switch checks cannot hide a row added. Old rows pruned: the
// count does not move; recent ones stay; the application's role cannot call the function.
func checkPrepareLogPrune(e *env) (bool, string, error) {
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare failed: " + oneLine(err.Error()), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO lokadb.prep_moved_log (xid, at) SELECT g, now() - interval '30 days' FROM generate_series(1, 5) g",
		"INSERT INTO lokadb.prep_moved_log (xid) VALUES (100), (101)"); err != nil {
		return false, "", err
	}
	before, err := one(e.ctx, e.cloud(), "SELECT lokadb.prep_moved_count()::text")
	if err != nil {
		return false, "", err
	}
	_, errApp := one(e.ctx, dsn(e.cloudHost, e.db, "app"), "SELECT lokadb.prep_moved_prune(0, interval '7 days')")
	pruned, err := one(e.ctx, e.cloud(), "SELECT lokadb.prep_moved_prune(0, interval '7 days')::text")
	if err != nil {
		return false, "", err
	}
	after, err := one(e.ctx, e.cloud(), "SELECT lokadb.prep_moved_count()::text")
	if err != nil {
		return false, "", err
	}
	rows, err := one(e.ctx, e.cloud(), "SELECT count(*)::text FROM lokadb.prep_moved_log")
	if err != nil {
		return false, "", err
	}
	// below the limit nothing is pruned
	none, err := one(e.ctx, e.cloud(), "SELECT lokadb.prep_moved_prune(1000000, interval '0')::text")
	if err != nil {
		return false, "", err
	}
	denied := errApp != nil && strings.Contains(errApp.Error(), "SQLSTATE 42501")
	ok := pruned == "5" && before == after && rows == "2" && none == "0" && denied
	return ok, fmt.Sprintf("the count %s before the prune and %s after it (want equal), %s old rows pruned (want 5), %s rows left (want 2), %s pruned below the limit (want 0), the application's role: %s",
		before, after, pruned, rows, none, short(errApp)), nil
}
