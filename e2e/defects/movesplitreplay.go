package main

// Block 5 (moving an organization), review 3 (R4-7): the split copied each table at a snapshot of
// its own and applied the log kept since before the first copy to every copy in turn: what the
// application did to the second table meanwhile was already in its copy and was applied again,
// through states the database never had. Ordinary writes failed the split.

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV47", name: "the split of an organization that writes on (a value of a UNIQUE column made, deleted and made again in the second table while the first one is copied): move-org succeeds and the rows are the writer's", run: checkSplitReplayUnique},
		check{id: "MV48", name: "no superuser: the split of an organization that makes and deletes a parent with its child in the second table while the first is copied succeeds, the FKs of the partition are valid", run: checkSplitReplayFK},
	)
}

// replayWriter runs steps (one transaction a step, the organization's lock shared as the proxy
// takes it) until stopped; the number of steps it finished and its first error.
type replayWriter struct {
	stop     chan struct{}
	done     chan struct{}
	mu       sync.Mutex
	finished int
	err      error
}

func (e *env) startReplayWriter(step func(tx pgx.Tx, n int) error) (*replayWriter, error) {
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return nil, err
	}
	// the organization the proxy would have set for the session (a child table has no tenant column)
	if _, err := w.Exec(e.ctx, "SELECT set_config('lokadb.org', '1', false)"); err != nil {
		w.Close(e.ctx)
		return nil, err
	}
	rw := &replayWriter{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(rw.done)
		defer w.Close(e.ctx)
		for n := 0; ; n++ {
			select {
			case <-rw.stop:
				return
			default:
			}
			err := pgx.BeginFunc(e.ctx, w, func(tx pgx.Tx) error {
				if _, err := tx.Exec(e.ctx, "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(1))"); err != nil {
					return err
				}
				return step(tx, n)
			})
			rw.mu.Lock()
			if err != nil {
				rw.err = err
				rw.mu.Unlock()
				return
			}
			rw.finished = n + 1
			rw.mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}()
	return rw, nil
}

func (rw *replayWriter) end() (int, error) {
	close(rw.stop)
	<-rw.done
	rw.mu.Lock()
	defer rw.mu.Unlock()
	// after the switch the organization's rows are at its database, which this session (the
	// cloud's admin, no user mapping) cannot reach: the writer stops there, as the move is done
	if rw.err != nil && strings.Contains(rw.err.Error(), "user mapping not found") {
		return rw.finished, nil
	}
	return rw.finished, rw.err
}

func bigOrgOne(e *env, rows int) error {
	return execSQL(e.ctx, e.cloud(),
		fmt.Sprintf("INSERT INTO documents (organization_id, user_id, title) SELECT 1, 1, repeat('x', 200) || g FROM generate_series(1, %d) g", rows),
		"ANALYZE")
}

func checkSplitReplayUnique(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE document_versions ADD COLUMN label text",
		"CREATE UNIQUE INDEX document_versions_label ON document_versions (document_id, label)"); err != nil {
		return false, "", err
	}
	if err := bigOrgOne(e, 300000); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	// three steps a label: made, deleted, made again (a step is one transaction)
	rw, err := e.startReplayWriter(func(tx pgx.Tx, n int) error {
		label := fmt.Sprintf("L%d", n/3)
		var sql string
		switch n % 3 {
		case 0, 2:
			sql = fmt.Sprintf("INSERT INTO document_versions (document_id, label, content) VALUES (%s, '%s', 'v%d')", doc, label, n%3)
		default:
			sql = fmt.Sprintf("DELETE FROM document_versions WHERE document_id = %s AND label = '%s'", doc, label)
		}
		_, err := tx.Exec(e.ctx, sql)
		return err
	})
	if err != nil {
		return false, "", err
	}
	out, _, moveErr := e.moveLines(1, nil, "-mode", "trigger")
	steps, werr := rw.end()
	if moveErr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if werr != nil {
		return false, "the writer failed: " + lastErr(werr.Error()), nil
	}
	// the labels with all three steps finished are there, and one with only its first (not the one cut off after its deletion)
	want := steps / 3
	if steps%3 == 1 {
		want++
	}
	got, _ := one(e.ctx, e.customer(), fmt.Sprintf("SELECT count(*) FROM document_versions WHERE document_id = %s AND label LIKE 'L%%'", doc))
	return got == fmt.Sprint(want), fmt.Sprintf("the writer finished %d steps; labels at the organization's database: %s (want %d)", steps, got, want), nil
}

func checkSplitReplayFK(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE document_versions DROP CONSTRAINT document_versions_document_id_fkey",
		"ALTER TABLE document_versions ADD FOREIGN KEY (document_id) REFERENCES documents ON DELETE CASCADE"); err != nil {
		return false, "", err
	}
	if err := bigOrgOne(e, 300000); err != nil {
		return false, "", err
	}
	if err := asAdmin(e); err != nil {
		return false, "enable as lokadb_saas failed: " + lastErr(err.Error()), nil
	}
	// a document with a version, then the document gone (and the version with it)
	rw, err := e.startReplayWriter(func(tx pgx.Tx, n int) error {
		if n%2 == 0 {
			var id int64
			if err := tx.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1, 'mv48') RETURNING id").Scan(&id); err != nil {
				return err
			}
			_, err := tx.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'mv48')", id)
			return err
		}
		_, err := tx.Exec(e.ctx, "DELETE FROM documents WHERE organization_id = 1 AND title = 'mv48'")
		return err
	})
	if err != nil {
		return false, "", err
	}
	out, _, moveErr := e.moveLines(1, nil, "-mode", "trigger")
	steps, werr := rw.end()
	if moveErr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if werr != nil {
		return false, "the writer failed: " + lastErr(werr.Error()), nil
	}
	// the last step may be a making without its deletion
	left := (steps % 2)
	got, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'mv48'")
	vers, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'mv48'")
	notValid, _ := one(e.ctx, e.cloud(), `SELECT count(*) FROM pg_constraint WHERE contype = 'f' AND NOT convalidated AND conrelid::regclass::text LIKE 'document%'`)
	ok := got == fmt.Sprint(left) && vers == fmt.Sprint(left) && notValid == "0"
	return ok, fmt.Sprintf("the writer finished %d steps; documents and versions it left at the organization's database: %s and %s (want %d); FKs not validated: %s (want 0)",
		steps, got, vers, left, notValid), nil
}
