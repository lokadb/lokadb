package main

// The remaining proxy and move checks (research/ANALYSIS.md 5.3, 5.4): P5, P14, P15, P17,
// P18, R5.

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
		check{id: "P5", name: "SET TRANSACTION right after BEGIN works through the proxy", run: checkSetTransaction},
		check{id: "P14", name: "an UPDATE sent straight to the organization's partition keeps to that organization", run: checkTargetFilter},
		check{id: "P15", name: "an id written with a leading zero finds its organization", run: checkLeadingZero},
		check{id: "P15", name: "a failed refs lookup inside a transaction does not abort it", run: checkLookupInTx},
		check{id: "P17", name: "connections over max_connections are refused, the others served", run: checkMaxConnections},
		check{id: "P18", name: "after an error in an implicit transaction a view sees every organization again", run: checkOrgAfterError},
		check{id: "R5", name: "a cloud organization's writes do not queue for long behind the move's lock on DEFAULT", run: checkDefaultLockWait},
	)
}

// P5. lokadb.org was set before every statement once it had been set: SET TRANSACTION
// ISOLATION LEVEL right after BEGIN failed ("must be called before any query").
func checkSetTransaction(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var errs []string
	for _, q := range []string{"SELECT count(*) FROM documents WHERE organization_id = 1", "BEGIN",
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", "SELECT count(*) FROM documents WHERE organization_id = 2", "COMMIT"} {
		if _, err := simple(e.ctx, c, q); err != nil {
			errs = append(errs, q+": "+short(err))
		}
	}
	// the same through the extended protocol, as drivers send it
	if _, err := raw(e, c, 0, &pgproto3.Parse{Query: "BEGIN"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"}, &pgproto3.Bind{}, &pgproto3.Execute{},
		&pgproto3.Parse{Query: "COMMIT"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{}); err != nil {
		errs = append(errs, "extended: "+short(err))
	}
	if len(errs) > 0 {
		return false, strings.Join(errs, "; "), nil
	}
	return true, "both protocols: no error", nil
}

// P14. The statement sent straight to the foreign partition had none of the parent's
// policies: rows the organization's database held for another organization were changed
// and returned.
func checkTargetFilter(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// the organization's database (it controls it) slips in a row of organization 2
	if err := execSQL(e.ctx, e.customer(),
		"ALTER TABLE documents DROP CONSTRAINT lokadb_owner",
		"INSERT INTO documents (id, organization_id, title) VALUES (999999, 2, 'planted')"); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.customer(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	// by id: the proxy finds organization 1 from its document and sends the UPDATE straight there
	res, err := simple(e.ctx, c, "UPDATE documents SET title = 'p14' WHERE id IN ("+doc+", 999999) RETURNING organization_id")
	if err != nil {
		return false, "", err
	}
	foreign := 0
	if len(res) == 1 {
		for _, r := range res[0].Rows {
			if string(r[0]) != "1" {
				foreign++
			}
		}
	}
	planted, err := one(e.ctx, e.customer(), "SELECT title FROM documents WHERE id = 999999")
	if err != nil {
		return false, "", err
	}
	return foreign == 0 && planted == "planted", fmt.Sprintf("rows of another organization returned: %d; the planted row's title: %s", foreign, planted), nil
}

// P15. Ids were cached as the text the client wrote: 0123 found nothing and the statement
// ran under "no organization".
func checkLeadingZero(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	// a text parameter, as drivers send it (a literal 00N the parser already reads as N)
	rows, err := raw(e, c, 0,
		&pgproto3.Parse{Query: "SELECT count(*) FROM document_versions WHERE document_id = $1::bigint"},
		&pgproto3.Bind{Parameters: [][]byte{[]byte("00" + doc)}}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if err != nil {
		return false, "", err
	}
	n := "?"
	if len(rows) == 1 {
		n = rows[0][0]
	}
	return n == "1", fmt.Sprintf("versions of document '00%s': %s (it has 1)", doc, n), nil
}

// P15. The refs lookup ran in the client's transaction: when it failed, the transaction was aborted.
func checkLookupInTx(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	if _, err := tx.Exec(e.ctx, "LOCK TABLE documents__refs IN ACCESS EXCLUSIVE MODE"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, q := range []string{"BEGIN", "SET LOCAL lock_timeout = '300ms'"} {
		if _, err := simple(e.ctx, c, q); err != nil {
			return false, "", err
		}
	}
	_, lookErr := simple(e.ctx, c, "SELECT count(*) FROM document_versions WHERE document_id = "+doc)
	_ = tx.Rollback(e.ctx)
	_, nextErr := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 2")
	_, _ = simple(e.ctx, c, "ROLLBACK")
	return lookErr != nil && nextErr == nil, fmt.Sprintf("the lookup: error %s; the next statement of the transaction: error %s", short(lookErr), short(nextErr)), nil
}

// P17. Neither a limit nor a handshake timeout: every connection was accepted.
func checkMaxConnections(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"max_connections": 3})
	if err != nil {
		return false, "", err
	}
	defer stop()
	var open []*pgconn.PgConn
	defer func() {
		for _, c := range open {
			c.Close(e.ctx)
		}
	}()
	for i := 0; i < 3; i++ {
		c, err := e.viaProxy()
		if err != nil {
			return false, fmt.Sprintf("connection %d of 3 refused: %s", i+1, short(err)), nil
		}
		open = append(open, c)
	}
	_, overErr := e.viaProxy()
	open[0].Close(e.ctx)
	open = open[1:]
	time.Sleep(200 * time.Millisecond)
	again, againErr := e.viaProxy()
	if againErr == nil {
		open = append(open, again)
	}
	ok := overErr != nil && strings.Contains(overErr.Error(), "too many connections") && againErr == nil
	return ok, fmt.Sprintf("4th connection: %s; after one closed: error %s", short(overErr), short(againErr)), nil
}

// P18. An error in an implicit transaction rolls back the set_config sent in it; the proxy
// still believed its value, and a view (not seen as a LokaDB table) ran under the old one.
func checkOrgAfterError(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := execSQL(e.ctx, e.cloud(), "CREATE VIEW v_docs AS SELECT * FROM documents", "ALTER VIEW v_docs OWNER TO app"); err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	// lokadb.org = 2, committed
	if _, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 2"); err != nil {
		return false, "", err
	}
	// a batch resets lokadb.org for the view, then fails: its implicit transaction rolls the reset back
	_, _ = raw(e, c, 0, &pgproto3.Parse{Query: "SELECT 1/0 FROM v_docs"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	rows, err := raw(e, c, 0, &pgproto3.Parse{Query: "SELECT count(*) FROM v_docs"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
	if err != nil {
		return false, "", err
	}
	n := "?"
	if len(rows) == 1 {
		n = rows[0][0]
	}
	return n == "200", "the view counts " + n + " documents (200 in both organizations)", nil
}

// R5. The move's ADD/DROP CONSTRAINT on DEFAULT waited for its ACCESS EXCLUSIVE lock with
// no limit: behind a long transaction, every cloud organization's writes queued behind it.
func checkDefaultLockWait(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// organization 3 lives in DEFAULT (created after enable); a long transaction reads it
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (3, 2, 'in default')"); err != nil {
		return false, "", err
	}
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	if _, err := tx.Exec(e.ctx, "SELECT count(*) FROM documents__cloud"); err != nil {
		return false, "", err
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the long transaction ends after 8 s
		defer wg.Done()
		time.Sleep(8 * time.Second)
		_ = tx.Rollback(e.ctx)
	}()
	moveDone := make(chan error, 1)
	go func() {
		_, err := e.move(1, "-lock-timeout", "300ms")
		moveDone <- err
	}()
	time.Sleep(2 * time.Second) // the move is at step 3 by now, waiting for DEFAULT
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	var worst time.Duration
	for i := 0; i < 5; i++ {
		t0 := time.Now()
		if _, err := w.Exec(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (3, 2, 'r5')"); err != nil {
			return false, "", err
		}
		if d := time.Since(t0); d > worst {
			worst = d
		}
		time.Sleep(300 * time.Millisecond)
	}
	wg.Wait()
	moveErr := <-moveDone
	state := "done"
	if moveErr != nil {
		state = "failed: " + short(moveErr)
	}
	return worst < 2*time.Second, fmt.Sprintf("slowest write of organization 3 during the move: %s; move-org %s", worst.Round(10*time.Millisecond), state), nil
}
