package main

// B9.11 (tasks/proxy-pooling.md): pgbouncer in transaction mode in front of the proxy, the
// pooling README recommends. pgbouncer keeps one server connection (default_pool_size = 1), so
// every client of the check shares one proxy session, transaction by transaction.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	if os.Getenv("DEFECTS_PGBOUNCER") == "" { // defects.sh starts pgbouncer only for DEFECTS_ONLY=B9.11
		return
	}
	checks = append(checks,
		check{id: "B9.11", name: "pgbouncer in transaction mode in front of the proxy: clients sharing one proxy session keep their organizations", run: checkPgbouncer},
	)
}

// pgbouncerPort: the proxy's port the pgbouncer of the stand points at (pgbouncer/pgbouncer.ini).
const pgbouncerPort = 6999

func (e *env) viaPgbouncer(extra string) string {
	return fmt.Sprintf("host=pgbouncer port=6432 dbname=%s user=app password=app sslmode=disable connect_timeout=10%s", e.db, extra)
}

func checkPgbouncer(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	docs := map[int]string{}
	for _, org := range []int{1, 2} {
		d, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT min(id) FROM documents WHERE organization_id = %d", org))
		if err != nil {
			return false, "", err
		}
		docs[org] = d
	}
	e.port = pgbouncerPort
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	var c0 *pgx.Conn
	for i := 0; ; i++ { // pgbouncer starts with the stand; the first login may race it
		if c0, err = pgx.Connect(e.ctx, e.viaPgbouncer("")); err == nil {
			break
		}
		if i == 20 {
			return false, "", fmt.Errorf("no connection through pgbouncer (is the check run with compose run --use-aliases?): %w", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	c0.Close(e.ctx)
	ok := true
	var notes []string

	// 1. Two clients, organizations 1 and 2, at once through one server connection: autocommit
	// inserts, transactions, batches (one Sync), all with pgx's named prepared statements.
	const rounds = 40
	pids := make([]string, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i, org := range []int{1, 2} {
		wg.Add(1)
		go func(i, org int) {
			defer wg.Done()
			errs[i] = pgbClient(e.ctx, e.viaPgbouncer(""), docs[org], fmt.Sprintf("B911-%d", org), rounds, &pids[i])
		}(i, org)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			ok = false
			notes = append(notes, fmt.Sprintf("WRONG client of organization %d: %s", i+1, firstErrLine(err)))
		}
	}
	placed, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(content || ':' || organization_id || ':' || n, ' ' ORDER BY content, organization_id), 'none') FROM (SELECT content, organization_id, count(*) n FROM document_versions WHERE content LIKE 'B911-%' GROUP BY 1, 2) x")
	if err != nil {
		return false, "", err
	}
	want := fmt.Sprintf("B911-1:1:%d B911-2:2:%d", rounds*7, rounds*7)
	if placed != want {
		ok = false
	}
	shared := pids[0] != "" && pids[0] == pids[1]
	if !shared {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("rows content:organization:count %s (want %s); one proxy session for both clients (pg_backend_pid %s, %s): %v", placed, want, pids[0], pids[1], shared))

	// 2. A plain SET lokadb.org of one client lasts on the server connection: the next client's
	// statement on a LokaDB table still gets the proxy's organization; what the proxy does not
	// route (here current_setting) sees the other client's value.
	a, err := pgx.Connect(e.ctx, e.viaPgbouncer(""))
	if err != nil {
		return false, "", err
	}
	b, err := pgx.Connect(e.ctx, e.viaPgbouncer(""))
	if err != nil {
		return false, "", err
	}
	if _, err := a.Exec(e.ctx, "SET lokadb.org = '2'"); err != nil {
		notes = append(notes, "SET lokadb.org alone: "+firstErrLine(err))
	}
	var seen string // right after A's SET: nothing routed in between
	_ = b.QueryRow(e.ctx, "SELECT coalesce(current_setting('lokadb.org', true), '')").Scan(&seen)
	if _, err := b.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'B911-set')", docs[1]); err != nil {
		ok = false
		notes = append(notes, "WRONG child row after another client's SET: "+firstErrLine(err))
	}
	org, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ','), 'none') FROM document_versions WHERE content = 'B911-set'")
	if err != nil {
		return false, "", err
	}
	if org != "1" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("after client A's SET lokadb.org = '2': client B's child row of organization 1 stored under %s; B's unrouted current_setting('lokadb.org') right after it = %q", org, seen))

	// 3. lokadb.org in the startup parameters does not reach the session through pgbouncer:
	// pgbouncer refuses the parameter, or the proxy does (B9.10).
	if c, err := pgx.Connect(e.ctx, e.viaPgbouncer(" options='-c lokadb.org=2'")); err == nil {
		var v string
		_ = c.QueryRow(e.ctx, "SELECT coalesce(current_setting('lokadb.org', true), '')").Scan(&v)
		c.Close(e.ctx)
		if v == "2" {
			ok = false
			notes = append(notes, "WRONG options=-c lokadb.org=2 through pgbouncer: the session has lokadb.org = 2")
		} else {
			notes = append(notes, fmt.Sprintf("options=-c lokadb.org=2 through pgbouncer: connected, lokadb.org of the session %q (pgbouncer dropped it)", v))
		}
	} else {
		notes = append(notes, "options=-c lokadb.org=2 through pgbouncer refused: "+firstErrLine(err))
	}
	a.Close(e.ctx)
	b.Close(e.ctx)

	// 4. The cooperative mode (lokadb.tenant) behind the pooler. Without tenant.local_only a session
	// value of one client names the organization of the next one on the shared server connection;
	// with it the session value is refused and SET LOCAL stays within its transaction.
	stop()
	stopped = true
	tok, tnotes, err := pgbTenant(e)
	if err != nil {
		return false, "", err
	}
	if !tok {
		ok = false
	}
	notes = append(notes, tnotes...)
	return ok, strings.Join(notes, "; "), nil
}

// pgbTenant: part 4 of B9.11, a proxy of its own for each of the two settings.
func pgbTenant(e *env) (bool, []string, error) {
	ok := true
	var notes []string
	count := func(c *pgx.Conn, sql string) (string, error) {
		var n string
		err := c.QueryRow(e.ctx, sql).Scan(&n)
		return n, err
	}
	const sel = "SELECT count(*)::text FROM documents"
	run := func(local *bool) (aErr, bErr error, bCount string, err error) {
		tenant := map[string]any{} // a section tenant without local_only: the default
		if local != nil {
			tenant["local_only"] = *local
		}
		stop, err := e.proxyCfg(map[string]string{}, map[string]any{
			"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
			"tenant": tenant,
		})
		if err != nil {
			return nil, nil, "", err
		}
		defer stop()
		var a, b *pgx.Conn
		for i := 0; ; i++ { // the pooler's server connection to the old proxy is gone: its first login may fail
			if a, err = pgx.Connect(e.ctx, e.viaPgbouncer("")); err == nil {
				break
			}
			if i == 20 {
				return nil, nil, "", err
			}
			time.Sleep(500 * time.Millisecond)
		}
		defer a.Close(e.ctx)
		if b, err = pgx.Connect(e.ctx, e.viaPgbouncer("")); err != nil {
			return nil, nil, "", err
		}
		defer b.Close(e.ctx)
		// client A names organization 2 for its session; client B, on the same server connection, names none
		_, aErr = a.Exec(e.ctx, "SET lokadb.tenant = '2'")
		bCount, bErr = count(b, sel)
		return aErr, bErr, bCount, nil
	}
	code := func(err error) string {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			return pe.Code
		}
		if err != nil {
			return "error"
		}
		return "none"
	}

	// local_only = false, set explicitly: the hazard, named as it is (README: the pooler needs local_only on)
	off := false
	aErr, bErr, bCount, err := run(&off)
	if err != nil {
		return false, nil, err
	}
	hazard := aErr == nil && bErr == nil && bCount == "100"
	notes = append(notes, fmt.Sprintf("with tenant.local_only = false: client A's SET lokadb.tenant = '2' %s; client B's SELECT count(*) naming no organization then ran in organization 2 (count %q, error %s): %v", map[bool]string{true: "went through", false: "was refused"}[aErr == nil], bCount, code(bErr), hazard))
	if !hazard {
		ok = false // the hazard is what local_only exists for: a changed behaviour is a finding either way
	}

	// local_only by default (on): the session value is refused, SET LOCAL lives in its transaction only
	aErr, bErr, bCount, err = run(nil)
	if err != nil {
		return false, nil, err
	}
	refusedSession := code(aErr) == "0A000"
	bRefused := bErr != nil && strings.Contains(bErr.Error(), "fanout")
	if !refusedSession || !bRefused {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("tenant.local_only by default: client A's SET lokadb.tenant = '2' refused %s (%v); client B's SELECT naming no organization: %s (refused by the fanout policy: %v)", code(aErr), refusedSession, bCount, bRefused))

	stop, err := e.proxyCfg(map[string]string{}, map[string]any{
		"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}},
		"tenant": map[string]any{},
	})
	if err != nil {
		return false, nil, err
	}
	defer stop()
	a, err := pgx.Connect(e.ctx, e.viaPgbouncer(""))
	if err != nil {
		return false, nil, err
	}
	defer a.Close(e.ctx)
	b, err := pgx.Connect(e.ctx, e.viaPgbouncer(""))
	if err != nil {
		return false, nil, err
	}
	defer b.Close(e.ctx)
	// SET LOCAL in client A's transaction names organization 2 there; client B's statement right after does not inherit it
	tx, err := a.Begin(e.ctx)
	if err != nil {
		return false, nil, err
	}
	var inTx string
	if _, err = tx.Exec(e.ctx, "SET LOCAL lokadb.tenant = '2'"); err == nil {
		err = tx.QueryRow(e.ctx, sel).Scan(&inTx)
	}
	if err != nil {
		_ = tx.Rollback(e.ctx)
		ok = false
		notes = append(notes, "WRONG SET LOCAL lokadb.tenant in a transaction: "+firstErrLine(err))
	} else if err = tx.Commit(e.ctx); err != nil {
		return false, nil, err
	}
	_, bErr2 := count(b, sel)
	afterRefused := bErr2 != nil && strings.Contains(bErr2.Error(), "fanout")
	if inTx != "100" || !afterRefused {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("SET LOCAL lokadb.tenant = '2' in A's transaction: count %q (want 100); B's next SELECT naming no organization refused by the fanout policy: %v", inTx, afterRefused))

	// set_config(…, false) refused alike; the setting in the startup parameters of a connection straight to the proxy too
	_, scErr := a.Exec(e.ctx, "SELECT set_config('lokadb.tenant', '2', false)")
	notes = append(notes, "set_config('lokadb.tenant', '2', false): "+code(scErr))
	if code(scErr) != "0A000" {
		ok = false
	}
	d := fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable options='-c lokadb.tenant=2'", e.port, e.db)
	if c, err := pgx.Connect(e.ctx, d); err == nil {
		c.Close(e.ctx)
		ok = false
		notes = append(notes, "WRONG options=-c lokadb.tenant=2 straight to the proxy was let in under the default tenant.local_only")
	} else {
		notes = append(notes, "options=-c lokadb.tenant=2 straight to the proxy: "+code(err))
		if code(err) != "0A000" {
			ok = false
		}
	}
	return ok, notes, nil
}

// pgbClient writes 7 child rows of doc a round: one autocommit insert, one transaction of
// one insert and a read, one batch of 5 inserts. pid gets the proxy's server session.
func pgbClient(ctx context.Context, dsn, doc, tag string, rounds int, pid *string) error {
	c, err := pgx.Connect(ctx, dsn) // pgx's default: named prepared statements, cached
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	ins := "INSERT INTO document_versions (document_id, content) VALUES ($1, $2)"
	for r := 0; r < rounds; r++ {
		if _, err := c.Exec(ctx, ins, doc, tag); err != nil {
			return fmt.Errorf("round %d, autocommit: %w", r, err)
		}
		tx, err := c.Begin(ctx)
		if err != nil {
			return err
		}
		var n int
		if _, err = tx.Exec(ctx, ins, doc, tag); err == nil {
			err = tx.QueryRow(ctx, "SELECT count(*) FROM document_versions WHERE document_id = $1", doc).Scan(&n)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("round %d, transaction: %w", r, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		bt := &pgx.Batch{}
		for i := 0; i < 5; i++ {
			bt.Queue(ins, doc, tag)
		}
		if err := c.SendBatch(ctx, bt).Close(); err != nil {
			return fmt.Errorf("round %d, batch: %w", r, err)
		}
	}
	return c.QueryRow(ctx, "SELECT pg_backend_pid()::text").Scan(pid)
}
