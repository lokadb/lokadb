package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// keyIndex names the index on the partition key of a table's DEFAULT partition.
func keyIndex(t string) string { return nameOf(hn(t, "__key")) }

// ensureKeyIndex builds the index on the partition key of DEFAULT (<table>__cloud) when it is
// missing. Every query of an organization reads DEFAULT too (the policy is a range, and a
// LIST DEFAULT is pruned by equality only), and the split looks the organization's rows up
// there: without the index both scan the whole cloud table. CONCURRENTLY: writes go on.
func ensureKeyIndex(ctx context.Context, conn *pgx.Conn, t, key string, log *slog.Logger) error {
	name := keyIndex(t)
	r, err := rowsOf(ctx, conn, `SELECT i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1 AND c.relnamespace = $2::regnamespace`, name, q(schemaOf(t)))
	if err != nil {
		return err
	}
	if len(r) == 1 && r[0][0] == "true" {
		return nil
	}
	if len(r) == 1 { // an interrupted CREATE INDEX CONCURRENTLY leaves an invalid index
		if _, err := conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+qr(inSchemaOf(t, name))); err != nil {
			return err
		}
	}
	start := time.Now()
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", q(name), qr(hn(t, "__cloud")), key)); err != nil {
		return fmt.Errorf("index on the organization column of %s: %w", hn(t, "__cloud"), err)
	}
	log.Info("index on the organization column of DEFAULT", "table", t, "index", name, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

var errNoReplicaRole = errors.New("session_replication_role cannot be set")

// splitOrg gives the organization a partition of its own in every spec table that has none
// yet (move-org's first step: enable leaves every organization in DEFAULT). Online when it
// can: the organization pauses (org_lock), the others go on, and DEFAULT is locked only for
// the attach (splitTable: copied in batches, moved in one short transaction). Without the right to set session_replication_role it falls
// back to lokadb.partition_org, which locks DEFAULT for the whole split.
func splitOrg(ctx context.Context, cloud *pgx.Conn, o MoveOptions, log *slog.Logger) error {
	var todo []SpecTable
	for _, st := range o.Spec.Tables {
		part := hn(st.Name, fmt.Sprintf("__o%d", o.Org))
		r, err := rowsOf(ctx, cloud, `SELECT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass($1))::text`, qr(part))
		if err != nil {
			return err
		}
		if r[0][0] == "false" {
			todo = append(todo, st)
		}
	}
	if len(todo) == 0 {
		return nil
	}
	// Before anything of the split (its CHECK on DEFAULT would stay after a refusal): enable
	// points every FK onto a LokaDB table at its refs table; one onto DEFAULT itself (missed by
	// an older enable, e.g. for a name that needs quotes) would fire on the rows moved.
	for _, st := range todo {
		r, err := rowsOf(ctx, cloud, `SELECT coalesce(string_agg(conname || ' on ' || conrelid::regclass::text, ', '), '')
			FROM pg_constraint WHERE contype = 'f' AND confrelid = $1::regclass`, qr(hn(st.Name, "__cloud")))
		if err != nil {
			return err
		}
		if r[0][0] != "" {
			return fmt.Errorf("%s__cloud is referenced by foreign keys that enable did not point at %s__refs (%s): moving the organization's rows would fire them; repoint them at %s__refs",
				st.Name, st.Name, r[0][0], st.Name)
		}
	}
	err := splitOnline(ctx, cloud, o, todo, log)
	if !errors.Is(err, errNoReplicaRole) {
		return err
	}
	log.Warn("this role cannot set session_replication_role: the organization is split out of DEFAULT under a lock on it (reads and writes of every cloud organization wait)")
	for _, st := range todo {
		start := time.Now()
		if err := splitOut(ctx, cloud, o, st.Name); err != nil {
			return err
		}
		log.Warn("organization split out of DEFAULT under a lock", "table", st.Name, "took", time.Since(start).Round(time.Millisecond))
	}
	return nil
}

func splitOnline(ctx context.Context, cloud *pgx.Conn, o MoveOptions, todo []SpecTable, log *slog.Logger) error {
	if err := pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica")
		return err
	}); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			return errNoReplicaRole
		}
		return err
	}
	chk := fmt.Sprintf("lokadb_not_org%d", o.Org)
	var prep []string
	guard := map[string]string{} // table -> its CHECK on DEFAULT, added right before its split
	for _, st := range todo {
		t := st.Name
		tc, err := rowsOf(ctx, cloud, `SELECT tenant_column FROM lokadb.tables WHERE table_name = $1`, t)
		if err != nil || len(tc) == 0 {
			return fmt.Errorf("%s is not enabled (run enable first): %v", t, err)
		}
		if err := ensureKeyIndex(ctx, cloud, t, o.org().Key(tc[0][0]), log); err != nil {
			return err
		}
		def, part := hn(t, "__cloud"), hn(t, fmt.Sprintf("__o%d", o.Org))
		// A partition left by an attempt that failed holds a copy of rows still in DEFAULT (the
		// rows leave DEFAULT and the partition is attached in one transaction): made again.
		left, err := rowsOf(ctx, cloud, `SELECT (to_regclass($1) IS NOT NULL)::text,
			coalesce(obj_description(to_regclass($1), 'pg_class'), '') = $2`, qr(part), splitMark(o.Org))
		if err != nil {
			return err
		}
		if left[0][0] == "true" {
			n, err := rowsOf(ctx, cloud, fmt.Sprintf("SELECT count(*)::text FROM %s", qr(part)))
			if err != nil {
				return err
			}
			if n[0][0] != "0" && left[0][1] != "true" {
				return fmt.Errorf("%s exists, is not attached and holds %s rows: not touched; look at it before moving again", part, n[0][0])
			}
			prep = append(prep, "DROP TABLE "+qr(part))
		}
		key, val := o.org().Key(tc[0][0]), o.org().Lit()
		owner, err := rowsOf(ctx, cloud, `SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass`, qr(t))
		if err != nil {
			return err
		}
		prep = append(prep,
			fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", qr(part), qr(def)),
			// block 3, A11: LIKE copies an invalid index of DEFAULT as a valid one
			fmt.Sprintf("SELECT lokadb.drop_invalid_copies(%s, %s)", lit(def), lit(part)),
			// IS NOT NULL too: without it ATTACH cannot prove the partition constraint and scans
			fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT lokadb_key CHECK (%s IS NOT NULL AND %s = %s)", qr(part), key, key, val),
			fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(part), q(owner[0][0])),
			fmt.Sprintf("COMMENT ON TABLE %s IS %s", qr(part), lit(splitMark(o.Org))))
		copied, err := rowsOf(ctx, cloud, `SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass AND conname LIKE 'lokadb\_not\_org%'`, qr(def))
		if err != nil {
			return err
		}
		for _, c := range copied {
			prep = append(prep, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", qr(part), q(c[0])))
		}
		// The FKs of DEFAULT (to the refs and cloud tables) go on the partition now, while it
		// is empty: adding an FK locks the table it references, and must not hold that lock
		// for as long as the rows take to move.
		fks, err := rowsOf(ctx, cloud, `SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'f'`, qr(def))
		if err != nil {
			return err
		}
		for _, f := range fks {
			prep = append(prep, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", qr(part), q(f[0]), f[1]))
		}
		valid, err := rowsOf(ctx, cloud, `SELECT convalidated::text FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2`, qr(def), chk)
		if err != nil {
			return err
		}
		if len(valid) == 0 {
			// From here on a row of the organization cannot reach DEFAULT, not even past the
			// proxy. Only once the organization is paused, and right before the table's split:
			// NOT VALID spares the rows already there, but not their updates, and a writer past
			// the proxy does not pause.
			guard[t] = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s <> %s) NOT VALID", qr(def), q(chk), key, val)
		}
	}
	if err := lockedDDL(ctx, cloud, o, prep); err != nil {
		return err
	}

	// The organization writes on while its rows are copied: triggers on DEFAULT log the changes
	// of its rows (lokadb.move_log, as the move's capture does), the copy reads one snapshot taken
	// after them, and the log is applied to the copy. The organization pauses only for the end:
	// the rest of the log and the last transaction of every table. Before, it paused for the
	// whole copy: 66 s for 3 million rows (block 14, 27.10).
	tables, err := splitMoveTables(ctx, cloud, todo, o)
	if err != nil {
		return err
	}
	so := o
	so.replica = &replicaRight{} // the log is applied in the cloud, not in the organization's database
	hbCtx, stopBeat := context.WithCancel(ctx)
	defer stopBeat()
	defer dropSplitCapture(cloud, o, tables, log)
	if err := installSplitCapture(ctx, cloud, o, tables); err != nil {
		return err
	}
	go beat(hbCtx, o.CloudDSN, o.Org, log)
	truncateAll := func() {
		for _, t := range tables {
			truncateCopy(cloud, o, t.local(), log)
		}
	}
	copyDeadline := time.Now().Add(o.copyTimeout())
	for _, t := range tables {
		def := hn(t.name, "__cloud")
		cols, err := copyColsOf(ctx, cloud, def)
		if err == nil {
			err = splitCopy(ctx, cloud, o, t.name, def, t.local(), o.org().Key(t.tenant), cols, copyDeadline, log)
		}
		if err != nil {
			truncateAll()
			return fmt.Errorf("split %s out of DEFAULT: copying the rows: %w; they stay in DEFAULT", t.name, err)
		}
	}
	// what changed meanwhile, the organization still writing (rounds until the log is short)
	cur := &logCursor{}
	for {
		if err := captureOK(ctx, cloud, o.Org); err != nil {
			truncateAll()
			return err
		}
		n, err := applyLog(ctx, nil, cloud, cloud, so, tables, applyRoundEntries, cur)
		if err != nil {
			truncateAll()
			return fmt.Errorf("split out of DEFAULT: applying the changes made during the copy: %w", err)
		}
		if !cur.more {
			log.Info("split: changes made during the copy applied", "last_round", n)
			break
		}
		if time.Now().After(copyDeadline) {
			truncateAll()
			return fmt.Errorf("split out of DEFAULT: the changes made during the copy not applied in %s (-copy-timeout)", o.copyTimeout())
		}
	}

	unpause, err := pauseOrg(ctx, cloud, o, log)
	if err != nil {
		truncateAll()
		return err
	}
	pausedAt := time.Now()
	defer unpause()
	fail := func(err error) error {
		// The rows stayed in DEFAULT, under a CHECK that refuses their updates: removed
		// before the organization resumes (MoveOrg's cleanup tries again on a new connection).
		dropGuards(cloud, o, log)
		truncateAll()
		return err
	}
	// Table by table: the CHECK on its DEFAULT (right before its own last transaction, so that
	// an application's transaction past the proxy, a cascade of a user into another table's rows,
	// is refused as late as it can be: MV17), the rest of the log, the table's last transaction.
	// The CHECK waits for the transactions of DEFAULT still open, so after it no change of the
	// organization's rows of the table is in flight; what they logged is applied before the
	// last transaction drops the copies of rows gone.
	cur = &logCursor{}
	for _, t := range tables {
		if g := guard[t.name]; g != "" {
			if err := lockedDDL(ctx, cloud, o, []string{g}); err != nil {
				return fail(err)
			}
		}
		for {
			if time.Since(pausedAt) > o.maxSplitPause() {
				return fail(fmt.Errorf("%w (%s): the log of the copy not applied; the rows stay in DEFAULT", errSplitPause, o.maxSplitPause()))
			}
			if err := captureOK(ctx, cloud, o.Org); err != nil {
				return fail(err)
			}
			n, err := applyLog(ctx, nil, cloud, cloud, so, tables, applyRoundEntries, cur)
			if err != nil {
				return fail(fmt.Errorf("split out of DEFAULT: applying the last changes: %w", err))
			}
			if n == 0 {
				break
			}
		}
		if err := splitTable(ctx, cloud, o, t.name, t.tenant, pausedAt, log); err != nil {
			return fail(fmt.Errorf("split %s out of DEFAULT: %w", t.name, err))
		}
	}
	return nil
}

// splitMoveTables: the tables of todo as applyLog takes them, applied to the organization's
// partition in the cloud (t.local()), made by LIKE DEFAULT: no unique index on the key alone
// (DEFAULT's include the organization column), so an entry replaces the row by deleting it.
func splitMoveTables(ctx context.Context, cloud *pgx.Conn, todo []SpecTable, o MoveOptions) ([]moveTable, error) {
	var out []moveTable
	for _, st := range todo {
		tc, err := rowsOf(ctx, cloud, `SELECT tenant_column, coalesce(to_jsonb(b) ->> 'key_column', 'id') FROM lokadb.tables b WHERE table_name = $1`, st.Name)
		if err != nil || len(tc) == 0 {
			return nil, fmt.Errorf("%s is not enabled (run enable first): %v", st.Name, err)
		}
		cols, err := tableColumns(ctx, cloud, st.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, moveTable{name: st.Name, tenant: tc[0][0], key: tc[0][1], cols: cols, org: o.Org, val: o.org().Lit(), replace: true})
	}
	return out, nil
}

// splitTrigger: the capture triggers of the split on DEFAULT, by what they log: i an insert of
// the organization's row, u an update leaving it the organization's (or making it so), x an
// update taking it to another organization (logged as its delete), d a delete. The names begin
// as the move's (lokadb_move_org<N>): move-cleanup finds them.
func splitTrigger(org int64, kind string) string {
	return fmt.Sprintf("%s_split_%s", captureTrigger(org), kind)
}

// installSplitCapture logs the changes of the organization's rows of DEFAULT (move_log).
func installSplitCapture(ctx context.Context, conn *pgx.Conn, o MoveOptions, tables []moveTable) error {
	if err := execAll(ctx, conn, []string{captureDDL(), fmt.Sprintf("DELETE FROM lokadb.move_log WHERE org = %d", o.Org),
		fmt.Sprintf(`INSERT INTO lokadb.move_heartbeat VALUES (%d, clock_timestamp(), false)
			ON CONFLICT (org) DO UPDATE SET at = excluded.at, lost = false`, o.Org)}); err != nil {
		return err
	}
	var funcs, stmts []string
	for _, t := range tables {
		def := hn(t.name, "__cloud")
		out := def + " out" // the function of x: its own name
		funcs = append(funcs, captureFuncDDL(o.Org, t, def), fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  PERFORM lokadb.move_log_entry(%d, %s, 'D', %s, NULL, NULL, NULL);
  RETURN NULL;
END $$`, captureFunc(o.Org, out), o.Org, lit(t.name), keyText(t, "OLD")))
		col, val := q(t.tenant), o.org().Lit()
		for _, tr := range []struct{ kind, event, when, fn string }{
			{"i", "INSERT", fmt.Sprintf("NEW.%s = %s", col, val), captureFunc(o.Org, def)},
			{"u", "UPDATE", fmt.Sprintf("NEW.%s = %s", col, val), captureFunc(o.Org, def)},
			{"x", "UPDATE", fmt.Sprintf("OLD.%s = %s AND NEW.%s IS DISTINCT FROM %s", col, val, col, val), captureFunc(o.Org, out)},
			{"d", "DELETE", fmt.Sprintf("OLD.%s = %s", col, val), captureFunc(o.Org, def)},
		} {
			stmts = append(stmts,
				fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(splitTrigger(o.Org, tr.kind)), qr(def)),
				fmt.Sprintf("CREATE TRIGGER %s AFTER %s ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION %s()",
					q(splitTrigger(o.Org, tr.kind)), tr.event, qr(def), tr.when, tr.fn))
		}
	}
	if err := execAll(ctx, conn, funcs); err != nil {
		return err
	}
	return lockedDDL(ctx, conn, o, stmts) // CREATE TRIGGER takes SHARE ROW EXCLUSIVE on DEFAULT
}

// dropSplitCapture removes the split's triggers and its log, on its own context (the split may
// be failing because its context was cancelled). After the ATTACH they log nothing: the
// organization's rows are in its partition.
func dropSplitCapture(conn *pgx.Conn, o MoveOptions, tables []moveTable, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stmts []string
	for _, t := range tables {
		def := hn(t.name, "__cloud")
		for _, k := range []string{"i", "u", "x", "d"} {
			stmts = append(stmts, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(splitTrigger(o.Org, k)), qr(def)))
		}
	}
	err := lockedDDL(ctx, conn, o, stmts)
	if err == nil {
		var tail []string
		for _, t := range tables {
			def := hn(t.name, "__cloud")
			tail = append(tail, "DROP FUNCTION IF EXISTS "+captureFunc(o.Org, def)+"()", "DROP FUNCTION IF EXISTS "+captureFunc(o.Org, def+" out")+"()")
		}
		tail = append(tail, fmt.Sprintf(`DO $$ BEGIN
			IF to_regclass('lokadb.move_log') IS NOT NULL THEN DELETE FROM lokadb.move_log WHERE org = %d; END IF;
			IF to_regclass('lokadb.move_heartbeat') IS NOT NULL THEN DELETE FROM lokadb.move_heartbeat WHERE org = %d; END IF;
			END $$`, o.Org, o.Org))
		err = execAll(ctx, conn, tail)
	}
	if err != nil {
		log.Warn("the split's change capture on DEFAULT is left (it stops logging after the heartbeat's time); move-cleanup removes it", "err", err)
	}
}

// pauseOrg holds the organization's org_lock and the one of statements not confined to one
// organization, both exclusive: their open transactions finish, new ones wait (the proxy
// takes them shared). Each wait is shorter than deadlock_timeout: in a deadlock with an
// application's transaction (it holds one lock shared and waits for the other) move-org
// gives way and tries again, instead of Postgres cancelling the application.
func pauseOrg(ctx context.Context, conn *pgx.Conn, o MoveOptions, log *slog.Logger) (func(), error) {
	// the key the proxy holds shared: by the organization's value for one numbered by uuid
	if err := o.resolve(ctx, conn); err != nil {
		return nil, err
	}
	r, err := rowsOf(ctx, conn, `SELECT (extract(epoch FROM current_setting('deadlock_timeout')::interval) * 1000)::bigint::text`)
	if err != nil {
		return nil, err
	}
	dl, _ := strconv.ParseInt(r[0][0], 10, 64)
	wait := o.LockTimeout.Milliseconds()
	if limit := dl * 4 / 5; limit > 0 && wait > limit {
		wait = limit
	}
	unlock := func(stmts ...string) {
		for _, s := range stmts {
			_, _ = conn.Exec(context.Background(), s)
		}
	}
	lockOrg, unlockOrg := "SELECT pg_advisory_lock("+o.org().LockCall()+")", "SELECT pg_advisory_unlock("+o.org().LockCall()+")"
	const lockAll, unlockAll = "SELECT pg_advisory_lock(lokadb.org_lock(NULL))", "SELECT pg_advisory_unlock(lokadb.org_lock(NULL))"
	attempts := max(o.Attempts, 1) * 4
	for attempt := 1; ; attempt++ {
		err := func() error {
			if _, err := conn.Exec(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", wait)); err != nil {
				return err
			}
			defer conn.Exec(context.Background(), "RESET lock_timeout") //nolint:errcheck
			if _, err := conn.Exec(ctx, lockOrg); err != nil {
				return err
			}
			if _, err := conn.Exec(ctx, lockAll); err != nil {
				unlock(unlockOrg)
				return err
			}
			return nil
		}()
		if err == nil {
			log.Info("organization paused (its statements wait, the other organizations' do not)", "attempt", attempt)
			paused := time.Now()
			return func() {
				unlock(unlockAll, unlockOrg)
				log.Info("organization resumed", "paused", time.Since(paused).Round(time.Millisecond))
			}, nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "55P03" && pgErr.Code != "40P01") || attempt >= attempts {
			return nil, fmt.Errorf("pause the organization: %w", err)
		}
		if attempt%4 == 0 {
			log.Warn("the organization's transactions keep running, retrying the pause", "attempt", attempt, "err", strings.TrimSpace(pgErr.Message))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// staleGuardsSQL lists the CHECKs lokadb_not_org<N> on DEFAULT partitions whose organization
// has no partition of its own attached (__o<N> in the cloud, __org<N> moved): what a split
// that failed or was killed left. With its rows still in DEFAULT, every UPDATE of them fails.
// $1: the organization, NULL for every one.
var staleGuardsSQL = `SELECT format('ALTER TABLE %s DROP CONSTRAINT %I', c.conrelid::regclass, c.conname)
	FROM lokadb.tables t JOIN pg_constraint c ON c.conrelid = to_regclass(` + relSQL("t.cloud_partition") + `)
	WHERE c.contype = 'c' AND c.conname ~ '^lokadb_not_org[0-9]+$'
	  AND ($1::bigint IS NULL OR c.conname = 'lokadb_not_org' || $1::bigint)
	  AND NOT EXISTS (SELECT 1 FROM pg_inherits i
	    WHERE i.inhparent = to_regclass(` + relSQL("t.table_name") + `)
	      AND i.inhrelid IN (to_regclass(` + relSQL(rel.HelperSQL("t.table_name", "('__o' || substr(c.conname, length('lokadb_not_org') + 1))")) + `),
	        to_regclass(` + relSQL(rel.HelperSQL("t.table_name", "('__org' || substr(c.conname, length('lokadb_not_org') + 1))")) + `)))
	ORDER BY 1`

// staleGuards: the statements that drop the stale CHECKs of org (0: of every organization).
func staleGuards(ctx context.Context, conn querier, org int64) ([]string, error) {
	var arg any
	if org != 0 {
		arg = org
	}
	r, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil || r[0][0] != "true" {
		return nil, err
	}
	rows, err := rowsOf(ctx, conn, staleGuardsSQL, arg)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, row := range rows {
		out = append(out, row[0])
	}
	return out, nil
}

// dropGuards removes the organization's stale CHECKs on DEFAULT, on its own context: the
// move may be failing because its context was cancelled.
func dropGuards(conn *pgx.Conn, o MoveOptions, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stmts, err := staleGuards(ctx, conn, o.Org)
	if err == nil {
		err = lockedDDL(ctx, conn, o, stmts)
	}
	if err != nil {
		log.Warn("the CHECK on DEFAULT of the failed split is left: updates of the organization's rows there fail until move-cleanup or a new move-org", "err", err)
		return
	}
	for _, s := range stmts {
		log.Info("removed the CHECK on DEFAULT of the failed split", "statement", s)
	}
}

// splitMark is the comment on the organization's partition made by the split: a copy of it
// left by an attempt that failed may be dropped.
func splitMark(org int64) string {
	return fmt.Sprintf("lokadb: split of organization %d out of DEFAULT", org)
}

var errSplitPause = errors.New("the organization's pause reached -max-split-pause")

// splitTable moves the organization's rows of table out of DEFAULT into its partition (copied
// there by splitOnline, unattached, with the changes since applied; the organization paused,
// the CHECK on DEFAULT added). The rows stayed in DEFAULT until here: the organization's rows
// are where they were if the move stops or is killed. Nothing adds or changes them now (the
// pause, the CHECK); a delete, an FK cascade from a cloud table, is undone in the copy below.
//
// One transaction: the copies of rows deleted since go, the counts must agree, the rows
// leave DEFAULT (one DELETE: row locks only, the other organizations go on), the CHECK is
// validated (SHARE UPDATE EXCLUSIVE: reads and writes go on) and the partition attached,
// which then scans nothing. It yields to the application: holding the deleted rows, a
// lock it waits for may be held by a writer waiting for one of them (a cascade from a
// cloud table into DEFAULT), and Postgres cancels whoever waits deadlock_timeout first.
// So each lock waits less than deadlock_timeout (switchLockTry), and a lock not had
// rolls the whole transaction back (the rows' locks go too) to run it again, up to
// LockTimeout times Attempts in all.
//
// The pause, from pausedAt, lasts at most MaxSplitPause: the last transaction is cancelled
// (statement_timeout), and the rows stay in DEFAULT.
func splitTable(ctx context.Context, cloud *pgx.Conn, o MoveOptions, t, tc string, pausedAt time.Time, log *slog.Logger) error {
	start := time.Now()
	def, part, key := hn(t, "__cloud"), hn(t, fmt.Sprintf("__o%d", o.Org)), o.org().Key(tc)
	val := o.org().Lit()
	limit := o.maxSplitPause()
	deadline := pausedAt.Add(limit)
	// the key (block 4: lokadb.tables.key_column; id where the column is not there yet)
	kr, err := rowsOf(ctx, cloud, `SELECT coalesce(to_jsonb(b) ->> 'key_column', 'id') FROM lokadb.tables b WHERE b.table_name = $1`, t)
	if err != nil || len(kr) == 0 {
		return fmt.Errorf("%s is not enabled: %v", t, err)
	}
	kcols := KeyCols(kr[0][0]) // a composite key: its columns (compkey.go)
	// Under the CHECK on DEFAULT: the rows referenced by the organization's rows are locked
	// FOR KEY SHARE, so a cascade from them waits and then finds the rows in the partition.
	fks, err := rowsOf(ctx, cloud, `SELECT format('SELECT 1 FROM %s x WHERE (%s) IN (SELECT %s FROM %s d WHERE %s = %s) FOR KEY SHARE OF x',
			c.confrelid::regclass,
			(SELECT string_agg('x.' || quote_ident(a.attname), ', ' ORDER BY k.ord) FROM unnest(c.confkey) WITH ORDINALITY k(n, ord)
			   JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n),
			(SELECT string_agg('d.' || quote_ident(a.attname), ', ' ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY k(n, ord)
			   JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n),
			$2::text, $3::text, $4::text)
		FROM pg_constraint c WHERE c.conrelid = $1::regclass AND c.contype = 'f'`, qr(def), qr(def), key, val)
	if err != nil {
		return err
	}
	var deadlockMs int64
	if err := cloud.QueryRow(ctx, "SELECT setting::bigint FROM pg_settings WHERE name = 'deadlock_timeout'").Scan(&deadlockMs); err != nil {
		return err
	}
	try := switchLockTry(o.LockTimeout, time.Duration(deadlockMs)*time.Millisecond)
	locked := func(tx pgx.Tx, stmts ...string) error {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", try.Milliseconds())); err != nil {
			return err
		}
		for _, st := range stmts {
			if _, err := tx.Exec(ctx, st); err != nil {
				return fmt.Errorf("%w\n  in: %s", err, st)
			}
		}
		_, err := tx.Exec(ctx, "SET LOCAL lock_timeout = 0")
		return err
	}
	last := time.Now()
	busySince := time.Now()
	for tries := 1; ; tries++ {
		err = pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
			left := time.Until(deadline)
			if left <= 0 {
				return errSplitPause
			}
			for _, s := range []string{
				fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", max(left.Milliseconds(), 1)),
				"SET LOCAL session_replication_role = replica",   // neither the application's triggers nor FK checks fire for the move
				"SELECT set_config('lokadb.moving', 'on', true)", // the refs keep pointing at the moved rows
			} {
				if _, err := tx.Exec(ctx, s); err != nil {
					return err
				}
			}
			for _, f := range fks {
				if _, err := tx.Exec(ctx, f[0]); err != nil {
					return err
				}
			}
			// The application's triggers enabled ALWAYS or REPLICA fire under session_replication_role
			// replica too: an AFTER DELETE one (an audit, a counter, a cascade by hand) saw the move
			// as a delete of the organization's rows (review 3, R3-3). They are switched off for the
			// DELETE, as partition_org does; that takes the table, so the parent is locked first
			// (EXCLUSIVE, as for the ATTACH below: writers wait, reads go on, for the DELETE too).
			always, err := rowsOf(ctx, tx, `SELECT tgname::text, tgenabled::text FROM pg_trigger
				WHERE tgrelid = $1::regclass AND NOT tgisinternal AND tgenabled IN ('A', 'R') ORDER BY tgname`, qr(def))
			if err != nil {
				return err
			}
			if len(always) > 0 {
				if tries == 1 {
					log.Warn("the table has triggers enabled ALWAYS or REPLICA: the split holds it (writers wait) while the organization's rows leave DEFAULT, with those triggers off",
						"table", t, "triggers", len(always))
				}
				if err := locked(tx, fmt.Sprintf("LOCK TABLE %s IN EXCLUSIVE MODE", qr(t))); err != nil {
					return err
				}
				for _, g := range always {
					if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qr(def), q(g[0]))); err != nil {
						return err
					}
				}
			}
			// One statement: the rows leave DEFAULT, and the partition keeps the copies of exactly
			// those. A copy whose row is gone (deleted since the snapshot: an FK cascade, a writer
			// past the proxy, even one that commits while this runs) goes too; separate statements
			// let such a delete slip in between and the row came back in the partition.
			var gone, stale int64
			var ret, goneK []string
			for i, c := range kcols {
				ret, goneK = append(ret, fmt.Sprintf("%s AS k_%d", q(c), i+1)), append(goneK, fmt.Sprintf("k_%d", i+1))
			}
			if err := tx.QueryRow(ctx, fmt.Sprintf(`WITH gone AS (DELETE FROM %s WHERE %s = %s RETURNING %s),
			stale AS (DELETE FROM %s p WHERE NOT EXISTS (SELECT 1 FROM gone WHERE %s) RETURNING 1)
			SELECT (SELECT count(*) FROM gone), (SELECT count(*) FROM stale)`, qr(def), key, val, strings.Join(ret, ", "), qr(part),
				keyEq("gone.", goneK, "p.", kcols))).Scan(&gone, &stale); err != nil {
				return err
			}
			var inPart int64
			if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qr(part))).Scan(&inPart); err != nil {
				return err
			}
			// Nothing adds or changes the organization's rows in DEFAULT during the split (the
			// pause, the CHECK): every row that left it has its copy.
			if inPart != gone {
				return fmt.Errorf("%d rows left DEFAULT, the copy holds %d of them: they changed during the split; nothing moved", gone, inPart)
			}
			if stale > 0 {
				log.Info("rows deleted during the split are not moved", "table", t, "rows", stale)
			}
			for _, g := range always { // as they were: ALWAYS or REPLICA
				mode := map[string]string{"A": "ALWAYS", "R": "REPLICA"}[g[1]]
				if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE %s TRIGGER %s", qr(def), mode, q(g[0]))); err != nil {
					return err
				}
			}
			if err := locked(tx, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(def), q(fmt.Sprintf("lokadb_not_org%d", o.Org)))); err != nil {
				return err
			}
			// An INSERT routed before the ATTACH (it takes only SHARE UPDATE EXCLUSIVE on the
			// parent) would go to DEFAULT: writers wait instead. EXCLUSIVE lets reads through.
			return locked(tx,
				fmt.Sprintf("LOCK TABLE %s IN EXCLUSIVE MODE", qr(t)),
				fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (%s)", qr(t), qr(part), val))
		})
		var pgErr *pgconn.PgError
		busy := errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01")
		if !busy || time.Since(busySince) > o.LockTimeout*time.Duration(max(o.Attempts, 1)) || time.Until(deadline) < try {
			break
		}
		if tries == 1 || tries%10 == 0 {
			log.Warn("table busy, the last transaction of the split again", "table", t, "try", tries, "err", pgErr.Message)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, errSplitPause) || errors.As(err, &pgErr) && pgErr.Code == "57014" && !time.Now().Before(deadline) {
			err = fmt.Errorf("%w (%s): the rows stay in DEFAULT", errSplitPause, limit)
		}
		truncateCopy(cloud, o, part, log)
		return err
	}
	log.Info("organization split out of DEFAULT (the others went on)", "table", t, "took", time.Since(start).Round(time.Millisecond),
		"last_transaction", time.Since(last).Round(time.Millisecond))
	return nil
}

// splitCopy copies the organization's rows of DEFAULT into its partition from one snapshot,
// BatchRows per transaction, reporting progress.
func splitCopy(ctx context.Context, cloud *pgx.Conn, o MoveOptions, t, def, part, key string, cols []string, deadline time.Time, log *slog.Logger) error {
	cctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	reader, err := connect(cctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer reader.Close(context.Background())
	snap, err := reader.BeginTx(cctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer snap.Rollback(context.Background()) //nolint:errcheck
	var total int64
	if err := snap.QueryRow(cctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = %s", qr(def), key, o.org().Lit())).Scan(&total); err != nil {
		return err
	}
	// The copy writes on a connection of its own: cancelled at the pause's limit, a connection
	// is closed, and cloud still has to empty the partition, drop the CHECK and resume.
	writer, err := connect(cctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer writer.Close(context.Background())
	list := strings.Join(cols, ", ")
	p := newProgress(log, "split", t, total, 0)
	_, err = copyChunked(cctx, reader.PgConn(), fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s = %s) TO STDOUT", list, qr(def), key, o.org().Lit()),
		writer, fmt.Sprintf("COPY %s (%s) FROM STDIN", qr(part), list), o.batchRows(),
		[]string{"SET LOCAL session_replication_role = replica"}, p.add) // no FK checks: the rows are valid where they come from
	if err != nil {
		return err
	}
	p.done()
	return nil
}

// truncateCopy empties the organization's partition after a split that did not attach it.
func truncateCopy(cloud *pgx.Conn, o MoveOptions, part string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := lockedDDL(ctx, cloud, o, []string{"TRUNCATE " + qr(part)}); err != nil {
		log.Warn("the copy of the split stays in its partition (not attached: the application does not see it); the next move-org drops it", "table", part, "err", err)
	}
}

// copyColsOf: the columns of a table a copy writes (generated ones are computed again).
func copyColsOf(ctx context.Context, conn querier, table string) ([]string, error) {
	r, err := rowsOf(ctx, conn, `SELECT quote_ident(attname) FROM pg_attribute
		WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = '' ORDER BY attnum`, qr(table))
	if err != nil {
		return nil, err
	}
	var cols []string
	for _, c := range r {
		cols = append(cols, c[0])
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("%s has no columns to copy", table)
	}
	return cols, nil
}
