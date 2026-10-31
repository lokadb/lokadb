package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
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

// splitOrg gives the organization a partition of its own in every spec table that has none
// yet (move-org's first step: enable leaves every organization in DEFAULT). Online when it
// can: the organization pauses (org_lock), the others go on, and DEFAULT is locked only for
// the attach (splitTable: copied in batches, moved in one short transaction). Without the right
// to set session_replication_role (a managed Postgres: RDS, Cloud SQL, Yandex) it does the same
// with the triggers and the FK checks left on where they cannot do harm and the application's
// triggers switched off for the DELETE (splitTable); lokadb.partition_org, which locks DEFAULT
// for the whole split, is no longer used by move-org.
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
	return splitOnline(ctx, cloud, o, todo, log)
}

func splitOnline(ctx context.Context, cloud *pgx.Conn, o MoveOptions, todo []SpecTable, log *slog.Logger) error {
	repl := true // false: this role may not set session_replication_role
	if err := pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica")
		return err
	}); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			return err
		}
		repl = false
		log.Info("this role cannot set session_replication_role: the organization is split out of DEFAULT online all the same (the FKs of its partition are added after the copy)")
	}
	o.noReplica = !repl
	chk := fmt.Sprintf("lokadb_not_org%d", o.Org)
	var prep []string
	guard := map[string]string{}                                    // table -> its CHECK on DEFAULT, added right before its split
	later, validate := map[string][]string{}, map[string][]string{} // noReplica: the FKs of the partition, after the copy
	for _, st := range todo {
		t := st.Name
		tc, err := rowsOf(ctx, cloud, `SELECT tenant_column FROM lokadb.tables WHERE table_name = $1`, t)
		if err != nil {
			return fmt.Errorf("reading lokadb.tables for %s: %w", t, err)
		}
		if len(tc) == 0 {
			return fmt.Errorf("%s is not enabled (run enable first)", t)
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
			// no worker on the copy while it is filled and until the attach (a worker that holds it at
			// the pause makes the last transaction wait for it, minutes on a big one: R39); the last
			// transaction gives the setting back
			fmt.Sprintf("ALTER TABLE %s SET (autovacuum_enabled = false)", qr(part)),
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
			if o.noReplica {
				// rows copied in batches meet their FKs in no order (a comment under a comment of a
				// later batch): the FKs go on after the copy, NOT VALID and then validated
				later[t] = append(later[t], fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", qr(part), q(f[0]), notValid(f[1])))
				validate[t] = append(validate[t], fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(part), q(f[0])))
				continue
			}
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
	tables, err := splitMoveTables(ctx, cloud, todo, o)
	if err != nil {
		return err
	}
	// What the organization's writes will wait for, before anything is made or copied: told, and the move
	// not begun when it would not fit (the copy alone takes minutes for a big organization).
	// A forecast, not a condition: it has been wrong by a factor of two and by a factor of eight either
	// way (a cold cache, the checkpoints of the moment), and a pause past -max-split-pause costs
	// nothing now (the copies are dropped, the tables already split stay, the next move-org goes on).
	// It is told so that the owner can choose the time.
	forecast := estimateSplitPause(ctx, cloud, o, tables, log)
	if err := lockedDDL(ctx, cloud, o, prep); err != nil {
		return err
	}

	// The organization writes on while its rows are copied: triggers on DEFAULT log the changes
	// of its rows (lokadb.move_log, as the move's capture does), the copy reads one snapshot taken
	// after them, and the log is applied to the copy. The organization pauses only for the end:
	// the rest of the log and the last transaction of every table. Before, it paused for the
	// whole copy: 66 s for 3 million rows (block 14, 27.10).
	so := o
	so.replica = &replicaRight{} // the log is applied in the cloud, not in the organization's database
	hbCtx, stopBeat := context.WithCancel(ctx)
	defer stopBeat()
	defer dropSplitCapture(o, tables, log)
	if err := installSplitCapture(ctx, cloud, o, tables); err != nil {
		return err
	}
	go beat(hbCtx, o.CloudDSN, o.Org, log)
	// attached: the tables whose last transaction has committed. Their partition holds the only
	// copy of the organization's rows (the rows left DEFAULT in that transaction): whatever
	// fails after it, it is not touched, and the error says which tables are split.
	attached := map[string]bool{}
	dropAll := func() {
		for _, t := range tables {
			if !attached[t.name] {
				dropCopy(cloud, o, t.local(), log)
			}
		}
	}
	splitState := func(err error) error {
		var done []string
		for _, t := range tables {
			if attached[t.name] {
				done = append(done, t.name)
			}
		}
		if len(done) == 0 || err == nil {
			return err
		}
		return fmt.Errorf("%w\n  already split out of DEFAULT, and left so (their rows are in their partitions <table>__o%d): %s; the next move-org goes on with the other tables",
			err, o.Org, strings.Join(done, ", "))
	}
	copyDeadline := time.Now().Add(o.copyTimeout())
	for _, t := range tables {
		def := hn(t.name, "__cloud")
		cols, err := copyColsOf(ctx, cloud, def)
		if err == nil {
			err = splitCopy(ctx, cloud, o, t.name, def, t.local(), o.org().Key(t.tenant), cols, copyDeadline, log)
		}
		if err != nil {
			dropAll()
			return fmt.Errorf("split %s out of DEFAULT: copying the rows: %w; they stay in DEFAULT", t.name, err)
		}
	}
	for _, t := range tables {
		// NOT VALID takes the referenced table for a moment; the validation reads the copy and
		// holds nobody back (the organization still writes, the others too)
		if err := lockedDDL(ctx, cloud, o, later[t.name]); err != nil {
			dropAll()
			return fmt.Errorf("split %s out of DEFAULT: the FKs of its partition: %w; the rows stay in DEFAULT", t.name, err)
		}
		if err := execAll(ctx, cloud, validate[t.name]); err != nil {
			dropAll()
			return fmt.Errorf("split %s out of DEFAULT: the FKs of its partition: %w; the rows stay in DEFAULT", t.name, err)
		}
	}
	// the copy has no statistics (no autovacuum on it): the comparison and the locks of the last
	// transaction plan their joins from them
	for _, t := range tables {
		if _, err := cloud.Exec(ctx, "ANALYZE "+qr(t.local())); err != nil {
			log.Warn("ANALYZE of the copy of the split failed (the last transaction plans without statistics)", "table", t.name, "err", err)
		}
	}
	// what changed meanwhile, the organization still writing (rounds until the log is short)
	cur := &logCursor{}
	for {
		if err := captureOK(ctx, cloud, o.Org); err != nil {
			dropAll()
			return err
		}
		n, err := applyLog(ctx, nil, cloud, cloud, so, tables, applyRoundEntries, cur)
		if err != nil {
			dropAll()
			return fmt.Errorf("split out of DEFAULT: applying the changes made during the copy: %w", err)
		}
		if !cur.more {
			log.Info("split: changes made during the copy applied", "last_round", n)
			break
		}
		if time.Now().After(copyDeadline) {
			dropAll()
			return fmt.Errorf("split out of DEFAULT: the changes made during the copy not applied in %s (-copy-timeout)", o.copyTimeout())
		}
	}

	// How long the pause will be, measured now on this server under its present load: a sample of the
	// organization's rows deleted in a transaction rolled back, as the last transaction deletes them.
	// Not begun when it would not fit, instead of stopping after -max-split-pause with the
	// organization paused all along. A table with triggers of the application is not sampled (they
	// would run): the copy's time, three times, stands for it.
	unpause, err := pauseOrg(ctx, cloud, o, log)
	if err != nil {
		dropAll()
		return err
	}
	pausedAt := time.Now()
	defer unpause()
	fail := func(err error) error {
		// The rows stayed in DEFAULT, under a CHECK that refuses their updates: removed
		// before the organization resumes (MoveOrg's cleanup tries again on a new connection).
		dropGuards(cloud, o, log)
		dropAll()
		return splitState(err)
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
				return fail(fmt.Errorf("%w (%s): the log of the copy not applied; the rows of %s and of the tables after it stay in DEFAULT", errSplitPause, o.maxSplitPause(), t.name))
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
		attached[t.name] = true
	}
	log.Info("the organization's pause for the split", "took", time.Since(pausedAt).Round(time.Second), "forecast", forecast.Round(time.Second))
	return nil
}

// splitMoveTables: the tables of todo as applyLog takes them, applied to the organization's
// partition in the cloud (t.local()), made by LIKE DEFAULT: no unique index on the key alone
// (DEFAULT's include the organization column), so an entry replaces the row by deleting it.
func splitMoveTables(ctx context.Context, cloud *pgx.Conn, todo []SpecTable, o MoveOptions) ([]moveTable, error) {
	var out []moveTable
	for _, st := range todo {
		tc, err := rowsOf(ctx, cloud, `SELECT tenant_column, coalesce(to_jsonb(b) ->> 'key_column', 'id') FROM lokadb.tables b WHERE table_name = $1`, st.Name)
		if err != nil {
			return nil, fmt.Errorf("reading lokadb.tables for %s: %w", st.Name, err)
		}
		if len(tc) == 0 {
			return nil, fmt.Errorf("%s is not enabled (run enable first)", st.Name)
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
			// not for the split's own DELETE from DEFAULT (splitTable sets lokadb.moving): without the
			// right to set session_replication_role the capture would log every row that leaves
			{"d", "DELETE", fmt.Sprintf("OLD.%s = %s AND current_setting('lokadb.moving', true) IS DISTINCT FROM 'on'", col, val), captureFunc(o.Org, def)},
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

// dropSplitCapture removes what is left of the split's change capture and its log: the triggers
// of a split that failed (a successful one dropped them in its last transaction, splitTable), on a
// connection and a context of their own. The split may be failing because its context was
// cancelled; and a wait for DEFAULT here must never end by cancelling the move's own connection
// (the deadline of a statement closes a pgx connection: the next query of the move said "conn
// closed", reported as "not enabled", MV17). Only the triggers that exist are touched: DROP
// TRIGGER IF EXISTS takes the table's ACCESS EXCLUSIVE lock whether the trigger is there or not.
func dropSplitCapture(o MoveOptions, tables []moveTable, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := connect(ctx, o.CloudDSN)
	if err != nil {
		log.Warn("the split's change capture on DEFAULT may be left: no connection to remove it (it stops logging after the heartbeat's time); move-cleanup removes it", "err", err)
		return
	}
	defer conn.Close(context.Background())
	left, err := rowsOf(ctx, conn, `SELECT quote_ident(t.tgname) || ' ON ' || t.tgrelid::regclass::text FROM pg_trigger t
		WHERE NOT t.tgisinternal AND t.tgname LIKE $1 ORDER BY 1`, strings.ReplaceAll(captureTrigger(o.Org), "_", `\_`)+`\_split\_%`)
	if err == nil {
		var stmts []string
		for _, l := range left {
			stmts = append(stmts, "DROP TRIGGER IF EXISTS "+l[0])
		}
		err = lockedDDL(ctx, conn, o, stmts)
	}
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

// splitCaptureDrops: the statements that remove the split's capture from table t's DEFAULT, for
// its last transaction (the organization is paused and the table held there anyway: no extra wait
// for DEFAULT later, when other organizations write to it).
func splitCaptureDrops(o MoveOptions, t string) []string {
	def := hn(t, "__cloud")
	var out []string
	for _, k := range []string{"i", "u", "x", "d"} {
		out = append(out, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(splitTrigger(o.Org, k)), qr(def)))
	}
	return append(out, "DROP FUNCTION IF EXISTS "+captureFunc(o.Org, def)+"()", "DROP FUNCTION IF EXISTS "+captureFunc(o.Org, def+" out")+"()")
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

// errSlowSplit: the last transaction found a copy with no row in DEFAULT: run again by the statement
// that accounts for each row.
var errSlowSplit = errors.New("a copy of a row deleted since the snapshot")

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
	if err != nil {
		return fmt.Errorf("reading lokadb.tables for %s: %w", t, err)
	}
	if len(kr) == 0 {
		return fmt.Errorf("%s is not enabled", t)
	}
	kcols := KeyCols(kr[0][0]) // a composite key: its columns (compkey.go)
	// Under the CHECK on DEFAULT: the rows referenced by the organization's rows are locked
	// FOR KEY SHARE, so a cascade from them waits and then finds the rows in the partition. They are
	// read from the copy, which holds exactly the organization's rows (compared below) in a few
	// pages read just now, not from DEFAULT's index and heap, scattered over a table of millions
	// (R39: 1 min 54 s for the 1 million rows of one organization).
	fks, err := refLockQueries(ctx, cloud, def, qr(part))
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
	slow := false      // the quick comparison of the copy with DEFAULT found a copy of a deleted row
	var steps []string // where the last transaction's time goes, logged when it is done
	stepAt := time.Now()
	step := func(name string) {
		steps = append(steps, fmt.Sprintf("%s %s", name, time.Since(stepAt).Round(10*time.Millisecond)))
		stepAt = time.Now()
	}
	for tries := 1; ; tries++ {
		steps, stepAt = nil, time.Now()
		err = pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
			left := time.Until(deadline)
			if left <= 0 {
				return errSplitPause
			}
			stmts := []string{fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", max(left.Milliseconds(), 1))}
			if !o.noReplica { // neither the application's triggers nor FK checks fire for the move
				stmts = append(stmts, "SET LOCAL session_replication_role = replica")
			}
			// no parallel workers: the comparison below would put its hash joins in shared memory, which
			// a Postgres in a container (/dev/shm of 64 MB) does not have for 600 000 rows
			stmts = append(stmts, "SET LOCAL max_parallel_workers_per_gather = 0")
			// the refs keep pointing at the moved rows (lokadb's own triggers on DEFAULT stand down)
			for _, s := range append(stmts, "SELECT set_config('lokadb.moving', 'on', true)") {
				if _, err := tx.Exec(ctx, s); err != nil {
					return err
				}
			}
			// Autovacuum skips a table somebody holds against vacuum (it only tries for the lock):
			// held first, DEFAULT and the partition are not picked up by a worker while the rows
			// leave (the DELETE leaves 480 000 dead rows: a worker would start at once and
			// make VALIDATE and ATTACH wait for it, try after try, and each try is the whole
			// transaction again). A worker that runs already is asked to stop between tries.
			// The parent first: an application's CREATE INDEX holds SHARE on it and then asks for the
			// partitions, so DEFAULT held before the parent made a deadlock with it (MV37). SHARE
			// UPDATE EXCLUSIVE on the parent lets writers through.
			if err := locked(tx, fmt.Sprintf("LOCK TABLE ONLY %s IN SHARE UPDATE EXCLUSIVE MODE", qr(t)),
				fmt.Sprintf("LOCK TABLE ONLY %s IN SHARE UPDATE EXCLUSIVE MODE", qr(def)),
				fmt.Sprintf("LOCK TABLE ONLY %s IN SHARE UPDATE EXCLUSIVE MODE", qr(part))); err != nil {
				return err
			}
			step("locks")
			for _, f := range fks {
				if _, err := tx.Exec(ctx, f[0]); err != nil {
					return err
				}
			}
			step("referenced rows locked")
			// The application's triggers enabled ALWAYS or REPLICA fire under session_replication_role
			// replica too: an AFTER DELETE one (an audit, a counter, a cascade by hand) saw the move
			// as a delete of the organization's rows (review 3, R3-3). They are switched off for the
			// DELETE, as partition_org does; that takes the table, so the parent is locked first
			// (EXCLUSIVE, as for the ATTACH below: writers wait, reads go on, for the DELETE too).
			// Without that role they all fire, the application's origin ones too: the same.
			// LokaDB's own (refs, key map) return at once while lokadb.moving is on, and its
			// capture of the split does not fire for a DELETE with it on (splitTrigger).
			always, err := rowsOf(ctx, tx, `SELECT g.tgname::text, g.tgenabled::text FROM pg_trigger g
				WHERE g.tgrelid = $1::regclass AND NOT g.tgisinternal AND (g.tgenabled IN ('A', 'R') OR ($2 AND g.tgenabled = 'O'))
				  AND g.tgfoid NOT IN (SELECT oid FROM pg_proc WHERE pronamespace = 'lokadb'::regnamespace) ORDER BY g.tgname`, qr(def), o.noReplica)
			if err != nil {
				return err
			}
			if len(always) > 0 {
				if tries == 1 {
					log.Warn("the table has triggers of the application that would see the move as a delete of the organization's rows (enabled ALWAYS or REPLICA, or any when this role may not set session_replication_role): the split holds it (writers wait) while the organization's rows leave DEFAULT, with those triggers off",
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
			step("triggers off")
			var gone, stale int64
			var ret, goneK []string
			for i, c := range kcols {
				ret, goneK = append(ret, fmt.Sprintf("%s AS k_%d", q(c), i+1)), append(goneK, fmt.Sprintf("k_%d", i+1))
			}
			if slow {
				// the copies of rows deleted since the snapshot go too (only after the quick way found
				// some): one statement, the rows deleted are the rows kept
				if err := tx.QueryRow(ctx, fmt.Sprintf(`WITH gone AS (DELETE FROM %s WHERE %s = %s RETURNING %s),
				stale AS (DELETE FROM %s p WHERE NOT EXISTS (SELECT 1 FROM gone WHERE %s) RETURNING 1)
				SELECT (SELECT count(*) FROM gone), (SELECT count(*) FROM stale)`, qr(def), key, val, strings.Join(ret, ", "), qr(part),
					keyEq("gone.", goneK, "p.", kcols))).Scan(&gone, &stale); err != nil {
					return err
				}
			} else {
				// The quick way, which the statement with RETURNING and the anti-join of the copies is
				// not (27 s against 6 for 600 000 rows): the copy holds exactly the rows of DEFAULT
				// (two anti-joins, a second for 600 000), then a plain DELETE.
				var missing, orphans int64
				if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT
					(SELECT count(*) FROM %[1]s d WHERE %[3]s = %[4]s AND NOT EXISTS (SELECT 1 FROM %[2]s p WHERE %[5]s)),
					(SELECT count(*) FROM %[2]s p WHERE NOT EXISTS (SELECT 1 FROM %[1]s d WHERE %[3]s = %[4]s AND %[5]s))`,
					qr(def), qr(part), key, val, keyEq("p.", kcols, "d.", kcols))).Scan(&missing, &orphans); err != nil {
					return err
				}
				step("compared")
				if missing > 0 {
					return fmt.Errorf("%d rows of DEFAULT have no copy: they changed during the split; nothing moved", missing)
				}
				if orphans > 0 { // a copy of a row deleted since the snapshot
					slow = true
					return errSlowSplit
				}
				tag, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(def), key, val))
				if err != nil {
					return err
				}
				gone = tag.RowsAffected()
			}
			step("delete")
			var inPart int64
			if err := tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qr(part))).Scan(&inPart); err != nil {
				return err
			}
			step("count")
			// Nothing adds or changes the organization's rows in DEFAULT during the split (the
			// pause, the CHECK): every row that left it has its copy.
			if inPart != gone {
				if !slow {
					slow = true // the rows changed under the quick way: the slow one accounts for each
					return errSlowSplit
				}
				return fmt.Errorf("%d rows left DEFAULT, the copy holds %d of them: they changed during the split; nothing moved", gone, inPart)
			}
			if stale > 0 {
				log.Info("rows deleted during the split are not moved", "table", t, "rows", stale)
			}
			for _, g := range always { // as they were: ALWAYS or REPLICA
				mode := map[string]string{"A": "ALWAYS", "R": "REPLICA", "O": ""}[g[1]]
				if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE %s TRIGGER %s", qr(def), mode, q(g[0]))); err != nil {
					return err
				}
			}
			if err := locked(tx, fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(def), q(fmt.Sprintf("lokadb_not_org%d", o.Org)))); err != nil {
				return err
			}
			step("validate")
			// An INSERT routed before the ATTACH (it takes only SHARE UPDATE EXCLUSIVE on the
			// parent) would go to DEFAULT: writers wait instead. EXCLUSIVE lets reads through.
			err = locked(tx, append([]string{
				fmt.Sprintf("LOCK TABLE %s IN EXCLUSIVE MODE", qr(t)),
				fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (%s)", qr(t), qr(part), val)}, splitCaptureDrops(o, t)...)...)
			step("attach")
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s RESET (autovacuum_enabled)", qr(part)))
			return err
		})
		if errors.Is(err, errSlowSplit) {
			continue
		}
		var pgErr *pgconn.PgError
		busy := errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01")
		if !busy || time.Since(busySince) > o.LockTimeout*time.Duration(max(o.Attempts, 1)) || time.Until(deadline) < try {
			break
		}
		if tries == 1 || tries%10 == 0 {
			log.Warn("table busy, the last transaction of the split again", "table", t, "try", tries, "err", pgErr.Message)
		}
		// the autovacuum worker the DELETE of an earlier try, or the copy, woke up holds DEFAULT
		// or the partition and does not give way to a wait under deadlock_timeout
		if _, stuck := yieldAutovacuum(ctx, cloud, []string{qr(def), qr(part), qr(t)}, log); stuck {
			try = time.Duration(patientTry(deadlockMs)) * time.Millisecond
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, errSplitPause) || errors.As(err, &pgErr) && pgErr.Code == "57014" && !time.Now().Before(deadline) {
			err = fmt.Errorf("%w (%s): the rows of %s stay in DEFAULT", errSplitPause, limit, t)
		}
		dropCopy(cloud, o, part, log)
		return err
	}
	log.Info("organization split out of DEFAULT (the others went on)", "table", t, "took", time.Since(start).Round(time.Millisecond),
		"last_transaction", time.Since(last).Round(time.Millisecond), "steps", strings.Join(steps, ", "))
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
	var setup []string
	if !o.noReplica { // no FK checks: the rows are valid where they come from
		setup = []string{"SET LOCAL session_replication_role = replica"}
	}
	p := newProgress(log, "split", t, total, 0)
	_, err = copyChunked(cctx, reader.PgConn(), fmt.Sprintf("COPY (SELECT %s FROM %s WHERE %s = %s) TO STDOUT", list, qr(def), key, o.org().Lit()),
		writer, fmt.Sprintf("COPY %s (%s) FROM STDIN", qr(part), list), o.batchRows(),
		setup, p.add)
	if err != nil {
		return err
	}
	p.done()
	return nil
}

// dropCopy removes the copy this attempt made of the organization's rows (the partition, with its
// own indexes, constraints and statistics) after a split that did not attach it. Only a table that
// carries this attempt's mark (splitMark) and is not attached: an attached partition holds the
// organization's only rows and is never touched, and one that cannot be told from one is not either
// (the last transaction may have committed though its answer did not arrive).
func dropCopy(cloud *pgx.Conn, o MoveOptions, part string, log *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := connect(ctx, o.CloudDSN)
	if err != nil {
		log.Warn("the copy of the split stays (whether it is attached is not known); the next move-org looks at it", "table", part, "err", err)
		return
	}
	defer conn.Close(context.Background())
	var exists, att, mine bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL,
		EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass($1)),
		coalesce(obj_description(to_regclass($1), 'pg_class'), '') = $2`, qr(part), splitMark(o.Org)).Scan(&exists, &att, &mine); err != nil {
		log.Warn("the copy of the split stays (whether it is attached is not known); the next move-org looks at it", "table", part, "err", err)
		return
	}
	switch {
	case !exists:
		return
	case att:
		log.Warn("the partition of the split is attached: its rows are the organization's only copy, not touched", "table", part)
		return
	case !mine:
		log.Warn("a table of this name is not the copy this split made: not touched", "table", part)
		return
	}
	if err := lockedDDL(ctx, conn, o, []string{"DROP TABLE " + qr(part)}); err != nil {
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

// notValid: the definition of a foreign key as ADD CONSTRAINT takes it without the scan of the
// rows (VALIDATE CONSTRAINT follows).
func notValid(def string) string {
	if strings.HasSuffix(def, " NOT VALID") {
		return def
	}
	return def + " NOT VALID"
}

// splitSamplePages: the pages of DEFAULT the estimate's sample is taken from.
const splitSamplePages = 1500

// estimateSplitPause: how long the organization's writes wait for the split's last transactions.
// The DELETE of a table costs by the pages of DEFAULT it touches, not by its rows: the
// organization's rows lie on most of them (every fifth row of the table is its own), each page
// is written whole into the log the first time it changes after a checkpoint, and a delete
// larger than shared_buffers and max_wal_size sets checkpoints off and makes the backends
// write what they evict (R39: 1302 MB table, 1.4 GB of log for one commit, 3 min 39 s against
// the 1 min 36 s a sample of 10 000 rows promised). So: a DELETE of the rows on a sample of
// pages (TABLESAMPLE SYSTEM, in a transaction rolled back, with lokadb.moving and the
// triggers as the last transaction has them) gives the time per row; times the rows; times
// 1 plus the log the pages would write against max_wal_size (at most 2 more, half of it counted)
// and the pages against shared_buffers (a tenth of that, at most 1, half of it counted), the
// two numbers measured on the run of the risk hunt (0.8 each). The factor is for the estimate before
// the copy, the server quiet; after it (afterCopy) the sample alone: it is taken under the
// checkpoints of the copy, as the pause will be. A table with triggers of the application is not
// sampled (they would run): 30 µs a row and the same factors.
func estimateSplitPause(ctx context.Context, cloud *pgx.Conn, o MoveOptions, tables []moveTable, log *slog.Logger) time.Duration {
	var maxWAL, buffers int64
	if err := cloud.QueryRow(ctx, "SELECT pg_size_bytes(current_setting('max_wal_size')), pg_size_bytes(current_setting('shared_buffers'))").Scan(&maxWAL, &buffers); err != nil {
		maxWAL, buffers = 1<<30, 128<<20
	}
	var total time.Duration
	var allRows int64
	for _, t := range tables {
		def := hn(t.name, "__cloud")
		key, val := o.org().Key(t.tenant), o.org().Lit()
		var rows int64
		if err := cloud.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = %s", qr(def), key, val)).Scan(&rows); err != nil || rows == 0 {
			continue
		}
		allRows += rows
		// the pages the rows lie on, if they lie as they come: pages x (1 - (1 - share)^rows per page)
		var relpages int64
		var reltuples float64
		_ = cloud.QueryRow(ctx, "SELECT relpages::bigint, reltuples::float8 FROM pg_class WHERE oid = $1::regclass", qr(def)).Scan(&relpages, &reltuples)
		pages := relpages
		if relpages > 0 && reltuples > 0 {
			share, perPage := min(float64(rows)/reltuples, 1), reltuples/float64(relpages)
			pages = max(1, int64(float64(relpages)*(1-math.Pow(1-share, perPage))))
		}
		perRow := 30 * time.Microsecond
		if took, ok := sampleDelete(ctx, cloud, o, def, key, val, relpages); ok {
			perRow = took
		}
		wal := float64(pages)*8192 + float64(rows)*80
		factor := 1 + 0.8*min(wal/float64(maxWAL), 2) + 0.8*min(float64(pages)*8192/float64(buffers)/10, 1)
		est := time.Duration(float64(perRow) * float64(rows) * factor * 1.3) // a third more: the locks of the referenced rows and the comparison
		log.Info("the DELETE of the split, estimated", "table", t.name, "rows", rows, "pages", pages, "per_row", perRow.Round(time.Microsecond),
			"log_mb", int64(wal)>>20, "factor", fmt.Sprintf("%.2f", factor), "estimate", est.Round(time.Second))
		total += est
	}
	log.Info("the organization's writes will wait for the split about (a forecast from a sample on a quiet table: on a big one under writes the pause has been up to four times longer than it says, and it is not a limit)", "forecast", total.Round(time.Second), "rows", allRows)
	return total
}

// sampleDelete: the time per row of a DELETE, in a transaction rolled back, of the organization's rows
// on about splitSamplePages pages of table def (all of its rows up to 10 000 when it has fewer
// pages); ok false: it could not be taken (triggers of the application would run, an error).
func sampleDelete(ctx context.Context, cloud *pgx.Conn, o MoveOptions, def, key, val string, relpages int64) (time.Duration, bool) {
	// three samples of other pages, the lowest: one alone has been 3 s and 22 s for the same
	// organization, and 207 µs a row right after enable (a cold cache, the checkpoints of the moment)
	// against 71 a quarter of a minute later (risk hunt). The factor of estimateSplitPause is for the
	// effects that come with the size of the DELETE; the cold ones of the moment are not its to add
	var took []time.Duration
	for seed := 1; seed <= 3; seed++ {
		if d, ok := sampleDeleteOnce(ctx, cloud, o, def, key, val, relpages, seed); ok {
			took = append(took, d)
		}
	}
	if len(took) == 0 {
		return 0, false
	}
	sort.Slice(took, func(i, j int) bool { return took[i] < took[j] })
	return took[0], true
}

func sampleDeleteOnce(ctx context.Context, cloud *pgx.Conn, o MoveOptions, def, key, val string, relpages int64, seed int) (time.Duration, bool) {
	tr, err := cloud.Begin(ctx)
	if err != nil {
		return 0, false
	}
	defer tr.Rollback(context.Background()) //nolint:errcheck
	var n int
	if err := tr.QueryRow(ctx, `SELECT count(*) FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal AND tgenabled <> 'D'
		AND tgfoid NOT IN (SELECT oid FROM pg_proc WHERE pronamespace = 'lokadb'::regnamespace)`, qr(def)).Scan(&n); err != nil || n > 0 {
		return 0, false
	}
	for _, s := range []string{"SET LOCAL statement_timeout = '15s'", "SELECT set_config('lokadb.moving', 'on', true)"} {
		if _, err := tr.Exec(ctx, s); err != nil {
			return 0, false
		}
	}
	if !o.noReplica {
		if _, err := tr.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
			return 0, false
		}
	}
	// Only the DELETE is timed. The locks of the referenced rows and the comparison were in the
	// sample too (one probe by index a row): on a cold cache that is a random read each, 6 ms a row
	// for 400 000 versions, 1 h 29 min for an organization that moves in 4 minutes, where the pause
	// reads the pages in turn and joins them by hash (risk hunt, 9ece9db9). They are a third more
	// of the DELETE's time in the estimate (estimateSplitPause).
	pick := fmt.Sprintf("SELECT ctid FROM %s WHERE %s = %s LIMIT 10000", qr(def), key, val)
	if relpages > 2*splitSamplePages {
		pick = fmt.Sprintf("SELECT ctid FROM %s TABLESAMPLE SYSTEM (%f) REPEATABLE (%d) WHERE %s = %s", qr(def), 100*float64(splitSamplePages)/float64(relpages), seed, key, val)
	}
	start := time.Now()
	tag, err := tr.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE ctid = ANY (ARRAY(%s))", qr(def), pick))
	if err != nil || tag.RowsAffected() == 0 {
		return 0, false
	}
	return time.Since(start) / time.Duration(tag.RowsAffected()), true
}

// refLockQueries: for each FK of table def, the statement that locks the rows it references FOR KEY
// SHARE, those named by the rows of src (a table, or a sub-select aliased in its place: the estimate
// takes a sample of pages), as rows of one column each: [][]string{{sql}}.
func refLockQueries(ctx context.Context, q querier, def, src string) ([][]string, error) {
	return rowsOf(ctx, q, `SELECT format('SELECT 1 FROM %s x WHERE (%s) IN (SELECT DISTINCT %s FROM '|| $2::text ||' d) FOR KEY SHARE OF x',
			c.confrelid::regclass,
			(SELECT string_agg('x.' || quote_ident(a.attname), ', ' ORDER BY k.ord) FROM unnest(c.confkey) WITH ORDINALITY k(n, ord)
			   JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n),
			(SELECT string_agg('d.' || quote_ident(a.attname), ', ' ORDER BY k.ord) FROM unnest(c.conkey) WITH ORDINALITY k(n, ord)
			   JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n))
		FROM pg_constraint c WHERE c.conrelid = $1::regclass AND c.contype = 'f'`, qr(def), src)
}
