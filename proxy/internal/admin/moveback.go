package admin

// The way back (tasks/rollback.md): move-back returns an organization from its own database
// into the cloud. It is the trigger move turned around: triggers on the organization's
// tables in its database log every change (FK cascades and writes that pass by the cloud
// included, they happen there), move-back copies the tables into unattached partitions
// T__o<N> of the cloud and applies the log to them, and the switch swaps the foreign
// partitions for them. Without the rights for the triggers, the copy runs under a full pause
// of the organization instead.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// MoveBackOptions configures MoveBack.
type MoveBackOptions struct {
	CloudDSN string // admin DSN of the cloud database
	// CustomerDSN: the organization's database, as its admin or as the cloud's role there
	// (the owner of the moved tables). Ignored with Abandon.
	CustomerDSN string
	Spec        Spec
	Org         int64
	LockTimeout time.Duration // how long the switch may wait for the write lock
	MaxPause    time.Duration // how long the switch may wait for the last changes
	Attempts    int
	// Bridge: how long the view standing in for a foreign partition stays after the switch
	// (proxies with the old metadata address the partition directly until they reload it).
	Bridge time.Duration
	DryRun bool
	// Abandon: the organization's database is lost; the organization comes back empty. Its
	// rows in the refs tables are deleted (with the FK actions of the cloud tables on them).
	Abandon bool
	Log     *slog.Logger
}

// Ways back (MoveBackPlan.Way).
const (
	WayCapture = "capture" // triggers in the organization's database, short pause
	WayPause   = "pause"   // copy under a pause of the organization: no triggers there
	WayAbandon = "abandon"
)

// MoveBackPlan is what move-back would do (-dry-run) and why.
type MoveBackPlan struct {
	Way    string
	Why    string // why not capture
	Rows   map[string]int64
	Bytes  int64
	Server string
	Schema string
}

// metaChanged tells proxies (block 9: they LISTEN on lokadb_meta) to reload the routing
// metadata at COMMIT instead of after metaTTL. Sent in the transaction that changes it.
const metaChanged = "NOTIFY lokadb_meta"

func (t moveTable) bridge() string { return t.foreign() } // the view's name: the partition's
func (t moveTable) gone() string   { return hn(t.name, fmt.Sprintf("__org%d_gone", t.org)) }

// returnedMark is the comment move-back leaves on the organization's tables in its database:
// not TableMark, so a later move-org into that database stops at them instead of dropping them.
func returnedMark(org int64, at time.Time) string {
	return fmt.Sprintf("%s%s (organization %d)%s", returnedPrefix, at.UTC().Format(time.RFC3339), org, returnedSuffix)
}

// MoveBack returns an organization from its own database into the cloud database.
func MoveBack(ctx context.Context, o MoveBackOptions) (err error) {
	if err := ValidOrg(o.Org); err != nil {
		return fmt.Errorf("move-back: %w", err)
	}
	log := o.Log.With("org", o.Org)
	if o.Attempts < 1 {
		o.Attempts = 1
	}
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer cloud.Close(context.Background())
	var mine bool
	if err := cloud.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", moveLockKey).Scan(&mine); err != nil {
		return err
	}
	if !mine {
		return errors.New("a move-org or move-back is running against this cloud database")
	}
	if err := execAll(ctx, cloud, []string{baseSQL}); err != nil {
		return err
	}
	// profile direct (stage 9b): its user mapping may be gone, and move-back reads the rows
	// through the FDW; the profile goes back first, which makes it again
	if r, err := rowsOf(ctx, cloud, `SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs') AND attname = 'profile')::text`); err == nil && r[0][0] == "true" {
		if r, err = rowsOf(ctx, cloud, `SELECT coalesce(profile, '') FROM lokadb.orgs WHERE org = $1`, o.Org); err == nil && len(r) > 0 && r[0][0] == ProfileDirect {
			return fmt.Errorf("organization %d is on profile direct: lokadb org-profile -org %d fdw -control-plane ... -key ... first (it makes the FDW's user mapping again), then move-back", o.Org, o.Org)
		}
	}
	mo := MoveOptions{CloudDSN: o.CloudDSN, CustomerDSN: o.CustomerDSN, Spec: o.Spec, Org: o.Org,
		LockTimeout: o.LockTimeout, MaxPause: o.MaxPause, Attempts: o.Attempts, Log: o.Log, replica: &replicaRight{}}
	// block 18: an organization numbered by uuid comes back by its value (lokadb.tenants)
	if err := mo.resolve(ctx, cloud); err != nil {
		return err
	}
	tables, _, err := describe(ctx, cloud, o.Spec, mo.org())
	if err != nil {
		return err
	}
	server, schema, err := orgServer(ctx, cloud, o.Org)
	if err != nil {
		// Switched by an earlier run that did not finish: only what follows the switch is left.
		if back, berr := attachedBack(ctx, cloud, tables); berr == nil && back {
			log.Info("the organization is already back in the cloud: finishing what the earlier run left")
			return afterSwitch(ctx, cloud, nil, nil, o, tables, "", "", true, log)
		}
		return err
	}

	if o.Abandon {
		if o.DryRun {
			log.Warn("dry run: -abandon would bring the organization back EMPTY: every row of it in its database is given up")
			return nil
		}
		return abandon(ctx, cloud, mo, tables, server, log)
	}

	cust, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("the organization's database is unreachable: %w\n"+
			"  lokadb holds no copy of its rows. Restore the database (from the customer's backup or a replica), "+
			"point the cloud at it (lokadb repoint -org %d -customer-app DSN), then run move-back again. "+
			"If the rows are lost for good: move-back -abandon -confirm-org %d brings the organization back empty", err, o.Org, o.Org)
	}
	defer cust.Close(context.Background())
	if err := setCustomerSchema(ctx, cust, schema); err != nil {
		return err
	}
	plan, err := planBack(ctx, cust, tables, schema)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	plan.Server = server
	if o.DryRun {
		log.Info("dry run", "way", plan.Way, "why_not_capture", plan.Why, "bytes", plan.Bytes)
		for t, n := range plan.Rows {
			log.Info("dry run: table", "table", t, "rows", n)
		}
		if plan.Way == WayPause {
			log.Warn("dry run: the organization's writes and reads pause while all its rows are copied", "rows_total", sum(plan.Rows))
		} else {
			log.Info("dry run: the organization pauses only for the last changes and the swap; the others only for the swap")
		}
		return nil
	}
	log.Info("move-back", "way", plan.Way, "why_not_capture", plan.Why)

	src := func(t moveTable) string { return t.name }
	switched := false
	defer func() {
		if switched {
			return
		}
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if c, cerr := connect(cctx, o.CloudDSN); cerr == nil {
			if cerr := execAll(cctx, c, dropStaging(tables)); cerr != nil {
				log.Warn("cleanup: unattached partitions left in the cloud database (the next move-back drops them)", "err", cerr)
			}
			c.Close(cctx)
		}
		if plan.Way != WayCapture {
			return
		}
		if c, cerr := connect(cctx, o.CustomerDSN); cerr == nil {
			_ = setCustomerSchema(cctx, c, schema)
			if cerr := execAll(cctx, c, dropCapture(o.Org, tables, src)); cerr != nil {
				log.Warn("cleanup: capture triggers left in the organization's database: they stop logging by themselves", "err", cerr)
			}
			c.Close(cctx)
		}
	}()

	log.Info("step 1/5: partitions in the cloud")
	if err := staging(ctx, cloud, mo, tables, log); err != nil {
		return err
	}
	apply, err := connect(ctx, o.CloudDSN) // writes the partitions while cloud holds the switch's locks
	if err != nil {
		return err
	}
	defer apply.Close(context.Background())
	locker, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	defer locker.Close(context.Background())
	snap, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	defer snap.Close(context.Background())
	for _, c := range []*pgx.Conn{locker, snap} {
		if err := setCustomerSchema(ctx, c, schema); err != nil {
			return err
		}
	}

	dialCloud := func(ctx context.Context) (*pgx.Conn, error) { return connect(ctx, o.CloudDSN) }
	var unpause func()
	var lockTx pgx.Tx
	if plan.Way == WayCapture {
		log.Info("step 2/5: change capture in the organization's database, copy")
		if err := execAll(ctx, cust, []string{"CREATE SCHEMA IF NOT EXISTS lokadb"}); err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		if err := installCapture(ctx, cust, mo, tables, src); err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		hbCtx, stopBeat := context.WithCancel(ctx)
		defer stopBeat()
		go beat(hbCtx, o.CustomerDSN, o.Org, log)
		if err := initialCopy(ctx, cust, dialCloud, tables, src, moveTable.local, mo.batchRows(), log); err != nil {
			return err
		}
		log.Info("step 3/5: catching up (writes continue)")
		if err := catchUp(ctx, cust, apply, mo, tables, log); err != nil {
			return err
		}
		if unpause, err = pauseOrg(ctx, cloud, mo, log); err != nil {
			return err
		}
		defer func() { unpause() }()
		// What the organization wrote through the proxy before the pause is applied now; writers
		// that pass by the proxy (and the organization's database) still write: the rest is
		// drained by the switch, under its locks and -max-pause.
		if err := catchUp(ctx, cust, apply, mo, tables, log); err != nil {
			return err
		}
	} else {
		log.Info("step 2/5: copy under a pause of the organization (no change capture)")
		if unpause, err = pauseOrg(ctx, cloud, mo, log); err != nil {
			return err
		}
		defer func() { unpause() }()
		// Writers that pass by the cloud wait too; reads go on.
		if lockTx, err = lockCustomer(ctx, locker, tables, o.LockTimeout); err != nil {
			return err
		}
		defer lockTx.Rollback(context.Background())
		start := time.Now()
		if err := initialCopy(ctx, cust, dialCloud, tables, src, moveTable.local, mo.batchRows(), log); err != nil {
			return err
		}
		log.Info("copied under the pause", "took", time.Since(start).Round(time.Millisecond))
		log.Info("step 3/5: nothing to catch up (the organization is paused)")
	}

	log.Info("step 4/5: switch")
	var snapTx, cloudSnap pgx.Tx
	for attempt := 1; ; attempt++ {
		var p pause
		p, snapTx, cloudSnap, err = switchBack(ctx, cloud, apply, cust, locker, lockTx, snap, mo, tables, plan.Way, server)
		if err == nil {
			switched = true
			log.Info("switched", "writes_paused", p.total, "wait_last_changes", p.catchUp, "swap_partitions", p.swap, "attempt", attempt)
			break
		}
		if !errors.Is(err, errRetry) || attempt >= o.Attempts {
			return err
		}
		log.Warn("switch attempt failed, retrying", "attempt", attempt, "err", err)
		time.Sleep(time.Second)
	}
	unpause()
	unpause = func() {}
	if lockTx != nil { // the pause way held its lock since before the copy
		_ = lockTx.Rollback(ctx)
	}
	return afterSwitch(ctx, cloud, snapTx, cloudSnap, o, tables, server, schema, plan.Way == WayCapture, log)
}

func sum(m map[string]int64) int64 {
	var n int64
	for _, v := range m {
		n += v
	}
	return n
}

// planBack decides the way back: capture when the role may put triggers on every table and
// keep the log in schema lokadb of the organization's database, a copy under a pause otherwise.
func planBack(ctx context.Context, cust *pgx.Conn, tables []moveTable, schema string) (MoveBackPlan, error) {
	p := MoveBackPlan{Way: WayCapture, Rows: map[string]int64{}, Schema: schema}
	var why []string
	r, err := rowsOf(ctx, cust, `SELECT (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)::text,
		CASE WHEN to_regnamespace('lokadb') IS NULL THEN has_database_privilege(current_database(), 'CREATE')
		     ELSE has_schema_privilege('lokadb', 'CREATE') AND has_schema_privilege('lokadb', 'USAGE') END::text`)
	if err != nil {
		return p, err
	}
	super := r[0][0] == "true"
	if !super && r[0][1] != "true" {
		why = append(why, "the role cannot create schema lokadb (or create in it) for the change log")
	}
	for _, t := range tables {
		rr, err := rowsOf(ctx, cust, `SELECT pg_has_role(current_user, c.relowner, 'USAGE')::text,
			c.reltuples::bigint::text, pg_total_relation_size(c.oid)::text
			FROM pg_class c WHERE c.oid = to_regclass(quote_ident($1) || '.' || quote_ident($2))`, custSchemaOf(t.name, schema), nameOf(t.name))
		if err != nil {
			return p, err
		}
		if len(rr) == 0 {
			return p, fmt.Errorf("no table %s.%s in the organization's database", custSchemaOf(t.name, schema), nameOf(t.name))
		}
		if !super && rr[0][0] != "true" {
			why = append(why, fmt.Sprintf("the role does not own %s (triggers)", t.name))
		}
		n, err := rowsOf(ctx, cust, fmt.Sprintf("SELECT count(*)::text FROM %s.%s", q(custSchemaOf(t.name, schema)), q(nameOf(t.name))))
		if err != nil {
			return p, err
		}
		var rows, size int64
		fmt.Sscan(n[0][0], &rows)
		fmt.Sscan(rr[0][2], &size)
		p.Rows[t.name] = rows
		p.Bytes += size
	}
	if len(why) > 0 {
		p.Way, p.Why = WayPause, strings.Join(why, "; ")
	}
	return p, nil
}

// attachedBack: every table has a local partition of the organization attached (an earlier
// move-back switched).
func attachedBack(ctx context.Context, cloud *pgx.Conn, tables []moveTable) (bool, error) {
	for _, t := range tables {
		r, err := rowsOf(ctx, cloud, `SELECT EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE c.oid = to_regclass($1) AND c.relkind = 'r')::text`, qr(t.local()))
		if err != nil {
			return false, err
		}
		if r[0][0] != "true" {
			return false, nil
		}
	}
	return true, nil
}

func dropStaging(tables []moveTable) []string {
	var out []string
	for i := len(tables) - 1; i >= 0; i-- {
		t := tables[i]
		// only an unattached one: an attached T__o<N> serves the organization
		out = append(out, doTag(fmt.Sprintf(`DO $$ BEGIN
			IF to_regclass(%s) IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass(%s)) THEN
				EXECUTE 'DROP TABLE ' || %s; END IF; END $$`, lit(qr(t.local())), lit(qr(t.local())), lit(qr(t.local())))))
	}
	return out
}

// staging makes the organization's partitions in the cloud, unattached and empty: the
// DEFAULT partition's columns, indexes and CHECKs, a CHECK on the key (ATTACH then scans
// nothing), no FK (one onto a refs table would refuse the application's deletes while the
// partition still holds rows the organization's database deleted; they go on in the switch).
// DEFAULT gets its validated CHECK that it holds no row of the organization, if missing.
func staging(ctx context.Context, cloud *pgx.Conn, o MoveOptions, tables []moveTable, log *slog.Logger) error {
	if err := execAll(ctx, cloud, dropStaging(tables)); err != nil {
		return err
	}
	chk := fmt.Sprintf("lokadb_not_org%d", o.Org)
	var prep, locked []string
	for _, t := range tables {
		def, part := hn(t.name, "__cloud"), t.local()
		owner, err := rowsOf(ctx, cloud, `SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass`, qr(t.name))
		if err != nil {
			return err
		}
		key := o.org().Key(t.tenant)
		prep = append(prep,
			fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING ALL)", qr(part), qr(def)),
			fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT lokadb_key CHECK (%s IS NOT NULL AND %s = %s)", qr(part), key, key, t.val),
			fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(part), q(owner[0][0])))
		copied, err := rowsOf(ctx, cloud, `SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c' AND conname LIKE 'lokadb\_not\_org%'`, qr(def))
		if err != nil {
			return err
		}
		for _, c := range copied {
			prep = append(prep, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", qr(part), q(c[0])))
		}
		valid, err := rowsOf(ctx, cloud, `SELECT convalidated::text FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2`, qr(def), chk)
		if err != nil {
			return err
		}
		if len(valid) == 0 || valid[0][0] != "true" {
			log.Warn("DEFAULT has no validated CHECK against the organization's rows: adding it", "table", def)
			locked = append(locked,
				fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s", qr(def), q(chk)),
				fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s <> %s) NOT VALID", qr(def), q(chk), key, t.val),
				fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(def), q(chk)))
		}
	}
	if err := execAll(ctx, cloud, prep); err != nil {
		return err
	}
	return lockedDDL(ctx, cloud, o, locked)
}

// lockCustomer stops writers of the organization's tables in its database (EXCLUSIVE: reads
// go on) until the returned transaction ends.
func lockCustomer(ctx context.Context, conn *pgx.Conn, tables []moveTable, wait time.Duration) (pgx.Tx, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range tables {
		names = append(names, qr(t.name))
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", wait.Milliseconds())); err == nil {
		_, err = tx.Exec(ctx, "LOCK TABLE "+strings.Join(names, ", ")+" IN EXCLUSIVE MODE")
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01") {
			return nil, fmt.Errorf("%w: customer database: %v", errRetry, err)
		}
		return nil, fmt.Errorf("customer database: %w", err)
	}
	return tx, nil
}

// switchBack takes the cloud's write lock, drains the log (capture) under a lock on the
// organization's tables in its database, takes the snapshots the verification reads (both
// sides as switched: the cloud's on apply, which the drain is done with), and swaps the
// foreign partitions for the local ones. lockTx: the lock the pause way holds since before
// its copy (nil: taken here).
func switchBack(ctx context.Context, cloud, apply, cust, locker *pgx.Conn, lockTx pgx.Tx, snap *pgx.Conn, o MoveOptions, tables []moveTable, way, server string) (pause, pgx.Tx, pgx.Tx, error) {
	var p pause
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return p, nil, nil, err
	}
	defer tx.Rollback(ctx)
	var lock []string
	for _, t := range tables {
		lock = append(lock, "ONLY "+qr(t.name))
	}
	// Not the foreign partitions: Postgres does not lock a foreign table (42809), and every
	// writer of them comes through the parent (DETACH locks them itself).
	for _, t := range tables {
		lock = append(lock, "ONLY "+qr(hn(t.name, "__cloud")))
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
		return p, nil, nil, err
	}
	// The cloud first: a writer of the cloud waiting for a lock in the organization's database
	// while holding one here would wait for us, and nobody sees a wait across two databases.
	if _, err := tx.Exec(ctx, "LOCK TABLE "+strings.Join(lock, ", ")+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01") {
			return p, nil, nil, fmt.Errorf("%w: %v", errRetry, err)
		}
		return p, nil, nil, err
	}
	paused := time.Now()
	if lockTx == nil {
		if lockTx, err = lockCustomer(ctx, locker, tables, o.LockTimeout); err != nil {
			return p, nil, nil, err
		}
		defer lockTx.Rollback(context.Background())
	}
	if way == WayCapture {
		// no writer is left on either side: the log is everything
		if err := drainLog(ctx, lockTx, apply, o, tables, paused); err != nil {
			return p, nil, nil, err
		}
	}
	p.catchUp = time.Since(paused)
	snapTx, err := snap.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return p, nil, nil, fmt.Errorf("customer database: %w", err)
	}
	if _, err := snapTx.Exec(ctx, "SELECT 1"); err != nil { // the snapshot is taken here
		_ = snapTx.Rollback(ctx)
		return p, nil, nil, fmt.Errorf("customer database: %w", err)
	}
	// The cloud side as switched: writers resume at COMMIT, and those that pass by the proxy
	// (its pause) write the new partition at once. The partitions hold every change now (the
	// drain is done, no writer is left); this transaction's snapshot, exported before it
	// changes anything, is read on apply after COMMIT.
	cloudSnap, err := exportedSnapshot(ctx, tx, apply)
	if err != nil {
		_ = snapTx.Rollback(ctx)
		return p, nil, nil, err
	}
	fail := func(err error) (pause, pgx.Tx, pgx.Tx, error) {
		_ = snapTx.Rollback(ctx)
		_ = cloudSnap.Rollback(ctx)
		return p, nil, nil, err
	}
	// The row counts before the point of no return (the admin review, A7; precheck.go): a
	// copy that differs stops here, while the organization still works in its own database,
	// instead of in the verification after the switch, when the cloud already serves it.
	if err := verifyCounts(ctx, tx, snapTx, tables, o.Log, "nothing switched, the organization stays in its own database; run move-back again (it copies from the start)"); err != nil {
		return fail(err)
	}
	owner, err := rowsOf(ctx, tx, `SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass`, qr(tables[0].name))
	if err != nil {
		return fail(err)
	}
	var steps []string
	for _, t := range tables {
		// the refs of rows written or deleted past the cloud (review3 R3-4, moveback_refs.go)
		refs, err := refsSteps(ctx, tx, t)
		if err != nil {
			return fail(err)
		}
		fks, err := missingFKs(ctx, tx, t)
		if err != nil {
			return fail(err)
		}
		steps = append(append(append(steps, refs...), fks...), swapSteps(t, owner[0][0])...)
	}
	// the copy left in its database, until it is marked as returned (review3 R3-13, returned.go)
	dbID, err := rowsOf(ctx, tx, `SELECT coalesce(to_jsonb(o) ->> 'db_id', '') FROM lokadb.orgs o WHERE org = $1`, o.Org)
	if err != nil {
		return fail(err)
	}
	if len(dbID) > 0 {
		steps = append(steps, returnedSteps(o.Org, dbID[0][0])...)
	}
	steps = append(steps, fmt.Sprintf("DELETE FROM lokadb.orgs WHERE org = %d", o.Org), metaChanged)
	for _, s := range steps {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fail(fmt.Errorf("%w\n  in: %s\n  the organization stays in its database; run move-back again", err, s))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fail(fmt.Errorf("%w (the organization stays in its database; run move-back again)", err))
	}
	p.total = time.Since(paused)
	p.swap = p.total - p.catchUp
	return p, snapTx, cloudSnap, nil
}

// missingFKs: the FKs of DEFAULT the organization's partition does not have yet, NOT VALID
// (no scan). Put on in the switch, with the log drained: from the first write after it the
// partition's rows follow the cloud's FK actions (a parent deleted in the cloud sets them NULL
// or deletes them), and VALIDATE checks them afterwards.
func missingFKs(ctx context.Context, conn querier, t moveTable) ([]string, error) {
	fks, err := rowsOf(ctx, conn, `SELECT d.conname::text, pg_get_constraintdef(d.oid) FROM pg_constraint d
		WHERE d.conrelid = $1::regclass AND d.contype = 'f'
		  AND NOT EXISTS (SELECT 1 FROM pg_constraint p WHERE p.conrelid = $2::regclass AND p.conname = d.conname)`,
		qr(hn(t.name, "__cloud")), qr(t.local()))
	if err != nil {
		return nil, err
	}
	var out []string
	for _, f := range fks {
		out = append(out, fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID", qr(t.local()), q(f[0]), strings.TrimSuffix(f[1], " NOT VALID")))
	}
	return out, nil
}

// exportedSnapshot opens a read-only transaction on conn that sees what tx sees now (tx's own
// changes aside), and keeps seeing it after tx commits.
func exportedSnapshot(ctx context.Context, tx pgx.Tx, conn *pgx.Conn) (pgx.Tx, error) {
	var id string
	if err := tx.QueryRow(ctx, "SELECT pg_export_snapshot()").Scan(&id); err != nil {
		return nil, err
	}
	snap, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err := snap.Exec(ctx, "SET TRANSACTION SNAPSHOT "+lit(id)); err != nil {
		_ = snap.Rollback(ctx)
		return nil, err
	}
	return snap, nil
}

// rowDiff names the tables whose rows differ and, up to a few, the keys that do: only in the
// cloud, only in the organization's database, or in both with other values. Keys are read
// only for tables of at most diffRowsMax rows.
func rowDiff(ctx context.Context, cloud, cust querier, tables []moveTable) string {
	const diffRowsMax, shown = 100000, 5
	var out []string
	for _, t := range tables {
		read := func(on querier, table string) (map[string]string, error) {
			rows, err := rowsOf(ctx, on, fmt.Sprintf("SELECT %s, hashtextextended(%s, 0)::text FROM %s t LIMIT %d",
				keyText(t, "t"), rowDigest(t), qr(table), diffRowsMax+1))
			if err != nil {
				return nil, err
			}
			m := map[string]string{}
			for _, r := range rows {
				m[r[0]] = r[1]
			}
			return m, nil
		}
		a, err := read(cloud, t.local())
		if err != nil {
			out = append(out, t.name+": "+err.Error())
			continue
		}
		b, err := read(cust, t.name)
		if err != nil {
			out = append(out, t.name+": customer database: "+err.Error())
			continue
		}
		if len(a) > diffRowsMax || len(b) > diffRowsMax {
			out = append(out, fmt.Sprintf("%s: more than %d rows, keys not compared", t.name, diffRowsMax))
			continue
		}
		var only, there, other []string
		for k, h := range a {
			if hb, ok := b[k]; !ok {
				only = append(only, k)
			} else if hb != h {
				other = append(other, k)
			}
		}
		for k := range b {
			if _, ok := a[k]; !ok {
				there = append(there, k)
			}
		}
		if len(only)+len(there)+len(other) == 0 {
			continue
		}
		cut := func(ks []string) string {
			sort.Strings(ks)
			if len(ks) > shown {
				return strings.Join(ks[:shown], ",") + fmt.Sprintf(",… %d", len(ks))
			}
			return strings.Join(ks, ",")
		}
		out = append(out, fmt.Sprintf("%s: %d rows in the cloud, %d there; only in the cloud [%s], only there [%s], other values [%s]",
			t.name, len(a), len(b), cut(only), cut(there), cut(other)))
		// the rows themselves, a few of each: what the move's trace is read against
		row := func(on querier, table, key string) string {
			r, err := rowsOf(ctx, on, fmt.Sprintf("SELECT t::text FROM %s t WHERE %s = $1", qr(table), keyText(t, "t")), key)
			if err != nil {
				return "(" + err.Error() + ")"
			}
			if len(r) == 0 {
				return "(none)"
			}
			return r[0][0]
		}
		for _, ks := range [][]string{only, there, other} {
			for i, k := range ks {
				if i == 3 {
					break
				}
				out = append(out, fmt.Sprintf("%s %s: cloud %s, there %s", t.name, k, row(cloud, t.local(), k), row(cust, t.name, k)))
			}
		}
	}
	return strings.Join(out, "; ")
}

// swapSteps puts the local partition in the foreign one's place. The foreign table is kept
// (renamed) until the server goes; a view under its name sends what a proxy with the old
// metadata addresses to it into the parent, and so into the new partition.
func swapSteps(t moveTable, owner string) []string {
	return []string{
		fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", qr(t.name), qr(t.foreign())),
		fmt.Sprintf("DROP POLICY IF EXISTS %s ON %s", q(fmt.Sprintf("lokadb_bind_org%d", t.org)), qr(t.name)),
		fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (%s)", qr(t.name), qr(t.local()), t.val),
		fmt.Sprintf("ALTER FOREIGN TABLE %s RENAME TO %s", qr(t.foreign()), q(nameOf(t.gone()))),
		fmt.Sprintf("CREATE VIEW %s WITH (security_invoker = true) AS SELECT * FROM %s WHERE %s = %s",
			qr(t.bridge()), qr(t.name), q(t.tenant), t.val),
		fmt.Sprintf("ALTER VIEW %s OWNER TO %s", qr(t.bridge()), q(owner)),
	}
}

// afterSwitch verifies the organization's rows (snapTx and cloudSnap: its database and the cloud as switched; nil when
// an earlier run switched), puts the FKs on the new partitions, marks the tables left in the
// organization's database, drops the server and, after Bridge, the views.
func afterSwitch(ctx context.Context, cloud *pgx.Conn, snapTx, cloudSnap pgx.Tx, o MoveBackOptions, tables []moveTable, server, schema string, capture bool, log *slog.Logger) error {
	if snapTx != nil {
		same, err := compare(ctx, cloudSnap, snapTx, tables, moveTable.local, func(t moveTable) string { return t.name })
		diff := ""
		if err == nil && !same {
			diff = rowDiff(ctx, cloudSnap, snapTx, tables)
		}
		_ = snapTx.Rollback(ctx)
		_ = cloudSnap.Rollback(ctx)
		if err != nil {
			return fmt.Errorf("verify after switch: %w", err)
		}
		if !same {
			return fmt.Errorf("verify after switch: the cloud's rows differ from the organization's database as switched (%s); "+
				"its tables there are left as they are: compare them, move-org moves the organization out again", diff)
		}
		log.Info("verified: the cloud's partitions equal the organization's database at the switch")
	}
	log.Info("step 5/5: foreign keys, cleanup")
	for _, t := range tables {
		// an earlier run that switched before the FKs went on in the switch
		fks, err := missingFKs(ctx, cloud, t)
		if err != nil {
			return err
		}
		if err := execAll(ctx, cloud, fks); err != nil {
			return err
		}
		left, err := rowsOf(ctx, cloud, `SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'f' AND NOT convalidated`, qr(t.local()))
		if err != nil {
			return err
		}
		for _, f := range left {
			if err := execAll(ctx, cloud, []string{fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", qr(t.local()), q(f[0]))}); err != nil {
				log.Warn("foreign key left NOT VALID: rows of the organization reference rows that are gone (its database did not check this one)",
					"table", t.local(), "constraint", f[0], "err", err)
			}
		}
	}
	if server == "" {
		r, err := rowsOf(ctx, cloud, `SELECT srvname::text FROM pg_foreign_server WHERE srvname = $1`, fmt.Sprintf("lokadb_org%d", o.Org))
		if err != nil {
			return err
		}
		if len(r) > 0 {
			server = r[0][0]
		}
	}
	if o.CustomerDSN != "" && !o.Abandon {
		if err := markReturned(ctx, o, tables, schema, capture, log); err != nil {
			log.Warn("the organization's database: capture triggers or the mark on its tables left (the triggers stop logging by themselves)", "err", err)
		}
	}
	if server != "" {
		// the foreign tables (renamed) and user mappings go with it
		if err := execAll(ctx, cloud, []string{"DROP SERVER IF EXISTS " + q(server) + " CASCADE"}); err != nil {
			return err
		}
		log.Info("server dropped", "server", server)
	}
	if o.Bridge > 0 {
		log.Info("waiting for proxies to reload their metadata before the bridge views go", "bridge", o.Bridge)
		select {
		case <-time.After(o.Bridge):
		case <-ctx.Done():
			return fmt.Errorf("interrupted: the bridge views stay; run move-back again to drop them: %w", ctx.Err())
		}
	}
	var drops []string
	for _, t := range tables {
		drops = append(drops, doTag(fmt.Sprintf(`DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_class WHERE oid = to_regclass(%s) AND relkind = 'v') THEN
			EXECUTE 'DROP VIEW ' || %s; END IF; END $$`, lit(qr(t.bridge())), lit(qr(t.bridge())))))
	}
	return execAll(ctx, cloud, drops)
}

// markReturned removes the capture from the organization's database and marks its tables:
// they stay (the customer's copy as of the switch), never dropped by lokadb.
func markReturned(ctx context.Context, o MoveBackOptions, tables []moveTable, schema string, capture bool, log *slog.Logger) error {
	cust, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return err
	}
	defer cust.Close(context.Background())
	if schema == "" {
		schema = "public"
	}
	if err := setCustomerSchema(ctx, cust, schema); err != nil {
		return err
	}
	if capture { // the pause way put no trigger there (and its role may not drop one)
		src := func(t moveTable) string { return t.name }
		if err := execAll(ctx, cust, dropCapture(o.Org, tables, src)); err != nil {
			return err
		}
	}
	// the database's id in lokadb's mark on its tables (dbid.go), recorded by the switch
	var dbID string
	if len(tables) > 0 {
		var mark string
		err := cust.QueryRow(ctx, `SELECT coalesce(obj_description(to_regclass($1), 'pg_class'), '')`,
			fmt.Sprintf("%s.%s", q(custSchemaOf(tables[0].name, schema)), q(nameOf(tables[0].name)))).Scan(&mark)
		if i := strings.Index(mark, " database "); err == nil && isOrgMark(mark, o.Org) && i >= 0 {
			dbID = mark[i+len(" database "):]
		}
	}
	at := time.Now()
	var drop, marks []string
	for i := len(tables) - 1; i >= 0; i-- {
		t := tables[i]
		name := fmt.Sprintf("%s.%s", q(custSchemaOf(t.name, schema)), q(nameOf(t.name)))
		drop = append(drop, name)
		marks = append(marks, fmt.Sprintf("COMMENT ON TABLE %s IS %s", name, lit(returnedMark(o.Org, at))))
	}
	// Only the owner may comment: a role that does not own the tables leaves it to the customer.
	if err := execAll(ctx, cust, marks); err != nil {
		log.Warn("the organization's tables stay in its database unmarked (this role does not own them); the customer marks them with: "+
			strings.Join(marks, "; "), "err", err)
	} else {
		log.Info("the organization's tables stay in its database, marked as returned")
		if err := clearReturned(ctx, o.CloudDSN, o.Org, dbID); err != nil {
			log.Warn("the cloud's record of the copy left in the organization's database stays (lokadb.returned): move-org into it refuses its tables until it is deleted", "err", err)
		}
	}
	log.Info("lokadb does not drop them; the customer drops them with: DROP TABLE " + strings.Join(drop, ", "))
	return nil
}

// abandonMaps: the statement that deletes the organization's rows of the maps of unique
// columns of tables (quoted names). The organization is its value, as a literal inside the
// string the DO block formats: a uuid is not its number (review A9).
func abandonMaps(tables []string, org Tenant) string {
	return doTag(fmt.Sprintf(`DO $$ DECLARE r record; BEGIN
		FOR r IN SELECT refs_table FROM lokadb.unique_refs WHERE table_name IN (%s) LOOP
			EXECUTE format('DELETE FROM %%s WHERE tenant = %s', lokadb.rel(r.refs_table));
		END LOOP; END $$`, strings.Join(tables, ", "), strings.ReplaceAll(org.Lit(), "'", "''")))
}

// abandon brings an organization whose database is lost back into the cloud empty.
func abandon(ctx context.Context, cloud *pgx.Conn, o MoveOptions, tables []moveTable, server string, log *slog.Logger) error {
	log.Warn("ABANDON: the organization comes back into the cloud EMPTY; every row of it in its database is given up")
	if err := staging(ctx, cloud, o, tables, log); err != nil {
		return err
	}
	owner, err := rowsOf(ctx, cloud, `SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass`, qr(tables[0].name))
	if err != nil {
		return err
	}
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
		return err
	}
	var steps []string
	for _, t := range tables {
		fks, err := missingFKs(ctx, tx, t)
		if err != nil {
			return err
		}
		steps = append(append(steps, fks...), swapSteps(t, owner[0][0])...)
	}
	// Children first; cloud tables' FKs on the refs tables act as declared (CASCADE, SET NULL)
	// or refuse (RESTRICT, NO ACTION): then nothing changes.
	var names []string
	for _, t := range tables {
		names = append(names, lit(t.name))
	}
	steps = append(steps, abandonMaps(names, o.org())) // the unique columns' maps (block 4, item 2)
	for i := len(tables) - 1; i >= 0; i-- {
		steps = append(steps, fmt.Sprintf("DELETE FROM %s WHERE tenant = %s", qr(hn(tables[i].name, "__refs")), tables[i].val))
	}
	steps = append(steps, fmt.Sprintf("DELETE FROM lokadb.orgs WHERE org = %d", o.Org), metaChanged)
	for _, s := range steps {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("%w\n  in: %s\n  nothing changed (cloud rows that reference the organization's rows and refuse their deletion stop -abandon)", err, s)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	log.Warn("ABANDONED: the organization is back in the cloud with no rows; its refs rows are deleted")
	return afterSwitch(ctx, cloud, nil, nil, MoveBackOptions{Org: o.Org, Bridge: 0, Abandon: true}, tables, server, "", false, log)
}
