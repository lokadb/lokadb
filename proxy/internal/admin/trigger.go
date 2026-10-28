package admin

// The move without logical replication ("trigger" mode). Triggers on the organization's
// partitions log every change into lokadb.move_log (whole row image, writing transaction);
// move-org copies the partitions with COPY and then applies the log in the organization's
// database over its own connection, whole cloud transactions at a time. Nothing in the
// organization's database connects to the cloud: no wal_level = logical, no replication
// slot, no subscription (and none of its privileges), no logical replication workers.
//
// Why it converges: the triggers are committed before the copy's snapshot is taken, so a
// change is in the copy or in the log (or both: applying an entry is idempotent, an upsert
// or a delete by id). Entries of one row follow the row's own order: the transaction that
// changes it next waits for the previous one to commit, so its entries come later, and
// transactions are applied in the order of their last entry. Under the switch's lock no
// writer is left: the log is applied to the end and the copy equals the source.

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Move modes (MoveOptions.Mode).
const (
	ModeAuto    = "auto"
	ModeLogical = "logical"
	ModeTrigger = "trigger"
)

const captureSQL = `
CREATE TABLE IF NOT EXISTS lokadb.move_log (
  seq    bigserial PRIMARY KEY,
  org    bigint NOT NULL,
  xid    xid8 NOT NULL DEFAULT pg_current_xact_id(),
  tbl    text NOT NULL,
  op     "char" NOT NULL, -- I, U, D
  old_id text,            -- U, D: the row's key before the change (as text: a key of any type)
  row    jsonb            -- I, U: the row after it
);
CREATE INDEX IF NOT EXISTS move_log_org ON lokadb.move_log (org, seq);
-- (A5) the transactions still open when the log was read: their entries come in late
CREATE INDEX IF NOT EXISTS move_log_xid ON lokadb.move_log (org, xid);
-- I, U of a row too large for a jsonb image (row NULL): the row is read from src, by new_id,
-- when the entry is applied
ALTER TABLE lokadb.move_log ADD COLUMN IF NOT EXISTS new_id text;
ALTER TABLE lokadb.move_log ADD COLUMN IF NOT EXISTS src text;
-- the log of an older lokadb kept bigint ids (every table's key was id)
DO $m$ BEGIN
  IF (SELECT atttypid FROM pg_attribute WHERE attrelid = 'lokadb.move_log'::regclass AND attname = 'old_id') <> 'text'::regtype THEN
    ALTER TABLE lokadb.move_log ALTER COLUMN old_id TYPE text;
  END IF;
  IF (SELECT atttypid FROM pg_attribute WHERE attrelid = 'lokadb.move_log'::regclass AND attname = 'new_id') <> 'text'::regtype THEN
    ALTER TABLE lokadb.move_log ALTER COLUMN new_id TYPE text;
  END IF;
END $m$;

-- move-org marks it every few seconds while it moves the organization. A capture whose move
-- is gone (move-org killed) stops logging after a while; lost tells a move still running
-- that a change went unlogged, so it gives up instead of finishing with it missing.
CREATE TABLE IF NOT EXISTS lokadb.move_heartbeat (
  org  bigint PRIMARY KEY,
  at   timestamptz NOT NULL,
  lost boolean NOT NULL DEFAULT false
);
-- move_log_entry logs one change (called by each table's capture trigger, see captureFuncDDL).
-- SECURITY DEFINER: the application's role writes the log without any right on it.
CREATE OR REPLACE FUNCTION lokadb.move_log_entry(e_org bigint, e_tbl text, e_op "char", e_old text, e_new text, e_row jsonb, e_src text)
  RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, lokadb, pg_temp AS $$
DECLARE
  beat timestamptz;
BEGIN
  SELECT h.at INTO beat FROM lokadb.move_heartbeat h WHERE h.org = e_org;
  IF beat IS NULL OR beat < clock_timestamp() - interval '%HEARTBEAT_TTL%' THEN
    -- in the writer's transaction: if it rolls back, nothing was missed
    UPDATE lokadb.move_heartbeat h SET lost = true WHERE h.org = e_org AND NOT h.lost;
    RETURN;
  END IF;
  INSERT INTO lokadb.move_log (org, tbl, op, old_id, new_id, row, src)
    VALUES (e_org, e_tbl, e_op, e_old, CASE WHEN e_row IS NULL THEN e_new END, e_row, CASE WHEN e_row IS NULL THEN e_src END);
END $$;
-- The same with bigint keys, as the capture functions of an earlier build call it: kept for
-- their triggers (PL/pgSQL records no dependency on what it calls, a DROP would break them).
CREATE OR REPLACE FUNCTION lokadb.move_log_entry(e_org bigint, e_tbl text, e_op "char", e_old bigint, e_new bigint, e_row jsonb, e_src text)
  RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, lokadb, pg_temp AS $$
  SELECT lokadb.move_log_entry(e_org, e_tbl, e_op, e_old::text, e_new::text, e_row, e_src)
$$;
-- The capture of an older lokadb (one function for every table; kept for its triggers).
CREATE OR REPLACE FUNCTION lokadb.move_capture() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path = pg_catalog, lokadb, pg_temp AS $$
DECLARE
  beat timestamptz;
BEGIN
  SELECT at INTO beat FROM lokadb.move_heartbeat WHERE org = TG_ARGV[0]::bigint;
  IF beat IS NULL OR beat < clock_timestamp() - interval '%HEARTBEAT_TTL%' THEN
    -- in the writer's transaction: if it rolls back, nothing was missed
    UPDATE lokadb.move_heartbeat SET lost = true WHERE org = TG_ARGV[0]::bigint AND NOT lost;
    RETURN NULL;
  END IF;
  IF TG_OP = 'DELETE' THEN
    INSERT INTO lokadb.move_log (org, tbl, op, old_id) VALUES (TG_ARGV[0]::bigint, TG_ARGV[1], 'D', OLD.id);
  ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO lokadb.move_log (org, tbl, op, old_id, row) VALUES (TG_ARGV[0]::bigint, TG_ARGV[1], 'U', OLD.id, to_jsonb(NEW));
  ELSE
    INSERT INTO lokadb.move_log (org, tbl, op, row) VALUES (TG_ARGV[0]::bigint, TG_ARGV[1], 'I', to_jsonb(NEW));
  END IF;
  RETURN NULL;
END $$;
-- Only the capture functions call move_log_entry, as their owner (SECURITY DEFINER): no other
-- role may write the move's log with it (schema lokadb is PUBLIC's to read, baseSQL).
REVOKE EXECUTE ON FUNCTION lokadb.move_log_entry(bigint, text, "char", text, text, jsonb, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION lokadb.move_log_entry(bigint, text, "char", bigint, bigint, jsonb, text) FROM PUBLIC;
`

func captureTrigger(org int64) string { return fmt.Sprintf("lokadb_move_org%d", org) }

// heartbeatTTL: a capture whose move-org has not marked the heartbeat for this long stops
// logging (move-org is gone). The move itself gives up at half of it, before that happens.
const heartbeatTTL = 10 * time.Minute

func captureDDL() string {
	return strings.ReplaceAll(captureSQL, "%HEARTBEAT_TTL%", fmt.Sprintf("%d seconds", int(heartbeatTTL.Seconds())))
}

// bigRow: a row whose large values add up to more than this is logged without its image
// (A5): to_jsonb of it could pass jsonb's limit (bytea doubles as hex: about 134 MB) and fail
// the application's write. Its entry is applied by reading the row where it is.
const bigRow = 32 << 20

// captureFunc names the capture trigger function of one table (src) of org.
func captureFunc(org int64, src string) string {
	return "lokadb." + q(fmt.Sprintf("move_capture_o%d_%08x", org, crc32.ChecksumIEEE([]byte(src))))
}

// captureFuncDDL: the capture trigger function of table t (on src), made for its columns:
// an UPDATE that changes nothing is not logged, and a row that is too large goes without its
// image (bigRow).
func captureFuncDDL(org int64, t moveTable, src string) string {
	var size []string
	for _, c := range t.cols {
		if c[5] == "v" { // virtual: computed when read, nothing stored
			continue
		}
		typ := c[1]
		if typ == "bytea" || typ == "text" || strings.HasPrefix(typ, "character") {
			size = append(size, fmt.Sprintf("coalesce(octet_length(NEW.%s), 0)", q(c[0]))) // the stored length: no detoasting
		} else {
			size = append(size, fmt.Sprintf("coalesce(pg_column_size(NEW.%s), 0)", q(c[0])))
		}
	}
	if len(size) == 0 {
		size = []string{"0"}
	}
	// SECURITY DEFINER: the application's role needs no right on schema lokadb
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  big boolean := false;
BEGIN
  IF TG_OP = 'DELETE' THEN
    PERFORM lokadb.move_log_entry(%d, %s, 'D', %s, NULL, NULL, NULL);
    RETURN NULL;
  END IF;
  -- *= compares the rows' binary images: any type, json and point too (no equality needed)
  IF TG_OP = 'UPDATE' AND NEW *= OLD THEN RETURN NULL; END IF;
  big := (%s)::bigint > %d;
  PERFORM lokadb.move_log_entry(%d, %s, CASE TG_OP WHEN 'UPDATE' THEN 'U' ELSE 'I' END::"char",
    CASE WHEN TG_OP = 'UPDATE' THEN %s END, %s, CASE WHEN NOT big THEN to_jsonb(NEW) END,
    quote_ident(TG_TABLE_SCHEMA) || '.' || quote_ident(TG_TABLE_NAME));
  RETURN NULL;
END $$`, captureFunc(org, src), org, lit(t.name), keyText(t, "OLD"), strings.Join(size, " + "), bigRow, org, lit(t.name), keyText(t, "OLD"), keyText(t, "NEW"))
}

// beat marks the heartbeat until ctx ends, on a connection of its own.
func beat(ctx context.Context, dsn string, org int64, log *slog.Logger) {
	conn, err := connect(ctx, dsn)
	if err != nil {
		log.Warn("move heartbeat: no connection", "err", err)
		return
	}
	defer conn.Close(context.Background())
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := conn.Exec(ctx, "UPDATE lokadb.move_heartbeat SET at = clock_timestamp() WHERE org = $1", org); err != nil && ctx.Err() == nil {
				log.Warn("move heartbeat failed", "err", err)
			}
		}
	}
}

var errCaptureLost = errors.New("the change capture stopped (the move's heartbeat was missing): a change may be unlogged; the move gives up, run it again")

// captureOK fails if the capture may have missed a change, or soon may.
func captureOK(ctx context.Context, q querier, org int64) error {
	r, err := rowsOf(ctx, q, fmt.Sprintf(`SELECT (lost OR at < clock_timestamp() - interval '%d seconds')::text FROM lokadb.move_heartbeat WHERE org = $1`,
		int(heartbeatTTL.Seconds()/2)), org)
	if err != nil {
		return err
	}
	if len(r) == 0 || r[0][0] == "true" {
		return errCaptureLost
	}
	return nil
}

// LogicalReady reports whether the cloud database can publish an organization's partitions
// by logical replication (wal_level, slots, WAL senders).
func LogicalReady(ctx context.Context, cloud *pgx.Conn) (bool, error) {
	r, err := rowsOf(ctx, cloud, `SELECT (current_setting('wal_level') = 'logical'
		AND current_setting('max_replication_slots')::int > 0 AND current_setting('max_wal_senders')::int > 0)::text`)
	if err != nil {
		return false, err
	}
	return r[0][0] == "true", nil
}

// CanSubscribe reports whether the organization's database role can run the subscription
// of a logical move (tried, rolled back: superuser, pg_create_subscription or a managed
// service's own role), with logical replication workers. why: what stops it.
func CanSubscribe(ctx context.Context, cust *pgx.Conn) (ok bool, why string, err error) {
	r, err := rowsOf(ctx, cust, `SELECT current_setting('max_logical_replication_workers')`)
	if err != nil {
		return false, "", err
	}
	if r[0][0] == "0" {
		return false, "max_logical_replication_workers = 0", nil
	}
	if err := Probe(ctx, cust, SubscribeProbe()); err != nil {
		if ctx.Err() != nil {
			return false, "", ctx.Err()
		}
		return false, err.Error(), nil
	}
	return true, "", nil
}

// MoveMode is how move-org moves an organization: requested (auto, logical, trigger), given
// what both sides can do. auto takes triggers, always: logical replication connects the
// organization's database to the cloud as a role with the REPLICATION attribute, which reads
// the change stream of the whole cloud database, so it is only by an explicit -mode logical
// (and -allow-customer-replication, see chooseMode).
func MoveMode(requested string, logicalReady, canSubscribe bool) (string, error) {
	switch requested {
	case "", ModeAuto:
		return ModeTrigger, nil
	case ModeLogical, ModeTrigger:
		return requested, nil
	}
	return "", fmt.Errorf("-mode %q: want auto, logical or trigger", requested)
}

// errLogicalConsent: -mode logical without -allow-customer-replication.
var errLogicalConsent = errors.New("-mode logical: the subscription of the organization's database connects to the cloud as a role with the REPLICATION attribute, " +
	"and such a role can read the change stream of the whole cloud database (the rows of every organization, not this one's only) for as long as the move lasts. " +
	"Add -allow-customer-replication only if the cloud and the organization's database have one owner; -mode trigger (what auto takes) needs no such role")

func chooseMode(ctx context.Context, cloud, cust *pgx.Conn, o MoveOptions, log *slog.Logger) (string, error) {
	if o.Mode == ModeLogical && !o.AllowCustomerReplication {
		return "", errLogicalConsent
	}
	ready, err := LogicalReady(ctx, cloud)
	if err != nil {
		return "", err
	}
	sub, why, err := CanSubscribe(ctx, cust)
	if err != nil {
		return "", fmt.Errorf("customer database: %w", err)
	}
	// A subscription writes into the tables of the publication's schema and name: the
	// cloud's partitions are in public.
	if o.Mode == ModeLogical && o.CustomerSchema != "" && o.CustomerSchema != "public" {
		return "", fmt.Errorf("-mode logical: the organization's tables go to schema %s, and logical replication writes to the cloud's schema (public); use -mode trigger or auto", o.CustomerSchema)
	}
	mode, err := MoveMode(o.Mode, ready, sub)
	if err != nil {
		return "", err
	}
	if mode == ModeLogical {
		log.Warn("LOGICAL REPLICATION: for the time of the move the organization's database can read the change stream of the WHOLE cloud database (a role with the REPLICATION attribute): the rows of every organization, not this one's only. Use it only if the cloud and the organization's database have one owner",
			"cloud_logical", ready, "customer_can_subscribe", sub, "customer_cannot_because", why)
		return mode, nil
	}
	if o.Mode == ModeTrigger || o.Mode == "" || o.Mode == ModeAuto {
		log.Info("the move captures changes with triggers (no role that reads the change stream of the cloud database; -mode logical -allow-customer-replication for a subscription)")
	}
	return mode, nil
}

// installCapture creates the log and the organization's triggers on src(t) of every table
// (move-org: the cloud's partition t.local()); dropCapture removes them.
func installCapture(ctx context.Context, conn *pgx.Conn, o MoveOptions, tables []moveTable, src func(moveTable) string) error {
	if err := execAll(ctx, conn, []string{captureDDL(), fmt.Sprintf("DELETE FROM lokadb.move_log WHERE org = %d", o.Org),
		fmt.Sprintf(`INSERT INTO lokadb.move_heartbeat VALUES (%d, clock_timestamp(), false)
			ON CONFLICT (org) DO UPDATE SET at = excluded.at, lost = false`, o.Org)}); err != nil {
		return err
	}
	var funcs, stmts []string
	for _, t := range tables {
		funcs = append(funcs, captureFuncDDL(o.Org, t, src(t)))
		stmts = append(stmts,
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(captureTrigger(o.Org)), qr(src(t))),
			fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
				q(captureTrigger(o.Org)), qr(src(t)), captureFunc(o.Org, src(t))))
	}
	if err := execAll(ctx, conn, funcs); err != nil {
		return err
	}
	return lockedDDL(ctx, conn, o, stmts) // CREATE TRIGGER takes SHARE ROW EXCLUSIVE on the partition
}

func dropCapture(org int64, tables []moveTable, src func(moveTable) string) []string {
	var steps []string
	for _, t := range tables {
		steps = append(steps, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(captureTrigger(org)), qr(src(t))),
			"DROP FUNCTION IF EXISTS "+captureFunc(org, src(t))+"()")
	}
	// the log may not exist yet (a move that failed before the capture began)
	return append(steps, fmt.Sprintf(`DO $$ BEGIN
		IF to_regclass('lokadb.move_log') IS NOT NULL THEN DELETE FROM lokadb.move_log WHERE org = %d; END IF;
		IF to_regclass('lokadb.move_heartbeat') IS NOT NULL THEN DELETE FROM lokadb.move_heartbeat WHERE org = %d; END IF;
		END $$`, org, org))
}

// keyOf: the column that identifies a row of t (block 4: lokadb.tables.key_column).
func keyOf(t moveTable) string {
	if t.key != "" {
		return t.key
	}
	return "id"
}

// copyCols: the columns a row is written with (generated columns are computed there).
func copyCols(t moveTable) []string {
	var cols []string
	for _, c := range t.cols {
		if c[3] == "" {
			cols = append(cols, q(c[0]))
		}
	}
	return cols
}

// initialCopy copies srcName(t) of every table into dstName(t) (move-org: the cloud's
// partitions into the organization's database, both t.local()). One snapshot of from for
// every table, in the order given (copyOrder: referenced tables first): references hold. In the database dial connects to,
// batch rows (by id) per transaction. A batch whose connection there breaks (a WAN between
// the two) is sent again over a new connection: the copy goes on where it stopped, not from
// the start; the snapshot stays, and what arrived is known by the largest id there.
func initialCopy(ctx context.Context, from *pgx.Conn, dial func(context.Context) (*pgx.Conn, error), tables []moveTable, srcName, dstName func(moveTable) string, batch int, log *slog.Logger) error {
	src, err := from.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer src.Rollback(context.Background()) //nolint:errcheck
	c := &copier{ctx: ctx, dial: dial, log: log}
	defer c.close()
	for _, t := range tables { // referenced tables first (copyOrder)
		if err := c.table(src, t, srcName(t), dstName(t), batch); err != nil {
			return err
		}
	}
	return nil
}

// copyOrder sorts the tables so that one referenced by another's FK (through its refs table)
// is copied first: the organization's database checks those FKs as the copy arrives. Two
// tables of their own organization column are in the spec in any order (A6). A cycle keeps
// the spec's order.
func copyOrder(ctx context.Context, cloud *pgx.Conn, tables []moveTable) ([]moveTable, error) {
	deps := map[string][]string{}
	for _, t := range tables {
		r, err := rowsOf(ctx, cloud, `SELECT DISTINCT bt.table_name FROM pg_constraint c
			JOIN (SELECT table_name, refs_table FROM lokadb.tables UNION ALL SELECT table_name, refs_table FROM lokadb.unique_refs) bt
			  ON c.confrelid = to_regclass(lokadb.rel(bt.refs_table))
			WHERE c.contype = 'f' AND c.conrelid = $1::regclass AND bt.table_name <> $2`, qr(hn(t.name, "__cloud")), t.name)
		if err != nil {
			return nil, err
		}
		for _, d := range r {
			deps[t.name] = append(deps[t.name], d[0])
		}
	}
	in := map[string]bool{}
	for _, t := range tables {
		in[t.name] = true
	}
	placed := map[string]bool{}
	var out []moveTable
	left := tables
	for len(left) > 0 {
		var next []moveTable
		for _, t := range left {
			ready := true
			for _, d := range deps[t.name] {
				if in[d] && !placed[d] {
					ready = false
				}
			}
			if ready {
				out = append(out, t)
				placed[t.name] = true
			} else {
				next = append(next, t)
			}
		}
		if len(next) == len(left) {
			return append(out, next...), nil
		}
		left = next
	}
	return out, nil
}

// copier is the destination side of initialCopy: one connection, made again when it breaks.
type copier struct {
	ctx  context.Context
	dial func(context.Context) (*pgx.Conn, error)
	log  *slog.Logger
	dst  *pgx.Conn
	// strict: the role may not switch the FK checks off (setReplica said no): a table that
	// references itself, or tables in a cycle, copy only if the batches come in an order the FKs accept
	strict bool
}

// setReplica switches the FK checks (and the organization database's own triggers) off for
// the rest of the transaction, as the logical mode's table synchronization works: the copy is
// of rows that satisfied the FKs where they were, in batches that need not come in their order
// (a comment under a comment of a later batch; two tables that reference each other). It needs
// superuser or the SET privilege on session_replication_role; false: the role has neither.
func setReplica(ctx context.Context, tx pgx.Tx) (bool, error) {
	if _, err := tx.Exec(ctx, "SAVEPOINT lokadb_replica"); err != nil {
		return false, err
	}
	if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = replica"); err != nil {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "42501" {
			return false, err
		}
		_, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_replica")
		return false, err
	}
	_, err := tx.Exec(ctx, "RELEASE SAVEPOINT lokadb_replica")
	return err == nil, err
}

func (c *copier) close() {
	if c.dst != nil {
		c.dst.Close(context.Background())
	}
}

// broken: the connection failed (not a statement): another one may do.
func broken(conn *pgx.Conn, err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return strings.HasPrefix(pgErr.Code, "08") || pgErr.Code == "57P01" || pgErr.Code == "57P02" || pgErr.Code == "57P03"
	}
	return conn == nil || conn.IsClosed()
}

func (c *copier) table(src pgx.Tx, t moveTable, from, to string, batch int) error {
	key, nk := keyList(t), len(keyColsOf(t))
	var cols []string // first: the last line of a batch starts with its last key
	for _, k := range keyColsOf(t) {
		cols = append(cols, q(k))
	}
	for _, col := range copyCols(t) {
		if !strings.Contains(", "+key+", ", ", "+col+", ") {
			cols = append(cols, col)
		}
	}
	list := strings.Join(cols, ", ")
	var total int64
	if err := src.QueryRow(c.ctx, fmt.Sprintf("SELECT count(*) FROM %s", qr(from))).Scan(&total); err != nil {
		return err
	}
	p := newProgress(c.log, "copy", t.name, total, 0)
	// Two files: the next batch is read out of the snapshot while this one is sent (over a WAN
	// the sending is the slow half). The source never waits for the destination, and a batch
	// in a file can be sent again.
	type filled struct {
		f           *os.File
		rows, bytes int64
		last        string
	}
	free := make(chan *os.File, 2)
	for i := 0; i < 2; i++ {
		f, err := os.CreateTemp("", "lokadb-copy-*")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		defer f.Close()
		free <- f
	}
	ready := make(chan filled, 1)
	readErr := make(chan error, 1)
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	go func() {
		defer close(ready)
		where := ""
		for {
			var f *os.File
			select {
			case f = <-free:
			case <-ctx.Done():
				readErr <- ctx.Err()
				return
			}
			if err := f.Truncate(0); err != nil {
				readErr <- err
				return
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				readErr <- err
				return
			}
			lw := &lastLine{w: f}
			tag, err := src.Conn().PgConn().CopyTo(ctx, lw, fmt.Sprintf("COPY (SELECT %s FROM %s %s ORDER BY %s LIMIT %d) TO STDOUT", list, qr(from), where, key, batch))
			if err != nil {
				readErr <- fmt.Errorf("copy %s out: %w", from, err)
				return
			}
			rows := tag.RowsAffected()
			if rows == 0 {
				readErr <- nil
				return
			}
			fields := strings.SplitN(strings.TrimRight(string(lw.last), "\n"), "\t", nk+1)
			var vals []string
			for _, field := range fields[:min(nk, len(fields))] {
				if field == `\N` {
					readErr <- fmt.Errorf("copy %s: the batch ends with a NULL key", from)
					return
				}
				vals = append(vals, copyUnescape(field))
			}
			// a literal of the key's type (a row of them for a composite key): the next batch
			// starts past it
			last := keyLiteral(t, vals)
			select {
			case ready <- filled{f, rows, lw.bytes, last}:
			case <-ctx.Done():
				readErr <- ctx.Err()
				return
			}
			if rows < int64(batch) {
				readErr <- nil
				return
			}
			where = "WHERE " + keyRow(t, "") + " > " + last
		}
	}()
	for b := range ready {
		if err := c.send(b.f, t, to, list, b.last); err != nil {
			cancel()
			for range ready { // the reader stops
			}
			<-readErr
			return err
		}
		p.add(b.rows, b.bytes)
		free <- b.f
	}
	if err := <-readErr; err != nil {
		return err
	}
	p.done()
	return nil
}

// copyUnescape: a value of COPY's text format as it is (backslash escapes undone).
func copyUnescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' || i+1 == len(s) {
			b.WriteByte(s[i])
			continue
		}
		i++
		switch c := s[i]; c {
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'v':
			b.WriteByte('\v')
		case 'x': // \xH or \xHH
			v, n := 0, 0
			for ; n < 2 && i+1 < len(s) && isHex(s[i+1]); n++ {
				i++
				v = v*16 + hexVal(s[i])
			}
			if n == 0 {
				b.WriteByte('x')
			} else {
				b.WriteByte(byte(v))
			}
		case '0', '1', '2', '3', '4', '5', '6', '7': // \N, \NN, \NNN octal
			v := int(c - '0')
			for n := 1; n < 3 && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '7'; n++ {
				i++
				v = v*8 + int(s[i]-'0')
			}
			b.WriteByte(byte(v))
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func hexVal(c byte) int {
	switch {
	case c >= 'a':
		return int(c-'a') + 10
	case c >= 'A':
		return int(c-'A') + 10
	}
	return int(c - '0')
}

// send copies the file's batch into table to, in one transaction; over a broken connection
// it tries again on a new one, unless the batch (its last key, a literal) arrived before it broke.
func (c *copier) send(f *os.File, t moveTable, to, cols, last string) error {
	for attempt := 1; ; attempt++ {
		if c.dst == nil {
			conn, err := c.dial(c.ctx)
			if err != nil {
				if c.ctx.Err() != nil || attempt >= 5 {
					return fmt.Errorf("copy into %s: %w", to, err)
				}
				c.log.Warn("copy: no connection to the destination, retrying", "table", t.name, "attempt", attempt, "err", err)
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
				continue
			}
			c.dst = conn
			if attempt > 1 { // did the batch arrive before the connection broke?
				var there bool
				if err := conn.QueryRow(c.ctx, fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s = %s)", qr(to), keyRow(t, ""), last)).Scan(&there); err != nil {
					return fmt.Errorf("copy into %s: %w", to, err)
				}
				if there {
					return nil
				}
			}
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		err := pgx.BeginFunc(c.ctx, c.dst, func(tx pgx.Tx) error {
			if !c.strict {
				ok, err := setReplica(c.ctx, tx)
				if err != nil {
					return err
				}
				if !ok {
					c.strict = true
					c.log.Warn("initial copy: the role cannot set session_replication_role, so the FKs are checked in each batch: a table that references itself, or tables that reference each other in a cycle, may fail when their rows come in another order")
				}
			}
			_, err := tx.Conn().PgConn().CopyFrom(c.ctx, f, fmt.Sprintf("COPY %s (%s) FROM STDIN", qr(to), cols))
			return err
		})
		if err == nil {
			return nil
		}
		if c.ctx.Err() != nil || !broken(c.dst, err) || attempt >= 5 {
			return fmt.Errorf("copy into %s: %w", to, err)
		}
		c.log.Warn("copy: the connection to the destination broke, sending the batch again on a new one", "table", t.name, "attempt", attempt, "err", err)
		c.dst.Close(context.Background())
		c.dst = nil
	}
}

// lastLine passes COPY rows on (one Write each) and keeps the last one.
type lastLine struct {
	w     io.Writer
	last  []byte
	bytes int64
}

func (l *lastLine) Write(p []byte) (int, error) {
	l.last = append(l.last[:0], p...)
	l.bytes += int64(len(p))
	return l.w.Write(p)
}

type logEntry struct {
	seq   int64
	tbl   string
	op    string
	oldID *string // keys as text, cast to the key's type where applied
	newID *string
	row   *string
	src   *string
	fresh bool // past the cursor (not a late entry of an open transaction)
}

// logCursor is where applyLog stands in the log (A5): the entries up to last are applied,
// except those of the transactions its last read did not see (snap: that read's snapshot),
// which come in late. Each round reads what is new and those late ones, not the whole log.
// Not seen is more than open in xip: a transaction whose xid is not below the snapshot's xmax
// (one past the last completed xid, not the next to assign) is running too and is in no xip
// (review 3, R3-1: its entry was skipped by the catch-up and applied by the switch's drain
// after a newer image of the same row). The zero value reads the log from the start.
type logCursor struct {
	last int64
	snap string // pg_snapshot of the last read ("": none yet)
	more bool   // the last round stopped at a limit (applyRoundEntries, applyRoundBytes): more is there
}

// noSnapshot: what an empty cursor's late part reads with (it matches no entry: seq <= 0)
const noSnapshot = "1:1:"

// One round of the catch-up and of the switch's drain: the entries past the cursor, up to
// applyRoundEntries of them and applyRoundBytes of their images (as stored: pg_column_size).
// A round costs its round trips to the organization's database whatever its size, so it is
// large: 500 entries a round in 6 round trips gave 1 570 entries/s at most 43 ms away, fewer
// than 8 writers made (block 14, 27.10).
const (
	applyRoundEntries = 5000
	applyRoundBytes   = 8 << 20
)

// replicaRight: whether the move's role may SET session_replication_role in the organization's
// database (the FK checks off while the log is applied), asked once per move.
type replicaRight struct{ asked, ok bool }

const replicaSQL = "SET LOCAL session_replication_role = replica"

const logCols = `seq, tbl, op::text, old_id, new_id, row::text, src`

// logRound reads a round: the late entries (at or before the cursor $2, of transactions the
// last read, $3, did not see) and up to $4 fresh ones past it, in the order of the log. over: a
// fresh entry past $5 bytes of images before it, read without its image and left for the next
// round (the first one never is: a row larger than the budget goes alone).
const logRound = `(SELECT ` + logCols + `, false, false FROM lokadb.move_log
		WHERE org = $1 AND seq <= $2 AND NOT pg_visible_in_snapshot(xid, $3::pg_snapshot))
	UNION ALL
	(SELECT seq, tbl, op::text, old_id, new_id, CASE WHEN NOT over THEN f.row::text END, src, true, over FROM (
		SELECT seq, tbl, op, old_id, new_id, row, src,
			sum(coalesce(pg_column_size(row), 0)) OVER (ORDER BY seq) - coalesce(pg_column_size(row), 0) > $5 AS over
		FROM lokadb.move_log WHERE org = $1 AND seq > $2 ORDER BY seq LIMIT $4) f)
	ORDER BY 1`

// applyLog applies up to max entries of the log, in their order, in the organization's
// database (in one transaction there), then deletes them from the log. Entries of one row
// follow the row's own order: the transaction that changes it next waits for the previous
// one to commit, so its entries come later; an entry in the log is committed. src reads and
// deletes the log (the switch passes its own locked transaction). cur: where the last round
// stopped (nil: the start). The round goes to the organization's database in one round trip:
// BEGIN, the entries and COMMIT in one pipeline (an entry without an image, its row read from
// the cloud on the way, splits it).
func applyLog(ctx context.Context, src pgx.Tx, cloud *pgx.Conn, cust *pgx.Conn, o MoveOptions, tables []moveTable, max int, cur *logCursor) (int, error) {
	if cur == nil {
		cur = &logCursor{}
	}
	var entries []logEntry
	var snap string
	var more bool
	prev := cur.snap
	if prev == "" {
		prev = noSnapshot
	}
	read := func(qr pgx.Tx) error {
		rows, err := qr.Query(ctx, logRound, o.Org, cur.last, prev, max, applyRoundBytes)
		if err != nil {
			return err
		}
		fresh, over := 0, false
		for rows.Next() {
			var e logEntry
			var past bool
			if err := rows.Scan(&e.seq, &e.tbl, &e.op, &e.oldID, &e.newID, &e.row, &e.src, &e.fresh, &past); err != nil {
				rows.Close()
				return err
			}
			if e.fresh {
				fresh++
			}
			if past { // and every one after it (the sum only grows): the next round's
				over = true
				continue
			}
			entries = append(entries, e)
		}
		more = over || fresh >= max
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		// the snapshot of this read (the transaction's: repeatable read): what it did not see
		// comes in late, in the next round
		return qr.QueryRow(ctx, `SELECT pg_current_snapshot()::text`).Scan(&snap)
	}
	var err error
	if src != nil {
		err = read(src)
	} else {
		err = pgx.BeginTxFunc(ctx, cloud, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, read)
	}
	if err != nil {
		return 0, err
	}
	if len(entries) == 0 {
		cur.snap, cur.more = snap, false
		return 0, nil
	}
	byName := map[string]moveTable{}
	for _, t := range tables {
		byName[t.name] = t
	}
	steps := []applyStep{{b: &pgx.Batch{}}}
	seqs := make([]int64, 0, len(entries))
	last := cur.last
	for i := range entries {
		e := entries[i]
		t, ok := byName[e.tbl]
		if !ok {
			return 0, fmt.Errorf("move log: unknown table %q", e.tbl)
		}
		seqs = append(seqs, e.seq)
		if e.fresh && e.seq > last {
			last = e.seq
		}
		b := steps[len(steps)-1].b
		switch e.op {
		case "D":
			b.Queue(fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(t.local()), keyRow(t, ""), keyOfText(t, "$1")), e.oldID)
		case "I", "U":
			if e.row == nil { // in order: what comes before it is sent first
				steps[len(steps)-1].fetched = &entries[i]
				steps = append(steps, applyStep{b: &pgx.Batch{}})
				continue
			}
			if e.op == "U" && e.oldID != nil {
				// an UPDATE of the key itself moves the row
				b.Queue(fmt.Sprintf("DELETE FROM %s WHERE %s = %s AND %s <> %s", qr(t.local()), keyRow(t, ""), keyOfText(t, "$1"), keyRow(t, ""), keyOfRow(t, "$2")), e.oldID, e.row)
			}
			cols := copyCols(t)
			if t.replace {
				b.Queue(fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(t.local()), keyRow(t, ""), keyOfRow(t, "$1")), e.row)
				b.Queue(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb)",
					qr(t.local()), strings.Join(cols, ", "), strings.Join(cols, ", "), qr(t.local())), e.row)
				continue
			}
			b.Queue(fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM jsonb_populate_record(NULL::%s, $1::jsonb) ON CONFLICT (%s) DO %s",
				qr(t.local()), strings.Join(cols, ", "), strings.Join(cols, ", "), qr(t.local()), keyList(t), upsertAction(cols, keyOf(t))), e.row)
		default:
			return 0, fmt.Errorf("move log: unknown operation %q", e.op)
		}
	}
	var from *pgconn.PgConn
	if src != nil {
		from = src.Conn().PgConn()
	} else {
		from = cloud.PgConn()
	}
	// the entries come in the order of the changes, but a cycle of deferred FKs of the cloud may
	// put a child before its parent in one transaction: the FK checks stay off, as in the copy
	right := o.replica
	if right == nil {
		right = &replicaRight{}
	}
	if !right.asked {
		if err := pgx.BeginFunc(ctx, cust, func(tx pgx.Tx) (err error) {
			right.ok, err = setReplica(ctx, tx)
			return err
		}); err != nil {
			return 0, fmt.Errorf("customer database: %w", err)
		}
		right.asked = true
	}
	if len(steps) == 1 && steps[0].fetched == nil {
		err = applyPipelined(ctx, cust, steps[0].b, right.ok)
	} else {
		err = applySteps(ctx, cust, from, byName, steps, right.ok)
	}
	if err != nil {
		return 0, fmt.Errorf("customer database: applying the move log: %w", err)
	}
	phase := "catch-up"
	if src != nil {
		phase = "switch"
	}
	traceApplied(phase, cur.last, cur.snap, entries, tables) // LOKADB_MOVE_TRACE (movetrace.go)
	// applied there: forget them here (applying them again would be harmless)
	del := `DELETE FROM lokadb.move_log WHERE org = $1 AND seq = ANY($2)`
	if src != nil {
		_, err = src.Exec(ctx, del, o.Org, seqs)
	} else {
		_, err = cloud.Exec(ctx, del, o.Org, seqs)
	}
	if err != nil {
		return 0, err
	}
	cur.last, cur.snap, cur.more = last, snap, more
	return len(entries), nil
}

// applyStep: entries of a round sent together (b), then an entry without an image (fetched).
type applyStep struct {
	b       *pgx.Batch
	fetched *logEntry // an entry without an image, applied after b
}

// applyPipelined applies a round in a transaction of the organization's database sent at once:
// BEGIN, the FK checks off (replica), the entries, COMMIT, one round trip. After an error
// Postgres skips the rest of the batch; the transaction, still open, is rolled back then.
func applyPipelined(ctx context.Context, cust *pgx.Conn, entries *pgx.Batch, replica bool) error {
	b := &pgx.Batch{}
	b.Queue("BEGIN")
	if replica {
		b.Queue(replicaSQL)
	}
	b.QueuedQueries = append(b.QueuedQueries, entries.QueuedQueries...)
	b.Queue("COMMIT")
	br := cust.SendBatch(ctx, b)
	var err error
	for i := 0; i < b.Len() && err == nil; i++ {
		_, err = br.Exec()
	}
	if cerr := br.Close(); err == nil {
		err = cerr
	}
	if err != nil && !cust.IsClosed() {
		_, _ = cust.Exec(ctx, "ROLLBACK")
	}
	return err
}

// applySteps applies a round with entries without an image: their rows are read from the cloud
// (from) between the batches of the entries around them, in one transaction.
func applySteps(ctx context.Context, cust *pgx.Conn, from *pgconn.PgConn, byName map[string]moveTable, steps []applyStep, replica bool) error {
	return pgx.BeginFunc(ctx, cust, func(tx pgx.Tx) error {
		if replica {
			if _, err := tx.Exec(ctx, replicaSQL); err != nil {
				return err
			}
		}
		for _, st := range steps {
			if st.b.Len() > 0 {
				br := tx.SendBatch(ctx, st.b)
				for i := 0; i < st.b.Len(); i++ {
					if _, err := br.Exec(); err != nil {
						br.Close()
						return err
					}
				}
				if err := br.Close(); err != nil {
					return err
				}
			}
			if st.fetched != nil {
				if err := applyFetched(ctx, from, tx.Conn().PgConn(), byName[st.fetched.tbl], *st.fetched); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// upsertAction: what an INSERT ... ON CONFLICT (key) does with a row that is there.
func upsertAction(cols []string, key string) string {
	var sets, exc []string
	keys := map[string]bool{}
	for _, k := range KeyCols(key) {
		keys[q(k)] = true
	}
	for _, c := range cols {
		if !keys[c] {
			sets = append(sets, c)
			exc = append(exc, "EXCLUDED."+c)
		}
	}
	if len(sets) == 0 {
		return "NOTHING"
	}
	return fmt.Sprintf("UPDATE SET (%s) = ROW(%s)", strings.Join(sets, ", "), strings.Join(exc, ", "))
}

// applyFetched applies an entry without an image (a large row): the row is read from its table
// (e.src) as it is now and written there, or deleted if it is gone (a later change of it is in
// the log too). Built-in types travel in their binary form, the same everywhere: a bytea as is,
// not as hex twice its size.
func applyFetched(ctx context.Context, from, to *pgconn.PgConn, t moveTable, e logEntry) error {
	if e.newID == nil || e.src == nil {
		return fmt.Errorf("move log: entry %d has neither a row nor where to read it", e.seq)
	}
	cols := copyCols(t)
	key := keyList(t)
	sel := fmt.Sprintf("SELECT %s FROM %s WHERE %s = %s", strings.Join(cols, ", "), *e.src, keyRow(t, ""), keyOfText(t, "$1"))
	sd, err := from.Prepare(ctx, "", sel, nil)
	if err != nil {
		return err
	}
	formats := make([]int16, len(sd.Fields))
	oids := make([]uint32, len(sd.Fields))
	for i, f := range sd.Fields {
		if f.DataTypeOID < 10000 { // built-in (FirstNormalObjectId is 16384, extensions' types are above)
			formats[i], oids[i] = 1, f.DataTypeOID
		}
	}
	id := func(v string) [][]byte { return [][]byte{[]byte(v)} } // text: the key's input form
	res := from.ExecParams(ctx, sel, id(*e.newID), nil, nil, formats).Read()
	if res.Err != nil {
		return res.Err
	}
	if e.op == "U" && e.oldID != nil && *e.oldID != *e.newID {
		if r := to.ExecParams(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(t.local()), keyRow(t, ""), keyOfText(t, "$1")), id(*e.oldID), nil, nil, nil).Read(); r.Err != nil {
			return r.Err
		}
	}
	if len(res.Rows) == 0 {
		return to.ExecParams(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(t.local()), keyRow(t, ""), keyOfText(t, "$1")), id(*e.newID), nil, nil, nil).Read().Err
	}
	var ph []string
	for i := range cols {
		ph = append(ph, fmt.Sprintf("$%d", i+1))
	}
	ins := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO %s", qr(t.local()), strings.Join(cols, ", "), strings.Join(ph, ", "), key, upsertAction(cols, keyOf(t)))
	if t.replace { // no unique index on the key alone there: the row goes, then its image
		if r := to.ExecParams(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s = %s", qr(t.local()), keyRow(t, ""), keyOfText(t, "$1")), id(*e.newID), nil, nil, nil).Read(); r.Err != nil {
			return r.Err
		}
		ins = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qr(t.local()), strings.Join(cols, ", "), strings.Join(ph, ", "))
	}
	return to.ExecParams(ctx, ins, res.Rows[0], oids, formats, nil).Read().Err
}

// catchUp applies the log until it is short: the switch then has little left to apply. Every 5 s
// it reports how fast changes come and go (convergence); once the log left has not got shorter
// for -catchup-stall, it gives up rather than wait out -catchup-timeout: at these rates it would
// not catch up at all.
func catchUp(ctx context.Context, cloud, cust *pgx.Conn, o MoveOptions, tables []moveTable, log *slog.Logger) error {
	start, reported := time.Now(), time.Now()
	var applied, rounds int64
	var inRounds time.Duration
	cur := &logCursor{}
	conv := &convergence{}
	for {
		if err := captureOK(ctx, cloud, o.Org); err != nil {
			return err
		}
		t := time.Now()
		n, err := applyLog(ctx, nil, cloud, cust, o, tables, applyRoundEntries, cur)
		if err != nil {
			return err
		}
		if n > 0 {
			rounds++
			inRounds += time.Since(t)
		}
		applied += int64(n)
		// what is left past the cursor (counted now and then only: a count each round would
		// read the whole log each round)
		left := func() int64 {
			var left int64
			_ = cloud.QueryRow(ctx, "SELECT count(*) FROM lokadb.move_log WHERE org = $1 AND seq > $2", o.Org, cur.last).Scan(&left)
			return left
		}
		if !cur.more { // the round reached the end of the log
			log.Info("caught up", "applied_last_round", n, "left", left(), "took", time.Since(start).Round(time.Millisecond))
			return nil
		}
		if time.Since(reported) >= 5*time.Second {
			reported = time.Now()
			l := left()
			r := conv.add(reported, applied, l)
			round := time.Duration(0)
			if rounds > 0 {
				round = inRounds / time.Duration(rounds)
			}
			// table *: the log of every table; rows: log entries applied, rows_total: those and the ones left
			args := []any{"step", "catch-up", "table", "*", "rows", applied, "rows_total", applied + l, "bytes", 0,
				"elapsed", time.Since(start).Round(time.Second), "left", l, "round", round.Round(time.Millisecond)}
			switch {
			case !r.known:
			case r.eta >= 0:
				args = append(args, "in_per_s", int64(r.in), "out_per_s", int64(r.out), "eta", r.eta.Round(time.Second))
			default:
				args = append(args, "in_per_s", int64(r.in), "out_per_s", int64(r.out), "eta", "never at these rates")
			}
			log.Info("progress", args...)
			if stall := o.catchUpStall(); stall > 0 && r.stalled >= stall {
				return fmt.Errorf("the organization's database is not catching up with the cloud: changes come at %d/s and are applied at %d/s "+
					"(rounds of up to %d entries, %s each), and the %d log entries left have not got fewer in %s (-catchup-stall). "+
					"Nothing switched, the organization stays in the cloud. Run the move when it writes less, or with "+
					"-mode logical -allow-customer-replication (the organization's Postgres applies the changes as a stream, with no round trips)",
					int64(r.in), int64(r.out), applyRoundEntries, round.Round(time.Millisecond), l, r.stalled.Round(time.Second))
			}
		}
		if time.Since(start) > o.catchUpTimeout() {
			return fmt.Errorf("the organization's database did not catch up with the cloud in %s (-catchup-timeout; %d log entries left)", o.catchUpTimeout(), left())
		}
		if n == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// convergence follows the catch-up from its samples every 5 s: how fast changes come (in: the
// ones applied plus the growth of the log left) and are applied (out), over the last minute,
// and how long the log left has not been shorter than it is now (stalled: since the last sample
// with more left; a log that only grows or stays, since the first).
type convergence struct {
	samples []convSample // the last minute's, oldest first
	longer  []convSample // the samples with more left than every one after them, oldest first
	first   time.Time
}

type convSample struct {
	at            time.Time
	applied, left int64
}

// convRates: in and out per second; eta: when the log left is gone at those rates (-1: never,
// out is not above in); stalled: since the log left was last at its shortest.
type convRates struct {
	known   bool // two samples at least: the rates mean something
	in, out float64
	eta     time.Duration
	stalled time.Duration
}

const convWindow = time.Minute

func (c *convergence) add(now time.Time, applied, left int64) convRates {
	if c.first.IsZero() {
		c.first = now
	}
	for len(c.longer) > 0 && c.longer[len(c.longer)-1].left <= left {
		c.longer = c.longer[:len(c.longer)-1]
	}
	since := c.first
	if len(c.longer) > 0 {
		since = c.longer[len(c.longer)-1].at
	}
	c.longer = append(c.longer, convSample{now, applied, left})
	c.samples = append(c.samples, convSample{now, applied, left})
	for len(c.samples) > 2 && now.Sub(c.samples[1].at) >= convWindow {
		c.samples = c.samples[1:]
	}
	r := convRates{eta: -1, stalled: now.Sub(since)}
	first := c.samples[0]
	if dt := now.Sub(first.at).Seconds(); dt > 0 {
		r.known = true
		r.out = float64(applied-first.applied) / dt
		r.in = r.out + float64(left-first.left)/dt
		if r.in < 0 {
			r.in = 0
		}
	}
	switch {
	case left == 0:
		r.eta = 0
	case r.out > r.in:
		r.eta = time.Duration(float64(left) / (r.out - r.in) * float64(time.Second))
	}
	return r
}

// drainLog applies the whole log inside the switch's locked transaction, writes paused
// since paused: past MaxPause the switch gives up and is tried again (what it applied in the
// organization's database is applied again then: harmless).
func drainLog(ctx context.Context, tx pgx.Tx, cust *pgx.Conn, o MoveOptions, tables []moveTable, paused time.Time) error {
	cur := &logCursor{} // from the start: every writer is done, every entry is there
	for {
		if o.MaxPause > 0 && time.Since(paused) > o.MaxPause {
			return fmt.Errorf("%w: the log not applied after %s (-max-pause)", errRetry, o.MaxPause)
		}
		n, err := applyLog(ctx, tx, nil, cust, o, tables, 1000, cur)
		if err != nil {
			return err
		}
		if n == 0 { // every writer is done (the lock): lost now covers every change
			return captureOK(ctx, tx, o.Org)
		}
	}
}

// MoveRunning reports whether a move-org holds the cloud database right now.
func MoveRunning(ctx context.Context, q querier) (bool, error) {
	r, err := rowsOf(ctx, q, `SELECT (NOT pg_try_advisory_lock($1))::text`, moveLockKey)
	if err != nil {
		return false, err
	}
	if r[0][0] == "false" { // we got it: nobody runs; give it back
		if _, err := rowsOf(ctx, q, `SELECT pg_advisory_unlock($1)::text`, moveLockKey); err != nil {
			return false, err
		}
		return false, nil
	}
	return true, nil
}

// MoveLeftovers lists what moves left in the cloud database (meaningful when none runs):
// capture triggers and log rows of the trigger mode; publications, replication slots and
// roles of the logical mode; partitions left with REPLICA IDENTITY FULL; the CHECK on DEFAULT
// of a split that did not finish.
func MoveLeftovers(ctx context.Context, q querier) ([]string, error) {
	var out []string
	add := func(sql, format string) error {
		rows, err := rowsOf(ctx, q, sql)
		if err != nil {
			return err
		}
		for _, r := range rows {
			out = append(out, fmt.Sprintf(format, r[0]))
		}
		return nil
	}
	steps := []struct{ sql, format string }{
		{`SELECT format('%I ON %s', tgname, tgrelid::regclass) FROM pg_trigger WHERE tgname LIKE 'lokadb\_move\_org%'`, "trigger %s"},
		{`SELECT format('%s', pubname) FROM pg_publication WHERE pubname LIKE 'lokadb\_org%'`, "publication %s"},
		// the slot's name is its record: lokadb_<database OID>_org<N>, made before the subscription
		// and found here whatever killed the move; with the WAL it keeps the cloud from recycling
		{`SELECT slot_name::text || ' (holds ' || coalesce(pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), 'no') || ' of WAL)'
			FROM pg_replication_slots WHERE slot_name LIKE 'lokadb\_%\_org%' AND database = current_database()`, "replication slot %s"},
		// roles belong to the cluster: only this database's (its OID in the name), whose
		// objects DROP OWNED here can remove
		{`SELECT rolname::text FROM pg_roles WHERE rolname LIKE 'lokadb\_repl\_' || (SELECT oid FROM pg_database WHERE datname = current_database()) || '\_org%'`, "role %s"},
	}
	for _, st := range steps {
		if err := add(st.sql, st.format); err != nil {
			return nil, err
		}
	}
	r, err := rowsOf(ctx, q, `SELECT (to_regclass('lokadb.move_log') IS NOT NULL)::text, (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil {
		return nil, err
	}
	if r[0][0] == "true" {
		if err := add(`SELECT count(*)::text FROM lokadb.move_log HAVING count(*) > 0`, "%s rows in lokadb.move_log"); err != nil {
			return nil, err
		}
	}
	if r[0][1] == "true" {
		// a split killed before its last transaction: the copy (the rows are still in DEFAULT)
		if err := add(`SELECT c.oid::regclass::text FROM pg_class c
			WHERE c.relkind = 'r' AND NOT c.relispartition AND c.relname !~ '_source$'
			  AND obj_description(c.oid, 'pg_class') ~ '^lokadb: split of organization [0-9]+ out of DEFAULT$'`, "split copy %s (not attached)"); err != nil {
			return nil, err
		}
		guards, err := staleGuards(ctx, q, 0)
		if err != nil {
			return nil, err
		}
		for _, g := range guards {
			out = append(out, "CHECK of a failed split: "+g)
		}
		if err := add(`SELECT c.oid::regclass::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			JOIN lokadb.tables t ON i.inhparent = to_regclass(`+relSQL("t.table_name")+`)
			WHERE c.relreplident = 'f' AND c.relkind = 'r'`, "REPLICA IDENTITY FULL on %s"); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// CleanupMoves removes what interrupted moves left (MoveLeftovers), when no move-org runs.
// The organizations stay where they are: a move that did not switch left them in the cloud.
func CleanupMoves(ctx context.Context, dsn string, log *slog.Logger) ([]string, error) {
	conn, err := connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	var mine bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", moveLockKey).Scan(&mine); err != nil {
		return nil, err
	}
	if !mine {
		return nil, errors.New("a move-org is running against this cloud database: it cleans up after itself")
	}
	left, err := MoveLeftovers(ctx, conn)
	if err != nil {
		return nil, err
	}
	for _, l := range left {
		var steps []string
		switch {
		case strings.HasPrefix(l, "trigger "):
			steps = []string{"DROP TRIGGER IF EXISTS " + strings.TrimPrefix(l, "trigger ")}
		case strings.HasPrefix(l, "publication "):
			steps = []string{"DROP PUBLICATION IF EXISTS " + q(strings.TrimPrefix(l, "publication "))}
		case strings.HasPrefix(l, "replication slot "):
			name, _, _ := strings.Cut(strings.TrimPrefix(l, "replication slot "), " (holds ")
			if err := dropSlot(ctx, conn, name); err != nil {
				return nil, err
			}
		case strings.HasPrefix(l, "role "):
			steps = dropRole(strings.TrimPrefix(l, "role "))
		case strings.HasSuffix(l, " rows in lokadb.move_log"):
			steps = []string{"DELETE FROM lokadb.move_log", "DELETE FROM lokadb.move_heartbeat"}
		case strings.HasPrefix(l, "CHECK of a failed split: "):
			// its rows are still in DEFAULT: the CHECK refuses their updates
			// (DROP CONSTRAINT locks DEFAULT: queued behind a long transaction it would stop everybody)
			steps = []string{"SET lock_timeout = '5s'", strings.TrimPrefix(l, "CHECK of a failed split: "), "RESET lock_timeout"}
		case strings.HasPrefix(l, "split copy "):
			steps = []string{"SET lock_timeout = '5s'", "DROP TABLE " + strings.TrimSuffix(strings.TrimPrefix(l, "split copy "), " (not attached)"), "RESET lock_timeout"}
		case strings.HasPrefix(l, "REPLICA IDENTITY FULL on "):
			steps = []string{"ALTER TABLE " + strings.TrimPrefix(l, "REPLICA IDENTITY FULL on ") + " REPLICA IDENTITY DEFAULT"}
		}
		if err := execAll(ctx, conn, steps); err != nil {
			return nil, err
		}
		log.Info("removed", "what", l)
	}
	// the capture functions of the triggers dropped above (one per table and organization)
	if fns, err := rowsOf(ctx, conn, `SELECT p.oid::regprocedure::text FROM pg_proc p
		WHERE p.pronamespace = to_regnamespace('lokadb') AND p.proname ~ '^move_capture_o[0-9]+_'
		  AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgfoid = p.oid)`); err == nil {
		for _, f := range fns {
			_ = execAll(ctx, conn, []string{"DROP FUNCTION IF EXISTS " + f[0]})
		}
	}
	// heartbeats of moves that are gone (none runs: every one of them is)
	if r, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.move_heartbeat') IS NOT NULL)::text`); err == nil && r[0][0] == "true" {
		_ = execAll(ctx, conn, []string{"DELETE FROM lokadb.move_heartbeat"})
	}
	return left, nil
}
