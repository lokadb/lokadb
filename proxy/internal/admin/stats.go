package admin

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// What the cloud keeps of a moved organization's rows by itself. ANALYZE of a foreign table stores
// what it samples in the cloud's pg_statistic: up to default_statistics_target values of each
// column, real values, in a database that was promised none of them (XT13). So the columns of a
// foreign partition keep no statistics (SET STATISTICS 0), and ANALYZE of it writes only its row
// count, which is what the planner needs for ORDER BY and LIMIT to reach the organization (F12).
// The parent too: ANALYZE of it, or a database-wide one, samples its foreign partitions for its own
// statistics; Postgres keeps none for it unless someone runs one of those. Its local partitions
// keep theirs. On Postgres 18 a foreign partition also gets the numbers without values of the
// organization's own statistics (StatNumbers).

// statsSQL: part of baseSQL, and all stats-sync needs of it in a database enabled by an older version.
const statsSQL = `
-- no_stat_values(r): no column of r keeps statistics. Only of what the role owns: a migration runs
-- it as the application's role. A virtual generated column has none, and refuses the setting.
CREATE OR REPLACE FUNCTION lokadb.no_stat_values(r regclass) RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  kind "char";
  col name;
BEGIN
  SELECT relkind INTO kind FROM pg_class WHERE oid = r AND pg_has_role(current_user, relowner, 'USAGE');
  IF NOT FOUND THEN RETURN; END IF;
  FOR col IN SELECT attname FROM pg_attribute WHERE attrelid = r AND attnum > 0 AND NOT attisdropped AND attgenerated <> 'v' AND coalesce(attstattarget::int, -1) <> 0 LOOP
    EXECUTE format('ALTER %s %s ALTER COLUMN %I SET STATISTICS 0', CASE kind WHEN 'f' THEN 'FOREIGN TABLE' ELSE 'TABLE ONLY' END, r, col);
  END LOOP;
END $$;
-- stats_no_values(): every LokaDB table and its foreign partitions. Takes no lock that stops
-- reads or writes (SHARE UPDATE EXCLUSIVE, and only where a column is not set yet); another
-- transaction's DDL or LOCK TABLE can make it wait, so the caller sets lock_timeout. A table it
-- could not lock is left for the next call, the rest are done. The number of tables left.
CREATE OR REPLACE FUNCTION lokadb.stats_no_values() RETURNS integer LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  p regclass;
  c regclass;
  left_out integer := 0;
BEGIN
  FOR p IN SELECT to_regclass(lokadb.qrel(table_name)) FROM lokadb.tables LOOP
    CONTINUE WHEN p IS NULL;
    BEGIN
      PERFORM lokadb.no_stat_values(p);
      FOR c IN SELECT i.inhrelid::regclass FROM pg_inherits i JOIN pg_class k ON k.oid = i.inhrelid WHERE i.inhparent = p AND k.relkind = 'f' LOOP
        PERFORM lokadb.no_stat_values(c);
      END LOOP;
    EXCEPTION WHEN lock_not_available THEN
      left_out := left_out + 1;
    END;
  END LOOP;
  RETURN left_out;
END $$;
`

// statsLockTimeout: how long stats-sync and schema-sync wait for a table's lock.
const statsLockTimeout = 5 * time.Second

// withLockTimeout runs f with lock_timeout set on c: what is done to statistics is never worth a
// command hanging behind another transaction's lock on a table (MV1).
func withLockTimeout(ctx context.Context, c *pgx.Conn, d time.Duration, f func() error) error {
	if d <= 0 {
		d = statsLockTimeout
	}
	if _, err := c.Exec(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", d.Milliseconds())); err != nil {
		return err
	}
	defer func() { _, _ = c.Exec(ctx, "RESET lock_timeout") }()
	return f()
}

// statsNoValues: no column of a LokaDB table or of its foreign partitions keeps statistics from
// now on. The number of tables left because another transaction holds them.
func statsNoValues(ctx context.Context, cloud *pgx.Conn) (int, error) {
	var left int
	err := cloud.QueryRow(ctx, "SELECT lokadb.stats_no_values()").Scan(&left)
	return left, err
}

// StatsNoValues is statsNoValues with the functions made first, for a database enabled by an
// older version (stats-sync clears what is there already).
func StatsNoValues(ctx context.Context, cloud *pgx.Conn) (int, error) {
	if _, err := cloud.Exec(ctx, statsSQL); err != nil {
		return 0, err
	}
	var left int
	err := withLockTimeout(ctx, cloud, statsLockTimeout, func() (err error) {
		left, err = statsNoValues(ctx, cloud)
		return err
	})
	return left, err
}

// noStatValuesForMove is statsNoValues for a move: it must not stop one. A table left is set at
// the end of the move, or by stats-sync.
func noStatValuesForMove(ctx context.Context, cloud *pgx.Conn, d time.Duration, log *slog.Logger) {
	if err := withLockTimeout(ctx, cloud, d, func() error {
		left, err := statsNoValues(ctx, cloud)
		if err == nil && left > 0 {
			err = fmt.Errorf("%d tables are held by another transaction", left)
		}
		return err
	}); err != nil {
		log.Warn("not every LokaDB table is set to keep no values in its statistics yet (lokadb stats-sync does it)", "err", err)
	}
}

// statsFuncs: Postgres 18's functions that write statistics, open to a table's owner.
func statsFuncs(ctx context.Context, c *pgx.Conn) bool {
	var v int
	err := c.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&v)
	return err == nil && v >= 180000
}

const regclassOf = `(quote_ident($1::text) || '.' || quote_ident($2::text))::regclass`

// ClearStats removes what the cloud's statistics hold of relation key (of a partitioned table,
// inherited: of it as a whole). False: it could not. Before Postgres 18 only a superuser can.
// The rows removed stay in pg_statistic's pages until it is vacuumed, and in backups made before.
func ClearStats(ctx context.Context, c *pgx.Conn, key string, inherited bool) (bool, error) {
	s, n := SplitKey(key)
	if statsFuncs(ctx, c) {
		_, err := c.Exec(ctx, `SELECT pg_clear_attribute_stats($1::text, $2::text, a.attname, $3::boolean)
			FROM pg_attribute a WHERE a.attrelid = `+regclassOf+` AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> 'v'`, s, n, inherited)
		return err == nil, err
	}
	var super bool
	if err := c.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&super); err != nil {
		return false, err
	}
	if !super {
		return false, nil
	}
	_, err := c.Exec(ctx, `DELETE FROM pg_statistic WHERE starelid = `+regclassOf+` AND stainherit = $3::boolean`, s, n, inherited)
	return err == nil, err
}

// HoldsStatValues: the statistics of relation key, as this role can see them, hold values.
func HoldsStatValues(ctx context.Context, c *pgx.Conn, key string) (bool, error) {
	s, n := SplitKey(key)
	var holds bool
	err := c.QueryRow(ctx, `SELECT EXISTS (SELECT FROM pg_stats WHERE schemaname = $1 AND tablename = $2
		AND (most_common_vals IS NOT NULL OR histogram_bounds IS NOT NULL OR most_common_elems IS NOT NULL))`, s, n).Scan(&holds)
	return holds, err
}

// StatNumbers gives foreign partition key, on Postgres 18, what the planner needs of its columns
// without a value from them: the share of NULLs, the average width and the number of distinct
// values, as the organization's own statistics of schema.table have them. Without them a statement
// by id is taken for 25 rows of a million and 2500 of a hundred million, which at some size turns a
// point read joined to a shared table into a scan of that table; with them the plans of the 12 forms
// measured on 6.11 are the ones full statistics give. Before Postgres 18 a role cannot write these,
// and the planner goes by its defaults. The number of columns given.
func StatNumbers(ctx context.Context, cloud, cust *pgx.Conn, key, schema, table string) (int, error) {
	if !statsFuncs(ctx, cloud) {
		return 0, nil
	}
	rows, err := rowsOf(ctx, cust, `SELECT attname::text, null_frac::text, avg_width::text, n_distinct::text FROM pg_stats
		WHERE schemaname = $1 AND tablename = $2 AND NOT inherited`, schema, table)
	if err != nil {
		return 0, err
	}
	s, n := SplitKey(key)
	given := 0
	for _, r := range rows {
		var ok bool
		err := cloud.QueryRow(ctx, `SELECT pg_restore_attribute_stats('schemaname', $1::text, 'relname', $2::text, 'attname', $3::text, 'inherited', false,
			'null_frac', $4::real, 'avg_width', $5::int, 'n_distinct', $6::real)`, s, n, r[0], r[1], r[2], r[3]).Scan(&ok)
		if err != nil {
			return given, fmt.Errorf("column %s: %w", r[0], err)
		}
		if ok {
			given++
		}
	}
	return given, nil
}

// CleanCloudPartition: the statistics of table's DEFAULT partition hold values of an organization
// from before its move until they are made again. ANALYZE makes them again from the rows there; of
// no rows it makes nothing and keeps the old, so those are removed. False: they could not be. The
// caller sets lock_timeout.
func CleanCloudPartition(ctx context.Context, cloud *pgx.Conn, table string) (string, bool, error) {
	var part string
	if err := cloud.QueryRow(ctx, `SELECT cloud_partition FROM lokadb.tables WHERE table_name = $1`, table).Scan(&part); err != nil {
		return "", false, err
	}
	var rows bool
	if err := cloud.QueryRow(ctx, "SELECT EXISTS (SELECT FROM "+qr(part)+")").Scan(&rows); err != nil {
		return part, false, err
	}
	if rows {
		_, err := cloud.Exec(ctx, "ANALYZE "+qr(part))
		return part, err == nil, err
	}
	ok, err := ClearStats(ctx, cloud, part, false)
	return part, ok, err
}

// analyzeForeign gives the organization's new foreign partitions their statistics. Postgres
// keeps none for a foreign table until it is analyzed by name (autovacuum and a database-wide
// ANALYZE skip it), and without them the planner takes the organization for a handful of rows:
// postgres_fdw then sends neither ORDER BY nor LIMIT to the organization's database, and a feed
// (ORDER BY id DESC LIMIT 20) fetched every row of the organization (Z2 of block 14's trial). A
// failure is only logged: the move is done, the statistics can be made later (stats-sync). When
// a partition's columns cannot be set, it is not analyzed: slower plans, not values in the cloud.
func analyzeForeign(ctx context.Context, cloud, cust *pgx.Conn, o MoveOptions, tables []moveTable, log *slog.Logger) {
	noStatValuesForMove(ctx, cloud, o.LockTimeout, log)
	err := withLockTimeout(ctx, cloud, o.LockTimeout, func() error {
		for _, t := range tables {
			t0 := time.Now()
			schema, table := custSchemaOf(t.name, o.CustomerSchema), nameOf(t.name)
			if _, err := cloud.Exec(ctx, fmt.Sprintf("SELECT lokadb.no_stat_values(%s::regclass)", lit(qr(t.foreign())))); err != nil {
				log.Warn("the foreign partition is not analyzed: its columns could not be set to keep no values (lokadb stats-sync does both)", "table", t.foreign(), "err", err)
				continue
			}
			// its own statistics first: ANALYZE of the foreign table samples by them instead of
			// reading every row, and the numbers come from them
			if _, err := cust.Exec(ctx, "ANALYZE "+q(schema)+"."+q(table)); err != nil {
				log.Warn("ANALYZE in the organization's database failed", "table", t.name, "err", err)
			}
			if _, err := cloud.Exec(ctx, "ANALYZE "+qr(t.foreign())); err != nil {
				log.Warn("ANALYZE of the foreign partition failed: the planner has no statistics of it until lokadb stats-sync", "table", t.foreign(), "err", err)
				continue
			}
			numbers, err := StatNumbers(ctx, cloud, cust, t.foreign(), schema, table)
			if err != nil {
				log.Warn("the numbers of the organization's statistics did not reach the foreign partition: the planner goes by its defaults until lokadb stats-sync", "table", t.foreign(), "err", err)
			}
			log.Info("statistics of the foreign partition", "table", t.foreign(), "columns", numbers, "took", time.Since(t0).Round(time.Millisecond))
			if part, ok, err := CleanCloudPartition(ctx, cloud, t.name); err != nil || !ok {
				log.Warn("the statistics of the cloud's partition still hold values from before the move (removing them when it is empty needs Postgres 18 or a superuser)", "table", part, "err", err)
			}
		}
		return nil
	})
	if err != nil {
		log.Warn("the foreign partitions are not analyzed (lokadb stats-sync does it)", "err", err)
	}
}
