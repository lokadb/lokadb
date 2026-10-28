package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The copy move-back leaves in the organization's database (review3 R3-13). markReturned
// changes the mark on its tables from lokadb's to "returned …", and move-org drops only tables
// with lokadb's mark (customerSchema: the leftovers of its own failed attempt). A role that does
// not own the tables cannot change the comment: the copy kept lokadb's mark, and a later
// move-org into the same database dropped it with CASCADE, with whatever the customer had
// built on it. Now the switch records the database's id (lokadb.orgs.db_id, in the mark) in
// lokadb.returned, markReturned clears it once the tables are marked, and move-org refuses
// tables whose mark names a recorded database.

// The comment move-back leaves on the copy, and what tells a copy of organization org from the
// customer's own tables: move-org into the same database drops these itself (customerSchema).
const (
	returnedPrefix = "returned to the cloud by lokadb at "
	returnedSuffix = ": not updated since; the customer may drop it"
)

// isReturnedMark: mark is the comment move-back left on organization org's tables.
func isReturnedMark(mark string, org int64) bool {
	return org > 0 && strings.HasPrefix(mark, returnedPrefix) && strings.HasSuffix(mark, fmt.Sprintf(" (organization %d)%s", org, returnedSuffix))
}

// lokadbTableLike: the SQL LIKE patterns of the comments of tables lokadb made in an organization's
// database that move-org may replace: its own mark and, for org, the one move-back leaves.
func lokadbTableLikes(org int64) (own, returned string) {
	if org <= 0 {
		return TableMark + "%", ""
	}
	return TableMark + "%", fmt.Sprintf("%s%% (organization %d)%s", returnedPrefix, org, returnedSuffix)
}

// returnedSQL: the record (baseSQL has it too; the switch creates it in a database enabled
// before it).
const returnedSQL = `CREATE TABLE IF NOT EXISTS lokadb.returned (db_id text PRIMARY KEY, org bigint NOT NULL, at timestamptz NOT NULL DEFAULT now())`

// returnedSteps: the switch's statements recording organization org's database (dbID "": moved
// before the database id, its tables carry no id to tell them by).
func returnedSteps(org int64, dbID string) []string {
	if dbID == "" {
		return nil
	}
	return []string{returnedSQL, fmt.Sprintf("INSERT INTO lokadb.returned (db_id, org) VALUES (%s, %d) ON CONFLICT (db_id) DO UPDATE SET org = EXCLUDED.org, at = now()", lit(dbID), org)}
}

// clearReturned: the tables are marked as returned, move-org's check of them is enough.
func clearReturned(ctx context.Context, cloudDSN string, org int64, dbID string) error {
	if dbID == "" {
		return nil
	}
	cloud, err := connect(ctx, cloudDSN)
	if err != nil {
		return err
	}
	defer cloud.Close(context.Background())
	_, err = cloud.Exec(ctx, `DELETE FROM lokadb.returned WHERE db_id = $1 AND org = $2`, dbID, org)
	return err
}

// checkReturned: move-org refuses the tables of a copy move-back left unmarked.
func checkReturned(ctx context.Context, cloud, cust *pgx.Conn, tables []moveTable, schema string) error {
	ok, err := rowsOf(ctx, cloud, `SELECT (to_regclass('lokadb.returned') IS NOT NULL)::text`)
	if err != nil || ok[0][0] != "true" {
		return err
	}
	var bad []string
	for _, t := range tables {
		for _, name := range []string{t.name, t.local()} {
			var mark string
			err := cust.QueryRow(ctx, `SELECT coalesce(obj_description(oid, 'pg_class'), '') FROM pg_class
				WHERE relnamespace = to_regnamespace($2) AND relname = $1`, nameOf(name), q(custSchemaOf(name, schema))).Scan(&mark)
			if err == pgx.ErrNoRows {
				continue
			}
			if err != nil {
				return err
			}
			i := strings.Index(mark, " database ")
			if !strings.HasPrefix(mark, TableMark) || i < 0 {
				continue
			}
			rec, err := rowsOf(ctx, cloud, `SELECT org::text, at::text FROM lokadb.returned WHERE db_id = $1`, mark[i+len(" database "):])
			if err != nil {
				return err
			}
			if len(rec) > 0 {
				bad = append(bad, fmt.Sprintf("%s.%s (organization %s, moved back at %s)", custSchemaOf(name, schema), nameOf(name), rec[0][0], rec[0][1]))
			}
		}
	}
	if len(bad) == 0 {
		return nil
	}
	return fmt.Errorf("these tables are the copy move-back left in this database, which it could not mark as returned (its role does not own them, or the comment was refused): %s; "+
		"lokadb does not drop them. Drop or rename them (or mark them: COMMENT ON TABLE … IS 'returned …'), then run move-org again", strings.Join(bad, ", "))
}

// setReturnedAside deals with the copy an earlier move-back left of org in this database (tables:
// quoted, qualified names), in the move's transaction on the organization's database. The copy
// is lokadb's and stale, and the customer may have kept it on purpose: by default it is moved to
// a schema of its own (SET SCHEMA: names, indexes and constraints go with it, nothing is
// dropped) and the move says where; with dropReturned it is dropped, without CASCADE. Either
// way something of the customer's built on it (a view, an FK from another table) stops the move
// with the command that gets past it: aside, such a view would go on reading the stale copy.
func setReturnedAside(ctx context.Context, tx pgx.Tx, tables []string, org int64, dropReturned bool, log *slog.Logger) error {
	list := strings.Join(tables, ", ")
	dep, err := rowsOf(ctx, tx, `WITH t AS (SELECT unnest($1::text[])::regclass AS oid)
		SELECT DISTINCT 'view ' || r.ev_class::regclass::text FROM pg_depend d JOIN pg_rewrite r ON r.oid = d.objid
		  WHERE d.classid = 'pg_rewrite'::regclass AND d.refobjid IN (SELECT oid FROM t) AND r.ev_class NOT IN (SELECT oid FROM t)
		UNION SELECT 'foreign key ' || conname || ' of ' || conrelid::regclass::text FROM pg_constraint
		  WHERE contype = 'f' AND confrelid IN (SELECT oid FROM t) AND conrelid NOT IN (SELECT oid FROM t)
		ORDER BY 1`, tables)
	if err != nil {
		return err
	}
	if len(dep) > 0 {
		var names []string
		for _, d := range dep {
			names = append(names, d[0])
		}
		how := "Drop them, or the copy with them: DROP TABLE " + list + " CASCADE"
		if !dropReturned {
			how += " (or run move-org with -drop-returned once they are gone)"
		}
		return fmt.Errorf("the copy an earlier move-back left of this organization (%s) has objects of the customer's built on it (%s): %s; then run move-org again. "+
			"They would go on reading the stale copy, not the moved tables", list, strings.Join(names, "; "), how)
	}
	if dropReturned {
		if _, err := tx.Exec(ctx, "DROP TABLE "+list); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "2BP01" {
				return fmt.Errorf("the copy an earlier move-back left of this organization (%s) cannot be dropped: %s. DROP TABLE %s CASCADE would", list, strings.TrimSpace(pgErr.Detail), list)
			}
			return err
		}
		log.Info("dropped the copy an earlier move-back left in this database (marked as returned, not updated since)", "tables", list)
		return nil
	}
	aside, err := asideSchema(ctx, tx, org)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "CREATE SCHEMA "+q(aside)); err != nil {
		return fmt.Errorf("the copy an earlier move-back left of this organization (%s) must be moved aside, and schema %s cannot be created (%w); "+
			"drop the copy instead: DROP TABLE %s, or run move-org with -drop-returned", list, aside, err, list)
	}
	for _, t := range tables {
		if _, err := tx.Exec(ctx, fmt.Sprintf("ALTER TABLE %s SET SCHEMA %s", t, q(aside))); err != nil {
			return fmt.Errorf("the copy an earlier move-back left (%s) cannot be moved aside to schema %s (%w); drop it: DROP TABLE %s, or run move-org with -drop-returned", t, aside, err, list)
		}
	}
	log.Warn("the copy an earlier move-back left of this organization in this database (marked as returned, not updated since) is moved aside, not dropped; "+
		"the moved tables are new. Drop the old copy when it is not needed: DROP SCHEMA "+q(aside)+" CASCADE",
		"tables", list, "now_in_schema", aside)
	return nil
}

// asideSchema: a name for the schema the copy of org moves to, not taken.
func asideSchema(ctx context.Context, tx pgx.Tx, org int64) (string, error) {
	base := fmt.Sprintf("lokadb_returned_%d_%s", org, time.Now().UTC().Format("20060102t150405"))
	for i := 0; ; i++ {
		name := base
		if i > 0 {
			name = fmt.Sprintf("%s_%d", base, i+1)
		}
		r, err := rowsOf(ctx, tx, `SELECT (to_regnamespace($1) IS NOT NULL)::text`, q(name))
		if err != nil {
			return "", err
		}
		if r[0][0] != "true" {
			return name, nil
		}
	}
}
