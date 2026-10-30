package admin

// The DDL move-org sends to the organization's database is written out by the cloud's
// pg_get_indexdef and pg_get_constraintdef, in the cloud's version of the syntax. A Postgres
// that is older than the cloud's cannot run some of it: UNIQUE NULLS NOT DISTINCT (15), and from
// 18 virtual generated columns, NOT ENFORCED constraints and WITHOUT OVERLAPS keys; before 14
// multirange columns and functions with a SQL-standard body (block 11). Foreign
// keys with ON DELETE SET NULL (columns) (15) are written another way (describe). What cannot
// be written another way is a refusal before the move changes anything, here and in lokadb check.

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
)

// serverVersionNum: server_version_num of the connection's server.
func serverVersionNum(ctx context.Context, c *pgx.Conn) (int, error) {
	var n int
	err := c.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&n)
	return n, err
}

// CustomerSyntaxProblems: what the cloud's tables (names as in the spec) have that a server of
// version custVer (server_version_num) cannot create, one sentence each; empty when none.
func CustomerSyntaxProblems(ctx context.Context, cloud querier, tables []string, custVer int) ([]string, error) {
	cv, err := rowsOf(ctx, cloud, "SELECT current_setting('server_version_num')")
	if err != nil {
		return nil, err
	}
	var cloudVer int
	fmt.Sscan(cv[0][0], &cloudVer)
	if custVer == 0 || custVer >= cloudVer {
		return nil, nil
	}
	var rels []string
	for _, t := range tables {
		rels = append(rels, qr(t), qr(hn(t, "__cloud")))
	}
	var out []string
	add := func(rows [][]string, what, need string) { // what: "x on" ... names ... (PostgreSQL N or later)
		var names []string
		for _, r := range rows {
			names = append(names, r[0])
		}
		if len(names) > 0 {
			out = append(out, fmt.Sprintf("%s %s (the cloud's PostgreSQL has them, the organization's needs version %s or later for them)", what, strings.Join(names, ", "), need))
		}
	}
	if custVer < 150000 && cloudVer >= 150000 {
		r, err := rowsOf(ctx, cloud, `SELECT c.relname::text || '.' || i.indexrelid::regclass::text FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
			WHERE i.indrelid = ANY (SELECT to_regclass(r) FROM unnest($1::text[]) r WHERE to_regclass(r) IS NOT NULL) AND i.indnullsnotdistinct`, rels)
		if err != nil {
			return nil, err
		}
		add(r, "UNIQUE NULLS NOT DISTINCT:", "15")
	}
	if custVer < 180000 && cloudVer >= 180000 {
		r, err := rowsOf(ctx, cloud, `SELECT a.attrelid::regclass::text || '.' || a.attname FROM pg_attribute a
			WHERE a.attrelid = ANY (SELECT to_regclass(r) FROM unnest($1::text[]) r WHERE to_regclass(r) IS NOT NULL) AND a.attgenerated = 'v' AND NOT a.attisdropped`, rels)
		if err != nil {
			return nil, err
		}
		add(r, "virtual generated columns:", "18")
		r, err = rowsOf(ctx, cloud, `SELECT c.conrelid::regclass::text || '.' || c.conname FROM pg_constraint c
			WHERE c.conrelid = ANY (SELECT to_regclass(r) FROM unnest($1::text[]) r WHERE to_regclass(r) IS NOT NULL)
			  AND (NOT c.conenforced OR c.conperiod)`, rels)
		if err != nil {
			return nil, err
		}
		add(r, "NOT ENFORCED constraints or WITHOUT OVERLAPS keys:", "18")
	}
	if custVer < 140000 && cloudVer >= 140000 {
		// multirange types (a column, an array of them, a domain over one) and the application's
		// functions with a SQL-standard body (BEGIN ATOMIC, RETURN), which customerDeps creates
		// there by pg_get_functiondef
		r, err := rowsOf(ctx, cloud, `SELECT a.attrelid::regclass::text || '.' || a.attname FROM pg_attribute a
			JOIN pg_type t ON t.oid = a.atttypid LEFT JOIN pg_type e ON e.oid = t.typelem LEFT JOIN pg_type b ON b.oid = t.typbasetype
			WHERE a.attrelid = ANY (SELECT to_regclass(r) FROM unnest($1::text[]) r WHERE to_regclass(r) IS NOT NULL) AND a.attnum > 0
			  AND NOT a.attisdropped AND 'm' IN (t.typtype, e.typtype, b.typtype)`, rels)
		if err != nil {
			return nil, err
		}
		add(r, "multirange columns:", "14")
		var oids []string
		for _, t := range tables {
			ds, err := Dependencies(ctx, cloud, t)
			if err != nil {
				return nil, err
			}
			for _, d := range ds {
				if d.Kind == "function" && d.Ext == "" && !slices.Contains(oids, d.OID) {
					oids = append(oids, d.OID)
				}
			}
		}
		if len(oids) > 0 {
			r, err := rowsOf(ctx, cloud, `SELECT oid::regprocedure::text FROM pg_proc WHERE oid = ANY ($1::oid[]) AND prosqlbody IS NOT NULL ORDER BY 1`, oids)
			if err != nil {
				return nil, err
			}
			add(r, "functions with a SQL-standard body (BEGIN ATOMIC or RETURN), which move-org creates there as the cloud defines them:", "14")
		}
	}
	return out, nil
}

// OtherOrgTables: the tables in the organization's database that lokadb made for another
// organization (their mark names it): "schema.table (organization N)". The database holds one
// organization of the cloud: replaced by this one's, such a table would be dropped with the
// only copy of its rows (R38). The tables of this organization (a failed attempt's, a moved one's)
// are not listed. org 0: all of them.
func OtherOrgTables(ctx context.Context, cust querier, org int64) ([]string, error) {
	rows, err := rowsOf(ctx, cust, `SELECT n.nspname::text || '.' || c.relname::text, obj_description(c.oid, 'pg_class')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p', 'f') AND NOT c.relispartition
		  AND coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $1 ORDER BY 1`, TableMark+" organization %")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		if other := markedOrg(r[1]); other > 0 && other != org {
			out = append(out, fmt.Sprintf("%s (organization %d)", r[0], other))
		}
	}
	return out, nil
}

// otherOrgMessage: what move-org and check say about OtherOrgTables.
func OtherOrgMessage(tables []string) string {
	return fmt.Sprintf("this database already holds another organization of the cloud (%s): one database takes one organization, and moving this one in would drop that one's tables with their only rows. "+
		"Use another database for this organization (if these are the leftovers of a move that failed and the organization is in the cloud, drop them by hand)", strings.Join(tables, ", "))
}
