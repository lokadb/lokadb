package admin

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// The organization's database id (review A1, with block 8's check of the database, R3). The
// mark move-org puts on the organization's tables names the organization; a copy of the
// database (a restore, a replica, the old primary after a failover) carries the same mark, so
// the proxy's check told the organization's database from another organization's, not from
// its own old one. move-org and repoint add a random id of the database to the mark and record
// it in lokadb.orgs.db_id; after repoint the old database keeps the old id and the check fails
// there. NULL: not recorded (moved before it, or repoint could not mark the tables).

// dbIDSQL adds the column to a database enabled before it (baseSQL has it too).
const dbIDSQL = `ALTER TABLE lokadb.orgs ADD COLUMN IF NOT EXISTS db_id text`

// newDBID: a random database id, 16 hexadecimal digits.
func newDBID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// orgMark: the comment on an organization's table in its database ("" id: none).
func orgMark(org int64, id string) string {
	m := fmt.Sprintf("%s organization %d", TableMark, org)
	if id != "" {
		m += " database " + id
	}
	return m
}

// isOrgMark: m is organization org's mark, with a database id or without.
func isOrgMark(m string, org int64) bool {
	base := orgMark(org, "")
	return m == base || strings.HasPrefix(m, base+" database ")
}

// markedOrg: the organization whose mark m is (with or without a database id), 0 if m is none's.
func markedOrg(m string) int64 {
	var org int64
	var rest string
	if n, _ := fmt.Sscanf(m, TableMark+" organization %d%s", &org, &rest); n >= 1 && (isOrgMark(m, org)) {
		return org
	}
	return 0
}

// markedDBID: the database id in the mark m ("" if it has none).
func markedDBID(m string) string {
	if i := strings.Index(m, " database "); i >= 0 {
		return strings.TrimSpace(m[i+len(" database "):])
	}
	return ""
}

// The ids this cloud gave the tables of its move-org attempts at the customer are in lokadb.attempts
// (baseSQL; the switch moves the id to lokadb.orgs). A table at the customer marked with this
// organization's number but an id the cloud never gave is another cloud's (a second install, this
// database restored from a backup, a staging copy): the same number does not make it this cloud's
// leftover.

// attemptIDs: the ids this cloud knows for organization org's tables at the customer: those of its
// attempts, the one of the organization's database once moved (lokadb.orgs) and of a copy
// move-back left there (lokadb.returned). A database enabled
// by an older version has no lokadb.attempts yet (move-org makes it).
func attemptIDs(ctx context.Context, cloud querier, org int64) (map[string]bool, error) {
	sql := `SELECT coalesce(to_jsonb(o) ->> 'db_id', '') FROM lokadb.orgs o WHERE org = $1`
	ok, err := rowsOf(ctx, cloud, `SELECT (to_regclass('lokadb.attempts') IS NOT NULL)::text, (to_regclass('lokadb.returned') IS NOT NULL)::text`)
	if err != nil {
		return nil, err
	}
	if ok[0][0] == "true" {
		sql += ` UNION SELECT db_id FROM lokadb.attempts WHERE org = $1`
	}
	if ok[0][1] == "true" { // a copy move-back left there (checkReturned has its say about it)
		sql += ` UNION SELECT db_id FROM lokadb.returned WHERE org = $1`
	}
	rows, err := rowsOf(ctx, cloud, sql, org)
	if err != nil {
		return nil, err
	}
	known := map[string]bool{}
	for _, r := range rows {
		if r[0] != "" {
			known[r[0]] = true
		}
	}
	return known, nil
}

// ForeignDBTables: the tables in the organization's database marked with this organization's
// number and a database id the cloud did not give (known: its attempts' ids): "schema.table
// (database id)". Tables without an id (made before it) are taken as the cloud's own.
func ForeignDBTables(ctx context.Context, cust querier, org int64, known map[string]bool) ([]string, error) {
	rows, err := rowsOf(ctx, cust, `SELECT n.nspname::text || '.' || c.relname::text, obj_description(c.oid, 'pg_class')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relkind IN ('r', 'p', 'f') AND NOT c.relispartition
		  AND coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $1 ORDER BY 1`, TableMark+" organization %")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		if id := markedDBID(r[1]); id != "" && isOrgMark(r[1], org) && !known[id] {
			out = append(out, fmt.Sprintf("%s (database %s)", r[0], id))
		}
	}
	return out, nil
}

// ForeignDBMessage: what move-org says about ForeignDBTables.
func ForeignDBMessage(org int64, tables []string) string {
	return fmt.Sprintf("this database holds tables of organization %d that this cloud database did not make (%s): they belong to another LokaDB database (a second install, this one restored from a backup, a staging copy) and may hold the only copy of the organization's rows, "+
		"so they are not replaced. Use another database for this organization (if they are the leftovers of a move that failed in a database that is gone, drop them by hand)", org, strings.Join(tables, ", "))
}

// nullLit: s as a literal, NULL for "".
func nullLit(s string) string {
	if s == "" {
		return "NULL"
	}
	return lit(s)
}

// withDBID: the DDL of describe with the marks given the database id.
func withDBID(ddl []string, org int64, id string) []string {
	plain, marked := lit(orgMark(org, "")), lit(orgMark(org, id))
	out := make([]string, len(ddl))
	for i, s := range ddl {
		if strings.HasPrefix(s, "COMMENT ON TABLE ") && strings.HasSuffix(s, " IS "+plain) {
			s = strings.TrimSuffix(s, plain) + marked
		}
		out[i] = s
	}
	return out
}
