package admin

import (
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
