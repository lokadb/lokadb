package proxy

import (
	"encoding/hex"
	"sort"
	"strings"
)

// Keys of any kind (block 17, tasks/uuid-keys.md). A refs table maps a key to its
// organization, and the key's type comes from the catalog: integers, uuid, text and varchar
// (admin/keys.go). The proxy reads a value of a statement as the key's kind and keeps it in
// one canonical form, the one the refs table gives back (id::text), so that the organization
// cache finds '007' as 7 and 'A0EEBC99…' or '{a0eebc99…}' as a0eebc99-….
const (
	kindInt  = "int"  // smallint, integer, bigint; the organization column too
	kindUUID = "uuid" // uuid
	kindText = "text" // text, varchar: as written
	kindChar = "char" // character(n): text without the blanks it is padded with (Laravel's ulid())
	kindRaw  = ""     // not a key: a literal as it is, a binary integer decoded (lokadb.tenant)
)

// keyKindOf: the kind of a column of type typ (format_type: "uuid", "character varying(36)",
// "bigint"), "" for a type the proxy does not read as a key.
func keyKindOf(typ string) string {
	base, _, _ := strings.Cut(typ, "(")
	switch strings.TrimSpace(base) {
	case "smallint", "integer", "bigint":
		return kindInt
	case "uuid":
		return kindUUID
	case "text", "character varying":
		return kindText
	case "character":
		return kindChar
	}
	return ""
}

// keyKind: the kind of the values refs maps (Meta.KeyKinds); the organization column ("")
// by Meta.TenantKind (block 18); anything unknown (metadata of an older lokadb) are integers,
// as before block 17.
func (m *Meta) keyKind(refs string) string {
	if m != nil && refs != "" {
		if k, ok := m.KeyKinds[refs]; ok {
			return k
		}
	}
	if m != nil && refs == "" {
		switch m.TenantKind {
		case kindUUID:
			return kindUUID
		case kindText: // a string organization: character(n) pads it, so its blanks do not count
			return kindChar
		}
	}
	return kindInt
}

// valueOrgs: the database's organizations are named by their value, a uuid or a string, and
// numbered apart (block 18): the pause lock takes the value as text (lokadb.org_lock_v).
func (m *Meta) valueOrgs() bool {
	return m != nil && (m.TenantKind == kindUUID || m.TenantKind == kindText)
}

// realOrg: org is one organization, not "" (every one), -1 (none found) or -2 (rows of none).
// An integer organization is never negative, a uuid never starts with a minus.
func realOrg(org string) bool { return org != "" && !strings.HasPrefix(org, "-") }

// orgNum: the number N of organization org (a session's organization is its value): an
// integer one's is itself, one numbered by uuid has it from lokadb.tenants (OrgNum), "" when
// it has none. lokadb.orgs, Profiles, Schemas and the proxy's config go by it.
func (m *Meta) orgNum(org string) string {
	if m == nil {
		return org
	}
	if n, ok := m.OrgNum[org]; ok {
		return n
	}
	if !m.valueOrgs() && intRe.MatchString(org) {
		return org
	}
	return ""
}

// orgLit: organization org as an SQL literal of table t's organization column: an integer as
// it is, a value (a uuid, a string: a numeric-looking one too) quoted.
func (t *Table) orgLit(org string) string {
	if t != nil && t.OrgValue || !intRe.MatchString(org) {
		return sqlString(org)
	}
	return org
}

// loadOrgNums reads the numbers of the organizations named by their value (Meta.OrgNum).
func loadOrgNums(q queryFunc, m *Meta) error {
	m.OrgNum = map[string]string{}
	if !m.valueOrgs() {
		return nil
	}
	ok, err := q(`SELECT (to_regclass('lokadb.tenants') IS NOT NULL)::text`)
	if err != nil || len(ok) == 0 || ok[0][0] != "true" {
		return err
	}
	rows, err := q(`SELECT tenant, org::text FROM lokadb.tenants`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		m.OrgNum[r[0]] = r[1]
	}
	return nil
}

// canonicalKey: x read as a value of kind, in the form the refs tables give back; false when
// it is not one (Postgres refuses such a literal for the column as well).
func canonicalKey(kind, x string) (string, bool) {
	switch kind {
	case kindInt:
		if !intRe.MatchString(x) {
			return "", false
		}
		return canonicalInt(x), true
	case kindUUID:
		return canonicalUUID(x)
	case kindChar:
		return strings.TrimRight(x, " "), true
	}
	return x, true
}

// canonicalUUID reads a uuid as Postgres does (uuid_in): 32 hex digits in either case, a
// hyphen allowed after any group of four of them, the whole optionally in braces; it gives the
// form Postgres prints (lower case, 8-4-4-4-12).
func canonicalUUID(s string) (string, bool) {
	braces := strings.HasPrefix(s, "{")
	if braces {
		s = s[1:]
	}
	var b [16]byte
	for i := 0; i < 16; i++ {
		if len(s) < 2 || !isHex(s[0]) || !isHex(s[1]) {
			return "", false
		}
		b[i] = hexVal(s[0])<<4 | hexVal(s[1])
		s = s[2:]
		if len(s) > 0 && s[0] == '-' && i%2 == 1 && i < 15 {
			s = s[1:]
		}
	}
	if braces {
		if !strings.HasPrefix(s, "}") {
			return "", false
		}
		s = s[1:]
	}
	if s != "" {
		return "", false
	}
	return formatUUID(b[:]), true
}

// formatUUID: 16 bytes (uuid_send) as Postgres prints them.
func formatUUID(b []byte) string {
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

func isHex(c byte) bool { return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F' }

func hexVal(c byte) byte {
	switch {
	case c >= 'a':
		return c - 'a' + 10
	case c >= 'A':
		return c - 'A' + 10
	}
	return c - '0'
}

// binaryKey reads a parameter in binary form as a value of kind: an integer by its length,
// a uuid of 16 bytes, text as its bytes.
func binaryKey(kind string, b []byte) (string, bool) {
	switch kind {
	case kindUUID:
		if len(b) != 16 {
			return "", false
		}
		return formatUUID(b), true
	case kindText:
		return string(b), true
	case kindChar:
		return strings.TrimRight(string(b), " "), true
	}
	return binaryInt(b)
}

// loadKeyKinds fills m.KeyKinds from what readMeta has read: the refs table of each LokaDB
// table by the type of its key, the map of a declared key (routing.keys) by its column's, and
// the id of a declared cloud table from the catalog.
func loadKeyKinds(q queryFunc, m *Meta) error {
	m.KeyKinds, m.TenantKind = map[string]string{}, kindInt
	for _, t := range m.Tables {
		switch keyKindOf(t.ColType[t.TenantCol]) { // one kind for every table (enable)
		case kindUUID:
			m.TenantKind = kindUUID
		case kindText, kindChar:
			m.TenantKind = kindText
		}
	}
	for _, t := range m.Tables {
		t.OrgValue = m.valueOrgs()
	}
	m.TupleKinds = map[string][]string{}
	for _, t := range m.Tables {
		key := t.Key
		if key == "" {
			key = "id"
		}
		if k := keyKindOf(t.ColType[key]); k != "" && t.Refs != "" {
			m.KeyKinds[t.Refs] = k
		}
		if cols, ok := tupleCols(key); ok && t.Refs != "" { // a composite key: each column's kind
			var kinds []string
			for _, c := range cols {
				k := keyKindOf(t.ColType[c])
				if k == "" {
					k = kindText
				}
				kinds = append(kinds, k)
			}
			m.KeyKinds[t.Refs], m.TupleKinds[t.Refs] = kindTuple, kinds
		}
		for col, refs := range m.Lookups[t.Name] {
			if _, ok := keyRefs(refs); ok { // a key's map holds col::text
				k := keyKindOf(t.ColType[col])
				if k == "" {
					k = kindText
				}
				m.KeyKinds[refs] = k
			}
		}
	}
	// the map of a unique column other than the key (block 4, item 2): that column's kind
	unknown := false // a lookup through a table that is no refs table: a unique column's map
	for _, cols := range m.Lookups {
		for _, refs := range cols {
			if _, known := m.KeyKinds[refs]; refs != "" && !known && m.Cloud[refs] == "" {
				unknown = true
			}
		}
	}
	if !unknown {
	} else if ok, err := q(`SELECT (to_regclass('lokadb.unique_refs') IS NOT NULL)::text`); err != nil {
		return err
	} else if len(ok) > 0 && ok[0][0] == "true" {
		rows, err := q(`SELECT table_name, column_name, refs_table FROM lokadb.unique_refs`)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if t := m.Tables[r[0]]; t != nil {
				if k := keyKindOf(t.ColType[r[1]]); k != "" {
					m.KeyKinds[r[2]] = k
				}
			}
		}
	}
	var clouds []string
	for t := range m.Cloud {
		clouds = append(clouds, t)
	}
	if len(clouds) == 0 {
		return nil
	}
	sort.Strings(clouds)
	for i, t := range clouds {
		clouds[i] = sqlString(t)
	}
	rows, err := q(`SELECT ` + keyOf("c") + `, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		WHERE ` + keyOf("c") + ` IN (` + strings.Join(clouds, ", ") + `) AND a.attname = 'id' AND NOT a.attisdropped`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if k := keyKindOf(r[1]); k != "" {
			m.KeyKinds[r[0]] = k
		}
	}
	return nil
}
