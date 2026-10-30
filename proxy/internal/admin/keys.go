package admin

import "fmt"

// Key kinds (block 17, tasks/uuid-keys.md). The key a refs table maps to the organization is
// one column whose type comes from the catalog: <table>__refs.id is made of that type, and
// nothing in lokadb.* repeats it. A kind is added in keyKindSQL and refsTypeSQL here, KeyKind,
// and the proxy's canonical form of a value (proxy.canonicalKey).
const (
	KindInt  = "int"  // smallint, integer, bigint: the refs table's id is bigint, as before block 17
	KindUUID = "uuid" // uuid
	KindText = "text" // text, varchar under a deterministic collation
	KindChar = "char" // character(n) (bpchar): text whose trailing blanks do not count (Laravel's ulid())
)

// KeyTypes names the types lokadb takes as a key, for messages.
const KeyTypes = "smallint, integer, bigint, uuid, text, varchar or character(n)"

// keyKindSQL: the kind of column a (pg_attribute) as a key, empty for a type lokadb does not take.
// A nondeterministic collation makes different texts equal keys, which the refs table's
// unique index would take as one and the proxy's cache of organizations as two.
const keyKindSQL = `CASE WHEN a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype) THEN 'int'
	WHEN a.atttypid = 'uuid'::regtype THEN 'uuid'
	WHEN a.atttypid IN ('text'::regtype, 'varchar'::regtype)
	  AND coalesce((SELECT co.collisdeterministic FROM pg_collation co WHERE co.oid = a.attcollation), true) THEN 'text'
	WHEN a.atttypid = 'bpchar'::regtype
	  AND coalesce((SELECT co.collisdeterministic FROM pg_collation co WHERE co.oid = a.attcollation), true) THEN 'char'
	ELSE '' END`

// refsTypeSQL: the type of the refs table's id for key column a: bigint for every integer,
// else the column's own type, with its collation when it is not the type's.
const refsTypeSQL = `CASE WHEN a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype) THEN 'bigint'
	ELSE format_type(a.atttypid, a.atttypmod) || coalesce((SELECT ' COLLATE ' || quote_ident(n.nspname) || '.' || quote_ident(co.collname)
	  FROM pg_collation co JOIN pg_namespace n ON n.oid = co.collnamespace
	  WHERE co.oid = a.attcollation AND a.attcollation <> (SELECT ty.typcollation FROM pg_type ty WHERE ty.oid = a.atttypid)), '') END`

// keyColsSQL: what KeySQL and ColumnKeySQL give of key column a: its name, kind, the type of
// the refs table's id, and its own type (what a value is cast to).
const keyColsSQL = `a.attname::text, ` + keyKindSQL + `, ` + refsTypeSQL + `, format_type(a.atttypid, a.atttypmod)`

// KeySQL gives the primary key column of table $1 when the key is one column of a kind lokadb
// takes (no row otherwise): see keyColsSQL. lokadb check reads the same.
const KeySQL = `SELECT ` + keyColsSQL + ` FROM pg_index i
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
	WHERE i.indrelid = $1::regclass AND i.indisprimary AND i.indnkeyatts = 1 AND ` + keyKindSQL + ` <> ''`

// ColumnKeySQL: the same for column $2 of table $1, whatever its type (kind empty for a type
// lokadb does not take); no row when there is no such column.
const ColumnKeySQL = `SELECT ` + keyColsSQL + ` FROM pg_attribute a
	WHERE a.attrelid = $1::regclass AND a.attname = $2 AND a.attnum > 0 AND NOT a.attisdropped`

// KeyKind: the kind of a column of base type base (regtype text: integer, uuid, character
// varying...) as a key, "" for a type lokadb does not take. It does not see the collation:
// KeySQL and ColumnKeySQL also refuse a nondeterministic one.
func KeyKind(base string) string {
	switch base {
	case "smallint", "integer", "bigint":
		return KindInt
	case "uuid":
		return KindUUID
	case "text", "character varying":
		return KindText
	case "character":
		return KindChar
	}
	return ""
}

// keyInfo: the key of a table (tableKey): its column, kind, the type of its refs table's id
// and the column's own type. A composite key (compkey.go) has kind KindComposite and its
// columns, refs types and types as JSON arrays, one per column.
type keyInfo struct {
	col, kind, refsType, typ string
}

// KindComposite: a key of several columns, each of a kind lokadb takes (compkey.go).
const KindComposite = "composite"

// CompositeKeySQL: the columns of table $1's primary key of several columns, in its order, as
// keyColsSQL gives them (a column of a type lokadb does not take has kind ”).
const CompositeKeySQL = `SELECT ` + keyColsSQL + ` FROM pg_index i
	CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, o)
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indrelid = $1::regclass AND i.indisprimary AND i.indnkeyatts > 1 AND k.o <= i.indnkeyatts
	ORDER BY k.o`

// IDUniqueSQL: whether column id of table rel ($1 the same as text) is unique: a unique
// index of it alone, or values that do not repeat. tableKey keys such a table by id, not by
// its composite primary key.
func IDUniqueSQL(rel string) string {
	return fmt.Sprintf(`SELECT (EXISTS (SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
			WHERE i.indrelid = $1::regclass AND i.indisunique AND i.indnkeyatts = 1 AND i.indpred IS NULL AND i.indexprs IS NULL AND a.attname = 'id')
			OR NOT EXISTS (SELECT 1 FROM %s GROUP BY id HAVING count(*) > 1))::text`, rel)
}

// CompositeOf: the key of rows (CompositeKeySQL) as lokadb writes it (a JSON array of the
// columns), or bad: a column of a type lokadb does not take.
func CompositeOf(rows [][]string) (key, bad string) {
	if len(rows) == 0 {
		// no primary key of several columns: no key (it was "null", the JSON of no columns, and
		// check took a table without a primary key and without id for one with a key)
		return "", ""
	}
	k, bad, ok := composite(rows)
	if !ok {
		return "", bad
	}
	return k.col, ""
}

// composite: the key of rows (CompositeKeySQL) as a keyInfo; ok false when a column is of a
// type lokadb does not take (bad names it).
func composite(rows [][]string) (k keyInfo, bad string, ok bool) {
	var cols, refs, types []string
	for _, r := range rows {
		if r[1] == "" {
			return keyInfo{}, r[0] + " (" + r[3] + ")", false
		}
		cols, refs, types = append(cols, r[0]), append(refs, r[2]), append(types, r[3])
	}
	return keyInfo{KeyName(cols), KindComposite, KeyName(refs), KeyName(types)}, "", true
}

// tableKey is the key refs and lookups map to the organization: the table's primary key when
// it is one column of a kind lokadb takes (Pagila's customer_id, a uuid); else id, when it is
// unique across the table (an index, or its values: an id unique beside a composite primary
// key routes WHERE id = $1 as before); else the primary key of several columns (a composite
// key: (account_id, id) numbered per organization, (order_id, line_no)); else, with no primary
// key and no id, a unique index of NOT NULL columns (uniquekey.go: Prisma's many-to-many
// tables). Its values must be unique across organizations: the refs table keeps one row per key.
func (r *runner) tableKey(t string) (keyInfo, error) {
	pk, err := r.rows(KeySQL, qr(t))
	if err != nil {
		return keyInfo{}, err
	}
	if len(pk) == 1 {
		return keyInfo{pk[0][0], pk[0][1], pk[0][2], pk[0][3]}, nil
	}
	comp, err := r.rows(CompositeKeySQL, qr(t))
	if err != nil {
		return keyInfo{}, err
	}
	ck, bad, cok := composite(comp)
	id, err := r.rows(ColumnKeySQL, qr(t), "id")
	if err != nil {
		return keyInfo{}, err
	}
	if len(id) == 1 && id[0][1] != "" && len(comp) > 0 && cok {
		// both: id only if nothing repeats it (organizations numbering their rows each from 1
		// share ids; the composite key tells their rows apart)
		u, err := r.rows(IDUniqueSQL(qr(t)), qr(t))
		if err != nil {
			return keyInfo{}, err
		}
		if u[0][0] == "true" {
			return keyInfo{id[0][0], id[0][1], id[0][2], id[0][3]}, nil
		}
		return ck, nil
	}
	switch {
	case len(id) == 0 && len(comp) > 0 && cok:
		return ck, nil
	case len(id) == 0 && len(comp) > 0:
		return keyInfo{}, fmt.Errorf("%s: its primary key has column %s: lokadb takes keys of type %s (with a deterministic collation)", t, bad, KeyTypes)
	case len(id) == 0:
		return uniqueKey(r.rows, t, qr(t)) // no primary key, no id: a unique index of NOT NULL columns (uniquekey.go)
	case id[0][1] == "":
		return keyInfo{}, fmt.Errorf("%s: its key %s.id is %s: lokadb takes keys of type %s (with a deterministic collation)", t, t, id[0][3], KeyTypes)
	}
	return keyInfo{id[0][0], id[0][1], id[0][2], id[0][3]}, nil
}

// keyOfColumn: what ColumnKeySQL says of column col of table t (enabled before: its key's
// type for the batches of enable -prepare).
func (r *runner) keyOfColumn(t, col string) (keyInfo, error) {
	if Composite(col) { // each of its columns
		var rows [][]string
		for _, c := range KeyCols(col) {
			k, err := r.rows(ColumnKeySQL, qr(t), c)
			if err != nil {
				return keyInfo{}, err
			}
			if len(k) == 0 {
				return keyInfo{}, fmt.Errorf("%s has no column %s", t, c)
			}
			rows = append(rows, k[0])
		}
		k, bad, ok := composite(rows)
		if !ok {
			return keyInfo{}, fmt.Errorf("%s: its key has column %s: lokadb takes keys of type %s", t, bad, KeyTypes)
		}
		return k, nil
	}
	k, err := r.rows(ColumnKeySQL, qr(t), col)
	if err != nil {
		return keyInfo{}, err
	}
	if len(k) == 0 {
		return keyInfo{}, fmt.Errorf("%s has no column %s", t, col)
	}
	return keyInfo{k[0][0], k[0][1], k[0][2], k[0][3]}, nil
}

// relExists: table t (a key) is there.
func (r *runner) relExists(t string) bool {
	ok, err := r.rows(`SELECT (to_regclass($1) IS NOT NULL)::text`, qr(t))
	return err == nil && len(ok) > 0 && ok[0][0] == "true"
}
