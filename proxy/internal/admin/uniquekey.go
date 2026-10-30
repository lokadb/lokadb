package admin

import "fmt"

// A table without a primary key and without an id column: Prisma's implicit many-to-many
// tables ("_DocumentToTag": columns A and B, a UNIQUE (A, B) index and an index of B) are in
// nearly every Prisma application. A unique index of NOT NULL columns identifies a row as a
// primary key does: it is the table's key then (refs, the split, a move and its way back go by
// the key; at the organization it becomes the table's primary key). One whose columns allow
// NULL does not: two rows with a NULL are both there.

// uniqueIndexSQL: the unique index of table $1 that serves as its key: no predicate, no
// expression, not deferrable; of NOT NULL columns ($2 true) or with a column that allows NULL
// ($2 false). The fewest columns first, then the oldest.
const uniqueIndexSQL = `SELECT i.indexrelid FROM pg_index i
	WHERE i.indrelid = $1::regclass AND i.indisunique AND NOT i.indisprimary AND i.indimmediate AND i.indisvalid
	  AND i.indpred IS NULL AND i.indexprs IS NULL
	  AND $2::bool = NOT EXISTS (SELECT 1 FROM unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, o)
	    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	    WHERE k.o <= i.indnkeyatts AND NOT a.attnotnull)
	ORDER BY i.indnkeyatts, i.indexrelid LIMIT 1`

// UniqueKeySQL: the columns of that index of NOT NULL columns, in its order, as keyColsSQL
// gives them (a column of a type lokadb does not take has kind ”). No row when there is none.
const UniqueKeySQL = `SELECT ` + keyColsSQL + ` FROM pg_index i
	CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, o)
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indexrelid = (` + uniqueIndexSQL + `) AND k.o <= i.indnkeyatts
	ORDER BY k.o`

// nullableUniqueSQL: the name of a unique index of table $1 with a column that allows NULL,
// and that column, for the refusal.
const nullableUniqueSQL = `SELECT i.indexrelid::regclass::text, a.attname::text FROM pg_index i
	CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY k(attnum, o)
	JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
	WHERE i.indexrelid = (` + uniqueIndexSQL + `) AND k.o <= i.indnkeyatts AND NOT a.attnotnull
	ORDER BY k.o LIMIT 1`

// keyRefusal: why a table has no key lokadb takes (enable refuses with it, check reports it).
type keyRefusal string

func (k keyRefusal) Error() string { return string(k) }

// UniqueKey: the key of table t (sqlName: as SQL names it) that has neither a primary key nor
// an id column, from its unique index of NOT NULL columns: one column, or several as a
// composite key (KindComposite). refused says why there is none, in the words enable refuses
// with; err is a failure of the query.
func UniqueKey(rows func(sql string, args ...any) ([][]string, error), t, sqlName string) (key, kind, refused string, err error) {
	k, err := uniqueKey(rows, t, sqlName)
	if r, ok := err.(keyRefusal); ok {
		return "", "", string(r), nil
	}
	return k.col, k.kind, "", err
}

func uniqueKey(rows func(sql string, args ...any) ([][]string, error), t, sqlName string) (keyInfo, error) {
	cols, err := rows(UniqueKeySQL, sqlName, true)
	if err != nil {
		return keyInfo{}, err
	}
	switch {
	case len(cols) == 1 && cols[0][1] != "":
		return keyInfo{cols[0][0], cols[0][1], cols[0][2], cols[0][3]}, nil
	case len(cols) == 1:
		return keyInfo{}, keyRefusal(fmt.Sprintf("%s has no primary key, and its unique key %s is %s: lokadb takes keys of type %s (with a deterministic collation)", t, cols[0][0], cols[0][3], KeyTypes))
	case len(cols) > 1:
		k, bad, ok := composite(cols)
		if !ok {
			return keyInfo{}, keyRefusal(fmt.Sprintf("%s has no primary key, and its unique key has column %s: lokadb takes keys of type %s (with a deterministic collation)", t, bad, KeyTypes))
		}
		return k, nil
	}
	nullable, err := rows(nullableUniqueSQL, sqlName, false)
	if err != nil {
		return keyInfo{}, err
	}
	if len(nullable) > 0 {
		return keyInfo{}, keyRefusal(fmt.Sprintf("%s has no primary key, and its unique index %s has column %s, which allows NULL: such an index does not tell rows apart (rows with a NULL repeat), so lokadb cannot take it as the key. Make its columns NOT NULL, or give %s a primary key or an id column", t, nullable[0][0], nullable[0][1], t))
	}
	return keyInfo{}, keyRefusal(fmt.Sprintf("%s: needs a primary key (of columns of type %s), a unique index of NOT NULL columns, or an id column", t, KeyTypes))
}
