package proxy

import (
	"fmt"
	"strings"
)

// The refs rows of a composite key on the direct path (tasks/composite-direct.md, stage 2). A
// key of several columns goes to the cloud as rows of its values, the columns an INSERT does not
// name as the cloud's defaults in place (Table.KeyDefaults): one statement writes the refs rows,
// answers how many map to another organization and gives each row's key back, with whether
// its refs row was added (an upsert's rows that were not inserted leave __refs by these). Block
// 10's review of stage 1: one exchange with the cloud, not one for the defaults and one for the
// refs rows.

// rowLits: the rows of an INSERT … VALUES as SQL of the refs table's key columns: a value named
// read by its kind (canonical, keyLits), the organization column not named as org, another
// column not named as its default, computed in the cloud. false: a value cannot be read.
func (w *directWrite) rowLits(vals valuesFunc, org string) ([]string, bool) {
	var out []string
	for _, row := range w.rows {
		l := make([]string, len(w.keyCols))
		for j, c := range w.keyCols {
			typ := w.table.ColType[c]
			switch {
			case w.keyAts[j] >= 0:
				v, _ := value(row[w.keyAts[j]])
				v.Kind = w.keyKinds[j]
				vs, ok := vals(v)
				if !ok || len(vs) != 1 || vs[0] == nullValue {
					return nil, false
				}
				l[j] = keyLits(vs, w.keyKinds[j], typ)[0]
			case c == w.table.TenantCol:
				l[j] = keyLits([]string{org}, w.keyKinds[j], typ)[0]
			default:
				l[j] = "(" + w.table.KeyDefaults[c] + ")::" + typ
			}
		}
		out = append(out, strings.Join(l, ", "))
	}
	return out, true
}

// tupleRefs: the cloud's statement for the refs rows of a composite key's rows (lits, SQL of the
// key columns each, rowLits or lits). Its first row: how many of them map to another organization
// (counted before the INSERT, as givenRefs does); then, in the order of rows, whether its refs
// row was added and the key's values as text. A key that maps to this organization already (a
// crash between the two commits, an upsert's row there) is left to the organization's table.
// The VALUES are computed once, MATERIALIZED (block 10's review of stage 2): a default read again
// would give the refs rows other keys than the organization's rows get.
func (w *directWrite) tupleRefs(lits []string, refs, org string) string {
	org = orgLit(org)
	cols := w.refsCols()
	var vs, nulls, out, keys []string
	for i, l := range lits {
		vs = append(vs, fmt.Sprintf("(%s, %d)", l, i+1))
	}
	for i := range w.keyCols {
		nulls = append(nulls, fmt.Sprintf("NULL::text AS lokadb_k%d", i+1))
		out = append(out, fmt.Sprintf("lokadb_k%d", i+1))
		keys = append(keys, fmt.Sprintf("v.id_%d::text", i+1))
	}
	return "WITH lokadb_v(" + cols + ", lokadb_n) AS MATERIALIZED (VALUES " + strings.Join(vs, ", ") + "), lokadb_r AS (INSERT INTO " + refs + " (" + cols + ", tenant) SELECT " + cols + ", " + org +
		" FROM lokadb_v ON CONFLICT (" + cols + ") DO NOTHING RETURNING " + cols + ") SELECT lokadb_c, lokadb_a, " + strings.Join(out, ", ") +
		" FROM (SELECT 0 AS lokadb_o, 0 AS lokadb_n, (SELECT count(*)::text FROM " + refs + " x JOIN lokadb_v USING (" + cols + ") WHERE x.tenant IS DISTINCT FROM " + org + ") AS lokadb_c, NULL::text AS lokadb_a, " + strings.Join(nulls, ", ") +
		" UNION ALL SELECT 1, v.lokadb_n, NULL, (r.id_1 IS NOT NULL)::text, " + strings.Join(keys, ", ") + " FROM lokadb_v v LEFT JOIN lokadb_r r USING (" + cols + ")) lokadb_x ORDER BY lokadb_o, lokadb_n"
}

// tupleAnswer reads tupleRefs' rows for n rows of keys: whether none maps to another
// organization, the keys (tupleKey) in the order of the rows, and those whose refs rows it added.
func (w *directWrite) tupleAnswer(rows [][]string, n int) (ok bool, keys, added []string, read bool) {
	k := len(w.keyCols)
	if len(rows) != n+1 || len(rows[0]) != k+2 {
		return false, nil, nil, false
	}
	for _, r := range rows[1:] {
		if len(r) != k+2 {
			return false, nil, nil, false
		}
		key := tupleKey(r[2:])
		keys = append(keys, key)
		if r[1] == "true" {
			added = append(added, key)
		}
	}
	return rows[0][0] == "0", keys, added, true
}

// refsAdd: the cloud's statement for the refs rows of keys an INSERT … SELECT, an unnest or a
// COPY inserted (the organization's RETURNING): givenRefs for a key of one column, tupleRefs for
// a composite one. othersNone reads its answer: none of them is another organization's.
func (w *directWrite) refsAdd(keys []string, kind, typ, refs, org string) string {
	if w.keyCols == nil {
		return givenRefs(keyLits(keys, kind, typ), "id", refs, org, false)
	}
	return w.tupleRefs(w.lits(keys, kind, typ), refs, org)
}

func othersNone(rows [][]string) bool {
	return len(rows) > 0 && len(rows[0]) > 0 && rows[0][0] == "0"
}

// genArrays: the arrays of the composite key's columns the cloud's defaults gave an unnest
// INSERT (gen: a row of genCols per element), one per column, typed.
func (w *directWrite) genArrays(gen []string) []string {
	cols := w.genCols()
	var out []string
	for j, c := range cols {
		var lits []string
		for _, g := range gen {
			vals, _ := tupleVals(g)
			if j < len(vals) {
				lits = append(lits, sqlString(vals[j]))
			}
		}
		out = append(out, "ARRAY["+strings.Join(lits, ", ")+"]::"+w.table.ColType[c]+"[]")
	}
	return out
}

// splitKey: the key of a row the organization's statement returned, from its values at the
// columns the proxy added (directWrite.added): one value, or a composite key's (tupleKey). bin:
// they came binary.
func (w *directWrite) splitKey(vals [][]byte, bin bool, kind string) (string, error) {
	if w.keyCols == nil {
		if !bin {
			return string(vals[0]), nil
		}
		k, ok := binaryKey(kind, vals[0])
		if !ok {
			return "", fmt.Errorf("a %s key of %d bytes", kind, len(vals[0]))
		}
		return k, nil
	}
	if len(vals) < len(w.keyCols) {
		return "", fmt.Errorf("a key of %d columns", len(vals))
	}
	out := make([]string, len(w.keyCols))
	for j := range w.keyCols {
		out[j] = string(vals[j])
		if bin {
			k, ok := binaryKey(w.keyKinds[j], vals[j])
			if !ok {
				return "", fmt.Errorf("a %s key column of %d bytes", w.keyKinds[j], len(vals[j]))
			}
			out[j] = k
		}
	}
	return tupleKey(out), nil
}
