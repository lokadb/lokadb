package admin

// The key of a table being moved by triggers (trigger.go, split.go): one column, or several
// (a composite key, compkey.go). The move log keeps a row's key as text: the value of a
// one-column key, a JSON array of a composite key's values.

import (
	"fmt"
	"strings"
)

// keyColsOf: the columns of t's key.
func keyColsOf(t moveTable) []string { return KeyCols(keyOf(t)) }

// keyTypesOf: the types of t's key's columns (bigint for a column not described).
func keyTypesOf(t moveTable) []string {
	var out []string
	for _, k := range keyColsOf(t) {
		typ := "bigint"
		for _, c := range t.cols {
			if c[0] == k {
				typ = c[1]
			}
		}
		out = append(out, typ)
	}
	return out
}

// keyRow: t's key's columns (after prefix), a row of them for a composite key.
func keyRow(t moveTable, prefix string) string {
	var out []string
	for _, c := range keyColsOf(t) {
		out = append(out, prefix+q(c))
	}
	if len(out) == 1 {
		return out[0]
	}
	return "(" + strings.Join(out, ", ") + ")"
}

// keyList: t's key's columns, comma-separated (ON CONFLICT, ORDER BY).
func keyList(t moveTable) string {
	var out []string
	for _, c := range keyColsOf(t) {
		out = append(out, q(c))
	}
	return strings.Join(out, ", ")
}

// keyText: the key of record rec (OLD, NEW) as the move log keeps it.
func keyText(t moveTable, rec string) string {
	cols := keyColsOf(t)
	if len(cols) == 1 {
		return rec + "." + q(cols[0]) + "::text"
	}
	var out []string
	for _, c := range cols {
		out = append(out, rec+"."+q(c))
	}
	return "jsonb_build_array(" + strings.Join(out, ", ") + ")::text"
}

// keyOfText: parameter param (a key as the move log keeps it, text) as a value of t's key, a
// row of values for a composite key.
func keyOfText(t moveTable, param string) string {
	types := keyTypesOf(t)
	if len(types) == 1 {
		return param + "::text::" + types[0]
	}
	var out []string
	for i, typ := range types {
		out = append(out, fmt.Sprintf("(%s::jsonb ->> %d)::%s", param, i, typ))
	}
	return "(" + strings.Join(out, ", ") + ")"
}

// keyOfRow: t's key read from jsonb row image param, a row for a composite key.
func keyOfRow(t moveTable, param string) string {
	cols, types := keyColsOf(t), keyTypesOf(t)
	var out []string
	for i, c := range cols {
		out = append(out, fmt.Sprintf("(%s::jsonb ->> %s)::%s", param, lit(c), types[i]))
	}
	if len(out) == 1 {
		return out[0]
	}
	return "(" + strings.Join(out, ", ") + ")"
}

// keyLiteral: the key whose COPY text fields are vals, as a literal of t's key's type (a row of
// them for a composite key).
func keyLiteral(t moveTable, vals []string) string {
	types := keyTypesOf(t)
	var out []string
	for i, v := range vals {
		out = append(out, lit(v)+"::"+types[i])
	}
	if len(out) == 1 {
		return out[0]
	}
	return "(" + strings.Join(out, ", ") + ")"
}
