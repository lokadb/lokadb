package proxy

// Composite keys (tasks/composite-keys.md). A lookup of several columns finds a row's
// organization by all of them together: a table's composite key (orders (account_id, id),
// parcels (tracking_no, piece)) or an FK onto one. lokadb.lookups names such a lookup by a JSON
// array of its columns; its refs table holds the key in id_1 … id_n. A statement names the
// organization through it only when it binds every column (WHERE a = $1 AND b = $2,
// (a, b) IN (…)); part of a key restricts nothing.

import (
	"encoding/json"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// TupleHint says "rows of Table are restricted to (Columns) IN Rows", through the lookup of
// those columns (Meta.Lookups[Table][tupleName(Columns)]).
type TupleHint struct {
	Table   string
	Columns []string
	Rows    [][]ValueRef
}

// maxTuples: no more rows of a tuple than this (a product of IN lists, say): beyond it the
// statement names no organization the proxy reads.
const maxTuples = 1000

// kindTuple marks a refs table of a composite key in Meta.KeyKinds; Meta.TupleKinds has the
// kinds of its columns.
const kindTuple = "tuple"

// tupleCols: the columns of lookup col when it is a lookup of several (a JSON array).
func tupleCols(col string) ([]string, bool) {
	if !strings.HasPrefix(col, "[") {
		return nil, false
	}
	var cols []string
	if json.Unmarshal([]byte(col), &cols) != nil || len(cols) < 2 {
		return nil, false
	}
	return cols, true
}

// tupleName: how lokadb.lookups names the lookup of cols.
func tupleName(cols []string) string {
	b, _ := json.Marshal(cols)
	return string(b)
}

// tupleKey: the key of values (canonical) as the organization cache keeps it.
func tupleKey(vals []string) string {
	b, _ := json.Marshal(vals)
	return string(b)
}

// tupleVals: the values of a key tupleKey made.
func tupleVals(key string) ([]string, bool) {
	var vals []string
	return vals, json.Unmarshal([]byte(key), &vals) == nil
}

// tupleGroups: the lookups of several columns of table t, in a fixed order.
func (m *Meta) tupleGroups(t string) [][]string {
	var out [][]string
	for col := range m.Lookups[t] {
		if cols, ok := tupleCols(col); ok {
			out = append(out, cols)
		}
	}
	sort.Slice(out, func(i, j int) bool { return tupleName(out[i]) < tupleName(out[j]) })
	return out
}

// tupleKinds: the kinds of the columns of composite key refs (nil for any other refs).
func (m *Meta) tupleKinds(refs string) []string {
	if m == nil {
		return nil
	}
	return m.TupleKinds[refs]
}

// rowExpr recognizes (a, b) = (x, y) and (a, b) IN ((x, y), …): the columns and the rows of
// values (constants and parameters).
func rowExpr(n *pg.Node) (cols [][]string, rows [][]ValueRef, ok bool) {
	e := n.GetAExpr()
	if e == nil || len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
		return nil, nil, false
	}
	row := func(n *pg.Node) []*pg.Node {
		if r := n.GetRowExpr(); r != nil {
			return r.Args
		}
		return nil
	}
	left := row(e.Lexpr)
	if len(left) < 2 {
		return nil, nil, false
	}
	for _, l := range left {
		f, ok := colRef(l)
		if !ok {
			return nil, nil, false
		}
		cols = append(cols, f)
	}
	values := func(items []*pg.Node) ([]ValueRef, bool) {
		if len(items) != len(cols) {
			return nil, false
		}
		var out []ValueRef
		for _, it := range items {
			v, ok := value(it)
			if !ok {
				return nil, false
			}
			out = append(out, v)
		}
		return out, true
	}
	switch e.Kind {
	case pg.A_Expr_Kind_AEXPR_OP:
		r, ok := values(row(e.Rexpr))
		if !ok {
			return nil, nil, false
		}
		return cols, [][]ValueRef{r}, true
	case pg.A_Expr_Kind_AEXPR_IN:
		list := e.Rexpr.GetList()
		if list == nil {
			return nil, nil, false
		}
		for _, it := range list.Items {
			r, ok := values(row(it))
			if !ok {
				return nil, nil, false
			}
			rows = append(rows, r)
		}
		return cols, rows, len(rows) > 0
	}
	return nil, nil, false
}

// product: the rows of the product of lists (one per column), false beyond maxTuples.
func product(lists [][]ValueRef) ([][]ValueRef, bool) {
	rows := [][]ValueRef{nil}
	for _, l := range lists {
		var next [][]ValueRef
		for _, r := range rows {
			for _, v := range l {
				next = append(next, append(append([]ValueRef{}, r...), v))
			}
		}
		if len(next) > maxTuples {
			return nil, false
		}
		rows = next
	}
	return rows, true
}
