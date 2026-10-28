package admin

// Composite keys (tasks/composite-keys.md). A table's key is the list of its primary key's
// columns. Wherever lokadb keeps the name of one key column (lokadb.tables.key_column, the
// plan's key, keyType and refsType, the proxy's Table.Key) a key of several columns is
// written as a JSON array: ["account_id","id"]. A key of one column keeps its bare name, so
// everything written before stays as it is. The refs table of a composite key holds it in
// columns id_1 … id_n (id for a key of one column).

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// KeyCols: the columns of a key as lokadb writes it (one name, or a JSON array of names).
func KeyCols(k string) []string {
	if strings.HasPrefix(k, "[") {
		var cols []string
		if json.Unmarshal([]byte(k), &cols) == nil && len(cols) > 0 {
			return cols
		}
	}
	return []string{k}
}

// KeyName: how lokadb writes a key of columns cols (KeyCols reads it back).
func KeyName(cols []string) string {
	if len(cols) == 1 {
		return cols[0]
	}
	b, _ := json.Marshal(cols)
	return string(b)
}

// Composite: a key of several columns.
func Composite(k string) bool { return len(KeyCols(k)) > 1 }

// RefsKeyCols: the columns of the refs table that hold key k: id, or id_1 … id_n.
func RefsKeyCols(k string) []string {
	cols := KeyCols(k)
	if len(cols) == 1 {
		return []string{"id"}
	}
	out := make([]string, len(cols))
	for i := range cols {
		out[i] = fmt.Sprintf("id_%d", i+1)
	}
	return out
}

// colList: the quoted columns of key k, each after prefix ("x." or "").
func colList(k, prefix string) string {
	var out []string
	for _, c := range KeyCols(k) {
		out = append(out, prefix+q(c))
	}
	return strings.Join(out, ", ")
}

// refsColList: the refs table's key columns, each after prefix.
func refsColList(k, prefix string) string {
	var out []string
	for _, c := range RefsKeyCols(k) {
		out = append(out, prefix+q(c))
	}
	return strings.Join(out, ", ")
}

// keyEq: the condition that columns a (prefix pa, key ka) equal columns b (prefix pb, key kb),
// column by column: (a1, a2) = (b1, b2) without a row constructor, which keeps indexes usable.
func keyEq(pa string, ca []string, pb string, cb []string) string {
	var out []string
	for i := range ca {
		out = append(out, pa+q(ca[i])+" = "+pb+q(cb[i]))
	}
	return strings.Join(out, " AND ")
}

// refsDDL: the columns and constraints of the refs table of key k, whose types are typ (one
// type, or a JSON array of them), with the organization of type tenantTyp.
func refsDDL(k, typ, tenantTyp string) string {
	types := KeyCols(typ)
	var cols []string
	for i, c := range RefsKeyCols(k) {
		cols = append(cols, q(c)+" "+types[i])
	}
	ids := refsColList(k, "")
	if !Composite(k) {
		return fmt.Sprintf("(id %s PRIMARY KEY, tenant %s, UNIQUE (tenant, id))", types[0], tenantTyp)
	}
	return fmt.Sprintf("(%s, tenant %s, PRIMARY KEY (%s), UNIQUE (tenant, %s))", strings.Join(cols, ", "), tenantTyp, ids, ids)
}

// fkLookup: the column (or the columns, written as a key) of an FK's table through which the
// proxy finds a row's organization in the refs table the FK points at: refsCols are the refs
// columns the FK references (RefsColumns), srcCols its own. ok false: the FK does not cover the
// key (a tenant-scoped FK's organization column is not part of the lookup).
func fkLookup(srcCols, refsCols []string) (string, bool) { return FKLookup(srcCols, refsCols) }

// FKLookup is fkLookup for check.
func FKLookup(srcCols, refsCols []string) (string, bool) {
	var single string
	group := map[int]string{}
	for i, c := range refsCols {
		switch {
		case c == "id":
			single = srcCols[i]
		case strings.HasPrefix(c, "id_"):
			var n int
			if _, err := fmt.Sscanf(c, "id_%d", &n); err == nil {
				group[n] = srcCols[i]
			}
		}
	}
	if single != "" {
		return single, true
	}
	if len(group) == 0 {
		return "", false
	}
	cols := make([]string, len(group))
	for n, c := range group {
		if n < 1 || n > len(cols) {
			return "", false
		}
		cols[n-1] = c
	}
	return KeyName(cols), true
}

// bkey is a key the batches of enable -prepare walk (prepare.go): one column, or several
// compared as a row ((a, b) > (…), which a btree index on (a, b) serves). A batch's last key
// travels as text: the value of a one-column key, a JSON array of a composite key's values.
type bkey struct{ cols, types []string }

func newBKey(k, typ string) bkey { return bkey{KeyCols(k), KeyCols(typ)} }

// sel: the key's columns of alias selected as k (one column) or k_1 … k_n.
func (b bkey) sel(alias string) string {
	if len(b.cols) == 1 {
		return alias + q(b.cols[0]) + " AS k"
	}
	var out []string
	for i, c := range b.cols {
		out = append(out, fmt.Sprintf("%s%s AS k_%d", alias, q(c), i+1))
	}
	return strings.Join(out, ", ")
}

// names: the selected key (sel) as a list: k, or k_1, …, k_n.
func (b bkey) names() string {
	if len(b.cols) == 1 {
		return "k"
	}
	var out []string
	for i := range b.cols {
		out = append(out, fmt.Sprintf("k_%d", i+1))
	}
	return strings.Join(out, ", ")
}

// order: ORDER BY the key's columns of alias (desc: descending).
func (b bkey) order(alias string, desc bool) string {
	var out []string
	for _, c := range b.cols {
		if desc {
			out = append(out, alias+q(c)+" DESC")
		} else {
			out = append(out, alias+q(c))
		}
	}
	return strings.Join(out, ", ")
}

// text: the key of alias as the text a batch's bound travels in (its value, or a JSON array).
func (b bkey) text(alias string) string {
	if len(b.cols) == 1 {
		return alias + q(b.cols[0])
	}
	var out []string
	for _, c := range b.cols {
		out = append(out, alias+q(c))
	}
	return "jsonb_build_array(" + strings.Join(out, ", ") + ")"
}

// lastOf: the text of the greatest key among the selected ones (sel) of relation from.
func (b bkey) lastOf(from string) string {
	if len(b.cols) == 1 {
		return fmt.Sprintf("(SELECT k FROM %s ORDER BY k DESC LIMIT 1)::text", from)
	}
	var desc []string
	for i := range b.cols {
		desc = append(desc, fmt.Sprintf("k_%d DESC", i+1))
	}
	return fmt.Sprintf("(SELECT jsonb_build_array(%s) FROM %s ORDER BY %s LIMIT 1)::text", b.names(), from, strings.Join(desc, ", "))
}

// bound: parameter $n (the text of a key) as a value of the key's type, or a row of them.
func (b bkey) bound(n int) string {
	if len(b.cols) == 1 {
		return fmt.Sprintf("$%d::%s", n, b.types[0])
	}
	var out []string
	for i, t := range b.types {
		out = append(out, fmt.Sprintf("($%d::jsonb ->> %d)::%s", n, i, t))
	}
	return "(" + strings.Join(out, ", ") + ")"
}

// row: the key's columns of alias, a row for a composite key.
func (b bkey) row(alias string) string {
	if len(b.cols) == 1 {
		return alias + q(b.cols[0])
	}
	var out []string
	for _, c := range b.cols {
		out = append(out, alias+q(c))
	}
	return "(" + strings.Join(out, ", ") + ")"
}

// within: the batch's condition on the key of alias: after the lower bound $1 (the comparison
// batchFrom stands for), up to the upper bound $2.
func (b bkey) within(alias string) string {
	return fmt.Sprintf("%s %s %s AND %s <= %s", b.row(alias), batchFrom, b.bound(1), b.row(alias), b.bound(2))
}

// trigCols: the columns a refs trigger fires on, UPDATE OF: the key's and the organization
// column, once (kind A keys hold the organization column).
func trigCols(k, tc string) string {
	cols := KeyCols(k)
	if !slices.Contains(cols, tc) {
		cols = append(cols, tc)
	}
	var out []string
	for _, c := range cols {
		out = append(out, q(c))
	}
	return strings.Join(out, ", ")
}
