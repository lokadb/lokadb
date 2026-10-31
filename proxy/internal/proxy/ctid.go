package proxy

import (
	"fmt"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// ctid: the physical address of a row, unique only inside one table. A LokaDB table is
// partitioned by organization (and a moved organization's partition is a table of another
// database), so `DELETE … WHERE ctid IN (SELECT ctid … LIMIT n)` (what Laravel's MassPrunable and
// every ORM's DELETE/UPDATE with a LIMIT send to Postgres) deletes the rows of every partition that
// have the same ctid as one the subquery picked: other organizations' rows, lost.
//
// A statement that finds rows of a LokaDB table by ctid is therefore never passed on as it is:
//   - the one shape that is exactly "the rows of this table found by this table's ctid" is
//     rewritten to the table's one-column key, which gives the same rows without the danger;
//   - every other use (a ctid read or compared to a value, a join, another table) is refused,
//     whatever the fanout policy, with the way out in the message.
//
// ctidUse: the LokaDB tables (their names) whose ctid the statement uses; empty if none.
func ctidUse(n *pg.Node, m *Meta) []string {
	refs := ctidRefs(n, m)
	var out []string
	seen := map[string]bool{}
	for _, r := range refs {
		for _, t := range r.tables {
			if !seen[t] {
				seen[t] = true
				out = append(out, t)
			}
		}
	}
	sort.Strings(out)
	return out
}

// CtidUse is ctidUse for a SQL text: what `lokadb check` reports about the workload.
func CtidUse(sql string, m *Meta, path []string) []string {
	if !containsFold(sql, "ctid") || m == nil {
		return nil
	}
	tree, _, err := parseQualified(sql, m, path)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, raw := range tree.Stmts {
		for _, t := range ctidUse(raw.Stmt, m) {
			if !seen[t] {
				seen[t] = true
				out = append(out, t)
			}
		}
	}
	sort.Strings(out)
	return out
}

type ctidRef struct {
	cr     *pg.ColumnRef
	tables []string // the LokaDB tables it may mean
}

// ctidRefs: every reference to a column named ctid that is (or may be) a LokaDB table's.
func ctidRefs(n *pg.Node, m *Meta) []ctidRef {
	if m == nil {
		return nil
	}
	// the relations of the statement, by the name they are known by
	rels := map[string]*Table{}
	var lokadb []string
	walk(n.ProtoReflect(), func(msg proto.Message) {
		if rv, ok := msg.(*pg.RangeVar); ok {
			key := rv.Relname
			if rv.Alias != nil && rv.Alias.Aliasname != "" {
				key = rv.Alias.Aliasname
			}
			t := m.table(rv)
			rels[key] = t
			if t != nil {
				lokadb = append(lokadb, t.Name)
			}
		}
	})
	if len(lokadb) == 0 {
		return nil
	}
	var out []ctidRef
	walk(n.ProtoReflect(), func(msg proto.Message) {
		cr, ok := msg.(*pg.ColumnRef)
		if !ok || len(cr.Fields) == 0 {
			return
		}
		last := cr.Fields[len(cr.Fields)-1].GetString_()
		if last == nil || last.Sval != "ctid" {
			return
		}
		if len(cr.Fields) >= 2 { // t.ctid, schema.t.ctid
			q := cr.Fields[len(cr.Fields)-2].GetString_().GetSval()
			t, known := rels[q]
			switch {
			case known && t != nil:
				out = append(out, ctidRef{cr, []string{t.Name}})
			case known: // another table's: not a LokaDB table's
			default: // a name this statement does not declare (a correlated reference): a LokaDB table's, to be safe
				out = append(out, ctidRef{cr, lokadb})
			}
			return
		}
		out = append(out, ctidRef{cr, lokadb})
	})
	return out
}

// ctidFix handles the use of ctid in a statement over LokaDB tables: it rewrites the tree in
// place when the use is the one shape that has a safe equivalent (fixed), or says why the
// statement must be refused (refusal). Neither when the statement does not use ctid of a
// LokaDB table.
func ctidFix(n *pg.Node, m *Meta) (fixed bool, refusal string, tables []string) {
	refs := ctidRefs(n, m)
	if len(refs) == 0 {
		return false, "", nil
	}
	tables = ctidUse(n, m)
	if fix := ctidKeyShape(n, m, refs); fix != nil {
		fix()
		return true, "", tables
	}
	return false, fmt.Sprintf("lokadb: %s is found by its ctid here: a ctid is unique only inside one partition and %s has one partition for each organization, so the statement would reach the rows of other organizations that have the same ctid. "+
		"Use the primary key instead (a DELETE or UPDATE of the table that picks its rows by `ctid IN (SELECT ctid FROM <the same table> … LIMIT n)` is rewritten to the key by the proxy)",
		strings.Join(tables, ", "), plural1(tables)), tables
}

func plural1(tables []string) string {
	if len(tables) == 1 {
		return "it"
	}
	return "they"
}

// ctidKeyShape: for `DELETE FROM t WHERE ctid IN (SELECT ctid FROM t WHERE … LIMIT n)` (and UPDATE),
// the function that turns both ctids into t's key; nil if the statement is anything else.
func ctidKeyShape(n *pg.Node, m *Meta, refs []ctidRef) func() {
	var rv *pg.RangeVar
	var where *pg.Node
	switch {
	case n.GetDeleteStmt() != nil:
		st := n.GetDeleteStmt()
		if st.WithClause != nil || len(st.UsingClause) > 0 || len(st.ReturningList) > 0 {
			return nil
		}
		rv, where = st.Relation, st.WhereClause
	case n.GetUpdateStmt() != nil:
		st := n.GetUpdateStmt()
		if st.WithClause != nil || len(st.FromClause) > 0 || len(st.ReturningList) > 0 {
			return nil
		}
		rv, where = st.Relation, st.WhereClause
	default:
		return nil
	}
	t := m.table(rv)
	if t == nil || where == nil || len(refs) != 2 {
		return nil
	}
	key := t.Key
	if key == "" {
		key = "id"
	}
	if _, composite := tupleCols(key); composite {
		return nil
	}
	name := rv.Relname
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		name = rv.Alias.Aliasname
	}
	// the one condition (alone, or one of the AND-ed ones): ctid IN (SELECT ctid FROM t …)
	var sl *pg.SubLink
	conds := []*pg.Node{where}
	if b := where.GetBoolExpr(); b != nil && b.Boolop == pg.BoolExprType_AND_EXPR {
		conds = b.Args
	}
	for _, c := range conds {
		if s := c.GetSubLink(); s != nil && s.SubLinkType == pg.SubLinkType_ANY_SUBLINK && len(s.OperName) == 0 && refOf(s.Testexpr, refs) != nil {
			if sl != nil {
				return nil
			}
			sl = s
		}
	}
	if sl == nil {
		return nil
	}
	outer := refOf(sl.Testexpr, refs)
	if q := qualifier(outer); q != "" && q != name {
		return nil
	}
	sel := sl.Subselect.GetSelectStmt()
	if sel == nil || sel.Op != pg.SetOperation_SETOP_NONE || sel.WithClause != nil || len(sel.TargetList) != 1 || len(sel.FromClause) != 1 ||
		len(sel.GroupClause) > 0 || sel.HavingClause != nil || len(sel.DistinctClause) > 0 || len(sel.WindowClause) > 0 || sel.IntoClause != nil {
		return nil
	}
	from := sel.FromClause[0].GetRangeVar()
	if from == nil || m.table(from) != t {
		return nil
	}
	inner := refOf(sel.TargetList[0].GetResTarget().GetVal(), refs)
	if inner == nil || inner == outer {
		return nil
	}
	fromName := from.Relname
	if from.Alias != nil && from.Alias.Aliasname != "" {
		fromName = from.Alias.Aliasname
	}
	if q := qualifier(inner); q != "" && q != fromName {
		return nil
	}
	return func() {
		for _, r := range []*pg.ColumnRef{outer, inner} {
			r.Fields[len(r.Fields)-1] = &pg.Node{Node: &pg.Node_String_{String_: &pg.String{Sval: key}}}
		}
	}
}

// refOf: n is exactly one of the ctid references.
func refOf(n *pg.Node, refs []ctidRef) *pg.ColumnRef {
	cr := n.GetColumnRef()
	if cr == nil {
		return nil
	}
	for _, r := range refs {
		if r.cr == cr {
			return cr
		}
	}
	return nil
}

func qualifier(cr *pg.ColumnRef) string {
	if len(cr.Fields) < 2 {
		return ""
	}
	return cr.Fields[len(cr.Fields)-2].GetString_().GetSval()
}

// containsFold: s has sub (lower case, ASCII) in any case, without allocating.
func containsFold(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		j := 0
		for ; j < len(sub); j++ {
			c := s[i+j]
			if c >= 'A' && c <= 'Z' {
				c += 'a' - 'A'
			}
			if c != sub[j] {
				break
			}
		}
		if j == len(sub) {
			return true
		}
	}
	return false
}
