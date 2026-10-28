package ext

import (
	"context"
	"flag"
	"fmt"
	"sort"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/check"
)

// A feed: a statement that reads one organization's newest (or first) rows of a LokaDB table,
// `WHERE <organization column> = $1 … ORDER BY <column> … LIMIT n`. Without an index that
// starts with (organization column, that column) Postgres reads the organization's rows one
// by one along the primary key, or all of them, and sorts; with one it reads n rows. That is
// so with lokadb and without it, and through the proxy the policy's estimate makes the plan
// worse still (research/ANALYSIS.md, the feed of F7: 94 → 3,8 ms through the proxy with the
// index). check suggests the index (BACKLOG, block 11: the feed index advice).

// feeds: the feed index advice (an extension of check, registered in ext.go; no flags).
type feeds struct{}

func (feeds) Flags(*flag.FlagSet) {}

// feed is one feed shape: the table, its organization column and the ordering column.
type feed struct {
	table, tenant, order string
}

// feedShape: the feed sql is, if it is one: a SELECT of one LokaDB table restricted by its
// organization column with =, ordered by a column, with a LIMIT.
func feedShape(s *check.Session, sql string) (feed, bool) {
	tree, err := pg.Parse(check.Analyzable(sql))
	if err != nil || len(tree.Stmts) != 1 {
		return feed{}, false
	}
	sel := tree.Stmts[0].Stmt.GetSelectStmt()
	if sel == nil || sel.LimitCount == nil || len(sel.SortClause) == 0 || len(sel.FromClause) != 1 {
		return feed{}, false
	}
	// the table itself, or the left one of a JOIN (an ORM's select_related: the feed's table
	// joined with a cloud table for each row)
	from := sel.FromClause[0]
	for from.GetJoinExpr() != nil {
		from = from.GetJoinExpr().Larg
	}
	rv := from.GetRangeVar()
	t := s.Meta().TableIn(rv, s.SearchPath())
	if t == nil || t.TenantCol == "" {
		return feed{}, false
	}
	// a column of a joined table under the same name is not the feed's: names qualified by
	// another table do not count
	name := rv.Relname
	if rv.Alias != nil {
		name = rv.Alias.Aliasname
	}
	col := func(n *pg.Node) string {
		cr := n.GetColumnRef()
		if cr == nil || len(cr.Fields) == 0 || len(cr.Fields) >= 2 && cr.Fields[len(cr.Fields)-2].GetString_().GetSval() != name {
			return ""
		}
		return colName(n)
	}
	order := col(sel.SortClause[0].GetSortBy().GetNode())
	if order == "" || order == t.TenantCol || !hasEquality(sel.WhereClause, t.TenantCol, col) {
		return feed{}, false
	}
	return feed{table: t.Name, tenant: t.TenantCol, order: order}, true
}

// colName: the column a node names (its last field), "" for anything else.
func colName(n *pg.Node) string {
	cr := n.GetColumnRef()
	if cr == nil || len(cr.Fields) == 0 {
		return ""
	}
	return cr.Fields[len(cr.Fields)-1].GetString_().GetSval()
}

// hasEquality: where restricts column want with = (at its top or under AND); name reads a
// column reference of the feed's table.
func hasEquality(where *pg.Node, want string, name func(*pg.Node) string) bool {
	if where == nil {
		return false
	}
	if b := where.GetBoolExpr(); b != nil {
		if b.Boolop != pg.BoolExprType_AND_EXPR {
			return false
		}
		for _, a := range b.Args {
			if hasEquality(a, want, name) {
				return true
			}
		}
		return false
	}
	e := where.GetAExpr()
	if e == nil || e.Kind != pg.A_Expr_Kind_AEXPR_OP || len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
		return false
	}
	return name(e.Lexpr) == want || name(e.Rexpr) == want
}

// feedIndexes suggests an index for every feed of the workload whose table has none that
// starts with (organization column, ordering column).
func (feeds) Run(_ context.Context, s *check.Session) error {
	if s.Meta() == nil {
		return nil
	}
	type found struct {
		f    feed
		pats []check.Pattern
	}
	feeds := map[feed]*found{}
	for sql, n := range s.Statements() {
		f, ok := feedShape(s, sql)
		if !ok {
			continue
		}
		if feeds[f] == nil {
			feeds[f] = &found{f: f}
		}
		fp := fpKey(sql)
		if fp == sql {
			fp = ""
		}
		feeds[f].pats = append(feeds[f].pats, check.Pattern{Fingerprint: fp, SQL: sql, Calls: n})
	}
	var keys []feed
	for f := range feeds {
		keys = append(keys, f)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].table < keys[j].table || keys[i].table == keys[j].table && keys[i].order < keys[j].order
	})
	for _, f := range keys {
		has, err := s.Rows(`SELECT EXISTS (SELECT 1 FROM pg_index i
				WHERE i.indrelid = $1::regclass AND i.indnkeyatts >= 2 AND i.indpred IS NULL
				  AND i.indkey[0] = (SELECT attnum FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $2)
				  AND i.indkey[1] = (SELECT attnum FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $3))::text`,
			check.QuoteKey(f.table), f.tenant, f.order)
		if err == check.ErrNoDatabase {
			return nil
		}
		if err != nil {
			return err
		}
		if has[0][0] == "true" {
			continue
		}
		pats := feeds[f].pats
		sort.Slice(pats, func(i, j int) bool {
			return pats[i].Calls > pats[j].Calls || pats[i].Calls == pats[j].Calls && pats[i].SQL < pats[j].SQL
		})
		var ex []string
		for i, p := range pats {
			if i == 3 {
				break
			}
			ex = append(ex, fmt.Sprintf("x%d  %s", p.Calls, check.Short(p.SQL)))
		}
		ex = append(ex, fmt.Sprintf("CREATE INDEX ON %s (%s, %s)", check.QuoteKey(f.table), check.QuoteIdent(f.tenant), check.QuoteIdent(f.order)))
		s.AddFinding(check.Finding{Severity: check.Info, Code: "feed_index", Object: f.table + "." + f.order, Subject: feedSubject(f),
			Message: fmt.Sprintf("%s read one organization's rows of %s ordered by %s with a LIMIT (a feed), and %s has no index on (%s, %s): Postgres then reads the organization's rows along another index, or all of them, and sorts, with lokadb as without it, and through the proxy the plan is worse still. With the index it reads only the rows the LIMIT returns: a feed of 20 measured 94 → 3,8 ms through the proxy (research/ANALYSIS.md, the feed of F7). Create it before enable: move-org makes the organizations' tables with the cloud's indexes. After enable, create it through the proxy as a migration (without CONCURRENTLY): the proxy replays it in the organizations' databases",
				check.Plural(len(pats), "query pattern", "query patterns"), f.table, f.order, f.table, f.tenant, f.order),
			Examples: ex, Patterns: pats})
	}
	return nil
}

// feedSubject: the index a feed lacks, keyed by its table and columns.
func feedSubject(f feed) *check.Subject {
	s := &check.Subject{Kind: "index", Columns: []string{f.tenant, f.order}}
	s.Schema, s.Table = check.SplitQual(f.table)
	return s
}
