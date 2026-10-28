package proxy

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// The partition key's condition (review of performance 3, F7, C1). A LokaDB table is
// partitioned by coalesce(<organization column>, -2) (the nil uuid for an organization
// numbered by uuid); the application's condition (organization_id = 1, id = 5) is not on that
// expression, and the policy's range is on lokadb.org(), a stable function: the planner had no
// constant to prune by, planned every partition (159 ms a point read at a thousand
// organizations, 342 at three thousand) and left the pruning to the executor. The proxy adds
// coalesce(t.organization_id, -2) = N for every LokaDB table of a statement of organization N,
// at every level: it is what the policy lokadb_tenant requires under lokadb.org = N, so the
// statement's rows are the same whatever N is (a wrong N found nothing before, and finds
// nothing now), and the planner prunes to the organization's partition (or DEFAULT) at once.
// Not on the side of an outer join that may be null (a condition in WHERE would drop the
// rows the join keeps without a match: the policy filters the table, not the join's rows),
// nor in a statement of WHERE CURRENT OF, nor on a name a CTE of the statement takes, nor on a
// table whose name the statement's level does not see: inside a join with an alias of its own,
// (documents d JOIN users u ON …) j, which hides d and u from WHERE (Postgres refused the
// statement: invalid reference to FROM-clause entry), or a table with column aliases,
// documents d(a, b, …), which renames its organization column. There the statement goes as it
// is: the policy keeps it to the organization, the planner prunes at run time.

// keyCond: sql (one statement) with the key's condition of org on each LokaDB table it reads or
// changes; false when nothing is added (no such table, a form left as it is, sql unreadable).
func keyCond(sql string, m *Meta, org string, path []string) (string, bool) {
	if m == nil || !realOrg(org) {
		return "", false
	}
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return "", false
	}
	stmt := tree.Stmts[0].Stmt
	switch {
	case stmt.GetSelectStmt() != nil, stmt.GetUpdateStmt() != nil, stmt.GetDeleteStmt() != nil,
		stmt.GetInsertStmt() != nil, stmt.GetExplainStmt() != nil:
	default:
		return "", false
	}
	if qualify(stmt, m, path) != "" {
		return "", false
	}
	ctes, currentOf := map[string]bool{}, false
	Walk(stmt, func(msg proto.Message) {
		switch x := msg.(type) {
		case *pg.CommonTableExpr:
			ctes[x.Ctename] = true
		case *pg.CurrentOfExpr:
			currentOf = true
		}
	})
	if currentOf {
		return "", false
	}
	none, lit := "-2", org
	if m.TenantKind == kindUUID {
		none, lit = "'00000000-0000-0000-0000-000000000000'::uuid", sqlString(org)+"::uuid"
	} else if _, err := strconv.ParseInt(org, 10, 64); err != nil {
		return "", false
	}
	added := false
	add := func(where **pg.Node, rels []keyRel) {
		for _, r := range rels {
			if r.nullable || r.hidden || r.rv.Schemaname == "" && ctes[r.rv.Relname] ||
				r.rv.Alias != nil && len(r.rv.Alias.Colnames) > 0 {
				continue
			}
			t := m.table(r.rv)
			if t == nil || t.TenantCol == "" || len(m.Local[t.Name]) == 0 && len(m.Foreign[t.Name]) == 0 {
				continue // no partition of an organization's own: DEFAULT and the rows of none, nothing to prune
			}
			ref := r.rv.Relname
			if r.rv.Alias != nil && r.rv.Alias.Aliasname != "" {
				ref = r.rv.Alias.Aliasname
			} else if r.rv.Schemaname != "" {
				ref = quoteIdent(r.rv.Schemaname) + "." + quoteIdent(ref)
			}
			if !strings.Contains(ref, ".") {
				ref = quoteIdent(ref)
			}
			c, err := pg.Parse(fmt.Sprintf("SELECT WHERE coalesce(%s.%s, %s) = %s", ref, quoteIdent(t.TenantCol), none, lit))
			if err != nil {
				continue
			}
			cond := c.Stmts[0].Stmt.GetSelectStmt().WhereClause
			if *where == nil {
				*where = cond
			} else if be := (*where).GetBoolExpr(); be != nil && be.Boolop == pg.BoolExprType_AND_EXPR {
				be.Args = append(be.Args, cond)
			} else {
				*where = &pg.Node{Node: &pg.Node_BoolExpr{BoolExpr: &pg.BoolExpr{Boolop: pg.BoolExprType_AND_EXPR, Args: []*pg.Node{*where, cond}}}}
			}
			added = true
		}
	}
	Walk(stmt, func(msg proto.Message) {
		switch x := msg.(type) {
		case *pg.SelectStmt:
			var rels []keyRel
			fromRels(x.FromClause, false, &rels)
			add(&x.WhereClause, rels)
		case *pg.UpdateStmt:
			rels := []keyRel{{rv: x.Relation}}
			fromRels(x.FromClause, false, &rels)
			add(&x.WhereClause, rels)
		case *pg.DeleteStmt:
			rels := []keyRel{{rv: x.Relation}}
			fromRels(x.UsingClause, false, &rels)
			add(&x.WhereClause, rels)
		}
	})
	if !added {
		return "", false
	}
	out, err := deparseStmt(stmt)
	if err != nil {
		return "", false
	}
	return out, true
}

// keyRel: a relation of a statement's FROM, whether an outer join may make it null, and
// whether a join's alias hides its name from the statement's WHERE.
type keyRel struct {
	rv       *pg.RangeVar
	nullable bool
	hidden   bool
}

// fromRels: the relations of FROM items (through joins; a subquery or a function is a level of
// its own, or no table), each with whether it is on a side of an outer join that may be null
// and whether a join's alias hides it.
func fromRels(items []*pg.Node, nullable bool, out *[]keyRel) {
	joinRels(items, nullable, false, out)
}

func joinRels(items []*pg.Node, nullable, hidden bool, out *[]keyRel) {
	for _, n := range items {
		switch {
		case n.GetRangeVar() != nil:
			*out = append(*out, keyRel{rv: n.GetRangeVar(), nullable: nullable, hidden: hidden})
		case n.GetJoinExpr() != nil:
			j := n.GetJoinExpr()
			left := nullable || j.Jointype == pg.JoinType_JOIN_RIGHT || j.Jointype == pg.JoinType_JOIN_FULL
			right := nullable || j.Jointype == pg.JoinType_JOIN_LEFT || j.Jointype == pg.JoinType_JOIN_FULL
			h := hidden || j.Alias != nil
			joinRels([]*pg.Node{j.Larg}, left, h, out)
			joinRels([]*pg.Node{j.Rarg}, right, h, out)
		}
	}
}

// keyedText: the simple protocol's text of a statement of org with the key's condition (cached
// per session), or sql as it is.
func (s *session) keyedText(sql, org string) string {
	k := org + "\x00" + s.pathNow() + "\x00" + sql
	if v, ok := s.keyTexts[k]; ok {
		if v == "" {
			return sql
		}
		return v
	}
	out, ok := keyCond(sql, s.meta, org, s.searchPathOrDefault())
	if !ok {
		out = ""
	}
	if s.keyTexts == nil || len(s.keyTexts) > 5000 {
		s.keyTexts = map[string]string{}
	}
	if len(sql) <= 64<<10 {
		s.keyTexts[k] = out
	}
	if out == "" {
		return sql
	}
	return out
}

// keyedStmt: a statement of the client's Parsed again with the key's condition of org, under a
// name of the proxy's own on the upstream connection: Parsed once there (confirmed by its
// ParseComplete; a DEALLOCATE ALL or DISCARD ALL of the client drops it, svcEpoch), then
// Bound by every execution of that organization. Up to keyedMax per connection; past them the
// least recently used one is closed and its name takes the new statement (block 14's orgs:
// with a bound of 256 and no eviction, the pairs past it were Parsed and planned at every
// execution, 84 % of the executions at 1000 organizations and 98.6 % at 3000, planning about
// 80 % of the cloud's cost of a point read). Only when every one is in use by a portal that may
// still run (closing a statement closes its portals) is the statement Parsed for this
// execution under lokadb_alt, as one aimed at a partition.
type keyedStmt struct {
	name  string
	epoch uint32
	ready atomic.Bool
	used  uint64 // session.keyedUse at its last Bind
	batch int    // session.batchNo of its last Bind: not closed within the batch that binds it
}

// defaultKeyedStatements: keyedMax without Config.KeyedStatements. About 25 KB of the cloud
// backend's memory each (the plan of one partition and the policy's query tree): 25 MB a
// connection at most.
const defaultKeyedStatements = 1024

func (s *session) keyedMax() int {
	if n := s.srv.cfg.KeyedStatements; n > 0 {
		return n
	}
	return defaultKeyedStatements
}

// keyedVictim: the key of the least recently used keyed statement no portal is bound to and
// the batch being sent does not bind (its portals are recorded before the batch is: an
// earlier Bind of it may be one that portalInfo.up does not show yet), or "" when there is none.
func (s *session) keyedVictim() string {
	inUse := map[string]bool{}
	for _, p := range s.portals {
		if p.up != "" {
			inUse[p.up] = true
		}
	}
	victim, oldest := "", uint64(0)
	for k, ks := range s.keyedStmts {
		if !inUse[ks.name] && ks.batch != s.batchNo && (victim == "" || ks.used < oldest) {
			victim, oldest = k, ks.used
		}
	}
	return victim
}

// keyedBind: the messages that make b execute st's statement (text: as the upstream has it,
// the proxy's rewriting of it included) with the key's condition of org (b is aimed at it);
// nil when it has none to add.
func (s *session) keyedBind(st *stmtInfo, text string, b *pgproto3.Bind, org string) []outMsg {
	// the text with the key's condition from the session's cache, not parsed, walked and
	// deparsed again at every Bind (block 14's measurements: keyCond was half of the proxy's
	// CPU on a read by id, 32 clients)
	sql := s.keyedText(text, org)
	if sql == text {
		return nil
	}
	k := org + "\x00" + s.pathNow() + "\x00" + fmt.Sprint(st.oids) + "\x00" + text
	ks := s.keyedStmts[k]
	if ks == nil {
		if s.keyedStmts == nil {
			s.keyedStmts = map[string]*keyedStmt{}
		}
		if len(s.keyedStmts) < s.keyedMax() {
			s.keyedN++
			ks = &keyedStmt{name: "lokadb_k" + strconv.Itoa(s.keyedN)}
		} else if v := s.keyedVictim(); v != "" {
			// its name takes the new statement: the Close below drops the old one upstream
			ks = &keyedStmt{name: s.keyedStmts[v].name}
			delete(s.keyedStmts, v)
		}
		if ks != nil {
			s.keyedStmts[k] = ks
		}
	}
	oids := st.oids // the types the server gave the client's statement, when known (as relink)
	if st.ptypes != nil {
		oids = st.ptypes
	}
	if ks == nil { // every keyed statement is bound to a portal that may still run: Parsed for this one
		b.PreparedStatement = "lokadb_alt"
		return []outMsg{
			{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_alt"}, mode: mDrop},
			{msg: &pgproto3.Parse{Name: "lokadb_alt", Query: sql, ParameterOIDs: oids}, mode: mDrop},
		}
	}
	s.keyedUse++
	ks.used, ks.batch = s.keyedUse, s.batchNo
	b.PreparedStatement = ks.name
	e0 := s.svcEpoch.Load()
	if ks.ready.Load() && ks.epoch == e0 {
		return []outMsg{}
	}
	ks.ready.Store(false)
	ks.epoch = e0
	return []outMsg{
		{msg: &pgproto3.Close{ObjectType: 'S', Name: ks.name}, mode: mDrop},
		{msg: &pgproto3.Parse{Name: ks.name, Query: sql, ParameterOIDs: oids}, mode: mDrop, onOK: func() {
			if s.svcEpoch.Load() == e0 {
				ks.ready.Store(true)
			}
		}},
	}
}

// searchPathOrDefault: the path bare names resolve by (schemas.go), as analysis uses it.
func (s *session) searchPathOrDefault() []string {
	if s.meta != nil && s.meta.SchemasUsed {
		return s.searchPath()
	}
	return defaultPath
}
