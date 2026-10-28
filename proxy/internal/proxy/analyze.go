package proxy

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/lokadb/lokadb/proxy/internal/ext"
	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// ValueRef is a value found in SQL: a literal or a $n parameter.
type ValueRef struct {
	Const string
	Param int  // 1-based parameter number; 0 means Const is set
	Array bool // the parameter is an array: each of its elements is a value (`= ANY($1)`, `UNNEST($1)`)
	Null  bool // the literal NULL: a row of no organization (an INSERT), or nothing (a WHERE)
	// Kind: the key's kind the value is read as (keys.go: int, uuid, text; block 17), set by
	// resolveOrg from the metadata, not by the analysis; "": as it is
	Kind string
}

// Hint says "rows of Table are restricted to Column IN Values".
type Hint struct {
	Table, Column string
	Values        []ValueRef
}

// Analysis is what the proxy needs to know about one SQL string.
type Analysis struct {
	Lokadb   bool // touches a LokaDB table
	Unhinted bool // some scope reads/writes a LokaDB table with no tenant restriction
	// UnhintedTable: the first LokaDB table found without a restriction; Write: the statement
	// inserts, updates or deletes rows of a LokaDB table.
	UnhintedTable string
	// Gaps: why each unhinted scope names no organization (empty when Unhinted is false).
	Gaps  []Gap
	Write bool
	Hints []Hint // tenant restrictions from top-level AND conjuncts
	// Tuples: restrictions through a lookup of several columns (a composite key, tuples.go)
	Tuples    []TupleHint
	DDL       []string // DDL on LokaDB tables to replay in customer databases
	DDLError  string   // DDL on LokaDB tables the MVP cannot replay safely
	DDLTables []string // the LokaDB tables the DDL changes
	Forbidden string   // the statement must not run at all (LokaDB's own objects, FDW commands)
	// FanoutByParam: Forbidden only for a set_config whose name is not a constant (setsFanout)
	FanoutByParam bool
	Commit        bool
	Rollback      bool
	TxControl     bool   // BEGIN/COMMIT/ROLLBACK/SAVEPOINT...: never inject anything before it
	Rewritten     string // SQL to send instead of the original ("" = unchanged)
	// Prelude: statements the proxy runs right before this one, after lokadb.org and the
	// pause lock, one statement each (COPY FROM: the staging table it loads into).
	Prelude []string
	// OwnLock: the statement takes the pause lock itself (orgLock): COPY FROM, in the
	// trigger that writes its rows. A lock the proxy took before an autocommit COPY FROM
	// would need its unlock while the server is in copy-in mode.
	OwnLock bool
	// Target/TargetSQL: a single-table SELECT, UPDATE or DELETE of a LokaDB table that may be sent
	// to one organization's partition directly (TargetSQL has the relation replaced by
	// targetMark). Target tells it may; the text is built when a partition is first addressed
	// (targetText: PF4 of the performance review, a Deparse most statements never need). TargetSQL
	// itself is set at once only for a native upsert of a cloud organization (TargetLocal).
	Target, TargetSQL string
	lazy              *lazyTarget
	// TargetLocal: TargetSQL is an upsert in its native form, for the local partition of a
	// cloud organization (Meta.Local, else DEFAULT) instead of a foreign one: atomic ON
	// CONFLICT where the rewritten upsert is not (block 2, item 9). Rewritten stays the
	// statement for an organization in its own database.
	TargetLocal bool
	// UpsertLock: the LokaDB table of a rewritten upsert (NOT EXISTS + INSERT, which does not
	// see another transaction's uncommitted insert of the same key): the proxy runs it under
	// an exclusive lock of the table and organization (upsertLock; block 2, item 9 (b)).
	UpsertLock string
	// Tag: the command tag the client gets for a statement the proxy sends as a SELECT of a
	// lokadb function (GRANT, REVOKE through lokadb.grant_sync); "" = the server's own.
	Tag        string
	ParseError bool
	// MovesOrg: an UPDATE sets the organization column of a LokaDB table: the rows may land
	// in any organization (the pause of a move holds them all, see orgLock).
	MovesOrg bool
	// ChangesLookups: the statement may change what a lookup answers: an UPDATE of an
	// organization column or a key (routing.keys), of a cloud table of routing.cloud_tables,
	// a DELETE that frees a key. Sessions then forget the organizations they cached (P7).
	ChangesLookups bool

	// Events: what the string does to the transaction and to the schema of LokaDB tables, in
	// order. The proxy applies them once the cloud has run the string (or the statement)
	// successfully: DDL is replayed in organizations' databases only as the cloud ran it.
	Events []TxEvent
	// Stmts: the text of each top-level statement.
	Stmts []string

	// Utility: every statement reads no table (SET, SHOW, VACUUM, LISTEN…): lokadb.org is
	// not set before it; some of them must come first in a transaction or outside one.
	// ResetsOrg: it may change lokadb.org itself (SET/RESET lokadb.org, RESET ALL, DISCARD ALL,
	// set_config, UPDATE pg_settings: see changesOrg). With a LokaDB statement it is refused.
	Utility, ResetsOrg bool
	// MayResetOrg: it calls what may set lokadb.org out of the proxy's sight (a function that is
	// not one of the server's plain ones, a procedure, a DO block): the value upstream is
	// not known after it (second review of the protocol, P8: PF3 skips a set_config equal to
	// the last one, so a value changed there lived on for the session).
	MayResetOrg bool

	// Reverts: a ROLLBACK, ROLLBACK TO or ROLLBACK PREPARED, which also undoes set_config.
	// OrgReverted: one of them comes before a statement on a LokaDB table, so lokadb.org set
	// before the string would not hold for that statement.
	Reverts, OrgReverted bool

	// outer: the scope a subquery's correlated references resolve in, for the subqueries
	// of scopes that name their organization (scope)
	outer map[*pg.SelectStmt]*scope

	fp     string // pg_query fingerprint, computed when a fanout allow list needs it
	logged string // the SQL as the route log shows it (literals redacted)
}

func (a *Analysis) unhinted(table string) {
	a.Unhinted = true
	if a.UnhintedTable == "" {
		a.UnhintedTable = table
	}
}

// changesLookups: see Analysis.ChangesLookups.
func changesLookups(n *pg.Node, m *Meta) bool {
	if m == nil {
		return false
	}
	var rv *pg.RangeVar
	var cols []string
	switch {
	case n.GetUpdateStmt() != nil:
		rv = n.GetUpdateStmt().Relation
		for _, tl := range n.GetUpdateStmt().TargetList {
			cols = append(cols, tl.GetResTarget().GetName())
		}
	case n.GetDeleteStmt() != nil:
		rv = n.GetDeleteStmt().Relation
	default:
		return false
	}
	if rv == nil {
		return false
	}
	t := m.table(rv)
	if t == nil {
		return m.cloud(relKey(rv)) != ""
	}
	if n.GetDeleteStmt() != nil {
		for _, refs := range m.Lookups[t.Name] {
			if _, key := keyRefs(refs); key {
				return true
			}
		}
		return false
	}
	for _, c := range cols {
		if _, key := keyRefs(m.Lookups[t.Name][c]); key || c == t.TenantCol {
			return true
		}
	}
	return false
}

// keyRefs: refs is the map of a key (routing.keys, "lokadb.<table>__key__<column>"); k is
// the map's table in schema lokadb. Its values are text, not ids.
func keyRefs(refs string) (k string, ok bool) { return strings.CutPrefix(refs, "lokadb.") }

// Kinds of Gap.
const (
	GapNoCondition     = "no_condition"     // the table has no condition at all
	GapUnknownColumn   = "unknown_column"   // a condition by value on a column without an organization lookup
	GapUnsupportedForm = "unsupported_form" // a form the analyzer does not read (OR, <> ANY, IN (SELECT …), INSERT … SELECT)
	GapUnreadableValue = "unreadable_value" // a column with a lookup compared with an expression instead of a value
)

// Gap is why a scope reading or writing a LokaDB table names no organization: what `lokadb
// check` builds suggestions from and the proxy logs when it refuses the statement.
type Gap struct {
	Kind   string `json:"kind"`
	Table  string `json:"table"`
	Column string `json:"column,omitempty"`
	// Form: the condition ("=", "IN", "= ANY", "<> ANY", "OR", "IN (SELECT …)", "INSERT")
	// or the statement form ("INSERT … SELECT").
	Form string `json:"form,omitempty"`
	// Cloud: Table is a cloud table of the statement (a value bound to its column could name
	// the organization if the table were declared in routing.cloud_tables).
	Cloud bool `json:"cloud,omitempty"`
}

func (g Gap) String() string {
	s := g.Kind + " " + g.Table
	if g.Column != "" {
		s += "." + g.Column
	}
	if g.Form != "" {
		s += " (" + g.Form + ")"
	}
	return s
}

// gap records why a scope is unhinted (once per kind, table, column and form).
func (a *Analysis) gap(g Gap) {
	for _, x := range a.Gaps {
		if x == g {
			return
		}
	}
	a.Gaps = append(a.Gaps, g)
}

// GapText: the reasons the statement names no organization, for a log or an error.
func (a *Analysis) GapText() string {
	var out []string
	for _, g := range a.Gaps {
		out = append(out, g.String())
	}
	return strings.Join(out, "; ")
}

// TxEvent is one step of a transaction that matters for the DDL replay.
type TxEvent struct {
	Kind byte   // 'D' DDL on a LokaDB table, 'B' begin, 'S' savepoint, 'R' release, 'T' rollback to, 'X' rollback, 'C' commit
	SQL  string // the DDL, or the savepoint's name
}

// Analyze is what the proxy would decide for sql: `lokadb check` runs the workload through it.
func Analyze(sql string, m *Meta) *Analysis { return analyze(sql, m) }

// AnalyzeIn is Analyze for a session whose search_path is path (nil: not known now, see qualify).
func AnalyzeIn(sql string, m *Meta, path []string) *Analysis { return analyzeIn(sql, m, path) }

// Walk calls fn for every message of a parse tree, depth first.
func Walk(m proto.Message, fn func(proto.Message)) { walk(m.ProtoReflect(), fn) }

func analyze(sql string, m *Meta) *Analysis { return analyzeIn(sql, m, defaultPath) }

// parseQualified parses sql and gives its bare names the schema the session's search_path
// resolves them in (schemas.go); ambiguous: the first name it cannot resolve.
func parseQualified(sql string, m *Meta, path []string) (tree *pg.ParseResult, ambiguous string, err error) {
	if tree, err = pg.Parse(sql); err != nil {
		return nil, "", err
	}
	for _, raw := range tree.Stmts {
		if name := qualify(raw.Stmt, m, path); name != "" && ambiguous == "" {
			ambiguous = name
		}
	}
	return tree, ambiguous, nil
}

func analyzeIn(sql string, m *Meta, path []string) *Analysis {
	a := &Analysis{}
	tree, ambiguous, err := parseQualified(sql, m, path)
	if err != nil {
		a.ParseError = true
		a.unparsed(sql, m)
		a.denied(m)
		return a
	}
	if ambiguous != "" {
		a.Forbidden = ambiguousMsg(ambiguous)
	}
	var out strings.Builder
	prev, changed, reverted := 0, false, false
	for _, raw := range tree.Stmts {
		start := int(raw.StmtLocation)
		end := len(sql)
		if raw.StmtLen > 0 {
			end = start + int(raw.StmtLen)
		}
		if msg := forbidden(raw.Stmt, m); msg != "" && a.Forbidden == "" {
			a.Forbidden = msg
		}
		if sets, byParam := setsFanout(raw.Stmt); sets && a.Forbidden == "" {
			a.Forbidden, a.FanoutByParam = fanoutSetMsg, byParam
		}
		if u := raw.Stmt.GetUpdateStmt(); u != nil {
			if t := m.table(u.Relation); t != nil {
				for _, tl := range u.TargetList {
					a.MovesOrg = a.MovesOrg || tl.GetResTarget().GetName() == t.TenantCol
				}
			}
		}
		a.ChangesLookups = a.ChangesLookups || changesLookups(raw.Stmt, m)
		a.MayResetOrg = a.MayResetOrg || callsOwn(raw.Stmt)
		was, ddls := a.Lokadb, len(a.DDL)
		a.Lokadb = false
		a.Stmts = append(a.Stmts, sql[start:end])
		repl := a.statement(raw.Stmt, sql[start:end], m)
		if len(a.DDL) > ddls {
			a.Events = append(a.Events, TxEvent{Kind: 'D', SQL: a.DDL[len(a.DDL)-1]})
			// it has to run in organizations whose tables are not in public too (ddl.go orgDDL):
			// refused here, before the cloud runs it, if it cannot be rewritten for them
			if _, err := orgDDL(a.DDL[len(a.DDL)-1], "lokadb", m); err != nil {
				a.DDLError = "lokadb: a schema change that names public.<table> could not be rewritten for organizations whose tables are in another schema: " + err.Error()
			}
		}
		if ts := raw.Stmt.GetTransactionStmt(); ts != nil {
			switch ts.Kind {
			case pg.TransactionStmtKind_TRANS_STMT_BEGIN, pg.TransactionStmtKind_TRANS_STMT_START:
				a.Events = append(a.Events, TxEvent{Kind: 'B'})
			case pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
				a.Events = append(a.Events, TxEvent{Kind: 'S', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_RELEASE:
				a.Events = append(a.Events, TxEvent{Kind: 'R', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
				a.Events = append(a.Events, TxEvent{Kind: 'T', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
				a.Events = append(a.Events, TxEvent{Kind: 'X'})
			case pg.TransactionStmtKind_TRANS_STMT_COMMIT:
				a.Events = append(a.Events, TxEvent{Kind: 'C'})
			}
		}
		if a.Lokadb && reverted {
			a.OrgReverted = true
		}
		a.Lokadb = a.Lokadb || was
		if ts := raw.Stmt.GetTransactionStmt(); ts != nil {
			switch ts.Kind {
			case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO,
				pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_PREPARED:
				a.Reverts, reverted = true, true
			}
		}
		out.WriteString(sql[prev:start])
		if repl != "" {
			out.WriteString(repl)
			changed = true
		} else {
			out.WriteString(sql[start:end])
		}
		prev = end
	}
	out.WriteString(sql[prev:])
	if changed {
		a.Rewritten = out.String()
	}
	a.Utility = len(tree.Stmts) > 0
	for _, raw := range tree.Stmts {
		n := raw.Stmt
		if changesOrg(n) { // set_config('lokadb.org', …), UPDATE pg_settings (orgset.go)
			a.ResetsOrg = true
		}
		switch {
		case n.GetVariableSetStmt() != nil:
			vs := n.GetVariableSetStmt()
			if vs.Kind == pg.VariableSetKind_VAR_RESET_ALL || strings.HasPrefix(strings.ToLower(vs.Name), "lokadb.") && !strings.EqualFold(vs.Name, TenantSetting) {
				a.ResetsOrg = true
			}
		case n.GetDiscardStmt() != nil:
			a.ResetsOrg = true
		case n.GetVariableShowStmt() != nil, n.GetVacuumStmt() != nil, n.GetListenStmt() != nil,
			n.GetUnlistenStmt() != nil, n.GetNotifyStmt() != nil, n.GetDeallocateStmt() != nil, n.GetCheckPointStmt() != nil:
		default:
			a.Utility = false
		}
	}
	if len(tree.Stmts) == 1 && a.Rewritten == "" && a.DDLError == "" && len(a.DDL) == 0 {
		a.target(tree.Stmts[0].Stmt, m, sql, path)
	}
	if len(tree.Stmts) != 1 || a.DDLError != "" || len(a.DDL) > 0 {
		a.Target, a.TargetSQL, a.TargetLocal, a.lazy = "", "", false, nil
	}
	if len(tree.Stmts) != 1 { // the lock is taken before the string, not before its upsert
		a.UpsertLock = ""
	}
	a.denied(m)
	return a
}

// denied refuses a statement on a LokaDB table when the proxy could not read the metadata
// (Meta.Denied): it cannot route it, and unrouted it would pass the fanout policy.
func (a *Analysis) denied(m *Meta) {
	if m != nil && m.Denied != "" && a.Lokadb {
		a.Forbidden = m.Denied
	}
}

const targetMark = "__lokadb_target__"

// target decides whether a single-table SELECT, UPDATE or DELETE may be sent to the partition
// of its organization instead of the parent (Analysis.Target), and leaves the statement for it to
// targetText, which builds it when a partition is first addressed. The parent's RLS filter (it
// reads current_setting) cannot be shipped to a foreign server, so through the parent
// postgres_fdw has to fetch each row and update it by ctid; addressed directly, the partition has
// no RLS and the whole statement can run on the organization's database in one round trip.
// The decision walks the tree without copying it; the text is a copy, a change of it and a
// Deparse (cgo), 43% of the analysis of a new text (PF4): most statements never reach a moved
// organization.
func (a *Analysis) target(n *pg.Node, m *Meta, sql string, path []string) {
	if t, _ := targetShape(n, m); t != nil {
		a.Target, a.lazy = t.Name, &lazyTarget{sql: sql, m: m, path: path}
	}
}

// targetShape: the LokaDB table of a statement the partition can take, and its relation; nil for
// the cloud's way: a single table (no FROM or USING, WITH, INTO, set operation), no other LokaDB
// table in it, an UPDATE that does not set the organization column.
func targetShape(n *pg.Node, m *Meta) (*Table, *pg.RangeVar) {
	var rv *pg.RangeVar
	switch {
	case n.GetUpdateStmt() != nil:
		st := n.GetUpdateStmt()
		if len(st.FromClause) > 0 || st.WithClause != nil {
			return nil, nil
		}
		rv = st.Relation
	case n.GetDeleteStmt() != nil:
		st := n.GetDeleteStmt()
		if len(st.UsingClause) > 0 || st.WithClause != nil {
			return nil, nil
		}
		rv = st.Relation
	case n.GetSelectStmt() != nil:
		// A SELECT of one table: through the parent nothing above the scan (GROUP BY,
		// aggregates, ORDER BY … LIMIT) reaches the organization's database, the RLS filter
		// stays in the cloud; addressed directly the partition gets the whole query.
		st := n.GetSelectStmt()
		if st.Op != pg.SetOperation_SETOP_NONE || st.WithClause != nil || st.IntoClause != nil || len(st.FromClause) == 0 {
			return nil, nil
		}
		if rv = st.FromClause[0].GetRangeVar(); len(st.FromClause) > 1 || rv == nil {
			// A join of the LokaDB table with cloud tables (Django's select_related): through the
			// parent the planner may take the whole organization from the partition and sort it
			// in the cloud; with the partition in its place ORDER BY reaches the organization.
			if rv = joinTarget(st.FromClause, m); rv == nil {
				return nil, nil
			}
			for _, x := range st.TargetList { // a bare * would come out in the partition's column order
				if cr := x.GetResTarget().GetVal().GetColumnRef(); cr != nil && len(cr.Fields) == 1 && cr.Fields[0].GetAStar() != nil {
					return nil, nil
				}
			}
		}
		// column aliases, documents d(i, o), rename the parent's columns: the organization's
		// condition and * in the parent's column order would name columns the alias hides
		if rv.Alias != nil && len(rv.Alias.Colnames) > 0 {
			return nil, nil
		}
	default:
		return nil, nil
	}
	t := m.table(rv)
	if t == nil {
		return nil, nil
	}
	lokadbRefs := 0
	walk(n.ProtoReflect(), func(msg proto.Message) {
		if r, ok := msg.(*pg.RangeVar); ok && m.table(r) != nil {
			lokadbRefs++
		}
	})
	if lokadbRefs != 1 { // subqueries over LokaDB tables need the parent's routing
		return nil, nil
	}
	// Moving a row to another organization must go through the parent (and its checks).
	if st := n.GetUpdateStmt(); st != nil {
		for _, tl := range st.TargetList {
			if tl.GetResTarget().GetName() == t.TenantCol {
				return nil, nil
			}
		}
	}
	return t, rv
}

// joinTarget: the LokaDB table among FROM items joined with others, nil when there is not exactly
// one or it may not take the partition's place. The statement keeps to the organization by a
// condition in WHERE, which on the nullable side of an outer join would drop the rows the join
// keeps without a match: there, inside a join with an alias of its own (the table's name is
// hidden behind it) and inside a subquery it goes through the parent.
func joinTarget(items []*pg.Node, m *Meta) *pg.RangeVar {
	var found []*pg.RangeVar
	ok := true
	var visit func(n *pg.Node, nullable, aliased bool)
	visit = func(n *pg.Node, nullable, aliased bool) {
		switch {
		case n.GetRangeVar() != nil:
			if m.table(n.GetRangeVar()) != nil {
				found = append(found, n.GetRangeVar())
				ok = ok && !nullable && !aliased
			}
		case n.GetJoinExpr() != nil:
			j := n.GetJoinExpr()
			aliased = aliased || j.Alias != nil
			visit(j.Larg, nullable || j.Jointype == pg.JoinType_JOIN_RIGHT || j.Jointype == pg.JoinType_JOIN_FULL, aliased)
			visit(j.Rarg, nullable || j.Jointype == pg.JoinType_JOIN_LEFT || j.Jointype == pg.JoinType_JOIN_FULL, aliased)
		}
	}
	for _, it := range items {
		visit(it, false, false)
	}
	if len(found) != 1 || !ok {
		return nil
	}
	return found[0]
}

// lazyTarget is the statement of Analysis.Target, not built yet: its text, with what the
// analysis was made with (the metadata, the search path), so that it is built as the analysis
// would have (the parse tree is not kept: a session caches thousands of analyses, PF7).
type lazyTarget struct {
	sql  string
	m    *Meta
	path []string
	once sync.Once
	text string
}

// targetText is TargetSQL: the statement aimed at a partition, "" when there is none or it
// cannot be made.
func (a *Analysis) targetText() string {
	if a.TargetSQL != "" || a.lazy == nil {
		return a.TargetSQL
	}
	return a.lazy.build()
}

func (l *lazyTarget) build() string {
	l.once.Do(func() {
		tree, _, err := parseQualified(l.sql, l.m, l.path)
		if err != nil || len(tree.Stmts) != 1 {
			return
		}
		n := tree.Stmts[0].Stmt
		t, rv := targetShape(n, l.m)
		if t == nil {
			return
		}
		l.text = buildTarget(n, rv, t, l.m)
	})
	return l.text
}

// buildTarget makes the text of targetShape's statement: the relation named targetMark (the
// partition goes there), the organization's value orgMark, which targetSQL replaces.
func buildTarget(n *pg.Node, rv *pg.RangeVar, t *Table, m *Meta) string {
	if rv.Alias == nil || rv.Alias.Aliasname == "" {
		// the statement is changed below: work on a copy
		n = proto.Clone(n).(*pg.Node)
		_, rv = targetShape(n, m)
		unschema(n, rv, rv.Relname)
	}
	orig, alias, schema := rv.Relname, rv.Alias, rv.Schemaname
	if rv.Alias == nil || rv.Alias.Aliasname == "" {
		rv.Alias = &pg.Alias{Aliasname: orig} // "documents"."id" keeps resolving
	}
	// The partition has none of the parent's policies: the statement itself keeps to the
	// organization, so rows the organization's database holds for another one (it controls
	// that database) are neither changed nor returned.
	if st := n.GetSelectStmt(); st != nil && len(t.Cols) > 0 {
		// * in the parent's column order: the client has the parent's row description (a join
		// with a bare * is not addressed, targetShape; alias.* of the table is expanded here)
		var tl []*pg.Node
		for _, x := range st.TargetList {
			cr := x.GetResTarget().GetVal().GetColumnRef()
			if cr == nil || cr.Fields[len(cr.Fields)-1].GetAStar() == nil || len(cr.Fields) > 2 ||
				len(cr.Fields) == 2 && cr.Fields[0].GetString_().GetSval() != rv.Alias.Aliasname {
				tl = append(tl, x)
				continue
			}
			for _, c := range t.Cols {
				tl = append(tl, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(rv.Alias.Aliasname), strNode(c)}}}}}}})
			}
		}
		origTL := st.TargetList
		st.TargetList = tl
		defer func() { st.TargetList = origTL }()
	}
	var where **pg.Node
	switch {
	case n.GetDeleteStmt() != nil:
		where = &n.GetDeleteStmt().WhereClause
	case n.GetUpdateStmt() != nil:
		where = &n.GetUpdateStmt().WhereClause
	default:
		where = &n.GetSelectStmt().WhereClause
	}
	origWhere := *where
	str := func(v string) *pg.Node { return &pg.Node{Node: &pg.Node_String_{String_: &pg.String{Sval: v}}} }
	col := func(fs ...string) *pg.Node {
		var f []*pg.Node
		for _, x := range fs {
			f = append(f, str(x))
		}
		return &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: f}}}
	}
	cond := &pg.Node{Node: &pg.Node_AExpr{AExpr: &pg.A_Expr{Kind: pg.A_Expr_Kind_AEXPR_OP, Name: []*pg.Node{str("=")},
		Lexpr: col(rv.Alias.Aliasname, t.TenantCol), Rexpr: col(orgMark)}}}
	if origWhere != nil {
		cond = &pg.Node{Node: &pg.Node_BoolExpr{BoolExpr: &pg.BoolExpr{Boolop: pg.BoolExprType_AND_EXPR, Args: []*pg.Node{origWhere, cond}}}}
	}
	*where = cond
	rv.Relname, rv.Schemaname = targetMark, ""
	s, err := deparseStmt(n)
	rv.Relname, rv.Alias, rv.Schemaname = orig, alias, schema
	*where = origWhere
	if err == nil && strings.Count(s, targetMark) == 1 && strings.Count(s, orgMark) == 1 {
		return s
	}
	return ""
}

// orgMark stands for the organization's value in TargetSQL.
const orgMark = "__lokadb_org__"

// targetSQL returns the statement aimed at partition part, kept to organization org: an
// integer as it is, a uuid (block 18) as a literal of the organization column's type.
func (a *Analysis) targetSQL(part, org string) string {
	if !intRe.MatchString(org) {
		org = sqlString(org)
	}
	return strings.Replace(strings.Replace(a.targetText(), targetMark, qrel(part), 1), orgMark, org, 1)
}

var allowedAlter = map[pg.AlterTableType]bool{
	pg.AlterTableType_AT_AddColumn:       true,
	pg.AlterTableType_AT_DropColumn:      true,
	pg.AlterTableType_AT_ColumnDefault:   true,
	pg.AlterTableType_AT_SetNotNull:      true,
	pg.AlterTableType_AT_DropNotNull:     true,
	pg.AlterTableType_AT_AlterColumnType: true,
}

// statement analyzes one top-level statement and returns its replacement text, if any.
func (a *Analysis) statement(n *pg.Node, text string, m *Meta) string {
	switch {
	case n.GetTransactionStmt() != nil:
		a.TxControl = true
		switch n.GetTransactionStmt().Kind {
		case pg.TransactionStmtKind_TRANS_STMT_COMMIT:
			a.Commit = true
		case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			a.Rollback = true
		}
		return ""
	case n.GetAlterTableStmt() != nil:
		st := n.GetAlterTableStmt()
		if m.table(st.Relation) == nil {
			return ""
		}
		for _, c := range st.Cmds {
			cmd := c.GetAlterTableCmd()
			if cmd == nil || !allowedAlter[cmd.Subtype] {
				a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s: only column changes are supported on LokaDB tables", st.Relation.Relname)
				continue
			}
			// a new column may say NULL, NOT NULL and DEFAULT; a key or a reference there
			// would be an index or a constraint, which is not replayed
			if col := cmd.GetDef().GetColumnDef(); col != nil {
				for _, cn := range col.Constraints {
					switch cn.GetConstraint().GetContype() {
					case pg.ConstrType_CONSTR_NULL, pg.ConstrType_CONSTR_NOTNULL, pg.ConstrType_CONSTR_DEFAULT:
					default:
						a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s ADD COLUMN %s: only NULL, NOT NULL and DEFAULT are supported on a new column of a LokaDB table; add keys, references and checks as statements of their own", st.Relation.Relname, col.Colname)
					}
				}
			}
		}
		if msg := unsafeExpr(n); msg != "" {
			a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s: %s", st.Relation.Relname, msg)
		}
		a.DDL = append(a.DDL, text)
		a.DDLTables = append(a.DDLTables, st.Relation.Relname)
		return ""
	case n.GetIndexStmt() != nil:
		st := n.GetIndexStmt()
		t := m.table(st.Relation)
		if t == nil {
			return ""
		}
		if msg := unsafeExpr(n); msg != "" {
			a.DDLError = fmt.Sprintf("lokadb: CREATE INDEX on %s: %s", t.Name, msg)
		}
		// block 7: CREATE INDEX CONCURRENTLY is built partition by partition (concurrent.go)
		a.DDL = append(a.DDL, text)
		a.DDLTables = append(a.DDLTables, t.Name)
		if st.Unique {
			// A unique index cannot exist on a parent with foreign partitions: build it on
			// every local partition (one per organization + DEFAULT); customers get the
			// original statement.
			orig := st.Idxname
			st.Relation.Relname, st.Relation.Schemaname = "__lokadb_part__", ""
			if orig != "" {
				st.Idxname = "__lokadb_idx__"
			}
			if s, err := deparseStmt(n); err == nil {
				return fmt.Sprintf("SELECT lokadb.create_index(%s, %s, %s)", sqlString(t.Name), sqlString(s), sqlString(orig))
			}
		}
		return ""
	case n.GetGrantStmt() != nil:
		// The proxy sends a statement of one organization in its own database straight to its
		// foreign partition: the partition needs the privileges the parent has. lokadb.grant_sync
		// runs the GRANT or REVOKE and gives the parent's privileges to its foreign partitions.
		gs := n.GetGrantStmt()
		if m == nil || !m.GrantSync || gs.Objtype != pg.ObjectType_OBJECT_TABLE {
			return ""
		}
		var names []string
		switch gs.Targtype {
		case pg.GrantTargetType_ACL_TARGET_OBJECT:
			for _, o := range gs.Objects {
				if t := m.table(o.GetRangeVar()); t != nil {
					names = append(names, sqlString(t.Name))
				}
			}
		case pg.GrantTargetType_ACL_TARGET_ALL_IN_SCHEMA:
			for name := range m.Tables {
				names = append(names, sqlString(name))
			}
		}
		if len(names) == 0 {
			return ""
		}
		sort.Strings(names)
		a.Tag = "REVOKE"
		if gs.IsGrant {
			a.Tag = "GRANT"
		}
		return fmt.Sprintf("SELECT lokadb.grant_sync(%s, ARRAY[%s]::text[])", sqlString(text), strings.Join(names, ", "))
	case n.GetRenameStmt() != nil:
		r := n.GetRenameStmt()
		if r.RenameType == pg.ObjectType_OBJECT_INDEX && m != nil && r.Relation != nil && m.indexOf(r.Relation.Schemaname, r.Relation.Relname) != "" {
			// block 7: an index of a LokaDB table (Rails renames those of a renamed column).
			// The organization's copy may not have it under this name (one built by move-org
			// is named as DEFAULT's): IF EXISTS there.
			c := proto.Clone(r).(*pg.RenameStmt)
			c.MissingOk = true
			if q, err := deparseStmt(&pg.Node{Node: &pg.Node_RenameStmt{RenameStmt: c}}); err == nil {
				a.DDL = append(a.DDL, q)
				a.DDLTables = append(a.DDLTables, m.indexOf(r.Relation.Schemaname, r.Relation.Relname))
			}
			return ""
		}
		t := m.table(r.Relation)
		if t == nil {
			return ""
		}
		// block 7: RENAME COLUMN (Rails rename_column) is replayed; the table's own name and
		// the names of its constraints are what enable and move-org recorded
		switch {
		case r.RenameType != pg.ObjectType_OBJECT_COLUMN:
			a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s RENAME: only RENAME COLUMN is supported on LokaDB tables", t.Name)
		case r.Subname == t.TenantCol || r.Subname == "id":
			a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s RENAME COLUMN %s: lokadb routes the table's rows by this column", t.Name, r.Subname)
		case m.Lookups[t.Name][r.Subname] != "":
			a.DDLError = fmt.Sprintf("lokadb: ALTER TABLE %s RENAME COLUMN %s: lokadb finds the organization of a row by this column (lokadb.lookups)", t.Name, r.Subname)
		}
		a.DDL = append(a.DDL, text)
		a.DDLTables = append(a.DDLTables, t.Name)
		return ""
	case n.GetTruncateStmt() != nil:
		for _, r := range n.GetTruncateStmt().Relations {
			if t := m.table(r.GetRangeVar()); t != nil {
				a.DDLError = fmt.Sprintf("lokadb: TRUNCATE %s would empty the table in every organization's own database: use DELETE", t.Name)
			}
		}
		return ""
	case n.GetCreatePolicyStmt() != nil:
		return a.policyStmt(m.table(n.GetCreatePolicyStmt().Table), text, "CREATE POLICY")
	case n.GetDropStmt() != nil && n.GetDropStmt().RemoveType == pg.ObjectType_OBJECT_POLICY:
		for _, o := range n.GetDropStmt().Objects {
			if l := o.GetList(); l != nil && len(l.Items) >= 2 {
				it := l.Items
				rv := &pg.RangeVar{Relname: it[len(it)-2].GetString_().GetSval()}
				if len(it) >= 3 {
					rv.Schemaname = it[len(it)-3].GetString_().GetSval()
				}
				return a.policyStmt(m.table(rv), text, "DROP POLICY")
			}
		}
		return ""
	case n.GetCreateExtensionStmt() != nil:
		// block 7 (extensions after a move): an extension the application installs in the cloud
		// is installed in the moved organizations' databases too, by the same replay
		if m != nil && !ext.CloudOnly(n.GetCreateExtensionStmt().Extname) {
			a.DDL = append(a.DDL, text)
		}
		return ""
	case n.GetAlterExtensionStmt() != nil: // ALTER EXTENSION … UPDATE [TO version]
		if m != nil && !ext.CloudOnly(n.GetAlterExtensionStmt().Extname) {
			a.DDL = append(a.DDL, text)
		}
		return ""
	case n.GetDropStmt() != nil && n.GetDropStmt().RemoveType == pg.ObjectType_OBJECT_EXTENSION:
		if m != nil {
			for _, o := range n.GetDropStmt().Objects {
				if !ext.CloudOnly(objName(o)) {
					a.DDL = append(a.DDL, text)
					break
				}
			}
		}
		return ""
	case n.GetDropStmt() != nil:
		st := n.GetDropStmt()
		if st.RemoveType == pg.ObjectType_OBJECT_INDEX && m != nil {
			for _, o := range st.Objects {
				if l := o.GetList(); l != nil && len(l.Items) > 0 {
					name := l.Items[len(l.Items)-1].GetString_().GetSval()
					schema := ""
					if len(l.Items) >= 2 {
						schema = l.Items[len(l.Items)-2].GetString_().GetSval()
					}
					if t := m.indexOf(schema, name); t != "" {
						a.DDLError = fmt.Sprintf("lokadb: DROP INDEX %s: indexes of LokaDB tables (here %s) cannot be dropped yet: the organizations' databases name them differently", name, t)
					}
				}
			}
		}
		if st.RemoveType == pg.ObjectType_OBJECT_TABLE {
			for _, o := range st.Objects {
				if l := o.GetList(); l != nil && len(l.Items) > 0 {
					if k := listKey(l.Items); m.Tables[k] != nil || len(l.Items) == 1 && m.named(k) {
						a.DDLError = "lokadb: DROP TABLE on LokaDB tables is not supported"
					}
				}
			}
		}
		return ""
	}

	if do := n.GetDoStmt(); do != nil {
		a.doBlock(do, m)
		return ""
	}
	if ms := n.GetMergeStmt(); ms != nil {
		if t := m.table(ms.Relation); t != nil {
			a.scan(n, m)
			s, refusal := rewriteMerge(ms, t)
			if refusal != "" {
				a.Forbidden = refusal
			}
			return s
		}
	}
	if cs := n.GetCopyStmt(); cs != nil {
		if t := m.table(cs.Relation); t != nil {
			return a.copyTable(n, cs, t, m)
		}
	}
	if u := n.GetUpdateStmt(); u != nil && m != nil && m.SameOrg {
		if t := m.table(u.Relation); t != nil {
			if s := a.sameOrg(n, u, t, m); s != "" {
				return s
			}
		}
	}
	a.scan(n, m)
	if ins := n.GetInsertStmt(); ins != nil && ins.OnConflictClause != nil {
		if t := m.table(ins.Relation); t != nil {
			native := proto.Clone(n).(*pg.Node) // rewriteUpsert changes the tree
			if s, ok := rewriteUpsert(n, ins, t); ok {
				a.UpsertLock = t.Name
				a.localUpsert(native, native.GetInsertStmt(), t, m)
				return s
			}
		}
	}
	return ""
}

// copyTable handles COPY of a LokaDB table itself (not of a query). Under FORCE ROW LEVEL
// SECURITY Postgres runs `COPY t TO` as a SELECT FROM ONLY t: the partitioned parent has no
// rows of its own, and the copy came out empty without an error. As COPY (SELECT … FROM t)
// it reads the partitions and is a SELECT for the routing and the fanout policy. COPY FROM
// Postgres refuses under RLS altogether.
func (a *Analysis) copyTable(n *pg.Node, cs *pg.CopyStmt, t *Table, m *Meta) string {
	a.Lokadb = true
	if cs.IsFrom {
		a.Write = true
		return a.copyFrom(n, cs, t, m)
	}
	var targets []*pg.Node
	for _, c := range cs.Attlist {
		targets = append(targets, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(c.GetString_().GetSval())}}}}}}})
	}
	if len(targets) == 0 {
		targets = []*pg.Node{{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{{Node: &pg.Node_AStar{AStar: &pg.A_Star{}}}}}}}}}}}
	}
	sel := &pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
		TargetList:  targets,
		FromClause:  []*pg.Node{{Node: &pg.Node_RangeVar{RangeVar: cs.Relation}}},
		Op:          pg.SetOperation_SETOP_NONE,
		LimitOption: pg.LimitOption_LIMIT_OPTION_DEFAULT,
	}}}
	a.scan(sel, m)
	rel, cols := cs.Relation, cs.Attlist
	cs.Query, cs.Relation, cs.Attlist = sel, nil, nil
	s, err := deparseStmt(n)
	cs.Query, cs.Relation, cs.Attlist = nil, rel, cols
	if err != nil {
		a.Forbidden = fmt.Sprintf("lokadb: COPY %s TO: use COPY (SELECT … FROM %s) TO", t.Name, t.Name)
		return ""
	}
	return s
}

// copyFrom: Postgres refuses COPY FROM into a table with row-level security. The rows are
// copied into a staging table of the session (pg_temp) instead, and a trigger at the end of
// that COPY inserts them into the table: through the parent, its policy, defaults and
// partitions, as INSERT … SELECT. An error there is the COPY's error, and its tag counts the
// rows. The rows carry their organization in the tenant column; a child table's rows get
// it from the parent's refs (no parent: no organization; a parent not found: -1, which the
// database refuses).
func (a *Analysis) copyFrom(n *pg.Node, cs *pg.CopyStmt, t *Table, m *Meta) string {
	if cs.IsProgram || cs.Filename != "" {
		a.Forbidden = fmt.Sprintf("lokadb: COPY %s FROM a file or a program is not supported on LokaDB tables: use COPY FROM STDIN (psql \\copy)", t.Name)
		return ""
	}
	var cols []string
	for _, c := range cs.Attlist {
		cols = append(cols, c.GetString_().GetSval())
	}
	if len(cols) == 0 {
		cols = t.Cols
	}
	if len(cols) == 0 {
		a.Forbidden = fmt.Sprintf("lokadb: COPY FROM into %s: name its columns", t.Name)
		return ""
	}
	q := quoteIdent
	stage := q("__lokadb_copy_" + t.Name)
	var qcols, src []string
	hasTenant := false
	for _, c := range cols {
		qcols = append(qcols, q(c))
		src = append(src, "s."+q(c))
		hasTenant = hasTenant || c == t.TenantCol
	}
	if !hasTenant && t.TenantCol != "" {
		var parent, refs string
		for _, c := range cols {
			if r := m.Lookups[t.Name][c]; r != "" && r != t.Refs {
				if parent != "" { // two parents: leave the organization to the default
					parent = ""
					break
				}
				parent, refs = c, r
			}
		}
		if parent != "" {
			qcols = append(qcols, q(t.TenantCol))
			src = append(src, fmt.Sprintf("CASE WHEN s.%s IS NULL THEN NULL ELSE coalesce((SELECT r.tenant FROM %s r WHERE r.id = s.%s), CASE WHEN EXISTS (SELECT 1 FROM %s r WHERE r.id = s.%s) THEN NULL ELSE -1 END) END",
				q(parent), qrel(refs), q(parent), qrel(refs), q(parent)))
		}
	}
	// The rows' organizations are known only in the database: the pause lock of statements
	// not confined to one organization, which a move holds with the organization's own,
	// taken right before the rows are written and held to the end of the COPY's transaction.
	lock := ""
	if m.OrgLock {
		lock = "PERFORM pg_advisory_xact_lock_shared(lokadb.org_lock(NULL)); "
		a.OwnLock = true
	}
	a.Prelude = []string{
		"DROP TABLE IF EXISTS pg_temp." + stage,
		fmt.Sprintf("CREATE TEMP TABLE %s AS SELECT * FROM %s WITH NO DATA", stage, qfull(t.Name)),
		fmt.Sprintf("CREATE OR REPLACE FUNCTION pg_temp.%s() RETURNS trigger LANGUAGE plpgsql AS $lokadb$ BEGIN %sINSERT INTO %s (%s) SELECT %s FROM pg_temp.%s s; DELETE FROM pg_temp.%s; RETURN NULL; END $lokadb$",
			stage, lock, qfull(t.Name), strings.Join(qcols, ", "), strings.Join(src, ", "), stage, stage),
		fmt.Sprintf("CREATE TRIGGER __lokadb_copy AFTER INSERT ON pg_temp.%s FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.%s()", stage, stage),
	}
	rel, attl := cs.Relation, cs.Attlist
	cs.Relation = &pg.RangeVar{Schemaname: "pg_temp", Relname: "__lokadb_copy_" + t.Name, Inh: true, Relpersistence: "p"}
	if len(attl) == 0 {
		for _, c := range cols {
			cs.Attlist = append(cs.Attlist, strNode(c))
		}
	}
	s, err := deparseStmt(n)
	cs.Relation, cs.Attlist = rel, attl
	if err != nil {
		a.Prelude = nil
		a.Forbidden = fmt.Sprintf("lokadb: COPY FROM into %s: this form is not supported", t.Name)
		return ""
	}
	return s
}

// identRe: the words of a DO block's body that may name a table or a function.
var identRe = regexp.MustCompile(`[A-Za-z_][A-Za-z0-9_$]*`)

// doBlock: a DO block whose body names a LokaDB table, a view or a function over one reads or
// writes rows the analyzer cannot attribute to an organization; the fanout policy decides.
func (a *Analysis) doBlock(do *pg.DoStmt, m *Meta) {
	if m == nil {
		return
	}
	for _, arg := range do.Args {
		de := arg.GetDefElem()
		if de == nil || de.Defname != "as" {
			continue
		}
		for _, w := range identRe.FindAllString(de.Arg.GetString_().GetSval(), -1) {
			for _, x := range []string{w, strings.ToLower(w)} {
				if m.named(x) || m.Funcs[x] {
					a.Lokadb, a.Write = true, true
					a.unhinted(x)
					a.gap(Gap{Kind: GapUnsupportedForm, Table: x, Form: "DO"})
					return
				}
			}
		}
	}
}

// policyStmt: the application's own CREATE or DROP POLICY on LokaDB table t (lokadb_* policies
// are refused by the guard). lokadb_tenant has to allow on its own, as a permissive policy,
// and narrow what the application's policies allow next to them, as a restrictive one: a
// permissive policy of the application (USING (true)) next to a permissive lokadb_tenant
// would let every organization's rows through. So the statement runs with
// lokadb.tenant_policy after it, which makes lokadb_tenant again as the policies now need it;
// the client gets the tag of what it sent.
func (a *Analysis) policyStmt(t *Table, text, tag string) string {
	if t == nil {
		return ""
	}
	a.Tag = tag
	return fmt.Sprintf("DO $lokadb$ BEGIN EXECUTE %s; PERFORM lokadb.tenant_policy(%s, %s); END $lokadb$",
		sqlString(strings.TrimSuffix(strings.TrimSpace(text), ";")), sqlString(t.Name), sqlString(t.TenantCol))
}

// unparsed: the parser the proxy uses (PostgreSQL 17's grammar) may refuse what the cloud
// (a later version) runs. A statement it cannot read that names a LokaDB table, a view or a
// function over one is not taken for a cloud statement: it names no organization the proxy
// can see, so the fanout policy decides (P6).
func (a *Analysis) unparsed(sql string, m *Meta) {
	if m == nil {
		return
	}
	for _, w := range identRe.FindAllString(sql, -1) {
		for _, x := range []string{w, strings.ToLower(w)} {
			if m.named(x) || m.Funcs[x] {
				a.Lokadb, a.Write = true, true
				a.unhinted(x)
				a.gap(Gap{Kind: GapUnsupportedForm, Table: x, Form: "not understood by the proxy's parser"})
				return
			}
		}
	}
}

// view: the LokaDB tables a view reads (nil for anything else). A bare name qualify did not
// resolve (no search path known for it) is a view of another schema of that name: the search
// path may reach it (review of routing R1: app.docs_v as docs_v read every organization).
func (m *Meta) view(rv *pg.RangeVar) []string {
	if k := m.viewKey(rv); k != "" {
		return m.Views[k]
	}
	return nil
}

// viewKey: the key of Meta.Views view reads by ("" for none).
func (m *Meta) viewKey(rv *pg.RangeVar) string {
	if m == nil || rv == nil {
		return ""
	}
	if k := relKey(rv); m.Views[k] != nil {
		return k
	}
	if rv.Schemaname != "" || rv.Catalogname != "" || len(m.Rels[rv.Relname]) > 0 {
		return ""
	}
	first := "" // the first matching key in sorted order, without sorting them all (third review of performance, perf3-24)
	for k := range m.Views {
		if _, n := rel.SplitKey(k); n == rv.Relname && (first == "" || k < first) {
			first = k
		}
	}
	return first
}

// scan visits every SELECT/UPDATE/DELETE/INSERT scope (including subqueries and CTEs).
func (a *Analysis) scan(n *pg.Node, m *Meta) {
	hints := len(a.Hints)
	var funcs []string
	defer func() {
		// A function over LokaDB tables runs under the statement's organization, if it has one.
		if len(funcs) > 0 {
			a.Lokadb = true
			if len(a.Hints) == hints {
				a.unhinted(funcs[0] + "()")
				for _, f := range funcs {
					a.gap(Gap{Kind: GapUnsupportedForm, Form: "function " + f + "()"})
				}
			}
		}
	}()
	walk(n.ProtoReflect(), func(msg proto.Message) {
		switch s := msg.(type) {
		case *pg.FuncCall:
			if m == nil || len(s.Funcname) == 0 {
				return
			}
			if len(s.Funcname) >= 2 {
				if sch := s.Funcname[len(s.Funcname)-2].GetString_().GetSval(); sch == "pg_catalog" || sch == "information_schema" {
					return
				}
			}
			// by its name in any schema (loadDeps reads every schema's): reports.doc_count() as
			// doc_count() (review of routing R1); another schema's function of the same name
			// goes under the policy too, which is the safe side
			if f := s.Funcname[len(s.Funcname)-1].GetString_().GetSval(); m.Funcs[f] {
				funcs = append(funcs, f)
			}
		case *pg.RefreshMatViewStmt:
			// its query runs over every organization's rows (review of routing R1): a fanout
			if k := m.viewKey(s.Relation); k != "" {
				a.Lokadb, a.Write = true, true
				a.unhinted(k)
				a.gap(Gap{Kind: GapUnsupportedForm, Table: k, Form: "REFRESH MATERIALIZED VIEW of " + strings.Join(m.Views[k], ", ")})
			}
		case *pg.SelectStmt:
			sc := newScope(m)
			sc.up = a.outer[s]
			for _, f := range s.FromClause {
				sc.collect(f)
			}
			a.scope(sc, s.WhereClause, m)
		case *pg.UpdateStmt:
			sc := newScope(m)
			sc.addRel(s.Relation)
			for _, f := range s.FromClause {
				sc.collect(f)
			}
			a.Write = a.Write || m.table(s.Relation) != nil || m.view(s.Relation) != nil
			a.scope(sc, s.WhereClause, m)
		case *pg.DeleteStmt:
			sc := newScope(m)
			sc.addRel(s.Relation)
			for _, f := range s.UsingClause {
				sc.collect(f)
			}
			a.Write = a.Write || m.table(s.Relation) != nil || m.view(s.Relation) != nil
			a.scope(sc, s.WhereClause, m)
		case *pg.MergeStmt:
			sc := newScope(m)
			sc.addRel(s.Relation)
			sc.collect(s.SourceRelation)
			a.Write = a.Write || m.table(s.Relation) != nil
			a.scope(sc, s.JoinCondition, m)
		case *pg.InsertStmt:
			if t := m.table(s.Relation); t != nil {
				a.Lokadb, a.Write = true, true
				a.insertHints(s, t, m)
			} else if m.view(s.Relation) != nil {
				a.Lokadb, a.Write = true, true
				a.unhinted(s.Relation.Relname)
				a.gap(Gap{Kind: GapUnsupportedForm, Table: s.Relation.Relname, Form: "INSERT into a view of " + strings.Join(m.view(s.Relation), ", ")})
			}
		}
	})
}

type scope struct {
	m      *Meta
	rels   map[string]*Table // name or alias -> LokaDB table (nil for other tables)
	lokadb []*Table
	alias  []string // the name or alias of each of lokadb
	// views: views over LokaDB tables (Meta.Views) the scope reads: their rows are LokaDB
	// rows, but no condition on their columns is known to name the organization
	views []string
	// names: the relation of each name or alias that is not a LokaDB table (cloud gaps)
	names map[string]string
	// up: the scope of the statement a correlated subquery belongs to, when that scope
	// names its organization (nil otherwise)
	up *scope
	// quals: ON conditions of inner joins outside any outer join: they restrict the rows of
	// the scope as WHERE does. Those of an outer join restrict one side only.
	quals []*pg.Node
}

func newScope(m *Meta) *scope {
	return &scope{m: m, rels: map[string]*Table{}, names: map[string]string{}}
}

func (sc *scope) addRel(rv *pg.RangeVar) {
	if rv == nil {
		return
	}
	t := sc.m.table(rv)
	key := rv.Relname
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		key = rv.Alias.Aliasname
	}
	sc.rels[key] = t
	if t != nil {
		sc.lokadb = append(sc.lokadb, t)
		sc.alias = append(sc.alias, key)
	} else if k := sc.m.viewKey(rv); k != "" {
		sc.views = append(sc.views, k)
	} else {
		sc.names[key] = relKey(rv)
		if tc := sc.m.cloud(relKey(rv)); tc != "" {
			// a cloud table that knows the organization: its columns resolve like a LokaDB
			// table's, it is not one (sc.lokadb)
			sc.rels[key] = &Table{Name: relKey(rv), TenantCol: tc}
		}
	}
}

func (sc *scope) collect(n *pg.Node) { sc.collectJoin(n, true) }

// collectJoin: inner says no outer join lies between n and the scope.
func (sc *scope) collectJoin(n *pg.Node, inner bool) {
	switch {
	case n.GetRangeVar() != nil:
		sc.addRel(n.GetRangeVar())
	case n.GetJoinExpr() != nil:
		j := n.GetJoinExpr()
		inner = inner && j.Jointype == pg.JoinType_JOIN_INNER
		if inner && j.Quals != nil {
			sc.quals = append(sc.quals, j.Quals)
		}
		sc.collectJoin(j.Larg, inner)
		sc.collectJoin(j.Rarg, inner)
	}
}

// column resolves a column reference to the name or alias of its relation in the scope and
// its LokaDB table (nil for another table); ok is false when the relation is not known.
func (sc *scope) column(fields []string) (alias string, t *Table, ok bool) {
	switch len(fields) {
	case 2, 3:
		alias = fields[len(fields)-2]
		t, ok = sc.rels[alias]
		return alias, t, ok
	case 1:
		for i, bt := range sc.lokadb {
			if _, has := bt.ColType[fields[0]]; has {
				if t != nil && t != bt {
					return "", nil, false
				}
				t, alias = bt, sc.alias[i]
			}
		}
		return alias, t, t != nil
	}
	return "", nil, false
}

// isCloud: t is a cloud table of routing.cloud_tables, not a LokaDB table.
func (sc *scope) isCloud(t *Table) bool { return t != nil && sc.m.Tables[t.Name] == nil }

// lookup: col of t has an organization lookup.
func (sc *scope) lookup(t *Table, col string) bool {
	if t == nil {
		return false
	}
	_, ok := sc.m.Lookups[t.Name][col]
	return ok
}

func (a *Analysis) scope(sc *scope, where *pg.Node, m *Meta) {
	if len(sc.lokadb) == 0 && len(sc.views) == 0 {
		return
	}
	a.Lokadb = true
	conds := conjuncts(where)
	for _, q := range sc.quals {
		conds = append(conds, conjuncts(q)...)
	}
	// Columns equal to each other (d.project_id = p.id) form classes: a value bound to one
	// column of a class binds them all, so d.project_id = p.id AND p.id = $1 restricts d too.
	type member struct {
		alias, col string
		t          *Table
	}
	parent := map[member]member{}
	find := func(x member) member {
		for {
			p, ok := parent[x]
			if !ok || p == x {
				return x
			}
			x = p
		}
	}
	var members []member
	add := func(x member) {
		if _, ok := parent[x]; !ok {
			parent[x] = x
			members = append(members, x)
		}
	}
	type bound struct {
		at   member
		vals []ValueRef
		form string
	}
	var bounds []bound
	// rowBounds: (a, b) IN ((x, y), …) of one table's columns
	type rowBound struct {
		alias string
		t     *Table
		cols  []string
		rows  [][]ValueRef
	}
	var rowBounds []rowBound
	direct := map[member]bool{} // classes with a hint of their own
	var gaps []Gap
	found := false
	for _, c := range conds {
		if l, r, ok := colEq(c); ok {
			la, lt, lok := sc.column(l)
			ra, rt, rok := sc.column(r)
			if lok && rok {
				x, y := member{la, l[len(l)-1], lt}, member{ra, r[len(r)-1], rt}
				add(x)
				add(y)
				parent[find(x)] = find(y)
			} else if sc.correlated(l, r) || sc.correlated(r, l) {
				found = true
			}
			continue
		}
		bind := func(fields []string, vals []ValueRef, form string) {
			alias, t, ok := sc.column(fields)
			if !ok {
				return
			}
			x := member{alias, fields[len(fields)-1], t}
			add(x)
			bounds = append(bounds, bound{x, vals, form})
			if sc.lookup(t, x.col) {
				a.Hints = append(a.Hints, Hint{Table: t.Name, Column: x.col, Values: vals})
				direct[x] = true
				found = true
			}
		}
		if fields, vals, ok := hintExpr(c); ok {
			bind(fields, vals, condForm(c))
			continue
		}
		if cols, rows, ok := rowExpr(c); ok {
			if len(rows) == 1 { // (a, b) = (x, y): a = x AND b = y
				for i, f := range cols {
					bind(f, []ValueRef{rows[0][i]}, "=")
				}
				continue
			}
			// (a, b) IN ((x, y), …): rows of a composite key's columns, all of one table
			var names []string
			alias0, t0 := "", (*Table)(nil)
			same := true
			for i, f := range cols {
				alias, t, ok := sc.column(f)
				same = same && ok && t != nil && (i == 0 || alias == alias0)
				alias0, t0 = alias, t
				names = append(names, f[len(f)-1])
			}
			if same {
				rowBounds = append(rowBounds, rowBound{alias0, t0, names, rows})
				continue
			}
		}
		if sc.subqueryHint(c) {
			found = true
			continue
		}
		gaps = append(gaps, sc.condGaps(c)...)
	}
	for x := range direct {
		direct[find(x)] = true
	}
	for _, b := range bounds {
		root := find(b.at)
		if direct[root] {
			continue
		}
		for _, x := range members {
			if find(x) == root && x.t != nil && sc.lookup(x.t, x.col) {
				a.Hints = append(a.Hints, Hint{Table: x.t.Name, Column: x.col, Values: b.vals})
				found = true
			}
		}
	}
	// A lookup of several columns (a composite key, tuples.go): every column bound in this
	// scope, by a row of them or each by values (their product).
	for i, t := range sc.lokadb {
		if found {
			break
		}
		alias := sc.alias[i]
		for _, g := range sc.m.tupleGroups(t.Name) {
			rows, ok := [][]ValueRef(nil), false
			for _, rb := range rowBounds {
				if rb.alias != alias || ok {
					continue
				}
				idx := make([]int, len(g))
				ok = true
				for j, c := range g {
					idx[j] = slices.Index(rb.cols, c)
					ok = ok && idx[j] >= 0
				}
				if ok {
					for _, r := range rb.rows {
						var row []ValueRef
						for _, k := range idx {
							row = append(row, r[k])
						}
						rows = append(rows, row)
					}
				}
			}
			if !ok {
				var lists [][]ValueRef
				for _, c := range g {
					x := member{alias, c, t}
					if _, there := parent[x]; !there {
						lists = nil
						break
					}
					var vals []ValueRef
					for _, b := range bounds {
						if find(b.at) == find(x) {
							vals = b.vals
							break
						}
					}
					if vals == nil {
						lists = nil
						break
					}
					lists = append(lists, vals)
				}
				if lists != nil {
					rows, ok = product(lists)
				}
			}
			if ok {
				a.Tuples = append(a.Tuples, TupleHint{Table: t.Name, Columns: g, Rows: rows})
				found = true
				break
			}
		}
	}
	if found {
		a.correlate(sc, where)
		return
	}
	if len(sc.lokadb) > 0 {
		a.unhinted(sc.lokadb[0].Name)
	} else {
		a.unhinted(sc.views[0])
	}
	for _, v := range sc.views {
		a.gap(Gap{Kind: GapUnsupportedForm, Table: v, Form: "view of " + strings.Join(sc.m.Views[v], ", ")})
	}
	for _, b := range bounds {
		root := find(b.at)
		for _, x := range members {
			switch {
			case find(x) != root:
			case x.t != nil && !sc.isCloud(x.t):
				gaps = append(gaps, Gap{Kind: GapUnknownColumn, Table: x.t.Name, Column: x.col, Form: b.form})
			case sc.names[x.alias] != "":
				gaps = append(gaps, Gap{Kind: GapUnknownColumn, Table: sc.names[x.alias], Column: x.col, Form: b.form, Cloud: true})
			}
		}
	}
	has := map[string]bool{}
	for _, g := range gaps {
		a.gap(g)
		if !g.Cloud {
			has[g.Table] = true
		}
	}
	for _, t := range sc.lokadb {
		if !has[t.Name] {
			a.gap(Gap{Kind: GapNoCondition, Table: t.Name})
		}
	}
}

// correlate: the subqueries in the conditions of a scope that names its organization may
// refer to its rows (EXISTS (SELECT 1 FROM v WHERE v.document_id = d.id)).
func (a *Analysis) correlate(sc *scope, where *pg.Node) {
	for _, n := range append([]*pg.Node{where}, sc.quals...) {
		if n == nil {
			continue
		}
		walk(n.ProtoReflect(), func(msg proto.Message) {
			if sl, ok := msg.(*pg.SubLink); ok {
				if sel := sl.Subselect.GetSelectStmt(); sel != nil {
					if a.outer == nil {
						a.outer = map[*pg.SelectStmt]*scope{}
					}
					if a.outer[sel] == nil {
						a.outer[sel] = sc
					}
				}
			}
		})
	}
}

// correlated recognizes `inner = outer` in a correlated subquery: inner is a column of the
// subquery's scope, outer one of the statement's scope (sc.up, which names its organization),
// and both find the organization through the same lookup: the subquery's rows are then that
// organization's.
func (sc *scope) correlated(inner, outer []string) bool {
	if sc.up == nil {
		return false
	}
	_, it, ok := sc.column(inner)
	if !ok || it == nil {
		return false
	}
	if _, _, shadowed := sc.column(outer); shadowed {
		return false
	}
	_, ot, ok := sc.up.column(outer)
	if !ok || ot == nil {
		return false
	}
	ir, ok1 := sc.m.Lookups[it.Name][inner[len(inner)-1]]
	or, ok2 := sc.m.Lookups[ot.Name][outer[len(outer)-1]]
	return ok1 && ok2 && ir == or
}

// subqueryHint recognizes `col IN (SELECT x FROM t WHERE <organization>)` (Rails'
// where(id: relation)): col and t.x find the organization through the same lookup, and the
// subquery names its organization, so the rows of col's table are that organization's. The
// subquery is a scope of its own: its hints are the statement's already.
func (sc *scope) subqueryHint(c *pg.Node) bool {
	sl := c.GetSubLink()
	if sl == nil || sl.SubLinkType != pg.SubLinkType_ANY_SUBLINK || len(sl.OperName) > 0 && sl.OperName[len(sl.OperName)-1].GetString_().GetSval() != "=" {
		return false
	}
	fields, ok := colRef(sl.Testexpr)
	if !ok {
		return false
	}
	_, t, ok := sc.column(fields)
	if !ok || t == nil {
		return false
	}
	refs, ok := sc.m.Lookups[t.Name][fields[len(fields)-1]]
	if !ok {
		return false
	}
	sub := sl.Subselect.GetSelectStmt()
	if sub == nil || len(sub.TargetList) != 1 || sub.Op != pg.SetOperation_SETOP_NONE {
		return false
	}
	inner := newScope(sc.m)
	for _, f := range sub.FromClause {
		inner.collect(f)
	}
	tf, ok := colRef(sub.TargetList[0].GetResTarget().GetVal())
	if !ok {
		return false
	}
	_, st, ok := inner.column(tf)
	if !ok || st == nil {
		return false
	}
	if r, ok := sc.m.Lookups[st.Name][tf[len(tf)-1]]; !ok || r != refs {
		return false
	}
	a := &Analysis{}
	a.scope(inner, sub.WhereClause, sc.m)
	return !a.Unhinted && len(a.Hints) > 0
}

// condGaps: what a condition that restricts nothing says about the LokaDB columns it uses.
// Columns of subqueries are the subqueries' own (their scopes are analyzed by themselves).
func (sc *scope) condGaps(c *pg.Node) []Gap {
	form := condForm(c)
	inner := map[*pg.ColumnRef]bool{}
	walk(c.ProtoReflect(), func(msg proto.Message) {
		if sl, ok := msg.(*pg.SubLink); ok && sl.Subselect != nil {
			walk(sl.Subselect.ProtoReflect(), func(msg proto.Message) {
				if cr, ok := msg.(*pg.ColumnRef); ok {
					inner[cr] = true
				}
			})
		}
	})
	operand := map[*pg.ColumnRef]bool{} // a column compared as it is, not inside an expression
	if e := c.GetAExpr(); e != nil {
		for _, n := range []*pg.Node{e.Lexpr, e.Rexpr} {
			for n.GetTypeCast() != nil {
				n = n.GetTypeCast().Arg
			}
			if cr := n.GetColumnRef(); cr != nil {
				operand[cr] = true
			}
		}
	}
	if sl := c.GetSubLink(); sl != nil && sl.Testexpr.GetColumnRef() != nil {
		operand[sl.Testexpr.GetColumnRef()] = true
	}
	var out []Gap
	walk(c.ProtoReflect(), func(msg proto.Message) {
		cr, ok := msg.(*pg.ColumnRef)
		if !ok || inner[cr] {
			return
		}
		fields, ok := colRef(&pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: cr}})
		if !ok {
			return
		}
		_, t, ok := sc.column(fields)
		if !ok || t == nil || sc.isCloud(t) {
			return
		}
		col := fields[len(fields)-1]
		switch {
		case !sc.lookup(t, col):
			out = append(out, Gap{Kind: GapUnknownColumn, Table: t.Name, Column: col, Form: form})
		case operand[cr] && (form == "=" || form == "IN" || form == "= ANY"):
			out = append(out, Gap{Kind: GapUnreadableValue, Table: t.Name, Column: col, Form: form})
		default:
			out = append(out, Gap{Kind: GapUnsupportedForm, Table: t.Name, Column: col, Form: form})
		}
	})
	return out
}

// condForm names the form of a condition for a Gap.
func condForm(c *pg.Node) string {
	op := func(name []*pg.Node) string {
		if len(name) == 0 {
			return "?"
		}
		return name[len(name)-1].GetString_().GetSval()
	}
	switch {
	case c.GetAExpr() != nil:
		e := c.GetAExpr()
		switch e.Kind {
		case pg.A_Expr_Kind_AEXPR_OP:
			return op(e.Name)
		case pg.A_Expr_Kind_AEXPR_IN:
			if op(e.Name) == "=" {
				return "IN"
			}
			return "NOT IN"
		case pg.A_Expr_Kind_AEXPR_OP_ANY:
			return op(e.Name) + " ANY"
		case pg.A_Expr_Kind_AEXPR_OP_ALL:
			return op(e.Name) + " ALL"
		case pg.A_Expr_Kind_AEXPR_BETWEEN, pg.A_Expr_Kind_AEXPR_NOT_BETWEEN,
			pg.A_Expr_Kind_AEXPR_BETWEEN_SYM, pg.A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM:
			return "BETWEEN"
		case pg.A_Expr_Kind_AEXPR_LIKE, pg.A_Expr_Kind_AEXPR_ILIKE, pg.A_Expr_Kind_AEXPR_SIMILAR:
			return "LIKE"
		case pg.A_Expr_Kind_AEXPR_DISTINCT, pg.A_Expr_Kind_AEXPR_NOT_DISTINCT:
			return "IS DISTINCT FROM"
		}
	case c.GetSubLink() != nil:
		switch c.GetSubLink().SubLinkType {
		case pg.SubLinkType_ANY_SUBLINK:
			return "IN (SELECT …)"
		case pg.SubLinkType_EXISTS_SUBLINK:
			return "EXISTS"
		}
		return "subquery"
	case c.GetBoolExpr() != nil:
		if c.GetBoolExpr().Boolop == pg.BoolExprType_OR_EXPR {
			return "OR"
		}
		return "NOT"
	case c.GetNullTest() != nil:
		return "IS NULL"
	}
	return "expression"
}

// colEq recognizes `a.x = b.y`, a join condition between two columns.
func colEq(n *pg.Node) ([]string, []string, bool) {
	e := n.GetAExpr()
	if e == nil || e.Kind != pg.A_Expr_Kind_AEXPR_OP || len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
		return nil, nil, false
	}
	strip := func(n *pg.Node) *pg.Node {
		for n.GetTypeCast() != nil {
			n = n.GetTypeCast().Arg
		}
		return n
	}
	l, lok := colRef(strip(e.Lexpr))
	r, rok := colRef(strip(e.Rexpr))
	return l, r, lok && rok
}

func (a *Analysis) insertHints(s *pg.InsertStmt, t *Table, m *Meta) {
	sel := s.SelectStmt.GetSelectStmt()
	unnest := unnestArgs(sel)
	if sel == nil || (len(sel.ValuesLists) == 0 && unnest == nil) {
		if sel != nil && insertSelect(s, sel, t, m, a) {
			return
		}
		a.unhinted(t.Name)
		switch {
		case s.SelectStmt == nil:
			a.gap(Gap{Kind: GapNoCondition, Table: t.Name, Form: "DEFAULT VALUES"})
		default:
			a.gap(Gap{Kind: GapUnsupportedForm, Table: t.Name, Form: "INSERT … SELECT"})
		}
		return
	}
	unreadable := func(col string) {
		a.unhinted(t.Name)
		a.gap(Gap{Kind: GapUnreadableValue, Table: t.Name, Column: col, Form: "INSERT"})
	}
	// The row's own key (id, looked up in its own refs table, or a declared key) names no
	// organization in a plain INSERT: the row is new, the lookup finds nothing. An upsert
	// may hit an existing row.
	own := func(col string) bool {
		refs := m.Lookups[t.Name][col]
		_, key := keyRefs(refs)
		return (refs == t.Refs || key) && s.OnConflictClause == nil
	}
	found := false
	for i, c := range s.Cols {
		col := c.GetResTarget().GetName()
		if _, ok := m.Lookups[t.Name][col]; !ok || own(col) {
			continue
		}
		if unnest != nil {
			var vals []ValueRef
			ok := i < len(unnest)
			if ok {
				vals, ok = arrayValues(unnest[i])
			}
			if !ok {
				unreadable(col)
				return
			}
			a.Hints = append(a.Hints, Hint{Table: t.Name, Column: col, Values: vals})
			found = true
			continue
		}
		var vals []ValueRef
		for _, row := range sel.ValuesLists {
			items := row.GetList().GetItems()
			if i >= len(items) {
				unreadable(col)
				return
			}
			v, ok := value(items[i])
			if !ok {
				unreadable(col)
				return
			}
			vals = append(vals, v)
		}
		a.Hints = append(a.Hints, Hint{Table: t.Name, Column: col, Values: vals})
		found = true
	}
	if !found {
		a.unhinted(t.Name)
		named := false
		for _, c := range s.Cols {
			if col := c.GetResTarget().GetName(); !own(col) {
				a.gap(Gap{Kind: GapUnknownColumn, Table: t.Name, Column: col, Form: "INSERT"})
				named = true
			}
		}
		if !named {
			a.gap(Gap{Kind: GapNoCondition, Table: t.Name, Form: "INSERT"})
		}
	}
}

// insertSelect reads `INSERT INTO t (cols) SELECT … FROM src WHERE …`: a column with a
// lookup gets a value (a constant or a parameter: a hint, as in VALUES) or a column of src
// that finds its organization through the same lookup, where the SELECT itself names its
// organization (its hints are the statement's already). False: the rows' organization is
// not known.
func insertSelect(s *pg.InsertStmt, sel *pg.SelectStmt, t *Table, m *Meta, a *Analysis) bool {
	if sel.Op != pg.SetOperation_SETOP_NONE || len(sel.TargetList) == 0 {
		return false
	}
	cols := t.Cols
	if len(s.Cols) > 0 {
		cols = nil
		for _, c := range s.Cols {
			cols = append(cols, c.GetResTarget().GetName())
		}
	}
	inner := newScope(m)
	for _, f := range sel.FromClause {
		inner.collect(f)
	}
	sub := &Analysis{}
	sub.scope(inner, sel.WhereClause, m)
	named := len(inner.lokadb) > 0 && !sub.Unhinted
	var hints []Hint
	found := false
	for i, col := range cols {
		refs, ok := m.Lookups[t.Name][col]
		if !ok || i >= len(sel.TargetList) {
			continue
		}
		if _, key := keyRefs(refs); (refs == t.Refs || key) && s.OnConflictClause == nil {
			continue // the row's own key: new
		}
		val := sel.TargetList[i].GetResTarget().GetVal()
		if v, ok := value(val); ok {
			hints = append(hints, Hint{Table: t.Name, Column: col, Values: []ValueRef{v}})
			found = true
			continue
		}
		f, ok := colRef(val)
		if !ok || !named {
			continue
		}
		if _, st, ok := inner.column(f); ok && st != nil {
			if r, ok := m.Lookups[st.Name][f[len(f)-1]]; ok && r == refs {
				found = true
			}
		}
	}
	if found {
		a.Hints = append(a.Hints, hints...)
	}
	return found
}

// unnestArgs recognizes `SELECT * FROM UNNEST(a1, a2, ...)`, the form Django 5.2+
// uses for multi-row inserts, and returns the array arguments.
func unnestArgs(sel *pg.SelectStmt) []*pg.Node {
	if sel == nil || len(sel.FromClause) != 1 || sel.WhereClause != nil {
		return nil
	}
	rf := sel.FromClause[0].GetRangeFunction()
	if rf == nil || len(rf.Functions) != 1 {
		return nil
	}
	items := rf.Functions[0].GetList().GetItems()
	if len(items) == 0 {
		return nil
	}
	fc := items[0].GetFuncCall()
	if fc == nil || len(fc.Funcname) == 0 || fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval() != "unnest" {
		return nil
	}
	return fc.Args
}

// arrayValues reads an array expression whose elements are the values: an array parameter
// (`$1::int2[]`, Django with psycopg 3, pgx with a slice), a literal like '{1,2}'::int2[],
// or ARRAY[...] of constants and parameters.
func arrayValues(n *pg.Node) ([]ValueRef, bool) {
	for n.GetTypeCast() != nil {
		n = n.GetTypeCast().Arg
	}
	switch {
	case n.GetParamRef() != nil:
		return []ValueRef{{Param: int(n.GetParamRef().Number), Array: true}}, true
	case n.GetAConst() != nil:
		s := n.GetAConst().GetSval()
		if s == nil {
			return nil, false
		}
		elems, ok := parseArrayText(s.Sval)
		var vals []ValueRef
		for _, e := range elems {
			vals = append(vals, ValueRef{Const: e})
		}
		return vals, ok
	case n.GetAArrayExpr() != nil:
		var vals []ValueRef
		for _, e := range n.GetAArrayExpr().Elements {
			v, ok := value(e)
			if !ok {
				return nil, false
			}
			vals = append(vals, v)
		}
		return vals, true
	}
	return nil, false
}

// parseArrayText reads a one-dimensional array in Postgres text form ({1,2,"3"}) by the
// rules of array_in: elements are separated by commas; one in double quotes may hold commas,
// braces and spaces; a backslash takes the next character as it is, in quotes or not;
// whitespace around an element goes, inside an unquoted one it stays. A NULL element (an
// unquoted NULL, in any case) or a nested array is not a list of organizations; a form this
// does not read (a dimension prefix [1:2]=, an empty element, a quote in the middle of an
// element, an unterminated quote) is refused as well: not ok.
func parseArrayText(s string) ([]string, bool) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '{' || s[len(s)-1] != '}' {
		return nil, false
	}
	b := s[1 : len(s)-1]
	i := 0
	skip := func() {
		for i < len(b) && strings.IndexByte(" \t\n\r\f\v", b[i]) >= 0 {
			i++
		}
	}
	skip()
	if i == len(b) {
		return nil, true // {} or { }
	}
	var out []string
	for {
		skip()
		if i >= len(b) {
			return nil, false // an element after a comma is missing
		}
		var el strings.Builder
		dq, escaped, protect := false, false, 0 // in double quotes; an escape was met; the bytes up to the last escaped one stay as they are
		switch b[i] {
		case '{', '}', ',':
			return nil, false // a nested array, or an empty element
		case '"':
			dq = true
			i++
			closed := false
			for i < len(b) && !closed {
				switch c := b[i]; c {
				case '\\':
					if i++; i >= len(b) {
						return nil, false
					}
					el.WriteByte(b[i])
					i++
				case '"':
					closed = true
					i++
				default:
					el.WriteByte(c)
					i++
				}
			}
			if !closed {
				return nil, false
			}
		default:
			for i < len(b) && b[i] != ',' {
				switch c := b[i]; c {
				case '{', '}', '"':
					return nil, false
				case '\\':
					if i++; i >= len(b) {
						return nil, false
					}
					escaped = true
					el.WriteByte(b[i])
					i++
					protect = el.Len()
				default:
					el.WriteByte(c)
					i++
				}
			}
		}
		e := el.String()
		if !dq {
			// the whitespace before the next comma is not the element's (an escaped one is)
			e = e[:protect] + strings.TrimRight(e[protect:], " \t\n\r\f\v")
			if !escaped && strings.EqualFold(e, "NULL") {
				return nil, false
			}
		}
		out = append(out, e)
		skip()
		if i >= len(b) {
			return out, true
		}
		if b[i] != ',' {
			return nil, false // something after a closing quote
		}
		i++
	}
}

func conjuncts(n *pg.Node) []*pg.Node {
	if n == nil {
		return nil
	}
	if b := n.GetBoolExpr(); b != nil && b.Boolop == pg.BoolExprType_AND_EXPR {
		var out []*pg.Node
		for _, arg := range b.Args {
			out = append(out, conjuncts(arg)...)
		}
		return out
	}
	return []*pg.Node{n}
}

// hintExpr recognizes `col = value`, `value = col`, `col IN (values...)` and `col = ANY(array)`.
func hintExpr(n *pg.Node) ([]string, []ValueRef, bool) {
	e := n.GetAExpr()
	if e == nil {
		return nil, nil, false
	}
	switch e.Kind {
	case pg.A_Expr_Kind_AEXPR_OP:
		if len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
			return nil, nil, false
		}
		if f, ok := colRef(e.Lexpr); ok {
			if v, ok := value(e.Rexpr); ok {
				return f, []ValueRef{v}, true
			}
		}
		if f, ok := colRef(e.Rexpr); ok {
			if v, ok := value(e.Lexpr); ok {
				return f, []ValueRef{v}, true
			}
		}
	case pg.A_Expr_Kind_AEXPR_IN:
		if len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
			return nil, nil, false // NOT IN
		}
		f, ok := colRef(e.Lexpr)
		if !ok || e.Rexpr.GetList() == nil {
			return nil, nil, false
		}
		var vals []ValueRef
		for _, it := range e.Rexpr.GetList().Items {
			v, ok := value(it)
			if !ok {
				return nil, nil, false
			}
			vals = append(vals, v)
		}
		return f, vals, len(vals) > 0
	case pg.A_Expr_Kind_AEXPR_OP_ANY:
		if len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
			return nil, nil, false // <> ANY, > ANY ...
		}
		if f, ok := colRef(e.Lexpr); ok {
			if vals, ok := arrayValues(e.Rexpr); ok {
				return f, vals, true
			}
		}
	}
	return nil, nil, false
}

func colRef(n *pg.Node) ([]string, bool) {
	cr := n.GetColumnRef()
	if cr == nil {
		return nil, false
	}
	var out []string
	for _, f := range cr.Fields {
		s := f.GetString_()
		if s == nil {
			return nil, false
		}
		out = append(out, s.Sval)
	}
	return out, true
}

func value(n *pg.Node) (ValueRef, bool) {
	switch {
	case n == nil:
		return ValueRef{}, false
	case n.GetTypeCast() != nil:
		return value(n.GetTypeCast().Arg)
	case n.GetParamRef() != nil:
		return ValueRef{Param: int(n.GetParamRef().Number)}, true
	case n.GetAConst() != nil:
		c := n.GetAConst()
		switch {
		case c.Isnull:
			return ValueRef{Null: true}, true
		case c.GetIval() != nil:
			return ValueRef{Const: strconv.Itoa(int(c.GetIval().Ival))}, true
		case c.GetFval() != nil:
			return ValueRef{Const: c.GetFval().Fval}, true
		case c.GetSval() != nil:
			return ValueRef{Const: c.GetSval().Sval}, true
		}
	}
	return ValueRef{}, false
}

func walk(m protoreflect.Message, fn func(proto.Message)) {
	fn(m.Interface())
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Message() == nil || fd.IsMap() {
			return true
		}
		if fd.IsList() {
			l := v.List()
			for i := 0; i < l.Len(); i++ {
				walk(l.Get(i).Message(), fn)
			}
			return true
		}
		walk(v.Message(), fn)
		return true
	})
}

// --- deparse helpers ---

// treeVersion is the parse tree version Deparse insists on.
var treeVersion = func() int32 {
	t, err := pg.Parse("SELECT 1")
	if err != nil {
		panic(err)
	}
	return t.Version
}()

func deparseStmt(n *pg.Node) (string, error) {
	return pg.Deparse(&pg.ParseResult{Version: treeVersion, Stmts: []*pg.RawStmt{{Stmt: n}}})
}

func deparseTarget(rt *pg.ResTarget) (string, error) {
	sel := &pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
		TargetList:  []*pg.Node{{Node: &pg.Node_ResTarget{ResTarget: rt}}},
		Op:          pg.SetOperation_SETOP_NONE,
		LimitOption: pg.LimitOption_LIMIT_OPTION_DEFAULT,
	}}}
	s, err := deparseStmt(sel)
	return strings.TrimPrefix(s, "SELECT "), err
}

func deparseExpr(n *pg.Node) (string, error) { return deparseTarget(&pg.ResTarget{Val: n}) }

func strNode(s string) *pg.Node {
	return &pg.Node{Node: &pg.Node_String_{String_: &pg.String{Sval: s}}}
}

// requalify rewrites column qualifiers in `from` to `to`; bare columns get `bare` as qualifier when set.
func requalify(n *pg.Node, from map[string]bool, to, bare string) {
	if n == nil {
		return
	}
	walk(n.ProtoReflect(), func(msg proto.Message) {
		cr, ok := msg.(*pg.ColumnRef)
		if !ok {
			return
		}
		switch len(cr.Fields) {
		case 1:
			if bare != "" && cr.Fields[0].GetString_() != nil {
				cr.Fields = append([]*pg.Node{strNode(bare)}, cr.Fields...)
			}
		case 2:
			if q := cr.Fields[0].GetString_(); q != nil && from[q.Sval] {
				cr.Fields[0] = strNode(to)
			}
		}
	})
}

// unschema rewrites references of the form schema.rel.col (Prisma, SQLAlchemy with a schema:
// "public"."documents"."id") to alias.col. The statements lokadb sends instead of the
// original name the relation differently (an alias, a partition), and a reference through
// the schema then points to no FROM item.
func unschema(n proto.Message, rv *pg.RangeVar, alias string) {
	walk(n.ProtoReflect(), func(msg proto.Message) {
		cr, ok := msg.(*pg.ColumnRef)
		if !ok || len(cr.Fields) < 3 {
			return
		}
		sch, rel := cr.Fields[len(cr.Fields)-3].GetString_(), cr.Fields[len(cr.Fields)-2].GetString_()
		if sch == nil || rel == nil || rel.Sval != rv.Relname || (sch.Sval != rv.Schemaname && !(sch.Sval == "public" && rv.Schemaname == "")) {
			return
		}
		if len(cr.Fields) == 4 { // database.schema.rel.col: the database must be the current one, as in Postgres
			return
		}
		cr.Fields = append([]*pg.Node{strNode(alias)}, cr.Fields[len(cr.Fields)-1:]...)
	})
}

const ordCol, rnCol = "__lokadb_ord", "__lokadb_rn"

var typmodRe = regexp.MustCompile(`^(character varying|character|bit varying|bit)\(\d+\)(\[\])?$`)

// castType is the type the rewritten upsert casts a value to: the column's type without a
// length for character and bit types. An explicit cast to varchar(n) silently cuts a value
// that is too long; storing it into the column (an assignment) fails as the original
// INSERT would have.
func castType(typ string) string { return typmodRe.ReplaceAllString(typ, "$1$2") }

// rewriteUpsert turns INSERT ... ON CONFLICT on a LokaDB table into an equivalent statement
// that does not need a unique index on the partitioned parent (which Postgres forbids
// once a partition is a foreign table).
func rewriteUpsert(stmt *pg.Node, ins *pg.InsertStmt, t *Table) (string, bool) {
	oc := ins.OnConflictClause
	if oc.Action == pg.OnConflictAction_ONCONFLICT_NOTHING && oc.Infer == nil {
		return "", false // plain DO NOTHING works through postgres_fdw as is
	}
	if oc.Infer == nil || oc.Infer.Conname != "" || oc.Infer.WhereClause != nil {
		return "", false
	}
	sel := ins.SelectStmt.GetSelectStmt()
	if sel == nil {
		return "", false
	}
	// The statement's own WITH comes first: its queries are the client's (P13: it was dropped).
	with := ""
	if wc := ins.WithClause; wc != nil {
		for _, c := range wc.Ctes {
			switch c.GetCommonTableExpr().GetCtename() {
			case "v", "u", "hit", "f", "i":
				return "", false
			}
		}
		s, err := deparseStmt(&pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
			WithClause:  wc,
			TargetList:  []*pg.Node{{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_AConst{AConst: &pg.A_Const{Val: &pg.A_Const_Ival{Ival: &pg.Integer{Ival: 1}}}}}}}}},
			Op:          pg.SetOperation_SETOP_NONE,
			LimitOption: pg.LimitOption_LIMIT_OPTION_DEFAULT,
		}}})
		if err != nil || !strings.HasPrefix(s, "WITH ") || !strings.HasSuffix(s, " SELECT 1") {
			return "", false
		}
		with = strings.TrimSuffix(strings.TrimPrefix(s, "WITH "), " SELECT 1") + ", "
	}
	var cols, keys []string
	colSet := map[string]bool{}
	for _, c := range ins.Cols {
		name := c.GetResTarget().GetName()
		cols = append(cols, name)
		colSet[name] = true
	}
	for _, e := range oc.Infer.IndexElems {
		ie := e.GetIndexElem()
		if ie == nil || ie.Name == "" {
			return "", false
		}
		keys = append(keys, ie.Name)
	}
	for _, k := range keys {
		if !colSet[k] {
			// The conflict key is generated by a default (e.g. a fresh id): no row can conflict.
			ins.OnConflictClause = nil
			s, err := deparseStmt(stmt)
			return s, err == nil
		}
	}

	q := quoteIdent
	tbl := qrel(relKey(ins.Relation)) // its schema too: the alias is bare (schemas.go)
	alias := ins.Relation.Relname
	if ins.Relation.Alias != nil && ins.Relation.Alias.Aliasname != "" {
		alias = ins.Relation.Alias.Aliasname
	} else {
		// the rewritten statements name the table by an alias: "public"."t"."col" in SET,
		// WHERE and RETURNING (Prisma) would point to nothing
		for _, l := range [][]*pg.Node{oc.TargetList, {oc.WhereClause}, ins.ReturningList} {
			for _, x := range l {
				if x != nil {
					unschema(x, ins.Relation, alias)
				}
			}
		}
	}
	qa := q(alias)
	var qcols []string
	for _, c := range cols {
		qcols = append(qcols, q(c))
	}
	colList := strings.Join(qcols, ", ")
	match := func(l, r string) string {
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s.%s = %s.%s", l, q(k), r, q(k)))
		}
		return strings.Join(parts, " AND ")
	}
	// reach narrows a scan of the table to the keys the statement inserts. A join with v is
	// not a condition postgres_fdw can send to an organization's database: without this the
	// foreign partition is read whole (FOR UPDATE in the UPDATE, REALISTIC_REVIEW 1.2). The
	// keys as an array are an initplan parameter, which it sends. (The partitions of other
	// organizations are pruned already: the RLS policy is on the partition key.)
	reach := func(x string) string {
		var parts []string
		for _, k := range keys {
			parts = append(parts, fmt.Sprintf("%s.%s = ANY (ARRAY(SELECT v.%s FROM v))", x, q(k), q(k)))
		}
		return strings.Join(parts, " AND ")
	}

	var v string
	if len(sel.ValuesLists) == 0 {
		// INSERT ... SELECT (e.g. Django's SELECT * FROM UNNEST(...)): number the rows it produces.
		inner, err := deparseStmt(ins.SelectStmt)
		if err != nil {
			return "", false
		}
		v = fmt.Sprintf("v AS (SELECT s.*, row_number() OVER () - 1 AS %s FROM (%s) AS s(%s))", ordCol, inner, colList)
	}
	var rows []string
	for i, vl := range sel.ValuesLists {
		items := vl.GetList().GetItems()
		if len(items) != len(cols) {
			return "", false
		}
		var parts []string
		for j, it := range items {
			if it.GetSetToDefault() != nil {
				return "", false
			}
			e, err := deparseExpr(it)
			typ := t.ColType[cols[j]]
			if err != nil || typ == "" {
				return "", false
			}
			parts = append(parts, fmt.Sprintf("CAST(%s AS %s)", e, castType(typ)))
		}
		parts = append(parts, strconv.Itoa(i))
		rows = append(rows, "("+strings.Join(parts, ", ")+")")
	}
	if v == "" {
		v = fmt.Sprintf("v AS (SELECT * FROM (VALUES %s) AS t(%s, %s))", strings.Join(rows, ", "), colList, ordCol)
	}

	var ctes []string
	ctes = append(ctes, v)
	fresh := "" // the condition of a row of v to be inserted
	if oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
		var sets []string
		for _, n := range oc.TargetList {
			rt := n.GetResTarget()
			if rt == nil || len(rt.Indirection) > 0 {
				return "", false
			}
			requalify(rt.Val, map[string]bool{"excluded": true}, "v", alias)
			e, err := deparseExpr(rt.Val)
			if err != nil {
				return "", false
			}
			sets = append(sets, fmt.Sprintf("%s = %s", q(rt.Name), e))
		}
		cond := match(qa, "v") + " AND " + reach(qa)
		if oc.WhereClause != nil {
			requalify(oc.WhereClause, map[string]bool{"excluded": true}, "v", alias)
			w, err := deparseExpr(oc.WhereClause)
			if err != nil {
				return "", false
			}
			cond += " AND (" + w + ")"
			// DO UPDATE … WHERE: a row whose key exists but whose WHERE is false is neither
			// updated nor inserted. Not being in u does not make it new: the table decides,
			// before anything changes (hit, read first by u). In an organization's database
			// postgres_fdw has no one snapshot for the statement: its transaction sees its own
			// writes, and u, which nothing would read, would run after the INSERT and update
			// the rows it just inserted.
			ctes = append(ctes, fmt.Sprintf("hit AS (SELECT v.%s FROM v WHERE EXISTS (SELECT 1 FROM %s x WHERE %s AND %s))", ordCol, tbl, match("x", "v"), reach("x")))
			cond += " AND (SELECT count(*) FROM hit) >= 0"
		}
		ctes = append(ctes, fmt.Sprintf("u AS (UPDATE %s AS %s SET %s FROM v WHERE %s RETURNING %s.*, v.%s)",
			tbl, qa, strings.Join(sets, ", "), cond, qa, ordCol))
		fresh = fmt.Sprintf("NOT EXISTS (SELECT 1 FROM u WHERE u.%s = v.%s)", ordCol, ordCol)
		if oc.WhereClause != nil {
			fresh = fmt.Sprintf("NOT EXISTS (SELECT 1 FROM hit WHERE hit.%s = v.%s) AND (SELECT count(*) FROM u) >= 0", ordCol, ordCol)
		}
	} else {
		fresh = fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s x WHERE %s AND %s)", tbl, match("x", "v"), reach("x"))
	}
	insert := fmt.Sprintf("INSERT INTO %s AS %s (%s) SELECT %s FROM v WHERE %s ORDER BY v.%s",
		tbl, qa, colList, colList, fresh, ordCol)

	if len(ins.ReturningList) == 0 {
		return "WITH " + with + strings.Join(ctes, ", ") + " " + insert, true
	}
	if oc.Action != pg.OnConflictAction_ONCONFLICT_UPDATE {
		ret, ok := returning(ins.ReturningList, t, map[string]bool{}, "")
		if !ok {
			return "", false
		}
		return "WITH " + with + strings.Join(ctes, ", ") + " " + insert + " RETURNING " + ret, true
	}
	ret, ok := returning(ins.ReturningList, t, map[string]bool{ins.Relation.Relname: true, alias: true}, "r")
	if !ok || len(t.Cols) == 0 {
		return "", false
	}
	// The inserted rows get their row of v by position, not by key: a NULL key matches
	// nothing (P13), and the client (Django's bulk_create) takes the ids in its order. f
	// holds the rows to insert, read once; the INSERT returns them in that order.
	var icols []string
	for _, c := range t.Cols {
		icols = append(icols, "i."+q(c))
	}
	ctes = append(ctes,
		fmt.Sprintf("f AS (SELECT * FROM v WHERE %s)", fresh),
		fmt.Sprintf("i AS (INSERT INTO %s AS %s (%s) SELECT %s FROM f ORDER BY f.%s RETURNING *)", tbl, qa, colList, colList, ordCol))
	return fmt.Sprintf("WITH %s%s SELECT %s FROM (SELECT * FROM u UNION ALL SELECT %s, f.%s FROM (SELECT *, row_number() OVER () AS %s FROM i) AS i JOIN (SELECT f.%s, row_number() OVER (ORDER BY f.%s) AS %s FROM f) AS f ON i.%s = f.%s) AS r ORDER BY r.%s",
		with, strings.Join(ctes, ", "), ret, strings.Join(icols, ", "), ordCol, rnCol, ordCol, ordCol, rnCol, rnCol, rnCol, ordCol), true
}

// sameOrg: an UPDATE that sets the organization column gets lokadb.same_org(<new>, <old>),
// which lets the value through unchanged (Django's save() and Rails write it every time) and
// refuses a real change with a lokadb error: moving a row to another organization is not
// supported (the RLS policy or the organization's CHECK refused it without saying why).
func (a *Analysis) sameOrg(n *pg.Node, u *pg.UpdateStmt, t *Table, m *Meta) string {
	alias := u.Relation.Relname
	if u.Relation.Alias != nil && u.Relation.Alias.Aliasname != "" {
		alias = u.Relation.Alias.Aliasname
	}
	var changed []*pg.ResTarget
	var orig []*pg.Node
	for _, x := range u.TargetList {
		rt := x.GetResTarget()
		if rt == nil || rt.Name != t.TenantCol || len(rt.Indirection) > 0 || rt.Val.GetMultiAssignRef() != nil {
			continue
		}
		changed, orig = append(changed, rt), append(orig, rt.Val)
		rt.Val = &pg.Node{Node: &pg.Node_FuncCall{FuncCall: &pg.FuncCall{
			Funcname:   []*pg.Node{strNode("lokadb"), strNode("same_org")},
			Args:       []*pg.Node{rt.Val, {Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(alias), strNode(t.TenantCol)}}}}},
			Funcformat: pg.CoercionForm_COERCE_EXPLICIT_CALL}}}
	}
	if len(changed) == 0 {
		return ""
	}
	s, err := deparseStmt(n)
	for i, rt := range changed {
		rt.Val = orig[i]
	}
	if err != nil {
		return ""
	}
	a.scan(n, m) // the statement as the client wrote it
	return s
}

// localUpsert prepares the upsert as written, aimed at a local partition (TargetLocal). The
// partition has none of the parent's policies, and DEFAULT holds every cloud organization:
// DO UPDATE is kept to the organization, so a conflict with another organization's row (a
// key unique only within the partition) updates nothing instead of that row.
func (a *Analysis) localUpsert(n *pg.Node, ins *pg.InsertStmt, t *Table, m *Meta) {
	oc := ins.OnConflictClause
	if oc == nil || t.TenantCol == "" || oc.Infer == nil || oc.Infer.Conname != "" || oc.Infer.WhereClause != nil {
		return
	}
	// The partition does not check the rows' organization (the parent's policy does): only
	// rows that name it in the organization column itself, which the proxy then reads, go
	// there. A child row's organization from its parent goes through the parent.
	if a.Unhinted || len(a.Hints) == 0 {
		return
	}
	// Nor does it have lokadb.same_org: a DO UPDATE that assigns the organization column would
	// move the row to another organization past the policy (R6 of the routing review). Through
	// the parent the policy stops it. Only excluded.<organization column> is the same value:
	// every row of the statement names the one organization the proxy found, and DO UPDATE is
	// kept to its rows.
	if oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
		for _, x := range oc.TargetList {
			rt := x.GetResTarget()
			if rt == nil || rt.Name != t.TenantCol {
				continue
			}
			cr := rt.Val.GetColumnRef()
			if cr == nil || len(cr.Fields) != 2 || cr.Fields[0].GetString_().GetSval() != "excluded" || cr.Fields[1].GetString_().GetSval() != t.TenantCol {
				return
			}
		}
	}
	for _, h := range a.Hints {
		if h.Table != t.Name || h.Column != t.TenantCol {
			return
		}
	}
	lokadbRefs := 0
	walk(n.ProtoReflect(), func(msg proto.Message) {
		if r, ok := msg.(*pg.RangeVar); ok && m.table(r) != nil {
			lokadbRefs++
		}
	})
	if lokadbRefs != 1 { // a subquery over LokaDB tables needs the parent
		return
	}
	n = proto.Clone(n).(*pg.Node)
	ins = n.GetInsertStmt()
	oc = ins.OnConflictClause
	rv := ins.Relation
	alias := rv.Relname
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		alias = rv.Alias.Aliasname
	} else {
		unschema(n, rv, alias)
		rv.Alias = &pg.Alias{Aliasname: alias} // "documents"."title" keeps resolving
	}
	rv.Relname, rv.Schemaname = targetMark, ""
	if oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
		cond := &pg.Node{Node: &pg.Node_AExpr{AExpr: &pg.A_Expr{Kind: pg.A_Expr_Kind_AEXPR_OP, Name: []*pg.Node{strNode("=")},
			Lexpr: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(alias), strNode(t.TenantCol)}}}},
			Rexpr: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(orgMark)}}}}}}}
		if oc.WhereClause != nil {
			cond = &pg.Node{Node: &pg.Node_BoolExpr{BoolExpr: &pg.BoolExpr{Boolop: pg.BoolExprType_AND_EXPR, Args: []*pg.Node{oc.WhereClause, cond}}}}
		}
		oc.WhereClause = cond
	}
	s, err := deparseStmt(n)
	marks := 0
	if oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
		marks = 1
	}
	if err == nil && strings.Count(s, targetMark) == 1 && strings.Count(s, orgMark) == marks {
		a.Target, a.TargetSQL, a.TargetLocal = t.Name, s, true
	}
}

// rewriteMerge turns MERGE into a LokaDB table into statements Postgres runs on a partitioned
// table with foreign partitions: MERGE itself it refuses there for every organization once one
// organization has its own database ("cannot execute MERGE on relation … foreign table").
// Supported: one WHEN MATCHED (UPDATE, DELETE, DO NOTHING) and one WHEN NOT MATCHED [BY
// TARGET] (INSERT, DO NOTHING), each with an optional AND condition:
//
//	WITH src AS (SELECT src.*, <row number> FROM <source>),
//	     hit AS (SELECT <row number> FROM src WHERE EXISTS (SELECT 1 FROM t WHERE <on>)),
//	     m AS (UPDATE t SET … FROM src WHERE <on> AND <cond> AND <hit read>)
//	INSERT INTO t (…) SELECT … FROM src WHERE <not in hit> AND <m done> AND <cond>
//
// MERGE decides "matched" before it acts. In the cloud the statement has one snapshot; in an
// organization's database postgres_fdw's transaction sees its own writes, so the order is
// forced: hit is read first, m runs before the INSERT (nothing reading m, it would run after
// it and change the rows just inserted). The second return value is a refusal for other forms.
func rewriteMerge(ms *pg.MergeStmt, t *Table) (string, string) {
	const unsupported = "lokadb: MERGE into %s: %s is not supported on LokaDB tables yet; supported are one WHEN MATCHED (UPDATE, DELETE, DO NOTHING) and one WHEN NOT MATCHED (INSERT, DO NOTHING), or INSERT … ON CONFLICT"
	no := func(what string) (string, string) { return "", fmt.Sprintf(unsupported, t.Name, what) }
	if ms.WithClause != nil {
		return no("WITH")
	}
	if len(ms.ReturningList) > 0 {
		return no("RETURNING")
	}
	q := quoteIdent
	ta := ms.Relation.Relname
	if ms.Relation.Alias != nil && ms.Relation.Alias.Aliasname != "" {
		ta = ms.Relation.Alias.Aliasname
	} else {
		unschema(ms, ms.Relation, ta)
	}
	tbl := qrel(relKey(ms.Relation)) + " AS " + q(ta)

	var src string
	switch {
	case ms.SourceRelation.GetRangeVar() != nil:
		rv := ms.SourceRelation.GetRangeVar()
		src = rv.Relname
		if rv.Alias != nil && rv.Alias.Aliasname != "" {
			src = rv.Alias.Aliasname
		}
	case ms.SourceRelation.GetRangeSubselect() != nil && ms.SourceRelation.GetRangeSubselect().Alias != nil:
		src = ms.SourceRelation.GetRangeSubselect().Alias.Aliasname
	default:
		return no("this source (a join or a function)")
	}
	srcSQL, err := deparseStmt(&pg.Node{Node: &pg.Node_SelectStmt{SelectStmt: &pg.SelectStmt{
		TargetList: []*pg.Node{
			{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(src), {Node: &pg.Node_AStar{AStar: &pg.A_Star{}}}}}}}}}},
			{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: ordCol, Val: &pg.Node{Node: &pg.Node_FuncCall{FuncCall: &pg.FuncCall{
				Funcname: []*pg.Node{strNode("row_number")}, Over: &pg.WindowDef{FrameOptions: 1058}, Funcformat: pg.CoercionForm_COERCE_EXPLICIT_CALL}}}}}},
		},
		FromClause:  []*pg.Node{ms.SourceRelation},
		Op:          pg.SetOperation_SETOP_NONE,
		LimitOption: pg.LimitOption_LIMIT_OPTION_DEFAULT,
	}}})
	if err != nil {
		return no("this source")
	}
	on, err := deparseExpr(ms.JoinCondition)
	if err != nil {
		return no("this ON condition")
	}
	on = "(" + on + ")"
	// As in the upsert (REALISTIC_REVIEW 1.2): the match by a join with src is not sent to an
	// organization's database, the keys as an array are.
	for _, c := range conjuncts(ms.JoinCondition) {
		ae := c.GetAExpr()
		if ae == nil || ae.Kind != pg.A_Expr_Kind_AEXPR_OP || len(ae.Name) != 1 || ae.Name[0].GetString_().GetSval() != "=" {
			continue
		}
		l, r := ae.Lexpr.GetColumnRef(), ae.Rexpr.GetColumnRef()
		if l == nil || r == nil {
			continue
		}
		if len(r.Fields) == 2 && r.Fields[0].GetString_().GetSval() == ta {
			l, r = r, l
		}
		if len(l.Fields) != 2 || l.Fields[0].GetString_().GetSval() != ta || len(r.Fields) != 2 || r.Fields[0].GetString_().GetSval() != src || l.Fields[1].GetString_() == nil || r.Fields[1].GetString_() == nil {
			continue
		}
		on += fmt.Sprintf(" AND %s.%s = ANY (ARRAY(SELECT %s.%s FROM %s))", q(ta), q(l.Fields[1].GetString_().Sval), q(src), q(r.Fields[1].GetString_().Sval), q(src))
	}

	var matched, notMatched *pg.MergeWhenClause
	for _, n := range ms.MergeWhenClauses {
		w := n.GetMergeWhenClause()
		switch {
		case w == nil || w.MatchKind == pg.MergeMatchKind_MERGE_WHEN_NOT_MATCHED_BY_SOURCE:
			return no("WHEN NOT MATCHED BY SOURCE")
		case w.MatchKind == pg.MergeMatchKind_MERGE_WHEN_MATCHED && matched == nil:
			matched = w
		case w.MatchKind == pg.MergeMatchKind_MERGE_WHEN_NOT_MATCHED_BY_TARGET && notMatched == nil:
			notMatched = w
		default:
			return no("more than one WHEN clause of a kind")
		}
	}
	cond := func(w *pg.MergeWhenClause) (string, bool) {
		if w.Condition == nil {
			return "", true
		}
		s, err := deparseExpr(w.Condition)
		return " AND (" + s + ")", err == nil
	}

	ctes := []string{fmt.Sprintf("%s AS (%s)", q(src), srcSQL)}
	const afterM = "__lokadb_after_m__" // where the INSERT waits for m, when there is one
	hit := fmt.Sprintf("hit AS (SELECT %s.%s FROM %s WHERE EXISTS (SELECT 1 FROM %s WHERE %s))", q(src), ordCol, q(src), tbl, on)
	var change, insert string
	if matched != nil && matched.CommandType != pg.CmdType_CMD_NOTHING {
		c, ok := cond(matched)
		if !ok {
			return no("this WHEN MATCHED condition")
		}
		switch matched.CommandType {
		case pg.CmdType_CMD_UPDATE:
			var sets []string
			for _, n := range matched.TargetList {
				rt := n.GetResTarget()
				if rt == nil || len(rt.Indirection) > 0 {
					return no("UPDATE SET of a part of a column")
				}
				e, err := deparseExpr(rt.Val)
				if err != nil {
					return no("this UPDATE SET")
				}
				sets = append(sets, q(rt.Name)+" = "+e)
			}
			change = fmt.Sprintf("UPDATE %s SET %s FROM %s WHERE %s%s", tbl, strings.Join(sets, ", "), q(src), on, c)
		case pg.CmdType_CMD_DELETE:
			change = fmt.Sprintf("DELETE FROM %s USING %s WHERE %s%s", tbl, q(src), on, c)
		default:
			return no("this WHEN MATCHED action")
		}
	}
	if notMatched != nil && notMatched.CommandType != pg.CmdType_CMD_NOTHING {
		c, ok := cond(notMatched)
		if !ok || notMatched.CommandType != pg.CmdType_CMD_INSERT {
			return no("this WHEN NOT MATCHED clause")
		}
		if notMatched.Override != pg.OverridingKind_OVERRIDING_NOT_SET {
			return no("INSERT OVERRIDING")
		}
		if len(notMatched.Values) == 0 {
			return no("INSERT DEFAULT VALUES")
		}
		var cols, vals []string
		for _, n := range notMatched.TargetList {
			cols = append(cols, q(n.GetResTarget().GetName()))
		}
		if len(cols) == 0 {
			for _, col := range t.Cols {
				cols = append(cols, q(col))
			}
		}
		for _, n := range notMatched.Values {
			if n.GetSetToDefault() != nil {
				return no("DEFAULT in INSERT VALUES")
			}
			e, err := deparseExpr(n)
			if err != nil {
				return no("these INSERT VALUES")
			}
			vals = append(vals, e)
		}
		if len(cols) != len(vals) {
			return no("INSERT without a column list")
		}
		insert = fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s.%s NOT IN (SELECT %s FROM hit)%s%s",
			tbl, strings.Join(cols, ", "), strings.Join(vals, ", "), q(src), q(src), ordCol, ordCol, afterM, c)
	}
	switch {
	case change != "" && insert != "":
		ctes = append(ctes, hit, "m AS ("+change+" AND (SELECT count(*) FROM hit) >= 0 RETURNING 1)")
		return "WITH " + strings.Join(ctes, ", ") + " " + strings.Replace(insert, afterM, " AND (SELECT count(*) FROM m) >= 0", 1), ""
	case insert != "":
		ctes = append(ctes, hit)
		return "WITH " + strings.Join(ctes, ", ") + " " + strings.Replace(insert, afterM, "", 1), ""
	case change != "":
		return "WITH " + strings.Join(ctes, ", ") + " " + change, ""
	}
	return "WITH " + strings.Join(ctes, ", ") + " SELECT FROM " + q(src) + " WHERE false", ""
}

// returning deparses a RETURNING list, expanding * so helper columns never leak to the client.
func returning(list []*pg.Node, t *Table, from map[string]bool, to string) (string, bool) {
	var out []string
	for _, n := range list {
		rt := n.GetResTarget()
		if rt == nil {
			return "", false
		}
		if cr := rt.Val.GetColumnRef(); cr != nil && cr.Fields[len(cr.Fields)-1].GetAStar() != nil {
			for _, c := range t.Cols {
				if to != "" {
					out = append(out, to+"."+quoteIdent(c))
				} else {
					out = append(out, quoteIdent(c))
				}
			}
			continue
		}
		if to != "" {
			requalify(rt.Val, from, to, to)
		}
		s, err := deparseTarget(rt)
		if err != nil {
			return "", false
		}
		out = append(out, s)
	}
	return strings.Join(out, ", "), true
}
