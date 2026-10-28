package proxy

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// Writes on the direct path, stage 8b (tasks/direct-path.md, section 5; coordinator's
// decision of 25.10). Through the FDW the cloud keeps a moved organization's writes whole: the
// parent's row triggers, cloned onto the foreign partition, keep <table>__refs (lokadb_refs),
// the cloud's defaults fill the key and the other columns. A write straight in the
// organization's database fires none of them, so the proxy does what they did, in the order
// postgres_fdw keeps:
//
//  1. the cloud: BEGIN, the organization's pause lock (for the transaction), the gate's check
//     and, for an INSERT, the refs rows of the new keys (nextval of the key's sequence when the
//     statement has no key), all in one round trip; the transaction stays open;
//  2. the organization's database: the INSERT, its key and organization column added; or the
//     DELETE in a transaction of its own, RETURNING the keys, then those refs rows deleted in
//     the cloud's open transaction (the cloud's FKs onto __refs act there: RESTRICT, CASCADE);
//  3. an error before the commits rolls both sides back (no refs left for a failed INSERT, the
//     organization's row kept when the cloud refuses the DELETE);
//  4. the organization's database commits first, then the cloud. A crash between the two
//     leaves a row without refs, or refs of a deleted row: the window postgres_fdw has, which
//     refs-diff / refs-sync find and repair.
//
// The client gets the organization's answer once both have committed.

// directWrite: an INSERT or a DELETE the direct path takes.
type directWrite struct {
	table *Table
	del   bool
	stmt  *pg.Node // the client's statement, copied and rewritten for each run
	// INSERT: its rows; keyAt: the key's place in a row (-1: the proxy adds it from the key's
	// sequence); addTenant: the statement does not name the organization column (a child table's
	// INSERT under its parent), the proxy adds it
	rows      [][]*pg.Node
	keyAt     int
	addTenant bool
	// upsert: an INSERT … ON CONFLICT (block 2 of 8b.2), run natively in the organization's
	// database, whose unique indexes are the cloud's DEFAULT partition's (Table.unique). The
	// organization's statement returns, after the client's own RETURNING, the key and whether
	// the row was inserted (directWrite.added): the keys the cloud gave or the refs rows it added
	// for rows that were not inserted (updated, or nothing done) leave __refs before its commit.
	// returning: the client's statement has a RETURNING of its own. doUpdate: ON CONFLICT DO
	// UPDATE, of the columns setCols: the gate checks the client's UPDATE privilege on them, as
	// Postgres does for the cloud's parent (the organization's database runs it as the direct
	// role, which may write everything).
	upsert, returning, doUpdate bool
	setCols                     []string
	// sel: an INSERT … SELECT that names the key (block 2 of 8b.2): its rows are known once it
	// has run, so it runs in a transaction of the organization's database, RETURNING the keys
	// after the client's own RETURNING; their refs rows follow in the cloud's open transaction
	// (a key of another organization rolls both back), then the two commits.
	sel bool
	// unnest: the arrays of INSERT … SELECT * FROM unnest(…) without its key (Django 5.2's
	// bulk_create): its rows are as many as the longest array, so the cloud gives their keys
	// beforehand as for VALUES rows, and the keys go in as one more array (block 2 of 8b.2).
	unnest []*pg.Node
	// keyCols, keyAts, keyKinds: a key of several columns (tasks/composite-direct.md): its
	// columns, the place of each in an INSERT's row (-1: not named; the organization column's
	// value is then the organization, another's the cloud's default, Table.KeyDefaults) and the
	// kinds of their values (Meta.TupleKinds). Its keys are tuples (tupleKey), its refs columns
	// id_1 … id_n. nil for a key of one column.
	keyCols  []string
	keyAts   []int
	keyKinds []string
}

// added: the columns the organization's statement returns after the client's own: an
// upsert's key and whether the row was inserted (xmax is 0 for a row the statement inserted,
// the transaction's id for one it updated); an INSERT … SELECT's key.
func (w *directWrite) added() int {
	k := max(1, len(w.keyCols)) // a composite key's columns
	switch {
	case w.upsert:
		return k + 1
	case w.sel:
		return k
	}
	return 0
}

// The proxy's own statements of a direct write in the cloud (the refs rows) name their CTEs
// and alias lokadb_k, lokadb_v, lokadb_r, lokadb_d: pg_stat_statements counts them under the
// client's role and keeps no leading comment (a statement's text starts at its first token),
// so check knows them by these names (proxyMade) and leaves them out of the workload.

// directIdleProbe: a direct connection idle this long is probed (a Sync) before a write: relay,
// agent or NAT drop idle connections, and a write that fails on one cannot be run again (it
// may have run).
const directIdleProbe = 30 * time.Second

// buildWrite: the write the direct path takes for top, nil for the cloud's way. An INSERT of
// VALUES rows with a column list, without ON CONFLICT, OVERRIDING, DEFAULT in a row or a WITH;
// every column with a default of the cloud named, but the key (its default computed in the
// cloud, KeyDefault) and the organization column; a key named is a constant or a parameter. A DELETE without USING,
// RETURNING or WITH. Neither on a table with row triggers of the cloud on that event
// (RowTriggers: an INSERT by triggers on INSERT, a DELETE by triggers on DELETE), identity
// or generated columns.
func buildWrite(top *pg.Node, m *Meta) *directWrite {
	switch {
	case top.GetInsertStmt() != nil:
		ins := top.GetInsertStmt()
		t := m.table(ins.Relation)
		sel := ins.SelectStmt.GetSelectStmt()
		if t == nil || ins.WithClause != nil || ins.Override != pg.OverridingKind_OVERRIDING_NOT_SET ||
			len(ins.Cols) == 0 || sel == nil || m.rowTrigger(t.Name, trigInsert) || t.Identity || t.Generated {
			return nil
		}
		isSel := len(sel.ValuesLists) == 0
		if oc := ins.OnConflictClause; oc != nil && (isSel || !upsertTakes(oc, t, m)) {
			return nil // an INSERT … SELECT … ON CONFLICT: the cloud's way
		}
		w := &directWrite{table: t, stmt: top, keyAt: -1, addTenant: true, upsert: ins.OnConflictClause != nil, returning: len(ins.ReturningList) > 0, sel: isSel}
		if oc := ins.OnConflictClause; oc != nil && oc.Action == pg.OnConflictAction_ONCONFLICT_UPDATE {
			w.doUpdate = true
			for _, n := range oc.TargetList {
				w.setCols = append(w.setCols, n.GetResTarget().GetName())
			}
		}
		key := tableKey(t)
		if !w.composite(t, m) {
			return nil
		}
		named := map[string]bool{}
		for i, c := range ins.Cols {
			name := c.GetResTarget().GetName()
			if len(c.GetResTarget().GetIndirection()) > 0 {
				return nil // a field or an element of a column
			}
			named[name] = true
			if j := slices.Index(w.keyCols, name); j >= 0 {
				w.keyAts[j] = i
			}
			switch name {
			case key:
				w.keyAt = i
			case t.TenantCol:
				w.addTenant = false
			}
		}
		for c := range t.Defaults {
			if !named[c] && c != key && c != t.TenantCol && !slices.Contains(w.keyCols, c) {
				return nil // its default is the cloud's: the organization's table has none
			}
		}
		if w.keyCols != nil {
			for _, c := range w.genCols() {
				if t.KeyDefaults[c] == "" {
					return nil // a column of the key neither named nor with a default
				}
			}
			if isSel { // RETURNING the key's columns; those it does not name: an unnest's, from the cloud's defaults
				if len(w.genCols()) > 0 {
					if w.unnest = plainUnnest(sel); w.unnest == nil {
						return nil // another INSERT … SELECT without its key: its rows are not counted before it runs
					}
				}
				return w
			}
			for _, vl := range sel.ValuesLists {
				row := vl.GetList().GetItems()
				if len(row) != len(ins.Cols) {
					return nil
				}
				for i, e := range row {
					if e.GetSetToDefault() != nil {
						return nil
					}
					if slices.Contains(w.keyAts, i) {
						if v, ok := value(e); !ok || v.Null || v.Array {
							return nil
						}
					}
				}
				w.rows = append(w.rows, row)
			}
			return w
		}
		if w.keyAt < 0 && w.sel && t.KeyDefault != "" {
			w.unnest, w.sel = plainUnnest(sel), false
			if w.unnest == nil {
				return nil // another INSERT … SELECT without its key: its rows are not counted before it runs
			}
		}
		if w.keyAt < 0 && (t.KeyDefault == "" || w.sel) {
			return nil
		}
		for _, vl := range sel.ValuesLists {
			row := vl.GetList().GetItems()
			if len(row) != len(ins.Cols) {
				return nil
			}
			for _, e := range row {
				if e.GetSetToDefault() != nil {
					return nil
				}
			}
			if w.keyAt >= 0 {
				if v, ok := value(row[w.keyAt]); !ok || v.Null || v.Array {
					return nil
				}
			}
			w.rows = append(w.rows, row)
		}
		return w
	case top.GetDeleteStmt() != nil:
		del := top.GetDeleteStmt()
		t := m.table(del.Relation)
		if t == nil || del.WithClause != nil || len(del.UsingClause) > 0 || len(del.ReturningList) > 0 || m.rowTrigger(t.Name, trigDelete) || m.RefsFromLokadb[t.Name] {
			return nil
		}
		w := &directWrite{table: t, del: true, stmt: top}
		if !w.composite(t, m) {
			return nil
		}
		return w
	}
	return nil
}

// composite: w's key columns when t's key has several (keyCols); false when their kinds are
// not known.
func (w *directWrite) composite(t *Table, m *Meta) bool {
	cols, ok := tupleCols(t.Key)
	if !ok {
		return true
	}
	w.keyCols, w.keyKinds = cols, m.tupleKinds(t.Refs)
	w.keyAts = slices.Repeat([]int{-1}, len(cols))
	return len(w.keyKinds) == len(cols)
}

// genCols: the columns of a composite key an INSERT does not name, but the organization
// column: the cloud's defaults give their values.
func (w *directWrite) genCols() []string {
	var out []string
	for i, c := range w.keyCols {
		if w.keyAts[i] < 0 && c != w.table.TenantCol {
			out = append(out, c)
		}
	}
	return out
}

// keysGiven: an INSERT whose rows give their keys whole (a composite key's organization
// column, when not named, is the organization).
func (w *directWrite) keysGiven() bool {
	if w.keyCols == nil {
		return w.keyAt >= 0
	}
	return len(w.genCols()) == 0
}

// genKeys: the cloud's statement giving n rows of the defaults of a composite key's columns an
// INSERT does not name (genCols), as text: an unnest's or a COPY's, whose other columns the
// proxy does not read; their refs rows follow the organization's RETURNING (refsAdd).
func (w *directWrite) genKeys(n int) string {
	var exprs []string
	for i, c := range w.genCols() {
		exprs = append(exprs, fmt.Sprintf("(%s)::text AS k%d", w.table.KeyDefaults[c], i+1))
	}
	return fmt.Sprintf("WITH lokadb_k AS (SELECT %s FROM generate_series(1, %d)) SELECT * FROM lokadb_k", strings.Join(exprs, ", "), n)
}

// refsCols: the key columns of the refs table: id, or id_1 … id_n.
func (w *directWrite) refsCols() string {
	if w.keyCols == nil {
		return "id"
	}
	var c []string
	for i := range w.keyCols {
		c = append(c, fmt.Sprintf("id_%d", i+1))
	}
	return strings.Join(c, ", ")
}

// lits: keys as SQL of the refs table's key columns (keyLits); a composite key's as the list
// of its values, "a, b".
func (w *directWrite) lits(keys []string, kind, typ string) []string {
	if w.keyCols == nil {
		return keyLits(keys, kind, typ)
	}
	out := make([]string, len(keys))
	for i, k := range keys {
		vals, _ := tupleVals(k)
		l := make([]string, len(vals))
		for j, v := range vals {
			if j < len(w.keyCols) {
				l[j] = keyLits([]string{v}, w.keyKinds[j], w.table.ColType[w.keyCols[j]])[0]
			}
		}
		out[i] = strings.Join(l, ", ")
	}
	return out
}

// refsDelete: the cloud's statement deleting the refs rows of keys.
func (w *directWrite) refsDelete(refs string, keys []string, kind, typ string) string {
	if w.keyCols == nil {
		return "DELETE FROM " + refs + " AS lokadb_d WHERE id IN (" + strings.Join(keyLits(keys, kind, typ), ", ") + ")"
	}
	return "DELETE FROM " + refs + " AS lokadb_d WHERE (" + w.refsCols() + ") IN ((" + strings.Join(w.lits(keys, kind, typ), "), (") + "))"
}

// plainUnnest: the arrays of SELECT * FROM unnest(a1, a2, …), with no column list for unnest's
// result (one more array, the keys, goes in after them), nil for another SELECT.
func plainUnnest(sel *pg.SelectStmt) []*pg.Node {
	args := unnestArgs(sel)
	if args == nil || len(sel.TargetList) != 1 || sel.Op != pg.SetOperation_SETOP_NONE || len(sel.SortClause) > 0 || sel.LimitCount != nil {
		return nil
	}
	cr := sel.TargetList[0].GetResTarget().GetVal().GetColumnRef()
	if cr == nil || len(cr.Fields) != 1 || cr.Fields[0].GetAStar() == nil {
		return nil
	}
	rf := sel.FromClause[0].GetRangeFunction()
	if rf.Ordinality || rf.Alias != nil && len(rf.Alias.Colnames) > 0 || len(rf.Coldeflist) > 0 {
		return nil
	}
	return args
}

// unnestRows: how many rows an unnest INSERT inserts: the elements of its longest array; false
// when an array cannot be read (a NULL element of a binary one, an expression).
func (w *directWrite) unnestRows(vals valuesFunc) (int, bool) {
	n := 0
	for _, a := range w.unnest {
		refs, ok := arrayValues(a)
		if !ok {
			return 0, false
		}
		count := 0
		for _, r := range refs {
			if !r.Array {
				count++
				continue
			}
			r.Kind = kindText // counted, not read as keys
			vs, ok := vals(r)
			if !ok {
				return 0, false
			}
			count += len(vs)
		}
		n = max(n, count)
	}
	return n, true
}

// upsertTakes: an ON CONFLICT the organization's database runs as the cloud would: DO NOTHING
// with no target (any unique index: the organization's are the DEFAULT partition's, Django's
// bulk_create(ignore_conflicts=True)), or a target of plain columns that are a unique index of
// the organization's table (Table.unique; not ON CONSTRAINT, not a partial index's WHERE); DO
// UPDATE neither changes the key or the organization column nor meets a row trigger of the
// cloud on UPDATE.
func upsertTakes(oc *pg.OnConflictClause, t *Table, m *Meta) bool {
	inf := oc.Infer
	switch {
	case inf == nil:
		if oc.Action != pg.OnConflictAction_ONCONFLICT_NOTHING {
			return false
		}
	case inf.Conname != "" || inf.WhereClause != nil:
		return false
	default:
		var cols []string
		for _, e := range inf.IndexElems {
			ie := e.GetIndexElem()
			if ie == nil || ie.Name == "" || ie.Expr != nil || len(ie.Collation) > 0 || len(ie.Opclass) > 0 {
				return false
			}
			cols = append(cols, ie.Name)
		}
		if !t.unique(cols) {
			return false
		}
	}
	switch oc.Action {
	case pg.OnConflictAction_ONCONFLICT_NOTHING:
		return true
	case pg.OnConflictAction_ONCONFLICT_UPDATE:
		if m.rowTrigger(t.Name, trigUpdate) {
			return false
		}
		for _, n := range oc.TargetList {
			rt := n.GetResTarget()
			kc, _ := tupleCols(t.Key)
			if rt == nil || rt.Name == tableKey(t) || rt.Name == t.TenantCol || slices.Contains(kc, rt.Name) {
				return false // the row would change its key or organization: __refs
			}
		}
		return true
	}
	return false
}

func tableKey(t *Table) string {
	if t.Key == "" {
		return "id"
	}
	return t.Key
}

// rewrite: the statement the organization's database runs: an INSERT with the key (keys, one
// per row, when the client gave none) and the organization column (org) added, a DELETE
// RETURNING the key.
func (w *directWrite) rewrite(keys []string, org string) (string, error) {
	n := proto.Clone(w.stmt).(*pg.Node)
	key := tableKey(w.table)
	lit := func(v string) *pg.Node {
		return &pg.Node{Node: &pg.Node_AConst{AConst: &pg.A_Const{Val: &pg.A_Const_Sval{Sval: &pg.String{Sval: v}}}}}
	}
	if w.del {
		cols := w.keyCols
		if cols == nil {
			cols = []string{key}
		}
		ret := &n.GetDeleteStmt().ReturningList
		for _, c := range cols {
			*ret = append(*ret, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{
				Val: &pg.Node{Node: &pg.Node_ColumnRef{ColumnRef: &pg.ColumnRef{Fields: []*pg.Node{strNode(c)}}}}}}})
		}
		return deparseStmt(n)
	}
	ins := n.GetInsertStmt()
	ret := quoteIdent(key) // the key's columns an upsert or an INSERT … SELECT returns after the client's RETURNING
	if w.keyCols != nil {
		var qk []string
		for _, c := range w.keyCols {
			qk = append(qk, quoteIdent(c))
		}
		ret = strings.Join(qk, ", ")
	}
	if w.keyCols != nil && !w.sel { // the composite key's columns the cloud's defaults gave, the organization column
		gen := w.genCols()
		for _, c := range gen {
			ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: c}}})
		}
		if w.addTenant {
			ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: w.table.TenantCol}}})
		}
		for i, vl := range ins.SelectStmt.GetSelectStmt().ValuesLists {
			l := vl.GetList()
			if len(gen) > 0 {
				vals, _ := tupleVals(keys[i])
				for _, c := range gen {
					l.Items = append(l.Items, lit(vals[slices.Index(w.keyCols, c)]))
				}
			}
			if w.addTenant {
				l.Items = append(l.Items, lit(org))
			}
		}
		if w.upsert {
			extra, err := pg.Parse("SELECT " + ret + ", xmax::text = '0'")
			if err != nil {
				return "", err
			}
			ins.ReturningList = append(ins.ReturningList, extra.Stmts[0].Stmt.GetSelectStmt().TargetList...)
		}
		return deparseStmt(n)
	}
	if w.sel {
		if w.keyCols != nil && w.unnest != nil { // the key's columns the cloud's defaults gave: more arrays of unnest (keys: a row of them per element)
			fc := ins.SelectStmt.GetSelectStmt().FromClause[0].GetRangeFunction().Functions[0].GetList().Items[0].GetFuncCall()
			for i, a := range w.genArrays(keys) {
				arr, err := pg.Parse("SELECT " + a)
				if err != nil {
					return "", err
				}
				fc.Args = append(fc.Args, arr.Stmts[0].Stmt.GetSelectStmt().TargetList[0].GetResTarget().GetVal())
				ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: w.genCols()[i]}}})
			}
		}
		if w.addTenant { // SELECT lokadb_s.*, <org> FROM (<the client's SELECT>) lokadb_s
			ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: w.table.TenantCol}}})
			wrap, err := pg.Parse("SELECT lokadb_s.*, " + sqlString(org) + " FROM (SELECT) lokadb_s")
			if err != nil {
				return "", err
			}
			outer := wrap.Stmts[0].Stmt
			outer.GetSelectStmt().FromClause[0].GetRangeSubselect().Subquery = ins.SelectStmt
			ins.SelectStmt = outer
		}
		extra, err := pg.Parse("SELECT " + ret)
		if err != nil {
			return "", err
		}
		ins.ReturningList = append(ins.ReturningList, extra.Stmts[0].Stmt.GetSelectStmt().TargetList...)
		return deparseStmt(n)
	}
	if w.unnest != nil { // the keys as one more array of unnest, the organization as a constant
		sel := ins.SelectStmt.GetSelectStmt()
		fc := sel.FromClause[0].GetRangeFunction().Functions[0].GetList().Items[0].GetFuncCall()
		arr, err := pg.Parse("SELECT ARRAY[" + strings.Join(keyLits(keys, keyKindOf(w.table.ColType[key]), w.table.ColType[key]), ", ") + "]::" + w.table.ColType[key] + "[]")
		if err != nil {
			return "", err
		}
		fc.Args = append(fc.Args, arr.Stmts[0].Stmt.GetSelectStmt().TargetList[0].GetResTarget().GetVal())
		ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: key}}})
		if w.addTenant {
			ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: w.table.TenantCol}}})
			sel.TargetList = append(sel.TargetList, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Val: lit(org)}}})
		}
		return deparseStmt(n)
	}
	if w.keyAt < 0 {
		ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: key}}})
	}
	if w.addTenant {
		ins.Cols = append(ins.Cols, &pg.Node{Node: &pg.Node_ResTarget{ResTarget: &pg.ResTarget{Name: w.table.TenantCol}}})
	}
	for i, vl := range ins.SelectStmt.GetSelectStmt().ValuesLists {
		l := vl.GetList()
		if w.keyAt < 0 {
			l.Items = append(l.Items, lit(keys[i]))
		}
		if w.addTenant {
			l.Items = append(l.Items, lit(org))
		}
	}
	if w.upsert {
		extra, err := pg.Parse("SELECT " + quoteIdent(key) + ", xmax::text = '0'")
		if err != nil {
			return "", err
		}
		ins.ReturningList = append(ins.ReturningList, extra.Stmts[0].Stmt.GetSelectStmt().TargetList...)
	}
	return deparseStmt(n)
}

// givenKeys: the keys an INSERT names, read with vals as values of the key's kind (keys.go,
// block 17) in their canonical form; ok false when one cannot be read or is NULL.
func (w *directWrite) givenKeys(vals valuesFunc, kind string) ([]string, bool) {
	var keys []string
	for _, row := range w.rows {
		v, _ := value(row[w.keyAt])
		v.Kind = kind
		vs, ok := vals(v)
		if !ok || len(vs) != 1 || vs[0] == nullValue {
			return nil, false
		}
		keys = append(keys, vs[0])
	}
	return keys, true
}

// keyLits: keys as SQL of the refs table's id: an integer as it is (canonicalInt: digits
// alone), any other kind as a literal cast to the key's type (typ, format_type), since VALUES
// of untyped literals would be text against a uuid id (block 17).
func keyLits(keys []string, kind, typ string) []string {
	out := make([]string, len(keys))
	for i, k := range keys {
		if kind == kindInt {
			out[i] = k
		} else {
			out[i] = sqlString(k) + "::" + typ
		}
	}
	return out
}

// cloudRun runs qs on the cloud connection, each under a statement of the proxy's own, then a
// Sync; the capture holds their rows and the first error.
func (s *session) cloudRun(qs ...string) *capture {
	c := s.cloudStart(qs...)
	<-c.done
	return c
}

// cloudStart sends qs as cloudRun does without waiting for the answer: the caller waits on
// c.done (item 9: the cloud's COMMIT at once with the organization's).
func (s *session) cloudStart(qs ...string) *capture {
	c := &capture{done: make(chan struct{}), untilSync: true}
	var ms []pgproto3.FrontendMessage
	for i, q := range qs {
		st, p := fmt.Sprintf("lokadb_w%d", i), fmt.Sprintf("lokadb_wp%d", i)
		ms = append(ms, &pgproto3.Close{ObjectType: 'S', Name: st}, &pgproto3.Close{ObjectType: 'P', Name: p},
			&pgproto3.Parse{Name: st, Query: q}, &pgproto3.Bind{DestinationPortal: p, PreparedStatement: st}, &pgproto3.Execute{Portal: p})
	}
	if err := s.send(s.extMsgs(c, ms...)); err != nil {
		c.fail(err)
	}
	return c
}

// writeDirect runs di's write on dc (see above). simple: the client's simple query; else batch
// (directBatch's) with ptypes, its values read by vals. false: nothing ran anywhere, the
// statement goes the cloud's way.
func (s *session) writeDirect(dc *directConn, org string, di *directInfo, vals valuesFunc, simple string, batch []pgproto3.FrontendMessage, ptypes []uint32, an *Analysis) bool {
	if d := s.direct.tx; d != nil && d.org != "" {
		return s.writeInTx(d.dc, org, di, vals, simple, batch, ptypes, an)
	}
	if err := s.probeDirect(dc); err != nil {
		s.dropDirect(dc)
		var err2 error
		if dc, err2 = s.directConnFor(org); err2 != nil {
			s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err2)
			return false
		}
	}
	if s.direct.tx != nil { // a deferred transaction: its first statement opens it
		return s.writeInTx(dc, org, di, vals, simple, batch, ptypes, an)
	}
	w := di.write
	t := w.table
	refs := qrel(t.Refs)
	kind, typ := s.meta.keyKind(t.Refs), t.ColType[tableKey(t)]
	var keys, added []string
	n := len(w.rows) // the rows the cloud gives keys for (an unnest INSERT's: counted below)
	// BEGIN, cloudBusy, the lock and the check prepared once on the connection (direct_prep.go)
	qs := []cloudStmt{prepared("BEGIN"), prepared(cloudBusy), s.lockStmt("SELECT FROM pg_advisory_xact_lock_shared", org), s.gateCheck(di, org)}
	switch {
	case w.sel && w.keyCols != nil && w.unnest != nil: // the cloud's defaults give the key's columns unnest does not name, a row per element
		var ok bool
		if n, ok = w.unnestRows(vals); !ok {
			return false
		}
		qs = append(qs, plain(w.genKeys(n)))
	case w.del, w.sel: // the keys come from the organization's statement
	case w.keyCols != nil: // a composite key's rows, their refs rows in the same exchange (direct_composite.go)
		lits, ok := w.rowLits(vals, org)
		if !ok {
			return false
		}
		qs = append(qs, plain(w.tupleRefs(lits, refs, org)))
	case w.keysGiven():
		var ok bool
		if keys, ok = w.givenKeys(vals, kind); !ok {
			return false
		}
		// a key that maps to this organization already (a crash between the two commits) is
		// left to the organization's primary key; one of another organization is the cloud's
		// to refuse
		qs = append(qs, plain(givenRefs(w.lits(keys, kind, typ), w.refsCols(), refs, org, w.upsert)))
	default:
		if w.unnest != nil {
			var ok bool
			if n, ok = w.unnestRows(vals); !ok {
				return false
			}
		}
		qs = append(qs, plain(fmt.Sprintf("WITH lokadb_k AS (SELECT %s AS id FROM generate_series(1, %d)), lokadb_r AS (INSERT INTO %s (id, tenant) SELECT id, %s FROM lokadb_k) SELECT id::text FROM lokadb_k",
			t.KeyDefault, n, refs, orgLit(org))))
	}
	c := s.cloudRunStmts(qs...)
	rollback := func() { s.cloudRun("ROLLBACK") }
	if c.err != nil || len(c.rows) < 2 || len(c.rows[1]) != 3 || !s.gateAllows(c.rows[1], org, di) {
		rollback()
		if r := c.resp; r != nil && (strings.HasPrefix(r.Code, "57") || r.Code == "55P03" || r.Code == "40P01") {
			s.refuseDirect(r)
			return true
		}
		if c.err != nil {
			s.log.Warn("direct path gate failed: the write goes through the cloud", "org", org, "err", c.err)
		}
		return false
	}
	switch {
	case w.sel && w.keyCols != nil && w.unnest != nil:
		if len(c.rows) != 2+n {
			rollback()
			return false
		}
		for _, r := range c.rows[2:] {
			keys = append(keys, tupleKey(r))
		}
	case w.del, w.sel:
	case w.keyCols != nil:
		none, ks, add, read := w.tupleAnswer(c.rows[2:], len(w.rows))
		if !read || !none {
			rollback() // a key of another organization: the cloud's way answers as it does
			return false
		}
		keys, added = ks, add
	case w.keysGiven():
		if len(c.rows) < 3 || len(c.rows[2]) != 1 || c.rows[2][0] != "0" || !w.upsert && len(c.rows) != 3 {
			rollback() // a key of another organization: the cloud's way answers as it does
			return false
		}
		for _, r := range c.rows[3:] { // an upsert's: the refs rows the cloud added
			added = append(added, r[0])
		}
	default:
		if len(c.rows) != 2+n {
			rollback()
			return false
		}
		for _, r := range c.rows[2:] {
			keys = append(keys, r[0])
		}
	}
	sql, err := w.rewrite(keys, org)
	if err != nil {
		rollback()
		return false
	}
	s.traceDirect(simple+stmtSQL(batch, s), an, org)
	// the organization's database
	var msgs []pgproto3.FrontendMessage
	own := w.del || w.sel // a transaction of the organization's database: the cloud acts on its keys before it commits
	if own {
		msgs = append(msgs, &pgproto3.Query{String: "BEGIN"})
	}
	if batch == nil {
		msgs = append(msgs, &pgproto3.Query{String: sql})
	} else {
		msgs = append(msgs, &pgproto3.Parse{Query: sql, ParameterOIDs: ptypes})
		msgs = append(msgs, w.binds(batch)...)
	}
	answer, deleted, failed, err := s.orgWrite(dc, msgs, w.del, batch == nil)
	if err != nil && own { // its BEGIN is not committed: the organization rolls it back
		rollback()
		s.directLost(org, err)
		return true
	}
	if err != nil { // an INSERT that commits by itself: it may have (review PR-1); its refs rows stay
		if c := s.cloudRun("COMMIT"); c.err != nil {
			s.log.Warn("a direct write of unknown outcome: its refs rows' commit failed too (refs-diff / refs-sync)", "org", org, "err", c.err)
		}
		s.log.Warn("organization's database connection lost during a write: its outcome unknown, the refs rows of its keys kept (refs-diff finds those of rows not written)", "org", org, "err", err)
		finish := []pgproto3.BackendMessage{errUnknown(org, err)}
		for _, m := range finish {
			s.toClient(m)
		}
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
		return true
	}
	finish := func(msgs []pgproto3.BackendMessage) {
		for _, m := range msgs {
			s.toClient(m)
		}
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
	}
	orgEnd := func(q string) error {
		_, _, f, err := s.orgWrite(dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: q}}, false, true)
		if err == nil && f != nil {
			return fmt.Errorf("%s", f.Message)
		}
		return err
	}
	if failed != nil {
		if own {
			_ = orgEnd("ROLLBACK")
		}
		rollback()
		finish(answer)
		return true
	}
	if w.sel {
		var inserted []string
		if answer, inserted, err = w.split(answer, batch == nil, upsertBinary(batch), kind); err != nil {
			_ = orgEnd("ROLLBACK")
			rollback()
			finish([]pgproto3.BackendMessage{&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()}})
			return true
		}
		if len(inserted) > 0 {
			c := s.cloudRun(w.refsAdd(inserted, kind, typ, refs, org))
			if c.err != nil || !othersNone(c.rows) {
				_ = orgEnd("ROLLBACK")
				rollback()
				e := c.resp
				if e == nil {
					e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint \"" + t.Refs + "_pkey\": a key of the INSERT belongs to another organization"}
				}
				finish([]pgproto3.BackendMessage{e})
				return true
			}
		}
	}
	if w.upsert {
		var inserted []string
		if answer, inserted, err = w.split(answer, batch == nil, upsertBinary(batch), kind); err != nil {
			rollback()
			s.log.Warn("a direct upsert's answer could not be read: the cloud rolled back its refs; the organization's rows are written (refs-diff finds them)", "org", org, "err", err)
			finish([]pgproto3.BackendMessage{&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()}})
			return true
		}
		// the keys the cloud gave (or the refs rows it added for keys named) of rows that were
		// updated or skipped: no row has them
		given := keys
		if w.keysGiven() || w.keyCols != nil {
			given = added
		}
		if unused := without(given, inserted); len(unused) > 0 {
			if c := s.cloudRun(w.refsDelete(refs, unused, kind, typ)); c.err != nil {
				s.log.Warn("a direct upsert's unused refs rows were not deleted: refs-diff finds them", "org", org, "table", t.Name, "err", c.err)
			}
		}
	}
	if w.del && len(deleted) > 0 {
		// the cloud's FKs onto __refs act here, before either side commits
		if c := s.cloudRun(w.refsDelete(refs, deleted, kind, typ)); c.err != nil {
			_ = orgEnd("ROLLBACK")
			rollback()
			if c.resp != nil {
				finish([]pgproto3.BackendMessage{c.resp})
			} else {
				finish([]pgproto3.BackendMessage{&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + c.err.Error()}})
			}
			return true
		}
	}
	if own {
		_, _, f, err := s.orgWrite(dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: "COMMIT"}}, false, true)
		switch {
		case err == nil && f != nil: // the server answered: rolled back there
			rollback()
			finish([]pgproto3.BackendMessage{f})
			return true
		case err != nil: // the outcome unknown (review PR-1): __refs kept a superset of the rows
			s.dropDirect(dc)
			if w.del {
				rollback() // the deleted rows' refs rows come back
			} else if c := s.cloudRun("COMMIT"); c.err != nil {
				s.log.Warn("a direct write of unknown outcome: its refs rows' commit failed too (refs-diff / refs-sync)", "org", org, "err", c.err)
			}
			finish([]pgproto3.BackendMessage{errUnknown(org, err)})
			return true
		}
	}
	if c := s.cloudRun("COMMIT"); c.err != nil {
		s.log.Warn("a direct write committed in the organization's database, its refs did not in the cloud: refs-diff / refs-sync repair them", "org", org, "table", t.Name, "err", c.err)
		finish([]pgproto3.BackendMessage{&pgproto3.ErrorResponse{Severity: "ERROR", Code: "40003",
			Message: "lokadb: organization " + org + "'s rows are written, the cloud's commit of their refs failed: " + c.err.Error()}})
		return true
	}
	finish(answer)
	return true
}

// givenRefs: the cloud's statement for the refs rows of the keys an INSERT names (lits, as
// SQL; cols: the refs table's key columns, directWrite.refsCols): each added for org unless there; it answers how many of them map to another
// organization, then, for an upsert, the keys it added (one row each), whose rows the upsert
// may not insert. A key that maps to this organization already (a crash between the two
// commits, or an upsert's row there) is left to the organization's primary key. The
// organization goes in as a literal of its column's type (orgLit: a uuid quoted, block 18).
func givenRefs(lits []string, cols, refs, org string, upsert bool) string {
	org = orgLit(org)
	q := "WITH lokadb_v(" + cols + ") AS (VALUES (" + strings.Join(lits, "), (") + ")), lokadb_r AS (INSERT INTO " + refs + " (" + cols + ", tenant) SELECT " + cols + ", " + org +
		" FROM lokadb_v ON CONFLICT (" + cols + ") DO NOTHING"
	other := "SELECT count(*)::text FROM " + refs + " x JOIN lokadb_v USING (" + cols + ") WHERE x.tenant IS DISTINCT FROM " + org
	if !upsert {
		return q + ") " + other
	}
	// the count first (its own row), then the keys added
	return q + " RETURNING id) SELECT k FROM (SELECT 0 AS o, (" + other + ") AS k UNION ALL SELECT 1, id::text FROM lokadb_r) lokadb_x ORDER BY o"
}

// binds: the batch for the organization's database: each Bind of the client's statement aimed
// at the unnamed one, its result formats fit for the columns the proxy adds (a DELETE's keys as
// text, its rows kept from the client; an upsert's two columns as text when the client gave a
// format per column).
func (w *directWrite) binds(batch []pgproto3.FrontendMessage) []pgproto3.FrontendMessage {
	var out []pgproto3.FrontendMessage
	for _, m := range batch {
		if b, ok := m.(*pgproto3.Bind); ok {
			c := *b
			c.PreparedStatement = ""
			switch {
			case w.del:
				c.ResultFormatCodes = nil // the keys, as text; the client's DELETE returns nothing
			case w.added() > 0 && len(c.ResultFormatCodes) > 1:
				c.ResultFormatCodes = append(append([]int16{}, c.ResultFormatCodes...), make([]int16, w.added())...)
			}
			m = &c
		}
		out = append(out, m)
	}
	return out
}

// upsertBinary: the client's Bind asked one binary format for every column: the columns the
// proxy adds (directWrite.added) come in binary too.
func upsertBinary(batch []pgproto3.FrontendMessage) bool {
	for _, m := range batch {
		if b, ok := m.(*pgproto3.Bind); ok {
			return len(b.ResultFormatCodes) == 1 && b.ResultFormatCodes[0] == 1
		}
	}
	return false
}

// split: the organization's answer to an upsert or an INSERT … SELECT without the columns the
// proxy added (directWrite.added; with no RETURNING of the client's, its rows and their
// description: NoData for the client's Describe in the extended protocol), and the keys of the
// rows inserted. bin: the added columns are binary; kind: the key's (keys.go).
func (w *directWrite) split(answer []pgproto3.BackendMessage, simple, bin bool, kind string) ([]pgproto3.BackendMessage, []string, error) {
	added := w.added()
	var out []pgproto3.BackendMessage
	var inserted []string
	for _, m := range answer {
		switch m := m.(type) {
		case *pgproto3.RowDescription:
			if len(m.Fields) < added {
				return nil, nil, fmt.Errorf("the statement returned %d columns", len(m.Fields))
			}
			if !w.returning {
				if !simple {
					out = append(out, &pgproto3.NoData{})
				}
				continue
			}
			c := *m
			c.Fields = m.Fields[:len(m.Fields)-added]
			out = append(out, &c)
			continue
		case *pgproto3.DataRow:
			n := len(m.Values)
			if n < added {
				return nil, nil, fmt.Errorf("the statement returned a row of %d columns", n)
			}
			ins := true // an INSERT … SELECT's every row
			var flag []byte
			if w.upsert {
				flag = m.Values[n-1]
				ins = string(flag) == "t"
				if bin {
					ins = len(flag) == 1 && flag[0] != 0
				}
			}
			k, err := w.splitKey(m.Values[n-added:], bin, kind)
			if err != nil {
				return nil, nil, err
			}
			if ins {
				inserted = append(inserted, k)
			}
			if w.returning {
				out = append(out, &pgproto3.DataRow{Values: m.Values[:n-added]})
			}
			continue
		}
		out = append(out, m)
	}
	return out, inserted, nil
}

// without: the elements of a not in b.
func without(a, b []string) []string {
	in := map[string]bool{}
	for _, x := range b {
		in[x] = true
	}
	var out []string
	for _, x := range a {
		if !in[x] {
			out = append(out, x)
		}
	}
	return out
}

// stmtSQL: the text a batch runs, for the route log ("" for none).
func stmtSQL(batch []pgproto3.FrontendMessage, s *session) string {
	for _, m := range batch {
		if b, ok := m.(*pgproto3.Bind); ok {
			if st := s.stmts[b.PreparedStatement]; st != nil {
				return st.sql
			}
		}
	}
	return ""
}

// orgWrite sends msgs on dc and reads the answers up to the last ReadyForQuery (two after a
// leading BEGIN of a DELETE). It returns what the client is to get of the statement's part, the
// keys a DELETE … RETURNING gave (its rows and row description kept from the client, which sent
// a DELETE without RETURNING), the statement's error, if any, and a broken connection. simple:
// the client sent a simple query, whose answer has no NoData: the row description is dropped
// there; in the extended protocol it answers the client's Describe of the portal, as NoData
// (review of block 9 of 8b, 1).
func (s *session) orgWrite(dc *directConn, msgs []pgproto3.FrontendMessage, del, simple bool) (answer []pgproto3.BackendMessage, keys []string, failed *pgproto3.ErrorResponse, err error) {
	// a DELETE of its own transaction starts with a BEGIN (writeDirect); one in a direct
	// transaction does not (writeInTx): the BEGIN is told by the message, not guessed (DP18 hung
	// when a batch's DELETE in a transaction lost its answer as the BEGIN's)
	q, _ := msgs[0].(*pgproto3.Query)
	begun := len(msgs) > 1 && q != nil && q.String == "BEGIN"
	if begun { // its error skips the statement (withBegin): never a write committed on its own
		msgs = withBegin("BEGIN", msgs[1:])
	}
	if b := s.beginFor(dc); b != nil { // a direct transaction's BEGIN with its first statement there (item 9)
		msgs, begun = withBegin(b.String, msgs), true
	}
	for _, m := range msgs {
		dc.fe.Send(m)
	}
	s.directBusy.Store(dc)
	defer s.directBusy.Store(nil)
	if err := dc.fe.Flush(); err != nil {
		s.dropDirect(dc)
		return nil, nil, nil, err
	}
	rfqs := 0
	for _, m := range msgs {
		if _, ok := m.(*pgproto3.Query); ok {
			rfqs++
		} else if _, ok := m.(*pgproto3.Sync); ok {
			rfqs++
		}
	}
	for done := 0; done < rfqs; {
		m, err := dc.fe.Receive()
		if err != nil {
			s.dropDirect(dc)
			return nil, nil, nil, err
		}
		if begun { // the BEGIN's own answer, up to its CommandComplete (withBegin)
			switch m := m.(type) {
			case *pgproto3.ErrorResponse: // the statement is skipped, its answer is the RFQ alone
				r := *m
				failed, begun = &r, false
				answer = append(answer, &r)
			case *pgproto3.ReadyForQuery:
				done++
				begun = false
			default:
				begun = !beginDone(m)
			}
			continue
		}
		switch m := m.(type) {
		case *pgproto3.ReadyForQuery:
			done++
			continue
		case *pgproto3.ParseComplete, *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
			continue
		case *pgproto3.ErrorResponse:
			r := *m
			if failed == nil {
				failed = &r
			}
			dc.markEvict(m.Code) // the pool (direct_pool.go)
			answer = append(answer, &r)
			continue
		case *pgproto3.DataRow:
			if del {
				switch len(m.Values) {
				case 0:
				case 1:
					keys = append(keys, string(m.Values[0]))
				default: // a composite key's columns (directWrite.rewrite)
					vals := make([]string, len(m.Values))
					for i, v := range m.Values {
						vals[i] = string(v)
					}
					keys = append(keys, tupleKey(vals))
				}
				continue
			}
		case *pgproto3.RowDescription:
			if del {
				if !simple {
					answer = append(answer, &pgproto3.NoData{})
				}
				continue
			}
		}
		answer = append(answer, copyBackend(m))
	}
	dc.used = time.Now()
	return answer, keys, failed, nil
}

// probeDirect: a connection idle past directIdleProbe answers a Sync first; a broken one is
// replaced (directConnFor) before anything of the write is sent.
func (s *session) probeDirect(dc *directConn) error {
	if time.Since(dc.used) < directIdleProbe {
		return nil
	}
	_ = dc.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer dc.conn.SetDeadline(time.Time{})
	dc.fe.Send(&pgproto3.Sync{})
	if err := dc.fe.Flush(); err != nil {
		return err
	}
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return err
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			dc.used = time.Now()
			return nil
		}
	}
}

// copyBackend: a message the frontend reused buffers for, copied for the client.
func copyBackend(m pgproto3.BackendMessage) pgproto3.BackendMessage {
	switch m := m.(type) {
	case *pgproto3.DataRow:
		vals := make([][]byte, len(m.Values))
		for i, v := range m.Values {
			if v != nil {
				vals[i] = append([]byte{}, v...)
			}
		}
		return &pgproto3.DataRow{Values: vals}
	case *pgproto3.CommandComplete:
		return &pgproto3.CommandComplete{CommandTag: append([]byte{}, m.CommandTag...)}
	case *pgproto3.RowDescription:
		c := *m
		c.Fields = append([]pgproto3.FieldDescription{}, m.Fields...)
		for i := range c.Fields {
			c.Fields[i].Name = append([]byte{}, m.Fields[i].Name...)
		}
		return &c
	case *pgproto3.NoticeResponse:
		c := *m
		return &c
	case *pgproto3.BindComplete:
		return &pgproto3.BindComplete{}
	case *pgproto3.CloseComplete:
		return &pgproto3.CloseComplete{}
	case *pgproto3.NoData:
		return &pgproto3.NoData{}
	case *pgproto3.EmptyQueryResponse:
		return &pgproto3.EmptyQueryResponse{}
	}
	return m
}
