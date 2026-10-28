package proxy

import (
	"fmt"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// declaresHold: sql declares a cursor WITH HOLD (it is filled at the COMMIT, see session.cursors).
func declaresHold(sql string) bool {
	if !hasFold(sql, "hold") {
		return false
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if d := raw.Stmt.GetDeclareCursorStmt(); d != nil && d.Options&cursorOptHold != 0 {
			return true
		}
	}
	return false
}

// cursorOptHold is CURSOR_OPT_HOLD of Postgres (parsenodes.h).
const cursorOptHold = 0x0020

// cursorAt: a cursor over LokaDB tables declared in the open transaction.
type cursorAt struct {
	org  string // the lokadb.org it was declared under
	hold bool   // WITH HOLD: filled at the COMMIT
	// any: every organization was allowed outright when it was declared (openOrg gave "");
	// only then may it share a FETCH string or a COMMIT with a cursor of another organization
	any bool
}

// Block 9, item 7. A cursor's query runs at each FETCH (and a cursor WITH HOLD is filled at the
// COMMIT), under the lokadb.org then in force; the policy reads all organizations when it is
// empty. A cursor whose own query does not keep to one organization (the cooperative mode:
// lokadb.tenant names it, the query need not) read every organization's rows at FETCH. The
// proxy now sets lokadb.org to the organization each cursor was declared under before the
// FETCH or MOVE that reads it and before the COMMIT that fills it.

// declared: the cursors sql declares, by name.
func declared(sql string) map[string]bool {
	if !hasFold(sql, "declare") {
		return nil
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil
	}
	var out map[string]bool
	for _, raw := range tree.Stmts {
		if d := raw.Stmt.GetDeclareCursorStmt(); d != nil {
			if out == nil {
				out = map[string]bool{}
			}
			out[d.Portalname] = d.Options&cursorOptHold != 0
		}
	}
	return out
}

// fetched: the cursors sql fetches from or moves; nil for a string that does anything else.
func fetched(sql string) []string {
	t := strings.TrimSpace(sql)
	if !hasPrefixFold(t, "fetch") && !hasPrefixFold(t, "move") {
		return nil
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil
	}
	var out []string
	for _, raw := range tree.Stmts {
		f := raw.Stmt.GetFetchStmt()
		if f == nil {
			return nil
		}
		out = append(out, f.Portalname)
	}
	return out
}

// noteCursors: the cursors sql declares, under org. Cursors WITH HOLD of two organizations in
// one transaction are refused (the COMMIT fills them all under one lokadb.org: one of them would
// get the other's rows, or every organization's), unless every organization is allowed outright
// (second review of routing: before, only the cooperative mode refused them, and the COMMIT
// filled them under "").
func (s *session) noteCursors(sql, org, named string) error {
	anyOrg := s.openOrg(sql, named) == ""
	for name, hold := range declared(sql) {
		if hold && !anyOrg {
			for other, c := range s.cursors {
				if c.hold && c.org != org && other != name {
					return fmt.Errorf("cursors WITH HOLD of two organizations (%s and %s) in one transaction: the COMMIT fills them under one; declare them in transactions of their own", c.org, org)
				}
			}
		}
		if s.cursors == nil {
			s.cursors = map[string]cursorAt{}
		}
		s.cursors[name] = cursorAt{org: org, hold: hold, any: anyOrg}
	}
	return nil
}

// openOrg: the lokadb.org a statement runs under that the analyzer does not see touch LokaDB
// tables (review of routing R1, coordinator's decision of 25.10: closed by default). Empty, the
// policy shows every organization's rows: a statement the analyzer misreads (SQL EXECUTE, a
// function in a schema of its own, a view by a bare name…) read them all, the fanout policy
// never asked. Now: the organization the application named (the cooperative mode: its rows,
// as for its statements on LokaDB tables); every organization only where a fanout is allowed
// outright (lokadb.tenant = '*' of a role of all_roles, the policy allow or log, the statement
// on the allow list); otherwise -1, no LokaDB row.
func (s *session) openOrg(sql, named string) string {
	switch {
	case named != "" && named != TenantAll:
		return named
	case named == TenantAll && s.allRole():
		return ""
	case s.fanout == nil:
	case s.fanout.Mode == FanoutAllow || s.fanout.Mode == FanoutLog:
		return ""
	case len(s.fanout.allow) > 0 && s.fanout.listed(sql):
		return ""
	}
	return "-1"
}

// fetchOrg: the lokadb.org a FETCH or MOVE string runs under: the organization its cursors were
// declared under. ok false: sql reads no cursor of the transaction. Cursors of several
// organizations in one string run under one lokadb.org: "" (every organization) only where it is
// allowed outright (openOrg), otherwise err (second review of routing: they ran under "" in
// any mode, the cooperative one included, and each cursor read every organization's rows).
func (s *session) fetchOrg(sql, named string) (org string, ok bool, err error) {
	if len(s.cursors) == 0 {
		return "", false, nil
	}
	mixed := false
	for i, name := range fetched(sql) {
		c, known := s.cursors[name]
		switch {
		case !known:
			return "", false, nil
		case i == 0:
			org, ok = c.org, true
		case c.org != org:
			mixed = true
		}
	}
	if mixed {
		if s.openOrg(sql, named) != "" {
			return "", false, fmt.Errorf("FETCH or MOVE of cursors of two organizations in one query string: they run under one; send each in a query of its own")
		}
		org = ""
	}
	return org, ok, nil
}

// holdOrg: the lokadb.org of the COMMIT that fills the transaction's cursors WITH HOLD: their
// organization. ok false: none was declared. Of several (noteCursors lets them only where every
// organization is allowed outright): "", as allowed; -1, no LokaDB row, should they ever come
// otherwise: never every organization by default.
func (s *session) holdOrg() (org string, ok bool) {
	mixed, anyOrg := false, false
	for _, c := range s.cursors {
		switch {
		case !c.hold:
			continue
		case !ok:
			org, ok = c.org, true
		case c.org != org:
			mixed = true
		}
		anyOrg = anyOrg || c.any
	}
	switch {
	case !mixed:
	case anyOrg:
		org = ""
	default:
		org = "-1"
	}
	return org, ok
}

// changesOrg: the statement may change lokadb.org itself, other than by SET / RESET (which
// analyze sees as VariableSetStmt): a set_config call whose name is 'lokadb.…' or not a
// constant, an UPDATE of pg_settings (it calls set_config), a DO block whose body names
// lokadb. The proxy sets lokadb.org right before a statement on LokaDB tables; a string that sets
// it again would run that statement under the application's organization (see session.go).
// Functions of the database that set it are out of reach (S7).
func changesOrg(n *pg.Node) bool {
	if u := n.GetUpdateStmt(); u != nil && u.Relation != nil && u.Relation.Relname == "pg_settings" &&
		(u.Relation.Schemaname == "" || u.Relation.Schemaname == "pg_catalog") {
		return true
	}
	if d := n.GetDoStmt(); d != nil {
		for _, a := range d.Args {
			if de := a.GetDefElem(); de != nil && de.Defname == "as" &&
				strings.Contains(strings.ToLower(de.Arg.GetString_().GetSval()), "lokadb.") {
				return true
			}
		}
	}
	return setsConfig(n, "lokadb.")
}

// setsConfig: n calls pg_catalog.set_config with a name that starts with prefix, or with a
// name that is not a constant ($1, an expression: it may be that one).
func setsConfig(n *pg.Node, prefix string) bool {
	named, unnamed := configNames(n, prefix)
	return named || unnamed
}

// configNames: n calls pg_catalog.set_config with a constant name that starts with prefix
// (named), or with a name that is not a constant (unnamed).
func configNames(n *pg.Node, prefix string) (named, unnamed bool) {
	walk(n.ProtoReflect(), func(m proto.Message) {
		fc, ok := m.(*pg.FuncCall)
		if !ok || named || len(fc.Funcname) == 0 {
			return
		}
		name := fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()
		if len(fc.Funcname) > 1 && fc.Funcname[0].GetString_().GetSval() != "pg_catalog" {
			return // another schema's function of that name
		}
		if !strings.EqualFold(name, "set_config") || len(fc.Args) == 0 {
			return
		}
		arg := fc.Args[0]
		for arg.GetTypeCast() != nil { // 'lokadb.org'::text
			arg = arg.GetTypeCast().Arg
		}
		c := arg.GetAConst()
		if c == nil || c.GetSval() == nil { // $1, an expression: it may be that one
			unnamed = true
			return
		}
		// lokadb.tenant is the application's to set (the cooperative mode, tenant.go); every
		// other lokadb.… setting is the proxy's. The walk goes on: another set_config in the
		// statement may still name one.
		if name := strings.ToLower(strings.TrimSpace(c.GetSval().Sval)); strings.HasPrefix(name, prefix) && name != TenantSetting {
			named = true
		}
	})
	return named, unnamed
}

// fanoutSetMsg: the refusal of a statement that sets lokadb.fanout.
const fanoutSetMsg = "lokadb: lokadb.fanout is the proxy's fanout policy, which lokadb's FK triggers follow: the application cannot set it (SET, set_config, UPDATE pg_settings, a DO block)"

// setsFanout: n may set lokadb.fanout, the fanout policy the proxy gives the database (the
// triggers of FKs onto cloud tables follow it, admin/fkcloud.go): SET or RESET of it, a
// set_config call naming it or a name not a constant, an UPDATE of pg_settings, a DO block
// whose body names it. Unlike lokadb.org it is refused in any statement: no statement of the
// application needs it. RESET ALL and DISCARD ALL give it back the proxy's value (a startup
// parameter is the session's default). Functions of the database that set it are out of reach.
//
// byParam: the only way it may is a set_config whose name is not a constant. In
// pg_stat_statements every constant is a parameter ($1), so check cannot tell
// set_config('lokadb.tenant', …) or the proxy's own set_config('lokadb.org', $1, false) from
// such a call, and does not count it (Analysis.FanoutByParam); the proxy, which sees the
// statement itself, refuses it.
func setsFanout(n *pg.Node) (sets, byParam bool) {
	if vs := n.GetVariableSetStmt(); vs != nil {
		return strings.EqualFold(vs.Name, "lokadb.fanout"), false
	}
	if u := n.GetUpdateStmt(); u != nil && u.Relation != nil && u.Relation.Relname == "pg_settings" &&
		(u.Relation.Schemaname == "" || u.Relation.Schemaname == "pg_catalog") {
		return true, false
	}
	if d := n.GetDoStmt(); d != nil {
		for _, a := range d.Args {
			if de := a.GetDefElem(); de != nil && de.Defname == "as" &&
				strings.Contains(strings.ToLower(de.Arg.GetString_().GetSval()), "lokadb.fanout") {
				return true, false
			}
		}
	}
	named, unnamed := configNames(n, "lokadb.fanout")
	return named || unnamed, !named && unnamed
}

// callsOwn: n calls something that may set lokadb.org where the proxy does not see it: a
// function that is not one of the server's plain ones (directFuncs), schema-qualified other
// than pg_catalog's, a procedure (CALL), a DO block (Analysis.MayResetOrg).
func callsOwn(n *pg.Node) bool {
	if n.GetCallStmt() != nil || n.GetDoStmt() != nil {
		return true
	}
	own := false
	walk(n.ProtoReflect(), func(m proto.Message) {
		fc, ok := m.(*pg.FuncCall)
		if !ok || own || len(fc.Funcname) == 0 {
			return
		}
		if len(fc.Funcname) > 1 && fc.Funcname[0].GetString_().GetSval() != "pg_catalog" {
			own = true
			return
		}
		own = !directFuncs[strings.ToLower(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval())]
	})
	return own
}
