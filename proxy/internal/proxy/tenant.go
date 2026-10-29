package proxy

import (
	"fmt"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// The cooperative mode (tasks/cooperative-mode.md): the application names the organization
// itself, SET LOCAL lokadb.tenant = 42, and the proxy takes it instead of guessing. This file
// recognizes the statements that set it and keeps its value as Postgres would; session.go
// applies them once the cloud ran them (stage 5).

// TenantAll: lokadb.tenant = '*', a statement across organizations declared as such.
const TenantAll = "*"

// TenantSetting is the setting the application names the organization by.
const TenantSetting = "lokadb.tenant"

// TenantSet is one statement that changes lokadb.tenant.
type TenantSet struct {
	Value string // an organization id, TenantAll, or "" (RESET)
	Param int    // > 0: the value is this parameter of the Bind (set_config('lokadb.tenant', $1, …))
	Local bool   // SET LOCAL / set_config(…, true): until the end of the transaction
	All   bool   // RESET ALL, DISCARD ALL: every setting, lokadb.tenant too
	kind  string // the database's kind of organizations (TenantConfig.kind), for the value of a parameter
}

// tenantSet recognizes a statement that sets lokadb.tenant (or setting, the name the proxy
// config gives the application's own variable). ok: it is one; err: it is one the proxy
// refuses (a value that is not an organization, set_config inside another query).
func tenantSet(n *pg.Node, setting, kind string) (ts TenantSet, ok bool, err error) {
	if setting == "" {
		setting = TenantSetting
	}
	if vs := n.GetVariableSetStmt(); vs != nil {
		switch vs.Kind {
		case pg.VariableSetKind_VAR_RESET_ALL:
			return TenantSet{All: true}, true, nil
		case pg.VariableSetKind_VAR_RESET:
			return TenantSet{}, strings.EqualFold(vs.Name, setting), nil
		case pg.VariableSetKind_VAR_SET_DEFAULT:
			return TenantSet{Local: vs.IsLocal}, strings.EqualFold(vs.Name, setting), nil
		case pg.VariableSetKind_VAR_SET_VALUE:
			if !strings.EqualFold(vs.Name, setting) {
				return TenantSet{}, false, nil
			}
			ts = TenantSet{Local: vs.IsLocal, kind: kind}
			if len(vs.Args) != 1 {
				return ts, true, fmt.Errorf("%s takes one value: an organization id or '*'", setting)
			}
			v, isValue := value(vs.Args[0])
			if !isValue || v.Param > 0 {
				return ts, true, fmt.Errorf("%s must be an organization id or '*'", setting)
			}
			ts.Value, err = tenantValue(v.Const, setting, kind)
			return ts, true, err
		}
		return TenantSet{}, false, nil
	}
	if d := n.GetDiscardStmt(); d != nil && d.Target == pg.DiscardMode_DISCARD_ALL {
		return TenantSet{All: true}, true, nil
	}
	// SELECT set_config('lokadb.tenant', <value>, <is_local>) as the whole statement
	found := false
	Walk(n, func(m proto.Message) {
		if fc, isCall := m.(*pg.FuncCall); isCall && setConfigOf(fc, setting) {
			found = true
		}
	})
	if !found {
		return TenantSet{}, false, nil
	}
	sel := n.GetSelectStmt()
	if sel == nil || len(sel.TargetList) != 1 || len(sel.FromClause) > 0 || sel.WhereClause != nil || sel.WithClause != nil {
		return TenantSet{}, true, fmt.Errorf("set_config('%s', …) must be a statement of its own (SELECT set_config(…)), not part of another query", setting)
	}
	fc := sel.TargetList[0].GetResTarget().GetVal().GetFuncCall()
	if fc == nil || !setConfigOf(fc, setting) || len(fc.Args) != 3 {
		return TenantSet{}, true, fmt.Errorf("set_config('%s', …) must be a statement of its own (SELECT set_config(…)), not part of another query", setting)
	}
	v, isValue := value(fc.Args[1])
	local := fc.Args[2].GetAConst()
	if !isValue || local == nil || local.GetBoolval() == nil {
		return TenantSet{}, true, fmt.Errorf("set_config('%s', value, is_local): the value must be a constant or a parameter, is_local true or false", setting)
	}
	ts = TenantSet{Local: local.GetBoolval().Boolval, Param: v.Param, kind: kind}
	if v.Param == 0 {
		ts.Value, err = tenantValue(v.Const, setting, kind)
	}
	return ts, true, err
}

func setConfigOf(fc *pg.FuncCall, setting string) bool {
	name := fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()
	if name != "set_config" || len(fc.Funcname) > 1 && fc.Funcname[0].GetString_().GetSval() != "pg_catalog" || len(fc.Args) == 0 {
		return false
	}
	v, ok := value(fc.Args[0])
	return ok && v.Param == 0 && strings.EqualFold(v.Const, setting)
}

// tenantValue checks a value of lokadb.tenant: an organization id (an integer, or for
// organizations named by their value a uuid or a string, block 18), '*' or "" (unset). The id
// in canonical form. kind: the database's kind of organizations (Meta.TenantKind; "" where it
// is not known, a connection's startup parameters, which then name an integer or a uuid
// only): for string organizations the value is the string without the blanks of character(n),
// as it is (not a number or a uuid read as such); one that a minus starts is lokadb's own
// (-1 and -2): not an organization.
func tenantValue(s, setting, kind string) (string, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "" || s == TenantAll:
		return s, nil
	case kind == kindText:
		if strings.HasPrefix(s, "-") || strings.ContainsRune(s, 0) {
			return "", fmt.Errorf("%s must be an organization id or '*', not %q", setting, s)
		}
		return strings.TrimRight(s, " "), nil
	case intRe.MatchString(s):
		return canonicalInt(s), nil
	}
	if u, ok := canonicalUUID(s); ok {
		return u, nil
	}
	return "", fmt.Errorf("%s must be an organization id or '*', not %q", setting, s)
}

// tenantState is lokadb.tenant as Postgres keeps it: a session value, and inside a
// transaction the value in force with what each savepoint saw, so ROLLBACK TO and ROLLBACK
// undo what came after them (SET LOCAL and a plain SET alike), COMMIT keeps a plain SET and
// drops a SET LOCAL.
type tenantState struct {
	session string // outside a transaction (and what COMMIT keeps)
	inTx    bool
	cur     string // in force in the transaction
	sessTx  string // the session value the transaction will commit
	sps     []tenantSavepoint
}

type tenantSavepoint struct{ name, cur, sess string }

// Value is what a statement runs under now ("" = not named).
func (t *tenantState) Value() string {
	if t.inTx {
		return t.cur
	}
	return t.session
}

func (t *tenantState) begin() {
	if !t.inTx {
		t.inTx, t.cur, t.sessTx, t.sps = true, t.session, t.session, nil
	}
}

// set applies one TenantSet whose value is known (a parameter resolved by the caller).
// SET LOCAL outside a transaction block lasts only until its own implicit transaction
// ends: the rest of its query string, which the caller handles (Value is not changed).
func (t *tenantState) set(ts TenantSet) {
	if ts.All {
		ts = TenantSet{}
	}
	if !t.inTx {
		if !ts.Local {
			t.session = ts.Value
		}
		return
	}
	t.cur = ts.Value
	if !ts.Local {
		t.sessTx = ts.Value
	}
}

func (t *tenantState) savepoint(name string) {
	t.begin()
	t.sps = append(t.sps, tenantSavepoint{name: name, cur: t.cur, sess: t.sessTx})
}

func (t *tenantState) find(name string) int {
	for i := len(t.sps) - 1; i >= 0; i-- {
		if strings.EqualFold(t.sps[i].name, name) {
			return i
		}
	}
	return -1
}

// release drops the savepoint and the ones after it; the values stay.
func (t *tenantState) release(name string) {
	if i := t.find(name); i >= 0 {
		t.sps = t.sps[:i]
	}
}

// rollbackTo restores the values the savepoint saw; the savepoint itself stays.
func (t *tenantState) rollbackTo(name string) {
	if i := t.find(name); i >= 0 {
		t.cur, t.sessTx = t.sps[i].cur, t.sps[i].sess
		t.sps = t.sps[:i+1]
	}
}

func (t *tenantState) commit() {
	if t.inTx {
		t.session = t.sessTx
	}
	t.inTx, t.sps = false, nil
}

func (t *tenantState) rollback() { t.inTx, t.sps = false, nil }

// apply runs the events of one string the cloud ran, in order (Analysis.Events with the
// tenant events 'N' among them, their TenantSet in sets, one per 'N').
func (t *tenantState) apply(evs []TxEvent, sets []TenantSet) {
	n := 0
	for _, e := range evs {
		switch e.Kind {
		case 'B':
			t.begin()
		case 'S':
			t.savepoint(e.SQL)
		case 'R':
			t.release(e.SQL)
		case 'T':
			t.rollbackTo(e.SQL)
		case 'X':
			t.rollback()
		case 'C':
			t.commit()
		case 'N':
			if n < len(sets) {
				t.set(sets[n])
				n++
			}
		}
	}
}
