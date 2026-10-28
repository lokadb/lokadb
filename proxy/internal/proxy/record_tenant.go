package proxy

import (
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// The cooperative mode in the recording (third review of check, H3 and M2): the statements
// that set lokadb.tenant are kept apart from the rest. pg.Normalize made both
// SET LOCAL lokadb.tenant = '42' and = '*' into "= $1", and their fingerprint is one, so check
// took a transaction across organizations ('*') for one that names an organization; and
// set_config('lokadb.tenant', …) became set_config($1, $2, $3), which check could not tell from
// any other setting. Now the setting's name stays in the recorded text, the value '*' stays as
// itself, an organization's value becomes the mark '<organization>' (not its value), and each
// has a fingerprint of its own. A recorded "= $1" (a recording of before 26.10: '*' or an
// organization, which is not known) stays as it was, a form of its own: check takes it for
// neither (block 11's format, third review of check).

// recordStar, recordTenant, recordUnknown: what the recording appends to the fingerprint of a
// statement that sets lokadb.tenant to '*' (across organizations), to an organization's value,
// and to a value an older recording does not keep ($1).
const (
	recordStar    = ":tenant*"
	recordTenant  = ":tenant"
	recordUnknown = ":tenant?"
	// recordOrg: the recorded value of an organization's (its value is data, not kept)
	recordOrg = "'<organization>'"
)

// tenantRecordKey: k for a statement that sets lokadb.tenant, its text and fingerprint as the
// recording keeps them (ok: it is one). params: a set_config's arguments are parameters (the
// value comes with the Bind: tenantBindKey).
func tenantRecordKey(stmt *pg.Node, k stmtKey) (out stmtKey, ok bool) {
	if vs := stmt.GetVariableSetStmt(); vs != nil {
		if !strings.EqualFold(vs.Name, TenantSetting) || vs.Kind != pg.VariableSetKind_VAR_SET_VALUE || len(vs.Args) != 1 {
			return k, false
		}
		set := "SET "
		if vs.IsLocal {
			set = "SET LOCAL "
		}
		switch {
		case vs.Args[0].GetParamRef() != nil: // a recording of before 26.10: which one, not known
			k.norm, k.fp = set+TenantSetting+" = $1", k.fp+recordUnknown
		case constString(vs.Args[0]) == TenantAll:
			k.norm, k.fp = set+TenantSetting+" = '*'", k.fp+recordStar
		default:
			k.norm, k.fp = set+TenantSetting+" = "+recordOrg, k.fp+recordTenant
		}
		return k, true
	}
	sel := stmt.GetSelectStmt()
	if sel == nil || len(sel.TargetList) != 1 || len(sel.FromClause) > 0 {
		return k, false
	}
	fc := sel.TargetList[0].GetResTarget().GetVal().GetFuncCall()
	if fc == nil || len(fc.Args) != 3 || len(fc.Funcname) == 0 || !strings.EqualFold(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval(), "set_config") {
		return k, false
	}
	name := constString(fc.Args[0])
	if fc.Args[0].GetParamRef() != nil { // set_config($1, $2, $3): which setting, the Bind says
		k.tenantParams = true
		return k, false
	}
	if !strings.EqualFold(name, TenantSetting) {
		return k, false
	}
	if fc.Args[1].GetParamRef() != nil { // set_config('lokadb.tenant', $1, $2): a parameter's value, not known here
		k.norm, k.fp = "SELECT set_config('"+TenantSetting+"', $1, $2)", k.fp+recordUnknown
		return k, true
	}
	return tenantConfigKey(k, constString(fc.Args[1]) == TenantAll), true
}

// tenantConfigKey: the recorded form of set_config('lokadb.tenant', value, local).
func tenantConfigKey(k stmtKey, star bool) stmtKey {
	if star {
		k.norm, k.fp = "SELECT set_config('"+TenantSetting+"', '*', $1)", k.fp+recordStar
	} else {
		k.norm, k.fp = "SELECT set_config('"+TenantSetting+"', "+recordOrg+", $1)", k.fp+recordTenant
	}
	k.tenantParams = false
	return k
}

// tenantBindKey: the keys of a Bind of a set_config($1, $2, $3), with its parameters: the
// recorded form of set_config('lokadb.tenant', …) when the first one names it (text format).
func tenantBindKey(keys []stmtKey, params [][]byte) []stmtKey {
	if len(keys) != 1 || !keys[0].tenantParams || len(params) < 2 || !strings.EqualFold(string(params[0]), TenantSetting) {
		return keys
	}
	return []stmtKey{tenantConfigKey(keys[0], string(params[1]) == TenantAll)}
}

// constString: the value of a string or integer constant ("" for anything else).
func constString(n *pg.Node) string {
	if tc := n.GetTypeCast(); tc != nil {
		n = tc.Arg
	}
	c := n.GetAConst()
	if c == nil {
		return ""
	}
	if s := c.GetSval(); s != nil {
		return s.Sval
	}
	if i := c.GetIval(); i != nil {
		return itoa(int(i.Ival))
	}
	return ""
}
