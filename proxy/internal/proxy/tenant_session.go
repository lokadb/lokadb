package proxy

import (
	"fmt"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// The cooperative mode in the session (tasks/cooperative-mode.md): which organization the
// application named, and what a statement on LokaDB tables runs under then.

// TenantConfig is Config.Tenant: the decisions of 23.10 (tasks/cooperative-mode.md, section 8).
type TenantConfig struct {
	// Setting: the application's own variable that names the organization (app.current_tenant),
	// recognized besides lokadb.tenant. Its values are organization ids as well.
	Setting string `json:"setting,omitempty"`
	// Mismatch: "error" (the default): a statement that names an organization other than the
	// one the application named fails (42501); "log": it runs under the named one, logged.
	Mismatch string `json:"mismatch,omitempty"`
	// AllRoles: roles whose lokadb.tenant = '*' lets a statement across organizations run under
	// the fanout policy reject; for any other role '*' needs the statement on the allow list.
	AllRoles []string `json:"all_roles,omitempty"`
	// LocalOnly: the organization may be named only until the end of the transaction (SET
	// LOCAL, set_config(…, true)). Behind a pooler in transaction mode clients share the
	// proxy's session, and a session value of one client would name the organization of the
	// next one: SET without LOCAL, set_config(…, false) and the connection string are refused.
	// On unless set to false (the coordinator's decision of 24.10, block 9's B9.11): without a
	// pooler before the proxy, or with one in session mode, a session value is safe.
	LocalOnly *bool `json:"local_only,omitempty"`
}

// localOnly: Tenant.LocalOnly, on by default.
func (c TenantConfig) localOnly() bool { return c.LocalOnly == nil || *c.LocalOnly }

// tenantLocalOnly: a statement names the organization for the session under Tenant.LocalOnly.
type tenantLocalOnly struct{ setting string }

func (e *tenantLocalOnly) Error() string {
	return e.setting + " is set for the session, but tenant.local_only is on (a pooler in transaction mode shares the session between clients): " +
		"name the organization in every transaction with SET LOCAL " + e.setting + " or set_config('" + e.setting + "', …, true)"
}

// sessionRefused refuses a statement that names the organization for the session under
// LocalOnly. Clearing it (RESET, SET … TO DEFAULT, DISCARD ALL) passes: it names none.
func (c TenantConfig) sessionRefused(ts TenantSet, setting string) error {
	if c.localOnly() && !ts.Local && !ts.All && (ts.Value != "" || ts.Param > 0) {
		return &tenantLocalOnly{setting: setting}
	}
	return nil
}

// tenantCode: the SQLSTATE of an error of a statement that sets lokadb.tenant: 0A000 for one
// LocalOnly refuses, 22023 for a value that is not an organization.
func tenantCode(err error) string {
	if _, ok := err.(*tenantLocalOnly); ok {
		return "0A000"
	}
	return tenantValueCode
}

// Validate refuses a config that would let the application set what the proxy sets: a
// Setting of lokadb.… other than lokadb.tenant (lokadb.org), and unknown Mismatch values.
func (c *TenantConfig) Validate() error {
	if c == nil {
		return nil
	}
	if s := strings.ToLower(strings.TrimSpace(c.Setting)); strings.HasPrefix(s, "lokadb.") && s != TenantSetting {
		return fmt.Errorf("tenant.setting %q: lokadb.… settings other than %s are the proxy's own", c.Setting, TenantSetting)
	}
	if c.Mismatch != "" && c.Mismatch != "error" && c.Mismatch != TenantMismatchLog {
		return fmt.Errorf("tenant.mismatch %q: error or log", c.Mismatch)
	}
	return nil
}

// TenantMismatchLog: Mismatch "log".
const TenantMismatchLog = "log"

// tenantSetOf recognizes lokadb.tenant, and extra (Config.Tenant.Setting) when it is set.
func tenantSetOf(n *pg.Node, extra string) (TenantSet, bool, error) {
	if ts, ok, err := tenantSet(n, TenantSetting); ok {
		return ts, ok, err
	}
	if extra != "" && !strings.EqualFold(extra, TenantSetting) {
		return tenantSet(n, extra)
	}
	return TenantSet{}, false, nil
}

// mentionsTenant: sql may set or reset the organization's setting (a cheap test before parsing).
func mentionsTenant(sql, extra string) bool {
	// lokadb and tenant both: a column tenant_id alone is no reason to parse (review of
	// performance, PF12)
	return hasFold(sql, "tenant") && hasFold(sql, "lokadb") || hasFold(sql, "reset") || hasFold(sql, "discard") ||
		extra != "" && hasFold(sql, strings.ToLower(extra))
}

// tenantString: the transaction and tenant events of a query string in order ('N' for each
// TenantSet, in sets), and the index of the last statement that sets the tenant (-1: none).
// No events when the string sets no tenant: Analysis.Events tell the rest.
func tenantString(sql string, cfg TenantConfig) (evs []TxEvent, sets []TenantSet, last int, err error) {
	extra := cfg.Setting
	if !mentionsTenant(sql, extra) {
		return nil, nil, -1, nil
	}
	tree, perr := pg.Parse(sql)
	if perr != nil {
		return nil, nil, -1, nil
	}
	last = -1
	for i, raw := range tree.Stmts {
		if ts := raw.Stmt.GetTransactionStmt(); ts != nil {
			switch ts.Kind {
			case pg.TransactionStmtKind_TRANS_STMT_BEGIN, pg.TransactionStmtKind_TRANS_STMT_START:
				evs = append(evs, TxEvent{Kind: 'B'})
			case pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
				evs = append(evs, TxEvent{Kind: 'S', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_RELEASE:
				evs = append(evs, TxEvent{Kind: 'R', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
				evs = append(evs, TxEvent{Kind: 'T', SQL: ts.SavepointName})
			case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
				evs = append(evs, TxEvent{Kind: 'X'})
			case pg.TransactionStmtKind_TRANS_STMT_COMMIT:
				evs = append(evs, TxEvent{Kind: 'C'})
			}
			continue
		}
		ts, ok, terr := tenantSetOf(raw.Stmt, extra)
		if terr == nil && ok {
			terr = cfg.sessionRefused(ts, TenantSetting)
		}
		if terr != nil {
			return nil, nil, -1, terr
		}
		if ok {
			if ts.Param > 0 {
				return nil, nil, -1, fmt.Errorf("%s: a parameter needs the extended protocol", TenantSetting)
			}
			evs = append(evs, TxEvent{Kind: 'N'})
			sets = append(sets, ts)
			last = i
		}
	}
	if len(sets) == 0 {
		return nil, nil, -1, nil
	}
	return evs, sets, last, nil
}

// stmtTenant: what a prepared statement does to the tenant (extended protocol: one statement).
func stmtTenant(sql string, cfg TenantConfig) (*TenantSet, error) {
	extra := cfg.Setting
	if !mentionsTenant(sql, extra) {
		return nil, nil
	}
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return nil, nil
	}
	ts, ok, err := tenantSetOf(tree.Stmts[0].Stmt, extra)
	if err == nil && ok {
		err = cfg.sessionRefused(ts, TenantSetting)
	}
	if err != nil || !ok {
		return nil, err
	}
	return &ts, nil
}

// clone: a copy whose savepoints do not share the original's array.
func (t *tenantState) clone() tenantState {
	c := *t
	c.sps = append([]tenantSavepoint(nil), t.sps...)
	return c
}

// tenantMismatch: a statement names another organization than the application did.
type tenantMismatch struct{ named, tenant, setting string }

func (e *tenantMismatch) Error() string {
	return fmt.Sprintf("the statement names organization %s, but %s is %s", e.named, e.setting, e.tenant)
}

// tenantError: the SQLSTATE and message of an error of tenantOrg: 42501 for a mismatch.
func tenantError(err error) (code, msg string) {
	if _, ok := err.(*tenantMismatch); ok {
		return "42501", "lokadb: " + err.Error()
	}
	return "0A000", "lokadb: " + err.Error()
}

// tenantValueCode: the SQLSTATE of a value of lokadb.tenant the proxy refuses.
const tenantValueCode = "22023"

// tenantApply follows one Execute in a batch: its transaction events, then the tenant it sets.
func (s *session) tenantApply(evs []TxEvent, ts *TenantSet) {
	if hasEvent(evs, 'B') {
		s.tenantImpl = false // the pipeline's implicit transaction became a transaction block
	}
	s.tenant.apply(evs, nil)
	if ts != nil {
		s.tenant.set(*ts)
	}
}

// boundTenant: the tenant a Bind of st sets, its parameter read from the Bind (p.values).
func boundTenant(st *stmtInfo, p *portalInfo) (*TenantSet, error) {
	if st.terr != nil {
		return nil, st.terr
	}
	if st.tset == nil {
		return nil, nil
	}
	ts := *st.tset
	if ts.Param > 0 {
		xs, ok := p.values(ValueRef{Param: ts.Param}) // as it is: tenantValue reads it
		if !ok || len(xs) != 1 {
			return nil, fmt.Errorf("%s: the value of the parameter cannot be read", TenantSetting)
		}
		v := xs[0]
		if v == nullValue {
			v = ""
		}
		var err error
		if ts.Value, err = tenantValue(v, TenantSetting); err != nil {
			return nil, err
		}
		ts.Param = 0
	}
	return &ts, nil
}

func (s *session) tenantCfg() TenantConfig {
	if c := s.srv.cfg.Tenant; c != nil && c.Validate() == nil { // lokadb proxy refuses to start with an invalid one
		return *c
	}
	return TenantConfig{}
}

// tenantOrg reconciles a statement on LokaDB tables with the organization the application
// named (an id): its organization column at once, ids and references when the session knows
// their organization already. Nothing is looked up for that (P4): the cloud's policy keeps
// reads within org, and a row of another organization is not found; a child row written under
// org with a parent of another organization is refused by lokadb_parent (23514). With
// Tenant.Mismatch "log" a mismatch is logged and the statement runs under org.
func (s *session) tenantOrg(sql string, an *Analysis, org string, vals valuesFunc) error {
	s.freshOrgs(an) // its changes of lookups count as in resolveOrg (orgVersion, the session's own cache)
	cfg := s.tenantCfg()
	setting := TenantSetting
	if cfg.Setting != "" {
		setting = cfg.Setting
	}
	check := func(named string) error {
		if named == org {
			return nil
		}
		if named == noOrg {
			named = "none"
		}
		e := &tenantMismatch{named: named, tenant: org, setting: setting}
		if cfg.Mismatch == TenantMismatchLog {
			s.log.Warn("runs under the organization the application named, not the one the statement names", "err", e.Error(), "sql", brief(sql))
			return nil
		}
		return e
	}
	for _, h := range an.Hints {
		refs, ok := s.meta.Lookups[h.Table][h.Column]
		if !ok {
			continue
		}
		kind := s.meta.keyKind(refs) // the values in canonical form (keys.go)
		for _, v := range h.Values {
			v.Kind = kind
			xs, ok := vals(v)
			if !ok {
				continue // a value the proxy cannot read: the cloud's policy and CHECKs stand
			}
			for _, x := range xs {
				switch {
				case x == nullValue:
					if refs == "" { // a row of no organization under a named one
						if err := check(noOrg); err != nil {
							return err
						}
					}
				case refs == "":
					if err := check(x); err != nil {
						return err
					}
				default:
					// the session's own knowledge alone, as before the process's cache (orgcache.go): a row
					// of another organization another session looked up is not found here, not refused
					if o, ok := s.orgCache[refs][x]; ok {
						if err := check(o); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

// allRole: the session's role may run statements across organizations with lokadb.tenant = '*'.
func (s *session) allRole() bool {
	for _, r := range s.tenantCfg().AllRoles {
		if r == s.user {
			return true
		}
	}
	return false
}

// refuseNamed is refuse in the cooperative mode: a statement under a named organization is
// never a fanout; under '*' a role of Tenant.AllRoles runs it whatever the policy, any other
// role needs it on the allow list (decision of 23.10).
func (s *session) refuseNamed(sql string, an *Analysis, unknown bool, named string) bool {
	all := named == TenantAll && an.Lokadb && (unknown || an.Unhinted)
	switch {
	case named != "" && named != TenantAll:
		return false
	case all && s.allRole():
		s.log.Info("runs in every organization's database: lokadb.tenant = '*'", "role", s.user, "table", fanoutTable(an), "sql", brief(sql))
		return false
	}
	if s.refuse(sql, an, unknown) {
		return true
	}
	if all {
		s.log.Info("runs in every organization's database: lokadb.tenant = '*'", "table", fanoutTable(an), "sql", brief(sql))
	}
	return false
}

// startupTenant: lokadb.tenant from the connection string (options=-c lokadb.tenant=42, or the
// setting as a startup parameter of its own, by either name): the session's value from the
// start. The parameters come as a map, in no order: set twice with different values (after
// tenantValue) it is refused, or the proxy could route by one and the server keep the other.
func startupTenant(params map[string]string, extra string) (string, error) {
	names := []string{TenantSetting}
	if extra != "" {
		names = append(names, strings.ToLower(extra))
	}
	val, found := "", false
	var bad error
	take := func(raw string) {
		v, err := tenantValue(raw, TenantSetting)
		switch {
		case bad != nil:
		case err != nil:
			bad = err
		case found && v != val:
			bad = fmt.Errorf("%s is set twice with different values", TenantSetting)
		}
		val, found = v, true
	}
	for k, v := range params { // the options are split as startupLokadb splits them
		for _, n := range names {
			if strings.EqualFold(strings.TrimSpace(k), n) {
				take(v)
			}
		}
		if !strings.EqualFold(k, "options") {
			continue
		}
		for _, opt := range splitOptions(v) {
			opt = strings.TrimPrefix(strings.TrimPrefix(opt, "-c"), "--")
			name, x, ok := strings.Cut(opt, "=")
			for _, n := range names {
				if ok && strings.EqualFold(strings.TrimSpace(name), n) {
					take(x)
				}
			}
		}
	}
	if bad != nil {
		return "", bad
	}
	return val, nil
}
