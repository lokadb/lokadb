package proxy

import (
	pg "github.com/pganalyze/pg_query_go/v6"
)

// SQL PREPARE and EXECUTE (review of routing R1). EXECUTE p(5) names no table: analysed as it
// is, it was no statement on LokaDB tables and ran under lokadb.org = '' (every organization),
// the fanout policy never asked. The proxy keeps the text of each statement a session PREPAREs
// with SQL and routes EXECUTE by it: the prepared statement's analysis, its parameters the
// EXECUTE's arguments (constants; another expression leaves the organization unknown, which is
// the policy's). An EXECUTE among other statements of a query string, or sent by Parse/Bind,
// of a statement on LokaDB tables is refused: the proxy would have to route the string's
// statements by two analyses.

// sqlPrepared: what sql PREPAREs (name → the statement's text, its parameters $n) and the
// names it deallocates (DEALLOCATE, DISCARD ALL: all) are kept in s.prepared. A name the
// session holds already keeps its text: Postgres refuses to prepare it again.
func (s *session) notePrepare(sql string) {
	if !prepareStart.MatchString(sql) { // see createStart
		return
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return
	}
	for _, raw := range tree.Stmts {
		switch {
		case raw.Stmt.GetPrepareStmt() != nil:
			p := raw.Stmt.GetPrepareStmt()
			if _, there := s.prepared[p.Name]; there {
				continue
			}
			body, err := deparseStmt(p.Query)
			if err != nil {
				body = "" // unreadable: its EXECUTE is refused when it names LokaDB tables (below)
			}
			if s.prepared == nil {
				s.prepared = map[string]string{}
			}
			s.prepared[p.Name] = body
		case raw.Stmt.GetDeallocateStmt() != nil:
			d := raw.Stmt.GetDeallocateStmt()
			if d.Isall {
				s.prepared = nil
			} else {
				delete(s.prepared, d.Name)
			}
		case raw.Stmt.GetDiscardStmt() != nil && raw.Stmt.GetDiscardStmt().Target == pg.DiscardMode_DISCARD_ALL:
			s.prepared = nil
		}
	}
}

// executeMsg: the refusal of an EXECUTE the proxy cannot route by its prepared statement.
const executeMsg = "lokadb: EXECUTE of a statement on LokaDB tables prepared with SQL PREPARE is routed only as a query string of its own " +
	"(not with other statements, not through Parse/Bind): send it alone, or prepare the statement through the driver's protocol"

// executed: the prepared statement of each EXECUTE of sql, with its arguments, by position of
// the EXECUTE among sql's statements; nil when sql executes none.
func executed(sql string) (names []string, args [][]*pg.Node, stmts int) {
	if !hasFold(sql, "execute") {
		return nil, nil, 0
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil, nil, 0
	}
	for _, raw := range tree.Stmts {
		if e := raw.Stmt.GetExecuteStmt(); e != nil {
			names = append(names, e.Name)
			args = append(args, e.Params)
		}
	}
	return names, args, len(tree.Stmts)
}

// executeAnalysis: the analysis an EXECUTE string is routed by, and the values of its
// parameters. nil, nil, "": sql executes nothing of LokaDB tables the proxy knows of (as before:
// an unknown name fails upstream, closed by openOrg meanwhile). msg: refused. extended: the
// statement came by Parse/Bind.
func (s *session) executeAnalysis(sql string, extended bool) (*Analysis, valuesFunc, string) {
	names, args, n := executed(sql)
	if len(names) == 0 {
		return nil, nil, ""
	}
	var found *Analysis
	for _, name := range names {
		body, ok := s.prepared[name]
		if !ok {
			continue
		}
		if body == "" { // a PREPARE the proxy could not read back
			return nil, nil, executeMsg
		}
		if an := s.analysis(body); an.Lokadb {
			found = an
		}
	}
	if found == nil {
		return nil, nil, ""
	}
	if extended || n != 1 {
		return nil, nil, executeMsg
	}
	ex := *found
	// sent as the client wrote it (EXECUTE p(…)): nothing of the body's rewriting or targeting
	ex.Rewritten, ex.Target, ex.TargetSQL, ex.TargetLocal, ex.UpsertLock = "", "", "", false, ""
	ex.Stmts, ex.Events, ex.DDL, ex.DDLTables = []string{sql}, nil, nil, nil
	params := args[0]
	vals := func(v ValueRef) ([]string, bool) {
		if v.Param != 0 {
			if v.Param > len(params) {
				return nil, false
			}
			c, ok := value(params[v.Param-1])
			if !ok || c.Param != 0 {
				return nil, false
			}
			c.Kind, c.Array = v.Kind, v.Array
			if c.Array {
				return nil, false
			}
			return constOnly(c)
		}
		return constOnly(v)
	}
	return &ex, vals, ""
}
