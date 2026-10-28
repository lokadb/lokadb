package proxy

// The session's search_path, as the proxy follows it (schemas.go: qualify resolves bare names
// by it).

import (
	"regexp"
	"strings"
	"sync"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// pathState: the session's search_path (session.sp). path nil: not known now, read again
// (searchPath) before a statement needs it. The client's SET, SET LOCAL and set_config with a
// constant are followed; RESET, DISCARD ALL, SET ROLE (for "$user") and a computed value make
// it unknown, and so does the end of a transaction that changed it (txDirty: a SET LOCAL ends,
// a rollback undoes a SET). A function's own SET search_path does not outlive the function.
type pathState struct {
	mu      sync.Mutex
	path    []string // as set, "$user" not expanded
	user    string   // what "$user" stands for
	txDirty bool
}

// pathEffect: what a statement string does to search_path: sets it to value (the last one),
// or makes it unknown to the proxy.
type pathEffect struct {
	sets    bool
	value   []string
	unknown bool
}

// setSchemaRe: SET [SESSION | LOCAL] SCHEMA, a spelling of SET search_path.
var setSchemaRe = regexp.MustCompile(`(?i)\bset\s+(?:(?:session|local)\s+)?schema\b`)

// pathEffects: sql -> pathEffect, for the strings the cheap test lets through. Bounded (PF6 of
// the performance review: it grew with every distinct statement text that names search_path,
// role or reset): emptied when it holds pathEffectsMax entries.
var pathEffects = struct {
	sync.Mutex
	m map[string]pathEffect
}{m: map[string]pathEffect{}}

const pathEffectsMax = 4096

func cachedPathEffect(sql string) (pathEffect, bool) {
	pathEffects.Lock()
	defer pathEffects.Unlock()
	e, ok := pathEffects.m[sql]
	return e, ok
}

func storePathEffect(sql string, e pathEffect) {
	pathEffects.Lock()
	defer pathEffects.Unlock()
	if len(pathEffects.m) >= pathEffectsMax {
		pathEffects.m = map[string]pathEffect{}
	}
	pathEffects.m[sql] = e
}

func effectOnPath(sql string) pathEffect {
	// SET SCHEMA 'x' is SET search_path TO 'x' (the parser says so); UPDATE pg_settings is
	// another way to set it
	if !hasFold(sql, "search_path") && !hasFold(sql, "reset") && !hasFold(sql, "discard") &&
		!hasFold(sql, "role") && !hasFold(sql, "authorization") && !hasFold(sql, "set_config") &&
		!(hasFold(sql, "schema") && setSchemaRe.MatchString(sql)) && !hasFold(sql, "pg_settings") {
		return pathEffect{}
	}
	if e, ok := cachedPathEffect(sql); ok {
		return e
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return pathEffect{}
	}
	var e pathEffect
	for _, raw := range tree.Stmts {
		if vs := raw.Stmt.GetVariableSetStmt(); vs != nil {
			n := strings.ToLower(vs.Name)
			switch {
			case vs.Kind == pg.VariableSetKind_VAR_RESET_ALL:
				e = pathEffect{unknown: true}
			case n == "role" || n == "session_authorization" || n == "session authorization":
				e = pathEffect{unknown: true} // "$user"
			case n != "search_path":
			case vs.Kind == pg.VariableSetKind_VAR_SET_VALUE:
				var v []string
				ok := true
				for _, a := range vs.Args {
					c := a.GetAConst()
					if c == nil || c.GetSval() == nil {
						ok = false
						break
					}
					v = append(v, c.GetSval().GetSval()) // the parser folded the unquoted ones
				}
				e = pathEffect{sets: ok, value: v, unknown: !ok}
			default: // RESET, SET … TO DEFAULT, FROM CURRENT: the role's or database's default
				e = pathEffect{unknown: true}
			}
		}
		if u := raw.Stmt.GetUpdateStmt(); u != nil && u.Relation != nil && u.Relation.Relname == "pg_settings" {
			e = pathEffect{unknown: true} // UPDATE pg_settings SET setting = … WHERE name = 'search_path'
		}
		if d := raw.Stmt.GetDiscardStmt(); d != nil && d.Target == pg.DiscardMode_DISCARD_ALL {
			e = pathEffect{unknown: true}
		}
		if raw.Stmt.GetDoStmt() != nil { // a DO block that names search_path (the cheap test): what it does is not read
			e = pathEffect{unknown: true}
		}
		Walk(raw, func(msg proto.Message) {
			f, ok := msg.(*pg.FuncCall)
			if !ok || len(f.Funcname) == 0 || f.Funcname[len(f.Funcname)-1].GetString_().GetSval() != "set_config" || len(f.Args) < 2 {
				return
			}
			name, isConst := constText(f.Args[0])
			if isConst && !strings.EqualFold(strings.TrimSpace(name), "search_path") {
				return
			}
			if v, ok := constText(f.Args[1]); isConst && ok {
				e = pathEffect{sets: true, value: parsePath(v, "$user")}
			} else {
				e = pathEffect{unknown: true}
			}
		})
	}
	if len(sql) <= 64<<10 {
		storePathEffect(sql, e)
	}
	return e
}

// constText: the value of a text constant, cast or not.
func constText(n *pg.Node) (string, bool) {
	if tc := n.GetTypeCast(); tc != nil {
		n = tc.Arg
	}
	if c := n.GetAConst(); c != nil && c.GetSval() != nil {
		return c.GetSval().GetSval(), true
	}
	return "", false
}

// notePath records what sql did to the search path once it ran (onOK); inTx: inside a
// transaction block, whose end makes the path unknown again.
func (s *session) notePath(sql string, inTx bool) func() {
	e := effectOnPath(sql)
	if !e.unknown && s.meta != nil && callsPathFunc(sql, s.meta.PathFuncs) {
		e = pathEffect{unknown: true} // the function's change: read again
	}
	if !e.sets && !e.unknown {
		return nil
	}
	return func() {
		s.sp.mu.Lock()
		defer s.sp.mu.Unlock()
		if e.unknown {
			s.sp.path = nil
		} else {
			s.sp.path = e.value
		}
		s.sp.txDirty = s.sp.txDirty || inTx
	}
}

// noteSettings: noteSetting (the direct path) and notePath.
func (s *session) noteSettings(sql string, inTx bool) func() {
	return chainOK(s.noteSetting(sql, inTx), s.notePath(sql, inTx))
}

// pathTxEnd: a transaction ended (ReadyForQuery idle after one): a path it changed is unknown.
func (s *session) pathTxEnd() {
	s.sp.mu.Lock()
	if s.sp.txDirty {
		s.sp.path, s.sp.txDirty = nil, false
	}
	s.sp.mu.Unlock()
}

// initPath: the search path at connect (the startup parameters', or the role's and the
// database's default) and the role, read on the upstream connection.
func (s *session) initPath(value, user string) {
	s.sp.mu.Lock()
	s.sp.path, s.sp.user = parsePath(value, "$user"), user
	s.sp.mu.Unlock()
}

// searchPath: the session's search path with "$user" expanded, for qualify. Not known: read on
// the upstream connection when the proxy may (the queue empty, the transaction not failed);
// still not: nil.
func (s *session) searchPath() []string {
	s.sp.mu.Lock()
	p, user := s.sp.path, s.sp.user
	s.sp.mu.Unlock()
	if p == nil && s.q.empty() && !s.q.isDead() && s.txStatus() != 'E' {
		rows, err := s.internal(`SELECT current_setting('search_path'), current_user::text`)
		if err == nil && len(rows) == 1 && len(rows[0]) == 2 {
			p, user = parsePath(rows[0][0], "$user"), rows[0][1]
			s.sp.mu.Lock()
			s.sp.path, s.sp.user = p, user
			s.sp.txDirty = s.sp.txDirty || s.txStatus() != 'I' // a SET LOCAL's value, maybe
			s.sp.mu.Unlock()
		}
	}
	if p == nil {
		return nil
	}
	out := make([]string, 0, len(p))
	for _, x := range p {
		if x == "$user" {
			x = user
		}
		out = append(out, x)
	}
	return out
}

// pathKey: a search path as part of an analysis cache key.
func pathKey(path []string) string {
	if path == nil {
		return "\x01"
	}
	return strings.Join(path, "\x00")
}

// orgSearchPath: the search_path of the session's DDL in an organization's database whose
// tables of public are in schema orgSchema: the session's own, public replaced by orgSchema
// (a table of another schema is in that schema there too), public last for the types.
func (s *session) orgSearchPath(orgSchema string) string {
	if orgSchema == "" {
		orgSchema = "public"
	}
	path := defaultPath
	if s.meta != nil && s.meta.SchemasUsed {
		if p := s.searchPath(); p != nil {
			path = p
		}
	}
	var out []string
	seen := map[string]bool{}
	add := func(x string) {
		if !seen[x] {
			seen[x] = true
			out = append(out, quoteIdent(x))
		}
	}
	for _, x := range path {
		if x == "public" {
			x = orgSchema
		}
		add(x)
	}
	add("public")
	return strings.Join(out, ", ")
}

// callsPathFunc: sql calls (SELECT f(), CALL p()) one of funcs, functions that set the
// session's search_path (Meta.PathFuncs).
func callsPathFunc(sql string, funcs map[string]bool) bool {
	if len(funcs) == 0 {
		return false
	}
	named := false
	for f := range funcs {
		if hasFold(sql, strings.ToLower(f)) {
			named = true
			break
		}
	}
	if !named {
		return false
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return true // not read: as if it did
	}
	calls := false
	for _, raw := range tree.Stmts {
		Walk(raw, func(msg proto.Message) {
			if f, ok := msg.(*pg.FuncCall); ok && len(f.Funcname) > 0 && funcs[f.Funcname[len(f.Funcname)-1].GetString_().GetSval()] {
				calls = true
			}
			if _, ok := msg.(*pg.DoStmt); ok { // its body names one of them: not read, as if it called it
				calls = true
			}
		})
	}
	return calls
}
