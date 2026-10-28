package proxy

import (
	"regexp"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// Functions and views created through the proxy (review of routing R1). Meta.Funcs and
// Meta.Views are read from the catalog (loadDeps) when the metadata is read: a function or a
// view over LokaDB tables created a moment ago was unknown until then, its use no statement on
// LokaDB tables. The proxy reads what such a statement creates: a function whose body names a
// LokaDB table, a view over one or a function that does, a view whose query reads one, are
// known to the session at once; the metadata of every session is read again (schemaVersion)
// for what the catalog adds (pg_depend, functions it calls).

// createStart, prepareStart: a statement of the text begins with CREATE or ALTER (PREPARE, DEALLOCATE,
// DISCARD), after comments: the full parse is for those alone (third review of performance,
// perf3-13: the word in created_at, which every INSERT of Laravel has, made it parse at every
// execution, 81 µs). A semicolon inside a literal only costs a parse.
var (
	createStart  = stmtStart{stmtStartRe("create|alter"), []string{"create", "alter"}}
	prepareStart = stmtStart{stmtStartRe("prepare|deallocate|discard"), []string{"prepare", "deallocate", "discard"}}
)

// stmtStart: the words, and the expression that finds them after a semicolon or comments.
type stmtStart struct {
	re    *regexp.Regexp
	words []string
}

// MatchString: a text of one statement without a leading comment (the rule) is told by its
// first word, without the expression (which scans the whole text: 12 µs for an INSERT).
func (st stmtStart) MatchString(sql string) bool {
	t := strings.TrimLeft(sql, " \t\r\n")
	if !strings.Contains(sql, ";") && !strings.HasPrefix(t, "--") && !strings.HasPrefix(t, "/*") {
		for _, w := range st.words {
			if len(t) >= len(w) && strings.EqualFold(t[:len(w)], w) && (len(t) == len(w) || !isWordByte(t[len(w)])) {
				return true
			}
		}
		return false
	}
	return st.re.MatchString(sql)
}

func isWordByte(b byte) bool {
	return b == '_' || b >= '0' && b <= '9' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= 0x80
}

func stmtStartRe(words string) *regexp.Regexp {
	return regexp.MustCompile(`(?is)(?:^|;)\s*(?:(?:--[^\n]*(?:\n|$)|/\*.*?\*/)\s*)*(?:` + words + `)\b`)
}

// noteCreated: what sql creates over LokaDB tables, added to the session's metadata (a copy:
// the metadata may be other sessions' too). Before the statement runs: one that fails leaves
// a name the analyzer treats as LokaDB, the safe side.
func (s *session) noteCreated(sql string) {
	if s.meta == nil {
		return
	}
	if !createStart.MatchString(sql) {
		return
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return
	}
	funcs, views := createdOver(tree, s.meta)
	if len(funcs) == 0 && len(views) == 0 {
		if creates(tree) { // another function or view: what it calls may be read only from the catalog
			s.srv.schemaVersion(s.db).Add(1)
			s.metaDirty, s.createdPending = true, true
		}
		return
	}
	m := *s.meta
	m.Funcs, m.Views = map[string]bool{}, map[string][]string{}
	for k, v := range s.meta.Funcs {
		m.Funcs[k] = v
	}
	for k, v := range s.meta.Views {
		m.Views[k] = v
	}
	for _, f := range funcs {
		m.Funcs[f] = true
	}
	for k, t := range views {
		m.Views[k] = t
	}
	s.meta = &m
	s.anCache.reset()
	s.srv.schemaVersion(s.db).Add(1)
	s.metaDirty, s.createdPending = true, true
}

// creates: tree creates or alters a function or a view.
func creates(tree *pg.ParseResult) bool {
	for _, raw := range tree.Stmts {
		n := raw.Stmt
		if n.GetCreateFunctionStmt() != nil || n.GetViewStmt() != nil || n.GetAlterFunctionStmt() != nil ||
			n.GetCreateTableAsStmt() != nil && n.GetCreateTableAsStmt().Objtype == pg.ObjectType_OBJECT_MATVIEW {
			return true
		}
	}
	return false
}

// createdOver: the functions (by name) and views (by key) tree creates over LokaDB tables of m.
func createdOver(tree *pg.ParseResult, m *Meta) (funcs []string, views map[string][]string) {
	for _, raw := range tree.Stmts {
		n := raw.Stmt
		switch {
		case n.GetCreateFunctionStmt() != nil:
			f := n.GetCreateFunctionStmt()
			if len(f.Funcname) > 0 && (bodyNamesLokadb(functionBody(f), m) || f.SqlBody != nil && len(readsLokadb(f.SqlBody, m)) > 0) {
				funcs = append(funcs, f.Funcname[len(f.Funcname)-1].GetString_().GetSval())
			}
		case n.GetViewStmt() != nil:
			v := n.GetViewStmt()
			if t := readsLokadb(v.Query, m); len(t) > 0 {
				if views == nil {
					views = map[string][]string{}
				}
				views[relKey(v.View)] = t
			}
		case n.GetCreateTableAsStmt() != nil && n.GetCreateTableAsStmt().Objtype == pg.ObjectType_OBJECT_MATVIEW:
			c := n.GetCreateTableAsStmt()
			if t := readsLokadb(c.Query, m); len(t) > 0 && c.Into != nil {
				if views == nil {
					views = map[string][]string{}
				}
				views[relKey(c.Into.Rel)] = t
			}
		}
	}
	return funcs, views
}

// functionBody: the source of a CREATE FUNCTION (AS '…'; a SQL-standard body is read as a tree).
func functionBody(f *pg.CreateFunctionStmt) string {
	var b strings.Builder
	for _, o := range f.Options {
		d := o.GetDefElem()
		if d == nil || d.Defname != "as" {
			continue
		}
		for _, item := range d.Arg.GetList().GetItems() {
			b.WriteString(item.GetString_().GetSval())
			b.WriteString(" ")
		}
	}
	return b.String()
}

// bodyNamesLokadb: src names a LokaDB table, a view over one or a function that reads one, as
// loadDeps reads pg_proc.prosrc: a word, in any case.
func bodyNamesLokadb(src string, m *Meta) bool {
	var names []string
	for k := range m.Tables {
		_, n := rel.SplitKey(k)
		names = append(names, regexp.QuoteMeta(n))
	}
	for k := range m.Views {
		_, n := rel.SplitKey(k)
		names = append(names, regexp.QuoteMeta(n))
	}
	for f := range m.Funcs {
		names = append(names, regexp.QuoteMeta(f))
	}
	if len(names) == 0 {
		return false
	}
	sort.Strings(names)
	re, err := regexp.Compile(`(?i)\b(` + strings.Join(names, "|") + `)\b`)
	return err != nil || re.MatchString(src)
}

// readsLokadb: the LokaDB tables query reads, directly, through a view over them or a function
// that reads them (sorted).
func readsLokadb(query *pg.Node, m *Meta) []string {
	set := map[string]bool{}
	Walk(query, func(msg proto.Message) {
		switch x := msg.(type) {
		case *pg.RangeVar:
			if t := m.table(x); t != nil {
				set[t.Name] = true
			}
			for _, t := range m.view(x) {
				set[t] = true
			}
		case *pg.FuncCall:
			if n := x.Funcname; len(n) > 0 && m.Funcs[n[len(n)-1].GetString_().GetSval()] {
				for k := range m.Tables {
					set[k] = true // which of them, the catalog tells: all, the safe side
				}
			}
		}
	})
	var out []string
	for t := range set {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
