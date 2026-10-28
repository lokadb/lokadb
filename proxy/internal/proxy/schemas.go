package proxy

// Tables outside schema public (block 4, item 9). A LokaDB table is named by its key (package
// rel): app.documents. A statement names it as the application does: qualified, or bare and
// resolved by the session's search_path, which the cloud connection has too (the client's SET
// goes there). qualify gives a bare name the schema the search path resolves it to, so that
// the analyzer, which looks tables up by key, sees what the cloud will run: a cloud table
// app.documents earlier in the path hides the LokaDB table public.documents, and the other way
// round.

import (
	"fmt"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// defaultPath: the search path of an analysis without a session (lokadb check, Direct): public.
var defaultPath = []string{"public"}

// qualify sets the schema of each unqualified relation of stmt whose name m.Rels knows to the
// schema path resolves it to, when that is not public (unqualified stays public's). path nil:
// the session's search path is not known now; a name of one schema only is that schema's, a
// name of several is returned (the statement is refused: the proxy cannot tell its table).
// CTE names and the names a statement creates are left alone.
func qualify(stmt *pg.Node, m *Meta, path []string) (ambiguous string) {
	if m == nil || !m.SchemasUsed || stmt == nil {
		return ""
	}
	ctes, created := map[string]bool{}, map[*pg.RangeVar]bool{}
	Walk(stmt, func(msg proto.Message) {
		switch x := msg.(type) {
		case *pg.CommonTableExpr:
			ctes[x.Ctename] = true
		case *pg.CreateStmt:
			created[x.Relation] = true
		case *pg.IntoClause:
			created[x.Rel] = true
		case *pg.ViewStmt:
			created[x.View] = true
		case *pg.CreateSeqStmt:
			created[x.Sequence] = true
		}
	})
	Walk(stmt, func(msg proto.Message) {
		rv, ok := msg.(*pg.RangeVar)
		if !ok || rv.Schemaname != "" || rv.Catalogname != "" || ctes[rv.Relname] || created[rv] || rv.Relpersistence == "t" {
			return
		}
		in := m.Rels[rv.Relname]
		if len(in) == 0 {
			return
		}
		s := resolveIn(in, path)
		switch {
		case s == "" && path == nil && len(in) == 1:
			s = in[0]
		case s == "" && path == nil:
			if ambiguous == "" {
				ambiguous = rv.Relname
			}
			return
		}
		if s != "" && s != "public" {
			rv.Schemaname = s
		}
	})
	return ambiguous
}

// resolveIn: the first schema of path among in ("" for none).
func resolveIn(in, path []string) string {
	for _, p := range path {
		for _, s := range in {
			if s == p {
				return s
			}
		}
	}
	return ""
}

// ambiguousMsg: the refusal of a statement whose bare table name qualify could not resolve.
func ambiguousMsg(name string) string {
	return fmt.Sprintf("lokadb: %s is a table of several schemas and the proxy does not know the session's search_path right now "+
		"(it changed in a way the proxy cannot follow, such as set_config with a computed value or RESET inside a pipeline): "+
		"name the schema (schema.%s), or run the statement again after a Sync", name, name)
}

// parsePath: the schemas of a search_path value, in order, "$user" as user. Quoted names keep
// their case, unquoted ones are folded to lower case, as Postgres does.
func parsePath(v, user string) []string {
	var out []string
	for _, part := range splitPath(v) {
		part = strings.TrimSpace(part)
		switch {
		case part == "":
			continue
		case strings.HasPrefix(part, `"`) && strings.HasSuffix(part, `"`) && len(part) >= 2:
			part = strings.ReplaceAll(part[1:len(part)-1], `""`, `"`)
		default:
			part = strings.ToLower(part)
		}
		if part == "$user" {
			part = user
		}
		out = append(out, part)
	}
	if out == nil {
		out = []string{}
	}
	return out
}

// splitPath splits a search_path value at the commas outside double quotes.
func splitPath(v string) []string {
	var out []string
	var cur strings.Builder
	quoted := false
	for _, r := range v {
		switch {
		case r == '"':
			quoted = !quoted
			cur.WriteRune(r)
		case r == ',' && !quoted:
			out = append(out, cur.String())
			cur.Reset()
		default:
			cur.WriteRune(r)
		}
	}
	return append(out, cur.String())
}

// qrel: how SQL names the relation of key k: unqualified in public, as before; qualified in
// another schema (a partition is in its parent's schema).
func qrel(k string) string {
	s, n := rel.SplitKey(k)
	if n == k {
		return quoteIdent(k)
	}
	return quoteIdent(s) + "." + quoteIdent(n)
}

// named: x is the name (without its schema) of a LokaDB table or of a view over one, in any
// schema: for the identifiers of a DO block or of a statement the parser cannot read, whose
// schema the proxy cannot tell.
func (m *Meta) named(x string) bool {
	if m.Tables[x] != nil || len(m.Views[x]) > 0 {
		return true
	}
	for _, keys := range []map[string]bool{tableKeys(m), viewKeys(m)} {
		for k := range keys {
			if _, n := rel.SplitKey(k); n == x {
				return true
			}
		}
	}
	return false
}

func viewKeys(m *Meta) map[string]bool {
	out := map[string]bool{}
	for k := range m.Views {
		out[k] = true
	}
	return out
}

// indexOf: the LokaDB table of index schema.name ("" for none). A bare name is public's, or the
// one index of that name in another schema (an index is named in its table's schema).
func (m *Meta) indexOf(schema, name string) string {
	if t := m.Indexes[rel.RelKey(schema, name)]; t != "" || schema != "" {
		return t
	}
	for k, t := range m.Indexes {
		if _, n := rel.SplitKey(k); n == name {
			return t
		}
	}
	return ""
}

// listKey: the key of a qualified name given as a list of strings (DROP's objects).
func listKey(items []*pg.Node) string {
	if len(items) == 0 {
		return ""
	}
	name := items[len(items)-1].GetString_().GetSval()
	if len(items) >= 2 {
		return rel.RelKey(items[len(items)-2].GetString_().GetSval(), name)
	}
	return name
}

// qfull: the relation of key k always with its schema (public. too): for a statement whose
// search_path the proxy does not control (a function of pg_temp).
func qfull(k string) string {
	s, n := rel.SplitKey(k)
	if s == "public" {
		return "public." + quoteIdent(n)
	}
	return quoteIdent(s) + "." + quoteIdent(n)
}

// inSchema: the key of relation name in the schema of the relation of key k.
func inSchema(k, name string) string {
	s, _ := rel.SplitKey(k)
	return rel.RelKey(s, name)
}

// pathFor: the search path to analyze the session's statements with.
func (s *session) pathFor() []string {
	if s.meta != nil && s.meta.SchemasUsed {
		return s.searchPath()
	}
	return defaultPath
}

// ParsePath: the schemas of a search_path value, "$user" as user (lokadb check).
func ParsePath(v, user string) []string {
	p := parsePath(v, "$user")
	for i, x := range p {
		if x == "$user" {
			p[i] = user
		}
	}
	return p
}

// TableIn: the LokaDB table rv names when the search path is path (lokadb check).
func (m *Meta) TableIn(rv *pg.RangeVar, path []string) *Table {
	if m == nil || rv == nil {
		return nil
	}
	if rv.Schemaname == "" && m.SchemasUsed {
		if s := resolveIn(m.Rels[rv.Relname], path); s != "" {
			return m.Tables[rel.RelKey(s, rv.Relname)]
		}
	}
	return m.table(rv)
}
