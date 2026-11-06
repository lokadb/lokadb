package proxy

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// rejection: the error the client gets instead of running the statement, "" if none.
func (a *Analysis) rejection() string {
	if a.Forbidden != "" {
		return a.Forbidden
	}
	return a.DDLError
}

// forbidden: why a statement must not run in a LokaDB database at all, "" if it may.
//
// Partitions, refs tables and the lokadb schema are LokaDB's own: addressed directly they skip
// the tenant policy of the parent (and a foreign partition reads the organization's
// database). Through an organization's server (lokadb_org<N>) a foreign table could point at
// any table there. Only these exact objects are LokaDB's: the application's own tables,
// servers and foreign tables, whatever their names look like, are left alone.
func forbidden(n *pg.Node, m *Meta) string {
	if m == nil {
		return ""
	}
	const fdwMsg = "lokadb: server %s belongs to LokaDB (an organization's database): not accessible through the proxy"
	if msg := ownStmtName(n); msg != "" {
		return msg
	}
	server := ""
	switch {
	case n.GetCreateForeignTableStmt() != nil:
		server = n.GetCreateForeignTableStmt().Servername
	case n.GetImportForeignSchemaStmt() != nil:
		server = n.GetImportForeignSchemaStmt().ServerName
	case n.GetCreateForeignServerStmt() != nil:
		server = n.GetCreateForeignServerStmt().Servername
	case n.GetAlterForeignServerStmt() != nil:
		server = n.GetAlterForeignServerStmt().Servername
	case n.GetCreateUserMappingStmt() != nil:
		server = n.GetCreateUserMappingStmt().Servername
	case n.GetAlterUserMappingStmt() != nil:
		server = n.GetAlterUserMappingStmt().Servername
	case n.GetDropUserMappingStmt() != nil:
		server = n.GetDropUserMappingStmt().Servername
	case n.GetAlterFdwStmt() != nil:
		if n.GetAlterFdwStmt().Fdwname == "postgres_fdw" {
			return "lokadb: postgres_fdw carries the organizations' partitions: ALTER FOREIGN DATA WRAPPER is not allowed through the proxy"
		}
	case n.GetDropStmt() != nil:
		if msg := dropsOwn(n.GetDropStmt(), m); msg != "" {
			return msg
		}
	case n.GetCreatePolicyStmt() != nil:
		st := n.GetCreatePolicyStmt()
		if msg := ownPolicy(st.Table, m, st.PolicyName); msg != "" {
			return msg
		}
	case n.GetAlterPolicyStmt() != nil:
		st := n.GetAlterPolicyStmt()
		if msg := ownPolicy(st.Table, m, st.PolicyName); msg != "" {
			return msg
		}
	case n.GetRenameStmt() != nil:
		r := n.GetRenameStmt()
		if r.RenameType == pg.ObjectType_OBJECT_POLICY {
			if msg := ownPolicy(r.Relation, m, r.Subname, r.Newname); msg != "" {
				return msg
			}
		}
		switch {
		case r.RenameType == pg.ObjectType_OBJECT_SCHEMA && r.Subname == "lokadb":
			return "lokadb: schema lokadb belongs to LokaDB: not accessible through the proxy"
		case r.RenameType == pg.ObjectType_OBJECT_FOREIGN_SERVER && ownServer(objName(r.Object)):
			server = objName(r.Object)
		}
	}
	if ownServer(server) {
		return fmt.Sprintf(fdwMsg, server)
	}
	if msg := ownInBody(n, m); msg != "" {
		return msg
	}
	msg, textView := "", false
	walk(n.ProtoReflect(), func(msgp proto.Message) {
		if msg != "" {
			return
		}
		// block 7: lokadb.replay_targets() hands out the organizations' credentials for the
		// proxy's DDL replay; no lokadb function is the application's to call
		// (same_org is the proxy's own rewrite of an UPDATE of the organization column and is harmless:
		// it shows in pg_stat_statements, so the check reads it back through this guard)
		if fc, ok := msgp.(*pg.FuncCall); ok && len(fc.Funcname) > 1 && fc.Funcname[len(fc.Funcname)-2].GetString_().GetSval() == "lokadb" &&
			fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval() != "same_org" {
			msg = fmt.Sprintf("lokadb: lokadb.%s() belongs to LokaDB: not accessible through the proxy", fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval())
			return
		}
		// block 2 (COPY FROM STDIN): the proxy's own helpers in pg_temp, __lokadb_copy_<table>
		// (a table, a function, a trigger of it); the client has no business naming them
		switch x := msgp.(type) {
		case *pg.FuncCall:
			if n := x.Funcname; len(n) > 0 && ownTempName(n[len(n)-1].GetString_().GetSval(), n[:len(n)-1]) {
				msg = ownTempMsg(n[len(n)-1].GetString_().GetSval())
				return
			}
			if msg = catalogFunc(x, m); msg != "" {
				return
			}
		case *pg.CreateTrigStmt:
			if ownTempName(x.Trigname, nil) || x.Relation != nil && ownTempName(x.Relation.Relname, schemaNodes(x.Relation.Schemaname)) {
				msg = ownTempMsg(x.Trigname)
				return
			}
		case *pg.CreateFunctionStmt:
			if n := x.Funcname; len(n) > 0 && ownTempName(n[len(n)-1].GetString_().GetSval(), n[:len(n)-1]) {
				msg = ownTempMsg(n[len(n)-1].GetString_().GetSval())
				return
			}
		case *pg.DropStmt:
			for _, o := range x.Objects {
				if owa := o.GetObjectWithArgs(); owa != nil { // DROP FUNCTION name(args)
					o = &pg.Node{Node: &pg.Node_List{List: &pg.List{Items: owa.Objname}}}
				}
				if name := objName(o); strings.HasPrefix(name, ownTempPrefix) && (objSchema(o) == "" || objSchema(o) == "pg_temp") {
					msg = ownTempMsg(name)
					return
				}
			}
		}
		rv, ok := msgp.(*pg.RangeVar)
		if !ok {
			return
		}
		textView = textView || statementTextViews[rv.Relname]
		if (rv.Schemaname == "" || rv.Schemaname == "pg_temp") && strings.HasPrefix(rv.Relname, ownTempPrefix) {
			msg = ownTempMsg(rv.Relname)
			return
		}
		if fdwCatalog[rv.Relname] && (rv.Schemaname == "" || rv.Schemaname == "pg_catalog" || rv.Schemaname == "information_schema") {
			msg = fmt.Sprintf("lokadb: %s describes the organizations' databases (addresses, credentials): not accessible through the proxy", rv.Relname)
			return
		}
		if sampledViews[rv.Relname] && (rv.Schemaname == "" || rv.Schemaname == "pg_catalog") {
			msg = fmt.Sprintf(sampledMsg, rv.Relname)
			return
		}
		if rv.Schemaname == "lokadb" {
			msg = fmt.Sprintf("lokadb: %s.%s belongs to LokaDB: not accessible through the proxy", rv.Schemaname, rv.Relname)
			return
		}
		if t := m.ownRelationIn(rv.Schemaname, rv.Relname); t != "" {
			msg = fmt.Sprintf("lokadb: %s is a partition or helper table of %s: use %s", rv.Relname, t, t)
		}
	})
	// its own walk only for a statement that names such a view: this runs on every statement (TestAnalyzeAllocs)
	if msg == "" && textView {
		if view := readsStatementText(n); view != "" {
			return fmt.Sprintf(statementTextMsg, view)
		}
	}
	return msg
}

var (
	ownServerRe = regexp.MustCompile(`^lokadb_org[0-9]+$`)
	// what enable and move-org name <table>__<suffix>; <column>_seq is checked separately
	ownSuffixRe = regexp.MustCompile(`^(cloud|refs|refs__.+|old|none|id_seq|o[0-9]+|o[0-9]+_source|org[0-9]+)$`)
)

// ownSuffixesPG: the suffixes of ownSuffixRe as a Postgres regular expression of a word
// (loadDeps looks for them in functions' sources).
const ownSuffixesPG = `cloud|refs|refs__\w+|old|none|id_seq|o[0-9]+|o[0-9]+_source|org[0-9]+`

func ownServer(name string) bool { return ownServerRe.MatchString(name) }

// ownTempPrefix: the objects the proxy makes in pg_temp for its own statements (block 2).
const ownTempPrefix = "__lokadb_"

// ownTempName: name, qualified by schema nodes (none: unqualified), is one of the proxy's.
func ownTempName(name string, schema []*pg.Node) bool {
	if !strings.HasPrefix(name, ownTempPrefix) {
		return false
	}
	return len(schema) == 0 || schema[len(schema)-1].GetString_().GetSval() == "pg_temp"
}

func schemaNodes(schema string) []*pg.Node {
	if schema == "" {
		return nil
	}
	return []*pg.Node{{Node: &pg.Node_String_{String_: &pg.String{Sval: schema}}}}
}

func ownTempMsg(name string) string {
	return fmt.Sprintf("lokadb: %s belongs to LokaDB (the proxy's own temporary object): not accessible through the proxy", name)
}

// fdwCatalog: the catalogs and views with the options of FDW servers and user mappings,
// which hold the organizations' addresses and the credentials of their roles.
var fdwCatalog = map[string]bool{
	"pg_foreign_server": true, "pg_user_mapping": true, "pg_user_mappings": true,
	"foreign_servers": true, "foreign_server_options": true, "user_mappings": true, "user_mapping_options": true,
	"_pg_foreign_servers": true, "_pg_user_mappings": true,
}

// What Postgres keeps of the rows by itself, whoever's they are: a session under one organization
// read another's values there (XT13). Only the views' readers are refused: tools that need them
// connect past the proxy. pg_stat_activity stays open without its statement text (a count of
// connections, a pid to cancel: monitoring, migrations).
const (
	sampledMsg       = "lokadb: %s shows values sampled from every organization's rows: not accessible through the proxy; connect to the cloud database directly"
	statementTextMsg = "lokadb: %s shows other sessions' statements with their values: not accessible through the proxy; name its other columns, or connect to the cloud database directly"
)

var (
	// sampledViews: the statistics, and the catalogs under them.
	sampledViews = map[string]bool{
		"pg_stats": true, "pg_stats_ext": true, "pg_stats_ext_exprs": true,
		"pg_statistic": true, "pg_statistic_ext_data": true,
	}
	// statementTextViews have a column query with the text of other sessions' statements.
	statementTextViews = map[string]bool{"pg_stat_activity": true, "pg_stat_statements": true}
	// statementTextFuncs return that text.
	statementTextFuncs = map[string]bool{"pg_stat_get_activity": true, "pg_stat_get_backend_activity": true, "pg_stat_statements": true}
	// stringQueryFuncs run a query, or read a relation, named in a string argument: the parser
	// sees no relation in it. The first argument is that string.
	stringQueryFuncs = map[string]bool{
		"query_to_xml": true, "query_to_xmlschema": true, "query_to_xml_and_xmlschema": true,
		"table_to_xml": true, "table_to_xmlschema": true, "table_to_xml_and_xmlschema": true,
		"ts_stat": true,
	}
	// wholeSchemaFuncs read every relation of a schema or of the database.
	wholeSchemaFuncs = map[string]bool{
		"schema_to_xml": true, "schema_to_xmlschema": true, "schema_to_xml_and_xmlschema": true,
		"database_to_xml": true, "database_to_xmlschema": true, "database_to_xml_and_xmlschema": true,
	}
)

// readsStatementText: the view whose statement text the statement reads, "" if none. A statement
// that names such a view reads the text with the column query, with * or with a whole row of the
// view (to_jsonb(a), (a).query), or under names it gave the columns itself.
func readsStatementText(n *pg.Node) string {
	view, names, renamed := "", map[string]bool{}, false
	walk(n.ProtoReflect(), func(msgp proto.Message) {
		rv, ok := msgp.(*pg.RangeVar)
		if !ok || !statementTextViews[rv.Relname] {
			return
		}
		view = rv.Relname
		names[rv.Relname] = true
		if rv.Alias != nil {
			names[rv.Alias.Aliasname] = true
			renamed = renamed || len(rv.Alias.Colnames) > 0
		}
	})
	if view == "" {
		return ""
	}
	reads := renamed
	walk(n.ProtoReflect(), func(msgp proto.Message) {
		cr, ok := msgp.(*pg.ColumnRef)
		if !ok || len(cr.Fields) == 0 {
			return
		}
		last := cr.Fields[len(cr.Fields)-1]
		name := last.GetString_().GetSval()
		if last.GetAStar() != nil || name == "query" || len(cr.Fields) == 1 && names[name] {
			reads = true
		}
	})
	if !reads {
		return ""
	}
	return view
}

// catalogFunc: why a call must not run: it returns the statements of other sessions, or reads
// through a string what the statement itself could not name.
func catalogFunc(fc *pg.FuncCall, m *Meta) string {
	if len(fc.Funcname) == 0 || len(fc.Funcname) > 1 && fc.Funcname[len(fc.Funcname)-2].GetString_().GetSval() != "pg_catalog" {
		// pg_stat_statements() is in the schema its extension was installed in
		if l := len(fc.Funcname); l == 0 || fc.Funcname[l-1].GetString_().GetSval() != "pg_stat_statements" {
			return ""
		}
	}
	name := fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()
	switch {
	case statementTextFuncs[name]:
		return fmt.Sprintf(statementTextMsg, name+"()")
	case wholeSchemaFuncs[name]:
		return fmt.Sprintf("lokadb: %s() reads every relation of a schema, the partitions of the organizations with them: not accessible through the proxy", name)
	case !stringQueryFuncs[name]:
		return ""
	}
	if len(fc.Args) == 0 {
		return ""
	}
	s := fc.Args[0].GetAConst().GetSval()
	if s == nil {
		return fmt.Sprintf("lokadb: %s() with a query that is not a constant cannot be checked: not accessible through the proxy", name)
	}
	if msg := namedInText(s.Sval, m); msg != "" {
		return fmt.Sprintf("lokadb: the query of %s() names %s", name, msg)
	}
	return ""
}

// namedInText: what a text the parser does not read (a DO block's body, a function's, a query in
// a string) names of what no statement may name, "" if nothing: said so that it follows "names".
// It looks at the words: a name put together while the text runs is not seen.
func namedInText(text string, m *Meta) string {
	running, reads := "", strings.Contains(text, "*")
	for _, w := range identRe.FindAllString(text, -1) {
		lw := strings.ToLower(w)
		switch {
		case sampledViews[lw]:
			return lw + ", which shows values sampled from every organization's rows"
		case statementTextFuncs[lw]:
			return lw + ", which shows other sessions' statements"
		case fdwCatalog[lw]:
			return lw + ", which describes the organizations' databases"
		case statementTextViews[lw]:
			running = lw
		case lw == "query":
			reads = true
		}
		for _, x := range []string{w, lw} {
			if t := m.ownRelationIn("", x); t != "" {
				return fmt.Sprintf("%s, a partition or helper table of %s: use %s", x, t, t)
			}
		}
	}
	if running != "" && reads {
		return running + ", with other sessions' statements"
	}
	return ""
}

// ownRelationIn: the LokaDB table whose partition or helper schema.name is, "" if it is not
// one. Unqualified: in public, or in the schema of a LokaDB table of another schema (the
// search path decides which; refused either way).
func (m *Meta) ownRelationIn(schema, name string) string {
	if schema != "" {
		return m.ownRelation(rel.RelKey(schema, name))
	}
	if t := m.ownRelation(name); t != "" {
		return t
	}
	for k := range m.Tables {
		if s, _ := rel.SplitKey(k); s != "public" {
			if t := m.ownRelation(rel.RelKey(s, name)); t != "" {
				return t
			}
		}
	}
	return ""
}

// ownInBody: why a DO block or a function (procedure) made through the proxy must not run: its
// body, a string the parser does not read, names a partition or helper table of a LokaDB table
// (the 2nd review of routing: SELECT * FROM documents__org2 is refused, the same statement in
// a DO block or in a function's body read organization 2's database). A body of SQL-standard
// form (BEGIN ATOMIC) is parsed: the walk of forbidden sees its relations. A function of the
// database made otherwise and naming one is in Meta.Funcs: the fanout policy decides.
func ownInBody(n *pg.Node, m *Meta) string {
	var args []*pg.Node
	what := ""
	switch {
	case n.GetDoStmt() != nil:
		args, what = n.GetDoStmt().Args, "a DO block"
	case n.GetCreateFunctionStmt() != nil:
		args, what = n.GetCreateFunctionStmt().Options, "the body of a function"
	default:
		return ""
	}
	for _, a := range args {
		de := a.GetDefElem()
		if de == nil || de.Defname != "as" || de.Arg == nil {
			continue
		}
		var bodies []string
		if l := de.Arg.GetList(); l != nil { // CREATE FUNCTION: AS 'body' (or 'file', 'symbol')
			for _, it := range l.Items {
				bodies = append(bodies, it.GetString_().GetSval())
			}
		} else {
			bodies = append(bodies, de.Arg.GetString_().GetSval()) // DO
		}
		for _, b := range bodies {
			if msg := namedInText(b, m); msg != "" {
				return fmt.Sprintf("lokadb: %s names %s", what, msg)
			}
		}
	}
	return ""
}

// ownRelation: the LokaDB table whose partition or helper the relation of key rel is, "" if
// it is not one (a helper is in its table's schema: its key is the table's with a suffix).
func (m *Meta) ownRelation(key string) string {
	for name, t := range m.Tables {
		suffix, ok := rel.HelperSuffix(name, key) // shortened past 63 bytes too
		if !ok {
			continue
		}
		suffix = strings.TrimPrefix(suffix, "__")
		if ownSuffixRe.MatchString(suffix) {
			return name
		}
		// an identity column's sequence (enable: <table>__<column>_seq)
		if col, ok := strings.CutSuffix(suffix, "_seq"); ok && slices.Contains(t.Cols, col) {
			return name
		}
	}
	return ""
}

// ownPolicy: why a statement on a policy of rv must not run (review 2 of routing): lokadb_*
// policies of a LokaDB table keep the organizations apart (lokadb_tenant; lokadb_owner, which
// enable gives the owner when row security was not forced), and the policies of a partition
// or helper table are LokaDB's. The application's own policies of the table are its own: the
// analysis keeps lokadb_tenant restrictive next to them (policyStmt).
func ownPolicy(rv *pg.RangeVar, m *Meta, names ...string) string {
	if rv == nil {
		return ""
	}
	if t := m.ownRelationIn(rv.Schemaname, rv.Relname); t != "" {
		return fmt.Sprintf("lokadb: %s is a partition or helper table of %s: its policies cannot be changed through the proxy", rv.Relname, t)
	}
	if t := m.table(rv); t != nil {
		for _, n := range names {
			if strings.HasPrefix(n, "lokadb_") {
				return fmt.Sprintf("lokadb: policy %s of %s belongs to LokaDB (it keeps each organization's rows apart): it cannot be created, changed or dropped through the proxy", n, t.Name)
			}
		}
	}
	return ""
}

// dropsOwn: why a DROP must not run, if it drops what LokaDB stands on.
func dropsOwn(st *pg.DropStmt, m *Meta) string {
	if st.RemoveType == pg.ObjectType_OBJECT_POLICY {
		for _, o := range st.Objects { // [schema,] table, policy
			if l := o.GetList(); l != nil && len(l.Items) >= 2 {
				it := l.Items
				rv := &pg.RangeVar{Relname: it[len(it)-2].GetString_().GetSval()}
				if len(it) >= 3 {
					rv.Schemaname = it[len(it)-3].GetString_().GetSval()
				}
				if msg := ownPolicy(rv, m, it[len(it)-1].GetString_().GetSval()); msg != "" {
					return msg
				}
			}
		}
	}
	for _, o := range st.Objects {
		name, schema := objName(o), objSchema(o)
		switch st.RemoveType {
		case pg.ObjectType_OBJECT_SCHEMA:
			if name == "lokadb" {
				return "lokadb: schema lokadb belongs to LokaDB: it cannot be dropped through the proxy"
			}
		case pg.ObjectType_OBJECT_FOREIGN_SERVER:
			if ownServer(name) {
				return fmt.Sprintf("lokadb: server %s belongs to LokaDB (an organization's database): it cannot be dropped through the proxy", name)
			}
		case pg.ObjectType_OBJECT_EXTENSION, pg.ObjectType_OBJECT_FDW:
			if name == "postgres_fdw" {
				return "lokadb: postgres_fdw carries the organizations' partitions: it cannot be dropped through the proxy"
			}
		case pg.ObjectType_OBJECT_TABLE, pg.ObjectType_OBJECT_FOREIGN_TABLE, pg.ObjectType_OBJECT_SEQUENCE:
			if schema == "lokadb" {
				return fmt.Sprintf("lokadb: lokadb.%s belongs to LokaDB: it cannot be dropped through the proxy", name)
			}
			if t := m.ownRelationIn(schema, name); t != "" {
				return fmt.Sprintf("lokadb: %s is a partition or helper table of %s: it cannot be dropped through the proxy", name, t)
			}
		}
	}
	return ""
}

// objName: the name a DROP or RENAME object node gives (the last part of a qualified name).
func objName(o *pg.Node) string {
	if o == nil {
		return ""
	}
	if s := o.GetString_(); s != nil {
		return s.GetSval()
	}
	if l := o.GetList(); l != nil && len(l.Items) > 0 {
		return l.Items[len(l.Items)-1].GetString_().GetSval()
	}
	return ""
}

// objSchema: the schema of a qualified object name, "" if unqualified.
func objSchema(o *pg.Node) string {
	if l := o.GetList(); l != nil && len(l.Items) > 1 {
		return l.Items[len(l.Items)-2].GetString_().GetSval()
	}
	return ""
}

// safeFuncs may appear in DDL replayed in organizations' databases (defaults, USING,
// index expressions): side-effect free, nothing outside the row.
var safeFuncs = map[string]bool{
	"now": true, "statement_timestamp": true, "transaction_timestamp": true, "clock_timestamp": true,
	"gen_random_uuid": true, "lower": true, "upper": true, "length": true, "char_length": true,
	"btrim": true, "ltrim": true, "rtrim": true, "md5": true, "date_trunc": true, "jsonb_build_object": true,
}

// unsafeExpr: why a DDL statement on a LokaDB table must not be replayed in organizations'
// databases, "" if it may. The replay runs with the cloud's role there, so an expression
// in it (a default, USING, an index expression) must not call arbitrary functions or read
// other tables.
func unsafeExpr(n *pg.Node) string {
	msg := ""
	walk(n.ProtoReflect(), func(msgp proto.Message) {
		if msg != "" {
			return
		}
		switch x := msgp.(type) {
		case *pg.SubLink:
			msg = "a subquery in a schema change is not replayed in organizations' databases"
		case *pg.FuncCall:
			var parts []string
			for _, p := range x.Funcname {
				parts = append(parts, p.GetString_().GetSval())
			}
			name := parts[len(parts)-1]
			if len(parts) > 2 || len(parts) == 2 && parts[0] != "pg_catalog" || !safeFuncs[name] {
				msg = fmt.Sprintf("%s() in a schema change is not replayed in organizations' databases (allowed: now(), gen_random_uuid(), lower(), upper() and similar)", strings.Join(parts, "."))
			}
		}
	})
	return msg
}
