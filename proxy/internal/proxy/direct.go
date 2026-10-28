package proxy

import (
	"slices"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"
)

// The direct path (profiles fdw+direct and direct, tasks/routing-declarations.md 7.2-7.3):
// a statement of one moved organization goes straight to its database instead of through
// postgres_fdw. A wrong choice there reads another database silently, so the test is
// stricter than the FDW path's: every LokaDB table of the statement must be confined to the
// organization (or the application names it, lokadb.tenant), and nothing else may be read.

// Reasons a statement does not go the direct path.
const (
	DirectNoTables   = "no_tables"            // reads no table at all (SET, SELECT 1): goes where the transaction is
	DirectCloudOnly  = "cloud_only"           // only cloud tables: the cloud
	DirectCloudTable = "cloud_table"          // LokaDB tables together with cloud tables (a JOIN, a subquery)
	DirectNoOrg      = "no_organization"      // no LokaDB table of it names the organization (fanout)
	DirectPartialOrg = "organization_partial" // some LokaDB tables name it, others do not
	DirectForm       = "unsupported_form"     // MERGE, INSERT ... SELECT and other forms the analyzer cannot confine
	DirectFunction   = "function"             // a function or type off directFuncs/directTypes: it may read cloud tables, or answer otherwise there
	DirectSequence   = "sequence"             // nextval/currval/setval: sequences live in the cloud
	DirectDDL        = "ddl"                  // DDL: the cloud runs it and the proxy replays it
	DirectRefused    = "refused"              // the proxy refuses it anyway (LokaDB's own objects, unparsable)
)

// DirectVerdict: may a statement go straight to an organization's database.
type DirectVerdict struct {
	OK     bool
	Reason string   // why not ("" when OK)
	Lokadb []string // LokaDB tables it reads or writes
	Cloud  []string // other tables it reads or writes (catalogs included)
	// ByValues: the organization comes from a list of values (IN, ANY, several VALUES rows):
	// direct only when they all belong to one organization, which the proxy sees at run time.
	ByValues bool
	// Inserts: LokaDB tables it inserts into (the new rows' ids, __refs).
	Inserts []string
}

var sequenceFuncs = map[string]bool{"nextval": true, "currval": true, "setval": true, "lastval": true}

// directFuncs: the functions a statement on the direct path may call (review of block 9,
// 24.10). Not "all of pg_catalog": current_database(), pg_backend_pid(), txid_current(),
// pg_notify(), pg_advisory_*(), set_config(), current_setting(), pg_sleep() and the like
// answer otherwise in the organization's database or act on the wrong server. These compute
// from their arguments alone (aggregates, strings, numbers, dates, JSON, arrays, window
// functions); now() and its kin read the organization's server clock (README).
var directFuncs = toSet(
	// aggregates and window functions
	"count", "sum", "avg", "min", "max", "bool_and", "bool_or", "every", "array_agg", "string_agg",
	"json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg", "stddev", "variance",
	"row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile", "lag", "lead", "first_value", "last_value", "nth_value",
	// strings
	"lower", "upper", "initcap", "length", "char_length", "character_length", "octet_length", "substr", "substring",
	"trim", "btrim", "ltrim", "rtrim", "replace", "concat", "concat_ws", "left", "right", "lpad", "rpad", "repeat",
	"reverse", "position", "strpos", "split_part", "starts_with", "format", "md5", "encode", "decode", "translate",
	"regexp_replace", "regexp_match", "regexp_matches", "regexp_split_to_array", "regexp_like", "string_to_array", "array_to_string",
	"quote_ident", "quote_literal", "quote_nullable", "to_hex", "ascii", "chr",
	// numbers
	"abs", "round", "trunc", "ceil", "ceiling", "floor", "mod", "power", "sqrt", "sign", "div",
	// dates
	"now", "date_trunc", "date_part", "extract", "age", "to_char", "to_date", "to_timestamp", "to_number", "make_date",
	"make_timestamp", "make_timestamptz", "make_interval", "date_bin", "justify_days", "justify_hours", "justify_interval", "timezone",
	"statement_timestamp", "transaction_timestamp", "clock_timestamp", "isfinite",
	// JSON and arrays
	"json_build_object", "jsonb_build_object", "json_build_array", "jsonb_build_array", "to_json", "to_jsonb", "row_to_json",
	"jsonb_set", "jsonb_insert", "jsonb_strip_nulls", "jsonb_pretty", "json_extract_path", "json_extract_path_text",
	"jsonb_extract_path", "jsonb_extract_path_text", "jsonb_array_length", "json_array_length", "jsonb_typeof", "json_typeof",
	"jsonb_array_elements", "jsonb_array_elements_text", "json_array_elements", "json_array_elements_text", "jsonb_each", "jsonb_each_text",
	"jsonb_object_keys", "jsonb_path_exists", "jsonb_path_query", "jsonb_path_query_first", "jsonb_path_match",
	"array_length", "array_position", "array_positions", "array_append", "array_prepend", "array_cat", "array_remove",
	"array_replace", "cardinality", "unnest", "generate_series", "generate_subscripts",
	// full text
	"to_tsvector", "to_tsquery", "plainto_tsquery", "phraseto_tsquery", "websearch_to_tsquery", "ts_rank", "ts_rank_cd", "ts_headline",
	// misc
	"gen_random_uuid", "num_nulls", "num_nonnulls",
)

// directTypes: the types a statement on the direct path may cast to by a name of its own
// (the parser writes the SQL standard ones as pg_catalog.int4 and so on, taken as well, but
// not the reg* types, which resolve names in the database at hand). A type of the
// application's (an enum, a domain) has another OID there.
var directTypes = toSet("text", "varchar", "char", "bpchar", "name", "int", "int2", "int4", "int8", "smallint", "integer", "bigint",
	"numeric", "decimal", "float4", "float8", "real", "bool", "boolean", "date", "time", "timetz", "timestamp", "timestamptz",
	"interval", "uuid", "json", "jsonb", "bytea", "inet", "cidr", "macaddr", "money", "bit", "varbit", "xml", "tsvector", "tsquery")

func toSet(xs ...string) map[string]bool {
	m := make(map[string]bool, len(xs))
	for _, x := range xs {
		m[x] = true
	}
	return m
}

// directType: a cast the direct path takes.
func directType(t *pg.TypeName) bool {
	if t == nil || len(t.Names) == 0 {
		return true
	}
	var names []string
	for _, n := range t.Names {
		names = append(names, n.GetString_().GetSval())
	}
	last := strings.ToLower(names[len(names)-1])
	switch {
	case strings.HasPrefix(last, "reg"):
		return false
	case len(names) == 2 && names[0] == "pg_catalog":
		return true
	case len(names) == 1:
		return directTypes[last]
	}
	return false
}

// Direct decides whether sql may go the direct path. named: the application named the
// organization (lokadb.tenant), so the statement's own conditions need not confine it.
func Direct(sql string, m *Meta, named bool) DirectVerdict {
	return DirectIn(sql, m, named, defaultPath)
}

// DirectIn is Direct with the bare names resolved by search path path (schemas.go, block 4;
// nil: not known, a name of several schemas is not direct).
func DirectIn(sql string, m *Meta, named bool, path []string) DirectVerdict {
	return directInWith(sql, analyzeIn(sql, m, path), m, named, path)
}

// directInWith is DirectIn with sql's analysis by path given (the session's, analysed once:
// review of performance, PF8).
func directInWith(sql string, a *Analysis, m *Meta, named bool, path []string) DirectVerdict {
	if v, done := directRefusedBy(a); done {
		return v
	}
	tree, _ := pg.Parse(sql) // analyze parsed it
	return directInTree(tree, a, m, named, path)
}

// directRefusedBy: the verdict a's analysis alone gives, before any parse (done).
func directRefusedBy(a *Analysis) (v DirectVerdict, done bool) {
	switch {
	case a.ParseError, a.Forbidden != "":
		v.Reason = DirectRefused
		return v, true
	case len(a.DDL) > 0 || a.DDLError != "":
		v.Reason = DirectDDL
		return v, true
	}
	return v, false
}

// directInTree is directInWith on sql's parse tree (directShape parses once for both: second
// review of performance, F9). The tree's bare names get their schemas (qualify).
func directInTree(tree *pg.ParseResult, a *Analysis, m *Meta, named bool, path []string) DirectVerdict {
	var v DirectVerdict
	if v, done := directRefusedBy(a); done {
		return v
	}
	for _, raw := range tree.Stmts {
		if qualify(raw.Stmt, m, path) != "" {
			v.Reason = DirectRefused // the proxy cannot tell its table (analyze refuses it too)
			return v
		}
	}
	ctes := map[string]bool{}
	lokadb, cloud, inserts := map[string]bool{}, map[string]bool{}, map[string]bool{}
	form, function, sequence := false, false, false
	lockNames := map[*pg.RangeVar]bool{} // FOR UPDATE OF d: an alias or a name of the FROM list, not a table of its own
	for _, raw := range tree.Stmts {
		Walk(raw, func(msg proto.Message) {
			switch s := msg.(type) {
			case *pg.CommonTableExpr:
				ctes[s.Ctename] = true
			case *pg.LockingClause:
				for _, n := range s.LockedRels {
					lockNames[n.GetRangeVar()] = true
				}
			case *pg.MergeStmt:
				form = true
			case *pg.InsertStmt:
				if t := m.table(s.Relation); t != nil {
					inserts[t.Name] = true
				}
			case *pg.FuncCall:
				var names []string
				for _, n := range s.Funcname {
					names = append(names, n.GetString_().GetSval())
				}
				last := ""
				if len(names) > 0 {
					last = strings.ToLower(names[len(names)-1])
				}
				switch {
				case sequenceFuncs[last]:
					sequence = true
				case len(names) > 2 || len(names) == 2 && names[0] != "pg_catalog" || !directFuncs[last]:
					function = true
				}
			case *pg.SQLValueFunction:
				switch s.Op {
				case pg.SQLValueFunctionOp_SVFOP_CURRENT_DATE, pg.SQLValueFunctionOp_SVFOP_CURRENT_TIME, pg.SQLValueFunctionOp_SVFOP_CURRENT_TIME_N,
					pg.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP, pg.SQLValueFunctionOp_SVFOP_CURRENT_TIMESTAMP_N,
					pg.SQLValueFunctionOp_SVFOP_LOCALTIME, pg.SQLValueFunctionOp_SVFOP_LOCALTIME_N,
					pg.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP, pg.SQLValueFunctionOp_SVFOP_LOCALTIMESTAMP_N:
				default: // current_user, session_user, current_role, user, current_catalog, current_schema
					function = true
				}
			case *pg.TypeCast:
				if !directType(s.TypeName) {
					function = true
				}
			}
		})
	}
	for _, raw := range tree.Stmts {
		Walk(raw, func(msg proto.Message) {
			rv, ok := msg.(*pg.RangeVar)
			if !ok || rv.Schemaname == "" && ctes[rv.Relname] || lockNames[rv] {
				return
			}
			if t := m.table(rv); t != nil {
				lokadb[t.Name] = true
			} else if rv.Schemaname != "" {
				cloud[rv.Schemaname+"."+rv.Relname] = true
			} else {
				cloud[rv.Relname] = true
			}
		})
	}
	v.Lokadb, v.Cloud, v.Inserts = sortedKeys(lokadb), sortedKeys(cloud), sortedKeys(inserts)
	switch {
	case len(lokadb) == 0 && len(cloud) == 0:
		v.Reason = DirectNoTables
	case len(lokadb) == 0:
		v.Reason = DirectCloudOnly
	case len(cloud) > 0:
		v.Reason = DirectCloudTable
	case function:
		v.Reason = DirectFunction
	case sequence:
		v.Reason = DirectSequence
	case named:
		v.OK = true
	case form:
		v.Reason = DirectForm
	default:
		covered, total, byValues := 0, 0, false
		for _, raw := range tree.Stmts {
			c, t, bv, unsupported := confined(raw.Stmt, m)
			covered, total, byValues = covered+c, total+t, byValues || bv
			form = form || unsupported
		}
		switch {
		case form:
			v.Reason = DirectForm
		case covered == 0:
			v.Reason = DirectNoOrg
		case covered < total:
			v.Reason = DirectPartialOrg
		default:
			v.OK, v.ByValues = true, byValues
		}
	}
	return v
}

// confined counts the LokaDB relations of a statement (one per FROM item, INSERT target) and
// how many of them its conditions confine to an organization: a condition on a lookup column
// (the organization column, id, a FK) of that very relation, or an equality with a confined
// relation's column that maps through the same lookup (`m.conversation_id = c.id`: both
// through conversations__refs, so both rows belong to one organization). unsupported: an
// INSERT whose rows the analyzer cannot read (INSERT ... SELECT).
func confined(n *pg.Node, m *Meta) (covered, total int, byValues, unsupported bool) {
	Walk(n, func(msg proto.Message) {
		var rels []*pg.Node
		var where *pg.Node
		var target *pg.RangeVar
		switch s := msg.(type) {
		case *pg.SelectStmt:
			rels, where = s.FromClause, s.WhereClause
		case *pg.UpdateStmt:
			rels, where, target = s.FromClause, s.WhereClause, s.Relation
		case *pg.DeleteStmt:
			rels, where, target = s.UsingClause, s.WhereClause, s.Relation
		case *pg.InsertStmt:
			t := m.table(s.Relation)
			if t == nil {
				return
			}
			total++
			a := &Analysis{}
			a.insertHints(s, t, m)
			if a.Unhinted {
				if sel := s.SelectStmt.GetSelectStmt(); sel != nil && len(sel.ValuesLists) == 0 && unnestArgs(sel) == nil {
					unsupported = true
				}
				return
			}
			covered++
			for _, h := range a.Hints {
				byValues = byValues || len(h.Values) > 1 || len(h.Values) == 1 && h.Values[0].Array
			}
			return
		default:
			return
		}
		ds := &directScope{m: m, rels: map[string]*Table{}}
		ds.add(target)
		quals := conjuncts(where)
		for _, r := range rels {
			quals = append(quals, ds.collect(r)...)
		}
		c, bv := ds.confined(quals)
		covered, total, byValues = covered+c, total+len(ds.order), byValues || bv
	})
	return
}

// directScope: the relations of one SELECT/UPDATE/DELETE level by alias. Unlike the FDW
// path's scope it keeps each alias apart: `documents a JOIN documents b` are two relations,
// each of which has to be confined.
type directScope struct {
	m     *Meta
	rels  map[string]*Table // alias -> LokaDB table, nil for other relations
	order []string          // aliases of LokaDB relations
}

func (ds *directScope) add(rv *pg.RangeVar) {
	if rv == nil {
		return
	}
	key := rv.Relname
	if rv.Alias != nil && rv.Alias.Aliasname != "" {
		key = rv.Alias.Aliasname
	}
	t := ds.m.table(rv)
	ds.rels[key] = t
	if t != nil {
		ds.order = append(ds.order, key)
	}
}

// collect adds the relations of a FROM item and returns the ON conditions of its inner
// joins, which confine rows like WHERE does (the ON of an outer join does not).
func (ds *directScope) collect(n *pg.Node) []*pg.Node {
	switch {
	case n.GetRangeVar() != nil:
		ds.add(n.GetRangeVar())
	case n.GetJoinExpr() != nil:
		j := n.GetJoinExpr()
		out := append(ds.collect(j.Larg), ds.collect(j.Rarg)...)
		if j.Jointype == pg.JoinType_JOIN_INNER {
			out = append(out, conjuncts(j.Quals)...)
		}
		return out
	}
	return nil
}

// column resolves a column reference to (alias, column) among the LokaDB relations.
func (ds *directScope) column(fields []string) (string, string, bool) {
	switch len(fields) {
	case 2, 3:
		alias := fields[len(fields)-2]
		if ds.rels[alias] != nil {
			return alias, fields[len(fields)-1], true
		}
	case 1:
		found := ""
		for _, alias := range ds.order {
			if _, ok := ds.rels[alias].ColType[fields[0]]; ok {
				if found != "" {
					return "", "", false // ambiguous: Postgres refuses it too
				}
				found = alias
			}
		}
		if found != "" {
			return found, fields[0], true
		}
	}
	return "", "", false
}

func (ds *directScope) confined(quals []*pg.Node) (int, bool) {
	ok := map[string]bool{}
	byValues := false
	type eq struct{ a, ac, b, bc string }
	var eqs []eq
	// bound[alias][col]: how many values a condition binds the column to (0: an array), for a
	// lookup of several columns (a composite key, tuples.go): it confines when every one of its
	// columns is bound; rowCols[alias]: columns bound together by (a, b) IN ((…), …)
	bound := map[string]map[string]int{}
	rowCols := map[string][][]string{}
	bind := func(alias, col string, n int) {
		if bound[alias] == nil {
			bound[alias] = map[string]int{}
		}
		bound[alias][col] = n
	}
	for _, c := range quals {
		if fields, vals, isHint := hintExpr(c); isHint {
			if alias, col, found := ds.column(fields); found {
				n := len(vals)
				if n == 1 && vals[0].Array {
					n = 0
				}
				bind(alias, col, n)
				if _, lookup := ds.m.Lookups[ds.rels[alias].Name][col]; lookup {
					ok[alias] = true
					byValues = byValues || len(vals) > 1 || len(vals) == 1 && vals[0].Array
				}
			}
			continue
		}
		if cols, rows, isRow := rowExpr(c); isRow {
			alias0, names, same := "", []string(nil), true
			for i, f := range cols {
				alias, col, found := ds.column(f)
				same = same && found && (i == 0 || alias == alias0)
				alias0, names = alias, append(names, col)
			}
			if same && len(rows) == 1 {
				for _, col := range names {
					bind(alias0, col, 1)
				}
			} else if same {
				rowCols[alias0] = append(rowCols[alias0], names)
				byValues = true
			}
			continue
		}
		e := c.GetAExpr()
		if e == nil || e.Kind != pg.A_Expr_Kind_AEXPR_OP || len(e.Name) != 1 || e.Name[0].GetString_().GetSval() != "=" {
			continue
		}
		lf, lok := colRef(e.Lexpr)
		rf, rok := colRef(e.Rexpr)
		if !lok || !rok {
			continue
		}
		a, ac, aok := ds.column(lf)
		b, bc, bok := ds.column(rf)
		if aok && bok && a != b {
			eqs = append(eqs, eq{a, ac, b, bc})
		}
	}
	// a lookup of several columns with every column bound (by values, or by a row of them)
	for _, alias := range ds.order {
		for _, g := range ds.m.tupleGroups(ds.rels[alias].Name) {
			all, multi := true, false
			for _, col := range g {
				k, there := bound[alias][col]
				all, multi = all && there, multi || k != 1 // several values, or an array
			}
			for _, rc := range rowCols[alias] {
				covers := true
				for _, col := range g {
					covers = covers && slices.Contains(rc, col)
				}
				all = all || covers
			}
			if all {
				ok[alias] = true
				byValues = byValues || multi || len(rowCols[alias]) > 0
			}
		}
	}
	// spread along equalities of columns looked up the same way, until nothing changes
	same := func(a, ac, b, bc string) bool {
		ra, okA := ds.m.Lookups[ds.rels[a].Name][ac]
		rb, okB := ds.m.Lookups[ds.rels[b].Name][bc]
		return okA && okB && ra == rb
	}
	for changed := true; changed; {
		changed = false
		for _, e := range eqs {
			if ok[e.a] != ok[e.b] && same(e.a, e.ac, e.b, e.bc) {
				ok[e.a], ok[e.b], changed = true, true, true
			}
		}
	}
	n := 0
	for _, alias := range ds.order {
		if ok[alias] {
			n++
		}
	}
	return n, byValues
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
