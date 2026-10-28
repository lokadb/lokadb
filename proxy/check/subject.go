package check

import (
	"runtime/debug"
	"sort"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// subjectKinds: what the object of a finding names, by code, for findings made with add.
// Codes missing here have no subject (they are about the database as a whole), or are made
// with addOn / addFinding, which give their subject themselves.
var subjectKinds = map[string]struct{ kind, side string }{}

func init() {
	set := func(kind, side string, codes ...string) {
		for _, c := range codes {
			subjectKinds[c] = struct{ kind, side string }{kind, side}
		}
	}
	set("table", "", "spec_duplicate", "spec_no_organization", "spec_two_organizations", "spec_parent_column",
		"table_missing", "table_partitioned", "parent_not_in_spec", "spec_cycle", "parent_without_organization",
		"id_missing", "tenant_column_missing", "parent_column_missing", "orphan_rows", "orphan_rows_sampled",
		"materialized_view", "matview_stores_moved_rows", "dependents_repointed", "dependent_refused", "table_rule", "user_trigger", "triggers_carried",
		"rls_policy", "rls_enabled", "identity_always", "grants_carried",
		"large_objects", "name_conflict",
		"enable_prepare", "enable_prepare_stalled", "enable_prepare_done")
	set("column", "", "id_type", "tenant_column_type", "parent_column_type", "tenant_null", "tenant_nullable",
		"child_without_fk", "id_not_unique", "id_null", "id_unique_not_checked", "child_has_tenant_column", "id_unique_after", "unlinked_table")
	// constraints are made with their subject (fkSubject, uniqueSubject): the table is not in the object
	set("constraint", "")
	set("setting", "cloud", "cloud_wal_level")
	set("extension", "cloud", "cloud_postgres_fdw", "cloud_postgres_fdw_install")
	set("database", "customer", "customer_encoding", "customer_collation_differs", "customer_extension_unavailable",
		"customer_extension_privilege", "customer_extension_version", "customer_index_skipped", "customer_object_missing", "customer_creates",
		"customer_collation_missing", "customer_connected", "customer_version", "customer_version_older", "move_mode", "customer_contents",
		"customer_function_untrusted", "customer_function_security_definer")
	set("role", "customer", "customer_app_database", "customer_app_superuser", "customer_app_is_admin",
		"customer_app_owner", "customer_privileges", "customer_table_exists")
	set("schema", "customer", "customer_schema")
	set("table", "customer", "customer_previous_attempt")
	// block 4
	set("table", "", "fk_to_cloud_delete")
	set("column", "", "parent_polymorphic")
	set("role", "cloud", "app_role_bypasses_rls", "workload_own_role_only")
	set("database", "customer", "customer_collation_unavailable")
	set("file", "", "workload_empty", "recording_truncated")
	// block 10 (profile.go)
	set("table", "", "profile_insert_cloud_id", "profile_direct_needs_mirror")
	set("reason", "", "profile_fdw_reason", "profile_direct_unroutable", "profile_direct_refused_form")
	// block 18 (tenants.go; tenant_kind_mixed names no one object)
	set("column", "", "tenant_value_reserved")
}

// subjectOf reads the subject of a finding from its object, by the code (subjectKinds).
// Tables are of schema public unless the object names the schema (schema.table[.column]).
func subjectOf(code, object string) *Subject {
	k, ok := subjectKinds[code]
	if !ok || object == "" {
		return nil
	}
	s := &Subject{Kind: k.kind, Side: k.side}
	switch k.kind {
	case "table":
		s.Schema, s.Table = splitQual(object)
		if k.side != "" { // a table of the organization's database: its schema is not known here
			s.Schema = ""
			s.Table = object
		}
	case "column":
		p := identParts(object)
		switch {
		case len(p) == 2:
			s.Schema, s.Table, s.Column = "public", p[0], p[1]
		case len(p) >= 3: // a table's key and the column: the column is the last part
			s.Schema, s.Table, s.Column = p[0], strings.Join(p[1:len(p)-1], "."), p[len(p)-1]
		default:
			s.Column = object
		}
	case "constraint":
		s.Constraint = object
	case "setting":
		s.Setting = object
	case "extension":
		s.Extension = object
	case "database":
		s.Database = object
	case "role":
		s.Role = object
	case "schema":
		s.Schema = object
	case "file":
		s.Path = object
	case "reason":
		s.Reason = object
	}
	return s
}

// keyOf: the stable key of a finding, its code and the parts of its subject. A query
// pattern's key is patternKey.
func keyOf(code string, s *Subject) string {
	if s == nil {
		return code
	}
	qual := func(schema, name string) string {
		if schema == "" {
			return name
		}
		return schema + "." + name
	}
	var parts []string
	switch s.Kind {
	case "table":
		parts = []string{qual(s.Schema, s.Table)}
	case "column":
		parts = []string{qual(s.Schema, s.Table), s.Column}
	case "constraint":
		parts = []string{qual(s.Schema, s.Table), s.Constraint}
		if s.Table == "" {
			parts = parts[1:]
		}
	case "tables":
		parts = []string{strings.Join(s.CloudTables, ",") + "+" + strings.Join(s.LokadbTables, ",")}
	case "database":
		parts = []string{s.Side, s.Database}
	case "role":
		parts = []string{s.Side, s.Role}
	case "extension":
		parts = []string{s.Side, s.Extension}
	case "setting":
		parts = []string{s.Side, s.Setting}
	case "schema":
		parts = []string{s.Side, s.Schema}
	case "file":
		parts = []string{s.Path}
	case "reason":
		parts = []string{s.Reason}
	case "index":
		parts = []string{qual(s.Schema, s.Table), strings.Join(s.Columns, ",")}
	case "pattern", "transaction":
		parts = []string{s.Fingerprint}
	}
	return code + ":" + strings.Join(parts, ":")
}

// patternKey: the key of one query pattern of a finding, for -ci -baseline.
func patternKey(code string, p Pattern) string {
	if p.Fingerprint == "" {
		return code + ":sql:" + p.SQL
	}
	return code + ":fp:" + p.Fingerprint
}

// fanoutInfo: the fanout policy the report applied, with the allow list as fingerprints.
func (c *checker) fanoutInfo() *FanoutInfo {
	if c.fan == nil {
		return nil
	}
	f := &FanoutInfo{Mode: c.fan.Mode, Allow: []string{}, Source: "default"}
	if c.o.Proxy != nil {
		f.Source = c.o.ProxyFile
		p, ok := c.o.Proxy.Fanout[c.rep.Database]
		if !ok {
			p = c.o.Proxy.Fanout["*"]
		}
		for _, s := range p.Allow {
			if fp, err := pg.Fingerprint(s); err == nil {
				f.Allow = append(f.Allow, fp)
			}
		}
		sort.Strings(f.Allow)
	}
	return f
}

// buildVersion: the module version and VCS revision lokadb was built from, as far as known.
func buildVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	v := bi.Main.Version
	for _, s := range bi.Settings {
		if s.Key == "vcs.revision" && len(s.Value) >= 12 {
			v += " " + s.Value[:12]
		}
		if s.Key == "vcs.modified" && s.Value == "true" {
			v += "+dirty"
		}
	}
	return v
}

// lokadbTables: the LokaDB tables a statement touches, sorted.
func (c *checker) lokadbTables(sql string) []string {
	tree, err := pg.Parse(analyzable(sql))
	if err != nil {
		return nil
	}
	found := map[string]bool{}
	for _, raw := range tree.Stmts {
		proxy.Walk(raw, func(m proto.Message) {
			if rv, ok := m.(*pg.RangeVar); ok && c.lokadbRel(rv) {
				found[c.meta.TableIn(rv, c.path).Name] = true
			}
		})
	}
	return keys(found)
}

// splitQual: "t" is table t of schema public, "s.t" is table t of schema s (as the catalog queries print it).
func splitQual(name string) (schema, table string) {
	switch p := identParts(name); {
	case len(p) == 1:
		return "public", p[0]
	case len(p) >= 2:
		// a key (package rel): the name after its schema as it is, dots and all
		return p[0], strings.Join(p[1:], ".")
	}
	return "public", name
}

// identParts splits a dotted name into its parts. A part in double quotes (as regclass and
// regnamespace print a name that needs them) may hold dots and doubled quotes, and is unquoted.
func identParts(name string) []string {
	var parts []string
	var b strings.Builder
	quoted := false
	for i := 0; i < len(name); i++ {
		switch ch := name[i]; {
		case ch == '"' && quoted && i+1 < len(name) && name[i+1] == '"':
			b.WriteByte('"')
			i++
		case ch == '"':
			quoted = !quoted
		case ch == '.' && !quoted:
			parts = append(parts, b.String())
			b.Reset()
		default:
			b.WriteByte(ch)
		}
	}
	return append(parts, b.String())
}

// fkSubject: the subject of a finding about a foreign key.
func fkSubject(name, src string, cols []string, dst string, refcols []string) *Subject {
	s := &Subject{Kind: "constraint", Constraint: name, Columns: cols, RefColumns: refcols}
	s.Schema, s.Table = splitQual(src)
	s.RefSchema, s.RefTable = splitQual(dst)
	return s
}

// onPartitions: an FK finding of a partitioned table's root, made from the FKs its partitions
// declare (not inherited from the root). One partition: the subject is that partition, with
// partition_of. Several: the subject stays the root and names them in partitions.
func onPartitions(s *Subject, parts []string) []string {
	if len(parts) == 0 {
		return nil
	}
	root := s.Table
	if s.Schema != "public" {
		root = s.Schema + "." + s.Table
	}
	if len(parts) == 1 {
		s.Schema, s.Table = splitQual(parts[0])
		s.PartitionOf = root
		return nil
	}
	for _, p := range parts {
		_, t := splitQual(p)
		s.Partitions = append(s.Partitions, t)
	}
	return parts
}

// uniqueSubject: the subject of a finding about a unique constraint or index of table t.
func uniqueSubject(t, name, columns string) *Subject {
	s := &Subject{Kind: "constraint", Constraint: name}
	s.Schema, s.Table = splitQual(t)
	if columns != "" {
		s.Columns = strings.Split(columns, ",")
	}
	return s
}
