package check

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"sort"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Suggestion is a routing declaration the check proposes for patterns that name no
// organization (tasks/routing-declarations.md, section 4). It is a guess from names: the
// data check comes with it, and the owner decides. It never changes the verdict.
type Suggestion struct {
	Kind      string               `json:"kind"`    // relation
	Routing   admin.Routing        `json:"routing"` // the spec fragment ("routing" of the spec)
	DataCheck *admin.RelationCheck `json:"data_check,omitempty"`
	KeyCheck  *KeyCheck            `json:"key_check,omitempty"` // kind key
	// Patterns: the fingerprints of the patterns the declaration routes; Calls, their executions.
	Patterns []string `json:"patterns"`
	Calls    int64    `json:"calls"`
}

// KeyCheck: what the data says about a key: Rows rows, Nulls of them without a value,
// Duplicates values used more than once (enable refuses the key then).
type KeyCheck struct {
	Rows       int64 `json:"rows"`
	Nulls      int64 `json:"nulls"`
	Duplicates int64 `json:"duplicate_values"`
}

func (k KeyCheck) String() string {
	return fmt.Sprintf("%d rows, %d without a value, %d values used more than once", k.Rows, k.Nulls, k.Duplicates)
}

// tableKey: the column the refs of a LokaDB table map (its primary key; id when not recorded).
func tableKey(t *proxy.Table) string {
	if t == nil || t.Key == "" {
		return "id"
	}
	return t.Key
}

// enabled: the metadata is the one enable recorded, not built from a spec.
func (c *checker) enabled() bool { return len(c.spec.Tables) == 0 }

// viewsAndFuncs: before enable the routing metadata is built from the spec; the views and
// functions over its tables come from the catalog, as the proxy reads them after enable.
func (c *checker) viewsAndFuncs() {
	if c.enabled() || c.meta == nil || c.tx == nil {
		return
	}
	sp, err := c.tx.Begin(c.ctx)
	if err != nil {
		return
	}
	if err := proxy.LoadDeps(func(sql string) ([][]string, error) { return rowsOf(c.ctx, sp, sql) }, c.meta); err != nil {
		_ = sp.Rollback(c.ctx)
		c.add(Warning, "views_not_checked", "", "views and functions over the LokaDB tables could not be read: "+firstLine(err.Error()), nil)
		return
	}
	_ = sp.Commit(c.ctx)
}

// relations applies the spec's relations and -patch to the routing metadata the workload is
// judged with, and checks them on the data as enable would.
func (c *checker) relations() {
	declared := admin.Spec{}
	if !c.enabled() {
		declared.Merge(c.spec.Routing)
	}
	if c.o.Patch != nil {
		declared.Merge(c.o.Patch)
		c.add(Info, "routing_patch", "", fmt.Sprintf("the workload is judged as if the patch were declared: %s, %s",
			plural(len(c.o.Patch.Relations), "relation", "relations"), plural(len(c.o.Patch.CloudTables), "cloud table", "cloud tables")), nil)
	}
	if c.meta == nil {
		return
	}
	if !c.enabled() { // the organizations table is a cloud table whose organization column is its id
		if implied, err := admin.ImpliedCloudTables(c.catalogRows, c.spec); err == nil && len(implied) > 0 {
			declared.Routing = ensureRouting(declared.Routing)
			declared.Routing.CloudTables = append(declared.Routing.CloudTables, implied...)
		}
	}
	if declared.Routing == nil {
		return
	}
	for _, ct := range declared.Routing.CloudTables {
		c.cloudTable(ct)
	}
	for _, k := range declared.Routing.Keys {
		c.key(k)
	}
	rels := declared.Routing.Relations
	covered := map[string]bool{}
	for _, rel := range rels {
		src, col, dst, key, err := rel.Parse()
		if err == nil {
			switch {
			case c.meta.Tables[src] == nil || c.meta.Tables[dst] == nil:
				err = fmt.Errorf("relation %s -> %s: both tables must be LokaDB tables (in the spec)", rel.From, rel.To)
			case key != tableKey(c.meta.Tables[dst]):
				err = fmt.Errorf("relation %s -> %s: the organization of %s rows is found by its key %s.%s, not by %s", rel.From, rel.To, dst, dst, tableKey(c.meta.Tables[dst]), rel.To)
			case c.meta.Tables[src].ColType[col] == "":
				err = fmt.Errorf("relation %s -> %s: %s has no column %s", rel.From, rel.To, src, col)
			case c.meta.Tables[src].ColType[admin.PolymorphicType(col)] != "":
				err = fmt.Errorf("relation %s -> %s: %s.%s says which table %s points at (a polymorphic reference): its ids of other tables would find their organization in %s, and enable refuses it. lokadb does not take a relation with a condition on the type yet",
					rel.From, rel.To, src, admin.PolymorphicType(col), col, dst)
			}
		}
		if err != nil {
			c.add(SpecError, "routing_invalid", rel.From, err.Error(), nil)
			continue
		}
		if c.meta.Lookups[src] == nil {
			c.meta.Lookups[src] = map[string]string{}
		}
		c.meta.Lookups[src][col] = admin.Helper(dst, "__refs")
		covered[src+"."+col] = true
		chk, err := c.checkRelation(src, col, dst)
		switch {
		case err != nil:
			c.add(Warning, "relation_not_checked", rel.From, fmt.Sprintf("relation %s -> %s could not be checked on the data: %s", rel.From, rel.To, firstLine(err.Error())), nil)
		case !chk.Clean() && !rel.Confirmed:
			c.add(Blocker, "relation_unverified", rel.From, fmt.Sprintf("relation %s -> %s: %s. Statements by %s would miss those rows, so enable refuses the relation: fix the data, or confirm it (\"confirmed\": true) if that is acceptable",
				rel.From, rel.To, chk, rel.From), nil)
		default:
			c.add(Info, "relation_declared", rel.From, fmt.Sprintf("relation %s -> %s: the proxy finds the organization through %s; data: %s", rel.From, rel.To, dst, chk), nil)
		}
	}
	// a declared relation is the FK the schema check missed
	kept := c.rep.Findings[:0]
	for _, f := range c.rep.Findings {
		if f.Code != "child_without_fk" || !covered[f.Object] {
			kept = append(kept, f)
		}
	}
	c.rep.Findings = kept
}

// key applies a declared key to the routing metadata and checks it on the data as enable would.
func (c *checker) key(k admin.Key) {
	t := c.meta.Tables[k.Table]
	switch {
	case t == nil:
		c.add(SpecError, "routing_invalid", k.Table, fmt.Sprintf("key %s.%s: %s is not a LokaDB table (in the spec)", k.Table, k.Column, k.Table), nil)
		return
	case t.ColType[k.Column] == "":
		c.add(SpecError, "routing_invalid", k.Table+"."+k.Column, fmt.Sprintf("key %s.%s: %s has no column %s", k.Table, k.Column, k.Table, k.Column), nil)
		return
	}
	if err := admin.KeyMapName(k); err != nil { // enable refuses it (admin.applyKeys)
		c.add(SpecError, "routing_invalid", k.Table+"."+k.Column, err.Error()+": enable refuses it; rename the table or the column", nil)
		return
	}
	c.meta.Lookups[k.Table][k.Column] = "lokadb." + admin.KeyTable(k.Table, k.Column)
	chk, err := c.checkKey(k.Table, k.Column)
	switch {
	case err != nil:
		c.add(Warning, "key_not_checked", k.Table+"."+k.Column, fmt.Sprintf("key %s.%s could not be checked on the data: %s", k.Table, k.Column, firstLine(err.Error())), nil)
	case chk.Duplicates > 0:
		c.add(Blocker, "key_not_unique", k.Table+"."+k.Column, fmt.Sprintf("key %s.%s: %s. A key must be unique across the whole table: enable refuses it", k.Table, k.Column, chk), nil)
	default:
		c.add(Info, "key_declared", k.Table+"."+k.Column, fmt.Sprintf("key %s.%s: the proxy finds the organization by its value; data: %s", k.Table, k.Column, chk), nil)
	}
}

// checkKey counts a key's rows, NULLs and duplicate values.
func (c *checker) checkKey(table, col string) (KeyCheck, error) {
	var k KeyCheck
	if c.tx == nil {
		return k, ErrNoDatabase
	}
	rows, err := c.heavyRows(fmt.Sprintf(`SELECT count(*)::text, (count(*) - count(%s))::text,
		(SELECT count(*) FROM (SELECT 1 FROM %s WHERE %s IS NOT NULL GROUP BY %s HAVING count(*) > 1) d)::text FROM %s`,
		q(col), qk(table), q(col), q(col), qk(table)))
	if err != nil {
		return k, err
	}
	_, err = fmt.Sscan(rows[0][0]+" "+rows[0][1]+" "+rows[0][2], &k.Rows, &k.Nulls, &k.Duplicates)
	return k, err
}

// uniqueColumns: the columns of a table (or of its partitions, after enable) that a unique
// index of their own covers.
func (c *checker) uniqueColumns(table string) map[string]bool {
	out := map[string]bool{}
	if c.tx == nil {
		return out
	}
	rows, err := c.rows(`SELECT DISTINCT a.attname::text FROM pg_index i
		JOIN pg_class t ON t.oid = i.indrelid
		LEFT JOIN pg_inherits h ON h.inhrelid = t.oid
		LEFT JOIN pg_class p ON p.oid = h.inhparent
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indisunique AND i.indnkeyatts = 1 AND i.indpred IS NULL
		  AND (`+relKeySQL("t")+` = $1 OR `+relKeySQL("p")+` = $1)`, table)
	if err != nil {
		return out
	}
	for _, r := range rows {
		out[r[0]] = true
	}
	return out
}

// cloudTable applies a declared cloud table to the routing metadata, after checking its
// columns as enable would.
func (c *checker) cloudTable(ct admin.CloudTable) {
	if c.meta.Tables[ct.Table] != nil {
		c.add(SpecError, "routing_invalid", ct.Table, fmt.Sprintf("cloud table %s: it is a LokaDB table", ct.Table), nil)
		return
	}
	if c.tx != nil {
		cols, err := c.cloudColumns(ct.Table)
		// the organizations' kind, as enable takes it (admin.routing: dbKind)
		kind := c.meta.TenantKind
		if kind == "" {
			kind = admin.TenantInt
		}
		if err == nil && (cols["id"] == "" || admin.TenantOfKey(cols[ct.TenantColumn]) != kind) {
			c.add(SpecError, "routing_invalid", ct.Table, fmt.Sprintf("cloud table %s: it needs a column id of type %s and a column %s of the organizations' kind (%s)", ct.Table, admin.KeyTypes, ct.TenantColumn, kind), nil)
			return
		}
	}
	m := withCloud(c.meta, ct.Table, ct.TenantColumn)
	c.meta.Lookups, c.meta.Cloud = m.Lookups, m.Cloud
	by := fmt.Sprintf("%s.%s or %s.id", ct.Table, ct.TenantColumn, ct.Table)
	if ct.TenantColumn == "id" {
		by = ct.Table + ".id (the organizations table)"
	}
	c.add(Info, "cloud_table_declared", ct.Table, fmt.Sprintf("cloud table %s: statements joining it to LokaDB tables name the organization by %s", ct.Table, by), nil)
}

// cloudColumns: the columns of a table (by key) whose type lokadb takes as a key, with their
// kind (admin.KindInt, KindUUID, KindText; block 17).
func (c *checker) cloudColumns(table string) (map[string]string, error) {
	rows, err := c.rows(`SELECT a.attname::text, a.atttypid::regtype::text FROM pg_attribute a JOIN pg_class t ON t.oid = a.attrelid
		WHERE `+relKeySQL("t")+` = $1 AND a.attnum > 0 AND NOT a.attisdropped`, table)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, r := range rows {
		if k := admin.KeyKind(r[1]); k != "" {
			out[r[0]] = k
		}
	}
	return out, nil
}

// checkRelation counts on the data how many rows of src find their parent through col, and
// how many of those parents belong to another organization. Before enable it reads the
// tables, after it the parent's refs table.
func (c *checker) checkRelation(src, col, dst string) (admin.RelationCheck, error) {
	if c.tx == nil {
		return admin.RelationCheck{OtherOrg: -1}, ErrNoDatabase
	}
	parent, srcTenant, dstTenant, dstKey := dst, "", "", tableKey(c.meta.Tables[dst])
	if c.enabled() {
		parent, srcTenant, dstTenant, dstKey = admin.Helper(dst, "__refs"), c.meta.Tables[src].TenantCol, "tenant", "id"
	} else {
		for _, st := range c.spec.Tables { // a child table gets its organization column from enable
			if st.Name == src {
				srcTenant = st.TenantColumn
			}
			if st.Name == dst {
				dstTenant = st.TenantColumn
			}
		}
	}
	var chk admin.RelationCheck
	err := c.heavy(func(q pgx.Tx) error { // a join of both tables: under -statement-timeout (review 3, M4)
		var err error
		chk, err = admin.CheckRelation(c.ctx, q, src, col, srcTenant, parent, dstKey, dstTenant)
		return err
	})
	return chk, err
}

// withCloud: a copy of m with a declared cloud table.
func withCloud(m *proxy.Meta, table, tc string) *proxy.Meta {
	cp := withLookup(m, table, tc, "")
	if tc != "id" { // the organizations table: its id is the organization
		cp.Lookups[table]["id"] = table
	}
	cp.Cloud = map[string]string{}
	for t, c := range m.Cloud {
		cp.Cloud[t] = c
	}
	cp.Cloud[table] = tc
	return cp
}

// withLookup: a copy of m with one more lookup.
func withLookup(m *proxy.Meta, table, col, refs string) *proxy.Meta {
	cp := *m
	cp.Lookups = map[string]map[string]string{}
	for t, l := range m.Lookups {
		cp.Lookups[t] = map[string]string{}
		for k, v := range l {
			cp.Lookups[t][k] = v
		}
	}
	if cp.Lookups[table] == nil {
		cp.Lookups[table] = map[string]string{}
	}
	cp.Lookups[table][col] = refs
	return &cp
}

// unlinked finds cloud tables that hold ids of LokaDB rows by name (<x>_id with a LokaDB table
// <x>s) without an FK or an organization column: their rows stay in the cloud, though they
// may well be the organization's data (Rails' contact_inboxes). Before enable only.
func (c *checker) unlinked() {
	if c.enabled() || c.tx == nil || c.meta == nil {
		return
	}
	tenants := map[string]bool{}
	for _, t := range c.meta.Tables {
		tenants[t.TenantCol] = true
	}
	rows, err := c.rows(`SELECT ` + relKeySQL("t") + `, a.attname::text,
		EXISTS (SELECT 1 FROM pg_constraint f WHERE f.contype = 'f' AND f.conrelid = t.oid AND a.attnum = ANY (f.conkey))::text
		FROM pg_class t JOIN pg_attribute a ON a.attrelid = t.oid
		WHERE t.relnamespace NOT IN (` + sysSchemas + `) AND t.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped
		  AND a.atttypid IN ('int2'::regtype, 'int4'::regtype, 'int8'::regtype)
		ORDER BY 1, 2`)
	if err != nil {
		return
	}
	cols := map[string][]string{} // cloud table -> its <x>_id columns without an FK
	skip := map[string]bool{}     // cloud tables with an organization column or an FK to a LokaDB table
	for _, r := range rows {
		table, col, fk := r[0], r[1], r[2] == "true"
		if c.meta.Tables[table] != nil || skip[table] {
			continue
		}
		switch {
		case tenants[col]:
			skip[table] = true
		case fk:
		case strings.HasSuffix(col, "_id"):
			cols[table] = append(cols[table], col)
		}
	}
	var names []string
	for t := range cols {
		if !skip[t] {
			names = append(names, t)
		}
	}
	sort.Strings(names)
	for _, t := range names {
		for _, col := range cols[t] {
			x := strings.TrimSuffix(col, "_id")
			parent := ""
			for _, p := range []string{x, x + "s", x + "es", strings.TrimSuffix(x, "y") + "ies"} {
				if c.meta.Tables[p] != nil {
					parent = p
					break
				}
			}
			if parent == "" {
				continue
			}
			data := ""
			if chk, err := c.checkRelation(t, col, parent); err == nil {
				data = " Data: " + chk.String() + "."
			}
			frag, _ := json.Marshal(map[string]any{"tables": []admin.SpecTable{{Name: t, ParentTable: parent, ParentColumn: col}}})
			c.add(Warning, "unlinked_table", t+"."+col, fmt.Sprintf("%s has no organization column and no FK to a LokaDB table, but %s by its name holds ids of %s rows: after enable %s stays in the cloud. If its rows are the organization's data, add it to the spec as a child of %s (the proxy follows %s once it is declared a relation or an FK).%s Spec: %s",
				t, col, parent, t, parent, col, data, frag), nil)
			break
		}
	}
}
