package check

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

var intTypes = map[string]bool{"smallint": true, "integer": true, "bigint": true}

// discover proposes a spec: every table with the tenant column, then, breadth first, every
// table that points at one of them with a single-column FK to its id.
func (c *checker) discover(tc string) (admin.Spec, error) {
	var spec admin.Spec
	roots, err := c.rows(`SELECT `+relKeySQL("c")+` FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE c.relnamespace NOT IN (`+sysSchemas+`) AND c.relkind = 'r' AND NOT c.relispartition
		  AND a.attname = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY 1`, tc)
	if err != nil {
		return spec, err
	}
	if len(roots) == 0 {
		return spec, fmt.Errorf("no table has column %s", tc)
	}
	fks, err := c.rows(`SELECT ` + relKeySQL("s") + `, ` + relKeySQL("d") + `, a.attname::text
		FROM pg_constraint c
		JOIN pg_class s ON s.oid = c.conrelid AND s.relkind = 'r' AND NOT s.relispartition AND s.relnamespace NOT IN (` + sysSchemas + `)
		JOIN pg_class d ON d.oid = c.confrelid AND d.relnamespace NOT IN (` + sysSchemas + `)
		JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = c.conkey[1]
		JOIN pg_attribute r ON r.attrelid = c.confrelid AND r.attnum = c.confkey[1]
		WHERE c.contype = 'f' AND c.conparentid = 0 AND array_length(c.conkey, 1) = 1 AND r.attname = 'id'
		ORDER BY 1, c.conname`)
	if err != nil {
		return spec, err
	}
	in := map[string]bool{}
	for _, r := range roots {
		in[r[0]] = true
		spec.Tables = append(spec.Tables, admin.SpecTable{Name: r[0], TenantColumn: tc})
	}
	for i := 0; i < len(spec.Tables); i++ {
		parent := spec.Tables[i].Name
		for _, f := range fks {
			if f[1] == parent && !in[f[0]] {
				in[f[0]] = true
				spec.Tables = append(spec.Tables, admin.SpecTable{Name: f[0], ParentTable: parent, ParentColumn: f[2]})
			}
		}
	}
	c.add(Info, "discovered", "", fmt.Sprintf("%s found: those with column %s and those reaching them by FK. The spec decides which data leaves the cloud: review the suggested spec, then pass it with -spec",
		plural(len(spec.Tables), "table", "tables"), tc), nil)
	return spec, nil
}

type column struct {
	typ, base string
	identity  bool
}

// schema checks the spec tables against what enable does to them, and builds the routing
// metadata enable would record, so the workload can be judged as the proxy would judge it.
func (c *checker) schema() error {
	m := &proxy.Meta{Tables: map[string]*proxy.Table{}, Lookups: map[string]map[string]string{}, Foreign: map[string]map[string]string{}}
	// After enable, tables of the spec that enable has not taken yet (a second enable adds them
	// while no organization is moved) are checked as before the first one, next to the tables
	// that are LokaDB already: the metadata is the proxy's, the new tables are added to it, and the
	// old ones are known parents (their organization column and key) and are not checked again
	// (review 2, R5).
	enabledBefore := map[string]*proxy.Table{}
	if c.enabledMeta != nil {
		m = c.enabledMeta
		for t, tb := range m.Tables {
			enabledBefore[t] = tb
		}
	}
	c.meta = m
	var names []string
	for _, st := range c.spec.Tables {
		names = append(names, st.Name)
	}
	type relInfo struct {
		kind                string
		partition, inherits bool
		rows                int64
	}
	rels := map[string]relInfo{}
	rows, err := c.rows(`SELECT `+relKeySQL("c")+`, c.relkind::text, c.relispartition::text,
		EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhparent = c.oid OR i.inhrelid = c.oid)::text,
		c.reltuples::bigint::text
		FROM pg_class c WHERE `+relKeySQL("c")+` = ANY($1)`, names)
	if err != nil {
		return err
	}
	for _, r := range rows {
		n, _ := strconv.ParseInt(r[4], 10, 64)
		rels[r[0]] = relInfo{kind: r[1], partition: r[2] == "true", inherits: r[3] == "true", rows: n}
	}

	state := map[string]string{} // table -> worst state found
	mark := func(t, s string) {
		if stateRank[s] > stateRank[state[t]] {
			state[t] = s
		}
	}
	unsupported := func(t, code, object, msg string, examples []string) {
		mark(t, StateUnsupported)
		c.add(Unsupported, code, object, msg, examples)
	}
	specErr := func(t, code, object, msg string, examples []string) {
		mark(t, StateSpecError)
		c.add(SpecError, code, object, msg, examples)
	}
	specOf := map[string]admin.SpecTable{}
	for _, st := range c.spec.Tables {
		if _, dup := specOf[st.Name]; !dup {
			specOf[st.Name] = st
		}
	}
	tenant := map[string]string{}
	key := map[string]string{}        // the key refs maps to the organization; "" when the table has none
	keyKind := map[string]string{}    // its kind (admin.KindInt, KindUUID, KindText)
	tenantKind := map[string]string{} // the kind of its organization column (admin.TenantInt, TenantUUID)
	cols := map[string]map[string]column{}
	inSpec := map[string]bool{}
	var statuses []TableStatus
	for _, st := range c.spec.Tables {
		t := st.Name
		ts := TableStatus{Name: t}
		if known := enabledBefore[t]; known != nil && !inSpec[t] {
			inSpec[t] = true
			tenant[t], key[t] = known.TenantCol, known.Key
			if k := m.KeyKinds[known.Refs]; k != "" {
				keyKind[t] = k
			}
			tenantKind[t] = m.TenantKind
			statuses = append(statuses, TableStatus{Name: t, Tenant: known.TenantCol, Rows: -1, Ready: true, State: StateReady})
			continue
		}
		if inSpec[t] {
			c.add(SpecError, "spec_duplicate", t, fmt.Sprintf("%s is listed twice in the spec: the first entry counts", t), nil)
			continue
		}
		inSpec[t] = true
		switch {
		case st.TenantColumn == "" && st.ParentTable == "":
			specErr(t, "spec_no_organization", t, fmt.Sprintf("%s: the spec gives neither tenant_column nor parent_table, so its rows have no organization", t), nil)
		case st.TenantColumn != "" && st.ParentTable != "":
			specErr(t, "spec_two_organizations", t, fmt.Sprintf("%s: the spec gives both tenant_column and parent_table: keep one", t), nil)
		case st.ParentTable != "" && st.ParentColumn == "":
			specErr(t, "spec_parent_column", t, fmt.Sprintf("%s: the spec gives parent_table %s without parent_column", t, st.ParentTable), nil)
		}
		if state[t] != "" {
			statuses = append(statuses, ts)
			continue
		}
		ri, ok := rels[t]
		switch {
		case !ok:
			specErr(t, "table_missing", t, fmt.Sprintf("table %s is not in the database%s", t, c.similarTable(t)), nil)
		case ri.kind != "r" || ri.partition || ri.inherits:
			unsupported(t, "table_partitioned", t, fmt.Sprintf("%s is already partitioned, a partition, a view or uses inheritance: lokadb moves plain tables only so far", t), nil)
		}
		if st.TenantColumn != "" {
			if state[t] != StateSpecError {
				tenant[t] = st.TenantColumn
			}
			ts.Tenant = st.TenantColumn
		} else if tenant[st.ParentTable] == "" {
			// The spec is sorted (admin.Spec.Sorted): a parent in the spec came first.
			switch p, in := specOf[st.ParentTable]; {
			case !in:
				specErr(t, "parent_not_in_spec", t, fmt.Sprintf("%s: its parent table %s is not in the spec: add %s (with its tenant_column, or its own parent)", t, st.ParentTable, st.ParentTable), nil)
			case parentCycle(specOf, t):
				specErr(t, "spec_cycle", t, fmt.Sprintf("%s: its parent tables lead back to it (%s -> %s ...): one of them needs a tenant_column", t, t, p.Name), nil)
			default:
				specErr(t, "parent_without_organization", t, fmt.Sprintf("%s: its parent table %s has no organization (see the spec error of %s)", t, st.ParentTable, st.ParentTable), nil)
			}
		} else {
			tenant[t] = tenant[st.ParentTable]
			ts.Tenant = fmt.Sprintf("via %s -> %s", st.ParentColumn, st.ParentTable)
		}
		if !ok || state[t] != "" {
			statuses = append(statuses, ts)
			continue
		}
		ts.Rows = ri.rows
		if ri.rows < 0 { // never analyzed (a restore without ANALYZE): count up to the limit, estimate past it
			if m, err := c.count(qk(t), "c", "true"); err == nil {
				ts.Rows = m.Rows
			}
		}

		cr, err := c.rows(`SELECT attname::text, format_type(atttypid, atttypmod), atttypid::regtype::text, attnotnull::text,
			(attidentity <> '')::text
			FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, qk(t))
		if err != nil {
			return err
		}
		cols[t] = map[string]column{}
		tbl := &proxy.Table{Name: t, TenantCol: tenant[t], Refs: admin.Helper(t, "__refs"), Cloud: admin.Helper(t, "__cloud"), ColType: map[string]string{}}
		notNull := map[string]bool{}
		for _, r := range cr {
			cols[t][r[0]] = column{typ: r[1], base: r[2], identity: r[4] == "true"}
			notNull[r[0]] = r[3] == "true"
			tbl.Cols = append(tbl.Cols, r[0])
			tbl.ColType[r[0]] = r[1]
		}
		// The key refs maps to the organization, as enable chooses it (admin tableKey): a
		// one-column primary key of a kind lokadb takes (block 17: integers, uuid, text); else
		// id when it is unique; else the primary key of several columns (a composite key,
		// tasks/composite-keys.md); else, with neither, a unique index of NOT NULL columns.
		pk, err := c.rows(admin.KeySQL, qk(t))
		if err != nil {
			return err
		}
		comp, err := c.rows(admin.CompositeKeySQL, qk(t))
		if err != nil {
			return err
		}
		compKey, compBad := admin.CompositeOf(comp)
		var id [][]string
		if _, ok := cols[t]["id"]; ok {
			if id, err = c.rows(admin.ColumnKeySQL, qk(t), "id"); err != nil {
				return err
			}
		}
		switch {
		case len(pk) == 1:
			key[t], keyKind[t] = pk[0][0], pk[0][1]
		case len(id) == 1 && id[0][1] != "" && compKey != "":
			u, err := c.rows(admin.IDUniqueSQL(qk(t)), qk(t))
			if err != nil {
				return err
			}
			if u[0][0] == "true" {
				key[t], keyKind[t] = "id", id[0][1]
			} else {
				key[t], keyKind[t] = compKey, admin.KindComposite
			}
		case len(id) == 0 && compKey != "":
			key[t], keyKind[t] = compKey, admin.KindComposite
		case len(id) == 0 && compBad != "":
			unsupported(t, "id_type", t, fmt.Sprintf("%s: its primary key has column %s: lokadb takes keys of type %s (under a deterministic collation) so far", t, compBad, admin.KeyTypes), nil)
		case len(id) == 0:
			// no primary key, no id: a unique index of NOT NULL columns, as enable takes it
			// (admin.UniqueKey; Prisma's many-to-many tables), or enable's refusal word for word
			k, kind, refused, err := admin.UniqueKey(c.rows, t, qk(t))
			if err != nil {
				return err
			}
			if refused != "" {
				unsupported(t, "id_missing", t, refused, nil)
			} else {
				key[t], keyKind[t] = k, kind
			}
		case id[0][1] == "":
			unsupported(t, "id_type", t+".id", fmt.Sprintf("%s.id is %s: lokadb takes keys of type %s (under a deterministic collation) so far", t, id[0][3], admin.KeyTypes), nil)
		default:
			key[t], keyKind[t] = "id", id[0][1]
		}
		tbl.Key = key[t]
		if st.TenantColumn != "" {
			tc, ok := cols[t][st.TenantColumn]
			switch {
			case !ok:
				delete(tenant, t)
				specErr(t, "tenant_column_missing", t, fmt.Sprintf("%s has no column %s%s", t, st.TenantColumn, c.parentHint(t, "", tenant)), nil)
			case admin.TenantKind(tc.base) == "":
				unsupported(t, "tenant_column_type", t+"."+st.TenantColumn, fmt.Sprintf("%s.%s is %s: lokadb takes organization columns of type %s so far", t, st.TenantColumn, tc.typ, admin.TenantTypes), nil)
			case !notNull[st.TenantColumn]:
				m, err := c.count(qk(t), "c", "c."+q(st.TenantColumn)+" IS NULL")
				if err != nil {
					return err
				}
				if m.Rows > 0 {
					mark(t, StateReview)
					c.addFinding(Finding{Severity: Change, Code: "tenant_null", Object: t + "." + st.TenantColumn, Subject: subjectOf("tenant_null", t+"."+st.TenantColumn), Measure: m,
						Message: fmt.Sprintf("%s of %s have no %s, so no organization: after enable they stay in the cloud (%s__none): no organization's move takes them; statements without an organization see them as before, an organization's statements never did. Giving such a row a parent of an organization is refused (lokadb does not move a row to another organization by its parent)", counted(m, "row", "rows"), t, st.TenantColumn, t)})
				} else {
					c.addFinding(Finding{Severity: Info, Code: "tenant_nullable", Object: t + "." + st.TenantColumn, Subject: subjectOf("tenant_nullable", t+"."+st.TenantColumn), Measure: m,
						Message: fmt.Sprintf("%s.%s allows NULL: rows without an organization stay in the cloud (%s__none) and no move takes them%s", t, st.TenantColumn, t, sampledNone(m))})
				}
			}
		} else {
			if _, has := cols[t][tenant[t]]; has && !c.prepared(t) {
				// enable would add it and refuses (admin.ChildTenantClash; review 3, M1)
				specErr(t, "child_has_tenant_column", t+"."+tenant[t], admin.ChildTenantClash(t, tenant[t], st.ParentTable).Error(), nil)
			}
			tbl.Cols = append(tbl.Cols, tenant[t]) // enable adds it, DEFAULT lokadb.org_default()
			tenantKind[t] = tenantKind[st.ParentTable]
			tbl.ColType[tenant[t]] = "bigint"
			switch tenantKind[t] {
			case admin.TenantUUID:
				tbl.ColType[tenant[t]] = "uuid"
			case admin.TenantText:
				tbl.ColType[tenant[t]] = "text"
			}
			if pc, ok := cols[t][st.ParentColumn]; !ok {
				delete(tenant, t)
				specErr(t, "parent_column_missing", t, fmt.Sprintf("%s has no column %s%s", t, st.ParentColumn, c.parentHint(t, st.ParentTable, tenant)), nil)
			} else if keyKind[st.ParentTable] == admin.KindComposite {
				// one column cannot reference a key of several: the child's organization would
				// come through an FK of several columns, which its triggers do not follow yet
				unsupported(t, "child_composite_fk", t+"."+st.ParentColumn, fmt.Sprintf("%s finds its organization through %s, whose key has several columns (%s): lokadb does not follow a composite reference to a parent yet. Give %s an organization column of its own (tenant_column), or keep it in the cloud", t, st.ParentTable, strings.Join(admin.KeyCols(key[st.ParentTable]), ", "), t), nil)
			} else if pk := keyKind[st.ParentTable]; pk != "" && admin.KeyKind(pc.base) != pk || pk == "" && admin.KeyKind(pc.base) == "" {
				// the reference is looked up in the parent's refs table, whose id is of the
				// parent's key's type (block 17)
				unsupported(t, "parent_column_type", t+"."+st.ParentColumn, fmt.Sprintf("%s.%s is %s: lokadb follows a reference to the parent's key of the same kind only (integer to integer, uuid to uuid, text or varchar to text or varchar, character(n) to character(n))", t, st.ParentColumn, pc.typ), nil)
			} else if typ := strings.TrimSuffix(st.ParentColumn, "_id") + "_type"; strings.HasSuffix(st.ParentColumn, "_id") && cols[t][typ].typ != "" {
				// Rails' polymorphic belongs_to: <x>_id with <x>_type naming the parent table.
				// Every row "found" in the parent would be counted, also those of other tables.
				unsupported(t, "parent_polymorphic", t+"."+st.ParentColumn, fmt.Sprintf("%s.%s goes with %s.%s (a polymorphic reference, as Rails makes them): its rows may point at other tables than %s, and lokadb would give them the organization of the %s row with the same number. lokadb does not follow polymorphic references yet: give %s an organization column of its own (tenant_column), or keep it in the cloud", t, st.ParentColumn, t, typ, st.ParentTable, st.ParentTable, t), nil)
			} else if pk := key[st.ParentTable]; pk != "" {
				m, err := c.count(qk(t), "c", fmt.Sprintf("NOT EXISTS (SELECT 1 FROM %s p WHERE p.%s = c.%s)", qk(st.ParentTable), q(pk), q(st.ParentColumn)))
				if err != nil {
					return err
				}
				if m.Rows > 0 {
					mark(t, StateReview)
					c.addFinding(Finding{Severity: Change, Code: "orphan_rows", Object: t, Subject: subjectOf("orphan_rows", t), Measure: m,
						Message: fmt.Sprintf("%s of %s have no row in %s (%s is NULL or points nowhere), so no organization (nor do rows under a parent that has none): after enable they stay in the cloud (%s__none): no organization's move takes them; statements without an organization see them as before, an organization's statements never did. Giving such a row a parent of an organization is refused (lokadb does not move a row to another organization by its parent)",
							counted(m, "row", "rows"), t, st.ParentTable, st.ParentColumn, t)})
				} else if m.Method == MeasureSample {
					c.addFinding(Finding{Severity: Info, Code: "orphan_rows_sampled", Object: t, Subject: subjectOf("orphan_rows_sampled", t), Measure: m,
						Message: fmt.Sprintf("no rows of %s without a row in %s were found%s", t, st.ParentTable, sampledNone(m))})
				}
			}
		}
		if tc, ok := cols[t][st.TenantColumn]; ok && admin.TenantKind(tc.base) != "" {
			tenantKind[t] = admin.TenantKind(tc.base)
			if err := c.reservedTenant(t, st.TenantColumn, tenantKind[t], func(s string) { mark(t, s) }); err != nil {
				return err
			}
		}
		if err := c.portability(t, key[t], cols[t], func(s string) { mark(t, s) }); err != nil {
			return err
		}
		m.Tables[t] = tbl
		m.Lookups[t] = map[string]string{tenant[t]: ""}
		if key[t] != "" {
			m.Lookups[t][key[t]] = admin.Helper(t, "__refs")
		}
		statuses = append(statuses, ts)
	}
	c.mixedTenants(tenantKind)
	// what the direct path's forms depend on, as proxy.LoadMeta reads it after enable: columns
	// of the application's types, its statement and row triggers (review 2 of check, R2): the
	// estimates of profiles fdw+direct and direct before enable judge by them as the proxy will
	if err := proxy.CatalogFacts(func(sql string) ([][]string, error) { return c.rows(sql) }, m); err != nil {
		return err
	}
	// the organizations' kind, which the routing declarations (a cloud table's organization
	// column) are judged by; after enable proxy.LoadMeta has it
	for _, k := range tenantKind {
		if admin.ValueKind(k) {
			m.TenantKind = k
		}
	}

	// FKs: enable moves every FK that touches a LokaDB table. One onto a LokaDB table must
	// reference its key, alone or with its organization column (admin.RefsColumns).
	// By key (package rel), not by regclass text (quoted for "order", "Doc": such FKs were
	// missed and the report said ready; qualified only outside the search path).
	// The FKs of a partition count as its root table's (Pagila declares one on every
	// partition of payment): one finding per root, column and target.
	fks, err := c.rows(`SELECT c.conname::text,
		`+relKeySQL("s")+`,
		`+relKeySQL("d")+`,
		to_json(ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(n, o)
			JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.o))::text,
		to_json(ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(n, o)
			JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n ORDER BY k.o))::text,
		concat_ws(' ', -- ON UPDATE too: lokadb carries out both (review 3 of check, L1)
			CASE c.confdeltype WHEN 'c' THEN 'ON DELETE CASCADE' WHEN 'n' THEN 'ON DELETE SET NULL' WHEN 'd' THEN 'ON DELETE SET DEFAULT'
				WHEN 'r' THEN 'ON DELETE RESTRICT' END,
			CASE c.confupdtype WHEN 'c' THEN 'ON UPDATE CASCADE' WHEN 'n' THEN 'ON UPDATE SET NULL' WHEN 'd' THEN 'ON UPDATE SET DEFAULT'
				WHEN 'r' THEN 'ON UPDATE RESTRICT' END),
		s.relnamespace::regnamespace::text,
		CASE WHEN c.conrelid = s.oid THEN '' ELSE `+relKeySQL("p")+` END
		FROM pg_constraint c
		JOIN pg_class p ON p.oid = c.conrelid
		JOIN pg_class s ON s.oid = coalesce(pg_partition_root(c.conrelid), c.conrelid)
		JOIN pg_class d ON d.oid = c.confrelid
		WHERE c.contype = 'f' AND c.conparentid = 0
		  AND (`+relKeySQL("s")+` = ANY($1) OR `+relKeySQL("d")+` = ANY($1))
		ORDER BY 2, 1`, names)
	if err != nil {
		return err
	}
	parentFK := map[string]map[string]string{} // table -> column -> LokaDB table it references
	var toCloud, fromCloud []string
	toCloudOf := map[string][]string{} // cloud table -> the FKs onto it, with their ON DELETE and ON UPDATE
	// fkKey: the FK without its name and the partition declaring it; partsOf: those partitions
	fkKey := func(f []string) string { return strings.Join(f[1:7], "|") }
	partsOf := map[string][]string{}
	for _, f := range fks {
		if f[7] != "" {
			partsOf[fkKey(f)] = append(partsOf[fkKey(f)], f[7])
		}
	}
	seenFK := map[string]bool{}
	for _, f := range fks {
		name, src, dst := f[0], f[1], f[2]
		var cols, refcols []string
		if err := json.Unmarshal([]byte(f[3]), &cols); err != nil {
			return err
		}
		if err := json.Unmarshal([]byte(f[4]), &refcols); err != nil {
			return err
		}
		desc := fmt.Sprintf("%s.%s -> %s", src, strings.Join(cols, ","), dst)
		k := fkKey(f)
		if seenFK[k] {
			continue
		}
		seenFK[k] = true
		refs, ok := admin.RefsColumns(refcols, key[dst], tenant[dst])
		// block 4, item 2: onto a unique column other than the key, alone, enable keeps the FK
		// on that column's map; not when the column is how its rows find their organization
		uref := inSpec[dst] && !ok && len(refcols) == 1 && specOf[src].ParentColumn != cols[0] && c.uniqueColumn(dst, refcols[0])
		switch {
		case uref:
			c.add(Info, "fk_to_unique_column", name, fmt.Sprintf("FK %s points at %s.%s, a unique column other than its key %s: enable keeps it on a map of that column (value -> organization), as onto the key", name, dst, refcols[0], key[dst]), nil)
			if inSpec[src] && m.Lookups[src] != nil {
				m.Lookups[src][cols[0]] = admin.Helper(dst, "__refs__"+refcols[0])
			}
			continue
		case inSpec[dst] && key[dst] == "":
			// the parent has no key lokadb takes: id_type or id_missing says so, once, and an FK onto
			// it names no key to point past
			continue
		case inSpec[dst] && !ok:
			for _, t := range []string{src, dst} {
				if inSpec[t] {
					mark(t, StateUnsupported)
				}
			}
			subj := fkSubject(name, src, cols, dst, refcols)
			parts := onPartitions(subj, partsOf[k])
			if len(refcols) > 1 {
				c.addOn(Unsupported, "fk_multicolumn", subj, name, fmt.Sprintf("FK %s (%s -> %s (%s)) has several columns, and they are not the organization column and the key of %s: lokadb moves an FK onto a LokaDB table only when it references the key (%s), alone or with the organization column, so far", name, src, dst, strings.Join(refcols, ", "), dst, key[dst]), parts)
			} else {
				c.addOn(Unsupported, "fk_not_to_id", subj, name, fmt.Sprintf("FK %s points at %s.%s, not at its key %s: lokadb keeps references to a LokaDB table by its key only so far", name, dst, refcols[0], key[dst]), parts)
			}
		case inSpec[src] && inSpec[dst]:
			if parentFK[src] == nil {
				parentFK[src] = map[string]string{}
			}
			for i, r := range refs {
				if r != "id" {
					continue
				}
				parentFK[src][cols[i]] = dst
			}
			// the lookup: the FK's column onto the key, or its columns onto a composite key
			if col, ok := admin.FKLookup(cols, refs); ok && m.Lookups[src] != nil {
				m.Lookups[src][col] = admin.Helper(dst, "__refs")
			}
		case inSpec[src]:
			if f[5] != "" {
				desc += " (" + f[5] + ")"
			}
			toCloud = append(toCloud, desc)
			toCloudOf[dst] = append(toCloudOf[dst], desc)
		default:
			fromCloud = append(fromCloud, desc)
		}
	}

	for _, st := range c.spec.Tables {
		t := st.Name
		if m.Tables[t] == nil {
			continue
		}
		if st.ParentTable != "" && parentFK[t][st.ParentColumn] != st.ParentTable {
			c.add(Warning, "child_without_fk", t+"."+st.ParentColumn, fmt.Sprintf("%s.%s has no FK to %s: the proxy cannot find the organization from it, so queries by %s run in every organization's database",
				t, st.ParentColumn, st.ParentTable, st.ParentColumn), nil)
		}
		// After enable a unique index exists per organization's partition. It stays globally
		// unique only if its key pins the organization: the organization column, id (unique
		// across organizations through the refs table) or an FK to another LokaDB table.
		pinned, hint := map[string]bool{key[t]: true}, st.TenantColumn
		if st.TenantColumn != "" {
			pinned[st.TenantColumn] = true
		} else {
			hint = st.ParentColumn
		}
		for col := range parentFK[t] {
			pinned[col] = true
		}
		// a declared routing key stays unique across organizations (its map's primary key)
		for _, rt := range []*admin.Routing{c.spec.Routing, c.o.Patch} {
			if rt == nil {
				continue
			}
			for _, k := range rt.Keys {
				if k.Table == t && k.Column != "" {
					pinned[k.Column] = true
				}
			}
		}
		// A table without id: its primary key is reported with id_missing, not again here.
		if err := c.tableObjects(t, pinned, key[t] == "", hint, func(s string) { mark(t, s) }); err != nil {
			return err
		}
	}
	for i := range statuses {
		s := state[statuses[i].Name]
		if s == "" {
			s = StateReady
		}
		statuses[i].State = s
		statuses[i].Ready = s == StateReady || s == StateReview
	}
	c.rep.Tables = statuses
	for _, f := range c.rep.Findings {
		if f.Code == "unique_not_tenant_scoped" {
			c.rep.Summary.UniquePerOrg++
		}
	}

	c.toCloud = toCloud
	if len(toCloud) > 0 {
		c.add(Change, "fk_to_cloud", "", "FKs from LokaDB tables to cloud tables: for a moved organization, whose rows are in its own database, lokadb's triggers stand in for them. A row written through the cloud must reference an existing cloud row, and deleting or changing the key of a cloud row applies the FK's action (CASCADE, SET NULL, RESTRICT) in the moved organizations' databases too (see fk_to_cloud_delete). There they are checked at the end of each statement, even if DEFERRABLE, and writes that do not go through the cloud database (made in the organization's database itself) are not checked", toCloud)
		if err := c.cloudDeletes(toCloudOf); err != nil {
			return err
		}
	}
	if len(fromCloud) > 0 {
		c.add(Warning, "cloud_references", "", "cloud tables reference LokaDB rows by FK (kept through the refs tables); their own rows stay in the cloud. If they are the organizations' data (child rows of a LokaDB table), add those tables to the spec with parent_table, or they never leave the cloud", fromCloud)
	}
	var tcs []string
	for _, st := range c.spec.Tables {
		if st.TenantColumn != "" {
			tcs = append(tcs, st.TenantColumn)
		}
	}
	if len(tcs) > 0 {
		left, err := c.rows(`SELECT DISTINCT `+relKeySQL("c")+` FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid
			WHERE c.relnamespace NOT IN (`+sysSchemas+`) AND c.relkind IN ('r', 'p') AND NOT c.relispartition
			  AND a.attname::text = ANY($1) AND NOT a.attisdropped AND NOT (`+relKeySQL("c")+` = ANY($2))
			ORDER BY 1`, tcs, names)
		if err != nil {
			return err
		}
		if len(left) > 0 {
			var l []string
			for _, r := range left {
				l = append(l, r[0])
			}
			c.add(Info, "tenant_table_in_cloud", "", fmt.Sprintf("these tables have an organization column but are not in the spec: their rows stay in the cloud (%s)", strings.Join(l, ", ")), nil)
		}
	}
	return nil
}

// tableObjects checks what enable rebuilds or loses when it replaces the table with a partitioned one.
// skipPK: the primary key is already reported (id_missing).
func (c *checker) tableObjects(t string, pinned map[string]bool, skipPK bool, hint string, mark func(state string)) error {
	idx, err := c.rows(`SELECT coalesce(con.conname::text, ic.relname::text), pg_get_indexdef(i.indexrelid),
		coalesce((SELECT string_agg(a.attname::text, ',' ORDER BY k.ord) FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
			JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum WHERE k.ord <= i.indnkeyatts), ''),
		i.indisprimary::text
		FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
		LEFT JOIN pg_constraint con ON con.conindid = i.indexrelid AND con.contype IN ('p', 'u')
		WHERE i.indrelid = $1::regclass AND i.indisunique ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	for _, r := range idx {
		ok := skipPK && r[3] == "true"
		for _, col := range strings.Split(r[2], ",") {
			ok = ok || pinned[col]
		}
		for k := range pinned { // a composite key pins an index holding all of its columns
			if admin.Composite(k) {
				all := true
				for _, kc := range admin.KeyCols(k) {
					all = all && slices.Contains(strings.Split(r[2], ","), kc)
				}
				ok = ok || all
			}
		}
		if !ok {
			mark(StateReview)
			fix := fmt.Sprintf("add %s to the key, or keep this uniqueness in a cloud table", hint)
			if col := r[2]; col != "" && !strings.Contains(col, ",") && !strings.Contains(r[1], " WHERE ") {
				// one plain column: a routing key keeps it unique across all organizations, and a
				// lookup by it (WHERE col = $1) finds the organization
				frag, _ := json.Marshal(map[string]any{"routing": admin.Routing{Keys: []admin.Key{{Table: t, Column: col}}}})
				fix = fmt.Sprintf("declare %s.%s a routing key: lokadb then keeps it unique across all organizations and finds the organization of a statement by its value (WHERE %s = $1). Spec: %s. Or %s", t, col, col, frag, fix)
			} else if cols := strings.Split(r[2], ","); len(cols) > 1 && !strings.Contains(r[1], " WHERE ") {
				// several columns cannot be a routing key (yet), but one of them may be unique on its own
				// on the data, as provider ids usually are
				for _, col := range cols {
					if chk, err := c.checkKey(t, col); err == nil && chk.Rows > chk.Nulls && chk.Duplicates == 0 && !pinned[col] {
						frag, _ := json.Marshal(map[string]any{"routing": admin.Routing{Keys: []admin.Key{{Table: t, Column: col}}}})
						fix = fmt.Sprintf("%s.%s is unique on the data, though not by a constraint: add a unique index on it, then declare it a routing key: lokadb keeps it unique across all organizations and finds the organization of a statement by its value (WHERE %s = $1; a statement by %s alone is routed). Spec: %s. Or %s", t, col, col, strings.Join(cols, " and "), frag, fix)
						break
					}
				}
			}
			c.addOn(Change, "unique_not_tenant_scoped", uniqueSubject(t, r[0], r[2]), r[0], fmt.Sprintf("%s on %s is global today; after the move it holds within each organization only, and two organizations can store the same value. Fine if the application never relies on it across organizations; otherwise %s",
				r[0], t, fix), []string{r[1]})
		}
	}

	// EXCLUDE constraints stay constraints of the DEFAULT partition, and move-org gives the
	// organization's table the same: two rows that overlap are refused within one organization,
	// never across two (Postgres has no exclusion constraint over several partitions, nor across
	// databases). As UNIQUE: unless an equality on the organization column (or the key, or the
	// FK to a LokaDB table) is in it, the constraint was global and is not any more.
	exc, err := c.rows(`SELECT con.conname::text, pg_get_constraintdef(con.oid),
		coalesce((SELECT string_agg(coalesce(a.attname::text, '') || ':' || o.oprname::text, ',' ORDER BY k.ord)
			FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord)
			JOIN unnest(con.conexclop) WITH ORDINALITY x(op, ord) ON x.ord = k.ord
			JOIN pg_operator o ON o.oid = x.op
			LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
			WHERE k.ord <= i.indnkeyatts), '')
		FROM pg_constraint con JOIN pg_index i ON i.indexrelid = con.conindid
		WHERE con.conrelid = $1::regclass AND con.contype = 'x' ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	for _, r := range exc {
		var cols, eq []string // the columns, and those compared with =
		for _, ce := range strings.Split(r[2], ",") {
			col, op, _ := strings.Cut(ce, ":")
			if col == "" {
				continue // an expression
			}
			cols = append(cols, col)
			if op == "=" {
				eq = append(eq, col)
			}
		}
		ok := false
		for _, col := range eq {
			ok = ok || pinned[col]
		}
		for k := range pinned { // a composite key pins a constraint comparing all of its columns
			if admin.Composite(k) {
				all := true
				for _, kc := range admin.KeyCols(k) {
					all = all && slices.Contains(eq, kc)
				}
				ok = ok || all
			}
		}
		if !ok {
			mark(StateReview)
			c.addOn(Change, "exclude_not_tenant_scoped", uniqueSubject(t, r[0], strings.Join(cols, ",")), r[0], fmt.Sprintf("the exclusion constraint %s on %s refuses overlapping rows across all organizations today; after the move it does so within each organization only, and two organizations can store rows that overlap. Fine if the application never relies on it across organizations; otherwise add %s WITH = to the constraint, or keep the rows that must not overlap in a cloud table",
				r[0], t, hint), []string{r[1]})
		}
	}

	// enable makes the table the DEFAULT partition of a new parent (admin.Enable): what
	// depends on the table is pointed at the parent, its triggers move there, its identity
	// columns become sequences, its row security, rules and materialized views are made
	// again over the parent (admin.carryOver). It refuses (admin.unsupported) constraint
	// triggers and triggers with transition tables, which Postgres does not allow with
	// foreign partitions, and materialized views that other views read.
	// what depends on t, as enable judges it (admin.Dependents, one query for both: review 3 of
	// check, H1 and H2): views, rules and SQL-standard functions; policies of other tables
	// that read t; functions, columns and domains over its row type
	deps, err := admin.Dependents(func(sql string, args ...any) ([][]string, error) { return c.rows(sql, args...) }, t)
	if err != nil {
		return err
	}
	var repointed, matviews, refused []string
	for _, d := range deps {
		name := strings.ReplaceAll(d.Kind, "_", " ") + " " + d.Name
		switch {
		case !d.Movable:
			refused = append(refused, name+": "+d.Reason)
		case d.Kind == "matview":
			matviews = append(matviews, "materialized view "+d.Name)
		default:
			repointed = append(repointed, name)
		}
	}
	if len(refused) > 0 {
		// enable refuses the table before any change (admin.Dependents, Movable false)
		mark(StateBlocked)
		c.add(Blocker, "dependent_refused", t, fmt.Sprintf("objects that depend on %s cannot be carried over to the new parent table, and enable refuses %s: change or drop them before enable (a column or a domain of %s's row type, an aggregate over it, a function other objects than views depend on)", t, t, t), refused)
	}
	if len(matviews) > 0 {
		// enable makes again the materialized views over t and those that read them (block 7:
		// admin.matviewSteps); what reads them is pointed at the new ones. What holds one in
		// another way (a policy, a function over its row type) it cannot carry over.
		rebuilt, err := c.rows(`WITH RECURSIVE m(oid) AS (
				SELECT w.ev_class FROM pg_depend d JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid
				JOIN pg_class v ON v.oid = w.ev_class
				WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = $1::regclass AND v.relkind = 'm'
				UNION
				SELECT w.ev_class FROM m JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = m.oid
				JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid JOIN pg_class v ON v.oid = w.ev_class
				WHERE v.relkind = 'm' AND w.ev_class <> m.oid)
			SELECT oid::regclass::text,
				(SELECT string_agg(DISTINCT w.ev_class::regclass::text, ', ') FROM pg_depend d
					JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid
					WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = m.oid AND w.ev_class <> m.oid),
				(SELECT string_agg(DISTINCT pg_describe_object(d.classid, d.objid, d.objsubid), ', ') FROM pg_depend d
					WHERE d.deptype = 'n' AND (d.refclassid = 'pg_class'::regclass AND d.refobjid = m.oid
						AND d.classid NOT IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)
						OR d.refclassid = 'pg_type'::regclass AND d.refobjid = (SELECT reltype FROM pg_class WHERE oid = m.oid)))
			FROM m ORDER BY 1`, qk(t))
		if err != nil {
			return err
		}
		var read, held []string
		for _, mv := range rebuilt {
			if mv[1] != "" {
				read = append(read, mv[1]+" read "+mv[0])
			}
			if mv[2] != "" {
				held = append(held, mv[2]+" depend on "+mv[0])
			}
		}
		if len(held) > 0 {
			mark(StateUnsupported)
			c.add(Unsupported, "materialized_view", t, fmt.Sprintf("enable makes the materialized views over %s again over the new parent table, and these depend on them in a way lokadb cannot carry over (only views, rules, materialized views and SQL-standard functions that read them are pointed at the new ones): drop them before enable and create them again after", t), held)
		} else {
			mark(StateReview)
			c.add(Change, "materialized_view", t, fmt.Sprintf("enable makes the materialized views over %s again over the new parent table (with their indexes, statistics, owner, privileges and comments) and points what reads them at the new ones: views, rules and SQL-standard functions keep their definitions, materialized views that read them are made again too. A populated one is filled again while enable holds its locks", t), append(matviews, read...))
		}
	}
	// A materialized view keeps its rows in the cloud database: over a LokaDB table, REFRESH
	// reads the moved organizations from their own databases and stores the result there.
	stores, err := c.rows(`WITH RECURSIVE r(oid) AS (
			SELECT $1::regclass::oid
			UNION
			SELECT w.ev_class FROM r JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = r.oid
			JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid AND w.rulename = '_RETURN'
			WHERE w.ev_class <> r.oid)
		SELECT c.oid::regclass::text FROM r JOIN pg_class c ON c.oid = r.oid WHERE c.relkind = 'm' ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	if len(stores) > 0 {
		c.add(Warning, "matview_stores_moved_rows", t, fmt.Sprintf("these materialized views read %s and keep their rows in the cloud database: after a move, REFRESH reads the moved organizations from their own databases and stores their rows (or what is computed from them) in the cloud again, which the move is meant to avoid. Either make each one read only the cloud's organizations (a condition on the organization column in its definition), or replace it with a view or a table the application fills", t), flat(stores))
	}
	if len(repointed) > 0 {
		c.add(Info, "dependents_repointed", t, fmt.Sprintf("enable points what depends on %s at the new parent table, with its own definition (CREATE OR REPLACE): it then reads every organization, the moved ones too", t), repointed)
	}
	rules, err := c.rows(`SELECT rulename::text FROM pg_rewrite WHERE ev_class = $1::regclass ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	if len(rules) > 0 {
		c.add(Info, "table_rule", t, fmt.Sprintf("enable moves the rules of %s to the new parent table: they act on the statements of every organization, the moved ones too", t), flat(rules))
	}
	trg, err := c.rows(`SELECT tgname::text, (tgconstraint <> 0 OR tgoldtable IS NOT NULL OR tgnewtable IS NOT NULL)::text
		FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	var carried, special []string
	for _, r := range trg {
		if r[1] == "true" {
			special = append(special, r[0])
		} else {
			carried = append(carried, r[0])
		}
	}
	if len(special) > 0 {
		mark(StateUnsupported)
		c.add(Unsupported, "user_trigger", t, fmt.Sprintf("%s has constraint triggers or triggers with transition tables: Postgres allows neither on a table with foreign partitions, where lokadb keeps the moved organizations, so enable refuses it. Until lokadb works around it: make them plain row triggers, or keep %s in the cloud", t, t), special)
	}
	if len(carried) > 0 {
		c.add(Info, "triggers_carried", t, fmt.Sprintf("enable moves the triggers of %s to the new parent table: they keep firing in the cloud database for every organization, the moved ones too (through their foreign partitions), and not for the rows move-org shifts between partitions", t), carried)
	}
	pol, err := c.rows(`SELECT polname::text FROM pg_policy WHERE polrelid = $1::regclass ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	rls, err := c.rows(`SELECT relrowsecurity::text FROM pg_class WHERE oid = $1::regclass`, qk(t))
	if err != nil {
		return err
	}
	switch {
	case rls[0][0] == "true":
		c.add(Info, "rls_enabled", t, fmt.Sprintf("row-level security of %s moves to the new parent table: its policies apply as before, and lokadb's organization policy (restrictive) narrows them. The owner, whom row security did not apply to unless forced, keeps access through policy lokadb_owner", t), flat(pol))
	case len(pol) > 0:
		c.add(Info, "rls_policy", t, fmt.Sprintf("%s has row-level security policies but row security is not enabled, so they do not apply: they stay on the cloud's partition and do not move to the new parent table", t), flat(pol))
	}
	// Identity columns become sequence defaults on the parent (a partition's identity would
	// refuse the values the parent routes to it); GENERATED ALWAYS no longer refuses values.
	idn, err := c.rows(`SELECT attname::text FROM pg_attribute
		WHERE attrelid = $1::regclass AND attidentity = 'a' AND NOT attisdropped ORDER BY attnum`, qk(t))
	if err != nil {
		return err
	}
	if len(idn) > 0 {
		mark(StateReview)
		c.add(Change, "identity_always", t, fmt.Sprintf("%s has GENERATED ALWAYS identity columns: enable turns them into sequence defaults, so an INSERT that supplies its own value is accepted instead of refused", t), flat(idn))
	}
	gr, err := c.rows(`SELECT DISTINCT CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee)::text END
		FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid = $1::regclass AND a.grantee <> c.relowner ORDER BY 1`, qk(t))
	if err != nil {
		return err
	}
	if len(gr) > 0 {
		c.add(Info, "grants_carried", t, fmt.Sprintf("other roles have privileges on %s: enable grants them on the new parent table, with what they need around it (schema lokadb, the refs table, the sequences); the table itself becomes the cloud's partition and keeps none, so they cannot pass by the organization policy", t), flat(gr))
	}
	return nil
}

func flat(rows [][]string) []string {
	var out []string
	for _, r := range rows {
		out = append(out, r[0])
	}
	return out
}

func sortedTables(m *proxy.Meta) []string {
	var out []string
	for t := range m.Tables {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// parentCycle: following t's parent tables in the spec leads back to t.
func parentCycle(spec map[string]admin.SpecTable, t string) bool {
	seen := map[string]bool{}
	for cur := spec[t].ParentTable; cur != ""; cur = spec[cur].ParentTable {
		if cur == t {
			return true
		}
		if seen[cur] {
			return false
		}
		seen[cur] = true
	}
	return false
}

// parentHint suggests how t's rows can find their organization: its single-column FKs to
// tables that have one (to parent only, if given). Empty when there is nothing to suggest.
func (c *checker) parentHint(t, parent string, tenant map[string]string) string {
	fks, err := c.rows(`SELECT a.attname::text, `+relKeySQL("cf")+` FROM pg_constraint k
		JOIN pg_class cf ON cf.oid = k.confrelid
		JOIN pg_attribute a ON a.attrelid = k.conrelid AND a.attnum = k.conkey[1]
		WHERE k.contype = 'f' AND k.conrelid = $1::regclass AND cardinality(k.conkey) = 1
		  AND cf.relnamespace NOT IN (`+sysSchemas+`) ORDER BY 1`, qk(t))
	if err != nil {
		return ""
	}
	var hints []string
	for _, fk := range fks {
		if (parent == "" || fk[1] == parent) && tenant[fk[1]] != "" {
			hints = append(hints, fmt.Sprintf(`"parent_table": %q, "parent_column": %q`, fk[1], fk[0]))
		}
	}
	switch {
	case len(hints) > 0:
		return ". Its FK says where its organization is: " + strings.Join(hints, " or ")
	case parent == "":
		return ". If its rows belong to no organization, remove it from the spec: it stays in the cloud"
	}
	return ""
}

// similarTable: a hint at a table with nearly the same key.
func (c *checker) similarTable(t string) string {
	names, err := c.rows(`SELECT ` + relKeySQL("c") + ` FROM pg_class c WHERE c.relnamespace NOT IN (` + sysSchemas + `) AND c.relkind IN ('r', 'p') ORDER BY 1`)
	if err != nil {
		return ""
	}
	best, dist := "", 3 // farther than 2 edits is another table
	for _, n := range names {
		if d := editDistance(strings.ToLower(t), strings.ToLower(n[0])); d < dist {
			best, dist = n[0], d
		}
	}
	if best == "" {
		return ""
	}
	return fmt.Sprintf(" (did you mean %s?)", best)
}

// editDistance: Levenshtein distance of a and b.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := make([]int, len(b)+1)
		cur[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev = cur
	}
	return prev[len(b)]
}

// cloudDeletes: what becomes of a DELETE (or a change of the key) of a row of a cloud table
// referenced by LokaDB tables once an organization is moved. Declared in routing.cloud_tables,
// the table knows the organization of the row: the FK acts in that organization's database
// only. Otherwise it would act in every moved organization's database, and the fanout policy
// decides whether it may (admin/fkcloud.go): reject refuses the change, reject-writes lets
// RESTRICT and NO ACTION through (they only read), log and allow run it.
func (c *checker) cloudDeletes(fks map[string][]string) error {
	declared := map[string]string{} // cloud table -> its organization column
	for _, rt := range []*admin.Routing{c.spec.Routing, c.o.Patch} {
		if rt != nil {
			for _, ct := range rt.CloudTables {
				declared[ct.Table] = ct.TenantColumn
			}
		}
	}
	implied, err := admin.ImpliedCloudTables(c.catalogRows, c.spec)
	if err != nil {
		return err
	}
	orgTables := map[string]bool{} // where the organizations themselves are kept
	for _, ct := range implied {
		declared[ct.Table], orgTables[ct.Table] = "id", true
	}
	for t, col := range declared {
		if col == "id" {
			orgTables[t] = true
		}
	}
	if d := c.rep.Discovery; d != nil {
		orgTables[d.OrgTable] = true
	}
	cfg, src := proxy.Config{}, "default, no -proxy-config"
	if c.o.Proxy != nil {
		cfg, src = *c.o.Proxy, "the proxy config"
	}
	fan, err := cfg.FanoutFor(c.rep.Database)
	if err != nil {
		return err
	}
	var ts []string
	for t := range fks {
		if declared[t] == "" || orgTables[t] {
			ts = append(ts, t)
		}
	}
	sort.Strings(ts)
	var tenantCols []string // the spec's organization columns
	for _, st := range c.spec.Tables {
		if st.TenantColumn != "" && !slices.Contains(tenantCols, st.TenantColumn) {
			tenantCols = append(tenantCols, st.TenantColumn)
		}
	}
	for _, t := range ts {
		if orgTables[t] {
			note := ""
			if slices.ContainsFunc(implied, func(ct admin.CloudTable) bool { return ct.Table == t }) {
				note = " (taken as declared in routing.cloud_tables, no declaration needed)"
			}
			c.add(Info, "fk_to_cloud_delete", t, fmt.Sprintf("%s is the organizations table (the organization columns of the spec's tables reference it; it is referenced by %s): deleting a row of it or changing its key concerns the one organization whose id it is, so lokadb runs the FK actions in that organization's database only, whatever the fanout policy is. It is a cloud table whose organization column is its id%s",
				t, strings.Join(fks[t], "; "), note), nil)
			continue
		}
		writes := false // an action that changes the organizations' rows
		for _, d := range fks[t] {
			writes = writes || strings.Contains(d, "CASCADE") || strings.Contains(d, "SET NULL") || strings.Contains(d, "SET DEFAULT")
		}
		lead := fmt.Sprintf("%s is referenced by LokaDB tables (%s) and does not tell the organization of its rows (it is not in routing.cloud_tables): once an organization is moved, deleting a row of %s or changing its key has to check or change the rows referencing it in every moved organization's database, as a statement on LokaDB tables of no one organization does. ", t, strings.Join(fks[t], "; "), t)
		policy := fmt.Sprintf("the fanout policy %s (%s)", fan.Mode, src)
		// what can be declared: a column of the table that holds the organization of its rows
		var orgCols []string
		if c.tx != nil {
			if cols, err := c.cloudColumns(t); err == nil {
				for _, tc := range tenantCols {
					if cols[tc] != "" && cols["id"] != "" {
						orgCols = append(orgCols, tc)
					}
				}
			}
		}
		var fix, alt string
		switch {
		case len(orgCols) > 0:
			fix = fmt.Sprintf(" To keep it within one organization's database, declare %s in routing.cloud_tables with the column that holds the organization of its rows (%s)", t, strings.Join(orgCols, ", "))
			alt = ", or set the fanout policy of the database to log or allow"
		default:
			linked := c.linkedToOrganizations(t, orgTables)
			why := fmt.Sprintf(" %s has no column that holds the organization of its rows, so it cannot be declared in routing.cloud_tables.", t)
			if len(linked) > 0 {
				why = fmt.Sprintf(" %s is shared by organizations (%s links it to the organizations table) and has no column that holds the organization of its rows, so it cannot be declared in routing.cloud_tables: deleting a row of it, or SET NULL on its key, touches every moved organization's database that references the row.", t, strings.Join(linked, ", "))
			}
			fix = why + " The statement has to be allowed to run in every organization's database: set the fanout policy of the database to log or allow, or allow only the reviewed statement (an allow-list entry for that DELETE in the proxy config), or delete softly in the application (a deleted_at column), so the foreign key is never triggered"
			alt = ""
		}
		switch {
		case fan.Mode == proxy.FanoutLog || fan.Mode == proxy.FanoutAllow || fan.Mode == proxy.FanoutRejectWrites && !writes:
			c.add(Info, "fk_to_cloud_delete", t, lead+"With "+policy+" lokadb does that: the DELETE runs in every moved organization's database, and fails while one of them is unreachable. CASCADE and SET NULL there have no two-phase commit, as any write through FDW."+fix, nil)
		default:
			c.add(Warning, "fk_to_cloud_delete", t, lead+"With "+policy+" lokadb refuses such a DELETE or UPDATE as soon as one organization is moved, the rows referencing it or not."+fix+alt, nil)
		}
	}
	return nil
}

// linkedToOrganizations: the cloud tables that hold a foreign key to t and one to the
// organizations table (tenant_users: user_id -> users, tenant_id -> tenants).
func (c *checker) linkedToOrganizations(t string, orgTables map[string]bool) []string {
	var orgs []string
	for o := range orgTables {
		orgs = append(orgs, o)
	}
	if len(orgs) == 0 || c.tx == nil {
		return nil
	}
	rows, err := c.rows(`SELECT DISTINCT `+relKeySQL("l")+` FROM pg_constraint a
		JOIN pg_constraint b ON b.conrelid = a.conrelid AND b.contype = 'f'
		JOIN pg_class l ON l.oid = a.conrelid
		JOIN pg_class ta ON ta.oid = a.confrelid JOIN pg_class tb ON tb.oid = b.confrelid
		WHERE a.contype = 'f' AND `+relKeySQL("ta")+` = $1 AND `+relKeySQL("tb")+` = ANY($2) ORDER BY 1`, t, orgs)
	if err != nil {
		return nil
	}
	var out []string
	for _, r := range rows {
		if !slices.ContainsFunc(c.spec.Tables, func(st admin.SpecTable) bool { return st.Name == r[0] }) { // a cloud table
			out = append(out, r[0])
		}
	}
	return out
}

// uniqueColumn: a unique index of column col alone, without a predicate, is on table t (as
// enable's map of a unique column needs).
func (c *checker) uniqueColumn(t, col string) bool {
	r, err := c.rows(`SELECT 1 FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indrelid IN (to_regclass($1), to_regclass($2)) AND i.indisunique AND i.indnkeyatts = 1
		  AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indimmediate AND a.attname = $3 LIMIT 1`, qk(t), qk(admin.Helper(t, "__cloud")), col)
	return err == nil && len(r) > 0
}

// catalogRows is c.rows where there is a database; without one (a check of a report alone) no rows.
func (c *checker) catalogRows(sql string, args ...any) ([][]string, error) {
	if c.tx == nil {
		return nil, nil
	}
	return c.rows(sql, args...)
}
