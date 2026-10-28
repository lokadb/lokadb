package admin

import "github.com/lokadb/lokadb/proxy/internal/rel"

// The organizations table (block 1, tasks of 28.10): a table outside the spec that an
// organization column of the spec's tables references by FK onto its id is where the
// organizations themselves are kept (tenants, organizations). A row of it belongs to the
// organization whose id it is, so it is a cloud table whose organization column is its id:
// deleting it, or changing its key, concerns that organization only, and the FK actions
// (lokadb_fk_<table>_<FK>) look in that organization's database alone. enable and check take
// such a table as declared in routing.cloud_tables ({"table": T, "tenant_column": "id"}).

// ImpliedCloudTables lists the organizations tables of spec that its routing does not
// declare. rows runs a catalog query and gives its rows as text.
func ImpliedCloudTables(rows func(sql string, args ...any) ([][]string, error), spec Spec) ([]CloudTable, error) {
	var names, cols []string
	in := map[string]bool{}
	for _, st := range spec.Tables {
		in[st.Name] = true
		if st.TenantColumn != "" {
			// after enable the FK is on the table and on its cloud partition
			names, cols = append(names, st.Name, rel.Helper(st.Name, "__cloud")), append(cols, st.TenantColumn, st.TenantColumn)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	declared := map[string]bool{}
	if spec.Routing != nil {
		for _, ct := range spec.Routing.CloudTables {
			declared[ct.Table] = true
		}
	}
	key := func(a string) string {
		return rel.KeyOfSQL("(SELECT nspname::text FROM pg_catalog.pg_namespace WHERE oid = "+a+".relnamespace)", a+".relname::text")
	}
	res, err := rows(`SELECT DISTINCT `+key("d")+` FROM pg_constraint c
		JOIN pg_class s ON s.oid = c.conrelid JOIN pg_class d ON d.oid = c.confrelid
		JOIN pg_attribute sa ON sa.attrelid = c.conrelid AND sa.attnum = c.conkey[1]
		JOIN pg_attribute da ON da.attrelid = c.confrelid AND da.attnum = c.confkey[1]
		WHERE c.contype = 'f' AND array_length(c.conkey, 1) = 1 AND da.attname = 'id' AND d.relkind = 'r'
		  AND (`+key("s")+`, sa.attname::text) IN (SELECT * FROM unnest($1::text[], $2::text[]))
		ORDER BY 1`, names, cols)
	if err != nil {
		return nil, err
	}
	var out []CloudTable
	for _, r := range res {
		if !in[r[0]] && !declared[r[0]] {
			out = append(out, CloudTable{Table: r[0], TenantColumn: "id"})
		}
	}
	return out, nil
}
