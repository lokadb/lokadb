package main

// The second review of lokadb check (tasks/review/check.md), findings 7, 10 and 12: what check
// said ready while enable or the proxy took the data otherwise.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "CK7", name: "check blocks rows whose integer organization is negative (lokadb keeps -2 and -1, the proxy takes any negative for no organization), and only those", run: checkNegativeTenant},
		check{id: "CK10", name: "check takes a cloud table of routing.cloud_tables whose organization column is a uuid in a database with uuid organizations, as enable does", run: checkCloudTableUUID},
		check{id: "CK12", name: "check reports rows with a NULL key (no primary key, id only UNIQUE) where enable stops on them, and not once they have one", run: checkNullKey},
	)
}

// CK7. check said nothing of integer organizations with tenant_id = -2: they went to the
// partition of rows of no organization, and a statement naming -2 was routed as one of none.
func checkNegativeTenant(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE items (id bigserial PRIMARY KEY, org_id bigint, name text)",
		"ALTER TABLE items OWNER TO app",
		"INSERT INTO items (org_id, name) SELECT 1 + g % 2, 'i' || g FROM generate_series(1, 20) g",
		"INSERT INTO items (org_id, name) VALUES (-2, 'minus two'), (-1, 'minus one'), (NULL, 'nobody')"); err != nil {
		return false, "", err
	}
	const spec = `{"tables": [{"name": "items", "tenant_column": "org_id"}]}`
	x := &expects{ok: true}
	out, code := e.checkWith(spec)
	x.eq("negative organizations: tenant_value_reserved, exit", fmt.Sprint(reported(out, "tenant_value_reserved"), " ", code), "true 3")
	x.eq("it counts both rows", fmt.Sprint(strings.Contains(finding(out, "tenant_value_reserved"), "2 rows")), "true")
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM items WHERE org_id < 0"); err != nil {
		return false, "", err
	}
	out, code = e.checkWith(spec)
	x.eq("only 1, 2 and NULL: no tenant_value_reserved, not blocked", fmt.Sprint(reported(out, "tenant_value_reserved"), " ", code != 3), "false true")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// CK10. cloudTable required an integer organization column of a cloud table, whatever the
// organizations of the database were: SpecError routing_invalid and the verdict invalid_spec,
// although enable takes it.
func checkCloudTableUUID(e *env) (bool, string, error) {
	if err := uuidTenantApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE plans (id bigint PRIMARY KEY, account_id uuid, name text)",
		"ALTER TABLE plans OWNER TO app",
		"INSERT INTO plans VALUES (1, '"+orgA+"', 'free'), (2, '"+orgB+"', 'pro')"); err != nil {
		return false, "", err
	}
	const spec = `{"tables": [
  {"name": "projects", "tenant_column": "account_id"},
  {"name": "tasks", "parent_table": "projects", "parent_column": "project_id"}
], "routing": {"cloud_tables": [{"table": "plans", "tenant_column": "account_id"}]}}`
	x := &expects{ok: true}
	out, code := e.checkWith(spec)
	x.eq("a uuid column in a database of uuid organizations: routing_invalid, exit 5 (invalid spec)", fmt.Sprint(reported(out, "routing_invalid"), " ", code == 5), "false false")
	x.eq("the cloud table is declared", fmt.Sprint(reported(out, "cloud_table_declared")), "true")
	// the integer column next to uuid organizations is still refused, by both
	if err := execSQL(e.ctx, e.cloud(), "CREATE TABLE plans_int (id bigint PRIMARY KEY, org bigint)", "ALTER TABLE plans_int OWNER TO app"); err != nil {
		return false, "", err
	}
	bad := strings.Replace(spec, `"plans", "tenant_column": "account_id"`, `"plans_int", "tenant_column": "org"`, 1)
	out, _ = e.checkWith(bad)
	x.eq("an integer column there: routing_invalid", fmt.Sprint(reported(out, "routing_invalid")), "true")
	// enable takes the declaration check accepted
	e.spec = spec
	err := e.enable()
	x.eq("enable with it", errText(err), errText(nil))
	return x.ok, strings.Join(x.notes, "; "), nil
}

// CK12. A table without a primary key, id bigint UNIQUE with NULLs: check found a unique
// index and looked no further, enable stopped on the NOT NULL of the refs table's id.
func checkNullKey(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE legacy (id bigint UNIQUE, org_id bigint NOT NULL, name text)",
		"ALTER TABLE legacy OWNER TO app",
		"INSERT INTO legacy SELECT g, 1 + g % 2, 'l' || g FROM generate_series(1, 20) g",
		"INSERT INTO legacy (id, org_id, name) VALUES (NULL, 1, 'no key'), (NULL, 2, 'no key either')"); err != nil {
		return false, "", err
	}
	const spec = `{"tables": [{"name": "legacy", "tenant_column": "org_id"}]}`
	x := &expects{ok: true}
	out, code := e.checkWith(spec)
	x.eq("two rows without a key: id_null, exit", fmt.Sprint(reported(out, "id_null"), " ", code), "true 4")
	x.eq("it counts both rows", fmt.Sprint(strings.Contains(finding(out, "id_null"), "2 rows")), "true")
	e.spec = spec
	x.eq("enable stops on them", fmt.Sprint(e.enable() != nil), "true")
	if err := execSQL(e.ctx, e.cloud(), "UPDATE legacy SET id = 100 + org_id WHERE id IS NULL"); err != nil {
		return false, "", err
	}
	out, code = e.checkWith(spec)
	x.eq("every row has a key: no id_null, not unsupported", fmt.Sprint(reported(out, "id_null"), " ", code != 4), "false true")
	x.eq("and enable passes", errText(e.enable()), errText(nil))
	return x.ok, strings.Join(x.notes, "; "), nil
}
