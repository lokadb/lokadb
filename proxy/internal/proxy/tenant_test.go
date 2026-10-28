package proxy

import (
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"
)

func TestTenantSetRecognized(t *testing.T) {
	cases := []struct {
		sql     string
		ok, err bool
		want    TenantSet
	}{
		{`SET LOCAL lokadb.tenant = 42`, true, false, TenantSet{Value: "42", Local: true}},
		{`SET lokadb.tenant = '007'`, true, false, TenantSet{Value: "7"}},
		{`SET lokadb.tenant TO '*'`, true, false, TenantSet{Value: "*"}},
		{`SET LOCAL lokadb.tenant = ''`, true, false, TenantSet{Local: true}},
		{`SET lokadb.tenant = 'acme'`, true, true, TenantSet{}},
		{`SET lokadb.tenant = DEFAULT`, true, false, TenantSet{}},
		{`RESET lokadb.tenant`, true, false, TenantSet{}},
		{`RESET ALL`, true, false, TenantSet{All: true}},
		{`DISCARD ALL`, true, false, TenantSet{All: true}},
		{`SELECT set_config('lokadb.tenant', $1, true)`, true, false, TenantSet{Param: 1, Local: true}},
		{`select pg_catalog.set_config('lokadb.tenant', '5', false)`, true, false, TenantSet{Value: "5"}},
		{`SELECT set_config('lokadb.tenant', $1, true), * FROM documents`, true, true, TenantSet{}},
		{`SELECT * FROM documents WHERE id = (SELECT set_config('lokadb.tenant', '1', true))::bigint`, true, true, TenantSet{}},
		{`SET LOCAL app.current_tenant = 3`, false, false, TenantSet{}},
		{`SET statement_timeout = 0`, false, false, TenantSet{}},
		{`RESET statement_timeout`, false, false, TenantSet{}},
		{`SELECT set_config('application_name', 'x', false)`, false, false, TenantSet{}},
		{`SELECT 1`, false, false, TenantSet{}},
	}
	for _, c := range cases {
		tree, err := pg.Parse(c.sql)
		if err != nil {
			t.Fatal(c.sql, err)
		}
		got, ok, err := tenantSet(tree.Stmts[0].Stmt, "")
		if ok != c.ok || (err != nil) != c.err || err == nil && got != c.want {
			t.Errorf("%s: got %+v ok=%v err=%v", c.sql, got, ok, err)
		}
	}
	tree, _ := pg.Parse(`SET LOCAL app.current_tenant = 3`)
	if got, ok, _ := tenantSet(tree.Stmts[0].Stmt, "app.current_tenant"); !ok || got.Value != "3" {
		t.Errorf("tenant_setting from the config: %+v %v", got, ok)
	}
}

// The value follows Postgres: SET LOCAL ends with the transaction, a plain SET inside one
// is kept by COMMIT and undone by ROLLBACK, ROLLBACK TO restores what the savepoint saw.
func TestTenantState(t *testing.T) {
	set := func(v string, local bool) TenantSet { return TenantSet{Value: v, Local: local} }
	var s tenantState
	step := func(want string, evs string, sets ...TenantSet) {
		t.Helper()
		var es []TxEvent
		for _, k := range evs {
			es = append(es, TxEvent{Kind: byte(k), SQL: "sp"})
		}
		s.apply(es, sets)
		if got := s.Value(); got != want {
			t.Errorf("after %q: %q, want %q", evs, got, want)
		}
	}
	step("", "N", set("1", true)) // SET LOCAL outside a transaction: no lasting effect
	step("1", "N", set("1", false))
	step("2", "BN", set("2", true))
	step("1", "C") // SET LOCAL ends with the transaction
	step("3", "BN", set("3", false))
	step("1", "X") // a plain SET in a rolled back transaction is undone
	step("4", "BN", set("4", false))
	step("4", "C") // ... and kept by COMMIT
	step("5", "BSN", set("5", true))
	step("4", "T") // ROLLBACK TO: what the savepoint saw
	step("6", "N", set("6", false))
	step("6", "R") // RELEASE keeps the values
	step("6", "C")
	step("", "N", TenantSet{All: true}) // RESET ALL
}
