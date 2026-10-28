package proxy

import (
	"reflect"
	"testing"
)

// compositeMeta: K8's tables. orders (account_id, id), parcels (tracking_no, piece) with an
// organization column of its own, order_items whose FK (account_id, order_id) is onto orders.
func compositeMeta() *Meta {
	types := func(cols ...string) map[string]string {
		m := map[string]string{}
		for _, c := range cols {
			m[c] = "bigint"
		}
		return m
	}
	m := &Meta{
		Tables: map[string]*Table{
			"orders":      {Name: "orders", TenantCol: "account_id", Refs: "orders__refs", Key: `["account_id","id"]`, Cols: []string{"account_id", "id", "title"}, ColType: types("account_id", "id")},
			"order_items": {Name: "order_items", TenantCol: "account_id", Refs: "order_items__refs", Key: `["account_id","order_id","line_no"]`, Cols: []string{"account_id", "order_id", "line_no"}, ColType: types("account_id", "order_id", "line_no")},
			"parcels":     {Name: "parcels", TenantCol: "account_id", Refs: "parcels__refs", Key: `["tracking_no","piece"]`, Cols: []string{"tracking_no", "piece", "account_id", "weight"}, ColType: types("tracking_no", "piece", "account_id", "weight")},
		},
		Lookups: map[string]map[string]string{
			"orders":      {"account_id": "", `["account_id","id"]`: "orders__refs"},
			"order_items": {"account_id": "", `["account_id","order_id","line_no"]`: "order_items__refs", `["account_id","order_id"]`: "orders__refs"},
			"parcels":     {"account_id": "", `["tracking_no","piece"]`: "parcels__refs"},
		},
	}
	if err := loadKeyKinds(func(string) ([][]string, error) { return nil, nil }, m); err != nil {
		panic(err)
	}
	return m
}

func TestTupleHints(t *testing.T) {
	m := compositeMeta()
	cols := []string{"tracking_no", "piece"}
	c := func(v string) ValueRef { return ValueRef{Const: v} }
	for _, tc := range []struct {
		name, sql string
		tuples    []TupleHint
		unhinted  bool
	}{
		{"the whole key", `SELECT * FROM parcels WHERE tracking_no = 1002 AND piece = $1`,
			[]TupleHint{{"parcels", cols, [][]ValueRef{{c("1002"), {Param: 1}}}}}, false},
		{"a row of the key", `SELECT * FROM parcels WHERE (tracking_no, piece) = (1002, 2)`,
			[]TupleHint{{"parcels", cols, [][]ValueRef{{c("1002"), c("2")}}}}, false},
		{"rows of the key", `SELECT * FROM parcels p WHERE (p.piece, p.tracking_no) IN ((1, 1001), (2, 1001))`,
			[]TupleHint{{"parcels", cols, [][]ValueRef{{c("1001"), c("1")}, {c("1001"), c("2")}}}}, false},
		{"a product of lists", `SELECT * FROM parcels WHERE tracking_no IN (1001, 1002) AND piece = 1`,
			[]TupleHint{{"parcels", cols, [][]ValueRef{{c("1001"), c("1")}, {c("1002"), c("1")}}}}, false},
		{"part of the key", `SELECT * FROM parcels WHERE tracking_no = 1001`, nil, true},
		// kind A: the organization column of the key names the organization by itself
		{"the organization column in the key", `SELECT * FROM orders WHERE account_id = 1 AND id = 3`, nil, false},
		{"an order's id alone", `SELECT * FROM orders WHERE id = 3`, nil, true},
		// a JOIN binds the other side's columns
		{"through a JOIN", `SELECT p.* FROM parcels p JOIN parcels q ON q.tracking_no = p.tracking_no AND q.piece = p.piece WHERE q.tracking_no = 1 AND q.piece = 2`,
			[]TupleHint{{"parcels", cols, [][]ValueRef{{c("1"), c("2")}}}}, false},
	} {
		a := analyze(tc.sql, m)
		if a.ParseError || a.Unhinted != tc.unhinted || !reflect.DeepEqual(a.Tuples, tc.tuples) {
			t.Errorf("%s: unhinted %v tuples %+v hints %+v gaps %+v", tc.name, a.Unhinted, a.Tuples, a.Hints, a.Gaps)
		}
	}
}

func TestResolveOrgTuples(t *testing.T) {
	m := compositeMeta()
	if !reflect.DeepEqual(m.TupleKinds["parcels__refs"], []string{"int", "int"}) || m.KeyKinds["parcels__refs"] != kindTuple {
		t.Fatalf("kinds %v %v", m.TupleKinds, m.KeyKinds)
	}
	s := &session{meta: m, orgCache: map[string]map[string]string{
		"parcels__refs": {`["1001","1"]`: "1", `["1001","2"]`: "1", `["2003","1"]`: "2"},
	}}
	for _, c := range []struct {
		sql, org        string
		lookup, unknown bool
	}{
		{`SELECT * FROM parcels WHERE tracking_no = 1001 AND piece = 2`, "1", false, false},
		{`SELECT * FROM parcels WHERE tracking_no = 0001001 AND piece = 2`, "1", false, false},          // canonical
		{`SELECT * FROM parcels WHERE (tracking_no, piece) IN ((1001, 1), (2003, 1))`, "", false, true}, // two organizations: the policy's (R5)
		{`SELECT * FROM parcels WHERE tracking_no = 1009 AND piece = 1`, "", true, false},
	} {
		org, incomplete, unknown, _ := s.resolveOrg(analyze(c.sql, m), constOnly, false)
		if org != c.org || incomplete != c.lookup || unknown != c.unknown {
			t.Errorf("%s: org %q incomplete %v unknown %v", c.sql, org, incomplete, unknown)
		}
	}
}
