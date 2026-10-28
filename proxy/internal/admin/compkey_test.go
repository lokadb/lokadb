package admin

import (
	"reflect"
	"testing"
)

func TestCompositeKey(t *testing.T) {
	k := KeyName([]string{"account_id", "id"})
	if k != `["account_id","id"]` || !Composite(k) || !reflect.DeepEqual(KeyCols(k), []string{"account_id", "id"}) {
		t.Fatalf("key %q, cols %v", k, KeyCols(k))
	}
	if KeyName([]string{"id"}) != "id" || Composite("id") || !reflect.DeepEqual(RefsKeyCols("customer_id"), []string{"id"}) {
		t.Errorf("one column")
	}
	if !reflect.DeepEqual(RefsKeyCols(k), []string{"id_1", "id_2"}) {
		t.Errorf("refs columns %v", RefsKeyCols(k))
	}
	if got := refsDDL(k, `["bigint","bigint"]`, "bigint"); got != `("id_1" bigint, "id_2" bigint, tenant bigint, PRIMARY KEY ("id_1", "id_2"), UNIQUE (tenant, "id_1", "id_2"))` {
		t.Errorf("refs %s", got)
	}
	if got := refsDDL("id", "uuid", "bigint"); got != "(id uuid PRIMARY KEY, tenant bigint, UNIQUE (tenant, id))" {
		t.Errorf("refs %s", got)
	}
	// FKs onto the key
	for _, c := range []struct {
		refs []string
		key  string
		want []string
		ok   bool
	}{
		{[]string{"id"}, "id", []string{"id"}, true},
		{[]string{"account_id", "id"}, "id", []string{"tenant", "id"}, true},
		{[]string{"account_id"}, "id", nil, false},
		{[]string{"account_id", "id"}, k, []string{"id_1", "id_2"}, true},
		{[]string{"id", "account_id"}, k, []string{"id_2", "id_1"}, true},
		{[]string{"id"}, k, []string{"id_2"}, false}, // part of the key
		{[]string{"order_id", "line_no"}, `["order_id","line_no"]`, []string{"id_1", "id_2"}, true},
	} {
		got, ok := RefsColumns(c.refs, c.key, "account_id")
		if ok != c.ok || ok && !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v onto %s: %v %v", c.refs, c.key, got, ok)
		}
	}
	if col, ok := fkLookup([]string{"acc", "ord"}, []string{"id_2", "id_1"}); !ok || col != `["ord","acc"]` {
		t.Errorf("lookup %s %v", col, ok)
	}
	if col, ok := fkLookup([]string{"acc", "doc"}, []string{"tenant", "id"}); !ok || col != "doc" {
		t.Errorf("lookup %s %v", col, ok)
	}
}

func TestBatchKey(t *testing.T) {
	one := newBKey("id", "bigint")
	if one.sel("x.") != `x."id" AS k` || one.within("x.") != `x."id" `+batchFrom+` $1::bigint AND x."id" <= $2::bigint` || one.lastOf("b") != "(SELECT k FROM b ORDER BY k DESC LIMIT 1)::text" {
		t.Errorf("one column: %s | %s | %s", one.sel("x."), one.within("x."), one.lastOf("b"))
	}
	two := newBKey(`["order_id","line_no"]`, `["bigint","integer"]`)
	if two.sel("x.") != `x."order_id" AS k_1, x."line_no" AS k_2` ||
		two.within("x.") != `(x."order_id", x."line_no") `+batchFrom+` (($1::jsonb ->> 0)::bigint, ($1::jsonb ->> 1)::integer) AND (x."order_id", x."line_no") <= (($2::jsonb ->> 0)::bigint, ($2::jsonb ->> 1)::integer)` ||
		two.lastOf("b") != "(SELECT jsonb_build_array(k_1, k_2) FROM b ORDER BY k_1 DESC, k_2 DESC LIMIT 1)::text" ||
		two.text("x.") != `jsonb_build_array(x."order_id", x."line_no")` {
		t.Errorf("composite: %s | %s | %s", two.sel("x."), two.within("x."), two.lastOf("b"))
	}
}
