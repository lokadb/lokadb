package admin

import "testing"

func TestFKOntoTable(t *testing.T) {
	for _, c := range []struct{ def, refs, table, key, want string }{
		{"FOREIGN KEY (document_id) REFERENCES documents__refs(id) ON DELETE SET NULL", "documents__refs", "documents", "id",
			`FOREIGN KEY (document_id) REFERENCES "documents"("id") ON DELETE SET NULL`},
		{`FOREIGN KEY (customer_id) REFERENCES public."Customer__refs"(id) DEFERRABLE INITIALLY DEFERRED NOT VALID`, "Customer__refs", "Customer", "customer_id",
			`FOREIGN KEY (customer_id) REFERENCES "Customer"("customer_id") DEFERRABLE INITIALLY DEFERRED`},
		{"FOREIGN KEY (a) REFERENCES users(id)", "documents__refs", "documents", "id", "FOREIGN KEY (a) REFERENCES users(id)"},
		// a tenant-scoped FK, a composite key (compkey.go)
		{"FOREIGN KEY (account_id, document_id) REFERENCES documents__refs(tenant, id)", "documents__refs", "documents", "id",
			`FOREIGN KEY (account_id, document_id) REFERENCES "documents"("organization_id", "id")`},
		{"FOREIGN KEY (account_id, order_id) REFERENCES orders__refs(id_1, id_2) ON DELETE CASCADE", "orders__refs", "orders", `["account_id","id"]`,
			`FOREIGN KEY (account_id, order_id) REFERENCES "orders"("account_id", "id") ON DELETE CASCADE`},
	} {
		if got := fkOntoTable(c.def, c.refs, c.table, c.key, "organization_id"); got != c.want {
			t.Errorf("fkOntoTable(%q) = %q, want %q", c.def, got, c.want)
		}
	}
}
