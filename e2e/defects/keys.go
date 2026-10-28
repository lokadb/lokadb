package main

// Block 4, item 1: a table whose key is not called id (Pagila: customer_id, inventory_id),
// and FKs onto such keys.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X5", name: "keys named <table>_id (Pagila): check, enable, move-org and the proxy's routing by them", run: checkTableKeys},
		check{id: "X6", name: "a tenant-scoped FK (organization column, key): check, enable, move-org keep it", run: checkTenantScopedFK},
		check{id: "X7", name: "check: a polymorphic parent (<x>_id with <x>_type) is not followed; a cloud table referencing LokaDB rows is a warning", run: checkParentFindings},
	)
}

// X5. store stays in the cloud; customer and inventory belong to a store; rental to the store
// of its inventory, and it references customer by customer_id.
func checkTableKeys(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE store (store_id serial PRIMARY KEY, address text)",
		"CREATE TABLE customer (customer_id serial PRIMARY KEY, store_id int NOT NULL REFERENCES store, first_name text)",
		"CREATE TABLE inventory (inventory_id serial PRIMARY KEY, store_id int NOT NULL REFERENCES store)",
		"CREATE TABLE rental (rental_id serial PRIMARY KEY, inventory_id int NOT NULL REFERENCES inventory, customer_id int NOT NULL REFERENCES customer, note text)",
		"ALTER TABLE store OWNER TO app", "ALTER TABLE customer OWNER TO app", "ALTER TABLE inventory OWNER TO app", "ALTER TABLE rental OWNER TO app",
		"INSERT INTO store (address) VALUES ('one'), ('two')",
		// odd ids belong to store 2, even ones to store 1
		"INSERT INTO customer (store_id, first_name) SELECT 1 + g % 2, 'c' || g FROM generate_series(1, 10) g",
		"INSERT INTO inventory (store_id) SELECT 1 + g % 2 FROM generate_series(1, 10) g",
		"INSERT INTO rental (inventory_id, customer_id, note) SELECT 1 + g % 10, 1 + g % 10, 'r' || g FROM generate_series(0, 19) g"); err != nil {
		return false, "", err
	}
	spc := `{"tables": [
  {"name": "customer", "tenant_column": "store_id"},
  {"name": "inventory", "tenant_column": "store_id"},
  {"name": "rental", "parent_table": "inventory", "parent_column": "inventory_id"}
]}`
	out, code := e.checkWith(spc)
	var bad []string
	for _, c := range []string{"id_missing", "id_type", "fk_not_to_id", "fk_multicolumn", "child_without_fk"} {
		if reported(out, c) {
			bad = append(bad, c)
		}
	}
	e.spec = spc
	if err := e.enable(); err != nil {
		return false, fmt.Sprintf("check: %s (exit %d), key findings %v; enable failed: %s", verdict(out), code, bad, lastErr(err.Error())), nil
	}
	lookups, err := one(e.ctx, e.cloud(), "SELECT string_agg(table_name || '.' || column_name || '>' || coalesce(refs_table, '-'), ' ' ORDER BY table_name || '.' || column_name || '>' || coalesce(refs_table, '-')) FROM lokadb.lookups")
	if err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, fmt.Sprintf("check: %s, key findings %v; lookups %s; move-org failed: %s", verdict(out), bad, lookups, lastErr(mv)), nil
	}
	shape, err := one(e.ctx, e.customer(), `SELECT (SELECT count(*) FROM rental)::text || ' rentals, customer key '
		|| (SELECT string_agg(a.attname, ',') FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
			WHERE i.indrelid = 'customer'::regclass AND i.indisprimary)
		|| ', FK rental -> customer ' || (SELECT coalesce(string_agg(pg_get_constraintdef(oid), '; '), 'none') FROM pg_constraint
			WHERE conrelid = 'rental'::regclass AND confrelid = 'customer'::regclass)`)
	if err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	routed := "ok"
	if err := e.script("UPDATE customer SET first_name = 'x5' WHERE customer_id = 2",
		"INSERT INTO rental (inventory_id, customer_id, note) VALUES (2, 2, 'x5')"); err != nil {
		routed = "error: " + firstErrLine(err)
	}
	there, _ := one(e.ctx, e.customer(), "SELECT (SELECT first_name FROM customer WHERE customer_id = 2) || ', ' || (SELECT count(*) FROM rental WHERE note = 'x5')")
	ok := len(bad) == 0 && code != 4 && strings.Contains(lookups, "customer.customer_id>customer__refs") &&
		strings.Contains(lookups, "rental.customer_id>customer__refs") && strings.Contains(lookups, "rental.inventory_id>inventory__refs") &&
		strings.HasPrefix(shape, "10 rentals, customer key customer_id, FK rental -> customer FOREIGN KEY (store_id, customer_id) REFERENCES customer") &&
		routed == "ok" && there == "x5, 1"
	return ok, fmt.Sprintf("check: %s (exit %d), key findings %v; lookups %s; at the organization: %s; through the proxy: %s, there: %s",
		verdict(out), code, bad, lookups, shape, routed, there), nil
}

// X6. Citus and Rails style: messages reference conversations by (account_id, id), so a
// message cannot point at another account's conversation.
func checkTenantScopedFK(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE conversations (id bigserial PRIMARY KEY, account_id bigint NOT NULL, subject text, UNIQUE (account_id, id))",
		"CREATE TABLE messages (id bigserial PRIMARY KEY, account_id bigint NOT NULL, conversation_id bigint NOT NULL, body text, FOREIGN KEY (account_id, conversation_id) REFERENCES conversations (account_id, id) ON DELETE CASCADE)",
		"ALTER TABLE conversations OWNER TO app", "ALTER TABLE messages OWNER TO app",
		"INSERT INTO conversations (account_id, subject) SELECT 1 + g % 2, 's' || g FROM generate_series(1, 10) g",
		"INSERT INTO messages (account_id, conversation_id, body) SELECT c.account_id, c.id, 'm' || c.id FROM conversations c"); err != nil {
		return false, "", err
	}
	spc := `{"tables": [{"name": "conversations", "tenant_column": "account_id"}, {"name": "messages", "tenant_column": "account_id"}]}`
	out, code := e.checkWith(spc)
	multi := reported(out, "fk_multicolumn")
	e.spec = spc
	if err := e.enable(); err != nil {
		return false, fmt.Sprintf("check: %s (exit %d), fk_multicolumn %v; enable failed: %s", verdict(out), code, multi, lastErr(err.Error())), nil
	}
	lookup, _ := one(e.ctx, e.cloud(), "SELECT coalesce((SELECT refs_table FROM lokadb.lookups WHERE table_name = 'messages' AND column_name = 'conversation_id'), 'none')")
	if mv, err := e.move(1); err != nil {
		return false, fmt.Sprintf("check: %s, fk_multicolumn %v; move-org failed: %s", verdict(out), multi, lastErr(mv)), nil
	}
	fk, _ := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(pg_get_constraintdef(oid), '; '), 'none') FROM pg_constraint WHERE conrelid = 'messages'::regclass AND contype = 'f'")
	// another account's conversation (2 belongs to account 1, 1 to account 2)
	cross := execSQL(e.ctx, e.customer(), "INSERT INTO messages (account_id, conversation_id, body) VALUES (1, 1, 'x6')")
	crossRes := "accepted"
	if cross != nil {
		crossRes = "refused: " + firstErrLine(cross)
	}
	// through the cloud: a delete of organization 1's conversation cascades there
	del := execSQL(e.ctx, e.cloud(), "DELETE FROM conversations WHERE account_id = 1 AND id = 2")
	left, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM messages WHERE conversation_id = 2")
	ok := !multi && code != 4 && lookup == "conversations__refs" &&
		strings.Contains(fk, "FOREIGN KEY (account_id, conversation_id) REFERENCES conversations(account_id, id) ON DELETE CASCADE") &&
		strings.Contains(crossRes, "refused") && del == nil && left == "0"
	return ok, fmt.Sprintf("check: %s (exit %d), fk_multicolumn %v; lookup of messages.conversation_id: %s; at the organization: %s; a message of another account's conversation: %s; delete through the cloud: %v, messages left %s",
		verdict(out), code, multi, lookup, fk, crossRes, del, left), nil
}

// X7. taggings.taggable_id points at documents or at users, as taggable_type says; comments
// stays out of the spec but references documents.
func checkParentFindings(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE taggings (id bigserial PRIMARY KEY, taggable_type text NOT NULL, taggable_id bigint NOT NULL, tag text)",
		"CREATE TABLE comments (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents, body text)",
		"INSERT INTO taggings (taggable_type, taggable_id, tag) VALUES ('Document', 1, 'a'), ('User', 2, 'b')"); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(`{"tables": [{"name": "documents", "tenant_column": "organization_id"},
  {"name": "taggings", "parent_table": "documents", "parent_column": "taggable_id"}]}`)
	poly := reported(out, "parent_polymorphic")
	cloudRefs := strings.Contains(out, "Warnings") && reported(out, "cloud_references") && strings.Contains(out, "comments.document_id -> documents")
	return poly && cloudRefs && code == 4, fmt.Sprintf("check: %s (exit %d); parent_polymorphic %v; cloud_references as a warning %v; %s",
		verdict(out), code, poly, cloudRefs, finding(out, "cloud_references")), nil
}
