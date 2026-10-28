package main

// Composite primary keys (tasks/composite-keys.md). lokadb found a row's organization by a key
// of one column; a table whose primary key has several columns was refused unless it also had
// an id unique across organizations. Now the key is the list of the primary key's columns:
// refs keeps the whole tuple, FKs onto such a key point at refs, the proxy finds the
// organization when a statement binds every column of the key.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "K8", name: "composite keys: (account_id, id) numbered per organization and (order_id, line_no): check, enable, a move, routing by the whole key under reject, FKs from a cloud table, refs-diff", run: checkCompositeKeys},
		check{id: "K10", name: "a child whose organization comes through a composite FK: check says unsupported, enable refuses clearly", run: checkCompositeChild},
	)
}

const compositeSpec = `{"tables": [
  {"name": "orders", "tenant_column": "account_id"},
  {"name": "order_items", "tenant_column": "account_id"},
  {"name": "parcels", "tenant_column": "account_id"}
]}`

// compositeSchema makes K8's tables: orders (kind A), order_items (an FK onto orders),
// parcels (kind B) and the cloud's invoices referencing orders.
func compositeSchema(e *env) error {
	e.spec = compositeSpec
	return execSQL(e.ctx, e.cloud(),
		// kind A: numbered per organization, the id alone repeats across them (Citus, Rails 7.1)
		"CREATE TABLE orders (account_id bigint NOT NULL, id bigint NOT NULL, title text, PRIMARY KEY (account_id, id))",
		"CREATE TABLE order_items (account_id bigint NOT NULL, order_id bigint NOT NULL, line_no int NOT NULL, qty int,"+
			" PRIMARY KEY (account_id, order_id, line_no), FOREIGN KEY (account_id, order_id) REFERENCES orders ON DELETE CASCADE)",
		// kind B: a key without the organization column, unique across organizations, no id
		"CREATE TABLE parcels (tracking_no bigint NOT NULL, piece int NOT NULL, account_id bigint NOT NULL, weight int, PRIMARY KEY (tracking_no, piece))",
		// a cloud table referencing the composite key
		"CREATE TABLE invoices (id bigserial PRIMARY KEY, account_id bigint NOT NULL, order_id bigint NOT NULL, FOREIGN KEY (account_id, order_id) REFERENCES orders)",
		"ALTER TABLE orders OWNER TO app", "ALTER TABLE order_items OWNER TO app", "ALTER TABLE parcels OWNER TO app", "ALTER TABLE invoices OWNER TO app",
		"INSERT INTO orders SELECT a, g, 'o' || a || '-' || g FROM generate_series(1, 2) a, generate_series(1, 5) g",
		"INSERT INTO order_items SELECT o.account_id, o.id, l, l FROM orders o, generate_series(1, 3) l",
		"INSERT INTO parcels SELECT a * 1000 + g, p, a, p * 10 FROM generate_series(1, 2) a, generate_series(1, 4) g, generate_series(1, 2) p",
		"INSERT INTO invoices (account_id, order_id) SELECT account_id, id FROM orders")
}

func checkCompositeKeys(e *env) (bool, string, error) {
	if err := compositeSchema(e); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	out, _ := e.checkWith(compositeSpec)
	bad := []string{}
	for _, code := range []string{"id_missing", "id_type", "id_not_unique", "fk_multicolumn", "fk_not_to_id"} {
		if reported(out, code) {
			bad = append(bad, code+": "+finding(out, code))
		}
	}
	note(len(bad) == 0, "check: "+verdict(out)+"; "+strings.Join(bad, "; "))
	if _, err := e.lokadb("enable", "-db", e.cloud(), "-spec", mustSpec(e, compositeSpec), "-owner", "app"); err != nil {
		note(false, "enable: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if _, err := e.move(1); err != nil {
		note(false, "move-org 1: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	n, _ := one(e.ctx, e.customer(), "SELECT (SELECT count(*) FROM orders)::text || ',' || (SELECT count(*) FROM order_items) || ',' || (SELECT count(*) FROM parcels)")
	note(n == "5,15,8", "orders, items, parcels of organization 1 in its database: "+n)
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(sql string, args ...string) string {
		var params [][]byte
		for _, a := range args {
			params = append(params, []byte(a))
		}
		res := c.ExecParams(e.ctx, sql, params, nil, nil, nil).Read()
		if res.Err != nil {
			var pe *pgconn.PgError
			if errors.As(res.Err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
				return "refused"
			}
			return "error: " + firstErrLine(res.Err)
		}
		if len(res.Rows) == 1 && len(res.Rows[0]) == 1 {
			return string(res.Rows[0][0])
		}
		return fmt.Sprintf("%d rows", len(res.Rows))
	}
	// both organizations number their orders 1..5; parcels are 1001.. and 2001..
	for _, c := range []struct{ what, sql, want string }{
		{"an order by the whole key, moved organization", `SELECT title FROM orders WHERE account_id = 1 AND id = 3`, "o1-3"},
		{"an order by the whole key, cloud organization", `SELECT title FROM orders WHERE account_id = 2 AND id = 3`, "o2-3"},
		{"orders by part of their key (the id repeats)", `SELECT count(*) FROM orders WHERE id = 3`, "refused"},
		{"a parcel by the whole key (no organization column), moved", `SELECT weight FROM parcels WHERE tracking_no = 1002 AND piece = 2`, "20"},
		{"a parcel by the whole key (no organization column), cloud", `SELECT weight FROM parcels WHERE tracking_no = 2003 AND piece = 1`, "10"},
		{"parcels by rows of the key", `SELECT count(*) FROM parcels WHERE (tracking_no, piece) IN ((1001, 1), (1001, 2))`, "2"},
		{"parcels by part of the key", `SELECT count(*) FROM parcels WHERE tracking_no = 1001`, "refused"},
	} {
		got := run(c.sql)
		note(got == c.want, fmt.Sprintf("%s: %s", c.what, got))
	}
	got := run(`INSERT INTO order_items (account_id, order_id, line_no, qty) VALUES ($1, $2, $3, $4)`, "1", "1", "9", "9")
	n, _ = one(e.ctx, e.customer(), "SELECT count(*) FROM order_items WHERE order_id = 1 AND line_no = 9")
	note(got == "0 rows" && n == "1", "a new item of organization 1: "+got+", in its database: "+n)
	got = run(`DELETE FROM orders WHERE account_id = 1 AND id = 5`)
	n, _ = one(e.ctx, e.customer(), "SELECT count(*) FROM order_items WHERE order_id = 5")
	note(strings.HasPrefix(got, "error") && strings.Contains(got, "invoices"), "deleting an order a cloud invoice references: "+got)
	// a cloud invoice for an order of the moved organization: the FK checks refs
	got = run(`INSERT INTO invoices (account_id, order_id) VALUES (1, 4)`)
	note(got == "0 rows", "a cloud invoice of a moved order: "+got)
	got = run(`INSERT INTO invoices (account_id, order_id) VALUES (1, 14)`)
	note(strings.HasPrefix(got, "error") && strings.Contains(got, "foreign key"), "a cloud invoice of an order that does not exist (1, 14): "+got)
	diff, code, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	note(code == 0, fmt.Sprintf("refs-diff exit %d: %s", code, oneLine(diff)))
	return ok, strings.Join(notes, "; "), nil
}

func mustSpec(e *env, body string) string {
	p, _ := e.specFile(body)
	return p
}

func checkCompositeChild(e *env) (bool, string, error) {
	spec := `{"tables": [
  {"name": "orders", "tenant_column": "account_id"},
  {"name": "shipments", "parent_table": "orders", "parent_column": "order_id"}
]}`
	e.spec = spec
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE orders (account_id bigint NOT NULL, id bigint NOT NULL, PRIMARY KEY (account_id, id))",
		"CREATE TABLE shipments (id bigserial PRIMARY KEY, order_account bigint, order_id bigint, FOREIGN KEY (order_account, order_id) REFERENCES orders)",
		"ALTER TABLE orders OWNER TO app", "ALTER TABLE shipments OWNER TO app",
		"INSERT INTO orders SELECT a, g FROM generate_series(1, 2) a, generate_series(1, 3) g",
		"INSERT INTO shipments (order_account, order_id) SELECT account_id, id FROM orders"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	out, _ := e.checkWith(spec)
	s := "check: " + finding(out, "child_composite_fk")
	if !reported(out, "child_composite_fk") || !strings.Contains(sectionOf(out, "Not supported"), "child_composite_fk") {
		ok, s = false, "WRONG "+s
	}
	notes = append(notes, s)
	eo, eerr := e.lokadb("enable", "-db", e.cloud(), "-spec", mustSpec(e, spec), "-owner", "app")
	s = "enable: " + oneLine(eo)
	if eerr == nil || !strings.Contains(eo, "composite") {
		ok, s = false, "WRONG "+s
	}
	notes = append(notes, s)
	return ok, strings.Join(notes, "; "), nil
}
