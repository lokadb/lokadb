package main

// The organization's tables go to schema lokadb of its database when public cannot take them:
// a table of the customer's has one of their names, or the role may not create there.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "C9", name: "a table of the customer's named like a LokaDB table: the move goes to schema lokadb and leaves it alone", run: checkSchemaClash},
		check{id: "C10", name: "a role without CREATE on public but on the database: the move goes to schema lokadb", run: checkSchemaNoPublic},
	)
}

func (e *env) moveTo(customer string, extra ...string) (string, error) {
	p, _ := e.specFile(spec)
	return e.lokadb(append([]string{"move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-customer", customer, "-publisher", e.cloud()}, extra...)...)
}

// C9.
func checkSchemaClash(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.customer(), "CREATE TABLE public.documents (id int PRIMARY KEY, note text)",
		"INSERT INTO public.documents VALUES (1, 'the customer''s own')"); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(spec, "-customer", e.customer(), "-customer-app", e.appCustomer())
	blocked := reported(out, "customer_table_exists")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	mv, moveErr := e.moveTo(e.customer(), "-customer-app", e.appCustomer())
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	own := val(e.customer(), "SELECT note FROM public.documents")
	there := val(e.customer(), "SELECT count(*) FROM lokadb.documents")
	viaCloud := val(e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	// the proxy replays the application's DDL there, not on the customer's own table
	ddl := "not tried"
	if moveErr == nil {
		stop, err := e.proxyWith(map[string]string{"1": e.appCustomer()})
		if err != nil {
			return false, "", err
		}
		defer stop()
		c, err := e.viaProxy()
		if err != nil {
			return false, "", err
		}
		defer c.Close(e.ctx)
		if _, err := simple(e.ctx, c, "ALTER TABLE documents ADD COLUMN c9 text"); err != nil {
			ddl = "error: " + firstErrLine(err)
		} else {
			ddl = fmt.Sprintf("lokadb.documents.c9 %s, public.documents.c9 %s",
				val(e.customer(), "SELECT count(*) FROM pg_attribute WHERE attrelid = 'lokadb.documents'::regclass AND attname = 'c9'"),
				val(e.customer(), "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.documents'::regclass AND attname = 'c9'"))
		}
	}
	ok := !blocked && code != 3 && reported(out, "customer_schema") && moveErr == nil &&
		own == "the customer's own" && there == "100" && viaCloud == "100" && ddl == "lokadb.documents.c9 1, public.documents.c9 0"
	return ok, fmt.Sprintf("check: %s (exit %d), customer_table_exists %v, %s; %s; the customer's own row: %q; lokadb.documents: %s, through the cloud: %s; DDL through the proxy: %s",
		verdict(out), code, blocked, finding(out, "customer_schema"), moved, own, there, viaCloud, ddl), nil
}

// C10.
func checkSchemaNoPublic(e *env) (bool, string, error) {
	adm := "np_" + e.db
	if err := role(e, "customer", adm); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON DATABASE "+e.db+" TO "+adm); err != nil {
		return false, "", err
	}
	admDSN := dsn("customer", e.db, adm)
	out, _ := e.checkWith(spec, "-customer", admDSN)
	blocked := reported(out, "customer_privileges")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	mv, moveErr := e.moveTo(admDSN)
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	there, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM lokadb.documents")
	owner, _ := one(e.ctx, e.customer(), "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = 'lokadb'")
	ok := !blocked && moveErr == nil && there == "100" && strings.Contains(owner, adm)
	return ok, fmt.Sprintf("check: customer_privileges %v, %s; %s; lokadb.documents: %s, schema owner %s",
		blocked, finding(out, "customer_schema"), moved, there, owner), nil
}
