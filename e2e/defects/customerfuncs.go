package main

// Review of admin (A2, A4): the application's functions that a moved table's CHECK uses are
// created in the organization's database owned by the application's role, not by the
// administrator whose connection move-org runs on; an untrusted language is refused; a
// function with named parameters no longer breaks move-org at step 2.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X24", name: "a SECURITY DEFINER function with a named parameter in a CHECK of a moved table (-customer-app): created owned by lokadb_app, not by the administrator; check names it", run: checkCustomerFuncDefiner},
		check{id: "X25", name: "a function of an untrusted language (internal) in a CHECK of a moved table: check says blocker, move-org refuses before it creates anything", run: checkCustomerFuncUntrusted},
	)
}

// X24 (A2, A4).
func checkCustomerFuncDefiner(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION public.valid_title(title text) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$ BEGIN RETURN length(title) < 200; END $$`,
		`ALTER TABLE documents ADD CONSTRAINT documents_title_ok CHECK (public.valid_title(title))`); err != nil {
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
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	out, code := e.checkWith(spec, "-customer", e.customer(), "-customer-app", e.appCustomer())
	f := finding(out, "customer_function_security_definer")
	note(reported(out, "customer_function_security_definer") && strings.Contains(out, "valid_title") && !reported(out, "customer_function_untrusted") && code != 3 && code != 5,
		fmt.Sprintf("check -customer (exit %d): %s", code, f))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	mv, err := e.moveTo(e.customer(), "-customer-app", e.appCustomer())
	if err != nil {
		note(false, "move-org failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	owner := val(e.customer(), `SELECT pg_get_userbyid(proowner) || ' ' || prosecdef::text FROM pg_proc WHERE proname = 'valid_title'`)
	bad := val(e.customer(), `SELECT count(*) FROM documents WHERE NOT public.valid_title(title)`)
	note(owner == "lokadb_app true" && bad == "0", fmt.Sprintf("valid_title at the organization: owner and SECURITY DEFINER %q (want lokadb_app true), rows failing it %s", owner, bad))
	return ok, strings.Join(notes, "; "), nil
}

// X25 (A2).
func checkCustomerFuncUntrusted(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION public.x25_abs(i int) RETURNS int LANGUAGE internal IMMUTABLE STRICT AS 'int4abs'`,
		`ALTER TABLE documents ADD CONSTRAINT documents_id_nonneg CHECK (public.x25_abs(id::int) >= 0)`); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(spec, "-customer", e.customer(), "-customer-app", e.appCustomer())
	f := finding(out, "customer_function_untrusted")
	blocker := reported(out, "customer_function_untrusted") && strings.Contains(out, "x25_abs") && code == 3
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	mv, err := e.moveTo(e.customer(), "-customer-app", e.appCustomer())
	made := ""
	if v, qerr := one(e.ctx, e.customer(), `SELECT count(*) FROM pg_proc WHERE proname = 'x25_abs'`); qerr == nil {
		made = v
	}
	tables, _ := one(e.ctx, e.customer(), `SELECT count(*) FROM pg_class WHERE relname IN ('documents', 'documents__o1')`)
	refused := err != nil && strings.Contains(mv, "not trusted")
	return blocker && refused && made == "0" && tables == "0",
		fmt.Sprintf("check -customer (exit %d, want 3): %s; move-org: %s; functions x25_abs there %s, tables there %s",
			code, f, map[bool]string{true: "refused: " + lastErr(mv), false: "not refused: " + lastErr(mv)}[refused], made, tables), nil
}
