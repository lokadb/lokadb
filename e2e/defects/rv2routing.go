package main

// The 2nd review of routing. X29: a DO block or a function's body naming a LokaDB helper table
// (documents__refs: every organization's keys) was not seen by the proxy's guard, which
// refuses the same statement written plainly. X30: lokadb.org(), inlined into the policy, took
// current_setting from the session's search_path: the application's public.current_setting
// under SET search_path = public, pg_catalog opened every organization's rows.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X29", name: "a DO block, a function made through the proxy and a function of the database that name documents__refs: refused (the guard, the fanout policy reject), as a plain SELECT from it is", run: checkBodiesNameHelpers},
		check{id: "X30", name: "the application's public.current_setting(text, boolean) under SET LOCAL search_path = public, pg_catalog: the policy still keeps the cooperative transaction to its organization", run: checkPolicyPath},
	)
}

// X29.
func checkBodiesNameHelpers(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION public.x29_refs() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM documents__refs'`,
		`ALTER FUNCTION public.x29_refs() OWNER TO app`); err != nil {
		return false, "", err
	}
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
	var notes []string
	ok := true
	try := func(sql, want string) {
		res, err := simple(e.ctx, c, sql)
		got := "accepted"
		if err != nil {
			got = "refused: " + firstErrLine(err)
		} else if len(res) > 0 && len(res[len(res)-1].Rows) > 0 {
			got += ", returned " + string(res[len(res)-1].Rows[0][0])
		}
		good := strings.HasPrefix(got, "refused") && strings.Contains(got, want)
		if !good {
			ok = false
			got = "WRONG " + got
		}
		notes = append(notes, fmt.Sprintf("%s: %s", sql, got))
	}
	try("SELECT count(*) FROM documents__refs", "helper table")
	try("DO $$ BEGIN PERFORM count(*) FROM documents__refs; END $$", "helper table")
	try("CREATE FUNCTION x29_made() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM documents__refs'", "helper table")
	try("SELECT x29_refs()", "lokadb")
	return ok, strings.Join(notes, "; "), nil
}

// X30.
func checkPolicyPath(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE FUNCTION public.current_setting(text, boolean) RETURNS text LANGUAGE sql AS $$ SELECT NULL::text $$`,
		`ALTER FUNCTION public.current_setting(text, boolean) OWNER TO app`); err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	all, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents")
	if err != nil {
		return false, "", err
	}
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
	res, err := simple(e.ctx, c, "BEGIN; SELECT set_config('lokadb.tenant', '2', true); SET LOCAL search_path = public, pg_catalog; SELECT count(*) FROM documents; COMMIT")
	got := ""
	if err != nil {
		got = "error: " + firstErrLine(err)
	} else {
		for _, r := range res {
			if len(r.Rows) > 0 && r.CommandTag.String() == "SELECT 1" {
				got = string(r.Rows[0][0])
			}
		}
	}
	return got == want, fmt.Sprintf("the cooperative transaction of organization 2 counts %s documents (organization 2 has %s, all organizations %s)", got, want, all), nil
}
