package main

// B9.12: the proxy reads the lokadb metadata under the client's role.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "B9.12", name: "a role that cannot read the lokadb metadata gets no other organization's rows; enable lets every role read it", run: checkMetaRole},
	)
}

// B9.12. A role without SELECT on lokadb's tables got no metadata: the proxy did not route it,
// and a statement naming no organization ran unrouted past the fanout policy (lokadb.org unset:
// the policy lokadb_tenant lets every organization's rows through). Now (1) enable grants
// PUBLIC what routing reads, so a role given SELECT on documents alone is routed, and (2) where
// the metadata is there but unreadable (a database enabled before, the grant revoked) statements
// on LokaDB tables are refused instead.
func checkMetaRole(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	for _, r := range []string{"b912a", "b912b"} {
		if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
			fmt.Sprintf(`DO $$ BEGIN CREATE ROLE %s LOGIN PASSWORD '%s'; EXCEPTION WHEN duplicate_object THEN END $$`, r, r)); err != nil {
			return false, "", err
		}
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO b912a, b912b"); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	ok := true
	var notes []string
	via := func(user string) (*pgx.Conn, error) {
		return pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable", e.port, e.db, user, user))
	}
	ask := func(c *pgx.Conn, sql string) string {
		var v string
		if err := c.QueryRow(e.ctx, sql).Scan(&v); err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}

	// (1) a role given SELECT on documents and nothing on lokadb: routed (PUBLIC reads the metadata)
	grants, err := one(e.ctx, e.cloud(), "SELECT has_table_privilege('b912a', 'lokadb.tables', 'SELECT') AND has_table_privilege('b912a', 'lokadb.lookups', 'SELECT') AND has_table_privilege('b912a', 'lokadb.orgs', 'SELECT') AND has_schema_privilege('b912a', 'lokadb', 'USAGE')")
	if err != nil {
		return false, "", err
	}
	a, err := via("b912a")
	if err != nil {
		return false, "", err
	}
	org1 := ask(a, "SELECT count(*)::text FROM documents WHERE organization_id = 1")
	all := ask(a, "SELECT count(*)::text FROM documents")
	a.Close(e.ctx)
	routed := grants == "true" && org1 == "100" && strings.Contains(all, "fanout")
	if !routed {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("role with SELECT on documents alone: metadata readable by PUBLIC %s; organization 1's documents %s (want 100); a count naming no organization: %s (want refused by the fanout policy)", grants, org1, all))

	// (2) the metadata there but unreadable: the role may call lokadb.org() (USAGE) but not read
	// lokadb.tables, as in a database enabled before or with the grant revoked
	if err := execSQL(e.ctx, e.cloud(),
		"REVOKE SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs FROM PUBLIC",
		"GRANT USAGE ON SCHEMA lokadb TO b912b"); err != nil {
		return false, "", err
	}
	b, err := via("b912b")
	if err != nil {
		return false, "", err
	}
	got := ask(b, "SELECT count(DISTINCT organization_id)::text FROM documents")
	b.Close(e.ctx)
	refused := strings.Contains(got, "cannot read the lokadb metadata")
	if !refused {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("role that cannot read lokadb.tables, a count of the organizations of documents: %s (want refused, not 2)", got))

	// (3) schema lokadb is PUBLIC's to use now: its SECURITY DEFINER functions must not be. A move
	// by triggers installs the move's log function (two signatures: text keys, and the bigint
	// wrapper an earlier build's capture functions call); a role of the application calls both.
	if _, err := e.move(1, "-mode", "trigger"); err != nil {
		return false, "", err
	}
	// straight to the cloud: the proxy refuses statements on schema lokadb itself (guard), and
	// this is about the database's own privileges
	a, err = pgx.Connect(e.ctx, dsn(e.cloudHost, e.db, "b912a"))
	if err != nil {
		return false, "", err
	}
	for _, call := range []string{
		`SELECT lokadb.move_log_entry(1, 'documents', 'I', '1', NULL, '{}', NULL)::text`,
		`SELECT lokadb.move_log_entry(1, 'documents', 'I', 1::bigint, NULL::bigint, '{}', NULL)::text`,
	} {
		_, err := a.Exec(e.ctx, call)
		var pe *pgconn.PgError
		denied := errors.As(err, &pe) && pe.Code == "42501"
		if !denied {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("%s by the application's role: %v (42501: %v)", call, firstErrLineOr(err), denied))
	}
	a.Close(e.ctx)
	// and none of lokadb's SECURITY DEFINER functions (a trigger's aside: it cannot be called) runs for PUBLIC
	open, err := one(e.ctx, e.cloud(), `SELECT coalesce(string_agg(p.oid::regprocedure::text, ', ' ORDER BY p.oid::regprocedure::text), 'none') FROM pg_proc p
		WHERE p.pronamespace = 'lokadb'::regnamespace AND p.prosecdef AND p.prorettype <> 'trigger'::regtype
		AND (p.proacl IS NULL OR EXISTS (SELECT FROM aclexplode(p.proacl) x WHERE x.grantee = 0 AND x.privilege_type = 'EXECUTE'))`)
	if err != nil {
		return false, "", err
	}
	if open != "none" {
		ok = false
	}
	notes = append(notes, "SECURITY DEFINER functions of lokadb that PUBLIC may run: "+open)
	return ok, strings.Join(notes, "; "), nil
}
