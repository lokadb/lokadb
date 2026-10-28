package admin

import (
	"fmt"
	"strings"
)

// carryOver returns the steps that rebuild on the new parent what the application declared
// on table t besides its triggers and dependents (Enable does those): its row security, its
// rules, the materialized views over it. They are read while the table still has its name,
// so every definition names t and, run after the rename, resolves to the parent. The steps
// go after the parent is attached and before Enable turns row security on for it.
//
//   - Row security: the policies move to the parent (those of a table without row security
//     enabled never applied: they stay on DEFAULT, where they still do not). lokadb.tenant_policy then makes
//     lokadb_tenant restrictive (it narrows what they allow instead of widening it). Row
//     security that was not forced let the owner through: lokadb_owner keeps that for the
//     new owner, since Enable forces it.
//   - Rules on t move to the parent: on DEFAULT they would fire for the rows move-org shifts.
//   - A materialized view is made again over the parent, with its indexes, owner, privileges
//     and comments, and what reads it is pointed at the new one (matviewSteps).
func (r *runner) carryOver(t, cloud, owner string) ([]string, error) {
	var steps []string

	rls, err := r.rows(`SELECT relrowsecurity::text, relforcerowsecurity::text FROM pg_class WHERE oid = $1::regclass`, qr(t))
	if err != nil {
		return nil, err
	}
	pols, err := r.rows(`SELECT quote_ident(polname), CASE WHEN polpermissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
			CASE polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE 'ALL' END,
			coalesce((SELECT string_agg(CASE WHEN o = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(o)) END, ', ') FROM unnest(polroles) o), 'PUBLIC'),
			coalesce(' USING (' || pg_get_expr(polqual, polrelid) || ')', ''),
			coalesce(' WITH CHECK (' || pg_get_expr(polwithcheck, polrelid) || ')', '')
		FROM pg_policy WHERE polrelid = $1::regclass ORDER BY polname`, qr(t))
	if err != nil {
		return nil, err
	}
	if rls[0][0] == "true" {
		for _, p := range pols {
			steps = append(steps, fmt.Sprintf("DROP POLICY %s ON %s", p[0], qr(cloud)),
				fmt.Sprintf("CREATE POLICY %s ON %s AS %s FOR %s TO %s%s%s", p[0], qr(t), p[1], p[2], p[3], p[4], p[5]))
		}
		// DEFAULT is read through the parent only; its own row security would only get in
		// the way of move-org, which works on it directly.
		steps = append(steps, fmt.Sprintf("ALTER TABLE %s NO FORCE ROW LEVEL SECURITY", qr(cloud)),
			fmt.Sprintf("ALTER TABLE %s DISABLE ROW LEVEL SECURITY", qr(cloud)))
		if rls[0][1] != "true" {
			steps = append(steps, fmt.Sprintf("CREATE POLICY lokadb_owner ON %s TO %s USING (true) WITH CHECK (true)", qr(t), q(owner)))
		}
	}

	rules, err := r.rows(`SELECT quote_ident(rulename), pg_get_ruledef(oid) FROM pg_rewrite
		WHERE ev_class = $1::regclass AND rulename <> '_RETURN' ORDER BY rulename`, qr(t))
	if err != nil {
		return nil, err
	}
	for _, ru := range rules {
		steps = append(steps, fmt.Sprintf("DROP RULE %s ON %s", ru[0], qr(cloud)), ru[1])
	}

	if err := r.carryCheck(t); err != nil {
		return nil, err
	}
	mvs, err := r.matviews(t)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, mv := range mvs {
		more, err := r.matviewSteps(mv[0], seen)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", t, err)
		}
		steps = append(steps, more...)
	}
	return steps, nil
}

// matviewSteps returns the steps that make the materialized view mv (an OID) again from its
// own definition and point what reads it at the new one. A materialized view has no CREATE OR
// REPLACE, and its definition names relations by OID: over a table that enable turns into
// DEFAULT (or disable back into the plain table), it keeps reading the old relation. Every
// definition is read now, before the renames these steps go after, so its text still names
// the relation by the name that will then mean the new one.
//
// The old one is renamed aside and its indexes and statistics dropped (their names are
// reused), the new one gets its indexes, statistics, owner, privileges and comments. Views,
// rules and SQL-standard functions that read it are replaced with their own definitions (they
// keep their OID, privileges, triggers and readers); a materialized view that reads it is made
// again the same way. The old one then goes without CASCADE: anything else that holds it
// (a policy, a function over its row type) is refused here, before any step runs.
func (r *runner) matviewSteps(mv string, seen map[string]bool) ([]string, error) {
	if seen[mv] {
		return nil, nil
	}
	seen[mv] = true
	def, err := r.rows(`SELECT c.oid::regclass::text, quote_ident(n.nspname), c.relname::text,
			coalesce(' WITH (' || array_to_string(c.reloptions, ', ') || ')', ''), pg_get_viewdef(c.oid),
			CASE WHEN c.relispopulated THEN 'WITH DATA' ELSE 'WITH NO DATA' END, quote_ident(pg_get_userbyid(c.relowner)),
			coalesce('COMMENT ON MATERIALIZED VIEW ' || c.oid::regclass::text || ' IS ' || quote_literal(obj_description(c.oid, 'pg_class')), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1::oid`, mv)
	if err != nil {
		return nil, err
	}
	d := def[0]
	name := d[0]
	held, err := r.rows(`SELECT DISTINCT pg_describe_object(d.classid, d.objid, d.objsubid) FROM pg_depend d
		WHERE d.deptype = 'n' AND (d.refclassid = 'pg_class'::regclass AND d.refobjid = $1::oid
				AND d.classid NOT IN ('pg_rewrite'::regclass, 'pg_proc'::regclass)
			OR d.refclassid = 'pg_type'::regclass AND d.refobjid = (SELECT reltype FROM pg_class WHERE oid = $1::oid))
		ORDER BY 1`, mv)
	if err != nil {
		return nil, err
	}
	if len(held) > 0 {
		var names []string
		for _, h := range held {
			names = append(names, h[0])
		}
		return nil, fmt.Errorf("materialized view %s has to be made again over the new table, and %s depend on it in a way lokadb cannot carry over: drop them before and create them again after",
			name, strings.Join(names, ", "))
	}
	idx, err := r.rows(`SELECT pg_get_indexdef(indexrelid), indexrelid::regclass::text FROM pg_index WHERE indrelid = $1::oid ORDER BY indexrelid`, mv)
	if err != nil {
		return nil, err
	}
	stats, err := r.rows(`SELECT pg_get_statisticsobjdef(oid), quote_ident(stxnamespace::regnamespace::text) || '.' || quote_ident(stxname)
		FROM pg_statistic_ext WHERE stxrelid = $1::oid ORDER BY oid`, mv)
	if err != nil {
		return nil, err
	}
	grants, err := r.rows(`SELECT a.privilege_type, coalesce(quote_ident(g.rolname), 'PUBLIC'), a.is_grantable::text
		FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
		WHERE c.oid = $1::oid AND a.grantee <> c.relowner`, mv)
	if err != nil {
		return nil, err
	}
	colComments, err := r.rows(`SELECT 'COMMENT ON COLUMN ' || $1::oid::regclass::text || '.' || quote_ident(attname) || ' IS ' || quote_literal(col_description($1::oid, attnum))
		FROM pg_attribute WHERE attrelid = $1::oid AND attnum > 0 AND NOT attisdropped AND col_description($1::oid, attnum) IS NOT NULL ORDER BY attnum`, mv)
	if err != nil {
		return nil, err
	}
	// what reads it: m a materialized view, v a view, r a rule, f a function
	readers, err := r.rows(`SELECT DISTINCT kind, oid, def FROM (
			SELECT CASE WHEN w.rulename <> '_RETURN' THEN 'r' WHEN v.relkind = 'm' THEN 'm' ELSE 'v' END AS kind, w.ev_class::text AS oid,
				CASE WHEN w.rulename <> '_RETURN' THEN regexp_replace(pg_get_ruledef(w.oid), '^CREATE RULE', 'CREATE OR REPLACE RULE')
					WHEN v.relkind = 'm' THEN ''
					ELSE 'CREATE OR REPLACE VIEW ' || w.ev_class::regclass::text || coalesce(' WITH (' || array_to_string(v.reloptions, ', ') || ')', '')
						|| ' AS ' || pg_get_viewdef(w.ev_class) END AS def
			FROM pg_depend d JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid JOIN pg_class v ON v.oid = w.ev_class
			WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = $1::oid AND w.ev_class <> $1::oid
			UNION ALL
			SELECT 'f', p.oid::text, pg_get_functiondef(p.oid)
			FROM pg_depend d JOIN pg_proc p ON d.classid = 'pg_proc'::regclass AND p.oid = d.objid
			WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = $1::oid) x
		ORDER BY 1, 2`, mv)
	if err != nil {
		return nil, err
	}

	old := d[2] + "__lokadb_old"
	if len(old) > 63 {
		old = old[:63]
	}
	steps := []string{fmt.Sprintf("ALTER MATERIALIZED VIEW %s RENAME TO %s", name, q(old))}
	for _, i := range idx {
		steps = append(steps, "DROP INDEX "+i[1])
	}
	for _, s := range stats {
		steps = append(steps, "DROP STATISTICS "+s[1])
	}
	steps = append(steps, fmt.Sprintf("CREATE MATERIALIZED VIEW %s%s AS %s %s", name, d[3], strings.TrimSuffix(strings.TrimSpace(d[4]), ";"), d[5]))
	for _, i := range idx {
		steps = append(steps, i[0])
	}
	for _, s := range stats {
		steps = append(steps, s[0])
	}
	steps = append(steps, fmt.Sprintf("ALTER MATERIALIZED VIEW %s OWNER TO %s", name, d[6]))
	for _, g := range grants {
		opt := ""
		if g[2] == "true" {
			opt = " WITH GRANT OPTION"
		}
		steps = append(steps, fmt.Sprintf("GRANT %s ON %s TO %s%s", g[0], name, g[1], opt))
	}
	if d[7] != "" {
		steps = append(steps, d[7])
	}
	for _, c := range colComments {
		steps = append(steps, c[0])
	}
	for _, x := range readers {
		if x[0] != "m" {
			steps = append(steps, x[2]) // its text names the materialized view: now the new one
			continue
		}
		more, err := r.matviewSteps(x[1], seen)
		if err != nil {
			return nil, err
		}
		steps = append(steps, more...)
	}
	return append(steps, fmt.Sprintf("DROP MATERIALIZED VIEW %s.%s", d[1], q(old))), nil
}

// matviews lists the materialized views over table t: oid, name, populated.
func (r *runner) matviews(t string) ([][]string, error) {
	return r.rows(`SELECT DISTINCT v.oid::text, v.oid::regclass::text, v.relispopulated::text FROM pg_depend d
		JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid JOIN pg_class v ON v.oid = w.ev_class
		WHERE d.refclassid = 'pg_class'::regclass AND d.refobjid = $1::regclass AND v.relkind = 'm' ORDER BY 2`, qr(t))
}

// carryCheck is what carryOver would refuse, read only (enable -prepare calls it): a
// materialized view over t, or one that reads it, that something holds in a way matviewSteps
// cannot carry over. A populated one is made again WITH DATA at the switch, under its locks:
// the log says so ahead of time.
func (r *runner) carryCheck(t string) error {
	mvs, err := r.matviews(t)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, mv := range mvs {
		if _, err := r.matviewSteps(mv[0], seen); err != nil {
			return fmt.Errorf("%s: %w", t, err)
		}
		if mv[2] == "true" && r.log != nil {
			r.log.Warn("a populated materialized view is made again over the new table at the switch: its query runs while the switch holds its locks",
				"table", t, "materialized_view", mv[1])
		}
	}
	return nil
}
