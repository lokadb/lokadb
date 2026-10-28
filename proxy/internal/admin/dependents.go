package admin

import (
	"fmt"
	"strings"
)

// Dependent: an object that holds a LokaDB table t by its OID, or by its row type. enable
// renames t to its DEFAULT partition and makes a new parent of t's name: what holds the old OID
// would go on with the partition. Dependents finds these objects for enable (and for disable,
// which goes the other way), and check reports them (review 3 of check, H1 and H2: the policies
// of other tables and the functions over the row type went unseen by both).
type Dependent struct {
	// Kind: view, matview, rule, function (SQL-standard body), policy (of another table),
	// rowtype_function (takes t's row type, or an array of it), rowtype_column (a column of
	// another table or of a composite type of it), rowtype_type (a domain over it).
	Kind string
	// Name: how Postgres prints the object (regclass, regprocedure; "<policy> on <table>").
	Name  string
	Table string
	// Movable: enable makes it again over the parent; DDL does it, read while t has its name
	// (run after the rename, every name in it resolves to the parent). Then runs after the
	// DDL of every dependent of t. A materialized view has no DDL here: carryOver makes it.
	Movable   bool
	DDL, Then string
	// Reason: why enable cannot carry it over (Movable false); enable refuses with it.
	Reason string
}

// dependentsSQL: the dependents of $1 (a relation): kind, name, DDL, then, movable, reason, and
// the object's oid (a function over the row type with an SQL-standard body is in both lists).
const dependentsSQL = `WITH t AS (SELECT c.oid, c.reltype AS rt, ty.typarray AS rta FROM pg_class c JOIN pg_type ty ON ty.oid = c.reltype
                                  WHERE c.oid = $1::regclass)
SELECT DISTINCT
    CASE WHEN w.oid IS NULL THEN 'function' WHEN w.rulename <> '_RETURN' THEN 'rule' WHEN v.relkind = 'm' THEN 'matview' ELSE 'view' END,
    CASE WHEN w.oid IS NULL THEN p.oid::regprocedure::text
         WHEN w.rulename <> '_RETURN' THEN quote_ident(w.rulename) || ' on ' || w.ev_class::regclass::text
         ELSE w.ev_class::regclass::text END,
    CASE WHEN w.oid IS NULL THEN pg_get_functiondef(p.oid)
         WHEN w.rulename <> '_RETURN' THEN regexp_replace(pg_get_ruledef(w.oid), '^CREATE RULE', 'CREATE OR REPLACE RULE')
         WHEN v.relkind = 'm' THEN ''
         ELSE 'CREATE OR REPLACE VIEW ' || w.ev_class::regclass::text
           || coalesce(' WITH (' || array_to_string(v.reloptions, ', ') || ')', '') || ' AS ' || pg_get_viewdef(w.ev_class) END,
    '', true, '', coalesce(p.oid, w.oid)::text
  FROM t JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = t.oid AND d.deptype = 'n'
  LEFT JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid
  LEFT JOIN pg_class v ON v.oid = w.ev_class
  LEFT JOIN pg_proc p ON d.classid = 'pg_proc'::regclass AND p.oid = d.objid
  WHERE w.oid IS NOT NULL AND w.ev_class <> t.oid OR p.oid IS NOT NULL
UNION
-- a policy of another table that reads t (one of t's own is carried by carryOver)
SELECT 'policy', quote_ident(pol.polname) || ' on ' || pol.polrelid::regclass::text,
    'ALTER POLICY ' || quote_ident(pol.polname) || ' ON ' || pol.polrelid::regclass::text
      || coalesce(' USING (' || pg_get_expr(pol.polqual, pol.polrelid) || ')', '')
      || coalesce(' WITH CHECK (' || pg_get_expr(pol.polwithcheck, pol.polrelid) || ')', ''),
    '', true, '', pol.oid::text
  FROM t JOIN pg_depend d ON d.refclassid = 'pg_class'::regclass AND d.refobjid = t.oid AND d.classid = 'pg_policy'::regclass
  JOIN pg_policy pol ON pol.oid = d.objid
  WHERE pol.polrelid <> t.oid
UNION
-- a function that takes the row type (or an array of it): made again over the parent's, the old
-- one dropped. One that only returns it goes on working (rows of the partition are of the parent's).
SELECT 'rowtype_function', p.oid::regprocedure::text,
    CASE WHEN p.prokind IN ('f', 'p') THEN pg_get_functiondef(p.oid) ELSE '' END,
    format('DO $lokadb$ BEGIN EXECUTE %L || %s::regprocedure::text; END $lokadb$',
      CASE p.prokind WHEN 'p' THEN 'DROP PROCEDURE ' ELSE 'DROP FUNCTION ' END, p.oid),
    p.prokind IN ('f', 'p') AND x.what IS NULL,
    CASE WHEN p.prokind NOT IN ('f', 'p') THEN 'it is an aggregate or a window function'
         ELSE coalesce(x.what, '') || ' depends on it' END,
    p.oid::text
  FROM t JOIN pg_depend d ON d.refclassid = 'pg_type'::regclass AND d.refobjid IN (t.rt, t.rta) AND d.classid = 'pg_proc'::regclass AND d.deptype = 'n'
  JOIN pg_proc p ON p.oid = d.objid
  LEFT JOIN LATERAL (SELECT string_agg(pg_describe_object(e.classid, e.objid, e.objsubid), ', ' ORDER BY 1) AS what
      FROM pg_depend e WHERE e.refclassid = 'pg_proc'::regclass AND e.refobjid = p.oid AND e.deptype = 'n'
        AND e.classid <> 'pg_rewrite'::regclass) x ON true
  WHERE t.rt = ANY (p.proargtypes::oid[]) OR t.rta = ANY (p.proargtypes::oid[])
UNION
-- a column of the row type (or an array of it) in another table or a composite type, a domain
-- over it: lokadb does not change their types
SELECT CASE WHEN d.classid = 'pg_type'::regclass THEN 'rowtype_type' ELSE 'rowtype_column' END,
    pg_describe_object(d.classid, d.objid, d.objsubid), '', '', false, '', d.objid::text
  FROM t JOIN pg_depend d ON d.refclassid = 'pg_type'::regclass AND d.refobjid IN (t.rt, t.rta) AND d.deptype = 'n'
  LEFT JOIN pg_class r ON d.classid = 'pg_class'::regclass AND r.oid = d.objid
  WHERE d.classid = 'pg_type'::regclass OR d.classid = 'pg_class'::regclass AND d.objsubid > 0 AND r.relkind NOT IN ('v', 'm')
ORDER BY 1, 2`

// Dependents: the dependents of LokaDB table t (its key, rel.RelKey), in the order enable makes
// them again: the functions over the row type first (a view made again may call them).
func Dependents(rows func(sql string, args ...any) ([][]string, error), t string) ([]Dependent, error) {
	rs, err := rows(dependentsSQL, qr(t))
	if err != nil {
		return nil, err
	}
	byRowtype := map[string]bool{}
	for _, r := range rs {
		if r[0] == "rowtype_function" {
			byRowtype[r[6]] = true
		}
	}
	var first, rest []Dependent
	for _, r := range rs {
		d := Dependent{Kind: r[0], Name: r[1], Table: t, DDL: r[2], Then: r[3], Movable: r[4] == "true"}
		switch d.Kind {
		case "function":
			if byRowtype[r[6]] {
				continue // made again as a function over the row type
			}
		case "rowtype_function":
			if !d.Movable {
				d.DDL, d.Then = "", ""
				d.Reason = fmt.Sprintf("%s takes the row type of %s, and %s: enable would have to make it again over the new parent's row type and drop it, and cannot. Drop it before enable and make it again after", d.Name, t, r[5])
			}
			first = append(first, d)
			continue
		case "rowtype_column", "rowtype_type":
			d.Reason = fmt.Sprintf("%s is of the row type of %s (or an array of it): after enable that type is the DEFAULT partition's (%s), not the table's, and lokadb does not change the type of a column. Store the row another way (jsonb, its key) before enable", d.Name, t, hn(t, "__cloud"))
		}
		rest = append(rest, d)
	}
	return append(first, rest...), nil
}

// unmovable: the dependents of t enable cannot carry over, as one error (nil if none).
func unmovable(t string, deps []Dependent) error {
	var why []string
	for _, d := range deps {
		if !d.Movable {
			why = append(why, d.Reason)
		}
	}
	if len(why) == 0 {
		return nil
	}
	return fmt.Errorf("%s: %s", t, strings.Join(why, "; "))
}

// depSteps: what makes the dependents again over the table of their name (enable: the parent;
// disable: the plain table), in order, then the steps that follow them.
func depSteps(deps []Dependent) []string {
	var steps, then []string
	for _, d := range deps {
		if d.DDL != "" {
			steps = append(steps, d.DDL)
		}
		if d.Then != "" {
			then = append(then, d.Then)
		}
	}
	return append(steps, then...)
}
