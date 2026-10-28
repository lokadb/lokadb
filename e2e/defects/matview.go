package main

// Materialized views over a LokaDB table (block 7, taken over from block 4): enable and disable
// make them again over the new relation and point what reads them at the new one (admin/carry.go).

import "fmt"

func init() {
	checks = append(checks,
		check{id: "DS6", name: "disable makes a materialized view over the table again over the plain table (its index, owner, privileges kept)", run: checkDisableMatview},
	)
}

// mvReads: the relations the materialized view or view v reads directly, as name:relkind.
func (e *env) mvReads(v string) string {
	got, err := one(e.ctx, e.cloud(), `SELECT coalesce(string_agg(DISTINCT c.relname || ':' || c.relkind::text, ','), '') FROM pg_depend d
		JOIN pg_rewrite w ON d.classid = 'pg_rewrite'::regclass AND w.oid = d.objid JOIN pg_class c ON c.oid = d.refobjid
		WHERE d.refclassid = 'pg_class'::regclass AND w.ev_class = $1::regclass AND c.oid <> w.ev_class`, v)
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	return got
}

// DS6. On main disable replaced the materialized view like a view (CREATE OR REPLACE VIEW)
// and failed: "is not a view".
func checkDisableMatview(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE MATERIALIZED VIEW doc_counts AS SELECT organization_id, count(*) AS n FROM documents GROUP BY 1",
		"CREATE UNIQUE INDEX doc_counts_org ON doc_counts (organization_id)",
		"ALTER MATERIALIZED VIEW doc_counts OWNER TO app",
		"GRANT SELECT ON doc_counts TO reader"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		return false, "disable failed: " + errLine(err), nil
	}
	reads := e.mvReads("doc_counts")
	ins := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'after disable')",
		"REFRESH MATERIALIZED VIEW CONCURRENTLY doc_counts")
	mv, err := e.asRole("reader", "", "SELECT string_agg(organization_id || ':' || n, ' ' ORDER BY organization_id) FROM doc_counts")
	if err != nil {
		mv = "error: " + firstErrLine(err)
	}
	attrs, _ := one(e.ctx, e.cloud(), `SELECT (SELECT count(*) FROM pg_indexes WHERE tablename = 'doc_counts' AND indexname = 'doc_counts_org')
		|| ' ' || pg_get_userbyid(relowner) || ' ' || (SELECT count(*) FROM pg_class WHERE relname LIKE '%lokadb_old%') FROM pg_class WHERE oid = 'doc_counts'::regclass`)
	note := fmt.Sprintf("doc_counts reads %s; insert and REFRESH CONCURRENTLY: %v; reader sees %q; index, owner, leftovers: %s", reads, ins, mv, attrs)
	return reads == "documents:r" && ins == nil && mv == "1:101 2:100" && attrs == "1 app 0", note, nil
}
