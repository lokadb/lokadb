package main

// A materialized view that other views read (block 7, taken over from block 4): enable makes it
// again over the parent and points its readers at the new one; move-org and disable keep them.

import (
	"encoding/json"
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X20b", name: "a materialized view that views, another materialized view and a function read: enable, move-org, REFRESH and disable keep them all working", run: checkMatviewReaders},
	)
}

// X20b. doc_counts over documents; the view doc_report over it and doc_total over that; the
// materialized view doc_top over doc_counts; the SQL-standard function doc_sum() over it. On
// main enable refused: it had to drop doc_counts and with it everything that reads it.
func checkMatviewReaders(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE MATERIALIZED VIEW doc_counts AS SELECT organization_id, count(*) AS n FROM documents GROUP BY 1",
		"CREATE UNIQUE INDEX doc_counts_org ON doc_counts (organization_id)",
		"COMMENT ON MATERIALIZED VIEW doc_counts IS 'per organization'",
		"ALTER MATERIALIZED VIEW doc_counts OWNER TO app",
		"CREATE VIEW doc_report AS SELECT organization_id, n * 2 AS n2 FROM doc_counts",
		"CREATE VIEW doc_total AS SELECT sum(n2) AS s FROM doc_report",
		"CREATE MATERIALIZED VIEW doc_top AS SELECT organization_id, n FROM doc_counts WHERE n > 0",
		"CREATE FUNCTION doc_sum() RETURNS numeric LANGUAGE sql BEGIN ATOMIC SELECT sum(n) FROM doc_counts; END",
		"GRANT SELECT ON doc_counts, doc_report, doc_total, doc_top TO reader"); err != nil {
		return false, "", err
	}
	// check before enable: the materialized views are made again (a change, not unsupported),
	// and they keep the moved organizations' rows in the cloud (a warning)
	out, _ := e.checkWith(spec, "-json")
	var rep struct {
		Findings []struct{ Severity, Code, Object string } `json:"findings"`
	}
	sev := map[string]string{}
	if i := strings.Index(out, "{"); i >= 0 && json.Unmarshal([]byte(out[i:]), &rep) == nil {
		for _, f := range rep.Findings {
			if strings.TrimPrefix(f.Object, "public.") == "documents" && (f.Code == "materialized_view" || f.Code == "matview_stores_moved_rows") {
				sev[f.Code] = f.Severity
			}
		}
	}
	checked := fmt.Sprintf("check: materialized_view %q, matview_stores_moved_rows %q", sev["materialized_view"], sev["matview_stores_moved_rows"])
	if err := e.enable(); err != nil {
		return false, checked + "; enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	read := func(sql string) string {
		v, err := e.asRole("reader", "", sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	state := func() string {
		return fmt.Sprintf("doc_counts reads %s, doc_top reads %s; doc_report %q, doc_total %q, doc_top %q, doc_sum() %q",
			e.mvReads("doc_counts"), e.mvReads("doc_top"),
			read("SELECT string_agg(organization_id || ':' || n2, ' ' ORDER BY organization_id) FROM doc_report"),
			read("SELECT s::text FROM doc_total"),
			read("SELECT string_agg(organization_id || ':' || n, ' ' ORDER BY organization_id) FROM doc_top"),
			read("SELECT doc_sum()::text"))
	}
	// a row of the moved organization, written through the parent into its own database
	refresh := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'moved')",
		"REFRESH MATERIALIZED VIEW CONCURRENTLY doc_counts", "REFRESH MATERIALIZED VIEW doc_top")
	moved := state()
	want := `doc_report "1:202 2:200", doc_total "402", doc_top "1:101 2:100", doc_sum() "201"`
	attrs, _ := one(e.ctx, e.cloud(), `SELECT (SELECT count(*) FROM pg_indexes WHERE tablename = 'doc_counts' AND indexname = 'doc_counts_org')
		|| ' ' || pg_get_userbyid(relowner) || ' ' || coalesce(obj_description(oid, 'pg_class'), '') FROM pg_class WHERE oid = 'doc_counts'::regclass`)
	notes := []string{checked, fmt.Sprintf("moved: REFRESH %v; %s; doc_counts index, owner, comment: %s", refresh, moved, attrs)}
	ok := sev["materialized_view"] == "change" && sev["matview_stores_moved_rows"] == "warning" && refresh == nil && strings.HasPrefix(moved, "doc_counts reads documents:p, doc_top reads doc_counts:m; ") &&
		strings.HasSuffix(moved, want) && attrs == "1 app per organization"

	if _, err := e.run6(e.moveBackArgs(1)...); err != nil {
		return false, strings.Join(notes, "; ") + "; move-back failed: " + errLine(err), nil
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		return false, strings.Join(notes, "; ") + "; disable failed: " + errLine(err), nil
	}
	refresh = execSQL(e.ctx, e.cloud(), "REFRESH MATERIALIZED VIEW CONCURRENTLY doc_counts", "REFRESH MATERIALIZED VIEW doc_top")
	after := state()
	left, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_class WHERE relname LIKE '%lokadb_old%'")
	notes = append(notes, fmt.Sprintf("after disable: REFRESH %v; %s; leftovers %s", refresh, after, left))
	ok = ok && refresh == nil && strings.HasPrefix(after, "doc_counts reads documents:r, doc_top reads doc_counts:m; ") &&
		strings.HasSuffix(after, want) && left == "0"
	return ok, strings.Join(notes, "; "), nil
}
