package admin

import (
	"regexp"
	"strings"
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// TestBaseSQLParses: the SQL of baseSQL and its plpgsql bodies parse, and so do the trigger
// functions fk_cloud_sync makes (a $$ inside a $$ body once ended it early, and every enable
// failed). Only syntax: names and types are checked by a database.
func TestBaseSQLParses(t *testing.T) {
	if _, err := pg.Parse(baseSQL); err != nil {
		t.Fatalf("baseSQL: %v", err)
	}
	if _, err := pg.ParsePlPgSqlToJSON(baseSQL); err != nil {
		t.Fatalf("baseSQL, plpgsql: %v", err)
	}
	made := regexp.MustCompile(`(?s)\$f\$(CREATE FUNCTION .*?) \$f\$`).FindAllStringSubmatch(fkCloudSQL, -1)
	if len(made) != 2 {
		t.Fatalf("fk_cloud_sync: %d generated functions, want 2", len(made))
	}
	for _, m := range made {
		sql := strings.Replace(m[1], "%I", "fk_check__t_0", 1)
		if _, err := pg.ParsePlPgSqlToJSON(sql); err != nil {
			t.Errorf("generated %s: %v", sql, err)
		}
	}
}

// org_lock is called with an untyped parameter (split.go, an older lokadb's commands): an
// overload of another argument type would make Postgres pick text for it, and pgx refuses to
// encode an integer as text. Block 18's lock by value has a name of its own (org_lock_v).
func TestOrgLockNotOverloaded(t *testing.T) {
	if n := strings.Count(baseSQL, "CREATE OR REPLACE FUNCTION lokadb.org_lock("); n != 1 {
		t.Fatalf("baseSQL creates lokadb.org_lock %d times, want 1", n)
	}
}

// The 2nd review of routing: the lokadb functions an application's session reaches (the
// policy, the defaults, the triggers, what the proxy calls) do not take its search_path. The
// policy's own are inlined: every name in them is written with pg_catalog; the others have
// SET search_path, pg_temp last (unlisted, it is searched first: a temporary pg_class).
func TestFunctionsPath(t *testing.T) {
	all := baseSQL + routingSQL + captureDDL()
	for _, set := range regexp.MustCompile(`search_path = [^A$']*`).FindAllString(all, -1) {
		if !strings.HasSuffix(strings.TrimSpace(set), "pg_temp") {
			t.Errorf("%q: pg_temp is not last", set)
		}
	}
	head := regexp.MustCompile(`(?s)CREATE OR REPLACE FUNCTION (lokadb\.[a-z_]+)\(.*?AS \$\$`)
	set := map[string]bool{}
	for _, m := range head.FindAllStringSubmatch(all, -1) {
		if strings.Contains(m[0], "SET search_path = pg_catalog") {
			set[m[1]] = true
		}
	}
	for _, f := range []string{"lokadb.org_default", "lokadb.org_default_uuid", "lokadb.moving", "lokadb.parent_guard", "lokadb.refs_sync",
		"lokadb.same_org", "lokadb.prep_tenant", "lokadb.local_parts", "lokadb.foreign_parts", "lokadb.bound_orgs", "lokadb.prep_moved_count",
		"lokadb.org_lock", "lokadb.org_lock_v", "lokadb.key_sync", "lokadb.prep_moved", "lokadb.replay_targets"} {
		if !set[f] {
			t.Errorf("%s: no SET search_path = pg_catalog, pg_temp", f)
		}
	}
	for _, f := range []string{"org", "org_lo_uuid", "org_hi_uuid"} {
		m := regexp.MustCompile(`(?s)CREATE OR REPLACE FUNCTION lokadb\.` + f + `\(\).*?\$\$(.*?)\$\$`).FindStringSubmatch(baseSQL)
		if m == nil {
			t.Fatalf("lokadb.%s not found", f)
		}
		body := regexp.MustCompile(`pg_catalog\.[a-z_]+|OPERATOR\(pg_catalog\.=\)|'[^']*'`).ReplaceAllString(m[1], "")
		for _, bad := range regexp.MustCompile(`[a-z_]+\(|::[a-z]|nullif|=`).FindAllString(body, -1) {
			if bad != "coalesce(" {
				t.Errorf("lokadb.%s: %q is not written with pg_catalog: %s", f, bad, m[1])
			}
		}
	}
}

// R3-14: a DO block's body with $$ in it (a name from the spec or the catalog) gets another
// dollar quote.
func TestDoTag(t *testing.T) {
	if got := doTag(`DO $$ BEGIN PERFORM 1; END $$`); got != `DO $lokadb$ BEGIN PERFORM 1; END $lokadb$` {
		t.Errorf("plain: %s", got)
	}
	evil := `DO $$ BEGIN EXECUTE 'DROP TABLE ' || 'x$$; DROP TABLE y; --$lokadb$'; END $$`
	got := doTag(evil)
	if !strings.HasPrefix(got, "DO $lokadb0$") || !strings.HasSuffix(got, "$lokadb0$") {
		t.Errorf("a body holding $$ and $lokadb$: %s", got)
	}
	if _, err := pg.Parse(got); err != nil {
		t.Errorf("%s: %v", got, err)
	}
	if tree, _ := pg.Parse(got); tree != nil && len(tree.Stmts) != 1 {
		t.Errorf("%s: %d statements, want 1", got, len(tree.Stmts))
	}
}
