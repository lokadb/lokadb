package admin

import (
	"regexp"
	"strings"
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// The trigger function fk_cascade_sync makes parses, once its format placeholders are filled.
func TestFKCascadeFunctionParses(t *testing.T) {
	made := regexp.MustCompile(`(?s)\$f\$(CREATE FUNCTION .*?) \$f\$`).FindAllStringSubmatch(fkCascadeSQL, -1)
	if len(made) != 1 {
		t.Fatalf("fk_cascade_sync: %d generated functions, want 1", len(made))
	}
	sql := strings.ReplaceAll(strings.Replace(made[0][1], "%I", "fkcascade__t_0", 1), "%%", "%")
	if _, err := pg.ParsePlPgSqlToJSON(sql); err != nil {
		t.Errorf("generated %s: %v", sql, err)
	}
	if !strings.Contains(sql, "SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS") || !strings.Contains(fkCascadeSQL, "REVOKE ALL ON FUNCTION lokadb.%I() FROM PUBLIC") {
		t.Error("the cascade's function must be SECURITY DEFINER with a fixed search_path and revoked from PUBLIC")
	}
}
