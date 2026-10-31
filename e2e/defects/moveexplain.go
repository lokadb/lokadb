package main

// Not a check of the suite: runs only when DEFECTS_ONLY names MV45. It prints what the DELETE of the
// split does in a database enabled as the move's role (EXPLAIN ANALYZE, rolled back): the time of
// every trigger and the plan, to see where the pause goes.

import (
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV45", name: "EXPLAIN ANALYZE of the split's DELETE as the move's role (runs only when named: DEFECTS_ONLY=MV45)", run: checkExplainSplitDelete},
	)
}

func checkExplainSplitDelete(e *env) (bool, string, error) {
	if !strings.Contains(","+os.Getenv("DEFECTS_ONLY")+",", ",MV45,") {
		return true, "skipped: run it with DEFECTS_ONLY=MV45", nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT CASE WHEN g % 5 = 0 THEN 1 ELSE 2 + g % 49 END, 1, repeat('x', 200) || g FROM generate_series(1, 1500000) g",
		"INSERT INTO document_versions (document_id, content) SELECT id, repeat('v', 100) FROM documents WHERE organization_id = 1 LIMIT 300000",
		"ANALYZE"); err != nil {
		return false, "", err
	}
	if err := asAdmin(e); err != nil {
		return false, "enable as lokadb_saas failed: " + lastErr(err.Error()), nil
	}
	conn, err := pgx.Connect(e.ctx, e.admin)
	if err != nil {
		return false, "", err
	}
	defer conn.Close(e.ctx)
	var out []string
	for _, tbl := range []string{"document_versions__cloud", "documents__cloud"} {
		tx, err := conn.Begin(e.ctx)
		if err != nil {
			return false, "", err
		}
		if _, err := tx.Exec(e.ctx, "SELECT set_config('lokadb.moving', 'on', true)"); err != nil {
			_ = tx.Rollback(e.ctx)
			return false, "", err
		}
		rows, err := tx.Query(e.ctx, "EXPLAIN (ANALYZE, BUFFERS) DELETE FROM "+tbl+" WHERE organization_id = 1")
		if err != nil {
			_ = tx.Rollback(e.ctx)
			return false, "", err
		}
		out = append(out, "== "+tbl)
		for rows.Next() {
			var l string
			_ = rows.Scan(&l)
			out = append(out, strings.TrimSpace(l))
		}
		rows.Close()
		_ = tx.Rollback(e.ctx)
	}
	return true, strings.Join(out, " | "), nil
}
