package main

// Transaction forms learnt ahead (block 10, stage 8a.2, the second step): lokadb check -profile
// fdw+direct -direct-forms turns a recorded workload into SQL for psql, lokadb.direct_learnt
// (direct from the first run) and lokadb.direct_cloud_first (the cloud's way, never refused).

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "DP20", name: "check -profile fdw+direct -direct-forms: a recorded form that fits goes direct from its first run, one that would need the cloud after going direct goes the cloud's way at once (no 0A000)", run: checkDirectForms},
	)
}

// the statements as pgx sends them: DP8's form; a form whose branches share the first statement,
// one direct whole, the other with a JOIN on a cloud table after it (refused once without the
// forms: learnt by the first branch, then the second meets the JOIN)
const (
	dp20Update = "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3"
	dp20Users  = "SELECT count(*) FROM users"
	dp20Insert = "INSERT INTO document_versions (document_id, content) VALUES ($1, $2) RETURNING id"
	dp20Branch = "UPDATE document_versions SET content = content WHERE organization_id = $1 AND id = $2"
	dp20Join   = "SELECT count(*) FROM document_versions v JOIN users u ON u.id = v.document_id WHERE v.organization_id = $1"
)

func checkDirectForms(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	dir, err := os.MkdirTemp("", e.db+"-dp20")
	if err != nil {
		return false, "", err
	}
	defer os.RemoveAll(dir)
	rec, forms := filepath.Join(dir, "workload.json"), filepath.Join(dir, "forms.sql")
	w := map[string]any{"version": 1, "updated": "2026-10-25T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{{"sql": dp20Update, "count": 3}, {"sql": dp20Users, "count": 3}, {"sql": dp20Insert, "count": 3}, {"sql": dp20Branch, "count": 3}, {"sql": dp20Join, "count": 3}},
		"transactions": []map[string]any{{"statements": []string{"begin", dp20Update, dp20Users, dp20Insert, "commit"}, "count": 3},
			{"statements": []string{"begin", dp20Branch, "commit"}, "count": 5},
			{"statements": []string{"begin", dp20Branch, dp20Join, "commit"}, "count": 1}}}}}
	b, _ := json.Marshal(w)
	if err := os.WriteFile(rec, b, 0o644); err != nil {
		return false, "", err
	}
	out, _ := e.runCheck("-db", e.cloud(), "-workload", rec, "-profile", "fdw+direct", "-direct-forms", forms)
	sql, err := os.ReadFile(forms)
	if err != nil {
		return false, "check wrote no forms: " + lastErr(out), nil
	}
	learnt, cloud := strings.Count(string(sql), "INSERT INTO lokadb.direct_learnt"), strings.Count(string(sql), "INSERT INTO lokadb.direct_cloud_first")
	if err := execSQL(e.ctx, e.cloud(), string(sql)); err != nil {
		return false, "psql -f of the forms: " + firstErrLine(err), nil
	}
	c, err := e.viaProxyPgx("dp20") // a new session: the metadata with the forms
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	// the learnt form: direct at once
	n := directCount(e.proxyRoutes(), "1")
	tx, err := c.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	var id int64
	for _, err := range []error{
		exec1(e, tx, dp20Update, "dp20", 1, ver),
		tx.QueryRow(e.ctx, dp20Users).Scan(new(int64)),
		tx.QueryRow(e.ctx, dp20Insert, doc, "dp20-i").Scan(&id),
	} {
		if err != nil {
			_ = tx.Rollback(e.ctx)
			return false, "the learnt form: " + firstErrLine(err), nil
		}
	}
	if err := tx.Commit(e.ctx); err != nil {
		return false, "the learnt form's COMMIT: " + firstErrLine(err), nil
	}
	direct := directCount(e.proxyRoutes(), "1") - n
	// the branching form: its direct branch three times (learnt by the proxy otherwise), then the
	// branch with the JOIN: the cloud's way all along, no refusal
	n = directCount(e.proxyRoutes(), "1")
	var refused string
	for i := 0; i < 4 && refused == ""; i++ {
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return false, "", err
		}
		steps := []error{exec1(e, tx, dp20Branch, 1, ver)}
		if i == 3 {
			steps = append(steps, tx.QueryRow(e.ctx, dp20Join, 1).Scan(new(int64)))
		}
		for _, err := range steps {
			if err != nil && refused == "" {
				refused = fmt.Sprintf("run %d: %s", i+1, firstErrLine(err))
			}
		}
		if refused == "" {
			if err := tx.Commit(e.ctx); err != nil {
				refused = fmt.Sprintf("run %d COMMIT: %s", i+1, firstErrLine(err))
			}
		} else {
			_ = tx.Rollback(e.ctx)
		}
	}
	branchDirect := directCount(e.proxyRoutes(), "1") - n
	ok := learnt == 1 && cloud == 1 && direct == 2 && refused == "" && branchDirect == 0
	return ok, fmt.Sprintf("forms: %d learnt, %d the cloud's way (want 1, 1); the learnt form's first run: on the direct path %d (want 2); the branching form, 3 direct-only runs and one with the JOIN: error %q (want none), on the direct path %d (want 0)",
		learnt, cloud, direct, refused, branchDirect), nil
}
