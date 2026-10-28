package main

// Block 9, from block 10's queue: SELECT set_config('lokadb.tenant', …, true) starts a direct
// cooperative transaction as SET LOCAL does (DP19). Before, it sent the transaction the cloud's
// way and taught nothing (DP21 and UT10 of block 18 checked that path alone).

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "SC1", name: "the cooperative mode by set_config('lokadb.tenant', …, true) after BEGIN (pgx with a parameter, a simple query): learnt through the cloud, then direct, the same rows, the value set_config returns", run: checkDirectTxSetConfig2},
	)
}

func checkDirectTxSetConfig2(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE document_versions SET content = 'sc1-0' WHERE id = "+ver); err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("sc1")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	// run: one transaction; simple: set_config as a simple query (its constant), else pgx's
	// prepared statement with a parameter
	run := func(mark string, simple bool) (string, int64, int, error) {
		n := directCount(e.proxyRoutes(), "1")
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return "", 0, 0, err
		}
		var got string
		if simple {
			err = tx.QueryRow(e.ctx, "SELECT set_config('lokadb.tenant', '1', true)", pgx.QueryExecModeSimpleProtocol).Scan(&got)
		} else {
			err = tx.QueryRow(e.ctx, "SELECT set_config('lokadb.tenant', $1, true)", "1").Scan(&got)
		}
		var count int64
		if err == nil { // each variant a form of its own: the transaction is learnt by it
			q := map[bool]string{false: "SELECT count(*) FROM document_versions", true: "SELECT count(id) FROM document_versions"}[simple]
			err = tx.QueryRow(e.ctx, q).Scan(&count)
		}
		if err == nil {
			_, err = tx.Exec(e.ctx, "UPDATE document_versions SET content = $1 WHERE content LIKE 'sc1-%'", mark)
		}
		if err != nil {
			_ = tx.Rollback(e.ctx)
			return got, 0, 0, err
		}
		err = tx.Commit(e.ctx)
		return got, count, directCount(e.proxyRoutes(), "1") - n, err
	}
	var notes []string
	ok := true
	for _, simple := range []bool{false, true} {
		name := map[bool]string{false: "pgx set_config($1)", true: "set_config by a simple query"}[simple]
		var directs []string
		var vals []string
		good := true
		for i := 0; i < 3; i++ {
			mark := fmt.Sprintf("sc1-%v-%d", simple, i)
			got, count, d, err := run(mark, simple)
			if err != nil {
				directs = append(directs, "error: "+firstErrLine(err))
				good = false
				continue
			}
			directs = append(directs, fmt.Sprint(d))
			vals = append(vals, got)
			good = good && fmt.Sprint(count) == want && got == "1"
		}
		content, _ := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
		good = good && strings.Join(directs, ",") == "0,0,2" && content == fmt.Sprintf("sc1-%v-2", simple)
		s := fmt.Sprintf("%s: three transactions on the direct path %v (want [0 0 2]: learnt through the cloud first), set_config returned %v (want 1 each), counts %s each, the version's content %q", name, directs, vals, want, content)
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	return ok, strings.Join(notes, "; "), nil
}
