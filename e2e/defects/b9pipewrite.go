package main

// Block 9, from block 10's queue: writes of 8b inside a pipeline on fdw+direct (pgx.Batch,
// JDBC executeBatch). Before, a pipeline with a write went the cloud's way and the FDW whole.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "PW1", name: "writes of 8b in a pipeline on fdw+direct (pgx.Batch, JDBC executeBatch): INSERTs, a DELETE and a read go the direct path as one transaction, their refs rows in the cloud; a failed write rolls the whole pipeline back", run: checkPipeWrite},
	)
}

func checkPipeWrite(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	var docID int64
	if _, err := fmt.Sscan(doc, &docID); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	direct := func() int { return directCount(e.proxyRoutes(), "1") }
	count := func(d, mark string) string {
		v, _ := one(e.ctx, d, "SELECT count(*) FROM document_versions WHERE content LIKE '"+mark+"%'")
		return v
	}
	refsOf := func(mark string) string { // the refs rows of organization 1's versions so marked
		ids, err := e.customerRows("SELECT id::text FROM document_versions WHERE content LIKE '" + mark + "%'")
		if err != nil || len(ids) == 0 {
			return "0"
		}
		v, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs WHERE tenant = 1 AND id IN ("+strings.Join(ids, ",")+")")
		return v
	}
	for _, mode := range []pgx.QueryExecMode{pgx.QueryExecModeCacheStatement, pgx.QueryExecModeExec} {
		name := map[pgx.QueryExecMode]string{pgx.QueryExecModeCacheStatement: "prepared statements", pgx.QueryExecModeExec: "unnamed Parse (JDBC before prepareThreshold)"}[mode]
		mark := map[pgx.QueryExecMode]string{pgx.QueryExecModeCacheStatement: "pw1p", pgx.QueryExecModeExec: "pw1u"}[mode]
		cfg, err := pgx.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable application_name=pw1", e.port, e.db))
		if err != nil {
			return false, "", err
		}
		cfg.DefaultQueryExecMode = mode
		c, err := pgx.ConnectConfig(e.ctx, cfg)
		if err != nil {
			return false, "", err
		}
		batch := func(fail bool, tag string) (int, error) {
			n := direct()
			b := &pgx.Batch{}
			b.Queue("INSERT INTO document_versions (document_id, content) VALUES ($1, $2)", docID, tag+"-a")
			b.Queue("INSERT INTO document_versions (document_id, content) VALUES ($1, $2)", docID, tag+"-b")
			b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1", 1)
			b.Queue("DELETE FROM document_versions WHERE organization_id = $1 AND content = $2", 1, tag+"-b")
			if fail { // a key of organization 1's that is taken: 23505 in its database
				var ver int64
				if err := c.QueryRow(e.ctx, "SELECT min(id) FROM document_versions WHERE organization_id = $1", 1).Scan(&ver); err != nil {
					return 0, err
				}
				n = direct()
				b.Queue("INSERT INTO document_versions (id, document_id, content) VALUES ($1, $2, $3)", ver, docID, tag+"-c")
			}
			br := c.SendBatch(e.ctx, b)
			var err error
			for i := 0; i < b.Len() && err == nil; i++ {
				_, err = br.Exec()
			}
			if cerr := br.Close(); err == nil {
				err = cerr
			}
			return direct() - n, err
		}
		// three batches: pgx prepares the statements of the first in a pipeline of its own
		var runs []string
		for i := 0; i < 3; i++ {
			d, err := batch(false, fmt.Sprintf("%s%d", mark, i))
			if err != nil {
				runs = append(runs, "error: "+firstErrLine(err))
				continue
			}
			runs = append(runs, fmt.Sprint(d))
		}
		kept := count(e.customer(), mark)
		gone, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content LIKE '"+mark+"%-b'")
		refs := refsOf(mark)
		note(strings.Join(runs, ",") == "4,4,4" && kept == "3" && gone == "0" && refs == "3",
			fmt.Sprintf("%s: three pipelines of two INSERTs, a read and a DELETE: on the direct path %v (want 4 each), versions kept in organization 1's database %s (want 3), deleted ones left %s, their refs rows %s (want 3)",
				name, runs, kept, gone, refs))
		// a failed write: the whole pipeline rolls back on both sides
		failMark := mark + "f"
		_, ferr := batch(true, failMark)
		left, leftRefs := count(e.customer(), failMark), refsOf(failMark)
		inCloud := count(e.cloud(), failMark)
		note(strings.Contains(firstErrLineOr(ferr), "23505") && left == "0" && leftRefs == "0" && inCloud == "0",
			fmt.Sprintf("%s: a pipeline whose last INSERT takes a key in use: error %q (want 23505), its versions in organization 1's database %s, through the cloud %s, refs rows %s (want 0: rolled back whole)",
				name, firstErrLineOr(ferr), left, inCloud, leftRefs))
		c.Close(e.ctx)
	}
	return ok, strings.Join(notes, "; "), nil
}
