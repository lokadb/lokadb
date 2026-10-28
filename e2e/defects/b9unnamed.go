package main

// Block 9, from block 10's queue: the client's unnamed statement Parsed in the batch (JDBC
// before prepareThreshold, pgx's QueryExecModeExec) on the direct path. Before, any batch with
// a Parse went the cloud's way and the FDW.

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "UN1", name: "unnamed Parse in the batch (JDBC before prepareThreshold, pgx exec mode) on fdw+direct: reads, an INSERT, a pipeline and a transaction go the direct path, the rows as through the cloud", run: checkDirectUnnamed},
	)
}

func checkDirectUnnamed(e *env) (bool, string, error) {
	stop, doc, err := e.directWriteSetup() // writes of document_versions (documents has defaults and triggers of the cloud)
	if err != nil {
		return false, "", err
	}
	defer stop()
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable application_name=un1", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec // Parse (unnamed), Bind, Describe, Execute, Sync: one round trip, as JDBC
	c, err := pgx.ConnectConfig(e.ctx, cfg)
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	verS, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	// exec mode types the parameters by their Go types: numbers, not texts
	var ver, docID int64
	if _, err := fmt.Sscan(verS+" "+doc, &ver, &docID); err != nil {
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

	// reads: the same title as the cloud gives
	want, err := one(e.ctx, e.cloud(), "SELECT content FROM document_versions WHERE id = "+verS)
	if err != nil {
		return false, "", err
	}
	n := direct()
	var got []string
	for i := 0; i < 3; i++ {
		var title string
		if err := c.QueryRow(e.ctx, "SELECT content FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ver).Scan(&title); err != nil {
			got = append(got, "error: "+firstErrLine(err))
			continue
		}
		got = append(got, title)
	}
	reads := direct() - n
	note(reads == 3 && strings.Join(got, ",") == strings.Repeat(want+",", 2)+want,
		fmt.Sprintf("three reads by unnamed Parse: %q (want %q each), on the direct path %d (want 3)", got, want, reads))

	// an INSERT: in organization 1's database, its key in __refs
	n = direct()
	_, ierr := c.Exec(e.ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, $2)", docID, "un1-insert")
	ins := direct() - n
	inOrg, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content = 'un1-insert'")
	refs, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions__refs r JOIN document_versions d ON d.id = r.id WHERE d.content = 'un1-insert' AND r.tenant = 1")
	note(ierr == nil && ins == 1 && inOrg == "1" && refs == "1",
		fmt.Sprintf("an INSERT by unnamed Parse: error %q, on the direct path %d (want 1), rows in organization 1's database %s, its refs row %s", firstErrLineOr(ierr), ins, inOrg, refs))

	// a pipeline of three unnamed statements
	n = direct()
	b := &pgx.Batch{}
	b.Queue("SELECT content FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ver)
	b.Queue("UPDATE document_versions SET content = content WHERE organization_id = $1 AND id = $2", 1, ver)
	b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1", 1)
	br := c.SendBatch(e.ctx, b)
	var t1 string
	var cnt int64
	e1 := br.QueryRow().Scan(&t1)
	_, e2 := br.Exec()
	e3 := br.QueryRow().Scan(&cnt)
	cerr := br.Close()
	pipe := direct() - n
	wantCnt, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions")
	note(e1 == nil && e2 == nil && e3 == nil && cerr == nil && t1 == want && fmt.Sprint(cnt) == wantCnt && pipe == 3,
		fmt.Sprintf("a pipeline of three unnamed statements: errors %q %q %q %q, title %q, count %d (want %s), on the direct path %d (want 3)",
			firstErrLineOr(e1), firstErrLineOr(e2), firstErrLineOr(e3), firstErrLineOr(cerr), t1, cnt, wantCnt, pipe))

	// transactions (pgx's BEGIN, then unnamed statements): two learnt through the cloud, the third direct
	var txs []string
	for i := 0; i < 3; i++ {
		n = direct()
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return false, "", err
		}
		var title string
		err = tx.QueryRow(e.ctx, "SELECT content FROM document_versions WHERE organization_id = $1 AND id = $2", 1, ver).Scan(&title)
		if err == nil {
			_, err = tx.Exec(e.ctx, "UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", fmt.Sprintf("un1-tx%d", i), 1, ver)
		}
		if err == nil {
			err = tx.Commit(e.ctx)
		} else {
			_ = tx.Rollback(e.ctx)
		}
		if err != nil {
			txs = append(txs, "error: "+firstErrLine(err))
			continue
		}
		txs = append(txs, fmt.Sprint(direct()-n))
	}
	last, _ := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+verS)
	note(strings.Join(txs, ",") == "0,0,2" && last == "un1-tx2",
		fmt.Sprintf("three transactions of unnamed statements: on the direct path %v (want [0 0 2]: learnt through the cloud first), the version's content now %q", txs, last))
	return ok, strings.Join(notes, "; "), nil
}
