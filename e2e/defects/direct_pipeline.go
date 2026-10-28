package main

// A pipeline on the direct path (block 10, BACKLOG item 6): pgx.Batch outside a transaction,
// every statement of organization 1, goes to its database in one exchange after one gate in
// the cloud; a batch with a statement of another organization goes the cloud's way whole.

import (
	"fmt"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "DP23", name: "a pipeline inside a transaction (pgx tx.SendBatch): two transactions learnt through the cloud, the third direct whole, the same rows", run: checkDirectTxPipeline},
		check{id: "DP22", name: "a pipeline (pgx.Batch) of organization 1's reads and an UPDATE goes direct whole, the same rows as through the cloud; a batch with another organization's statement goes through the cloud", run: checkDirectPipeline},
	)
}

func checkDirectPipeline(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	want2, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp22")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(org2 int, mark string) (string, string, int, error) {
		n := directCount(e.proxyRoutes(), "1")
		b := &pgx.Batch{}
		b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1", 1)
		b.Queue("UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark, 1, ver)
		b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1", org2)
		br := c.SendBatch(e.ctx, b)
		var a, z int64
		err := br.QueryRow().Scan(&a)
		if err == nil {
			_, err = br.Exec()
		}
		if err == nil {
			err = br.QueryRow().Scan(&z)
		}
		if cerr := br.Close(); err == nil {
			err = cerr
		}
		return fmt.Sprint(a), fmt.Sprint(z), directCount(e.proxyRoutes(), "1") - n, err
	}
	// twice: pgx prepares the statements in a pipeline of their own first, then binds them
	var notes []string
	ok := true
	for i, mark := range []string{"dp22-a", "dp22-b"} {
		a, z, direct, err := run(1, mark)
		if err != nil {
			return false, "batch of organization 1: " + firstErrLine(err), nil
		}
		content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
		if err != nil {
			return false, "", err
		}
		ok = ok && a == want && z == want && direct == 3 && content == mark
		notes = append(notes, fmt.Sprintf("batch %d of organization 1: counts %s, %s (want %s), on the direct path %d (want 3), the UPDATE %q", i+1, a, z, want, direct, content))
	}
	a, z, direct, err := run(2, "dp22-c")
	if err != nil {
		return false, "batch with organization 2: " + firstErrLine(err), nil
	}
	ok = ok && a == want && z == want2 && direct == 0
	notes = append(notes, fmt.Sprintf("batch with organization 2: counts %s, %s (want %s, %s), on the direct path %d (want 0)", a, z, want, want2, direct))
	return ok, fmt.Sprint(notes), nil
}

// DP23. pgx Begin, SendBatch of two counts and an UPDATE of organization 1, Commit; twice
// through the cloud (the form is learnt), the third time all three statements direct.
func checkDirectTxPipeline(e *env) (bool, string, error) {
	stop, _, err := e.directWriteSetup()
	if err != nil {
		return false, "", err
	}
	defer stop()
	ver, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("dp23")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	for i, mark := range []string{"dp23-a", "dp23-b", "dp23-c"} {
		n := directCount(e.proxyRoutes(), "1")
		tx, err := c.Begin(e.ctx)
		if err != nil {
			return false, "", err
		}
		b := &pgx.Batch{}
		b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1", 1)
		b.Queue("UPDATE document_versions SET content = $1 WHERE organization_id = $2 AND id = $3", mark, 1, ver)
		b.Queue("SELECT count(*) FROM document_versions WHERE organization_id = $1 AND id >= $2", 1, ver)
		br := tx.SendBatch(e.ctx, b)
		var a, z int64
		err = br.QueryRow().Scan(&a)
		if err == nil {
			_, err = br.Exec()
		}
		if err == nil {
			err = br.QueryRow().Scan(&z)
		}
		if cerr := br.Close(); err == nil {
			err = cerr
		}
		if err == nil {
			err = tx.Commit(e.ctx)
		} else {
			_ = tx.Rollback(e.ctx)
		}
		if err != nil {
			return false, fmt.Sprintf("transaction %d: %s", i+1, firstErrLine(err)), nil
		}
		direct := directCount(e.proxyRoutes(), "1") - n
		content, err := one(e.ctx, e.customer(), "SELECT content FROM document_versions WHERE id = "+ver)
		if err != nil {
			return false, "", err
		}
		wantDirect := 0
		if i == 2 {
			wantDirect = 3
		}
		ok = ok && fmt.Sprint(a) == want && z > 0 && direct == wantDirect && content == mark
		notes = append(notes, fmt.Sprintf("transaction %d: count %d (want %s), on the direct path %d (want %d), the UPDATE %q", i+1, a, want, direct, wantDirect, content))
	}
	return ok, fmt.Sprint(notes), nil
}
