package main

// Third review of performance, P3-12: a statement of the simple protocol in autocommit went with
// three statements of the proxy's own, each a transaction of the cloud's (set_config, the pause
// lock, the statement, the unlock); set_config and the lock are one statement now.

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "PF14", name: "a read by a cold key takes its organization within its own exchange: the cloud's transactions per read of a document's versions by a new document id", run: checkInlineLookup},
		check{id: "PF13", name: "the organizations of ids shared by the sessions: 30 connections reading the same child row look its organization up once", run: checkOrgCacheShared},
		check{id: "PF12", name: "the simple protocol in autocommit: the cloud's transactions per UPDATE of alternating organizations (set_config and the pause lock in one statement)", run: checkSimpleServiceStatements},
	)
}

func checkSimpleServiceStatements(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	xacts := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "SELECT xact_commit::text FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	var ids [3]string
	for org := 1; org <= 2; org++ {
		id, err := one(e.ctx, e.cloud(), fmt.Sprintf("SELECT min(id) FROM documents WHERE organization_id = %d", org))
		if err != nil {
			return false, "", err
		}
		ids[org] = id
	}
	stop, err := e.metaCacheProxy(map[string]any{})
	if err != nil {
		return false, "", err
	}
	defer stop()
	// run: the cloud's transactions of a connection that sends n UPDATEs (its own queries and the
	// count's included: the difference of two runs is the UPDATEs' alone)
	run := func(n int) (int64, error) {
		x0, err := xacts()
		if err != nil {
			return 0, err
		}
		c, err := e.viaProxy()
		if err != nil {
			return 0, err
		}
		for i := 0; i < n; i++ {
			org := 1 + i%2
			if _, err := simple(e.ctx, c, fmt.Sprintf("UPDATE documents SET title = title WHERE organization_id = %d AND id = %s", org, ids[org])); err != nil {
				c.Close(e.ctx)
				return 0, err
			}
		}
		c.Close(e.ctx) // its statistics flushed at its exit
		x1, err := xacts()
		return x1 - x0, err
	}
	// the least of three tries: other transactions of the cloud (a subscription's apply worker
	// in the logical mode's stand) only add to a count
	per, few, many := 0.0, int64(0), int64(0)
	for try := 0; try < 3; try++ {
		f, err := run(10)
		if err != nil {
			return false, "", err
		}
		m, err := run(60)
		if err != nil {
			return false, "", err
		}
		if p := float64(m-f) / 50; try == 0 || p < per {
			per, few, many = p, f, m
		}
	}
	return per < 3.0, fmt.Sprintf("UPDATEs of organizations 1 and 2 in turn, each a query string of its own, the least of three tries: %d transactions of the cloud for 10, %d for 60, %.2f per UPDATE (set_config and the pause lock as statements of their own: 3.5 on the stand; as one: one fewer)", few, many, per), nil
}

// PF13 (BACKLOG block 9, item 12; P3-10): 30 connections, one per request, read the same child
// row: its organization is looked up in documents__refs. Each session looked it up before;
// the process's cache now has it after the first. Counted in the index scans of the refs table.
func checkOrgCacheShared(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	scans := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, e.cloud(), "SELECT (coalesce(idx_scan, 0) + coalesce(seq_scan, 0))::text FROM pg_stat_user_tables WHERE relname = 'documents__refs'")
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	stop, err := e.metaCacheProxy(map[string]any{})
	if err != nil {
		return false, "", err
	}
	defer stop()
	s0, err := scans()
	if err != nil {
		return false, "", err
	}
	const conns = 30
	for i := 0; i < conns; i++ {
		c, err := e.poolConn("app", "app", "")
		if err != nil {
			return false, "", err
		}
		if _, err := c.PgConn().Exec(e.ctx, "SELECT count(*) FROM document_versions WHERE document_id = "+doc).ReadAll(); err != nil {
			c.Close(e.ctx)
			return false, "", err
		}
		c.Close(e.ctx)
	}
	s1, err := scans()
	if err != nil {
		return false, "", err
	}
	return s1-s0 <= 3, fmt.Sprintf("%d connections, each reads document %s's versions: %d scans of documents__refs (one lookup per connection before, one in all with the process's cache)", conns, doc, s1-s0), nil
}

// PF14: a read by a cold key took a lookup in documents__refs of its own, a transaction of the
// cloud's, before its statement; the statement now takes its key's organization within its own
// exchange (inlinelookup.go). The cloud's transactions per read of a document's versions by the
// document's id, a new id each time, on one connection (pgx, the extended protocol): the
// difference of 10 and 60 reads, the least of three tries.
func checkInlineLookup(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	xacts := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond)
		v, err := one(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "SELECT xact_commit::text FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	ids, err := one(e.ctx, e.cloud(), "SELECT string_agg(id::text, ',' ORDER BY id) FROM documents")
	if err != nil {
		return false, "", err
	}
	docs := strings.Split(ids, ",")
	if len(docs) < 70 {
		return false, "", fmt.Errorf("%d documents, want 70", len(docs))
	}
	// one try: a proxy of its own (the organizations of ids it knows are its process's: none),
	// 10 reads then 60, every id new to it; the difference is the 50 reads' alone
	try := func() (float64, error) {
		stop, err := e.metaCacheProxy(map[string]any{})
		if err != nil {
			return 0, err
		}
		defer stop()
		next := 0
		run := func(n int) (int64, error) {
			x0, err := xacts()
			if err != nil {
				return 0, err
			}
			c, err := e.poolConn("app", "app", "")
			if err != nil {
				return 0, err
			}
			defer c.Close(e.ctx)
			for i := 0; i < n; i++ {
				var k int
				if err := c.QueryRow(e.ctx, "SELECT count(*) FROM document_versions WHERE document_id = $1", docs[next]).Scan(&k); err != nil {
					return 0, err
				}
				next++
			}
			c.Close(e.ctx)
			x1, err := xacts()
			return x1 - x0, err
		}
		f, err := run(10)
		if err != nil {
			return 0, err
		}
		m, err := run(60)
		if err != nil {
			return 0, err
		}
		return float64(m-f) / 50, nil
	}
	var pers []float64
	for i := 0; i < 3; i++ {
		p, err := try()
		if err != nil {
			return false, "", err
		}
		pers = append(pers, p)
	}
	sort.Float64s(pers)
	per := pers[1] // the median: other transactions of the cloud add to a count, a flush in between takes from one
	// counted so (the statistics of a session that stays), a read is about 0.5 below its real
	// count: 1.54 on the stand with the lookup a transaction of its own, 0.54 within the exchange
	return per < 1.04, fmt.Sprintf("reads of a document's versions by a document id new to the proxy, three tries: %.2f, %.2f, %.2f transactions of the cloud per read, median %.2f (the lookup a transaction of its own: 1.54 on the stand; within the statement's exchange: one fewer)", pers[0], pers[1], pers[2], per), nil
}
