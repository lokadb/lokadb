package main

// Block 9, item 5, step 3: the shared cache of the lokadb metadata (meta_cache; by database and
// role, used while the database's stamp is unchanged).

import (
	"fmt"
	"strings"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MC1", name: "the metadata cache: each role gets what it read itself (rights on the partitions, no rights on lokadb.*), never the role read before it", run: checkMetaCacheRoles},
		check{id: "MC2", name: "the metadata cache: enable while no session listens (cached connections only), the next connection routes at once", run: checkMetaCacheStamp},
		check{id: "MC4", name: "the metadata cache: a REVOKE on a partition is seen by the next connection at once, not after metaTTL", run: checkMetaCacheGrant},
		check{id: "MC3", name: "the metadata cache: the cloud's transactions for 40 clients per request, without and with it", run: checkMetaCacheCount},
	)
}

func (e *env) metaCacheProxy(extra map[string]any) (func(), error) {
	cfg := map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}}
	for k, v := range extra {
		cfg[k] = v
	}
	return e.proxyCfg(map[string]string{}, cfg)
}

// MC1. Three roles read documents through one proxy with the cache, the role with every right
// first: b9mc_full (the parent and the partitions), b9mc_part (the parent alone: a read of one
// organization is not aimed at its partition, Meta.Direct), b9mc_none (no rights on lokadb.*:
// refused, B9.12). A cache keyed by the database alone hands the first role's metadata on: the
// second one's read then goes to the partition (42501), the third one is routed.
func checkMetaCacheRoles(e *env) (bool, string, error) {
	part, err := e.metaCacheRoles()
	if err != nil {
		return false, "", err
	}
	// b9mc_part keeps the parent alone: the partition's grant move-org copied is taken back
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON "+part+" FROM b9mc_part"); err != nil {
		return false, "", err
	}
	stop, err := e.metaCacheProxy(map[string]any{"meta_cache": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	const org1 = "SELECT count(*)::text FROM documents WHERE organization_id = 1"
	full := e.metaCacheAsk("b9mc_full", org1)
	partR := e.metaCacheAsk("b9mc_part", org1)
	none := e.metaCacheAsk("b9mc_none", "SELECT count(DISTINCT organization_id)::text FROM documents")
	ok := full == "100" && partR == "100" && strings.Contains(none, "cannot read the lokadb metadata")
	return ok, fmt.Sprintf("organization 1's documents read by the role with every right: %s; by the role with the parent alone, after it: %s (want 100, not 42501); the role without rights on lokadb.*: %s (want refused)", full, partR, none), nil
}

// metaCacheRoles: enable, the roles of MC1 and MC4 granted documents before move-org (it maps
// the roles granted then onto the organization's server and copies their grants to the
// partition), organization 1 moved, b9mc_full granted every table; b9mc_none loses lokadb.*
// (PUBLIC's grant taken back, the others granted by name). Returns organization 1's partition.
func (e *env) metaCacheRoles() (string, error) {
	if err := e.enable(); err != nil {
		return "", err
	}
	for _, r := range []string{"b9mc_full", "b9mc_part", "b9mc_none"} {
		if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
			fmt.Sprintf(`DO $$ BEGIN CREATE ROLE %s LOGIN PASSWORD '%s'; EXCEPTION WHEN duplicate_object THEN END $$`, r, r)); err != nil {
			return "", err
		}
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO b9mc_full, b9mc_part, b9mc_none"); err != nil {
		return "", err
	}
	if _, err := e.move(1); err != nil {
		return "", err
	}
	part, err := one(e.ctx, e.cloud(), "SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'documents'::regclass AND c.relkind = 'f'")
	if err != nil {
		return "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO b9mc_full",
		"REVOKE SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs FROM PUBLIC",
		"GRANT SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs TO b9mc_full, b9mc_part, app",
		"GRANT USAGE ON SCHEMA lokadb TO b9mc_none"); err != nil {
		return "", err
	}
	return part, nil
}

// metaCacheAsk: one statement of role on a connection of its own through the proxy.
func (e *env) metaCacheAsk(role, sql string) string {
	c, err := e.poolConn(role, role, "")
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	defer c.Close(e.ctx)
	var v string
	if err := c.QueryRow(e.ctx, sql).Scan(&v); err != nil {
		return "error: " + firstErrLine(err)
	}
	return v
}

// MC4. A REVOKE on a partition seen at once: b9mc_part reads organization 1 with the
// partition's grant (the read is aimed at the partition, its metadata cached), then loses the
// grant; its next connection, within metaTTL, reads again through the parent instead of getting
// 42501 from the cached metadata (the rights on the partitions are part of the stamp).
func checkMetaCacheGrant(e *env) (bool, string, error) {
	part, err := e.metaCacheRoles()
	if err != nil {
		return false, "", err
	}
	stop, err := e.metaCacheProxy(map[string]any{"meta_cache": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	const org1 = "SELECT count(*)::text FROM documents WHERE organization_id = 1"
	before := e.metaCacheAsk("b9mc_part", org1)
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON "+part+" FROM b9mc_part"); err != nil {
		return false, "", err
	}
	after := e.metaCacheAsk("b9mc_part", org1)
	return before == "100" && after == "100", fmt.Sprintf("organization 1's documents with the partition's grant: %s; right after REVOKE on %s: %s (want 100, not 42501 from metadata cached before)", before, part, after), nil
}

// MC2. The cache of cloud connections is on too, so after a client leaves no session listens to
// lokadb_meta: enable's notification reaches nobody. The next connection must route at once (a
// child row gets its parent's organization), as the stamp no longer matches the entry read
// before enable.
func checkMetaCacheStamp(e *env) (bool, string, error) {
	stop, err := e.metaCacheProxy(map[string]any{"meta_cache": map[string]any{}, "upstream_pool": map[string]any{}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	a, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	if _, err := a.Exec(e.ctx, "SELECT count(*) FROM documents"); err != nil { // not enabled yet: no metadata, kept
		return false, "", err
	}
	a.Close(e.ctx)
	time.Sleep(300 * time.Millisecond)
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	b, err := e.poolConn("app", "app", "")
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	_, ierr := b.PgConn().Exec(e.ctx, fmt.Sprintf("INSERT INTO document_versions (document_id, content) VALUES (%s, 'MC2')", doc)).ReadAll()
	org, err := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(organization_id::text, ','), 'none') FROM document_versions WHERE content = 'MC2'")
	if err != nil {
		return false, "", err
	}
	note := "ok"
	if ierr != nil {
		note = firstErrLine(ierr)
	}
	return org == "1", fmt.Sprintf("a child row of organization 1's document right after enable, no session having heard it: stored under %s (insert: %s)", org, note), nil
}

// MC3. 40 clients, one statement each, every one on a connection of its own (no cache of cloud
// connections): the cloud's committed transactions per client (pg_stat_database.xact_commit; each
// query of the proxy's own outside a transaction is one) without the metadata cache (every
// connection reads the metadata, some 25 queries) and with it (the stamp, one query).
func checkMetaCacheCount(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	scans := func() (int64, error) {
		time.Sleep(1500 * time.Millisecond) // the statistics of ended sessions are flushed at their exit
		v, err := one(e.ctx, dsn(e.cloudHost, "postgres", "postgres"), "SELECT xact_commit::text FROM pg_stat_database WHERE datname = $1", e.db)
		if err != nil {
			return 0, err
		}
		var n int64
		_, err = fmt.Sscan(v, &n)
		return n, err
	}
	const clients = 40
	run := func(cache bool) (int64, error) {
		extra := map[string]any{"meta_cache": map[string]any{"off": !cache}, // on by default since PF1 (block 9)
			"upstream_pool": false} // on by default (27.10): a connection per client, and the statistics of each flushed at its exit
		stop, err := e.metaCacheProxy(extra)
		if err != nil {
			return 0, err
		}
		defer stop()
		s0, err := scans()
		if err != nil {
			return 0, err
		}
		for i := 0; i < clients; i++ {
			c, err := e.poolConn("app", "app", "")
			if err != nil {
				return 0, err
			}
			if _, err := c.PgConn().Exec(e.ctx, "SELECT count(*) FROM documents WHERE organization_id = 1").ReadAll(); err != nil {
				c.Close(e.ctx)
				return 0, err
			}
			c.Close(e.ctx)
		}
		s1, err := scans()
		return s1 - s0, err
	}
	off, err := run(false)
	if err != nil {
		return false, "", err
	}
	on, err := run(true)
	if err != nil {
		return false, "", err
	}
	return on*2 < off, fmt.Sprintf("%d clients: the cloud's transactions %d without the metadata cache (%.1f a client), %d with it (%.1f a client)", clients, off, float64(off)/clients, on, float64(on)/clients), nil
}
