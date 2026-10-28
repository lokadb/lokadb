package main

// Second review of the protocol, P4: the metadata cache's stamp found the LokaDB tables by
// relname, but lokadb.tables keys a table outside public as schema.name: a GRANT or REVOKE on a
// partition of app.documents (a new partition, a trigger) left the stamp as it was, and the next
// connection took the metadata cached before it.

import "fmt"

func init() {
	checks = append(checks,
		check{id: "MC6", name: "the metadata's change log: enable records DDL, GRANT and rows of lokadb's tables (the stamp reads it), a temporary table is no change", run: checkMetaChanges},
		check{id: "MC5", name: "the metadata cache, a table of schema app: a REVOKE on its partition is seen by the next connection at once, not after metaTTL", run: checkMetaCacheGrantSchema},
	)
}

// MC5. MC4 for app.documents: b9mc_app reads organization 1 with the partition's grant (the read
// is aimed at the partition), then loses the grant; its next connection reads through the parent
// instead of getting 42501 from the metadata cached before.
func checkMetaCacheGrantSchema(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// the role before the move: the move maps every role of the cloud to the organization's server
	if err := execSQL(e.ctx, dsn(e.cloudHost, "postgres", "postgres"),
		`DO $$ BEGIN CREATE ROLE b9mc_app LOGIN PASSWORD 'b9mc_app'; EXCEPTION WHEN duplicate_object THEN END $$`); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT USAGE ON SCHEMA app TO b9mc_app",
		"GRANT SELECT ON app.documents TO b9mc_app",
		"GRANT SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs TO b9mc_app"); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	part, err := one(e.ctx, e.cloud(), "SELECT c.oid::regclass::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'app.documents'::regclass AND c.relkind = 'f'")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON "+part+" TO b9mc_app"); err != nil {
		return false, "", err
	}
	stop, err := e.metaCacheProxy(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	const org1 = "SELECT count(*)::text FROM app.documents WHERE org = 1"
	before := e.metaCacheAsk("b9mc_app", org1)
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON "+part+" FROM b9mc_app"); err != nil {
		return false, "", err
	}
	after := e.metaCacheAsk("b9mc_app", org1)
	return before == "50" && after == "50", fmt.Sprintf("organization 1's app.documents with the partition's grant: %s; right after REVOKE on %s: %s (want 50, not 42501 from metadata cached before)", before, part, after), nil
}

// MC6 (BACKLOG block 9, item 10): enable records the metadata's changes (lokadb.meta_changes,
// an event trigger for DDL, GRANT and REVOKE): the stamp reads a few rows instead of hashing the
// catalog. A GRANT and a row of lokadb.orgs add a change, a temporary table does not; a REVOKE
// on a partition is seen by the next connection at once (MC4 with the change log).
func checkMetaChanges(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	count := func() string {
		v, err := one(e.ctx, e.cloud(), "SELECT count(*)::text FROM lokadb.meta_changes")
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	tracked, err := one(e.ctx, e.cloud(), "SELECT (SELECT ddl_tracked FROM lokadb.meta_tracking)::text || ' ' || EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'lokadb_meta_ddl')::text")
	if err != nil {
		return false, "lokadb.meta_tracking: " + firstErrLine(err), nil
	}
	c0 := count()
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO PUBLIC"); err != nil {
		return false, "", err
	}
	c1 := count()
	if err := execSQL(e.ctx, e.cloud(), "BEGIN; CREATE TEMP TABLE mc6 (x int); CREATE INDEX ON mc6 (x); COMMIT"); err != nil {
		return false, "", err
	}
	c2 := count()
	return tracked == "true true" && c1 != c0 && c2 == c1,
		fmt.Sprintf("tracked and the event trigger: %s; changes %s, after GRANT %s, after a temporary table %s (want true true; GRANT adds, the temporary table does not)", tracked, c0, c1, c2), nil
}
