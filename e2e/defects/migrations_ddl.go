package main

// Replay of the DDL forms migrations use (tasks/BACKLOG.md, block 7, item 6).

import (
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "M5", name: "a Rails rename_column migration reaches the organizations; the application then uses the new name", run: checkRenameColumn},
		check{id: "M6", name: "CREATE INDEX CONCURRENTLY on a LokaDB table: built in the organizations and the cloud, writes go on meanwhile", run: checkConcurrentIndex},
		check{id: "M8", name: "a migration's index on an extension the organization lacks: built in the cloud, left out there, the migration passes", run: checkIndexOnMissingExtension},
		check{id: "M9", name: "rows written into the organization's database past the cloud: refs-diff finds them, refs-sync adds them (and moves the sequence on)", run: checkRefsDiffSync},
		check{id: "M10", name: "the proxy's own pg_temp objects of COPY FROM STDIN (__lokadb_copy_*) are refused to clients", run: checkOwnTempObjects},
		check{id: "M11", name: "CREATE INDEX CONCURRENTLY by a role that does not own the table: refused by the cloud before any organization's database builds an index", run: checkConcurrentIndexRights},
		check{id: "M12", name: "a migration that names public.<table> for an organization whose tables are in schema lokadb: replayed on lokadb.<table>, the customer's own public.<table> untouched", run: checkQualifiedPublicDDL},
		check{id: "M13", name: "refs-sync does not move the cloud's shared sequence to a huge id the organization wrote past it, and says so", run: checkRefsSequenceJump},
		check{id: "M14", name: "CREATE INDEX CONCURRENTLY on a LokaDB table outside public (app.documents): built in the cloud and in the organization's app.documents", run: checkConcurrentIndexOutside},
		check{id: "M15", name: "an organization moved back to the cloud: refs-diff -org N and -cloud compare its refs and the map of a unique column with its rows there, refs-sync fixes them", run: checkRefsCloudOrg},
		check{id: "M7", name: "the organization's role may not create in public: move-org takes schema lokadb (by triggers), CREATE INDEX is replayed there; -mode logical says what to grant", run: checkIndexInPublic},
	)
}

// rails: the statements of a Rails migration as ActiveRecord's PostgreSQL adapter sends them
// (simple query protocol, one transaction, the version recorded at the end).
func (e *env) rails(stmts ...string) error {
	all := []string{"BEGIN"}
	all = append(all, stmts...)
	all = append(all, `INSERT INTO "schema_migrations" ("version") VALUES ('20261023000000') RETURNING "version"`, "COMMIT")
	return e.script(all...)
}

// M5. rename_column was refused: "RENAME on LokaDB tables is not supported".
func checkRenameColumn(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), `CREATE TABLE schema_migrations (version varchar PRIMARY KEY)`, "ALTER TABLE schema_migrations OWNER TO app"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	if err := e.script(`CREATE INDEX "index_documents_on_title" ON "documents" ("title")`); err != nil {
		return false, "", err
	}
	// rename_column :documents, :title, :name (with the index Rails renames along)
	migErr := e.rails(`ALTER TABLE "documents" RENAME COLUMN "title" TO "name"`,
		`ALTER INDEX "index_documents_on_title" RENAME TO "index_documents_on_name"`)
	got, err := e.bothSides("title", "name")
	if err != nil {
		return false, "", err
	}
	idx, err := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(indexname::text, ',' ORDER BY indexname), '-') FROM pg_indexes WHERE tablename = 'documents' AND indexname LIKE 'index_documents_on_%'")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, insErr := simple(e.ctx, c, "INSERT INTO documents (organization_id, name) VALUES (1, 'm5')")
	res, selErr := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 1 AND name LIKE 'd%'")
	n := "-"
	if selErr == nil && len(res) > 0 && len(res[0].Rows) > 0 {
		n = string(res[0].Rows[0][0])
	}
	at, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE name = 'm5'")
	if err != nil {
		at = "error: " + oneLine(err.Error())
	}
	ok := migErr == nil && got == "cloud name, organization name" && idx == "index_documents_on_name" && insErr == nil && n == "100" && at == "1"
	return ok, fmt.Sprintf("migration error %s; %s; the organization's index %s; insert with the new name: error %s, at the organization %s; read through the proxy: error %s, %s rows",
		errText(migErr), got, idx, errText(insErr), at, errText(selErr), n), nil
}

// M6. CREATE INDEX CONCURRENTLY was refused; a plain CREATE INDEX holds the writes of every
// organization while it builds.
func checkConcurrentIndex(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// A transaction left open on the table: CONCURRENTLY waits for it to end, writes go on
	// meanwhile; a plain CREATE INDEX would hold them (and wait for it too).
	holder, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	if _, err := simple(e.ctx, holder, "BEGIN; SELECT count(*) FROM documents WHERE organization_id = 2"); err != nil {
		return false, "", err
	}
	done := make(chan error, 1)
	go func() {
		c, err := e.viaProxy()
		if err != nil {
			done <- err
			return
		}
		defer c.Close(e.ctx)
		_, err = simple(e.ctx, c, "CREATE INDEX CONCURRENTLY documents_title_m6 ON documents (title)")
		done <- err
	}()
	time.Sleep(2 * time.Second) // the build waits for the holder now
	w, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	start := time.Now()
	_, w1 := simple(e.ctx, w, "SET statement_timeout = '3s'")
	if w1 == nil {
		_, w1 = simple(e.ctx, w, "INSERT INTO documents (organization_id, title) VALUES (2, 'm6')")
	}
	if w1 == nil {
		_, w1 = simple(e.ctx, w, "INSERT INTO documents (organization_id, title) VALUES (1, 'm6')")
	}
	took := time.Since(start)
	if _, err := simple(e.ctx, holder, "COMMIT"); err != nil {
		return false, "", err
	}
	var idxErr error
	select {
	case idxErr = <-done:
	case <-time.After(90 * time.Second):
		idxErr = fmt.Errorf("CREATE INDEX CONCURRENTLY did not finish in 90 s")
	}
	cl, err := one(e.ctx, e.cloud(), `SELECT coalesce(string_agg(c.relname || ':' || i.indisvalid, ',' ORDER BY c.relname), '-') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname LIKE 'documents_title_m6%'`)
	if err != nil {
		return false, "", err
	}
	org, err := one(e.ctx, e.customer(), `SELECT coalesce(string_agg(c.relname || ':' || i.indisvalid, ','), '-') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = 'documents_title_m6'`)
	if err != nil {
		return false, "", err
	}
	diff, code, err := e.schemaCmd("schema-diff")
	if err != nil {
		return false, "", err
	}
	// each write within its statement_timeout while the build is held by the holder: a plain
	// CREATE INDEX would hold them until the holder ends
	ok := idxErr == nil && w1 == nil && org == "documents_title_m6:true" &&
		strings.HasPrefix(cl, "documents_title_m6:true,") && !strings.Contains(cl, ":false") && code == 0
	return ok, fmt.Sprintf("index error %s; writes during the build: error %s in %s; cloud %s; organization %s; schema-diff exit %d: %s",
		errText(idxErr), errText(w1), took.Round(time.Millisecond), cl, org, code, oneLine(diff)), nil
}

// M7 (found by block 7). move-org put the organization's tables in public when the admin
// role could create there and give them to -customer-app; that role may not create in public
// (Postgres 15+), and CREATE INDEX needs it: every CREATE INDEX on a LokaDB table through the
// proxy was then refused, in every organization. Now such an organization's tables go to
// schema lokadb (owned by the role; the move by triggers), and -mode logical says what to grant.
func checkIndexInPublic(e *env) (bool, string, error) {
	if moveMode == "logical" { // the check is about the move by triggers (schema lokadb); its logical side is in the check
		return true, "DEFECTS_MODE=logical: the check moves by triggers", nil
	}
	if err := execSQL(e.ctx, e.customer(), "REVOKE CREATE ON SCHEMA public FROM lokadb_app"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	if moveMode == "" || moveMode == "auto" { // DEFECTS_MODE trigger: no logical attempt to refuse
		out, err := e.lokadb(append(e.moveArgs(1, "-customer-app", e.appCustomer()), "-mode", "logical")...)
		if err == nil || !strings.Contains(out, `GRANT CREATE ON SCHEMA public TO "lokadb_app"`) {
			ok = false
		}
		left, lerr := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_class WHERE relname LIKE 'documents%'")
		if lerr != nil {
			return false, "", lerr
		}
		if left != "0" {
			ok = false
		}
		notes = append(notes, fmt.Sprintf("-mode logical: refused %v, with the GRANT %v, tables left at the organization %s", err != nil, strings.Contains(out, `GRANT CREATE ON SCHEMA public TO "lokadb_app"`), left))
	}
	out, err := e.move(1, "-customer-app", e.appCustomer())
	if err != nil {
		return false, strings.Join(append(notes, "move: "+tail(out)), "; "), nil
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	err = e.script("CREATE INDEX documents_title_m7 ON documents (title)")
	idx, ierr := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(schemaname::text, ','), '-') FROM pg_indexes WHERE indexname = 'documents_title_m7'")
	if ierr != nil {
		return false, "", ierr
	}
	if err != nil || sch != "lokadb" || idx != "lokadb" || !strings.Contains(out, "mode=trigger") {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("auto: tables in schema %s, by triggers %v; CREATE INDEX through the proxy: error %s, at the organization in %s", sch, strings.Contains(out, "mode=trigger"), errText(err), idx))
	return ok, strings.Join(notes, "; "), nil
}

// M8 (C8 for later migrations). move-org leaves out an index the organization's database
// cannot have (an operator class of an extension it lacks); a migration adding such an
// index was refused, for every organization, because one database could not build it.
func checkIndexOnMissingExtension(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE EXTENSION pg_trgm"); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	plainErr := e.script("CREATE INDEX documents_title_trgm ON documents USING gin (title gin_trgm_ops)")
	concErr := e.script("CREATE INDEX CONCURRENTLY documents_title_trgm_c ON documents USING gin (title gin_trgm_ops)")
	cl, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_indexes WHERE tablename = 'documents' AND indexname LIKE 'documents_title_trgm%'")
	if err != nil {
		return false, "", err
	}
	org, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE indexname LIKE 'documents_title_trgm%'")
	if err != nil {
		return false, "", err
	}
	diff, code, err := e.schemaCmd("schema-diff")
	if err != nil {
		return false, "", err
	}
	ok := plainErr == nil && concErr == nil && cl == "2" && org == "0" && code == 2 && strings.Count(diff, "missing_index") == 2
	return ok, fmt.Sprintf("CREATE INDEX error %s; CONCURRENTLY error %s; indexes on the cloud's parent %s, at the organization %s; schema-diff exit %d, missing_index %d",
		errText(plainErr), errText(concErr), cl, org, code, strings.Count(diff, "missing_index")), nil
}

// M9 (block 7, item 5). Rows written straight into the organization's database, past the
// cloud, were not in the refs table: the proxy did not find them by id, and nothing said so.
func checkRefsDiffSync(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	gone, err := one(e.ctx, e.customer(), "SELECT min(id) FROM "+sch+".documents")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(),
		"INSERT INTO "+sch+".documents (id, organization_id, title) VALUES (900001, 1, 'written past the cloud')",
		"DELETE FROM "+sch+".documents WHERE id = "+gone); err != nil {
		return false, "", err
	}
	diff, dcode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	var notes []string
	ok := dcode == 2 && strings.Contains(diff, "1 row(s) written past the cloud") && strings.Contains(diff, "1 id(s) of rows the organization no longer has") && strings.Contains(diff, "900001 is past the cloud's sequence")
	notes = append(notes, fmt.Sprintf("refs-diff exit %d: %s", dcode, oneLine(diff)))
	sync, scode, err := e.schemaCmd("refs-sync")
	if err != nil {
		return false, "", err
	}
	if scode != 2 || !strings.Contains(sync, "kept") {
		ok = false
		notes = append(notes, fmt.Sprintf("refs-sync without -delete-stale: exit %d: %s", scode, oneLine(sync)))
	}
	// the id is far past the sequence (a jump over -max-sequence-jump's default, M13): moved on
	// only when asked for
	if sync, scode, err = e.schemaCmd("refs-sync", "-delete-stale", "-max-sequence-jump", "1000000"); err != nil {
		return false, "", err
	}
	after, acode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	if scode != 0 || acode != 0 {
		ok = false
		notes = append(notes, fmt.Sprintf("refs-sync -delete-stale exit %d, refs-diff after it exit %d: %s", scode, acode, oneLine(sync+after)))
	}
	// the proxy now finds the row by its id, and the cloud's next id is past it
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	res, selErr := simple(e.ctx, c, "SELECT title FROM documents WHERE id = 900001")
	got := "-"
	if selErr == nil && len(res) > 0 && len(res[0].Rows) > 0 {
		got = string(res[0].Rows[0][0])
	}
	res, insErr := simple(e.ctx, c, "INSERT INTO documents (organization_id, title) VALUES (2, 'next') RETURNING id")
	next := "-"
	if insErr == nil && len(res) > 0 && len(res[0].Rows) > 0 {
		next = string(res[0].Rows[0][0])
	}
	if got != "written past the cloud" || insErr != nil || len(next) < 6 || next <= "900001" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("by id through the proxy: %q (error %s); the cloud's next id %s (error %s)", got, errText(selErr), next, errText(insErr)))
	return ok, strings.Join(notes, "; "), nil
}

// M10 (block 2's COPY FROM STDIN). The proxy makes pg_temp."__lokadb_copy_<table>" (a table, a
// function, a trigger) for its own statements; a client naming them must be refused, as for
// the other objects of LokaDB's.
func checkOwnTempObjects(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ok := true
	var notes []string
	for _, q := range []string{
		`SELECT * FROM pg_temp."__lokadb_copy_documents"`,
		`CREATE TEMP TABLE "__lokadb_copy_documents" (id int)`,
		`SELECT pg_temp."__lokadb_copy_documents"()`,
		`CREATE TRIGGER "__lokadb_copy" BEFORE INSERT ON documents FOR EACH ROW EXECUTE FUNCTION pg_temp."__lokadb_copy_documents"()`,
		`DROP TABLE IF EXISTS pg_temp."__lokadb_copy_documents"`,
	} {
		_, err := simple(e.ctx, c, q)
		if err == nil || !strings.Contains(err.Error(), "belongs to LokaDB") {
			ok = false
			notes = append(notes, "not refused: "+q+" ("+errText(err)+")")
		}
	}
	if _, err := simple(e.ctx, c, "CREATE TEMP TABLE app_scratch (id int)"); err != nil {
		ok = false
		notes = append(notes, "the application's own temp table refused: "+errText(err))
	}
	if len(notes) == 0 {
		notes = append(notes, "five statements naming __lokadb_copy_* refused; an ordinary temp table works")
	}
	return ok, strings.Join(notes, "; "), nil
}

// M11 (protocol review PR-8). CREATE INDEX CONCURRENTLY built the index in every organization's
// database, as the owner, before the cloud checked the role's rights; the cloud then refused
// ("must be owner") and the organizations' indexes were dropped again. An event trigger in
// the organization's database counts the CREATE INDEX statements it ran.
func checkConcurrentIndexRights(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO intruder"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(),
		"CREATE TABLE pr8_audit (cmd text)",
		"CREATE FUNCTION pr8_log() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER AS $$ BEGIN INSERT INTO pr8_audit VALUES (tg_tag); END $$",
		"CREATE EVENT TRIGGER pr8 ON ddl_command_start WHEN TAG IN ('CREATE INDEX') EXECUTE FUNCTION pr8_log()"); err != nil {
		return false, "", err
	}
	// the organization's credentials in the proxy's config: every role's DDL reaches the
	// organization with them, so the cloud's rights check is all that stands before the build
	stop, err := e.proxyWith(map[string]string{"1": e.appCustomer()})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := pgconn.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=intruder password=intruder sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, ierr := simple(e.ctx, c, "CREATE INDEX CONCURRENTLY documents_title_m11 ON documents (title)")
	built, err := one(e.ctx, e.customer(), "SELECT count(*) FROM pr8_audit")
	if err != nil {
		return false, "", err
	}
	// the owner's own CREATE INDEX CONCURRENTLY still builds (M6)
	return ierr != nil && built == "0", fmt.Sprintf("by a role that does not own the table: error %s; CREATE INDEX statements run in the organization's database: %s", errText(ierr), built), nil
}

// M12 (routing review R2). The organization's tables are in schema lokadb of its database: the
// customer has a table of its own named documents in public (as in C9). A migration that
// names public.documents (pg_dump, Rails structure.sql, Flyway) was replayed there as it was:
// it changed the customer's own public.documents and left lokadb.documents behind.
func checkQualifiedPublicDDL(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.customer(), "CREATE TABLE public.documents (id int PRIMARY KEY, note text)",
		"INSERT INTO public.documents VALUES (1, 'the customer''s own')",
		// the customer's application role owns its own table: nothing but the name stops a replay there
		"ALTER TABLE public.documents OWNER TO lokadb_app"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if out, err := e.moveTo(e.customer(), "-customer-app", e.appCustomer()); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	stop, err := e.proxyWith(map[string]string{"1": e.appCustomer()})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var errs []string
	for _, q := range []string{
		"ALTER TABLE public.documents ADD COLUMN m12 text",
		`CREATE INDEX documents_m12_idx ON "public"."documents" (m12)`,
		"ALTER TABLE public.documents RENAME COLUMN m12 TO m12b",
		"CREATE INDEX CONCURRENTLY documents_m12_cic ON public.documents (title)",
	} {
		if _, err := simple(e.ctx, c, q); err != nil {
			errs = append(errs, q+": "+firstErrLine(err))
		}
	}
	cols := func(rel string) string {
		v, err := one(e.ctx, e.customer(), `SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, rel)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	idx := func(rel string) string {
		v, _ := one(e.ctx, e.customer(), `SELECT coalesce(string_agg(c.relname, ',' ORDER BY c.relname), '-') FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE i.indrelid = $1::regclass AND c.relname LIKE 'documents_m12%'`, rel)
		return v
	}
	own, _ := one(e.ctx, e.customer(), "SELECT note FROM public.documents WHERE id = 1")
	pubCols, pubIdx := cols("public.documents"), idx("public.documents")
	orgCols, orgIdx := cols("lokadb.documents"), idx("lokadb.documents")
	diff, code, err := e.schemaCmd("schema-diff")
	if err != nil {
		return false, "", err
	}
	ok := len(errs) == 0 && pubCols == "id,note" && pubIdx == "-" && own == "the customer's own" &&
		strings.HasSuffix(orgCols, ",m12b") && orgIdx == "documents_m12_cic,documents_m12_idx" && code == 0
	return ok, fmt.Sprintf("errors %v; the customer's public.documents: columns %s, indexes %s, its row %q; lokadb.documents: columns %s, indexes %s; schema-diff exit %d: %s",
		errs, pubCols, pubIdx, own, orgCols, orgIdx, code, oneLine(diff)), nil
}

// M13 (admin review A6). The organization writes its own ids: refs-sync moved the cloud's
// sequence, which every organization shares, to the largest of them. One row with a huge id
// there moved it near its maximum: every organization's next insert would fail (nextval
// reached maximum value), or the ids of the others would be taken.
func checkRefsSequenceJump(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	seq := func() string {
		v, _ := one(e.ctx, e.cloud(), "SELECT last_value FROM documents_id_seq")
		return v
	}
	before := seq()
	if err := execSQL(e.ctx, e.customer(),
		"INSERT INTO "+sch+".documents (id, organization_id, title) VALUES (9223372036854000000, 1, 'a huge id')"); err != nil {
		return false, "", err
	}
	out, code, err := e.schemaCmd("refs-sync")
	if err != nil {
		return false, "", err
	}
	after := seq()
	refs, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents__refs WHERE id = 9223372036854000000 AND tenant = 1")
	// the cloud's next insert still gets an id of its own range
	ins := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (2, 3, 'after refs-sync')")
	ok := code == 2 && after == before && refs == "1" && ins == nil &&
		strings.Contains(out, "the sequence is not moved") && strings.Contains(out, "-max-sequence-jump")
	return ok, fmt.Sprintf("refs-sync exit %d: %s; sequence %s before, %s after; the id in the refs: %s; an insert after it: %s",
		code, oneLine(out), before, after, refs, errText(ins)), nil
}

// M14 (review 3 of the protocol, P12). CREATE INDEX CONCURRENTLY checked the role's rights on
// public."app.documents", which does not exist: on a table outside public it always failed
// ("nothing was built").
func checkConcurrentIndexOutside(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if out, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, ierr := simple(e.ctx, c, "CREATE INDEX CONCURRENTLY documents_title_m14 ON app.documents (title)")
	idx := func(d string) string {
		v, err := one(e.ctx, d, `SELECT coalesce(string_agg(i.indexrelid::regclass::text || ':' || i.indisvalid, ',' ORDER BY 1), '-') FROM pg_index i
			WHERE i.indrelid = 'app.documents'::regclass AND i.indexrelid::regclass::text LIKE '%documents_title_m14'`)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	cloud, org := idx(e.cloud()), idx(e.customer())
	return ierr == nil && cloud == "app.documents_title_m14:true" && org == "app.documents_title_m14:true",
		fmt.Sprintf("CREATE INDEX CONCURRENTLY: %s; the cloud's parent: %s; the organization's app.documents: %s", errText(ierr), cloud, org), nil
}

// M15 (review 3, R3-4). refs-diff compared only the organizations of lokadb.orgs: one moved back
// to the cloud was not looked at, and the maps of unique columns (lokadb.unique_refs) never were.
// The tables of X19 (docs.code unique, mapped); organization 1 is moved and back, then its
// refs and its map are spoilt: an id and a code left out, an id and a code of no row added.
func checkRefsCloudOrg(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE docs (id bigserial PRIMARY KEY, org bigint NOT NULL, code text UNIQUE, title text)`,
		`CREATE TABLE notes (id bigserial PRIMARY KEY, org bigint NOT NULL, doc_code text REFERENCES docs (code) ON DELETE CASCADE, body text)`,
		`ALTER TABLE docs OWNER TO app`, `ALTER TABLE notes OWNER TO app`,
		`INSERT INTO docs (org, code, title) SELECT 1 + g % 2, 'c' || g, 'd' || g FROM generate_series(1, 40) g`,
		`INSERT INTO docs (org, code, title) VALUES (1, NULL, 'no code')`); err != nil {
		return false, "", err
	}
	e.spec = fkUniqueSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + firstErrLine(err), nil
	}
	if out, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if _, err := e.run6(e.moveBackArgs(1)...); err != nil {
		return false, "move-back failed: " + errLine(err), nil
	}
	gone, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM docs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"DELETE FROM docs__refs WHERE id = "+gone,
		"INSERT INTO docs__refs (id, tenant) VALUES (990001, 1)",
		"DELETE FROM docs__refs__code WHERE id = 'c2'",
		"INSERT INTO docs__refs__code (id, tenant) VALUES ('zz', 1)"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	cmd := func(args ...string) (string, int) {
		out, code, err := e.schemaCmd(args...)
		if err != nil {
			return "error: " + err.Error(), -1
		}
		return out, code
	}
	found := func(out string) bool {
		return strings.Contains(out, "organization 1 (in the cloud), docs: 1 row(s) written past the cloud") &&
			strings.Contains(out, "1 id(s) of rows the organization no longer has (ids 990001)") &&
			strings.Contains(out, "organization 1 (in the cloud), docs, the map of code: 1 row(s) written past the cloud, not in the refs table (ids c2)") &&
			strings.Contains(out, "(ids zz)")
	}
	plain, pcode := cmd("refs-diff")
	byOrg, ocode := cmd("refs-diff", "-org", "1")
	all, acode := cmd("refs-diff", "-cloud")
	if !(ocode == 2 && found(byOrg) && acode == 2 && found(all) && strings.Contains(all, "organization 2 (in the cloud), docs: refs match")) {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("refs-diff (moved ones only) exit %d: %s; -org 1 exit %d: %s; -cloud exit %d: %s",
		pcode, oneLine(plain), ocode, oneLine(byOrg), acode, oneLine(all)))
	sync, scode := cmd("refs-sync", "-org", "1", "-delete-stale")
	after, fcode := cmd("refs-diff", "-cloud")
	if scode != 0 || fcode != 0 {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("refs-sync -org 1 -delete-stale exit %d: %s; refs-diff -cloud after it exit %d: %s", scode, oneLine(sync), fcode, oneLine(after)))
	return ok, strings.Join(notes, "; "), nil
}
