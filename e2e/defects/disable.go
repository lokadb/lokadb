package main

// disable undoes enable (tasks/rollback.md, section 3).

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "DS1", name: "enable, move-org, move-back, disable: schema and rows are as before enable", run: checkRoundTrip},
		check{id: "DS2", name: "disable gives an identity column back its kind (GENERATED ALWAYS) and the table its owner", run: checkDisableIdentity},
		check{id: "DS3", name: "disable is refused while an organization is in its own database; -dry-run counts the rows it would copy", run: checkDisableRefused},
		check{id: "DS4", name: "a publication of a table publishes it again after disable, with its column list and publish_via_partition_root as before", run: checkDisablePublication},
		check{id: "DS5", name: "enable -prepare without enable, then disable: schema and rows are as before", run: checkDisableAfterPrepare},
		check{id: "DS7", name: "disable leaves schema lokadb and says so while an object of the application depends on it, instead of dropping that object with it", run: checkDisableDependents},
	)
}

// DS7 (review A11). After enable the application made a view over lokadb.org(). DROP SCHEMA
// lokadb CASCADE dropped it silently; now disable stops before the schema, names the view, and
// drops the schema once the view is gone.
func checkDisableDependents(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE VIEW current_org AS SELECT lokadb.org() AS org"); err != nil {
		return false, "", err
	}
	out, err := e.run6(e.disableArgs()...)
	refused := err != nil && strings.Contains(out, "view current_org")
	view, err2 := one(e.ctx, e.cloud(), "SELECT (to_regclass('current_org') IS NOT NULL)::text || ' ' || (to_regnamespace('lokadb') IS NOT NULL)::text || ' ' || (SELECT relkind::text FROM pg_class WHERE oid = 'documents'::regclass)")
	if err2 != nil {
		return false, "", err2
	}
	if err := execSQL(e.ctx, e.cloud(), "DROP VIEW IF EXISTS current_org"); err != nil {
		return false, "", err
	}
	_, againErr := e.run6(e.disableArgs()...)
	schema, err3 := one(e.ctx, e.cloud(), "SELECT (to_regnamespace('lokadb') IS NOT NULL)::text")
	if err3 != nil {
		return false, "", err3
	}
	msg := "accepted"
	if err != nil {
		msg = errLine(err)
	}
	return refused && view == "true true r" && againErr == nil && schema == "false",
		fmt.Sprintf("disable with the view: %s; view, schema lokadb, documents' kind after it: %q (want \"true true r\"); "+
			"disable again without the view: %v, schema lokadb left: %s", msg, view, againErr, schema), nil
}

// DS5. enable -prepare added refs tables, triggers, a child's organization column, FKs and an
// index to the plain tables; enable never ran. disable takes all of it back.
func checkDisableAfterPrepare(e *env) (bool, string, error) {
	if err := richer(e); err != nil {
		return false, "", err
	}
	if err := e.enableArgs("-prepare"); err != nil {
		return false, "enable -prepare failed: " + oneLine(err.Error()), nil
	}
	write := func() error { // past -prepare its triggers fill the refs tables and the new column
		return execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'prepared')",
			"INSERT INTO document_versions (document_id, content) SELECT id, 'prepared' FROM documents WHERE title = 'prepared'")
	}
	if err := write(); err != nil {
		return false, "", err
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		return false, "disable failed: " + errLine(err), nil
	}
	got, err := one(e.ctx, e.cloud(), catalogOf)
	if err != nil {
		return false, "", err
	}
	before, err := e.beforeEnable(write)
	if err != nil {
		return false, "", err
	}
	if got != before {
		return false, "differs from before enable -prepare: " + diffLines(before, got), nil
	}
	return true, fmt.Sprintf("%d catalog lines and rows equal to before enable -prepare", strings.Count(got, "\n")+1), nil
}

// catalogOf describes the application's tables the way the application sees them: columns,
// defaults, identity, constraints, indexes, triggers, views over them, owner and privileges,
// and every table's rows (count and a hash). OIDs and attribute numbers are left out.
const catalogOf = `
WITH t AS (SELECT oid, relname FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relname IN ('users', 'documents', 'document_versions'))
SELECT string_agg(x, E'\n' ORDER BY x) FROM (
  SELECT 'col ' || t.relname || '.' || a.attname || ' ' || format_type(a.atttypid, a.atttypmod) || ' notnull=' || a.attnotnull
         || ' identity=' || a.attidentity::text || ' generated=' || a.attgenerated::text || ' default=' || coalesce(pg_get_expr(d.adbin, d.adrelid), '')
    FROM t JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
  UNION ALL SELECT 'con ' || t.relname || ' ' || c.conname || ' ' || pg_get_constraintdef(c.oid) || ' valid=' || c.convalidated
    FROM t JOIN pg_constraint c ON c.conrelid = t.oid
  UNION ALL SELECT 'idx ' || t.relname || ' ' || pg_get_indexdef(i.indexrelid)
    FROM t JOIN pg_index i ON i.indrelid = t.oid
  UNION ALL SELECT 'trg ' || t.relname || ' ' || pg_get_triggerdef(g.oid) || ' ' || g.tgenabled::text
    FROM t JOIN pg_trigger g ON g.tgrelid = t.oid AND NOT g.tgisinternal
  UNION ALL SELECT 'view ' || viewname || ' ' || definition FROM pg_views WHERE schemaname = 'public'
  UNION ALL SELECT 'own ' || t.relname || ' ' || pg_get_userbyid(c.relowner) || ' ' || coalesce(c.relacl::text, '') || ' kind=' || c.relkind::text
    FROM t JOIN pg_class c ON c.oid = t.oid
  UNION ALL SELECT 'seq ' || s.relname || ' ' || pg_get_userbyid(s.relowner) FROM pg_class s
    WHERE s.relkind = 'S' AND s.relnamespace = 'public'::regnamespace
  UNION ALL SELECT 'rows users ' || count(*) || ' ' || coalesce(sum(hashtext(u::text)::numeric), 0) FROM users u
  UNION ALL SELECT 'rows documents ' || count(*) || ' ' || coalesce(sum(hashtext(d::text)::numeric), 0) FROM documents d
  UNION ALL SELECT 'rows document_versions ' || count(*) || ' ' || coalesce(sum(hashtext(v::text)::numeric), 0) FROM document_versions v
  UNION ALL SELECT 'lokadb ' || count(*) FROM pg_namespace WHERE nspname = 'lokadb'
  UNION ALL SELECT 'rls ' || t.relname || ' ' || c.relrowsecurity || ' ' || c.relforcerowsecurity FROM t JOIN pg_class c ON c.oid = t.oid
  UNION ALL SELECT 'pol ' || tablename || ' ' || policyname || ' ' || permissive || ' ' || roles::text || ' ' || cmd || ' ' || coalesce(qual, '') || ' ' || coalesce(with_check, '')
    FROM pg_policies WHERE schemaname = 'public' AND tablename IN ('users', 'documents', 'document_versions')
  UNION ALL SELECT 'rule ' || tablename || ' ' || definition FROM pg_rules WHERE schemaname = 'public' AND tablename IN ('users', 'documents', 'document_versions')
  UNION ALL SELECT 'colacl ' || t.relname || '.' || a.attname || ' ' || a.attacl::text
    FROM t JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum > 0 AND NOT a.attisdropped AND a.attacl IS NOT NULL
  UNION ALL SELECT 'other ' || relname FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p', 'f', 'v')
    AND relname NOT IN ('users', 'documents', 'document_versions', 'documents_titles')
) s(x)`

// richer: what enable has to carry and disable to give back.
func richer(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reader') THEN CREATE ROLE reader; END IF; END $$",
		"GRANT SELECT ON documents, document_versions TO reader",
		"CREATE VIEW documents_titles AS SELECT id, title FROM documents",
		"ALTER VIEW documents_titles OWNER TO app",
		"CREATE FUNCTION touch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.title := NEW.title; RETURN NEW; END $$",
		"CREATE TRIGGER documents_touch BEFORE UPDATE ON documents FOR EACH ROW EXECUTE FUNCTION touch()",
		"CREATE INDEX documents_title ON documents (title)",
		// the application's own row security, a rule and a privilege on a column (review3 R3-2:
		// disable dropped them with the parent enable carried them to)
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'colreader') THEN CREATE ROLE colreader; END IF; END $$",
		"GRANT SELECT (title) ON documents TO colreader",
		"CREATE RULE documents_notify AS ON INSERT TO documents DO ALSO NOTIFY documents_inserted",
		"ALTER TABLE document_versions ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY versions_reader ON document_versions FOR SELECT TO reader USING (content IS NOT NULL)")
}

func diffLines(a, b string) string {
	in := map[string]int{}
	for _, l := range strings.Split(a, "\n") {
		in[l]++
	}
	for _, l := range strings.Split(b, "\n") {
		in[l]--
	}
	var out []string
	for l, n := range in {
		switch {
		case n > 0:
			out = append(out, "before only: "+l)
		case n < 0:
			out = append(out, "after only: "+l)
		}
	}
	if len(out) > 4 {
		out = append(out[:4], fmt.Sprintf("… %d more", len(out)-4))
	}
	return strings.Join(out, "; ")
}

func (e *env) disableArgs(extra ...string) []string {
	body := spec
	if e.spec != "" {
		body = e.spec
	}
	p, _ := e.specFile(body)
	return append([]string{"disable", "-db", e.cloud(), "-spec", p, "-grace", "1s"}, extra...)
}

// DS1. Rows written while enabled (organization 1 moved and back) are kept; schema lokadb,
// refs tables and partitions are gone.
func checkRoundTrip(e *env) (bool, string, error) {
	if err := richer(e); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	// rows written in each state: moved, back. While enabled the versions' organization column
	// is given (no proxy sets lokadb.org here); the reference database has no such column.
	write := func(title string, enabled bool) error {
		org, from := "", ""
		if enabled {
			org, from = ", organization_id", ", organization_id"
		}
		return execSQL(e.ctx, e.cloud(), fmt.Sprintf(`INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, '%s'), (2, 3, '%s')`, title, title),
			fmt.Sprintf("INSERT INTO document_versions (document_id, content%s) SELECT id, '%s'%s FROM documents WHERE title = '%s' ORDER BY id", org, title, from, title))
	}
	if err := write("moved", true); err != nil {
		return false, "", err
	}
	if _, err := e.run6(e.moveBackArgs(1)...); err != nil {
		return false, "move-back failed: " + errLine(err), nil
	}
	if err := write("back", true); err != nil {
		return false, "", err
	}
	out, err := e.run6(e.disableArgs()...)
	if err != nil {
		return false, "disable failed: " + errLine(err), nil
	}
	if err := execSQL(e.ctx, e.cloud(), "UPDATE documents SET title = title"); err != nil { // the trigger still fires
		return false, "an update after disable: " + short(err), nil
	}
	got, err := one(e.ctx, e.cloud(), catalogOf)
	if err != nil {
		return false, "", err
	}
	// The catalog right after enable is not the reference: it is before enable, rebuilt.
	before, err := e.beforeEnable(func() error {
		if err := write("moved", false); err != nil {
			return err
		}
		return write("back", false)
	})
	if err != nil {
		return false, "", err
	}
	paused := ""
	for _, l := range strings.Split(out, "\n") {
		if i := strings.Index(l, "paused="); i >= 0 {
			paused = ", " + strings.Fields(l[i:])[0]
		}
	}
	if got != before {
		return false, "differs from before enable: " + diffLines(before, got), nil
	}
	return true, fmt.Sprintf("%d catalog lines and rows equal to before enable%s", strings.Count(got, "\n")+1, paused), nil
}

// beforeEnable builds the same database again without lokadb, with the same writes, and
// returns its catalog: the reference a disabled database must equal.
func (e *env) beforeEnable(writes func() error) (string, error) {
	saved := e.db
	e.db = saved + "_ref"
	defer func() { e.db = saved }()
	if err := fresh(e); err != nil {
		return "", err
	}
	if err := richer(e); err != nil {
		return "", err
	}
	if err := writes(); err != nil {
		return "", err
	}
	return one(e.ctx, e.cloud(), catalogOf)
}

// DS2. documents.n is GENERATED ALWAYS; the tables belong to role owner0, not to app (the
// -owner of enable).
func checkDisableIdentity(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'owner0') THEN CREATE ROLE owner0; END IF; END $$",
		"ALTER TABLE documents ADD COLUMN n bigint GENERATED ALWAYS AS IDENTITY (START WITH 7 INCREMENT BY 3)",
		"ALTER TABLE documents OWNER TO owner0"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'while enabled')"); err != nil {
		return false, "", err
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		return false, "disable failed: " + errLine(err), nil
	}
	kind, err := one(e.ctx, e.cloud(), `SELECT attidentity::text || ' ' || pg_get_userbyid((SELECT relowner FROM pg_class WHERE oid = 'documents'::regclass))
		FROM pg_attribute WHERE attrelid = 'documents'::regclass AND attname = 'n'`)
	if err != nil {
		return false, "", err
	}
	maxN, err := one(e.ctx, e.cloud(), "SELECT max(n) FROM documents")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 2, 'after disable')"); err != nil {
		return false, "an insert after disable: " + short(err), nil
	}
	next, err := one(e.ctx, e.cloud(), "SELECT n FROM documents WHERE title = 'after disable'")
	if err != nil {
		return false, "", err
	}
	var mx, nx int64
	fmt.Sscan(maxN, &mx)
	fmt.Sscan(next, &nx)
	return kind == "a owner0" && nx == mx+3, fmt.Sprintf("identity kind and owner %q (want \"a owner0\"); last n %s, next %s", kind, maxN, next), nil
}

// DS3. Organization 1 is moved: disable refuses and changes nothing. After move-back the dry
// run counts organization 1's rows (now outside DEFAULT) and changes nothing either.
func checkDisableRefused(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	out, err := e.run6(e.disableArgs()...)
	refused := err != nil && strings.Contains(out, "move-back")
	if _, err := e.run6(e.moveBackArgs(1)...); err != nil {
		return false, "move-back failed: " + errLine(err), nil
	}
	dry, err := e.run6(e.disableArgs("-dry-run")...)
	if err != nil {
		return false, "dry run failed: " + short(err), nil
	}
	enabled, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.tables")
	if err != nil {
		return false, "", err
	}
	want, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	counted := strings.Contains(dry, "table=documents rows="+want)
	return refused && counted && enabled == "2",
		fmt.Sprintf("refused while moved: %v; dry run counts %s rows of documents outside DEFAULT: %v; still enabled: %v", refused, want, counted, enabled == "2"), nil
}

// DS4. enable moves the publication to the parent and may turn publish_via_partition_root on
// (block 3, A10); disable gives the table back to it as it was.
func checkDisablePublication(e *env) (bool, string, error) {
	const pub = `SELECT coalesce((SELECT string_agg(pr.prrelid::regclass::text || ' ' || coalesce(pr.prattrs::text, ''), ', ')
		FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid WHERE p.pubname = 'docs_pub'), '')
		|| ' viaroot=' || (SELECT pubviaroot FROM pg_publication WHERE pubname = 'docs_pub')`
	if err := execSQL(e.ctx, e.cloud(), "CREATE PUBLICATION docs_pub FOR TABLE documents (id, organization_id, title)"); err != nil {
		return false, "", err
	}
	before, err := one(e.ctx, e.cloud(), pub)
	if err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	during, err := one(e.ctx, e.cloud(), pub)
	if err != nil {
		return false, "", err
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		return false, "disable failed: " + errLine(err), nil
	}
	after, err := one(e.ctx, e.cloud(), pub)
	if err != nil {
		return false, "", err
	}
	return after == before, fmt.Sprintf("before enable %q, enabled %q, after disable %q", before, during, after), nil
}
