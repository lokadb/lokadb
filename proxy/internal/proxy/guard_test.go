package proxy

import (
	"strings"
	"testing"
)

func TestForbidden(t *testing.T) {
	m := testMeta()
	cases := map[string]bool{
		"SELECT * FROM documents WHERE organization_id = 1":                                      false,
		"SELECT * FROM documents__org1":                                                          true,
		"SELECT * FROM documents d JOIN document_versions__cloud v ON v.id = 1":                  true,
		"INSERT INTO documents__refs VALUES (1, 2)":                                              true,
		"UPDATE public.documents__o2 SET title = 'x'":                                            true,
		"SELECT * FROM lokadb.orgs":                                                              true,
		"ALTER FOREIGN TABLE documents__org1 OPTIONS (SET table_name 'pg_authid')":               true,
		"CREATE FOREIGN TABLE leak (x text) SERVER lokadb_org1 OPTIONS (table_name 'pg_authid')": true,
		"IMPORT FOREIGN SCHEMA public FROM SERVER lokadb_org1 INTO public":                       true,
		"CREATE USER MAPPING FOR app SERVER lokadb_org1 OPTIONS (user 'postgres')":               true,
		"SELECT * FROM other_schema.documents__org1":                                             false,
		"SELECT * FROM documents__o1_source":                                                     true,
		"SELECT last_value FROM documents__id_seq":                                               true,
		// the application's own objects, named like LokaDB's but not LokaDB's
		"SELECT * FROM documents__archive":                                               false,
		"INSERT INTO documents__2024 VALUES (1)":                                         false,
		"SELECT * FROM documents__orgchart":                                              false,
		"CREATE SERVER warehouse FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'dwh')": false,
		"CREATE USER MAPPING FOR app SERVER warehouse OPTIONS (user 'etl')":              false,
		"CREATE FOREIGN TABLE dwh_sales (x int) SERVER warehouse":                        false,
		"IMPORT FOREIGN SCHEMA public FROM SERVER warehouse INTO staging":                false,
		"ALTER SERVER warehouse OPTIONS (SET host 'dwh2')":                               false,
		"DROP SERVER warehouse CASCADE":                                                  false,
		"CREATE FOREIGN DATA WRAPPER mine":                                               false,
		"DROP TABLE documents__archive":                                                  false,
		"DROP SCHEMA reporting CASCADE":                                                  false,
		// what LokaDB stands on
		"CREATE SERVER lokadb_org7 FOREIGN DATA WRAPPER postgres_fdw": true,
		"ALTER SERVER lokadb_org1 OPTIONS (SET host 'evil')":          true,
		"DROP SERVER lokadb_org1 CASCADE":                             true,
		"DROP USER MAPPING FOR app SERVER lokadb_org1":                true,
		"ALTER FOREIGN DATA WRAPPER postgres_fdw OPTIONS (ADD x 'y')": true,
		"DROP EXTENSION postgres_fdw CASCADE":                         true,
		"DROP SCHEMA lokadb CASCADE":                                  true,
		"ALTER SCHEMA lokadb RENAME TO old":                           true,
		"ALTER SERVER lokadb_org1 RENAME TO x":                        true,
		"DROP TABLE documents__cloud":                                 true,
		"DROP TABLE public.documents__refs, other":                    true,
		"DROP FOREIGN TABLE documents__org1":                          true,
		"DROP TABLE lokadb.orgs":                                      true,
		// review 2 of routing: the policies that keep the organizations apart, and those of
		// partitions; the application's own policies of the table are its own (TestPolicyStmt)
		"DROP POLICY lokadb_tenant ON documents":                    true,
		"DROP POLICY IF EXISTS lokadb_tenant ON public.documents":   true,
		"ALTER POLICY lokadb_tenant ON documents USING (true)":      true,
		"ALTER POLICY lokadb_owner ON documents TO PUBLIC":          true,
		"ALTER POLICY lokadb_tenant ON documents RENAME TO old":     true,
		"ALTER POLICY mine ON documents RENAME TO lokadb_tenant":    true,
		"CREATE POLICY lokadb_x ON documents USING (true)":          true,
		"CREATE POLICY mine ON documents__cloud USING (true)":       true,
		"DROP POLICY mine ON documents__cloud":                      true,
		"ALTER TABLE documents__cloud DISABLE ROW LEVEL SECURITY":   true,
		"CREATE POLICY mine ON documents USING (title <> 'hidden')": false,
		"DROP POLICY mine ON documents":                             false,
		"ALTER POLICY mine ON documents USING (title <> 'x')":       false,
		"CREATE POLICY lokadb_x ON projects USING (true)":           false,
		// the 2nd review of routing: a partition named in a body the parser does not read
		"DO $$ BEGIN PERFORM count(*) FROM documents__org2; END $$":                                                     true,
		"DO $x$ BEGIN EXECUTE 'SELECT 1 FROM public.\"documents__refs\"'; END $x$":                                      true,
		"CREATE FUNCTION leak() RETURNS SETOF record LANGUAGE sql AS 'SELECT * FROM documents__org2'":                   true,
		"CREATE OR REPLACE PROCEDURE p() LANGUAGE plpgsql AS $$ BEGIN DELETE FROM Documents__O2; END $$":                true,
		"CREATE FUNCTION f() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT count(*) FROM documents__cloud; END":       true,
		"DO $$ BEGIN PERFORM count(*) FROM documents; END $$":                                                           false,
		"CREATE FUNCTION g() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM documents__archive'":                  false,
		"CREATE FUNCTION h() RETURNS bigint LANGUAGE sql AS 'SELECT count(*) FROM documents WHERE organization_id = 1'": false,
		// block 7: the organizations' credentials of the DDL replay
		"SELECT * FROM lokadb.replay_targets()": true,
		"UPDATE documents SET organization_id = lokadb.same_org($1, documents.organization_id) WHERE id = $2": false,
		"SELECT x FROM (SELECT lokadb.org_lock(1) AS x) s":                                                    true,
		"SELECT umoptions FROM pg_user_mappings":                                                              true,
		"SELECT srvoptions FROM pg_catalog.pg_foreign_server":                                                 true,
		"SELECT * FROM information_schema.user_mapping_options":                                               true,
		"SELECT lower(title), public.my_func(1) FROM documents WHERE id = 1":                                  false,
		// the proxy's own objects of COPY FROM STDIN (block 2)
		`SELECT * FROM pg_temp."__lokadb_copy_documents"`:                                          true,
		`SELECT * FROM "__lokadb_copy_documents"`:                                                  true,
		`INSERT INTO pg_temp."__lokadb_copy_documents" (id) VALUES (1)`:                            true,
		`SELECT pg_temp."__lokadb_copy_documents"()`:                                               true,
		`DROP TABLE pg_temp."__lokadb_copy_documents"`:                                             true,
		`DROP FUNCTION "__lokadb_copy_documents"()`:                                                true,
		`CREATE TRIGGER x BEFORE INSERT ON pg_temp."__lokadb_copy_documents" EXECUTE FUNCTION f()`: true,
		`CREATE TRIGGER "__lokadb_copy" BEFORE INSERT ON other EXECUTE FUNCTION f()`:               true,
		`CREATE TEMP TABLE my_temp (id int)`:                                                       false,
		`COPY documents (id, organization_id) FROM STDIN`:                                          false,
	}
	for sql, want := range cases {
		if got := analyze(sql, m).Forbidden != ""; got != want {
			t.Errorf("%s: forbidden %v, want %v", sql, got, want)
		}
	}
	if analyze("SELECT * FROM documents__org1", nil).Forbidden != "" {
		t.Error("a database without LokaDB tables has nothing to protect")
	}
}

func TestUnsafeDDL(t *testing.T) {
	m := testMeta()
	cases := map[string]bool{
		"ALTER TABLE documents ADD COLUMN status varchar(20) DEFAULT 'draft' NOT NULL":        false,
		"ALTER TABLE documents ADD COLUMN created_at timestamptz DEFAULT now()":               false,
		"ALTER TABLE documents ADD COLUMN created_at timestamptz DEFAULT CURRENT_TIMESTAMP":   false,
		"ALTER TABLE documents ADD COLUMN uid uuid DEFAULT pg_catalog.gen_random_uuid()":      false,
		"CREATE INDEX documents_title_lower ON documents (lower(title))":                      false,
		"ALTER TABLE documents ADD COLUMN x text DEFAULT pg_read_file('/etc/passwd')":         true,
		"ALTER TABLE documents ALTER COLUMN title TYPE text USING dblink_exec('x', 'y')":      true,
		"ALTER TABLE documents ADD COLUMN n int DEFAULT (SELECT count(*) FROM pg_authid)":     true,
		"CREATE INDEX ON documents (title) WHERE title <> (SELECT max(title) FROM documents)": true,
		"ALTER TABLE documents ADD COLUMN y text DEFAULT evil.now()":                          true,
	}
	for sql, want := range cases {
		a := analyze(sql, m)
		if got := a.DDLError != ""; got != want {
			t.Errorf("%s: refused %v (%s), want %v", sql, got, a.DDLError, want)
		}
		if len(a.DDLTables) != 1 || a.DDLTables[0] != "documents" {
			t.Errorf("%s: DDL tables %v", sql, a.DDLTables)
		}
	}
}

func TestRedact(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO documents (organization_id, title, body) VALUES (1, 'Q3 board minutes', 'secret body')",
		"UPDATE documents SET body = 'secret body' WHERE id = 5",
	} {
		got := brief(sql)
		if strings.Contains(got, "secret") || strings.Contains(got, "board") {
			t.Errorf("literal left in the log line: %s", got)
		}
	}
	if got := brief("SELEC garbage 'secret'"); strings.Contains(got, "secret") {
		t.Errorf("unparsed SQL logged: %s", got)
	}
}

func TestOwnRelation(t *testing.T) {
	m := testMeta()
	cases := map[string]string{
		"documents__cloud": "documents", "documents__refs": "documents", "documents__o12": "documents", "documents__none": "documents",
		"documents__org3": "documents", "documents__o3_source": "documents", "documents__id_seq": "documents",
		"documents__project_id_seq": "documents", // an identity column's sequence
		"document_versions__cloud":  "document_versions",
		"documents__archive":        "", "documents__orgs": "", "documents__nope_seq": "", "documents": "", "documents_cloud": "",
	}
	for rel, want := range cases {
		if got := m.ownRelation(rel); got != want {
			t.Errorf("%s: %q, want %q", rel, got, want)
		}
	}
}

// lokadb.fanout is the proxy's: the application cannot set it in any statement (block 4, item 8).
func TestSetsFanout(t *testing.T) {
	m := testMeta()
	cases := map[string]bool{
		"SET lokadb.fanout = 'log'":                                              true,
		"SET LOCAL lokadb.fanout TO allow":                                       true,
		"RESET lokadb.fanout":                                                    true,
		"SELECT set_config('lokadb.fanout', 'log', false)":                       true,
		"SELECT pg_catalog.set_config('LOKADB.FANOUT'::text, 'log', true)":       true,
		"SELECT set_config($1, $2, false)":                                       true,
		"UPDATE pg_settings SET setting = 'log' WHERE name = 'lokadb.fanout'":    true,
		"DO $$ BEGIN PERFORM set_config('lokadb.fanout', 'log', false); END $$":  true,
		"BEGIN; SET LOCAL lokadb.fanout = 'log'; DELETE FROM users WHERE id = 1": true,
		"SET search_path = public":                                               false,
		"SELECT set_config('app.user', 'x', false)":                              false,
		"RESET ALL":                      false,
		"DELETE FROM users WHERE id = 1": false,
	}
	for sql, want := range cases {
		a := analyze(sql, m)
		if got := a.Forbidden == fanoutSetMsg; got != want {
			t.Errorf("%s: refused %v (%q), want %v", sql, got, a.Forbidden, want)
		}
	}
	// check reads pg_stat_statements, where constants are parameters: a set_config whose
	// name is a parameter is refused by the proxy, but marked so that check does not count it
	for sql, want := range map[string]bool{
		"SELECT set_config($1, $2, false)":                                             true,
		"SELECT set_config($2, $1, $3)":                                                true,
		"SELECT set_config('lokadb.fanout', $1, false)":                                false,
		"SELECT set_config($1, 'x', false), set_config('lokadb.fanout', 'log', false)": false,
	} {
		if a := analyze(sql, m); a.Forbidden != fanoutSetMsg || a.FanoutByParam != want {
			t.Errorf("%s: refused %q, by a parameter %v, want %v", sql, a.Forbidden, a.FanoutByParam, want)
		}
	}
	if a := analyze("SET lokadb.fanout = 'log'", nil); a.rejection() != fanoutSetMsg {
		t.Errorf("a database not enabled: %q", a.rejection())
	}
}
