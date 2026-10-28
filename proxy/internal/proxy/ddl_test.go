package proxy

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestOptionsDSN(t *testing.T) {
	dsn := OptionsDSN(
		[]string{"host=relay", "port=15001", "dbname=cust db", "async_capable=true", "connect_timeout=3", "fetch_size=1000"},
		[]string{"user=lokadb_app", `password=p'w\x =y`})
	c, err := pgconn.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if c.Host != "relay" || c.Port != 15001 || c.Database != "cust db" || c.User != "lokadb_app" || c.Password != `p'w\x =y` {
		t.Errorf("parsed %s:%d/%s as %s with password %q", c.Host, c.Port, c.Database, c.User, c.Password)
	}
	if len(c.RuntimeParams) != 0 {
		t.Errorf("postgres_fdw's own options reach the server as settings: %v", c.RuntimeParams)
	}
}

func TestDDLReplayed(t *testing.T) {
	m := testMeta()
	cases := map[string]bool{ // replayed (true) or refused (false)
		"ALTER TABLE documents RENAME COLUMN title TO name":                     true,
		"ALTER TABLE documents RENAME title TO name":                            true,
		"CREATE INDEX CONCURRENTLY documents_slug ON documents (slug)":          true,
		"ALTER TABLE documents RENAME COLUMN organization_id TO org":            false,
		"ALTER TABLE documents RENAME COLUMN id TO key":                         false,
		"ALTER TABLE document_versions RENAME COLUMN document_id TO doc":        false,
		"ALTER TABLE documents RENAME TO docs":                                  false,
		"ALTER TABLE documents RENAME CONSTRAINT documents_pkey TO documents_k": false,
	}
	for sql, want := range cases {
		a := analyze(sql, m)
		if got := a.DDLError == "" && len(a.DDL) == 1; got != want {
			t.Errorf("%s: replayed %v, want %v (%s)", sql, got, want, a.DDLError)
		}
	}
	if st, tb := concurrentIndex(analyze("CREATE INDEX CONCURRENTLY documents_slug ON documents (slug)", m), m, defaultPath); st == nil || tb.Name != "documents" {
		t.Error("CREATE INDEX CONCURRENTLY on a LokaDB table is not recognized")
	}
	if st, _ := concurrentIndex(analyze("CREATE INDEX documents_slug ON documents (slug)", m), m, defaultPath); st != nil {
		t.Error("a plain CREATE INDEX taken for a concurrent one")
	}
	st, _ := concurrentIndex(analyze("CREATE INDEX CONCURRENTLY documents_slug ON documents (slug)", m), m, defaultPath)
	if q, err := indexSQL(st, "documents__cloud", partIndex("documents_slug", "documents__cloud"), true, true); err != nil || q != `CREATE INDEX CONCURRENTLY IF NOT EXISTS documents_slug__documents__cloud ON public.documents__cloud USING btree (slug)` {
		t.Errorf("partition index: %s %v", q, err)
	}
}

// metaBackoff doubles from metaRetry and stops at metaTTL.
func TestMetaBackoff(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		if got := metaBackoff(i + 1); got != w {
			t.Errorf("failure %d: %s, want %s", i+1, got, w)
		}
	}
	if got := metaBackoff(1000); got != metaTTL {
		t.Errorf("failure 1000: %s, want %s", got, metaTTL)
	}
}

// Review R2: an organization whose tables are in schema lokadb (the customer has a table of
// the same name in public) gets public.<LokaDB table> as lokadb.<table>; types, functions and
// tables that are not LokaDB keep public; unqualified names are left to the search path.
func TestOrgDDL(t *testing.T) {
	m := testMeta()
	m.Indexes = map[string]string{"documents_title_idx": "documents"}
	cases := []struct{ in, want string }{
		{"ALTER TABLE public.documents DROP COLUMN title", "ALTER TABLE lokadb.documents DROP title"},
		{`ALTER TABLE "public"."documents" ADD COLUMN status public.doc_status DEFAULT public.next_status()`,
			"ALTER TABLE lokadb.documents ADD COLUMN status public.doc_status DEFAULT public.next_status()"},
		{"ALTER TABLE ONLY public.documents ALTER COLUMN title TYPE text USING public.documents.title::text",
			"ALTER TABLE ONLY lokadb.documents ALTER COLUMN title TYPE text USING lokadb.documents.title::text"},
		{"CREATE INDEX documents_slug_idx ON public.documents USING btree (slug)", "CREATE INDEX documents_slug_idx ON lokadb.documents USING btree (slug)"},
		{"CREATE INDEX CONCURRENTLY IF NOT EXISTS d_t ON public.documents (title)", "CREATE INDEX CONCURRENTLY IF NOT EXISTS d_t ON lokadb.documents USING btree (title)"},
		{"ALTER TABLE public.documents RENAME COLUMN title TO name", "ALTER TABLE lokadb.documents RENAME COLUMN title TO name"},
		{"ALTER INDEX IF EXISTS public.documents_title_idx RENAME TO documents_name_idx", "ALTER INDEX IF EXISTS lokadb.documents_title_idx RENAME TO documents_name_idx"},
		// left alone
		{"ALTER TABLE documents ADD COLUMN x int", "ALTER TABLE documents ADD COLUMN x int"},
		{"ALTER TABLE public.projects ADD COLUMN x int", "ALTER TABLE public.projects ADD COLUMN x int"},
		{"ALTER TABLE app.documents ADD COLUMN x int", "ALTER TABLE app.documents ADD COLUMN x int"},
	}
	for _, c := range cases {
		got, err := orgDDL(c.in, "lokadb", m)
		if err != nil || got != c.want {
			t.Errorf("%s:\n got %q (%v)\nwant %q", c.in, got, err, c.want)
		}
		if same, _ := orgDDL(c.in, "public", m); same != c.in {
			t.Errorf("%s in public: %q", c.in, same)
		}
	}
}

// Extensions after a move: the cloud's CREATE, ALTER … UPDATE and DROP EXTENSION are replayed in
// the organizations' databases (orgExtension), the cloud's own extensions are not.
func TestOrgExtension(t *testing.T) {
	m := testMeta()
	cases := []struct{ in, sch, want string }{
		{"CREATE EXTENSION vector", "public", "CREATE EXTENSION IF NOT EXISTS vector"},
		{"CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public", "lokadb", "CREATE EXTENSION IF NOT EXISTS vector SCHEMA lokadb"},
		{"create extension postgis with schema extensions cascade", "lokadb", "CREATE EXTENSION IF NOT EXISTS postgis SCHEMA extensions CASCADE"},
		{`CREATE EXTENSION "uuid-ossp" VERSION '1.1'`, "public", `CREATE EXTENSION IF NOT EXISTS "uuid-ossp" VERSION "1.1"`},
		{"ALTER EXTENSION vector UPDATE", "lokadb", "ALTER EXTENSION vector UPDATE"},
		{"ALTER EXTENSION vector UPDATE TO '0.8.0'", "public", "ALTER EXTENSION vector UPDATE TO \"0.8.0\""},
		{"DROP EXTENSION vector", "public", "DROP EXTENSION IF EXISTS vector"},
		{"DROP EXTENSION IF EXISTS vector, postgis CASCADE", "lokadb", "DROP EXTENSION IF EXISTS vector, postgis"},
	}
	for _, c := range cases {
		got, err := orgDDL(c.in, c.sch, m)
		if err != nil || got != c.want {
			t.Errorf("%s (schema %s):\n got %q (%v)\nwant %q", c.in, c.sch, got, err, c.want)
		}
	}
	for q, want := range map[string]string{
		"CREATE EXTENSION vector": "create", "ALTER EXTENSION vector UPDATE": "alter", "DROP EXTENSION vector": "drop",
		"ALTER EXTENSION vector SET SCHEMA x": "", "ALTER TABLE documents ADD COLUMN extension int": "", "DROP TABLE x": "",
	} {
		if op, _ := extStatement(q); op != want {
			t.Errorf("extStatement(%q) = %q, want %q", q, op, want)
		}
	}
	// the analysis replays them, except the cloud's own
	for q, want := range map[string]int{
		"CREATE EXTENSION vector": 1, "ALTER EXTENSION vector UPDATE": 1, "DROP EXTENSION vector": 1,
		"CREATE EXTENSION postgres_fdw": 0, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements": 0,
		"ALTER EXTENSION vector SET SCHEMA x": 0,
	} {
		if a := Analyze(q, m); len(a.DDL) != want || a.DDLError != "" {
			t.Errorf("%s: DDL %v, error %q, want %d statements", q, a.DDL, a.DDLError, want)
		}
	}
}
