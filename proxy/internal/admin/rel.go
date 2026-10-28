package admin

import (
	"fmt"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// A table is named in lokadb's metadata (see package rel) (lokadb.tables, lookups, routing, the spec) by its key:
// the bare name for a table of schema public, as before schemas other than public were
// supported, and schema.name for any other schema. The schema part is in double quotes when
// it holds a dot or a quote ("a.b".t); the name part is the name as it is, so that a helper
// table's key is the table's key with a suffix: app.documents__cloud is in schema app.

// SplitKey: the schema ("public" for a bare key) and the name of the table a key names.
func SplitKey(k string) (schema, name string) { return rel.SplitKey(k) }

// RelKey: the key of table name of schema schema.
func RelKey(schema, name string) string { return rel.RelKey(schema, name) }

// qr: how SQL names the table a key names. A table of schema public goes unqualified, as
// before, whatever its name (the search path of lokadb's connections has public, and the
// organization's schema at the organization's database); one of another schema is qualified,
// in the cloud and at the organization alike (move-org makes the same schema there).
func qr(k string) string {
	s, n := SplitKey(k)
	if n == k || s == "public" {
		// public's tables stay unqualified, a name with a dot too ("acct.docs", key
		// public.acct.docs): at an organization the move's unqualified names resolve to its
		// schema (setCustomerSchema), a qualified one to public, where the table is not
		// (review of block 6: move-org of such a table into schema lokadb failed at its index)
		return q(n)
	}
	return q(s) + "." + q(n)
}

// nameOf: the name of the table a key names, without its schema (RENAME TO, an index's name).
func nameOf(k string) string { _, n := SplitKey(k); return n }

// schemaOf: the schema of the table a key names.
func schemaOf(k string) string { s, _ := SplitKey(k); return s }

// inSchemaOf: the key of relation name in the schema of the table k names.
func inSchemaOf(k, name string) string { return RelKey(schemaOf(k), name) }

// relSQL is lokadb.rel(e) inline, for queries that may run before this version's baseSQL
// (move-cleanup, check against a database enabled by an older version).
func relSQL(e string) string { return rel.QuotedSQL(e) }

// keySQL is the same in SQL: lokadb.key_parts splits a key, lokadb.rel names its table,
// lokadb.qrel too, always with its schema (functions with search_path = pg_catalog),
// lokadb.key_of is the key of a table (RelKey).
var keySQL = `
CREATE OR REPLACE FUNCTION lokadb.key_parts(k text, OUT nsp text, OUT name text) LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN m IS NOT NULL THEN replace(m[1], '""', '"') WHEN strpos(k, '.') > 0 THEN split_part(k, '.', 1) ELSE 'public' END,
         CASE WHEN m IS NOT NULL THEN m[2] WHEN strpos(k, '.') > 0 THEN substr(k, strpos(k, '.') + 1) ELSE k END
  FROM (SELECT regexp_match(k, '^"((?:[^"]|"")*)"\.(.*)$', 's') AS m) x $$;
CREATE OR REPLACE FUNCTION lokadb.rel(k text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN strpos(k, '.') = 0 THEN quote_ident(k) ELSE quote_ident(nsp) || '.' || quote_ident(name) END
  FROM lokadb.key_parts(k) $$;
CREATE OR REPLACE FUNCTION lokadb.qrel(k text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT pg_catalog.quote_ident(nsp) || '.' || pg_catalog.quote_ident(name) FROM lokadb.key_parts(k) $$;
CREATE OR REPLACE FUNCTION lokadb.helper_name(name text, suffix text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT ` + rel.HelperNameSQL("name", "suffix") + ` $$;
CREATE OR REPLACE FUNCTION lokadb.helper(k text, suffix text) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT ` + rel.HelperSQL("k", "suffix") + ` $$;
CREATE OR REPLACE FUNCTION lokadb.key_of(rel oid) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN n.nspname = 'public' AND strpos(c.relname, '.') = 0 AND left(c.relname, 1) <> '"' THEN c.relname::text
    ELSE CASE WHEN n.nspname ~ '[."]' THEN '"' || replace(n.nspname, '"', '""') || '"' ELSE n.nspname::text END || '.' || c.relname END
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = rel $$;
`

// QR, RelSQL, CustSchemaOf, NameOf: qr, relSQL, custSchemaOf and nameOf for lokadb migrate.
func QR(k string) string                      { return qr(k) }
func RelSQL(e string) string                  { return relSQL(e) }
func CustSchemaOf(t, orgSchema string) string { return custSchemaOf(t, orgSchema) }
func NameOf(k string) string                  { return nameOf(k) }

// hn: the key of the helper suffix of the table of key k (rel.Helper: a name too long for
// Postgres is shortened, never cut).
func hn(k, suffix string) string { return rel.Helper(k, suffix) }

// Helper is hn for check and migrate.
func Helper(k, suffix string) string { return rel.Helper(k, suffix) }

// doTag: the DO block do ("DO $$ … $$") with a dollar quote its body does not hold. Names in
// the body come from the spec and the catalog: one with $$ in it closed the body early, and
// what followed ran as SQL (review 3 of admin, R3-14).
func doTag(do string) string {
	body := strings.TrimSuffix(strings.TrimPrefix(do, "DO $$"), "$$")
	tag := "$lokadb$"
	for i := 0; strings.Contains(body, tag); i++ {
		tag = fmt.Sprintf("$lokadb%d$", i)
	}
	return "DO " + tag + body + tag
}
