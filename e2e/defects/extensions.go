package main

// Extensions after a move (block 7, the user's request of 28.10): CREATE EXTENSION, ALTER
// EXTENSION … UPDATE and DROP EXTENSION that the application runs in the cloud reach the
// databases of the moved organizations, as migrations do. What the organization's database
// cannot take stops the migration in the cloud too, with the statement its administrator runs.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "M16", name: "CREATE/ALTER/DROP EXTENSION in the cloud reach a moved organization: pgvector and a vector column by a migration; PostGIS the organization's server lacks, a role without CREATE on the database and a DROP the organization's objects hold stop the migration in the cloud too, with the administrator's statement", run: checkExtensionsAfterMove},
	)
}

// M16. On main nothing replayed an extension statement: the cloud got pgvector, the
// organization did not, and the next migration adding a vector column then failed at the
// organization after the cloud had run it.
func checkExtensionsAfterMove(e *env) (bool, string, error) {
	// the application's role may create trusted extensions in the cloud (CREATE on the database)
	if err := execSQL(e.ctx, e.cloud(), "GRANT CREATE ON DATABASE "+e.db+" TO app"); err != nil {
		return false, "", err
	}
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
	run := func(q string) error { _, err := simple(e.ctx, c, q); return err }
	has := func(d, ext string) string {
		v, err := one(e.ctx, d, "SELECT count(*) FROM pg_extension WHERE extname = '"+ext+"'")
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	state := func(ext string) string {
		return "cloud " + has(e.cloud(), ext) + ", organization " + has(e.customer(), ext)
	}
	var notes []string
	ok := true
	step := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	mentions := func(err error, parts ...string) bool {
		if err == nil {
			return false
		}
		for _, p := range parts {
			if !strings.Contains(err.Error(), p) {
				return false
			}
		}
		return true
	}

	// 1. the organization's role may not create an extension there: the migration stops, in
	// the cloud too, and the error is the administrator's statement
	err1 := run("CREATE EXTENSION vector")
	step(mentions(err1, "organization 1", "CREATE EXTENSION IF NOT EXISTS", "vector", "repeat the migration") && state("vector") == "cloud 0, organization 0",
		fmt.Sprintf("CREATE EXTENSION vector without CREATE on the organization's database: %s (%s)", errText(err1), state("vector")))

	// 2. the administrator grants it; the same migration passes, in both
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON DATABASE "+e.db+" TO lokadb_app"); err != nil {
		return false, "", err
	}
	err2 := run("CREATE EXTENSION vector")
	step(err2 == nil && state("vector") == "cloud 1, organization 1", fmt.Sprintf("the same after the grant: %s (%s)", errText(err2), state("vector")))

	// 3. a vector column by a migration, written through the proxy for the moved organization
	err3 := run("ALTER TABLE documents ADD COLUMN embedding vector(3)")
	col, _ := one(e.ctx, e.customer(), "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'documents'::regclass AND attname = 'embedding'")
	err3b := run("INSERT INTO documents (organization_id, user_id, title, embedding) VALUES (1, 2, 'vec-1', '[1,2,3]')")
	row, _ := one(e.ctx, e.customer(), "SELECT embedding::text FROM documents WHERE title = 'vec-1'")
	res, err3c := simple(e.ctx, c, "SELECT embedding::text FROM documents WHERE organization_id = 1 AND title = 'vec-1'")
	got := "-"
	if err3c == nil && len(res) > 0 && len(res[0].Rows) > 0 {
		got = string(res[0].Rows[0][0])
	}
	step(err3 == nil && col == "vector(3)" && err3b == nil && row == "[1,2,3]" && got == "[1,2,3]",
		fmt.Sprintf("ADD COLUMN embedding vector(3): %s, the organization's column %s; INSERT: %s, the row there %s, read through the proxy %s", errText(err3), col, errText(err3b), row, got))

	// 4. an extension the organization's server has no package for
	err4 := run("CREATE EXTENSION postgis")
	step(mentions(err4, "organization 1", "is not available", "package", `CREATE EXTENSION IF NOT EXISTS "postgis"`) && state("postgis") == "cloud 0, organization 0",
		fmt.Sprintf("CREATE EXTENSION postgis: %s (%s)", errText(err4), state("postgis")))

	// 5. an update goes to both (already the newest: no error)
	err5 := run("ALTER EXTENSION vector UPDATE")
	step(err5 == nil, "ALTER EXTENSION vector UPDATE: "+errText(err5))

	// 6. DROP EXTENSION: the cloud's own objects hold it first, then the organization's
	err6a := run("DROP EXTENSION vector")
	step(err6a != nil && state("vector") == "cloud 1, organization 1", fmt.Sprintf("DROP EXTENSION vector while the column is there: %s (%s)", errText(err6a), state("vector")))
	if err := run("ALTER TABLE documents DROP COLUMN embedding"); err != nil {
		step(false, "DROP COLUMN embedding: "+errText(err))
		return false, strings.Join(notes, "; "), nil
	}
	if err := execSQL(e.ctx, e.customer(), "ALTER TABLE documents ADD COLUMN stray vector(3)"); err != nil { // something of the organization's own
		step(false, "the organization's own vector column: "+errText(err))
		return false, strings.Join(notes, "; "), nil
	}
	err6b := run("DROP EXTENSION vector")
	step(mentions(err6b, "organization 1", "still used", "the cloud did not drop it either") && state("vector") == "cloud 1, organization 1",
		fmt.Sprintf("DROP EXTENSION vector, the cloud has no use for it, the organization's column does: %s (%s)", errText(err6b), state("vector")))
	if err := execSQL(e.ctx, e.customer(), "ALTER TABLE documents DROP COLUMN stray"); err != nil {
		step(false, "dropping the organization's own column: "+errText(err))
		return false, strings.Join(notes, "; "), nil
	}
	err6c := run("DROP EXTENSION vector")
	step(err6c == nil && state("vector") == "cloud 0, organization 0", fmt.Sprintf("DROP EXTENSION vector when nothing uses it: %s (%s)", errText(err6c), state("vector")))
	return ok, strings.Join(notes, "; "), nil
}
