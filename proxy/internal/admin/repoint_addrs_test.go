package admin

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// withAddress keeps a stored DSN's role, password and settings and moves it to the new host,
// port and database: keyword=value (quoted values, a password with a quote) and URL forms.
func TestWithAddress(t *testing.T) {
	for _, dsn := range []string{
		"host=old port=5432 dbname=cust user=lokadb_dml password='p w\\'q' sslmode=disable application_name=x",
		"postgres://lokadb_dml:p%20w%27q@old:5432/cust?sslmode=disable&application_name=x",
	} {
		got, err := withAddress(dsn, "new-host", 15001, "cust copy")
		if err != nil {
			t.Fatalf("%s: %v", dsn, err)
		}
		c, err := pgconn.ParseConfig(got)
		if err != nil {
			t.Fatalf("%s -> %s: %v", dsn, got, err)
		}
		if c.Host != "new-host" || c.Port != 15001 || c.Database != "cust copy" || c.User != "lokadb_dml" || c.Password != "p w'q" || c.RuntimeParams["application_name"] != "x" {
			t.Errorf("%s -> %s: host %q port %d db %q user %q password %q params %v", dsn, got, c.Host, c.Port, c.Database, c.User, c.Password, c.RuntimeParams)
		}
	}
}

// The mark of an organization's table, with the database id or without (dbid.go).
func TestOrgMark(t *testing.T) {
	id := newDBID()
	if len(id) != 16 || id == newDBID() {
		t.Fatalf("database id %q", id)
	}
	for m, want := range map[string]bool{
		orgMark(7, ""): true, orgMark(7, id): true, orgMark(17, id): false, orgMark(7, "") + "x": false, "returned to the cloud by lokadb": false,
	} {
		if isOrgMark(m, 7) != want {
			t.Errorf("isOrgMark(%q, 7) = %v", m, !want)
		}
	}
	ddl := withDBID([]string{`CREATE TABLE "documents__o7" (id bigint)`, `COMMENT ON TABLE "documents__o7" IS 'lokadb: organization 7'`}, 7, id)
	if ddl[1] != `COMMENT ON TABLE "documents__o7" IS 'lokadb: organization 7 database `+id+`'` || ddl[0] != `CREATE TABLE "documents__o7" (id bigint)` {
		t.Errorf("withDBID: %q", ddl)
	}
}

// A DSN with hostaddr or service would stay on the old address whatever host says: refused.
func TestWithAddressHostaddr(t *testing.T) {
	for _, dsn := range []string{"hostaddr=10.0.0.5 dbname=cust user=u", "service=cust user=u", "postgres://u@old/cust?hostaddr=10.0.0.5"} {
		if _, err := withAddress(dsn, "new", 5432, "cust"); err == nil || !strings.Contains(err.Error(), "-direct") {
			t.Errorf("%s: %v", dsn, err)
		}
	}
	if _, err := withAddress("host=old dbname=cust user=u application_name=myservice", "new", 5432, "cust"); err != nil {
		t.Errorf("a value that only contains the word: %v", err)
	}
}
