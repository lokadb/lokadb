package check

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

func TestProfileTransactions(t *testing.T) {
	c := testChecker()
	c.meta.Tables["documents"].KeyDefault = "nextval('documents__id_seq'::regclass)" // as enable leaves an identity
	tx := func(n int64, stmts ...string) *proxy.RecordedTx {
		return &proxy.RecordedTx{Statements: stmts, Count: n}
	}
	c.txs = []*proxy.RecordedTx{
		tx(10, `select * from documents where id = $1`),                                                      // direct
		tx(5, `insert into documents (organization_id, slug) values ($1, $2)`, `update projects set n = $1`), // direct, then the cloud: two connections
		tx(3, `select * from projects where id = $1`, `select * from documents where id = $1`),               // the cloud first: FDW after it
		tx(2, `update documents set title = $1 where id = $2`,
			`select * from documents d join projects p on p.id = d.project_id where d.id = $1`), // direct, then a JOIN with the cloud
		tx(4, `SET LOCAL lokadb.tenant = '<organization>'`, `select * from documents where slug = $1`), // named: direct without a condition
		tx(7, `select * from projects`), // no LokaDB table: not counted
		tx(6, `select * from documents where id = $1`, `insert into documents (organization_id, slug) select organization_id, slug from documents where id = $1`), // direct, then an INSERT … SELECT without its key: refused once (8a.2)
	}
	pr := &ProfileReport{}
	named := c.profileTransactions(pr)
	want := &ProfileTx{Shapes: 6, Count: 30, Direct: 2, DirectCount: 14, AfterCloud: 1, TwoConnections: 1, FDWAfterDirect: 2, Named: 1}
	if !reflect.DeepEqual(pr.Transactions, want) {
		t.Errorf("got %+v, want %+v", pr.Transactions, want)
	}
	if len(named) != 1 {
		t.Errorf("named fingerprints: %v", named)
	}
	// the forms the proxy decides by the first statement: direct whole (two connections too),
	// or direct first and then the cloud (the INSERT … SELECT's transaction shares its first statement
	// with a direct one: the cloud's way wins)
	got := map[string]bool{}
	for _, f := range pr.Forms {
		got[fmt.Sprintf("%s named=%v", f.Example, f.Named)] = f.Direct
	}
	wantForms := map[string]bool{
		`select * from documents where id = $1 named=false`:                         false,
		`insert into documents (organization_id, slug) values ($1, $2) named=false`: true,
		`update documents set title = $1 where id = $2 named=false`:                 false,
		`select * from documents where slug = $1 named=true`:                        true,
	}
	if !reflect.DeepEqual(got, wantForms) {
		t.Errorf("forms: got %v, want %v", got, wantForms)
	}
	sql := DirectFormsSQL(pr.Forms)
	for _, want := range []string{"INSERT INTO lokadb.direct_learnt (fingerprint, named) VALUES ('", "', true) ON CONFLICT DO NOTHING; -- select * from documents where slug = $1",
		"INSERT INTO lokadb.direct_cloud_first (fingerprint, reason) VALUES ('", "COMMIT;\nNOTIFY lokadb_meta;\n"} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL lacks %q:\n%s", want, sql)
		}
	}
}

func TestTenantSet(t *testing.T) {
	for sql, want := range map[string]bool{
		`SET LOCAL lokadb.tenant = $1`:                         true,
		`set lokadb.tenant = '42'`:                             true,
		`SELECT set_config('lokadb.tenant', $1, true)`:         true,
		`SET LOCAL lokadb.org = 1`:                             false,
		`RESET lokadb.tenant`:                                  false,
		`select * from documents where slug = 'lokadb.tenant'`: false,
	} {
		if got := tenantSet(analyzable(sql)); got != want {
			t.Errorf("%s: got %v", sql, got)
		}
	}
}

// The estimate of fdw+direct: the share, and each reason with its complete patterns.
func TestProfileShare(t *testing.T) {
	c := testChecker()
	c.o = Options{Profile: "fdw+direct"}
	c.rep.Sources = []string{"recording"}
	c.pats = map[string]int64{
		`select * from documents where id = $1`:                                            30,
		`select * from document_versions where document_id = $1`:                           10,
		`select * from documents d join projects p on p.id = d.project_id where d.id = $1`: 5,
		`select * from documents where slug = $1`:                                          2,
		`select * from projects where id = $1`:                                             50, // cloud: not counted
	}
	if err := c.profile(); err != nil {
		t.Fatal(err)
	}
	pr := c.rep.Profile
	if pr.Patterns != 4 || pr.PatternsDirect != 2 || pr.Calls != 47 || pr.CallsDirect != 40 {
		t.Errorf("share: %+v", pr)
	}
	reasons := map[string]int64{}
	for _, r := range pr.Reasons {
		reasons[r.Reason] = r.Calls
	}
	if !reflect.DeepEqual(reasons, map[string]int64{proxy.DirectCloudTable: 5, proxy.DirectNoOrg: 2}) {
		t.Errorf("reasons: %v", reasons)
	}
	found := 0
	for _, f := range c.rep.Findings {
		if f.Code == "profile_fdw_reason" {
			found++
			if len(f.Patterns) != 1 || f.Patterns[0].Fingerprint == "" || f.Subject == nil || f.Subject.Kind != "reason" {
				t.Errorf("%s: patterns %+v subject %+v", f.Object, f.Patterns, f.Subject)
			}
		}
	}
	if found != 2 {
		t.Errorf("profile_fdw_reason findings: %d", found)
	}
}

// lokadb.tenant named for the session in the recording: a warning (a pooler in transaction mode).
func TestTenantSessionSets(t *testing.T) {
	for sql, want := range map[string]bool{
		`SET lokadb.tenant = $1`:                        true,
		`SET SESSION lokadb.tenant = '7'`:               true,
		`SELECT set_config('lokadb.tenant', $1, false)`: true,
		`SELECT set_config('lokadb.tenant', $1, $2)`:    true,
		`SET LOCAL lokadb.tenant = $1`:                  false,
		`SELECT set_config('lokadb.tenant', $1, true)`:  false,
		`RESET lokadb.tenant`:                           false,
		`SET lokadb.org = 1`:                            false,
	} {
		if got := tenantSessionSet(analyzable(sql)); got != want {
			t.Errorf("%s: %v", sql, got)
		}
	}
	c := testChecker()
	c.tenantSessionSets([]*proxy.RecordedTx{{Statements: []string{`SET LOCAL lokadb.tenant = $1`, `select 1`}, Count: 3}})
	if hasCode(c.rep.Findings, "tenant_session_set") {
		t.Errorf("SET LOCAL warned: %+v", c.rep.Findings)
	}
	c.tenantSessionSets([]*proxy.RecordedTx{{Statements: []string{`SET lokadb.tenant = $1`, `select 1`}, Count: 3}})
	if !hasCode(c.rep.Findings, "tenant_session_set") {
		t.Errorf("a session SET not warned: %+v", c.rep.Findings)
	}
}

// The statements the application runs only after naming the organization.
func TestNamedOnly(t *testing.T) {
	tx := func(stmts ...string) *proxy.RecordedTx { return &proxy.RecordedTx{Statements: stmts, Count: 1} }
	got := namedOnly([]*proxy.RecordedTx{
		tx(`SELECT set_config('lokadb.tenant', '<organization>', $1)`, `select * from documents where slug = $1`, `select * from projects`),
		tx(`SET LOCAL lokadb.tenant = $1`, `select * from documents where id = $1`), // a recording of before 26.10: not known
		tx(`SET LOCAL lokadb.tenant = '<organization>'`, `select * from documents where title = $1`),
		tx(`select * from documents where title = $1`), // once without the name: not named
		tx(`SET LOCAL lokadb.tenant = '*'`, `select count(*) from documents`),
	})
	want := map[string]bool{`select * from documents where slug = $1`: true, `select * from projects`: true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

// An example with comments of its own (/* opens a nested block comment in Postgres) leaves
// the file whole: every statement parses, COMMIT and NOTIFY included (block 9's review).
func TestDirectFormsSQLComments(t *testing.T) {
	sql := DirectFormsSQL([]DirectForm{{Fingerprint: "ab", Direct: true, Example: "SELECT /* x */ 1 /* y"}, {Fingerprint: "cd", Example: "SELECT 1 -- z */ DROP TABLE t"}})
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 5 {
		t.Fatalf("%v, %d statements:\n%s", err, len(tree.GetStmts()), sql)
	}
}
