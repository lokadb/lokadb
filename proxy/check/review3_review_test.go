package check

// Review 3 of lokadb check (tasks/review3/check.md). Each test states what check should
// report and fails on main b2310e1: that failure is the proof of the finding.
//
// The C*-live tests need a Postgres (postgres:16 is enough) and are skipped without it:
//   LOKADB_REVIEW3_DSN=postgres://postgres:pw@host:5432 go test ./check -run Review3
// They create databases rev3_* there, run check.Run and then admin.Enable on the same schema.

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// C1. The proxy records SET LOCAL lokadb.tenant = '*' normalized, as SET LOCAL lokadb.tenant = $1
// (record.go statementKeys: pg.Normalize), in the same shape as a named organization (the shape
// is keyed by fingerprints). namedOnly and acrossOrgs look for the constant '*' and never find
// it in a real recording: a report run under '*' counts as named, so routed. The proxy judges a
// statement under '*' by the fanout policy (tenant_session.go refuseNamed; default reject:
// refused) for a role outside tenant.all_roles, and refuses it on profile direct.
func TestReview3StarTenantLostInRecording(t *testing.T) {
	// what the proxy's recording writes for it since block 10 kept '*' apart (record_tenant.go):
	// the constant as it is, a fingerprint of its own
	recorded := `SET LOCAL lokadb.tenant = '*'`
	c := testChecker()
	fan, err := proxy.Config{}.FanoutFor("app")
	if err != nil {
		t.Fatal(err)
	}
	c.fan = fan
	report := `select count(*) from documents`
	txs := []*proxy.RecordedTx{{Statements: []string{recorded, report}, Count: 5}}
	k, _ := c.classifyNamed(report, namedOnly(txs))
	if k == kFanout {
		k = c.fanoutKind(report)
	}
	if live := fan.Decide(report, proxy.Analyze(report, c.meta)); live != proxy.FanoutRefused {
		t.Fatalf("the proxy does not refuse it (%v)", live)
	}
	if k != kRefused {
		t.Errorf("a report recorded after %q: check classifies it %v (kRouted=%v: no finding); under '*' the proxy refuses it (fanout policy reject): want query_refused", recorded, k, kRouted)
	}

	// profile direct: the same report is not refused there either
	c = testChecker()
	for _, tb := range c.meta.Tables {
		tb.Refs = ""
	}
	c.o = Options{Profile: "direct", Workload: []string{"recording"}}
	c.rep.Sources = []string{"recording"}
	c.pats = map[string]int64{recorded: 5, report: 5}
	c.txs = txs
	if err := c.profile(); err != nil {
		t.Fatal(err)
	}
	blocked := false
	for _, f := range c.rep.Findings {
		blocked = blocked || f.Severity == Blocker && (f.Code == "profile_direct_all_organizations" || f.Code == "profile_direct_refused_form" || f.Code == "profile_direct_tx_refused")
	}
	if !blocked {
		t.Errorf("profile direct: no blocker for a statement recorded under lokadb.tenant = '*' (findings %s); the proxy refuses it on direct (direct_only.go)", codes(c.rep.Findings))
	}
}

// C2. set_config('lokadb.tenant', '42', true) is recorded as select set_config($1, $2, $3):
// tenantSet looks for the text set_config('lokadb.tenant' and does not see it, so the statements
// after it count as naming no organization: query_refused for what the proxy routes (a false
// blocker), the cooperative mode by set_config invisible to check.
func TestReview3SetConfigTenantInRecording(t *testing.T) {
	// what the recording writes for it (record_tenant.go, block 10): the setting's name kept
	recorded := `SELECT set_config('lokadb.tenant', '<organization>', $1)`
	if !tenantSet(`select set_config('lokadb.tenant', '42', true)`) {
		t.Fatal("tenantSet does not take the literal form either")
	}
	if !tenantSet(analyzable(recorded)) {
		t.Errorf("the recorded form %q is not taken for naming the organization: statements after it are judged as naming none", recorded)
	}
}

// C7. -ci keys a finding without patterns and subject by its code alone. Like R3 of review 2
// (fk_to_cloud), now for profile_direct_tx_refused (a blocker of profile direct whose examples
// are the refused transaction shapes), tenant_session_set, profile_two_connections and
// profile_fdw_after_direct: once the baseline accepts one shape, a new one is never new.
func TestReview3CIKeyOfRefusedTransactions(t *testing.T) {
	run := func(txs []*proxy.RecordedTx) *Report {
		c := testChecker()
		c.meta.Tables["documents"].UserTypes = true
		c.txs = txs
		c.directTxRefused()
		return c.rep
	}
	move := proxy.RecordedTx{Statements: []string{`SET LOCAL lokadb.tenant = '<organization>'`, `update document_versions set organization_id = $1 where id = $2`}, Count: 3}
	read := proxy.RecordedTx{Statements: []string{`SET LOCAL lokadb.tenant = '<organization>'`, `select * from documents where id = $1`}, Count: 10}
	before := run([]*proxy.RecordedTx{&move})
	if len(before.Findings) == 0 {
		t.Fatal("no profile_direct_tx_refused for the first shape: the case of the finding is not met")
	}
	base := before.MakeBaseline(nil)
	after := run([]*proxy.RecordedTx{&move, &read}) // the application's new code: a second refused shape
	if res := after.CompareBaseline(&base, "accepted.json"); len(res.New) == 0 {
		t.Errorf("a new transaction shape refused on profile direct is accepted by the baseline entry %q (examples now %q): -ci exits %d",
			base.Accepted[0].Key, after.Findings[0].Examples, res.ExitCode())
	}
}

func liveDB(t *testing.T, name string) string {
	base := os.Getenv("LOKADB_REVIEW3_DSN")
	if base == "" {
		t.Skip("LOKADB_REVIEW3_DSN not set")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, base+"/postgres")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	if _, err := c.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	_, _ = c.Exec(ctx, "DO $$BEGIN CREATE ROLE app_owner LOGIN; EXCEPTION WHEN duplicate_object THEN END$$")
	_, _ = c.Exec(ctx, "DO $$BEGIN CREATE ROLE eve LOGIN; EXCEPTION WHEN duplicate_object THEN END$$")
	return base + "/" + name
}

func liveExec(t *testing.T, dsn string, sqls ...string) error {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	for _, s := range sqls {
		if _, err := c.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func liveCheck(t *testing.T, dsn string, spec admin.Spec) *Report {
	s := spec.Sorted()
	rep, err := Run(context.Background(), Options{DSN: dsn, Spec: &s, CountLimit: -1})
	if err != nil {
		t.Fatal(err)
	}
	var fs []string
	for _, f := range rep.Findings {
		if f.Severity != Info {
			fs = append(fs, string(f.Severity)+":"+f.Code+"("+f.Object+")")
		}
	}
	t.Logf("check: verdict %s; findings above info: %s", rep.Verdict, strings.Join(fs, ", "))
	return rep
}

func liveEnable(dsn string, spec admin.Spec) error {
	return admin.Enable(context.Background(), dsn, spec.Sorted(), "app_owner", slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// named: a finding above info names s.
func named(rep *Report, s string) bool {
	for _, f := range rep.Findings {
		if f.Severity != Info && strings.Contains(f.Code+" "+f.Object+" "+f.Message+" "+strings.Join(f.Examples, " "), s) {
			return true
		}
	}
	return false
}

func asEve(t *testing.T, dsn, sql string) (int, error) {
	ctx := context.Background()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	tx, err := c.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ROLE eve`); err != nil {
		t.Fatal(err)
	}
	var n int
	err = tx.QueryRow(ctx, sql).Scan(&n)
	return n, err
}

// C3-live. A function over the row type of a LokaDB table (PostgREST and Supabase computed
// columns: f(docs)). enable renames the table to docs__cloud, and the function's argument
// stays of the old row type: after enable SELECT body_len(d) FROM docs d fails. check names
// nothing (enable and check both look at pg_depend of pg_class only, not of pg_type).
func TestReview3LiveRowtypeFunction(t *testing.T) {
	dsn := liveDB(t, "rev3_rowtype")
	if err := liveExec(t, dsn,
		`CREATE TABLE docs (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`CREATE FUNCTION body_len(d docs) RETURNS int LANGUAGE sql STABLE AS 'SELECT length(d.body)'`,
		`INSERT INTO docs VALUES (1, 1, 'hello')`,
		`SELECT body_len(d) FROM docs d`); err != nil {
		t.Fatal(err)
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "docs", TenantColumn: "org_id"}}}
	rep := liveCheck(t, dsn, spec)
	if err := liveEnable(dsn, spec); err != nil {
		t.Fatalf("enable: %v", err)
	}
	err := liveExec(t, dsn, `SELECT body_len(d) FROM docs d`)
	t.Logf("after enable: SELECT body_len(d) FROM docs d: %v", err)
	if err != nil && !named(rep, "body_len") {
		t.Errorf("check: %s, body_len not named; after enable the function over the row type of docs fails: %v", rep.Verdict, err)
	}
}

// C4-live. A row-security policy that reads a LokaDB table, on a cloud table (notes) or on
// another LokaDB table. Postgres keeps the policy's reference by OID: after enable it reads
// <table>__cloud. Every role but the owner then gets "permission denied for table
// blocked__cloud" (enable revokes the grants there), and after a move the policy no longer
// sees the moved organization's rows (NOT EXISTS lets through, EXISTS refuses). check names
// nothing (rls_enabled of a LokaDB table says, as info, that its policies apply as before).
func TestReview3LivePolicyReadsLokadbTable(t *testing.T) {
	dsn := liveDB(t, "rev3_policy")
	if err := liveExec(t, dsn,
		`CREATE TABLE blocked (id bigint PRIMARY KEY, org_id bigint NOT NULL, user_name text)`,
		`CREATE TABLE notes (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`, // stays in the cloud
		`ALTER TABLE notes ENABLE ROW LEVEL SECURITY`,
		`CREATE POLICY not_blocked ON notes USING (NOT EXISTS (SELECT 1 FROM blocked b WHERE b.org_id = notes.org_id AND b.user_name = current_user))`,
		`GRANT SELECT ON notes, blocked TO eve`,
		`INSERT INTO notes VALUES (1, 1, 'note')`); err != nil {
		t.Fatal(err)
	}
	if n, err := asEve(t, dsn, `SELECT count(*) FROM notes`); err != nil || n != 1 {
		t.Fatalf("before enable eve reads notes: %d, %v", n, err)
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "blocked", TenantColumn: "org_id"}}}
	rep := liveCheck(t, dsn, spec)
	if err := liveEnable(dsn, spec); err != nil {
		t.Fatalf("enable: %v", err)
	}
	n, err := asEve(t, dsn, `SELECT count(*) FROM notes`)
	t.Logf("after enable eve reads notes: %d rows, %v", n, err)
	if err != nil && !named(rep, "not_blocked") {
		t.Errorf("check: %s, policy not_blocked not named; after enable the application's role cannot read notes: %v", rep.Verdict, err)
	}
}

// C5-live. A child table by parent_table that already has a column of the organization
// column's name (a denormalized org_id). check: ready; enable: ALTER TABLE ADD COLUMN fails.
func TestReview3LiveChildHasTenantColumn(t *testing.T) {
	dsn := liveDB(t, "rev3_child")
	if err := liveExec(t, dsn,
		`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL)`,
		`CREATE TABLE items (id bigint PRIMARY KEY, order_id bigint REFERENCES orders, org_id bigint)`,
		`INSERT INTO orders VALUES (1, 1)`, `INSERT INTO items VALUES (1, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "orders", TenantColumn: "org_id"}, {Name: "items", ParentTable: "orders", ParentColumn: "order_id"}}}
	rep := liveCheck(t, dsn, spec)
	err := liveEnable(dsn, spec)
	t.Logf("enable: %v", err)
	if err != nil && (rep.Verdict == Ready || rep.Verdict == Review) {
		t.Errorf("check: %s; enable refuses: %v", rep.Verdict, err)
	}
}

// C6-live (admin's, found on the way). A spec naming public.<table>: check says invalid_spec
// (table_missing, did you mean orders?); enable converts the tables, commits, and then fails
// on the key index: its loop after COMMIT reads tenant[st.Name] by the spec's raw name
// (admin.go, enableOnce: buildKeyIndex), "" for public.orders. Only logged here.
func TestReview3LivePublicQualifiedSpec(t *testing.T) {
	dsn := liveDB(t, "rev3_public")
	if err := liveExec(t, dsn,
		`CREATE TABLE orders (id bigint PRIMARY KEY, org_id bigint NOT NULL)`,
		`CREATE TABLE items (id bigint PRIMARY KEY, order_id bigint REFERENCES orders)`); err != nil {
		t.Fatal(err)
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "public.orders", TenantColumn: "org_id"}, {Name: "public.items", ParentTable: "public.orders", ParentColumn: "order_id"}}}
	rep := liveCheck(t, dsn, spec)
	err := liveEnable(dsn, spec)
	var done bool
	ctx := context.Background()
	if c, cerr := pgx.Connect(ctx, dsn); cerr == nil {
		_ = c.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'lokadb' AND tablename = 'tables') AND EXISTS (SELECT 1 FROM lokadb.tables)`).Scan(&done)
		c.Close(ctx)
	}
	t.Logf("check: %s; enable: %v; tables converted and committed: %v", rep.Verdict, err, done)
}

// L2: a declared key whose map's name is longer than 63 bytes: enable refuses it, check says so.
func TestReview3KeyMapNameTooLong(t *testing.T) {
	c := testChecker()
	long := strings.Repeat("x", 60)
	c.meta.Tables["documents"].ColType[long] = "text"
	c.key(admin.Key{Table: "documents", Column: long})
	for _, f := range c.rep.Findings {
		if f.Code == "routing_invalid" && f.Severity == SpecError {
			return
		}
	}
	t.Errorf("a key with a map name over 63 bytes: findings %s, want routing_invalid (enable refuses it)", codes(c.rep.Findings))
}

// H1, H2 live: check reports what enable does with policies of other tables and functions over
// the row type, from admin.Dependents: no live database, the unit side is that schema.go makes
// its findings from that function only (the queries are block 4's, tested there).
