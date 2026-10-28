// Package check builds a compatibility report before `lokadb enable` and `move-org`: can the
// selected tables be split by organization, does the observed workload name the
// organization, and are the cloud and the organization's databases ready for the move.
// It only reads: catalogs, pg_stat_statements and the proxy's workload recording.
//
// The report covers the schema and the queries seen. It cannot prove that queries nobody
// has seen yet (rare code paths, dynamic SQL) are compatible: that takes tests through
// the proxy and the proxy's own refusal of operations it cannot handle.
package check

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

type Severity string

// A Blocker is only what stops moving the tables whatever lokadb does: rows that cannot be
// attributed to an organization, an environment that is not ready, customer data that
// would be overwritten. A schema lokadb cannot handle yet is Unsupported: a limitation of
// lokadb, not a defect of the schema.
const (
	SpecError   Severity = "spec_error"  // the spec does not match the database: fix the spec
	Blocker     Severity = "blocker"     // the migration cannot run until this is fixed
	Unsupported Severity = "unsupported" // lokadb cannot move this yet; enable refuses it for now
	Change      Severity = "change"      // works, but behaves differently after the move: the owner decides
	Warning     Severity = "warning"     // works, but somebody has to look
	Info        Severity = "info"
)

// Table states, worst last.
const (
	StateReady       = "ready"
	StateReview      = "review"      // a Change applies to it
	StateUnsupported = "unsupported" // lokadb cannot move it yet
	StateBlocked     = "blocked"
	StateSpecError   = "spec_error" // the spec does not match the table
)

var stateRank = map[string]int{"": 0, StateReady: 0, StateReview: 1, StateUnsupported: 2, StateBlocked: 3, StateSpecError: 4}

// SchemaVersion of the JSON report (tasks/check-report-format.md). It grows only on an
// incompatible change: a field removed, renamed or changing meaning, a finding code renamed.
// New fields and codes keep it.
const SchemaVersion = 1

type Finding struct {
	Severity Severity `json:"severity"`
	Code     string   `json:"code"` // never renamed: consumers match on it
	// Key identifies the finding across runs and machines (-ci -baseline): the code and its subject.
	Key     string   `json:"key"`
	Object  string   `json:"object,omitempty"` // for people; Subject is the machine-readable form
	Subject *Subject `json:"subject,omitempty"`
	Message string   `json:"message"`
	// Examples are cut for people (at most 8); Patterns and Transactions are complete.
	Examples     []string  `json:"examples,omitempty"`
	Patterns     []Pattern `json:"patterns,omitempty"`
	Transactions []TxShape `json:"transactions,omitempty"`
	Measure      *Measure  `json:"measure,omitempty"`
	// Suggestion: the declaration a routing_suggestion proposes (suggest.go)
	Suggestion *Suggestion `json:"suggestion,omitempty"`
}

// Subject is what a finding is about. Kind decides which of the other fields are set.
type Subject struct {
	Kind       string   `json:"kind"`           // table, column, constraint, database, role, extension, setting, schema, file, tables, reason
	Side       string   `json:"side,omitempty"` // cloud or customer (the organization's database): for database, role, extension, setting, schema
	Schema     string   `json:"schema,omitempty"`
	Table      string   `json:"table,omitempty"`
	Column     string   `json:"column,omitempty"`
	Constraint string   `json:"constraint,omitempty"`
	Columns    []string `json:"columns,omitempty"`
	RefSchema  string   `json:"ref_schema,omitempty"` // a foreign key's target
	RefTable   string   `json:"ref_table,omitempty"`
	RefColumns []string `json:"ref_columns,omitempty"`
	// PartitionOf: the table is a partition of this one; Partitions: the partitions a finding
	// on the parent stands for.
	PartitionOf string   `json:"partition_of,omitempty"`
	Partitions  []string `json:"partitions,omitempty"`
	Database    string   `json:"database,omitempty"`
	Role        string   `json:"role,omitempty"`
	Extension   string   `json:"extension,omitempty"`
	Setting     string   `json:"setting,omitempty"`
	Path        string   `json:"path,omitempty"`
	Reason      string   `json:"reason,omitempty"` // why a statement cannot take the direct path (check -profile)
	// Fingerprint: of a query pattern (kind pattern), of a transaction shape's statements joined by + (kind transaction)
	Fingerprint  string   `json:"fingerprint,omitempty"`
	CloudTables  []string `json:"cloud_tables,omitempty"`
	LokadbTables []string `json:"lokadb_tables,omitempty"`
}

// Pattern is one query pattern: the statements sharing a pg_query fingerprint and a verdict.
type Pattern struct {
	Fingerprint string   `json:"fingerprint"` // as the proxy's fanout allow list matches it; "" when it does not parse
	SQL         string   `json:"sql"`         // the most executed normalized text, uncut
	Calls       int64    `json:"calls"`
	Write       bool     `json:"write"`
	Tables      []string `json:"tables,omitempty"`  // the LokaDB tables it touches
	Roles       []string `json:"roles,omitempty"`   // who sent it, when the source tells
	Sources     []string `json:"sources,omitempty"` // pg_stat_statements, recording:FILE
	Note        string   `json:"note,omitempty"`    // why the proxy refuses it
	// Gaps: why the organization was not found, one per table or column (proxy.Gap, block 1);
	// set for the patterns that name no organization.
	Gaps []proxy.Gap `json:"gaps,omitempty"`
}

// TxShape is a group of transaction shapes writing the same cloud and LokaDB tables.
type TxShape struct {
	CloudTables  []string      `json:"cloud_tables"`
	LokadbTables []string      `json:"lokadb_tables"`
	Shapes       int           `json:"shapes"`
	Count        int64         `json:"count"`
	Statements   []TxStatement `json:"statements"` // of the most frequent shape, in order
}

type TxStatement struct {
	Fingerprint string   `json:"fingerprint"`
	SQL         string   `json:"sql"`
	Write       bool     `json:"write"`
	Tables      []string `json:"tables,omitempty"` // the tables it writes
}

// Measure: how many rows a data check found, and how it counted them.
type Measure struct {
	Rows   int64  `json:"rows"`
	Of     int64  `json:"of"` // rows in the table; -1 unknown
	Method string `json:"method"`
}

// Measure methods.
const (
	MeasureExact      = "exact"      // a full pass over the table
	MeasureSample     = "sample"     // TABLESAMPLE: Rows is an estimate
	MeasureStatistics = "statistics" // pg_stats: Rows is an estimate
	MeasureExists     = "exists"     // a sample found none, EXISTS found one: Rows is 1, at least one
)

// TableStatus is one table selected for LokaDB (from the spec or discovered).
type TableStatus struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	Tenant string `json:"tenant"`        // how its rows find their organization
	Rows   int64  `json:"rows_estimate"` // -1: not counted
	Ready  bool   `json:"ready"`         // ready or review: enable can partition it
	State  string `json:"state"`
}

type Summary struct {
	Tables            int  `json:"tables"`
	TablesReady       int  `json:"tables_ready"`
	TablesUnsupported int  `json:"tables_unsupported"`
	UniquePerOrg      int  `json:"unique_constraints_becoming_per_organization"`
	QueryPatterns     int  `json:"query_patterns"`
	UnroutedPatterns  int  `json:"query_patterns_without_organization"`
	RefusedPatterns   int  `json:"query_patterns_refused_by_proxy"`
	TxPatterns        int  `json:"transaction_patterns"`
	MixedTxPatterns   int  `json:"transaction_patterns_writing_cloud_and_lokadb"`
	TxPatternsChecked bool `json:"transactions_checked"`
}

type Verdict string

const (
	Ready        Verdict = "ready"
	Review       Verdict = "review"
	NotSupported Verdict = "unsupported" // no blockers, but lokadb cannot move some tables yet
	Blocked      Verdict = "blocked"
	InvalidSpec  Verdict = "invalid_spec" // the spec does not match the database: the rest is incomplete
)

type Report struct {
	SchemaVersion int           `json:"schema_version"`
	LokadbVersion string        `json:"lokadb_version"` // the build of lokadb that made the report
	Database      string        `json:"database"`
	Verdict       Verdict       `json:"verdict"`
	Summary       Summary       `json:"summary"`
	Tables        []TableStatus `json:"tables"`
	Spec          *admin.Spec   `json:"suggested_spec,omitempty"` // when the tables were discovered
	// Discovery: with -org-table, why every table is in the spec or not (orgtable.go)
	Discovery *Discovery `json:"discovery,omitempty"`
	// Input: the spec the report was computed for (given or discovered); empty after enable.
	Input   *admin.Spec `json:"spec,omitempty"`
	Sources []string    `json:"workload_sources"`
	// FanoutPolicy: the proxy's policy for queries that name no organization, as the report
	// applied it, for people; Fanout is the same for machines.
	FanoutPolicy string      `json:"fanout_policy"`
	Fanout       *FanoutInfo `json:"fanout,omitempty"`
	// Routing: every declaration the report suggests, as one patch (check -suggest writes it)
	Routing  *admin.Routing `json:"suggested_routing,omitempty"`
	Findings []Finding      `json:"findings"`
	// CI: with -ci, what the baseline has not accepted (CompareBaseline).
	CI *CIResult `json:"ci,omitempty"`
	// Profile: what -profile would do with the workload (profile.go)
	Profile *ProfileReport `json:"profile,omitempty"`
	// Latency: with -rtt, the estimated wait of a moved organization's transactions (latency.go)
	Latency *LatencyReport `json:"latency,omitempty"`
}

type FanoutInfo struct {
	Mode   string   `json:"mode"`
	Allow  []string `json:"allow"`  // fingerprints of the allow list, sorted
	Source string   `json:"source"` // the proxy config file, or "default"
}

type Options struct {
	DSN          string      // cloud database (admin)
	Spec         *admin.Spec // nil: discover the tables from TenantColumn
	TenantColumn string
	// OrgTable: discover the tables by the organizations table instead of a column name
	// (orgtable.go): the table's key is the organization, every foreign key to it is a tenant column
	OrgTable    string
	Workload    []string      // proxy recording files
	AppRoles    []string      // pg_stat_statements of these roles only (default: all but the role running the check)
	ProxyHosts  []string      // the addresses the application's role may reach the cloud from (the proxy, the relay): cloud_direct_access
	Proxy       *proxy.Config // its fanout policy decides which queries the proxy refuses (nil: the default, reject)
	ProxyFile   string        // where Proxy came from, for the report
	CustomerDSN string        // the organization's database (optional)
	// CustomerAppDSN: the role the cloud keeps for the organization's database (move-org -customer-app)
	CustomerAppDSN string
	CustomerSchema string // move-org -customer-schema ("": public if it can take the tables, else lokadb)
	Org            int64  // the organization to move (optional, sharpens the customer checks)
	// Profile: estimate this routing profile (fdw, fdw+direct, direct) on the workload; "": not asked (profile.go)
	Profile string
	// CountLimit: data checks count tables of up to this many rows exactly and estimate
	// bigger ones from a sample (0: 1000000; negative: always exactly).
	CountLimit int64
	// Patch: routing declarations to judge the workload with, as if the spec had them (the
	// -patch of check/ext sets it; extension.go)
	Patch *admin.Routing
	// StatementTimeout: the longest a full pass over a table may take (duplicate keys, a
	// declared key or relation checked on the data, EXISTS after an empty sample); past it
	// the report says the check was not made (0: 1 minute; negative: no limit)
	StatementTimeout time.Duration
}

type checker struct {
	// twoShapes, fdwAfterShapes: the transaction shapes behind profile_two_connections and
	// profile_fdw_after_direct, each a -ci item (profile.go; review 3, M3)
	twoShapes, fdwAfterShapes []TxShape
	// toCloud: the FKs from LokaDB tables to cloud tables the schema check found (fk_to_cloud):
	// profile direct cannot write those tables (profile.go)
	toCloud []string
	ctx     context.Context
	path    []string // the search path statements are analyzed with (keys.go)
	tx      pgx.Tx
	o       Options
	rep     *Report
	spec    admin.Spec
	meta    *proxy.Meta

	cloudVersion int           // server_version_num
	cloudLogical bool          // the cloud can publish by logical replication (admin.LogicalReady)
	fan          *proxy.Fanout // the proxy's fanout policy for this database
	// deps: what the LokaDB tables need in the organization's database (types, functions,
	// operator classes, collations); filled by the schema checks.
	deps map[string]*dependency
	// cloudEncoding, cloudCollate: the cloud database's encoding and default collation.
	cloudEncoding, cloudCollate string
	// custSchema: where move-org would put the organization's tables (admin.ChooseCustomerSchema).
	custSchema string
	// pats, txs: the workload as workload() collected it (normalized SQL -> executions, recorded transaction shapes)
	pats map[string]int64
	txs  []*proxy.RecordedTx
	// groups: the workload's query patterns as workload() judged them (Session.Patterns)
	groups []*group
	// wasEnabled: lokadb enable has run (c.meta is the proxy's, from lokadb.tables).
	wasEnabled bool
	// depsRead: the tables whose dependencies c.deps holds (portable.go: each table once)
	depsRead map[string]bool
	// enabledMeta: the proxy's metadata, when the spec has tables that enable has not taken
	// yet: schema() checks those (nil otherwise).
	enabledMeta *proxy.Meta
	// seenRoles: the roles whose statements pg_stat_statements counted as the application's.
	seenRoles map[string]bool
}

func Run(ctx context.Context, o Options) (*Report, error) {
	conn, err := ownsql.Connect(ctx, o.DSN) // check's own statements: its workload analysis skips them (internal/ownsql)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)
	tx, err := beginCheck(ctx, conn)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	c := &checker{ctx: ctx, tx: tx, o: o, rep: &Report{SchemaVersion: SchemaVersion, LokadbVersion: buildVersion(), Sources: []string{}, Findings: []Finding{}, Tables: []TableStatus{}}}
	db, err := c.rows(`SELECT current_database()`)
	if err != nil {
		return nil, err
	}
	c.rep.Database = db[0][0]

	enabled, err := c.rows(`SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil {
		return nil, err
	}
	if enabled[0][0] == "true" {
		// enable -prepare makes lokadb.tables too (block 3): enabled means a table in it
		if enabled, err = c.rows(`SELECT EXISTS (SELECT 1 FROM lokadb.tables)::text`); err != nil {
			return nil, err
		}
	}
	if enabled[0][0] == "true" {
		if c.meta, err = proxy.LoadMeta(func(sql string) ([][]string, error) { return c.rows(sql) }); err != nil {
			return nil, err
		}
	}
	if c.meta != nil {
		c.wasEnabled = true
		c.add(Info, "already_enabled", "", "lokadb enable has already run here: the schema checks apply before enable and are skipped; the workload is checked against the recorded LokaDB tables, and what the tables need in an organization's database as before enable", nil)
		c.orgsOnFDW()
		for _, t := range sortedTables(c.meta) {
			c.rep.Tables = append(c.rep.Tables, TableStatus{Name: t, Tenant: c.meta.Tables[t].TenantCol, Rows: -1, Ready: true, State: StateReady})
			// What the table needs in an organization's database (extensions, types, functions,
			// collations), as before enable: an organization moved after enable needs the same
			// (move-org works from the same list). admin.DependencySQL reads the parent and its
			// partitions' indexes; lokadb's own objects (the __cloud partition, its triggers and
			// defaults) are not on that list.
			if err := c.dependencies(t); err != nil {
				return nil, err
			}
		}
		if c.newSpecTables() {
			// the spec's new tables: the schema checks, with the LokaDB tables as known parents
			c.enabledMeta = c.meta
			c.spec = c.specWithEnabled(*o.Spec).Sorted()
			if err := c.addingTables(); err != nil {
				return nil, err
			}
			c.rep.Input = &c.spec
			c.rep.Tables = nil
			if err := c.schema(); err != nil {
				return nil, err
			}
			for _, t := range sortedTables(c.enabledMeta) { // LokaDB tables that the spec does not list
				if !c.inReport(t) {
					c.rep.Tables = append(c.rep.Tables, TableStatus{Name: t, Tenant: c.enabledMeta.Tables[t].TenantCol, Rows: -1, Ready: true, State: StateReady})
				}
			}
		}
	} else {
		if o.Spec != nil {
			c.spec = o.Spec.Sorted() // as enable and move-org take it
		} else {
			switch {
			case o.OrgTable != "" && o.TenantColumn != "":
				return nil, fmt.Errorf("give -org-table or -tenant-column, not both")
			case o.OrgTable != "":
				d, err := c.discoverOrg(o.OrgTable)
				if err != nil {
					return nil, err
				}
				c.spec = d.Spec.Sorted()
				d.Spec = c.spec
				c.rep.Discovery = d
				c.orgDiscovered(d)
			case o.TenantColumn != "":
				if c.spec, err = c.discover(o.TenantColumn); err != nil {
					return nil, err
				}
			default:
				return nil, c.noDiscovery()
			}
			c.rep.Spec = &c.spec
		}
		c.rep.Input = &c.spec
		if err := c.schema(); err != nil {
			return nil, err
		}
	}
	if err := c.prepareProgress(); err != nil { // block 3: enable -prepare
		return nil, err
	}
	if err := c.workload(); err != nil {
		return nil, err
	}
	if o.Profile != "" {
		if err := c.profile(); err != nil {
			return nil, err
		}
	}
	for _, e := range extensions { // check/ext: -rtt, -suggest, the feed index (extension.go)
		if err := e.Run(ctx, &Session{c}); err != nil {
			return nil, err
		}
	}
	// The data checks are done: their snapshot and their locks on the spec's tables (counts,
	// max, distinct keys) end here, before the steps that wait on the network (the FDW probe,
	// the organization's database, DDL replay), so a migration's ALTER TABLE does not queue
	// behind the check and the application's queries behind it (review 2, R9).
	_ = c.tx.Rollback(ctx)
	if c.tx, err = beginCheck(ctx, conn); err != nil {
		return nil, err
	}
	tx = c.tx
	if c.wasEnabled {
		c.replayReady() // block 7: replay.go
	}
	if err := c.cloudReady(); err != nil {
		return nil, err
	}
	if err := c.cdc(); err != nil {
		return nil, err
	}
	if o.CustomerDSN != "" {
		c.customerReady()
	} else if len(c.deps) > 0 {
		c.add(Info, "org_dependencies", "", "the organization's database must have these for the LokaDB tables (check it with -customer)", describeDeps(c.sortedDeps()))
	}
	c.finish()
	return c.rep, nil
}

// newSpecTables: tables of -spec that enable has not taken yet (a second enable adds them
// while no organization is moved, admin/prepare.go).
func (c *checker) newSpecTables() bool {
	if c.o.Spec == nil || c.meta == nil {
		return false
	}
	for _, st := range c.o.Spec.Tables {
		if c.meta.Tables[st.Name] == nil {
			return true
		}
	}
	return false
}

// specWithEnabled: the spec with the LokaDB tables it does not list: enable takes a new child's
// organization from an enabled parent that the spec does not repeat (admin plan), so check
// knows them as parents too (review 3, M5).
func (c *checker) specWithEnabled(s admin.Spec) admin.Spec {
	in := map[string]bool{}
	for _, st := range s.Tables {
		in[st.Name] = true
	}
	out := admin.Spec{Tables: append([]admin.SpecTable{}, s.Tables...), Routing: s.Routing}
	for _, t := range sortedTables(c.meta) {
		if !in[t] {
			out.Tables = append(out.Tables, admin.SpecTable{Name: t, TenantColumn: c.meta.Tables[t].TenantCol})
		}
	}
	return out
}

// addingTables: enable refuses the spec's new tables while organizations are moved or a move
// runs (admin.AddingTablesRefused, the rule enable applies): a blocker (review 3, M5).
func (c *checker) addingTables() error {
	var names []string
	for _, st := range c.o.Spec.Tables {
		if c.meta.Tables[st.Name] == nil {
			names = append(names, st.Name)
		}
	}
	why, err := admin.AddingTablesRefused(c.ctx, c.tx, names)
	if err != nil || why == "" {
		return err
	}
	c.add(Blocker, "spec_tables_after_move", "", "enable would not add the spec's new tables: "+why+". Move the organizations back (move-back) first, or keep these tables in the cloud for now", names)
	return nil
}

// inReport: table t has a row in the report's table list.
func (c *checker) inReport(t string) bool {
	for _, ts := range c.rep.Tables {
		if ts.Name == t {
			return true
		}
	}
	return false
}

// add records a finding; its subject is read from object by the code (subjectOf).
func (c *checker) add(sev Severity, code, object, msg string, examples []string) {
	c.addFinding(Finding{Severity: sev, Code: code, Object: object, Subject: subjectOf(code, object), Message: msg, Examples: examples})
}

// addOn records a finding whose subject the caller knows better than its object tells.
func (c *checker) addOn(sev Severity, code string, s *Subject, object, msg string, examples []string) {
	c.addFinding(Finding{Severity: sev, Code: code, Object: object, Subject: s, Message: msg, Examples: examples})
}

// addFinding records a finding made in full (patterns, transactions, measure).
func (c *checker) addFinding(f Finding) {
	f.Key = keyOf(f.Code, f.Subject)
	c.rep.Findings = append(c.rep.Findings, f)
}

func (c *checker) finish() {
	c.rep.Summary.Tables = len(c.rep.Tables)
	for i := range c.rep.Tables {
		if c.rep.Tables[i].Schema == "" {
			c.rep.Tables[i].Schema, _ = admin.SplitKey(c.rep.Tables[i].Name)
		}
	}
	c.rep.Fanout = c.fanoutInfo()
	c.rep.Verdict = Ready
	for _, t := range c.rep.Tables {
		if t.Ready {
			c.rep.Summary.TablesReady++
		}
		if t.State == StateUnsupported {
			c.rep.Summary.TablesUnsupported++
		}
	}
	rank := map[Verdict]int{Ready: 0, Review: 1, NotSupported: 2, Blocked: 3, InvalidSpec: 4}
	for _, f := range c.rep.Findings {
		v := map[Severity]Verdict{SpecError: InvalidSpec, Blocker: Blocked, Unsupported: NotSupported, Change: Review, Warning: Review}[f.Severity]
		if v != "" && rank[v] > rank[c.rep.Verdict] {
			c.rep.Verdict = v
		}
	}
	order := map[Severity]int{SpecError: -1, Blocker: 0, Unsupported: 1, Change: 2, Warning: 3, Info: 4}
	fs := c.rep.Findings
	for i := 1; i < len(fs); i++ { // stable: keep the order findings were made in within a severity
		for j := i; j > 0 && order[fs[j].Severity] < order[fs[j-1].Severity]; j-- {
			fs[j], fs[j-1] = fs[j-1], fs[j]
		}
	}
}

// Counting rows for a data check (tenant_null, orphan_rows): tables up to the limit are
// counted exactly; bigger ones from a block sample of about sampleRows rows, so that the
// check does not read the whole table.
const (
	defaultCountLimit = 1_000_000
	sampleRows        = 100_000
)

// count: how many rows of table (quoted, aliased as alias in cond) match cond.
func (c *checker) count(table, alias, cond string) (*Measure, error) {
	limit := c.o.CountLimit
	if limit == 0 {
		limit = defaultCountLimit
	}
	exact := func() (*Measure, error) {
		r, err := c.rows(fmt.Sprintf("SELECT count(*) FILTER (WHERE %s)::text, count(*)::text FROM %s %s", cond, table, alias))
		if err != nil {
			return nil, err
		}
		n, _ := strconv.ParseInt(r[0][0], 10, 64)
		of, _ := strconv.ParseInt(r[0][1], 10, 64)
		return &Measure{Rows: n, Of: of, Method: MeasureExact}, nil
	}
	if limit < 0 {
		return exact()
	}
	r, err := c.rows(`SELECT reltuples::bigint::text FROM pg_class WHERE oid = $1::regclass`, table)
	if err != nil {
		return nil, err
	}
	est, _ := strconv.ParseInt(r[0][0], 10, 64)
	if est < 0 { // never analyzed: a 0.1% sample tells the size
		s, err := c.rows(fmt.Sprintf("SELECT (count(*) * 1000)::text FROM %s TABLESAMPLE SYSTEM (0.1)", table))
		if err != nil {
			return nil, err
		}
		est, _ = strconv.ParseInt(s[0][0], 10, 64)
	}
	if est <= limit {
		return exact()
	}
	pct := min(100, 100*float64(sampleRows)/float64(est))
	s, err := c.rows(fmt.Sprintf("SELECT count(*) FILTER (WHERE %s)::text, count(*)::text FROM %s %s TABLESAMPLE SYSTEM (%g) REPEATABLE (0)", cond, table, alias, pct))
	if err != nil {
		return nil, err
	}
	n, _ := strconv.ParseInt(s[0][0], 10, 64)
	seen, _ := strconv.ParseInt(s[0][1], 10, 64)
	if seen == 0 {
		return exact()
	}
	return &Measure{Rows: (n*est + seen/2) / seen, Of: est, Method: MeasureSample}, nil
}

// present: as count, for a check whose finding is a blocker: a sample that finds nothing does
// not prove there is nothing, so then EXISTS looks for one (it stops at the first; with nothing
// to find it reads the table, as an exact count would). Rows is 1 (at least one) with method
// exists (review 2, R8).
func (c *checker) present(table, alias, cond string) (*Measure, error) {
	m, err := c.count(table, alias, cond)
	if err != nil || m.Method != MeasureSample || m.Rows > 0 {
		return m, err
	}
	r, err := c.heavyRows(fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s %s WHERE %s)::text", table, alias, cond))
	if errors.Is(err, errTooLong) {
		return m, nil // the sample stands, and the finding says it is one
	}
	if err != nil {
		return nil, err
	}
	if r[0][0] == "true" {
		return &Measure{Rows: 1, Of: m.Of, Method: MeasureExists}, nil
	}
	return &Measure{Rows: 0, Of: m.Of, Method: MeasureExact}, nil
}

// sampledNone: for a sample that found nothing, that it is a sample (empty otherwise).
func sampledNone(m *Measure) string {
	if m == nil || m.Method != MeasureSample || m.Rows > 0 {
		return ""
	}
	return fmt.Sprintf(" (a sample of the ~%d rows found none, which does not prove there are none: -count-limit -1 counts exactly)", m.Of)
}

// counted: how a measure reads in a message ("1200 rows", "about 1200 of ~9000000 rows (sample)").
func counted(m *Measure, one, many string) string {
	if m.Method == MeasureExact {
		return plural(int(m.Rows), one, many)
	}
	if m.Method == MeasureExists {
		return fmt.Sprintf("some %s (at least one of ~%d; a sample found none, -count-limit -1 counts them)", many, m.Of)
	}
	return fmt.Sprintf("about %d %s of ~%d (estimated from a sample; -count-limit -1 counts exactly)", m.Rows, many, m.Of)
}

// errTooLong: a full pass over a table took longer than -statement-timeout.
var errTooLong = errors.New("it takes longer than -statement-timeout on this table: not checked (a larger -statement-timeout checks it)")

// heavy runs a full pass over a table under -statement-timeout, in a savepoint: a statement
// cancelled for it leaves the check's transaction as it was (review 3, M4), and the caller
// reports the check as not made (errTooLong).
func (c *checker) heavy(run func(q pgx.Tx) error) error {
	limit := c.o.StatementTimeout
	if limit == 0 {
		limit = time.Minute
	}
	sp, err := c.tx.Begin(c.ctx)
	if err != nil {
		return err
	}
	if limit > 0 {
		if _, err := sp.Exec(c.ctx, fmt.Sprintf("%sSET LOCAL statement_timeout = %d", ownMark, limit.Milliseconds())); err != nil {
			_ = sp.Rollback(c.ctx)
			return err
		}
	}
	if err := run(sp); err != nil {
		_ = sp.Rollback(c.ctx) // undoes the SET LOCAL too
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "57014" { // query_canceled: the statement timeout
			return errTooLong
		}
		return err
	}
	if limit > 0 { // SET LOCAL outlives RELEASE: back to none for what follows
		if _, err := sp.Exec(c.ctx, ownMark+"SET LOCAL statement_timeout = 0"); err != nil {
			_ = sp.Rollback(c.ctx)
			return err
		}
	}
	return sp.Commit(c.ctx)
}

// heavyRows: c.rows under heavy.
func (c *checker) heavyRows(sql string, args ...any) ([][]string, error) {
	var out [][]string
	err := c.heavy(func(q pgx.Tx) error {
		var err error
		out, err = rowsOf(c.ctx, q, sql, args...)
		return err
	})
	return out, err
}

// prepared: enable -prepare has started on table t (lokadb.prepare): it adds a child's
// organization column itself, so the column is no clash.
func (c *checker) prepared(t string) bool {
	// two queries: a statement naming lokadb.prepare fails, and aborts the check's
	// transaction, where it does not exist
	if r, err := c.rows(`SELECT (to_regclass('lokadb.prepare') IS NOT NULL)::text`); err != nil || r[0][0] != "true" {
		return false
	}
	r, err := c.rows(`SELECT EXISTS (SELECT FROM lokadb.prepare WHERE table_name = $1)::text`, t)
	return err == nil && len(r) > 0 && r[0][0] == "true"
}

// beginCheck: a read-only transaction that waits for no lock long: behind a migration's
// ALTER TABLE the check gives up instead of waiting, and of keeping others waiting.
func beginCheck(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, ownMark+"SET LOCAL lock_timeout = '10s'"); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

// rows runs a catalog query and returns its values as text.
func (c *checker) rows(sql string, args ...any) ([][]string, error) {
	return rowsOf(c.ctx, c.tx, sql, args...)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// ownMark starts every query of the check, so pg_stat_statements can be read without them.
const ownMark = "/* lokadb check */ "

func rowsOf(ctx context.Context, q querier, sql string, args ...any) ([][]string, error) {
	rows, err := q.Query(ctx, ownMark+sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%w\n  in: %s", err, strings.TrimSpace(sql))
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v != nil {
				row[i] = fmt.Sprint(v)
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func q(s string) string { return pgx.Identifier{s}.Sanitize() }

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}
