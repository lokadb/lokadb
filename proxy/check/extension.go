package check

import (
	"context"
	"errors"
	"flag"
	"io"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Extensions: parts of check built on this package from outside it, registered by Register.
// check/ext has the latency estimate (-rtt), the routing suggestions (-suggest, -patch) and the
// feed index advice; lokadb's main imports it, so every binary has them (tasks/check-split.md).
// An extension inside this module may use the proxy's internal packages for its analysis.

// Extension is a part of check registered with Register.
type Extension interface {
	// Flags adds the extension's flags to lokadb check's.
	Flags(fs *flag.FlagSet)
	// Run: after the workload and the profile are analyzed, in check's transaction (its
	// snapshot and locks: the data checks are made there).
	Run(ctx context.Context, s *Session) error
}

// Configurer: an extension that sets the options from its flags (after they are parsed).
type Configurer interface {
	Configure(o *Options) error
}

// TextSection: an extension with a section of the text report, after the summary.
type TextSection interface {
	WriteText(w io.Writer, r *Report)
}

// Finisher: an extension with work after the report is made (files its flags asked for).
type Finisher interface {
	Finish(r *Report) error
}

var extensions []Extension

// Register adds an extension (from an init function).
func Register(e Extension) { extensions = append(extensions, e) }

// Extensions: the registered extensions, in the order registered.
func Extensions() []Extension { return extensions }

// ErrNoDatabase: a data check of a session without a database (NewOfflineSession).
var ErrNoDatabase = errors.New("no database")

// Session is what an extension sees of a check: the report so far, the routing metadata
// the workload was judged with, the workload, and queries in check's transaction. What it
// returns is check's own: read it, do not change it.
type Session struct{ c *checker }

// Class: what the proxy would do with a statement.
const (
	ClassCloud       = "cloud"        // touches no LokaDB table
	ClassRouted      = "routed"       // confined to the organization it names
	ClassFanout      = "fanout"       // LokaDB tables without the organization: every organization's database
	ClassRefused     = "refused"      // ... and the fanout policy refuses it
	ClassAllowed     = "allowed"      // ... and the fanout policy allows it explicitly
	ClassUpsert      = "upsert"       // a rewritten upsert
	ClassDDL         = "ddl"          // replayed in the organizations' databases
	ClassDDLRejected = "ddl_rejected" // refused after enable
	ClassForbidden   = "forbidden"    // lokadb's own objects or FDW commands: refused after enable
	ClassParse       = "parse"        // not understood
)

var classNames = map[kind]string{kCloud: ClassCloud, kRouted: ClassRouted, kFanout: ClassFanout, kRefused: ClassRefused,
	kAllowed: ClassAllowed, kUpsert: ClassUpsert, kDDL: ClassDDL, kDDLRejected: ClassDDLRejected, kForbidden: ClassForbidden, kParse: ClassParse}

// WorkloadPattern: a query pattern of the workload as check judged it.
type WorkloadPattern struct {
	SQL         string // the most executed text of the pattern
	Fingerprint string // its pg_query fingerprint, "" when it does not parse
	Calls       int64
	Class       string // Class*
}

// Offline: what a session without a database is made of (an extension's tests; the service
// over an uploaded schema and recording). Data checks return ErrNoDatabase.
type Offline struct {
	Options      Options
	Spec         admin.Spec // the spec's tables; none: the metadata is enable's
	Meta         *proxy.Meta
	Fanout       *proxy.Fanout // nil: the default policy (reject)
	SearchPath   []string      // nil: public
	Statements   map[string]int64
	Transactions []*proxy.RecordedTx
	Patterns     []WorkloadPattern
	Report       *Report // nil: an empty one
}

// NewOfflineSession: a session over o, without a database.
func NewOfflineSession(o Offline) *Session {
	c := &checker{ctx: context.Background(), o: o.Options, spec: o.Spec, meta: o.Meta, fan: o.Fanout, path: o.SearchPath,
		pats: o.Statements, txs: o.Transactions, rep: o.Report}
	if c.rep == nil {
		c.rep = &Report{SchemaVersion: SchemaVersion, Sources: []string{}, Findings: []Finding{}, Tables: []TableStatus{}}
	}
	if c.fan == nil {
		c.fan = &proxy.Fanout{Mode: proxy.FanoutReject}
	}
	for _, p := range o.Patterns {
		k := kCloud
		for kk, name := range classNames {
			if name == p.Class {
				k = kk
			}
		}
		c.groups = append(c.groups, &group{kind: k, sql: p.SQL, fingerprint: p.Fingerprint, count: p.Calls})
	}
	return &Session{c}
}

// Report: the report so far; an extension fills its own fields (Latency, Routing).
func (s *Session) Report() *Report { return s.c.rep }

// Options: the check's options.
func (s *Session) Options() Options { return s.c.o }

// Meta: the routing metadata the workload was judged with (the spec's, with its routing
// declarations and -patch; or enable's).
func (s *Session) Meta() *proxy.Meta { return s.c.meta }

// SearchPath: the search path bare names resolve by (nil: public).
func (s *Session) SearchPath() []string { return s.c.path }

// Analyze: the proxy's analysis of sql against m, with the session's search path.
func (s *Session) Analyze(sql string, m *proxy.Meta) *proxy.Analysis {
	return s.c.analyze(Analyzable(sql), m)
}

// Classify: what the proxy would do with sql (Class*), by the session's metadata.
func (s *Session) Classify(sql string) string {
	k, _ := s.c.classify(sql)
	return classNames[k]
}

// Patterns: the workload's query patterns, as check grouped and judged them.
func (s *Session) Patterns() []WorkloadPattern {
	out := make([]WorkloadPattern, 0, len(s.c.groups))
	for _, g := range s.c.groups {
		out = append(out, WorkloadPattern{SQL: g.sql, Fingerprint: g.fingerprint, Calls: g.count, Class: classNames[g.kind]})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Calls > out[j].Calls || out[i].Calls == out[j].Calls && out[i].SQL < out[j].SQL
	})
	return out
}

// Statements: the workload's statements (normalized SQL -> executions).
func (s *Session) Statements() map[string]int64 { return s.c.pats }

// Transactions: the transaction shapes of the proxy's recording (none without one).
func (s *Session) Transactions() []*proxy.RecordedTx { return s.c.txs }

// Rows runs a query in check's transaction (ErrNoDatabase without one).
func (s *Session) Rows(sql string, args ...any) ([][]string, error) {
	if s.c.tx == nil {
		return nil, ErrNoDatabase
	}
	return s.c.rows(sql, args...)
}

// Heavy runs a pass over a table under -statement-timeout, in a savepoint of check's transaction.
func (s *Session) Heavy(run func(q pgx.Tx) error) error {
	if s.c.tx == nil {
		return ErrNoDatabase
	}
	return s.c.heavy(run)
}

// CheckRelation: how the rows of src find their parent in dst through col, on the data (as
// enable checks a declared relation).
func (s *Session) CheckRelation(src, col, dst string) (admin.RelationCheck, error) {
	return s.c.checkRelation(src, col, dst)
}

// CheckKey: a key's rows, NULLs and duplicate values (as enable checks a declared key).
func (s *Session) CheckKey(table, col string) (KeyCheck, error) { return s.c.checkKey(table, col) }

// CloudColumns: the columns of a table whose type lokadb takes as a key, with their kind
// (admin.KindInt, KindUUID, KindText).
func (s *Session) CloudColumns(table string) (map[string]string, error) {
	if s.c.tx == nil {
		return nil, ErrNoDatabase
	}
	return s.c.cloudColumns(table)
}

// UniqueColumns: the columns of a table that a unique index of their own covers.
func (s *Session) UniqueColumns(table string) map[string]bool { return s.c.uniqueColumns(table) }

// Add records a finding; its subject is read from object by the code, as check's own.
func (s *Session) Add(sev Severity, code, object, msg string, examples []string) {
	s.c.add(sev, code, object, msg, examples)
}

// AddFinding records a finding made in full; without a subject it is read from the object.
func (s *Session) AddFinding(f Finding) {
	if f.Subject == nil {
		f.Subject = subjectOf(f.Code, f.Object)
	}
	s.c.addFinding(f)
}

// Helpers check's own findings use, for an extension's findings to read the same.

// Analyzable: sql as the parser takes it ($n in utility statements read as names).
func Analyzable(sql string) string { return analyzable(sql) }

// Short: a statement on one line, cut to 160 characters.
func Short(sql string) string { return short(sql) }

// Plural: "1 call", "2 calls".
func Plural(n int, one, many string) string { return plural(n, one, many) }

// FirstLine: the first line of s.
func FirstLine(s string) string { l, _, _ := strings.Cut(s, "\n"); return l }

// TableKey: the column the refs of a LokaDB table map (its primary key; id when not recorded).
func TableKey(t *proxy.Table) string { return tableKey(t) }

// TenantSet: sql names the organization of its transaction (SET lokadb.tenant, set_config).
func TenantSet(sql string) bool { return tenantSet(sql) }

// TenantNamed: sql names an organization, or every one ('*').
func TenantNamed(sql string) (named, all bool) { return tenantNamed(sql) }

// QuoteIdent quotes an identifier; QuoteKey a table by its key (schema.table or table).
func QuoteIdent(s string) string { return q(s) }
func QuoteKey(k string) string   { return qk(k) }

// WithLookup: a copy of m with one more lookup (table.col finds the organization in refs).
func WithLookup(m *proxy.Meta, table, col, refs string) *proxy.Meta {
	return withLookup(m, table, col, refs)
}

// WithCloud: a copy of m with a declared cloud table (its organization column tc).
func WithCloud(m *proxy.Meta, table, tc string) *proxy.Meta { return withCloud(m, table, tc) }

// SplitQual: the schema ("" for public) and the table of a key.
func SplitQual(name string) (schema, table string) { return splitQual(name) }
