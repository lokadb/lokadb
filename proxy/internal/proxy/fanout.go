package proxy

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// Fanout policy: what the proxy does with a statement on LokaDB tables that it cannot confine
// to one organization (no organization column, id or parent id restricted by = or IN). Run as
// is, Postgres reads or changes every organization's partition, the foreign ones included:
// slower, and failing while any organization's database is unreachable.

const (
	FanoutReject       = "reject"        // refuse the statement (the default)
	FanoutRejectWrites = "reject-writes" // refuse INSERT/UPDATE/DELETE, run reads in every organization's database
	FanoutLog          = "log"           // run it, log a warning
	FanoutAllow        = "allow"         // run it
)

// FanoutPolicy is set per database in Config.Fanout; "*" applies to databases without an entry.
type FanoutPolicy struct {
	Mode string `json:"mode,omitempty"` // default: reject
	// Allow: statements reviewed and allowed to run in every organization's database in any mode.
	// Matched by pg_query fingerprint: constants, parameters and list lengths do not matter.
	Allow []string `json:"allow,omitempty"`
	// Tables: a decision for every statement on a table that names no organization, whatever its shape and
	// in any mode: "read" runs its reads in every organization's database and leaves its writes to the mode,
	// "allow" runs reads and writes there, "refuse" refuses it. Statements still
	// to come are covered too, which a list of statements cannot do.
	Tables map[string]string `json:"tables,omitempty"`
}

// Fanout is the compiled policy of one database.
type Fanout struct {
	Mode    string
	Allowed int // statements on the allow list
	allow   map[string]bool
	tables  map[string]string
	// strict: the same policy under reject, for log once an organization has moved (session.policy)
	strict *Fanout
	// fromLog: this is the strict twin of a log policy, so the refusal says log's name
	fromLog bool
}

// FanoutFor compiles the policy of database db: its own entry, else "*", else reject.
func (c Config) FanoutFor(db string) (*Fanout, error) {
	p, ok := c.Fanout[db]
	if !ok {
		p = c.Fanout["*"]
	}
	f := &Fanout{Mode: p.Mode, allow: map[string]bool{}, tables: map[string]string{}}
	if f.Mode == "" {
		f.Mode = FanoutReject
	}
	switch f.Mode {
	case FanoutReject, FanoutRejectWrites, FanoutLog, FanoutAllow:
	default:
		return nil, fmt.Errorf("fanout policy of %s: mode %q, want reject, reject-writes, log or allow", db, p.Mode)
	}
	for _, s := range p.Allow {
		fp, err := pg.Fingerprint(s)
		if err != nil {
			return nil, fmt.Errorf("fanout policy of %s: allow %q: %w", db, s, err)
		}
		f.allow[fp] = true
	}
	f.Allowed = len(p.Allow)
	for t, d := range p.Tables {
		if d != TableRead && d != TableAllow && d != TableRefuse {
			return nil, fmt.Errorf("fanout policy of %s: table %s: %q, want read, allow or refuse", db, t, d)
		}
		f.tables[t] = d
	}
	if f.Mode == FanoutLog {
		f.strict = &Fanout{Mode: FanoutReject, Allowed: f.Allowed, allow: f.allow, tables: f.tables, fromLog: true}
	}
	return f, nil
}

type FanoutVerdict int

const (
	FanoutNone    FanoutVerdict = iota // confined to one organization, or touches no LokaDB table
	FanoutAllowed                      // on the allow list: runs in every organization's database
	FanoutRun                          // runs in every organization's database (mode allow or log, a read under reject-writes)
	FanoutRefused                      // the client gets an error
	FanoutDenied                       // refused by a decision about its table: the client gets an error, and it is not counted as open
)

// The decisions about a table (FanoutPolicy.Tables).
const (
	TableRead   = "read" // its reads run everywhere; a write is the mode's to decide, as without a decision
	TableAllow  = "allow"
	TableRefuse = "refuse"
)

// Decide applies the policy to a statement and its analysis.
func (f *Fanout) Decide(sql string, an *Analysis) FanoutVerdict {
	return f.decide(sql, an, an.Unhinted)
}

// decide: unhinted is true also when the restriction's values turned out unreadable at run time.
func (f *Fanout) decide(sql string, an *Analysis, unhinted bool) FanoutVerdict {
	if !an.Lokadb || !unhinted {
		return FanoutNone
	}
	if len(f.allow) > 0 {
		if an.fp == "" {
			if an.fp, _ = pg.Fingerprint(sql); an.fp == "" {
				an.fp = "-"
			}
		}
		if f.allow[an.fp] {
			return FanoutAllowed
		}
	}
	if v, decided := f.tableVerdict(an); decided {
		return v
	}
	if f.Mode == FanoutAllow || f.Mode == FanoutLog || f.Mode == FanoutRejectWrites && !an.Write {
		return FanoutRun
	}
	return FanoutRefused
}

// openTables: every LokaDB table the statement leaves without an organization (the scopes' and the gaps').
func (a *Analysis) openTables() []string {
	out := append([]string(nil), a.UnhintedTables...)
	seen := map[string]bool{}
	for _, t := range out {
		seen[t] = true
	}
	for _, g := range a.Gaps {
		if g.Table != "" && !seen[g.Table] {
			seen[g.Table] = true
			out = append(out, g.Table)
		}
	}
	return out
}

// tableVerdict: what the decisions about tables say of a statement that leaves several tables without an
// organization: "refuse" of any of them refuses it; "read" / "allow" lets it run only when each of them has one.
func (f *Fanout) tableVerdict(an *Analysis) (FanoutVerdict, bool) {
	tables := an.openTables()
	if len(tables) == 0 {
		tables = []string{fanoutTable(an)}
	}
	allowed := true
	for _, t := range tables {
		switch f.tables[t] {
		case TableRefuse:
			return FanoutDenied, true
		case TableAllow:
		case TableRead:
			allowed = allowed && !an.Write
		default:
			allowed = false
		}
	}
	return FanoutAllowed, allowed
}

// openView: the analysis as the open list shows it: only the tables nobody decided (a statement that
// joins a decided table and an undecided one is listed under the undecided one, where the person can answer it).
func (f *Fanout) openView(an *Analysis) *Analysis {
	open := an.openTables()
	if len(f.tables) == 0 || len(open) < 2 {
		return an
	}
	v := *an
	v.UnhintedTable, v.UnhintedTables, v.Gaps = "", nil, nil
	for _, t := range open {
		if f.tables[t] == "" {
			v.UnhintedTables = append(v.UnhintedTables, t)
		}
	}
	if len(v.UnhintedTables) == 0 {
		return an
	}
	v.UnhintedTable = v.UnhintedTables[0]
	for _, g := range an.Gaps {
		if g.Table == "" || f.tables[g.Table] == "" {
			v.Gaps = append(v.Gaps, g)
		}
	}
	return &v
}

// listed: sql is on the allow list (by fingerprint).
func (f *Fanout) listed(sql string) bool {
	fp, _ := pg.Fingerprint(sql)
	return fp != "" && f.allow[fp]
}

// raiseSQL is what the proxy sends instead of a refused statement: the server raises the
// error itself, so the transaction ends up exactly as after any error there (an implicit
// transaction rolls back, an explicit one is aborted) and the rest of a pipeline is skipped.
func (f *Fanout) raiseSQL(an *Analysis) string {
	msg := fmt.Sprintf("lokadb: the statement does not name the organization of %s rows, so it covers many organizations at once and would run separately in each organization's database (%s)",
		fanoutTable(an), f.Name())
	hint := `Restrict it by the organization column, id or a parent id (= or IN), or add it to "allow" of this database's fanout policy in the proxy config.`
	detail := ""
	if g := an.GapText(); g != "" {
		detail = ", DETAIL = " + sqlString("why: "+g)
	}
	return ownsql.Tag(fmt.Sprintf("DO $lokadb$ BEGIN RAISE EXCEPTION USING ERRCODE = '0A000', MESSAGE = %s%s, HINT = %s; END $lokadb$", sqlString(msg), detail, sqlString(hint)))
}

// Name is the policy as an error or a log line says it: a log policy that refuses because an
// organization has moved says "log" and why, not the "reject" it behaves as.
func (f *Fanout) Name() string {
	if f.fromLog {
		return `fanout policy "log", which refuses once an organization has moved; "allow" runs it in every database`
	}
	return fmt.Sprintf("fanout policy %q", f.Mode)
}

// fanoutTable: the LokaDB table the statement fails to confine.
func fanoutTable(an *Analysis) string {
	if an.UnhintedTable == "" && len(an.Hints) > 0 {
		return an.Hints[0].Table
	}
	return an.UnhintedTable
}

// raise runs raiseSQL through a self-contained named statement/portal, like setOrg. Its
// error reaches the client (see route: errors of injected messages are passed on).
func raise(sql string) []outMsg {
	return []outMsg{
		{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_r"}, mode: mDrop},
		{msg: &pgproto3.Close{ObjectType: 'P', Name: "lokadb_rp"}, mode: mDrop},
		{msg: &pgproto3.Parse{Name: "lokadb_r", Query: ownsql.Tag(sql)}, mode: mDrop},
		{msg: &pgproto3.Bind{DestinationPortal: "lokadb_rp", PreparedStatement: "lokadb_r"}, mode: mDrop},
		{msg: &pgproto3.Execute{Portal: "lokadb_rp"}, mode: mDrop},
	}
}

// For: the policy as it applies under metadata m. log is reject once an organization has moved
// out of the cloud (its foreign partition, or a profile in lokadb.orgs), as its log line
// promises: the mode was read once at the start, and a proxy nobody restarted after the first
// move ran a statement without a customer in every organization's database, the moved one's
// through FDW, and a statement it cannot read under every organization (third review R4-26).
func (f *Fanout) For(m *Meta) *Fanout {
	if f != nil && f.strict != nil && m != nil && m.anyMoved() {
		return f.strict
	}
	return f
}

// policy: the session's fanout policy as it applies now (For).
func (s *session) policy() *Fanout { return s.fanout.For(s.meta) }
