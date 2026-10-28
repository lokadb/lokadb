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
}

// Fanout is the compiled policy of one database.
type Fanout struct {
	Mode    string
	Allowed int // statements on the allow list
	allow   map[string]bool
}

// FanoutFor compiles the policy of database db: its own entry, else "*", else reject.
func (c Config) FanoutFor(db string) (*Fanout, error) {
	p, ok := c.Fanout[db]
	if !ok {
		p = c.Fanout["*"]
	}
	f := &Fanout{Mode: p.Mode, allow: map[string]bool{}}
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
	return f, nil
}

type FanoutVerdict int

const (
	FanoutNone    FanoutVerdict = iota // confined to one organization, or touches no LokaDB table
	FanoutAllowed                      // on the allow list: runs in every organization's database
	FanoutRun                          // runs in every organization's database (mode allow or log, a read under reject-writes)
	FanoutRefused                      // the client gets an error
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
	if f.Mode == FanoutAllow || f.Mode == FanoutLog || f.Mode == FanoutRejectWrites && !an.Write {
		return FanoutRun
	}
	return FanoutRefused
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
	msg := fmt.Sprintf("lokadb: the statement does not name the organization of %s rows, so it would run in every organization's database (fanout policy %q)",
		fanoutTable(an), f.Mode)
	hint := `Restrict it by the organization column, id or a parent id (= or IN), or add it to "allow" of this database's fanout policy in the proxy config.`
	detail := ""
	if g := an.GapText(); g != "" {
		detail = ", DETAIL = " + sqlString("why: "+g)
	}
	return ownsql.Tag(fmt.Sprintf("DO $lokadb$ BEGIN RAISE EXCEPTION USING ERRCODE = '0A000', MESSAGE = %s%s, HINT = %s; END $lokadb$", sqlString(msg), detail, sqlString(hint)))
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
