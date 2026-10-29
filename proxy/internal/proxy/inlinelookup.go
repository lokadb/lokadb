package proxy

import (
	"strconv"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The organization of a cold key found in the statement's own exchange (block 14's
// measurements, paths, 32 clients: a read whose second statement names a document by its id,
// SELECT count(*) FROM document_versions WHERE document_id = $1, waited for a lookup in
// documents__refs, an exchange of its own with the cloud, before set_config and the statement
// went). For the plainest such statement the proxy sends, in the same messages and ahead of the
// client's Bind, a statement of its own that reads the id's organization from the refs table
// and sets lokadb.org to it, with the pause lock: '-1' (no row anywhere) when the id is not
// there, '-2' for a row of no organization, as the lookup would have decided (resolveOrg): the
// statement runs under the same lokadb.org, closed by default all the same (R1). One statement
// per refs table, Parsed once per connection.
//
// Taken only when nothing else needs the organization before the statement runs: one value of
// one integer key and no other condition naming an organization, a read (no rewrite, no cursor,
// no event; a read of a moved organization goes through the parent, pruned to its partition by
// the policy, instead of aimed at its foreign partition), no direct path in the database (the routing to an
// organization's database needs it), not the cooperative mode, organizations numbered by
// integers, and the client's Bind followed by its Execute to the end (MaxRows 0: no portal left
// to go on later under another lokadb.org). Everything else looks up as before.

// inlineLookup: the messages that set lokadb.org (and take the pause lock) from b's statement's
// one cold key within the exchange, and true; false: the statement looks up as before.
func (s *session) inlineLookup(an *Analysis, vals valuesFunc, batch []pgproto3.FrontendMessage, idx int, b *pgproto3.Bind, tx byte) ([]outMsg, bool) {
	refs, id, ok := s.inlineKey(an, vals, batch, idx, b, tx)
	if !ok {
		return nil, false
	}
	s.freshOrgs(an)
	if _, known := s.orgOf(refs, id); known {
		return nil, false // a warm key: set as before, with the partition key's condition
	}
	_, lock := s.orgLock(an, "?")
	ks := s.inlineStmt(refs, lock)
	out := []outMsg{}
	e0 := s.svcEpoch.Load()
	if !ks.ready.Load() || ks.epoch != e0 {
		ks.ready.Store(false)
		ks.epoch = e0
		out = append(out,
			outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: ks.name}, mode: mDrop},
			outMsg{msg: &pgproto3.Parse{Name: ks.name, Query: s.inlineSQL(refs, lock)}, mode: mDrop, onOK: func() {
				if s.svcEpoch.Load() == e0 {
					ks.ready.Store(true)
				}
			}})
	}
	return append(out,
		outMsg{msg: &pgproto3.Close{ObjectType: 'P', Name: "lokadb_ilp"}, mode: mDrop},
		outMsg{msg: &pgproto3.Bind{DestinationPortal: "lokadb_ilp", PreparedStatement: ks.name, Parameters: [][]byte{[]byte(id)}}, mode: mDrop},
		outMsg{msg: &pgproto3.Execute{Portal: "lokadb_ilp"}, mode: mDrop}), true
}

// inlineKey: b's statement may take its organization within the exchange (inlineLookup): its
// one key's refs table and value. No side effect: the batch's first pass asks it too (a
// statement taken so needs no lookup, and no wait for the pending answers before it).
func (s *session) inlineKey(an *Analysis, vals valuesFunc, batch []pgproto3.FrontendMessage, idx int, b *pgproto3.Bind, tx byte) (refs, id string, ok bool) {
	m := s.meta
	if m == nil || tx == 'E' || !an.Lokadb || an.Write || an.TxControl || an.Utility || an.ResetsOrg || an.MovesOrg ||
		an.Rewritten != "" || len(an.Events) > 0 || len(an.Prelude) > 0 || len(an.Tuples) > 0 ||
		len(an.Stmts) > 1 || an.Unhinted || an.ParseError || m.anyDirect() || m.valueOrgs() {
		return "", "", false
	}
	// the client's Bind, then (past Describes of its portal) its Execute to the end
	follows := false
	for _, msg := range batch[idx+1:] {
		if d, ok := msg.(*pgproto3.Describe); ok && d.ObjectType == 'P' && d.Name == b.DestinationPortal {
			continue
		}
		e, ok := msg.(*pgproto3.Execute)
		follows = ok && e.Portal == b.DestinationPortal && e.MaxRows == 0
		break
	}
	if !follows {
		return "", "", false
	}
	// without the partition key's condition of a known organization (keycond.go) the planner
	// prunes at execution, not when it plans: a database with a table of many partitions (an
	// organization each, thousands) keeps the lookup and the condition
	for t := range m.Tables {
		if len(m.Local[t])+len(m.Foreign[t]) > inlineMaxParts {
			return "", "", false
		}
	}
	refs, id = "", ""
	for _, h := range an.Hints {
		r, ok := m.Lookups[h.Table][h.Column]
		if !ok {
			continue
		}
		if r == "" || refs != "" || len(h.Values) != 1 { // an organization column, or a second key
			return "", "", false
		}
		if _, keyMap := keyRefs(r); keyMap || m.keyKind(r) != kindInt || m.tupleKinds(r) != nil {
			return "", "", false
		}
		v := h.Values[0]
		v.Kind = kindInt
		xs, ok := vals(v)
		if !ok || len(xs) != 1 || xs[0] == nullValue {
			return "", "", false
		}
		if _, err := strconv.ParseInt(xs[0], 10, 64); err != nil { // past int8: found nowhere, as the lookup has it
			return "", "", false
		}
		refs, id = r, xs[0]
	}
	return refs, id, refs != ""
}

// inlineMaxParts: the partitions of organizations of a table up to which a read takes its
// organization within its exchange (inlineKey).
const inlineMaxParts = 32

// inlineStmt: the connection's statement of refs (with the pause lock or without).
func (s *session) inlineStmt(refs string, lock bool) *keyedStmt {
	k := refs
	if lock {
		k += "\x00lock"
	}
	ks := s.inlineStmts[k]
	if ks == nil {
		if s.inlineStmts == nil {
			s.inlineStmts = map[string]*keyedStmt{}
		}
		s.keyedN++
		ks = &keyedStmt{name: "lokadb_il" + strconv.Itoa(s.keyedN)}
		s.inlineStmts[k] = ks
	}
	return ks
}

// inlineSQL: refs's id $1's organization into lokadb.org ('-1' when it is not there, '-2' for no
// organization), and the pause lock of it (of every organization when it is none: orgLock).
func (s *session) inlineSQL(refs string, lock bool) string {
	tenant := "tenant"
	if tc := s.meta.cloud(refs); tc != "" { // a cloud table that knows the organization
		tenant = quoteIdent(tc)
	}
	sel := "set_config('lokadb.org', o, false)"
	if lock {
		sel += ", pg_advisory_xact_lock_shared(lokadb.org_lock(CASE WHEN o LIKE '-%' THEN NULL ELSE o::bigint END))"
	}
	return "SELECT " + sel + " FROM (SELECT coalesce((SELECT coalesce(" + tenant + "::text, " + sqlString(noOrg) + ") FROM " + qrel(refs) +
		" WHERE id = $1::bigint), " + sqlString(noRows) + ") AS o) lokadb_x"
}
