package proxy

import (
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// pipeWrite runs a pipeline of one organization with writes of 8b among its statements (block
// 9, from block 10's queue: JDBC executeBatch of INSERTs, pgx.Batch) as Postgres runs a
// pipeline: one transaction up to its Sync (or the client's open one), an error rolls all of
// it back. The cloud: one round trip for the pause, every statement's check and every write's
// refs rows (writeInTx's); the organization's database: one exchange with all the statements,
// each write rewritten (its key, its organization column) and Parsed by the proxy; the cloud
// again: what the writes' answers leave to do (a DELETE's refs rows, an upsert's unused ones, an
// INSERT … SELECT's), then the two commits, the organization's first. The client's answer
// streams as it comes, a write's once its statement has run; a failed commit comes as an error
// before the ReadyForQuery, as Postgres answers a pipeline whose implicit commit fails. Not
// taken here: an upsert that names its keys of one column (the cloud's answer has as many rows
// as the keys it added, which would not tell one write's rows from the next one's), an INSERT …
// SELECT or an unnest of a composite key (its refs rows' answer is a row per key). A composite
// key's VALUES rows go with tupleRefs (direct_composite.go): a row per row of the write and one.
// false: nothing ran.
func (s *session) pipeWrite(org string, units []*pipeUnit, dis []*directInfo, dc *directConn) bool {
	for _, u := range units {
		if w := u.di.write; w != nil && !w.del && (w.upsert && w.keysGiven() && w.keyCols == nil || w.keyCols != nil && w.sel) {
			return false
		}
	}
	implicit := s.direct.tx == nil
	if implicit {
		if err := s.probeDirect(dc); err != nil {
			s.dropDirect(dc)
			var err2 error
			if dc, err2 = s.directConnFor(org); err2 != nil {
				s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err2)
				return false
			}
		}
		s.setTx(&directTx{begin: "BEGIN", implicit: true})
	}
	d := s.direct.tx
	notTaken := func(why string) bool { // nothing ran
		switch {
		case implicit:
			s.setTx(nil)
			return false
		case d.org == "":
			return false // a deferred transaction stays deferred
		}
		s.refuseInTx(why)
		return true
	}
	// each write's refs rows in the cloud, after the checks: one row (a count) for the keys an
	// INSERT names, one per row for the keys the cloud gives, a composite key's one more than its
	// rows (tupleRefs: the count, then each row's key)
	keys := make([][]string, len(units))
	added := make([][]string, len(units)) // a composite key's upsert: the refs rows tupleRefs added
	rowsOf := make([]int, len(units))
	var extra []string
	for i, u := range units {
		w := u.di.write
		if w == nil || w.del || w.sel {
			continue
		}
		t := w.table
		kind, typ := s.meta.keyKind(t.Refs), t.ColType[tableKey(t)]
		if w.keyCols != nil { // a composite key's rows, the defaults of its columns not named in place
			lits, ok := w.rowLits(u.p.values, org)
			if !ok {
				return notTaken("an INSERT whose key is an expression")
			}
			rowsOf[i] = len(w.rows) + 1
			extra = append(extra, w.tupleRefs(lits, qrel(t.Refs), org))
			continue
		}
		if w.keysGiven() {
			var ok bool
			if keys[i], ok = w.givenKeys(u.p.values, kind); !ok {
				return notTaken("an INSERT whose key is an expression")
			}
			rowsOf[i] = 1
			extra = append(extra, givenRefs(w.table, w.lits(keys[i], kind, typ), w.refsCols(), qrel(t.Refs), org, false))
			continue
		}
		n := len(w.rows)
		if w.unnest != nil {
			var ok bool
			if n, ok = w.unnestRows(u.p.values); !ok {
				return notTaken("an INSERT … SELECT * FROM unnest(…) whose arrays cannot be read")
			}
		}
		rowsOf[i] = n
		extra = append(extra, "WITH lokadb_k AS (SELECT "+t.KeyDefault+" AS id FROM generate_series(1, "+itoa(n)+")), lokadb_r AS (INSERT INTO "+qrel(t.Refs)+
			" (id, tenant) SELECT id, "+t.orgLit(org)+" FROM lokadb_k) SELECT id::text FROM lokadb_k")
	}
	rows, ok, sent := s.txGateAll(org, dis, dc, extra...)
	if !ok {
		if implicit {
			s.setTx(nil)
		}
		return sent
	}
	dc = d.dc
	d.cloudWork = true // the writes' refs rows: the commits in order, the organization's first
	for _, u := range units {
		if w := u.di.write; w != nil && w.del {
			d.refsDeleted = true
		} else if w != nil {
			d.refsAdded = true
		}
	}
	// end: the pipeline failed (the client has what came before and the error): both sides
	// roll back (an implicit transaction) or the client's transaction is aborted
	end := func(e *pgproto3.ErrorResponse) bool {
		if e != nil {
			s.toClient(e)
		}
		if implicit {
			s.rollbackTx()
		} else {
			d.aborted = true
		}
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
		return true
	}
	at := 0
	sqls := make([]string, len(units))
	for i, u := range units {
		w := u.di.write
		if w == nil {
			continue
		}
		switch {
		case w.del, w.sel:
		case w.keyCols != nil:
			if at+rowsOf[i] > len(rows) {
				return end(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			}
			none, ks, add, read := w.tupleAnswer(rows[at:at+rowsOf[i]], len(w.rows))
			if !read {
				return end(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			}
			if !none {
				return end(otherOrgKey(w.table))
			}
			keys[i], added[i] = ks, add
		case w.keysGiven():
			if at >= len(rows) || len(rows[at]) != 1 || rows[at][0] != "0" {
				return end(otherOrgKey(w.table))
			}
		default:
			if at+rowsOf[i] > len(rows) {
				return end(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			}
			for _, r := range rows[at : at+rowsOf[i]] {
				keys[i] = append(keys[i], r[0])
			}
		}
		at += rowsOf[i]
		sql, err := w.rewrite(keys[i], org)
		if err != nil {
			return end(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()})
		}
		sqls[i] = sql
	}
	if d.first == "" {
		d.setFirst(units[0].st.sql)
	}
	for _, u := range units {
		s.traceDirect(u.st.sql, u.an, org)
	}

	// the organization's database: every message tagged with its unit and whether its answer is
	// the client's
	if len(dc.names)+len(units) > 1000 {
		s.runDirectQuiet(dc, &pgproto3.Query{String: "DEALLOCATE ALL"})
		dc.names = map[*stmtInfo]string{}
	}
	type tag struct {
		unit int
		own  bool      // an answer of the proxy's own
		st   *stmtInfo // a Parse of the proxy's own under a name: the statement it is for
		name string
	}
	var msgs []pgproto3.FrontendMessage
	var tags []tag
	add := func(m pgproto3.FrontendMessage, t tag) {
		msgs = append(msgs, m)
		tags = append(tags, t)
	}
	names := map[*stmtInfo]string{}
	wrote := false // a write's Parse replaced the organization's unnamed statement
	for i, u := range units {
		w := u.di.write
		if w != nil {
			wrote = true
			parsed := false
			for _, m := range u.msgs {
				if _, ok := m.(*pgproto3.Parse); ok {
					parsed = true
				}
			}
			// the rewritten text unnamed: its ParseComplete is the client's when the client
			// Parsed its statement in the unit
			add(&pgproto3.Parse{Query: sqls[i], ParameterOIDs: u.st.ptypes}, tag{unit: i, own: !parsed})
			for _, m := range u.msgs {
				switch m := m.(type) {
				case *pgproto3.Parse:
					continue
				case *pgproto3.Bind:
					add(w.binds([]pgproto3.FrontendMessage{m})[0], tag{unit: i})
				default:
					add(m, tag{unit: i})
				}
			}
			continue
		}
		if u.st.unnamed {
			first, _ := u.msgs[0].(*pgproto3.Parse)
			if first == nil && wrote { // the client's unnamed statement again, after a write's
				add(&pgproto3.Parse{Query: u.parse.Query, ParameterOIDs: u.parse.ParameterOIDs}, tag{unit: i, own: true})
			}
			for _, m := range u.msgs {
				add(m, tag{unit: i})
			}
			continue
		}
		name := dc.names[u.st]
		if name == "" {
			name = names[u.st]
		}
		if name == "" {
			dc.n++
			name = fmt.Sprintf("lokadb_d%d", dc.n)
			names[u.st] = name
			add(&pgproto3.Parse{Name: name, Query: u.st.sql, ParameterOIDs: u.st.ptypes}, tag{unit: i, own: true, st: u.st, name: name})
		}
		for _, m := range u.msgs {
			if b, ok := m.(*pgproto3.Bind); ok {
				c := *b
				c.PreparedStatement = name
				m = &c
			}
			add(m, tag{unit: i})
		}
	}
	add(&pgproto3.Sync{}, tag{unit: -1, own: true})
	if begin := s.beginFor(dc); begin != nil { // the transaction's BEGIN there, ahead of the statements (item 9 of block 10)
		msgs = withBegin(begin.String, msgs) // Parse, Bind, Execute of it: its error skips the statements
		own := tag{unit: -1, own: true}
		tags = append([]tag{own, own, own}, tags...)
	}
	for _, m := range msgs {
		dc.fe.Send(m)
	}
	s.directBusy.Store(dc)
	defer s.directBusy.Store(nil)
	lost := func(err error) bool {
		s.dropDirect(dc)
		d.dc = nil
		s.log.Warn("organization's database connection lost in a pipeline: both sides roll back", "org", org, "err", err)
		e := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + org + "'s database was lost: " + err.Error()}
		if implicit {
			s.cloudRun("ROLLBACK")
			s.endTx()
			s.toClient(e)
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return true
		}
		s.failTx(e)
		return true
	}
	if err := dc.fe.Flush(); err != nil {
		return lost(err)
	}
	// a write's answer is held up to its statement's end: a DELETE's keys and an upsert's or an
	// INSERT … SELECT's added columns come out of it
	deleted := make([][]string, len(units))
	inserted := make([][]string, len(units))
	var held []pgproto3.BackendMessage
	released := -1 // the write whose statement has answered: what follows (a Close) goes on at once
	release := func(i int) *pgproto3.ErrorResponse {
		released = i
		w := units[i].di.write
		out := held
		held = nil
		if w.upsert || w.sel {
			var err error
			kind := s.meta.keyKind(w.table.Refs)
			if out, inserted[i], err = w.split(out, false, upsertBinary(units[i].msgs), kind); err != nil {
				return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()}
			}
		}
		for _, m := range out {
			s.toClient(m)
		}
		return nil
	}
	next := 0 // msgs[next]: the message the next answer is for
	var failed *pgproto3.ErrorResponse
	var orgTx byte
	for orgTx == 0 {
		m, err := dc.fe.Receive()
		if err != nil {
			return lost(err)
		}
		switch m := m.(type) {
		case *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
			continue // the organization's server, not the client's session
		case *pgproto3.ReadyForQuery:
			orgTx = m.TxStatus
			dc.used = time.Now()
			continue
		case *pgproto3.NoticeResponse:
			if held != nil {
				held = append(held, copyBackend(m))
			} else {
				s.toClient(m)
			}
			continue
		case *pgproto3.ErrorResponse:
			r := *m
			failed = &r
			dc.markEvict(m.Code) // the pool (direct_pool.go)
			if next < len(tags) && tags[next].unit >= 0 && units[tags[next].unit].di.write != nil {
				for _, h := range held { // what the write answered before it failed
					s.toClient(h)
				}
				held = nil
			}
			s.toClient(&r)
			next = len(msgs) - 1 // the server skips to the Sync
			continue
		}
		if next >= len(tags) {
			continue
		}
		t := tags[next]
		if _, row := m.(*pgproto3.DataRow); !row {
			next++
		}
		if t.own {
			if t.st != nil {
				dc.names[t.st] = t.name
			}
			continue
		}
		w := units[t.unit].di.write
		if w == nil || t.unit == released {
			s.toClient(m)
			continue
		}
		switch m := m.(type) {
		case *pgproto3.DataRow:
			if w.del {
				if len(m.Values) > 0 {
					deleted[t.unit] = append(deleted[t.unit], string(m.Values[0]))
				}
				continue
			}
		case *pgproto3.RowDescription:
			if w.del { // the keys the proxy asked for: the client's DELETE returns nothing
				held = append(held, &pgproto3.NoData{})
				continue
			}
		}
		held = append(held, copyBackend(m))
		switch m.(type) {
		case *pgproto3.CommandComplete, *pgproto3.EmptyQueryResponse:
			if e := release(t.unit); e != nil && failed == nil {
				failed = e
				s.toClient(e)
			}
		}
	}
	if failed != nil || orgTx == 'E' {
		return end(nil)
	}

	// the cloud's part after the organization's statements, in its open transaction
	var post []string
	var selChecks int
	for i, u := range units {
		w := u.di.write
		if w == nil {
			continue
		}
		t := w.table
		kind, typ := s.meta.keyKind(t.Refs), t.ColType[tableKey(t)]
		refs := qrel(t.Refs)
		switch {
		case w.del && len(deleted[i]) > 0: // the cloud's FKs onto __refs act here
			post = append(post, w.refsDelete(refs, deleted[i], kind, typ))
		case w.sel && len(inserted[i]) > 0: // the keys the rows named: none another organization's
			post = append(post, givenRefs(w.table, w.lits(inserted[i], kind, typ), w.refsCols(), refs, org, false))
			selChecks++
		case w.upsert: // the keys the cloud gave rows that were updated or skipped leave __refs
			given := keys[i]
			if w.keyCols != nil {
				given = added[i]
			}
			if unused := without(given, inserted[i]); len(unused) > 0 {
				post = append(post, w.refsDelete(refs, unused, kind, typ))
			}
		}
	}
	if len(post) > 0 {
		c := s.cloudRun(post...)
		if c.err != nil {
			e := c.resp
			if e == nil {
				e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + c.err.Error()}
			}
			return end(e)
		}
		if len(c.rows) != selChecks {
			return end(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud's refs check answered no row"})
		}
		for _, r := range c.rows {
			if len(r) != 1 || r[0] != "0" {
				for _, u := range units {
					if u.di.write != nil && u.di.write.sel {
						return end(otherOrgKey(u.di.write.table))
					}
				}
			}
		}
	}
	if implicit {
		if e := s.commitQuiet(); e != nil {
			s.toClient(e)
		}
	}
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
	return true
}
