package proxy

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// COPY … FROM STDIN on the direct path (block 2 of 8b.2, form 2; coordinator's decision of
// 25.10). The rows of a COPY may be any organization's and are not known before they come, so it
// goes direct only when the application named the organization (lokadb.tenant): the rows are
// then that organization's, and one of another is refused by the organization's CHECK, which
// fails the whole COPY. The order is 8b's:
//
//  1. the cloud: BEGIN, the organization's pause lock, the gate (INSERT on the table); the
//     transaction stays open;
//  2. the organization's database, in a transaction: a temporary table of the COPY's columns
//     with a row number (copyStage), the client's stream copied into it;
//  3. at CopyDone: no key in the columns, the cloud gives one per row and writes their refs rows,
//     and the rows go from the stage into the table with them; with the key, the rows go in
//     RETURNING it and the refs rows follow in the cloud (a key of another organization: 23505);
//  4. the organization commits, then the cloud; the client gets COPY n. Any error before rolls
//     both back, no refs row stays.
//
// The commits are the end of an autocommit COPY's own transactions; one in a direct transaction
// (step 4 of 8b.2) leaves them to the transaction's COMMIT (directCopy.own).

// copyStage, copyRowNum: the organization's temporary table of a direct COPY, and its row
// number column (an identity, filled in the order of the stream).
const (
	copyStage  = "__lokadb_dcopy"
	copyRowNum = "__lokadb_n"
)

// directCopy: a COPY FROM STDIN streaming into the organization's stage.
type directCopy struct {
	dc        *directConn
	org       string
	table     *Table
	cols      []string // the COPY's columns, the table's when it names none
	keyAt     int      // the key's place in cols, -1 for none (the cloud gives the keys)
	addTenant bool     // the organization column is not among cols: the proxy adds it
	own       bool     // its own transactions on both sides (autocommit): it commits them
	sql       string   // the client's COPY, for the route log
	an        *Analysis
	unflushed int   // bytes of CopyData sent to the organization since the last flush
	lost      error // the organization's connection broke during the stream (copyLost)
	// w: a composite key's columns (directWrite.keyCols, keyAts by cols), nil for a key of one
	// column: the cloud gives the defaults of those the COPY does not name (genKeys), the rows go
	// in RETURNING the key and their refs rows follow (refsAdd)
	w *directWrite
}

// copyFlushAt: CopyData is flushed to the organization's database in pieces of about this size.
const copyFlushAt = 256 << 10

// copyShape: a COPY … FROM STDIN of a LokaDB table that the direct path takes, nil for the
// cloud's way: no file, no program, no query; a table without row triggers of the cloud on
// INSERT, a statement trigger, identity or generated columns or columns of the application's
// types; every column with a default of the cloud named but the key (which the cloud computes,
// KeyDefault) and the organization column.
func copyShape(sql string, m *Meta) (*Table, []string, int, bool, *pg.Node) {
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return nil, nil, 0, false, nil
	}
	top := tree.Stmts[0].Stmt
	cs := top.GetCopyStmt()
	if cs == nil || !cs.IsFrom || cs.IsProgram || cs.Filename != "" || cs.Query != nil || cs.Relation == nil {
		return nil, nil, 0, false, nil
	}
	t := m.table(cs.Relation)
	if t == nil || m.rowTrigger(t.Name, trigInsert) || m.StmtTriggers[t.Name] || t.Identity || t.Generated || t.UserTypes {
		return nil, nil, 0, false, nil
	}
	cols := t.Cols
	if len(cs.Attlist) > 0 {
		cols = nil
		for _, a := range cs.Attlist {
			cols = append(cols, a.GetString_().GetSval())
		}
	}
	key := tableKey(t)
	keyAt, addTenant := -1, true
	named := map[string]bool{}
	for i, c := range cols {
		named[c] = true
		switch c {
		case key:
			keyAt = i
		case t.TenantCol:
			addTenant = false
		}
	}
	kc, composite := tupleCols(t.Key)
	for c := range t.Defaults {
		if !named[c] && c != key && c != t.TenantCol && !slices.Contains(kc, c) {
			return nil, nil, 0, false, nil // its default is the cloud's: the organization's table has none
		}
	}
	for _, c := range kc { // a composite key: each column named, the organization column, or one with a default
		if !named[c] && c != t.TenantCol && t.KeyDefaults[c] == "" {
			return nil, nil, 0, false, nil
		}
	}
	if !composite && keyAt < 0 && t.KeyDefault == "" {
		return nil, nil, 0, false, nil
	}
	return t, cols, keyAt, addTenant, top
}

// directCopyStart: a COPY FROM STDIN of one table of the organization the application named, in
// fdw+direct, outside a transaction: the gate in the cloud, the stage in the organization's
// database, then the client gets the organization's CopyInResponse and its stream goes there
// (copyMsg). false: nothing ran anywhere, the COPY goes the cloud's way.
func (s *session) directCopyStart(sql string, an *Analysis) bool {
	d := s.direct.tx // a deferred or direct transaction (step 4): its own BEGIN and COMMIT
	if s.direct.off != "" || s.meta == nil || len(s.meta.Profiles) == 0 || s.meta.SchemasUsed || d == nil && s.txStatus() != 'I' || d != nil && d.aborted {
		return false
	}
	if !an.Lokadb || an.Forbidden != "" || an.rejection() != "" || len(an.Stmts) != 1 {
		return false
	}
	org := s.tenant.Value()
	if !realOrg(org) || org == TenantAll {
		return false // no organization named, or every one: the cloud's way
	}
	t, cols, keyAt, addTenant, top := copyShape(sql, s.meta)
	if t == nil || !directProfile(s.meta.Profiles[s.meta.orgNum(org)]) || s.customersApp(org) == "" || s.meta.Foreign[t.Name][org] == "" {
		return false
	}
	if d != nil && d.org != "" && d.org != org {
		return false // another organization's: the direct transaction refuses it
	}
	var kw *directWrite // a composite key's columns, their places in the COPY's
	if w := (&directWrite{table: t}); !w.composite(t, s.meta) {
		return false // the kinds of the key's columns unknown
	} else if w.keyCols != nil {
		for i, c := range cols {
			if j := slices.Index(w.keyCols, c); j >= 0 {
				w.keyAts[j] = i
			}
		}
		kw = w
	}
	var dc *directConn
	if d != nil && d.org != "" {
		dc = d.dc
	} else {
		var err error
		if dc, err = s.directConnFor(org); err != nil {
			s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err)
			return false
		}
		if err := s.probeDirect(dc); err != nil {
			s.dropDirect(dc)
			if dc, err = s.directConnFor(org); err != nil {
				return false
			}
		}
	}
	own := d == nil
	if !own {
		d.cloudWork = true // its refs rows in the cloud's open transaction
	}
	di := &directInfo{tables: []string{t.Name}, write: &directWrite{table: t}}
	rollback := func() {
		if own {
			s.cloudRun("ROLLBACK")
		}
	}
	if own {
		c := s.cloudRunStmts(prepared("BEGIN"), prepared(cloudBusy), s.lockStmt("SELECT FROM pg_advisory_xact_lock_shared", org), s.gateCheck(di, org))
		if c.err != nil || len(c.rows) < 2 || len(c.rows[1]) != 3 || !s.gateAllows(c.rows[1], org, di) {
			rollback()
			if r := c.resp; r != nil && (strings.HasPrefix(r.Code, "57") || r.Code == "55P03" || r.Code == "40P01") {
				s.refuseDirect(r)
				return true
			}
			return false
		}
	} else {
		if _, ok, sent := s.txGate(org, di, dc); !ok {
			return sent
		}
		d.setFirst(sql)
		dc = d.dc
	}
	cp := &directCopy{dc: dc, org: org, table: t, cols: cols, keyAt: keyAt, addTenant: addTenant, own: own, sql: sql, an: an, w: kw}
	stage, copySQL, err := cp.stageSQL(top)
	if err != nil {
		rollback()
		if !own {
			s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()})
			return true
		}
		return false
	}
	if own {
		stage = "BEGIN; " + stage
	}
	// the stage: a failure here (no TEMP privilege, a table the organization lacks) leaves the
	// client unanswered yet: an autocommit COPY still goes the cloud's way; one in a direct
	// transaction is refused (its transaction there is aborted), and its form goes the cloud's
	// way from then on
	if _, failed, err := s.orgSimple(dc, stage); err != nil || failed != nil {
		switch {
		case err != nil && own:
			rollback()
			s.directLost(org, err)
			return true
		case err != nil:
			d.dc = nil
			s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + org + "'s database was lost: " + err.Error()})
			return true
		case own:
			_, _, _ = s.orgSimple(dc, "ROLLBACK")
			rollback()
			s.log.Warn("direct COPY: the organization's stage failed: through the cloud", "org", org, "table", t.Name, "err", failed.Message)
			return false
		}
		s.refuseInTx("a COPY whose stage the organization's database refused (" + failed.Message + ")")
		return true
	}
	dc.fe.Send(&pgproto3.Query{String: copySQL})
	if err := dc.fe.Flush(); err != nil {
		cp.lostAt(s, err)
		return true
	}
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			cp.lostAt(s, err)
			return true
		}
		switch m := m.(type) {
		case *pgproto3.CopyInResponse:
			s.traceDirect(sql, an, org)
			s.direct.copy = cp
			s.directBusy.Store(dc)
			r := *m
			r.ColumnFormatCodes = append([]uint16{}, m.ColumnFormatCodes...)
			s.toClient(&r)
			s.flushClient()
			return true
		case *pgproto3.ErrorResponse: // the client's options (FORMAT, DELIMITER): the organization's answer is Postgres's
			e := *m
			s.drainRFQ(dc)
			cp.fail(s, &e)
			return true
		}
	}
}

// fail ends a direct COPY with e: an autocommit one rolls both sides back; one in a direct
// transaction aborts it (the client's ROLLBACK undoes both).
func (cp *directCopy) fail(s *session, e *pgproto3.ErrorResponse) {
	if !cp.own {
		s.failTx(e)
		return
	}
	_, _, _ = s.orgSimple(cp.dc, "ROLLBACK")
	s.cloudRun("ROLLBACK")
	s.toClient(e)
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

// lostAt: the organization's connection broke before the client's stream: an autocommit COPY's
// cloud transaction rolls back; a direct transaction ends with 08006.
func (cp *directCopy) lostAt(s *session, err error) {
	if cp.own {
		s.cloudRun("ROLLBACK")
		s.directLost(cp.org, err)
		return
	}
	s.dropDirect(cp.dc)
	s.direct.tx.dc = nil
	s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + cp.org + "'s database was lost: " + err.Error()})
}

// stageSQL: the organization's temporary table of the COPY's columns with the row number (one
// of an earlier COPY of the same transaction dropped first), and the client's COPY aimed at it
// with the columns named (the row number is not among them).
func (cp *directCopy) stageSQL(top *pg.Node) (string, string, error) {
	var qcols []string
	for _, c := range cp.cols {
		qcols = append(qcols, quoteIdent(c))
	}
	stage := "DROP TABLE IF EXISTS pg_temp." + copyStage + "; CREATE TEMP TABLE " + copyStage + " ON COMMIT DROP AS SELECT " + strings.Join(qcols, ", ") + " FROM " + quoteIdent(cp.table.Name) +
		" WITH NO DATA; ALTER TABLE " + copyStage + " ADD COLUMN " + copyRowNum + " bigint GENERATED ALWAYS AS IDENTITY"
	cs := top.GetCopyStmt()
	cs.Relation = &pg.RangeVar{Relname: copyStage, Inh: true, Relpersistence: "p"}
	cs.Attlist = nil
	for _, c := range cp.cols {
		cs.Attlist = append(cs.Attlist, strNode(c))
	}
	copySQL, err := deparseStmt(top)
	return stage, copySQL, err
}

// copyMsg: a message of the client's stream while a direct COPY runs: CopyData goes to the
// organization's database, CopyDone and CopyFail end it (copyEnd).
func (s *session) copyMsg(msg pgproto3.FrontendMessage) {
	cp := s.direct.copy
	if cp.lost != nil { // the rest of the stream is dropped; its end gets the error
		switch msg.(type) {
		case *pgproto3.CopyData:
		default:
			s.direct.copy = nil
			e := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + cp.org + "'s database was lost during COPY: " + cp.lost.Error()}
			if !cp.own {
				s.failTx(e)
				return
			}
			s.toClient(e)
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
		}
		return
	}
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		cp.dc.fe.Send(m)
		cp.unflushed += len(m.Data) + 5
		if cp.unflushed >= copyFlushAt {
			cp.unflushed = 0
			if err := cp.dc.fe.Flush(); err != nil {
				s.copyLost(err)
			}
		}
	case *pgproto3.CopyDone, *pgproto3.CopyFail:
		s.copyEnd(msg)
	default: // anything else ends copy-in mode on the server too (a protocol error there)
		s.copyEnd(&pgproto3.CopyFail{Message: fmt.Sprintf("lokadb: %T during COPY FROM STDIN", msg)})
	}
}

// copyLost: the organization's connection broke during the stream: the cloud rolls back, the
// client gets 08006 once its stream ends (the rest of it is dropped).
func (s *session) copyLost(err error) {
	cp := s.direct.copy
	cp.lost = err
	s.directBusy.Store(nil)
	if cp.own {
		s.cloudRun("ROLLBACK")
	} else {
		s.direct.tx.dc = nil
	}
	s.dropDirect(cp.dc)
}

// copyEnd: the client's CopyDone or CopyFail. The organization's COPY ends; on success the rows
// go from the stage into the table with their keys and refs rows (above), then the commits.
func (s *session) copyEnd(end pgproto3.FrontendMessage) {
	cp := s.direct.copy
	s.direct.copy = nil
	defer s.directBusy.Store(nil)
	fail := func(e *pgproto3.ErrorResponse) { cp.fail(s, e) }
	cp.dc.fe.Send(end)
	if err := cp.dc.fe.Flush(); err != nil {
		cp.lostAt(s, err)
		return
	}
	answer, failed, err := s.orgRead(cp.dc)
	if err != nil {
		cp.lostAt(s, err)
		return
	}
	if failed != nil {
		fail(failed)
		return
	}
	n := 0
	for _, m := range answer {
		if cc, ok := m.(*pgproto3.CommandComplete); ok {
			n, _ = strconv.Atoi(strings.TrimPrefix(string(cc.CommandTag), "COPY "))
		}
	}
	t := cp.table
	refs := qrel(t.Refs)
	key := tableKey(t)
	kind, typ := s.meta.keyKind(t.Refs), t.ColType[key]
	if n > 0 && cp.w != nil {
		var gen []string
		if len(cp.w.genCols()) > 0 {
			c := s.cloudRun(cp.w.genKeys(n))
			if c.err != nil || len(c.rows) != n {
				fail(errorOf(c, "the cloud gave no keys for the COPY"))
				return
			}
			for _, r := range c.rows {
				gen = append(gen, tupleKey(r))
			}
		}
		keys, failed, err := s.orgKeys(cp.dc, cp.insertSQL(gen, kind, typ))
		if err != nil {
			cp.lostAt(s, err)
			return
		}
		if failed != nil {
			fail(failed)
			return
		}
		c := s.cloudRun(cp.w.refsAdd(keys, kind, typ, refs, cp.org))
		if c.err != nil || !othersNone(c.rows) {
			e := c.resp
			if e == nil {
				e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint \"" + t.Refs + "_pkey\": a key of the COPY belongs to another organization"}
			}
			fail(e)
			return
		}
	} else if n > 0 {
		var keys []string
		if cp.keyAt < 0 { // the cloud gives one key per row, with its refs row
			c := s.cloudRun(fmt.Sprintf("WITH lokadb_k AS (SELECT %s AS id FROM generate_series(1, %d)), lokadb_r AS (INSERT INTO %s (id, tenant) SELECT id, %s FROM lokadb_k) SELECT id::text FROM lokadb_k",
				t.KeyDefault, n, refs, t.orgLit(cp.org)))
			if c.err != nil || len(c.rows) != n {
				fail(errorOf(c, "the cloud gave no keys for the COPY"))
				return
			}
			for _, r := range c.rows {
				keys = append(keys, r[0])
			}
		}
		rows, failed, err := s.orgSimple(cp.dc, cp.insertSQL(keys, kind, typ))
		if err != nil {
			cp.lostAt(s, err)
			return
		}
		if failed != nil { // an FK, the organization's CHECK (a row of another organization), a key twice
			fail(failed)
			return
		}
		if cp.keyAt >= 0 { // the keys the rows named: their refs rows, none another organization's
			for _, r := range rows {
				keys = append(keys, r)
			}
			c := s.cloudRun(givenRefs(t, keyLits(keys, kind, typ), "id", refs, cp.org, false))
			if c.err != nil || len(c.rows) != 1 || len(c.rows[0]) != 1 || c.rows[0][0] != "0" {
				e := c.resp
				if e == nil {
					e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint \"" + t.Refs + "_pkey\": a key of the COPY belongs to another organization"}
				}
				fail(e)
				return
			}
		}
	}
	if cp.own {
		if _, failed, err := s.orgSimple(cp.dc, "COMMIT"); err != nil || failed != nil {
			e := failed
			if err != nil { // the outcome unknown (review PR-1): the COPY's refs rows stay, a superset
				if c := s.cloudRun("COMMIT"); c.err != nil {
					s.log.Warn("a direct COPY of unknown outcome: its refs rows' commit failed too (refs-diff / refs-sync)", "org", cp.org, "err", c.err)
				}
				e = errUnknown(cp.org, err)
			} else {
				s.cloudRun("ROLLBACK")
			}
			s.toClient(e)
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return
		}
		if c := s.cloudRun("COMMIT"); c.err != nil {
			s.log.Warn("a direct COPY committed in the organization's database, its refs did not in the cloud: refs-diff / refs-sync repair them", "org", cp.org, "table", t.Name, "err", c.err)
			s.toClient(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "40003",
				Message: "lokadb: organization " + cp.org + "'s rows are written, the cloud's commit of their refs failed: " + c.err.Error()})
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return
		}
	}
	s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("COPY " + strconv.Itoa(n))})
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

// insertSQL: the rows from the stage into the table, in the stream's order: with keys (the
// cloud's, one per row by the row number), else RETURNING the keys the rows name; the
// organization column added as a constant when the COPY does not name it.
func (cp *directCopy) insertSQL(keys []string, kind, typ string) string {
	key := tableKey(cp.table)
	var into, from []string
	for _, c := range cp.cols {
		into = append(into, quoteIdent(c))
		from = append(from, "s."+quoteIdent(c))
	}
	if cp.addTenant {
		into = append(into, quoteIdent(cp.table.TenantCol))
		from = append(from, sqlString(cp.org))
	}
	q := "INSERT INTO " + quoteIdent(cp.table.Name) + " ("
	if w := cp.w; w != nil { // a composite key: the columns the cloud's defaults gave (keys: a row of them per row), RETURNING its columns
		var ret, arrs, ks []string
		for _, c := range w.keyCols {
			ret = append(ret, quoteIdent(c))
		}
		for j, c := range w.genCols() {
			into = append(into, quoteIdent(c))
			from = append(from, fmt.Sprintf("k.k%d", j+1))
			ks = append(ks, fmt.Sprintf("k%d", j+1))
		}
		arrs = w.genArrays(keys)
		if len(arrs) == 0 {
			return q + strings.Join(into, ", ") + ") SELECT " + strings.Join(from, ", ") + " FROM " + copyStage + " s ORDER BY s." + copyRowNum + " RETURNING " + strings.Join(ret, ", ")
		}
		return q + strings.Join(into, ", ") + ") SELECT " + strings.Join(from, ", ") + " FROM " + copyStage + " s JOIN unnest(" + strings.Join(arrs, ", ") +
			") WITH ORDINALITY AS k(" + strings.Join(ks, ", ") + ", n) ON k.n = s." + copyRowNum + " RETURNING " + strings.Join(ret, ", ")
	}
	if cp.keyAt < 0 {
		into = append(into, quoteIdent(key))
		from = append(from, "k.id")
		return q + strings.Join(into, ", ") + ") SELECT " + strings.Join(from, ", ") + " FROM " + copyStage + " s JOIN unnest(ARRAY[" +
			strings.Join(keyLits(keys, kind, typ), ", ") + "]::" + typ + "[]) WITH ORDINALITY AS k(id, n) ON k.n = s." + copyRowNum
	}
	return q + strings.Join(into, ", ") + ") SELECT " + strings.Join(from, ", ") + " FROM " + copyStage + " s ORDER BY s." + copyRowNum + " RETURNING " + quoteIdent(key)
}

// errorOf: the cloud's error of c, or one of the proxy's with msg.
func errorOf(c *capture, msg string) *pgproto3.ErrorResponse {
	if c.resp != nil {
		return c.resp
	}
	if c.err != nil {
		msg += ": " + c.err.Error()
	}
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + msg}
}

// orgSimple runs sql (one or several statements) as a simple query on dc and reads up to its
// ReadyForQuery: the first column of each row, the first error, a broken connection.
func (s *session) orgSimple(dc *directConn, sql string) ([]string, *pgproto3.ErrorResponse, error) {
	if b := s.beginFor(dc); b != nil { // a direct transaction's BEGIN, not sent yet: in the same string (its error skips the rest)
		sql = b.String + "; " + sql
	}
	dc.fe.Send(&pgproto3.Query{String: sql})
	if err := dc.fe.Flush(); err != nil {
		s.dropDirect(dc)
		return nil, nil, err
	}
	answer, failed, err := s.orgRead(dc)
	var rows []string
	for _, m := range answer {
		if r, ok := m.(*pgproto3.DataRow); ok && len(r.Values) > 0 {
			rows = append(rows, string(r.Values[0]))
		}
	}
	return rows, failed, err
}

// orgRead reads dc's answer up to ReadyForQuery, its messages copied, and the first error.
func (s *session) orgRead(dc *directConn) ([]pgproto3.BackendMessage, *pgproto3.ErrorResponse, error) {
	var answer []pgproto3.BackendMessage
	var failed *pgproto3.ErrorResponse
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			s.dropDirect(dc)
			return nil, nil, err
		}
		switch m := m.(type) {
		case *pgproto3.ReadyForQuery:
			return answer, failed, nil
		case *pgproto3.ErrorResponse:
			if failed == nil {
				e := *m
				failed = &e
			}
			continue
		}
		answer = append(answer, copyBackend(m))
	}
}

// drainRFQ reads dc up to its ReadyForQuery (after an error the client gets on its own).
func (s *session) drainRFQ(dc *directConn) {
	_, _, _ = s.orgRead(dc)
}

// orgKeys runs sql on dc like orgSimple: each row's key, its columns joined (tupleKey) when it
// has several.
func (s *session) orgKeys(dc *directConn, sql string) ([]string, *pgproto3.ErrorResponse, error) {
	if b := s.beginFor(dc); b != nil { // a direct transaction's BEGIN, not sent yet (as orgSimple)
		sql = b.String + "; " + sql
	}
	dc.fe.Send(&pgproto3.Query{String: sql})
	if err := dc.fe.Flush(); err != nil {
		s.dropDirect(dc)
		return nil, nil, err
	}
	answer, failed, err := s.orgRead(dc)
	var keys []string
	for _, m := range answer {
		if r, ok := m.(*pgproto3.DataRow); ok && len(r.Values) > 0 {
			vals := make([]string, len(r.Values))
			for i, v := range r.Values {
				vals[i] = string(v)
			}
			if len(vals) == 1 {
				keys = append(keys, vals[0])
			} else {
				keys = append(keys, tupleKey(vals))
			}
		}
	}
	return keys, failed, err
}
