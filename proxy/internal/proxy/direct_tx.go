package proxy

import (
	"strconv"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// Transactions on the direct path, stage 8a.2 (tasks/direct-path.md, section 4; block 9's
// review of 25.10). A client's BEGIN is held back (deferred); the transaction's first
// statement decides where it goes: one the direct path takes for organization N opens a
// direct transaction (the cloud's transaction holding N's pause, the organization's database
// running its statements); any other sends the held BEGIN to the cloud, and the transaction is
// the cloud's to the end, as before.
//
// Two transaction statuses: txStatus() is always the cloud's (every decision about the cloud
// connection reads it: in a deferred transaction the cloud is still 'I'); clientTx() is what
// the client sees. The client's ReadyForQuery is built in clientRFQ alone.
//
// Inside a direct transaction of N: a statement the direct path takes for N runs in N's
// database (the gate's check and a write's refs rows in the cloud's open transaction); one on
// cloud tables alone runs in the cloud; one on LokaDB tables that needs the FDW (another
// organization, a form the direct path does not take) is refused with 0A000 and ends the
// transaction, and its first statement's fingerprint sends such transactions the cloud's way
// from then on (lokadb.direct_cloud_first: the coordinator's decision). Whitelisted SET goes
// to both sides; SAVEPOINT, RELEASE, ROLLBACK TO to both, the organization first; COMMIT
// commits the organization, then the cloud; ROLLBACK both.

// directTx: the session's deferred or direct transaction (nil: none).
type directTx struct {
	begin   string      // the client's BEGIN, as it wrote it
	org     string      // "": deferred, no statement yet; else the organization of a direct transaction
	dc      *directConn // the organization's connection (org != "")
	aborted bool        // an error ended it: 25P02 up to ROLLBACK
	errSent bool        // a batch's 25P02 went to the client, up to its Sync
	first   string      // the fingerprint of its first statement
	refused string      // why a statement was refused (lokadb.direct_cloud_first)
	sets    []directSet // session settings it ran on both sides, kept at COMMIT
	spSets  []spMark    // its savepoints, oldest first, with len(sets) at each (ROLLBACK TO drops what came after)
	tenant  []string    // SET LOCAL lokadb.tenant held with the BEGIN (the cooperative mode), sent after it
	// implicit: a pipeline's transaction up to its Sync (pipeWrite), not the client's: the client
	// sees the cloud's status
	implicit bool
	// beginPending: the organization's BEGIN is not sent yet: it goes in front of the first
	// statement there, in one exchange (item 9: beginFor); nothing ran there before it
	beginPending bool
	// checked: the gate's checks the transaction passed (their text): the pause lock it holds
	// keeps the profile and the partitions as they are to its end; a privilege revoked meanwhile
	// counts from the next transaction (item 9: one check per form, not per statement)
	checked map[string]bool
	// cloudWork: something of the transaction's own ran in the cloud (refs rows, a statement on
	// cloud tables, a COPY's refs): its commits go in order, the organization's first; without
	// it the cloud holds the pause and the checks alone, and both commit at once (item 9)
	cloudWork bool
	// refsAdded, refsDeleted: its writes added or deleted refs rows in the cloud: the side the
	// cloud takes when the organization's commit has an unknown outcome (unknownSide, PR-1)
	refsAdded, refsDeleted bool
}

// spMark: a savepoint of a direct transaction and how many session settings it had seen.
type spMark struct {
	name string
	sets int
}

// clientTx: the status the client is to see, given the cloud's.
func (s *session) clientTx(cloud byte) byte {
	d := s.direct.tx
	switch {
	case d == nil || d.implicit:
		return cloud
	case d.aborted || cloud == 'E':
		return 'E'
	}
	return 'T'
}

// clientRFQ: the client's ReadyForQuery, with the status it is to see (clientTx).
func (s *session) clientRFQ(cloud byte) *pgproto3.ReadyForQuery { return rfq(s.clientTx(cloud)) }

// relayRFQ: the cloud's ReadyForQuery as the relay goroutine passes it on: in a deferred
// transaction the cloud is outside one ('I') and the client inside ('T'); in an open direct
// transaction the cloud is in one too. The relay reads the atomic flag alone (the session's
// goroutine owns directTx).
func (s *session) relayRFQ(cloud byte) *pgproto3.ReadyForQuery {
	if s.direct.deferred.Load() && cloud == 'I' {
		if s.direct.deferredErr.Load() {
			return rfq('E')
		}
		return rfq('T')
	}
	return rfq(cloud)
}

// rfq: the one place the proxy builds a ReadyForQuery for the client (TestClientRFQOnly): a
// status taken from the cloud elsewhere would tell a client in a direct transaction that it is
// over.
func rfq(status byte) *pgproto3.ReadyForQuery { return &pgproto3.ReadyForQuery{TxStatus: status} }

// setTx sets the session's deferred or direct transaction and the relay's flag.
func (s *session) setTx(d *directTx) {
	if old := s.direct.tx; old != nil && old.dc != nil && d == nil {
		old.dc.path = "" // a SET search_path inside it may be rolled back with it (directConnFor sets it again)
	}
	if d != s.direct.tx {
		s.direct.deferredErr.Store(false)
	}
	s.direct.tx = d
	s.direct.deferred.Store(d != nil && d.org == "")
}

// txFailed marks d aborted after an error of the cloud in it: the cloud's transaction is 'E',
// or, deferred, the relay passed an error the cloud raised outside a transaction (PR-4).
func (s *session) txFailed(d *directTx) {
	if s.txStatus() == 'E' || d.org == "" && s.direct.deferredErr.Load() {
		d.aborted = true
	}
}

// cloudFirst: fingerprints of the first statements of transactions that met a statement the
// direct path could not take inside them, per database: those go the cloud's way (in memory
// for the proxy's life, lokadb.direct_cloud_first for every proxy). Bounded (maxCloudFirst,
// the oldest dropped).
type cloudFirst struct {
	mu    sync.Mutex
	set   map[string]bool
	order []string
}

const maxCloudFirst = 5000

func (c *cloudFirst) has(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.set[key]
}

// add: false when key was there already.
func (c *cloudFirst) add(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.set == nil {
		c.set = map[string]bool{}
	}
	if c.set[key] {
		return false
	}
	if len(c.order) >= maxCloudFirst {
		delete(c.set, c.order[0])
		c.order = c.order[1:]
	}
	c.set[key] = true
	c.order = append(c.order, key)
	return true
}

func fingerprint(sql string) string {
	fp, err := pg.Fingerprint(sql)
	if err != nil {
		return ""
	}
	return fp
}

// txStmt: sql's one transaction statement, nil when it is not exactly one.
// txWords: the first words of a transaction statement (pg.TransactionStmt): BEGIN, START
// TRANSACTION, COMMIT, END, ROLLBACK, ABORT, SAVEPOINT, RELEASE, PREPARE TRANSACTION.
var txWords = map[string]bool{"begin": true, "start": true, "commit": true, "end": true, "rollback": true, "abort": true,
	"savepoint": true, "release": true, "prepare": true}

// firstWord: the lower-case word sql starts with (leading space skipped); ok false when it
// starts with anything but letters (a comment, a parenthesis), which only a parse can read.
func firstWord(sql string) (w string, ok bool) {
	i := 0
	for i < len(sql) && (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n' || sql[i] == '\r') {
		i++
	}
	j := i
	for j < len(sql) && (sql[j] >= 'a' && sql[j] <= 'z' || sql[j] >= 'A' && sql[j] <= 'Z') {
		j++
	}
	if j == i {
		return "", false
	}
	return strings.ToLower(sql[i:j]), true
}

func txStmt(sql string) *pg.TransactionStmt {
	// a statement of the direct or deferred transaction is not a transaction statement almost
	// always, and parsing it to find out costs 30-130 µs and more (third review of performance,
	// perf3-27): its first word tells
	if w, ok := firstWord(sql); ok && !txWords[w] {
		return nil
	}
	tree, err := pg.Parse(sql)
	if err != nil || len(tree.Stmts) != 1 {
		return nil
	}
	return tree.Stmts[0].Stmt.GetTransactionStmt()
}

// directTxPossible: a BEGIN of this session may be deferred.
func (s *session) directTxPossible() bool {
	if s.direct.off != "" || s.meta == nil || s.tenantImpl || !s.srv.directDSNs(s.db) {
		return false
	}
	return s.meta.anyDirect() // counted once per metadata (review of performance, PF13)
}

// answer sends the client a command's tag (and a notice before it) and its ReadyForQuery.
func (s *session) answer(tag string, notice *pgproto3.NoticeResponse) {
	if notice != nil {
		s.toClient(notice)
	}
	s.toClient(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

// failTx answers the client's statement with an error that ends the direct transaction.
func (s *session) failTx(e *pgproto3.ErrorResponse) {
	if d := s.direct.tx; d != nil {
		d.aborted = true
	}
	s.toClient(e)
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

func abortedErr() *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "25P02", Message: "current transaction is aborted, commands ignored until end of transaction block"}
}

// refuseInTx: a statement the direct transaction cannot take (it needs the FDW, or the cloud
// for LokaDB tables): 0A000, the transaction ends, and transactions that start as this one did
// go the cloud's way from now on.
func (s *session) refuseInTx(why string) { s.refuseInTxRFQ(why, true) }

// refuseInTxRFQ: refuseInTx; rfq false: inside a pipeline, the ReadyForQuery comes at its Sync.
func (s *session) refuseInTxRFQ(why string, withRFQ bool) {
	d := s.direct.tx
	d.refused = why
	key := s.db + "/" + d.first
	if d.first != "" && s.srv.cloudFirst.add(key) {
		s.log.Warn("direct transaction refused a statement: transactions starting as this one go through the cloud from now on", "org", d.org, "why", why, "first", d.first)
	}
	e := &pgproto3.ErrorResponse{Severity: "ERROR", Code: "0A000",
		Message: "lokadb: this transaction runs in organization " + d.org + "'s database directly (profile fdw+direct); " + why +
			" cannot join it there. Roll the transaction back and run it again: transactions that start as this one did now go through the cloud"}
	if withRFQ {
		s.failTx(e)
		return
	}
	d.aborted, d.errSent = true, true
	s.toClient(e)
	s.flushClient()
}

// endTx: the direct transaction is over; its refusal, if any, is written down for every proxy.
func (s *session) endTx() {
	d := s.direct.tx
	s.setTx(nil)
	if d == nil || d.refused == "" || d.first == "" || s.meta == nil || !s.meta.CloudFirstTable {
		return
	}
	why := d.refused
	if len(why) > 200 {
		why = why[:200]
	}
	if c := s.cloudRun("INSERT INTO lokadb.direct_cloud_first (fingerprint, reason) SELECT " + sqlString(d.first) + ", " + sqlString(why) +
		" WHERE (SELECT count(*) FROM lokadb.direct_cloud_first) < " + strconv.Itoa(maxCloudFirstRows) + " ON CONFLICT DO NOTHING"); c.err == nil {
		s.cloudRun("NOTIFY lokadb_meta")
	}
}

// materialize sends the deferred BEGIN to the cloud: the transaction is the cloud's from now on.
func (s *session) materialize() {
	d := s.direct.tx
	s.setTx(nil)
	if c := s.cloudRun(append([]string{d.begin}, d.tenant...)...); c.err != nil {
		s.log.Warn("the deferred BEGIN failed in the cloud", "err", c.err)
	}
}

// cloudFirstFor: the transaction that starts with sql goes the cloud's way (it met a statement
// the direct path could not take before).
func (s *session) cloudFirstFor(sql string) (string, bool) {
	fp := fingerprint(sql)
	return fp, fp != "" && (s.srv.cloudFirst.has(s.db+"/"+fp) || s.meta.CloudFirst[fp])
}

// directTxQuery: a simple query and the deferred or direct transaction. true: handled (the
// client answered); false: the caller goes on (the cloud's way; a deferred BEGIN sent first).
func (s *session) directTxQuery(sql string, an *Analysis, wasUnsynced bool) bool {
	d := s.direct.tx
	if d == nil {
		ts := txStmt(sql)
		if ts == nil || (ts.Kind != pg.TransactionStmtKind_TRANS_STMT_BEGIN && ts.Kind != pg.TransactionStmtKind_TRANS_STMT_START) ||
			s.txStatus() != 'I' || wasUnsynced || !s.directTxPossible() {
			return false
		}
		s.setTx(&directTx{begin: sql})
		s.tenant.apply(an.Events, nil)
		s.answer("BEGIN", nil)
		return true
	}
	s.txFailed(d) // a statement the cloud ran failed: the whole transaction is aborted
	ts := txStmt(sql)
	if d.aborted {
		switch {
		case ts != nil && (ts.Kind == pg.TransactionStmtKind_TRANS_STMT_ROLLBACK || ts.Kind == pg.TransactionStmtKind_TRANS_STMT_COMMIT) && !ts.Chain:
			s.tenant.apply([]TxEvent{{Kind: 'X'}}, nil)
			s.rollbackTx()
			s.answer("ROLLBACK", nil)
		case ts != nil && ts.Kind == pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO && d.org != "":
			if s.savepointTx(sql, ts) {
				d.aborted = false
				s.tenant.apply(an.Events, nil)
				s.answer("ROLLBACK", nil)
			}
		default:
			s.failTx(abortedErr())
		}
		return true
	}
	if ts != nil {
		switch ts.Kind {
		case pg.TransactionStmtKind_TRANS_STMT_BEGIN, pg.TransactionStmtKind_TRANS_STMT_START:
			s.answer("BEGIN", &pgproto3.NoticeResponse{Severity: "WARNING", Code: "25001", Message: "there is already a transaction in progress"})
			return true
		case pg.TransactionStmtKind_TRANS_STMT_COMMIT, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
			if ts.Chain {
				break
			}
			commit := ts.Kind == pg.TransactionStmtKind_TRANS_STMT_COMMIT
			if d.org == "" { // nothing ran: the cloud never saw the BEGIN
				s.tenant.apply(an.Events, nil)
				s.setTx(nil)
				s.answer(map[bool]string{true: "COMMIT", false: "ROLLBACK"}[commit], nil)
				return true
			}
			s.tenant.apply(an.Events, nil)
			if commit {
				s.commitTx()
			} else {
				s.rollbackTx()
				s.answer("ROLLBACK", nil)
			}
			return true
		case pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT, pg.TransactionStmtKind_TRANS_STMT_RELEASE, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
			if d.org != "" {
				if s.savepointTx(sql, ts) {
					s.tenant.apply(an.Events, nil)
					s.answer(map[pg.TransactionStmtKind]string{pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT: "SAVEPOINT",
						pg.TransactionStmtKind_TRANS_STMT_RELEASE: "RELEASE", pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO: "ROLLBACK"}[ts.Kind], nil)
				}
				return true
			}
		}
		if d.org != "" {
			s.refuseInTx("a transaction statement of this form (PREPARE TRANSACTION, AND CHAIN)")
			return true
		}
		s.materialize()
		return false
	}
	if d.org == "" {
		if s.tenantLocal(sql, an) {
			// the cooperative mode: held with the BEGIN; the transaction is known by the
			// statement after it (every transaction of the application starts with this one)
			d.tenant = append(d.tenant, sql)
			tevs, tsets, _, _ := tenantString(sql, s.tenantCfg())
			s.tenant.apply(tevs, tsets)
			s.answer("SET", nil)
			return true
		}
		if ts, ok := tenantConfigLocal(sql, s.tenantCfg()); ok && ts.Param == 0 {
			// SELECT set_config(…, true): the same, its row answered here (block 9, from block 10's queue)
			s.holdTenant(*ts)
			s.setConfigRow(ts.Value, true, nil)
			s.answer("SELECT 1", nil)
			return true
		}
		if mentionsTenant(sql, s.tenantCfg().Setting) {
			s.materialize() // set_config, a session SET, a value to refuse: the cloud's way, not learnt
			return false
		}
		fp, cloud := s.cloudFirstFor(sql)
		if cloud {
			s.materialize()
			return false
		}
		if !s.learnt(fp) && !s.directOnly(s.directOrgOf(sql, an, constOnly, nil)) { // not learnt yet: the cloud's way, watched (profile direct: no cloud way to learn on)
			s.materialize()
			s.startLearn(fp)
			return false
		}
		if s.directCopyStart(sql, an) || s.directQuery(sql, an) {
			return true
		}
		s.materialize()
		return false
	}
	// a direct transaction of d.org
	if mentionsTenant(sql, s.tenantCfg().Setting) && !an.Lokadb {
		// lokadb.tenant (SET LOCAL, set_config): the cloud's open transaction, the session's
		// tenant state as on the cloud's way; a statement after it under another organization
		// is refused as any other one
		return false
	}
	if s.settingsInTx(sql, an) {
		return true
	}
	if !an.Lokadb {
		d.cloudWork = true
		return false // the cloud's tables alone: the cloud's open transaction
	}
	if s.directCopyStart(sql, an) || s.directQuery(sql, an) {
		return true
	}
	s.refuseInTx("a statement on LokaDB tables that needs the cloud (another organization, a form the direct path does not take: MERGE, a COPY of no organization named, an INSERT … SELECT without its key, an upsert on a target the organization may lack, DDL, a cursor, a table with a trigger of the cloud)")
	return true
}

// settingsInTx: a string of settings alone in a direct transaction. Whitelisted ones
// (directSafe, SET LOCAL too) run on both sides, the organization first, and the cloud's answer
// is the client's; session ones are kept for the session's later direct connections at COMMIT.
// Others: 0A000. false: not a settings string (the caller goes on); true: answered.
func (s *session) settingsInTx(sql string, an *Analysis) bool {
	tree, err := pg.Parse(sql)
	if err != nil || an.Lokadb {
		return false
	}
	local := false
	for _, raw := range tree.Stmts {
		vs := raw.Stmt.GetVariableSetStmt()
		if vs == nil {
			if sel := raw.Stmt.GetSelectStmt(); sel != nil && onlySetConfig(sel) {
				continue
			}
			return false
		}
		n := strings.ToLower(vs.Name)
		if vs.IsLocal {
			if !directSafe[n] {
				s.refuseInTx("SET LOCAL " + n + " (off the settings the direct path carries)")
				return true
			}
			local = true
		}
	}
	sets, unknown := directSetting(sql)
	if unknown {
		s.refuseInTx("a setting the direct path does not carry")
		return true
	}
	if len(sets) == 0 && !local {
		return false
	}
	d := s.direct.tx
	_, _, failed, err := s.orgWrite(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: sql}}, false, true)
	switch {
	case err != nil:
		s.dropDirect(d.dc)
		s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + d.org + "'s database was lost: " + err.Error()})
		return true
	case failed != nil:
		s.failTx(failed)
		return true
	}
	d.sets = append(d.sets, sets...)
	_ = s.send([]outMsg{{msg: &pgproto3.Query{String: sql}}})
	return true
}

// savepointTx runs a SAVEPOINT, RELEASE or ROLLBACK TO on both sides, the organization first;
// false: one side refused it, the transaction is aborted and the client has its error.
func (s *session) savepointTx(sql string, ts *pg.TransactionStmt) bool {
	d := s.direct.tx
	_, _, failed, err := s.orgWrite(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: sql}}, false, true)
	switch {
	case err != nil:
		s.dropDirect(d.dc)
		d.dc = nil
		s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + d.org + "'s database was lost: " + err.Error()})
		return false
	case failed != nil:
		s.failTx(failed)
		return false
	}
	if c := s.cloudRun(sql); c.err != nil {
		e := c.resp
		if e == nil {
			e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + c.err.Error()}
		}
		s.failTx(e)
		return false
	}
	// the session settings set after a savepoint go with it at ROLLBACK TO, stay at RELEASE
	name := ts.SavepointName // as the parser gives it: folded unless quoted, as the server compares
	at := -1
	for i := len(d.spSets) - 1; i >= 0; i-- {
		if d.spSets[i].name == name {
			at = i
			break
		}
	}
	switch ts.Kind {
	case pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT:
		d.spSets = append(d.spSets, spMark{name: name, sets: len(d.sets)})
	case pg.TransactionStmtKind_TRANS_STMT_RELEASE:
		if at >= 0 {
			d.spSets = d.spSets[:at]
		}
	case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		if at >= 0 {
			d.sets = d.sets[:d.spSets[at].sets]
			d.spSets = d.spSets[:at+1]
		}
	}
	return true
}

// commitTx commits the organization's side, then the cloud's (postgres_fdw's order); the
// client gets COMMIT, or the error of the side that failed.
func (s *session) commitTx() {
	d := s.direct.tx
	if !d.cloudWork && d.dc != nil && !d.beginPending {
		s.commitBoth()
		return
	}
	if e := s.commitQuiet(); e != nil {
		s.toClient(e)
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
		return
	}
	s.answer("COMMIT", nil)
}

// commitQuiet commits the direct transaction in order, the organization's side first, and ends
// it; the error the client is to get instead of COMMIT, if any (both sides rolled back when it
// comes before the organization's commit). The client is not answered (a pipeline's implicit
// transaction: pipeWrite).
func (s *session) commitQuiet() *pgproto3.ErrorResponse {
	d := s.direct.tx
	// the cloud's deferred constraints act now, while both sides can still roll back: after it
	// the cloud's COMMIT fails for the system's reasons alone (serialization, a lost connection)
	// the organization's transaction id, at once with it: what became of a COMMIT whose
	// connection broke is asked by it (orgCommit, review PR-1)
	c := s.cloudStart("SET CONSTRAINTS ALL IMMEDIATE")
	xid, xidKnown := "", false
	var lost error
	if d.dc != nil && !d.beginPending {
		rows, f, err := s.orgSimple(d.dc, "SELECT txid_current_if_assigned()")
		switch {
		case err != nil:
			lost = err // the COMMIT was not sent: the organization rolls the transaction back
		case f == nil && len(rows) == 1:
			xid, xidKnown = rows[0], true
		}
	}
	<-c.done
	if lost != nil {
		s.cloudRun("ROLLBACK")
		s.endTx()
		return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006",
			Message: "lokadb: the connection to organization " + d.org + "'s database was lost before the COMMIT: the transaction is rolled back on both sides: " + lost.Error()}
	}
	if c.err != nil {
		if d.dc != nil && !d.beginPending {
			if _, _, _, err := s.orgWrite(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: "ROLLBACK"}}, false, true); err != nil {
				s.dropDirect(d.dc)
			}
		}
		s.cloudRun("ROLLBACK")
		s.endTx()
		e := c.resp
		if e == nil {
			e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + c.err.Error()}
		}
		return e
	}
	if d.dc != nil && !d.beginPending {
		switch failed, outcome := s.orgCommit(d.dc, xid, xidKnown); outcome {
		case orgAborted:
			s.cloudRun("ROLLBACK")
			s.endTx()
			return failed
		case orgUnknown:
			s.unknownSide(d)
			s.endTx()
			return failed
		}
	}
	if c := s.cloudRun("COMMIT"); c.err != nil {
		s.log.Warn("a direct transaction committed in the organization's database, the cloud's commit failed: refs-diff / refs-sync repair its refs", "org", d.org, "err", c.err)
		s.endTx()
		return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "40003",
			Message: "lokadb: organization " + d.org + "'s side of the transaction is committed, the cloud's commit failed: " + c.err.Error()}
	}
	if len(d.sets) > 0 { // the session settings it ran on both sides stay, as on the server
		s.direct.mu.Lock()
		s.direct.sets = append(s.direct.sets, d.sets...)
		s.direct.mu.Unlock()
		if d.dc != nil {
			d.dc.applied += len(d.sets)
		}
	}
	s.endTx()
	return nil
}

// rollbackTx rolls both sides back (a lost organization connection is rolled back by its server).
func (s *session) rollbackTx() {
	d := s.direct.tx
	if d.org != "" {
		if d.dc != nil && !d.beginPending {
			if _, _, _, err := s.orgWrite(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: "ROLLBACK"}}, false, true); err != nil {
				s.dropDirect(d.dc)
			}
		}
		s.cloudRun("ROLLBACK")
	}
	s.endTx()
}

// txGate: the cloud's part of a statement in a direct transaction, in one round trip. A
// deferred transaction opens here: the client's BEGIN, the pause for the transaction, the
// gate's check (and extra: a write's refs rows), then the organization's BEGIN on dc; an open
// one runs the check and extra alone. rows: extra's answer. ok false: a deferred transaction
// stays deferred (the cloud rolled back), an open one is aborted (the client has its error:
// refusedSent).
func (s *session) txGate(org string, di *directInfo, dc *directConn, extra ...string) (rows [][]string, ok, refusedSent bool) {
	return s.txGateAll(org, []*directInfo{di}, dc, extra...)
}

// txGateAll is txGate for the statements of a pipeline (item 6): a check for each of dis, all
// of them allowed; rows: extra's answer, after the checks'.
func (s *session) txGateAll(org string, dis []*directInfo, dc *directConn, extra ...string) (rows [][]string, ok, refusedSent bool) {
	d := s.direct.tx
	open := d.org == ""
	if !open { // the checks this transaction passed already: not again (item 9)
		var left []*directInfo
		for _, di := range dis {
			if !d.checked[s.gateKey(di, org)] {
				left = append(left, di)
			}
		}
		if dis = left; len(dis) == 0 && len(extra) == 0 {
			return nil, true, false
		}
	}
	var qs []cloudStmt // the BEGIN, the lock and the checks prepared once on the connection (direct_prep.go)
	if open {
		qs = append(qs, prepared(d.begin), prepared(cloudBusy))
		for _, t := range d.tenant {
			qs = append(qs, plain(t)) // no rows: the check's row stays after the lock's
		}
		qs = append(qs, s.lockStmt("SELECT FROM pg_advisory_xact_lock_shared", org))
	}
	for _, di := range dis {
		qs = append(qs, s.gateCheck(di, org))
	}
	for _, e := range extra {
		qs = append(qs, plain(e))
	}
	c := s.cloudRunStmts(qs...)
	at := 0 // the first check's row
	if open {
		at = 1 // after the lock's
	}
	allowed := c.err == nil && len(c.rows) >= at+len(dis)
	for i := 0; allowed && i < len(dis); i++ {
		allowed = len(c.rows[at+i]) == 3 && s.gateAllows(c.rows[at+i], org, dis[i])
	}
	if !allowed {
		if open {
			s.cloudRun("ROLLBACK")
			if r := c.resp; r != nil && (strings.HasPrefix(r.Code, "57") || r.Code == "55P03" || r.Code == "40P01") {
				s.failTx(r) // the client's statement fails in its (deferred) transaction, as on the server
				return nil, false, true
			}
			return nil, false, false
		}
		if c.resp != nil {
			s.failTx(c.resp)
		} else {
			s.refuseInTx("a statement the gate does not let through (the client's privileges, row level security of the application)")
		}
		return nil, false, true
	}
	if open { // the organization's BEGIN goes with the statement (beginFor)
		d.org, d.dc, d.beginPending = org, dc, true
		s.direct.deferred.Store(false)
	}
	if len(extra) > 0 { // a write's refs rows
		d.cloudWork = true
	}
	if d.checked == nil {
		d.checked = map[string]bool{}
	}
	for _, di := range dis {
		d.checked[s.gateKey(di, org)] = true
	}
	return c.rows[at+len(dis):], true, false
}

// txFirst: the fingerprint the transaction is known by, set at its first statement.
func (d *directTx) setFirst(sql string) {
	if d.first == "" {
		d.first = fingerprint(sql)
	}
}

// txBatch: a batch and the deferred or direct transaction (handleBatch, before directBatch).
// true: handled.
func (s *session) directTxBatch(batch []pgproto3.FrontendMessage, isSync, unsynced bool) bool {
	d := s.direct.tx
	if d == nil {
		return false
	}
	s.txFailed(d) // as directTxQuery: a statement of the batch does not run at the organization after it
	// COMMIT, ROLLBACK, SAVEPOINT … by the extended protocol: as by a simple query (an aborted
	// transaction too: its ROLLBACK must get through)
	if sql, pre, ok := s.extTxStmt(batch, isSync, unsynced); ok {
		for _, m := range pre {
			s.toClient(m)
		}
		s.forgetUnnamed()
		s.directTxQuery(sql, s.analysis(sql), false) // answers (extTxStmt takes only the forms it does)
		return true
	}
	if d.aborted {
		if !d.errSent {
			s.toClient(abortedErr())
			d.errSent = true
		}
		if isSync {
			d.errSent = false
			s.toClient(s.clientRFQ(s.txStatus()))
		}
		s.flushClient()
		return true
	}
	if d.org == "" && isSync && !unsynced && s.extTenantLocal(batch) {
		return true
	}
	if d.org == "" {
		if sql := s.batchSQL(batch); sql != "" {
			fp, cloud := s.cloudFirstFor(sql)
			if cloud {
				s.materialize() // this shape met a statement the direct path could not take before
				return false
			}
			if !s.learnt(fp) && !s.directOnly(s.namedOrg()) { // not learnt yet: the cloud's way, watched (profile direct: no cloud way to learn on)
				s.materialize()
				s.startLearn(fp)
				return false
			}
		}
	}
	if isSync && !unsynced && s.directBatch(batch) {
		return true
	}
	if s.direct.tx == nil || s.direct.tx.aborted { // directBatch answered (a refusal)
		return s.direct.tx != nil
	}
	hasExec := false
	for _, msg := range batch {
		if _, ok := msg.(*pgproto3.Execute); ok {
			hasExec = true
		}
	}
	if !hasExec {
		return false // a statement prepared (Parse, Describe): the cloud's, the transaction untouched
	}
	if d.org == "" {
		s.materialize()
		return false
	}
	// a direct transaction: what the batch runs on LokaDB tables needs the cloud; a transaction
	// statement in it would end the cloud's side alone
	portals := map[string]string{}
	parsed := map[string]string{}
	for _, msg := range batch {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			parsed[m.Name] = m.Query
		case *pgproto3.Bind:
			portals[m.DestinationPortal] = m.PreparedStatement
		case *pgproto3.Execute:
			name, ok := portals[m.Portal]
			if !ok {
				if p := s.portals[m.Portal]; p != nil {
					name = p.stmt
				}
			}
			sql, ok := parsed[name]
			if !ok {
				if st := s.stmts[name]; st != nil {
					sql = st.sql
				}
			}
			if sql == "" || s.analysis(sql).Lokadb {
				s.refuseInTxRFQ("a statement on LokaDB tables in a pipeline or a form the direct path does not take", isSync)
				return true
			}
			if txStmt(sql) != nil || s.analysis(sql).TxControl {
				s.refuseInTxRFQ("a transaction statement in a pipeline (BEGIN, COMMIT, ROLLBACK, SAVEPOINT among other statements)", isSync)
				return true
			}
		}
	}
	d.cloudWork = true // the batch's statements on cloud tables: the cloud's open transaction
	return false
}

// writeInTx: a write of a deferred or direct transaction (direct_write.go's order without the
// commits, which are the transaction's): the refs rows in the cloud's open transaction with the
// gate, the statement in the organization's. An error aborts the transaction: the client's
// ROLLBACK (or ROLLBACK TO) undoes both sides. false: a deferred transaction stays deferred,
// nothing ran (the cloud's way).
func (s *session) writeInTx(dc *directConn, org string, di *directInfo, vals valuesFunc, simple string, batch []pgproto3.FrontendMessage, ptypes []uint32, an *Analysis) bool {
	d := s.direct.tx
	w := di.write
	refs := qrel(w.table.Refs)
	kind, typ := s.meta.keyKind(w.table.Refs), w.table.ColType[tableKey(w.table)]
	notTaken := func(why string) bool { // nothing ran: a deferred transaction stays deferred
		if d.org == "" {
			return false
		}
		s.refuseInTx(why)
		return true
	}
	var keys, extra []string
	n := len(w.rows)
	switch {
	case w.sel && w.keyCols != nil && w.unnest != nil: // the cloud's defaults give the key's columns unnest does not name
		var ok bool
		if n, ok = w.unnestRows(vals); !ok {
			return notTaken("an INSERT … SELECT * FROM unnest(…) whose arrays cannot be read")
		}
		extra = append(extra, w.genKeys(n))
	case w.del, w.sel: // the keys come from the organization's statement
	case w.keyCols != nil: // a composite key's rows, their refs rows in the same exchange (direct_composite.go)
		lits, ok := w.rowLits(vals, org)
		if !ok {
			return notTaken("an INSERT whose key is an expression")
		}
		extra = append(extra, w.tupleRefs(lits, refs, org))
	case w.keysGiven():
		var ok bool
		if keys, ok = w.givenKeys(vals, kind); !ok {
			return notTaken("an INSERT whose key is an expression")
		}
		extra = append(extra, givenRefs(w.lits(keys, kind, typ), w.refsCols(), refs, org, w.upsert))
	default:
		if w.unnest != nil {
			var ok bool
			if n, ok = w.unnestRows(vals); !ok {
				return notTaken("an INSERT … SELECT * FROM unnest(…) whose arrays cannot be read")
			}
		}
		extra = append(extra, "WITH lokadb_k AS (SELECT "+w.table.KeyDefault+" AS id FROM generate_series(1, "+itoa(n)+")), lokadb_r AS (INSERT INTO "+refs+
			" (id, tenant) SELECT id, "+orgLit(org)+" FROM lokadb_k) SELECT id::text FROM lokadb_k")
	}
	rows, ok, sent := s.txGate(org, di, dc, extra...)
	if !ok {
		return sent
	}
	d.cloudWork = true // a write: its refs rows in the cloud, the commits in order
	if w.del {
		d.refsDeleted = true
	} else {
		d.refsAdded = true
	}
	stmt := simple + stmtSQL(batch, s)
	d.setFirst(stmt)
	var added []string // an upsert's: the refs rows the cloud added for the keys it names
	switch {
	case w.sel && w.keyCols != nil && w.unnest != nil:
		if len(rows) != n {
			s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			return true
		}
		for _, r := range rows {
			keys = append(keys, tupleKey(r))
		}
	case w.del, w.sel:
	case w.keyCols != nil:
		none, ks, add, read := w.tupleAnswer(rows, len(w.rows))
		if !read {
			s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			return true
		}
		if !none {
			s.failTx(otherOrgKey(w.table))
			return true
		}
		keys, added = ks, add
	case w.keysGiven():
		if len(rows) < 1 || len(rows[0]) != 1 || rows[0][0] != "0" || !w.upsert && len(rows) != 1 {
			s.failTx(otherOrgKey(w.table))
			return true
		}
		for _, r := range rows[1:] {
			added = append(added, r[0])
		}
	default:
		if len(rows) != n {
			s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: the cloud gave no keys for the INSERT"})
			return true
		}
		for _, r := range rows {
			keys = append(keys, r[0])
		}
	}
	sql, err := w.rewrite(keys, org)
	if err != nil {
		s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()})
		return true
	}
	s.traceDirect(stmt, an, org)
	var msgs []pgproto3.FrontendMessage
	if batch == nil {
		msgs = append(msgs, &pgproto3.Query{String: sql})
	} else {
		msgs = append(msgs, &pgproto3.Parse{Query: sql, ParameterOIDs: ptypes})
		msgs = append(msgs, w.binds(batch)...)
	}
	answer, deleted, failed, err := s.orgWrite(d.dc, msgs, w.del, batch == nil)
	if err != nil {
		s.dropDirect(d.dc)
		d.dc = nil
		s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006", Message: "lokadb: the connection to organization " + org + "'s database was lost: " + err.Error()})
		return true
	}
	// the cloud's part after the organization's statement, in its open transaction
	cloudErr := func(c *capture) bool {
		if c.err == nil {
			return false
		}
		e := c.resp
		if e == nil {
			e = &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + c.err.Error()}
		}
		s.failTx(e)
		return true
	}
	if failed == nil {
		switch {
		case w.del && len(deleted) > 0: // the cloud's FKs onto __refs act here
			if cloudErr(s.cloudRun(w.refsDelete(refs, deleted, kind, typ))) {
				return true
			}
		case w.upsert, w.sel:
			var inserted []string
			if answer, inserted, err = w.split(answer, batch == nil, upsertBinary(batch), kind); err != nil {
				s.failTx(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "lokadb: " + err.Error()})
				return true
			}
			if w.sel { // the keys the rows named: their refs rows, none another organization's
				if len(inserted) > 0 {
					c := s.cloudRun(w.refsAdd(inserted, kind, typ, refs, org))
					if cloudErr(c) {
						return true
					}
					if !othersNone(c.rows) {
						s.failTx(otherOrgKey(w.table))
						return true
					}
				}
				break
			}
			given := keys // an upsert's keys of rows updated or skipped leave __refs
			if w.keysGiven() || w.keyCols != nil {
				given = added
			}
			if unused := without(given, inserted); len(unused) > 0 {
				if cloudErr(s.cloudRun(w.refsDelete(refs, unused, kind, typ))) {
					return true
				}
			}
		}
	}
	if failed != nil {
		d.aborted = true
	}
	for _, m := range answer {
		s.toClient(m)
	}
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
	return true
}

// otherOrgKey: the error of a write whose key __refs maps to another organization, as the
// parent's refs would refuse it through the FDW.
func otherOrgKey(t *Table) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "23505", Message: "duplicate key value violates unique constraint \"" + t.Refs + "_pkey\": a key of the INSERT belongs to another organization"}
}

func itoa(n int) string { return strconv.Itoa(n) }

// DirectTxTakes: a statement a direct transaction takes: what the direct path takes
// (DirectTakes), and a COPY FROM STDIN of the organization the application named (block 2 of
// 8b.2, step 4); check -profile asks it for a transaction's statements.
func DirectTxTakes(sql string, m *Meta, named bool) bool {
	return DirectTxTakesIn(sql, m, named, defaultPath)
}

// DirectTxTakesIn is DirectTxTakes with the bare names resolved by search path path.
func DirectTxTakesIn(sql string, m *Meta, named bool, path []string) bool {
	return directTxTakesWith(sql, analyzeIn(sql, m, path), m, named, path)
}

// directTxTakesWith is DirectTxTakesIn with sql's analysis by path given (the session's: PF8).
func directTxTakesWith(sql string, an *Analysis, m *Meta, named bool, path []string) bool {
	if named && !m.SchemasUsed { // as directCopyStart: not in a database with LokaDB tables outside public yet
		if t, _, _, _, _ := copyShape(sql, m); t != nil {
			return true
		}
	}
	return directShape(sql, an, m, named, path).ok
}

// batchSQL: the text of the statement the batch's first Execute runs ("" for none known).
func (s *session) batchSQL(batch []pgproto3.FrontendMessage) string {
	portals, parsed := map[string]string{}, map[string]string{}
	for _, msg := range batch {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			parsed[m.Name] = m.Query
		case *pgproto3.Bind:
			portals[m.DestinationPortal] = m.PreparedStatement
		case *pgproto3.Execute:
			name, ok := portals[m.Portal]
			if !ok {
				if p := s.portals[m.Portal]; p != nil {
					name = p.stmt
				}
			}
			if q, ok := parsed[name]; ok {
				return q
			}
			if st := s.stmts[name]; st != nil {
				return st.sql
			}
			return ""
		}
	}
	return ""
}

// Learn first (coordinator's decision of 25.10): a transaction goes direct only in a form
// learnt for it. A deferred transaction whose first statement's fingerprint is not in
// Server.directOK goes the cloud's way, as it always did, and the proxy watches it: every
// statement on LokaDB tables one the direct path takes (DirectTakes), all of one organization
// in fdw+direct, the others on cloud tables alone and no setting it could not carry. Two such
// transactions committed in a row put the fingerprint in directOK; a metadata change (a
// profile, a move, enable, DDL) empties it. A form that still meets a statement the direct
// path cannot take (it branches) is refused once and goes the cloud's way (cloudFirst).

// learnTx: a cloud transaction watched for the direct path.
type learnTx struct {
	key  string // db/fingerprint of its first statement
	org  string // the organization of its statements on LokaDB tables
	good bool   // so far every statement fits a direct transaction
}

const learnRuns = 2 // committed transactions in a row that fit before a form goes direct

// directOK: forms learnt for the direct path (counts of committed transactions in a row that
// fit), per db/fingerprint; bounded as cloudFirst.
type directOK struct {
	mu    sync.Mutex
	runs  map[string]int
	order []string
}

func (o *directOK) ok(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.runs[key] >= learnRuns
}

// result counts a learnt transaction: fit and committed, or not (the count starts over).
func (o *directOK) result(key string, fit bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.runs == nil {
		o.runs = map[string]int{}
	}
	if _, seen := o.runs[key]; !seen {
		if len(o.order) >= maxCloudFirst {
			delete(o.runs, o.order[0])
			o.order = o.order[1:]
		}
		o.order = append(o.order, key)
	}
	if fit {
		o.runs[key]++
	} else {
		o.runs[key] = 0
	}
}

// reset forgets what was learnt for db (its keys leave the order too: learnt again, a key is
// not there twice, and the eviction takes the oldest form, not a live one: block 9's review).
func (o *directOK) reset(db string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	kept := o.order[:0]
	for _, k := range o.order {
		if strings.HasPrefix(k, db+"/") {
			delete(o.runs, k)
		} else {
			kept = append(kept, k)
		}
	}
	o.order = kept
}

func (s *session) startLearn(fp string) {
	if fp != "" {
		s.direct.learn = &learnTx{key: s.learnKey(fp), good: true}
	}
}

// learnt: the form whose first statement has fingerprint fp goes direct: the proxy learnt it,
// or lokadb.direct_learnt has it (check -profile fdw+direct -direct-forms).
func (s *session) learnt(fp string) bool {
	key := s.learnKey(fp)
	return s.srv.directOK.ok(key) || s.meta.Learnt[strings.TrimPrefix(key, s.db+"/")]
}

// learnKey: a transaction form in directOK, by its first statement's fingerprint and whether
// the application named the organization: a form learnt under a named one (its statements
// need no conditions of their own) is not one learnt without.
func (s *session) learnKey(fp string) string {
	if s.namedOrg() != "" {
		return s.db + "/" + fp + "#named"
	}
	return s.db + "/" + fp
}

// tenantLocal: sql is a SET LOCAL of lokadb.tenant (or Tenant.Setting) alone, to a value the
// proxy takes: what a deferred transaction holds with its BEGIN (the cooperative mode). Its
// SELECT set_config(…, true) form answers with a row: the cloud's way.
func (s *session) tenantLocal(sql string, an *Analysis) bool {
	if an.Lokadb || len(an.Stmts) != 1 {
		return false
	}
	_, tsets, _, err := tenantString(sql, s.tenantCfg())
	if err != nil || len(tsets) != 1 || !tsets[0].Local || tsets[0].Param > 0 || tsets[0].All {
		return false
	}
	tree, perr := pg.Parse(sql)
	return perr == nil && len(tree.Stmts) == 1 && tree.Stmts[0].Stmt.GetVariableSetStmt() != nil
}

// learnStmt: a statement on LokaDB tables the cloud's way routed to org, in a watched
// transaction. An organization the direct path does not serve (not moved, not in fdw+direct,
// without customers_app) teaches nothing either way: the watching stops, the count stays (the
// same form of an organization in fdw+direct may still learn: block 9's review).
func (s *session) learnStmt(sql, org string, named bool) {
	l := s.direct.learn
	if l == nil || !l.good {
		return
	}
	switch {
	case !realOrg(org):
		l.good = false // no organization, or rows of none: never one database
	case !directProfile(s.meta.Profiles[s.meta.orgNum(org)]) || s.customersApp(org) == "":
		if l.org == "" || l.org == org {
			s.direct.learn = nil // neither fit nor unfit
			return
		}
		l.good = false // a second organization
	case l.org != "" && l.org != org:
		l.good = false
	case !directTxTakesWith(sql, s.analysis(sql), s.meta, named, s.pathFor()): // the session's analysis, cached (PF8)
		l.good = false
	}
	if l.org == "" {
		l.org = org
	}
}

// learnNonLokadb: a statement of a watched transaction not on LokaDB tables: a setting the
// direct transaction would refuse, or a transaction statement of a form it does not take,
// makes it unfit; a watched transaction that ended otherwise (the cloud is out of it) is dropped.
func (s *session) learnNonLokadb(sql string, an *Analysis) {
	l := s.direct.learn
	if l == nil {
		return
	}
	if s.txStatus() == 'I' {
		s.direct.learn = nil
		return
	}
	if an.Lokadb || !l.good {
		return
	}
	if ts := txStmt(sql); ts != nil {
		if ts.Kind == pg.TransactionStmtKind_TRANS_STMT_PREPARE || ts.Chain {
			l.good = false
		}
		return
	}
	if mentionsTenant(sql, s.tenantCfg().Setting) {
		return // lokadb.tenant: a direct transaction runs it in the cloud's (directTxQuery)
	}
	if hasFold(sql, "set") {
		if _, unknown := directSetting(sql); unknown {
			l.good = false
		}
		if tree, err := pg.Parse(sql); err == nil {
			for _, raw := range tree.Stmts {
				if vs := raw.Stmt.GetVariableSetStmt(); vs != nil && vs.IsLocal && !directSafe[strings.ToLower(vs.Name)] {
					l.good = false
				}
			}
		}
	}
}

// learnEnd: at the COMMIT or ROLLBACK of a watched transaction, what it taught (once the cloud
// answered the COMMIT; tx: the cloud's status before it). The result runs in the relay
// goroutine (onOK): directOK has its own lock.
func (s *session) learnEnd(sql string, an *Analysis, tx byte) func() {
	l := s.direct.learn
	if l == nil || !an.TxControl {
		return nil
	}
	ts := txStmt(sql)
	if ts == nil {
		return nil
	}
	switch ts.Kind {
	case pg.TransactionStmtKind_TRANS_STMT_COMMIT:
		s.direct.learn = nil
		fit := l.good && l.org != "" && tx == 'T' && !ts.Chain
		return func() { s.srv.directOK.result(l.key, fit) }
	case pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		s.direct.learn = nil // neither fit nor unfit
	}
	return nil
}

// extTxStmt: the batch is one transaction statement by the extended protocol, the whole of it
// up to its Sync: Parse (unnamed) of COMMIT, ROLLBACK, SAVEPOINT, RELEASE or ROLLBACK TO, Bind of
// it to the unnamed portal, Describe of the portal maybe, Execute (block 10, item 6; JDBC and
// drivers that send every statement so). pre: the answers to Parse, Bind and Describe the client
// gets before the statement's own (directTxQuery's). Only forms directTxQuery answers in the
// transaction's state: COMMIT and ROLLBACK without AND CHAIN always, the savepoint ones in a
// direct transaction (deferred: the cloud's way, which opens it).
func (s *session) extTxStmt(batch []pgproto3.FrontendMessage, isSync, unsynced bool) (string, []pgproto3.BackendMessage, bool) {
	d := s.direct.tx
	if d == nil || !isSync || unsynced || len(batch) < 4 || len(batch) > 5 {
		return "", nil, false
	}
	p, ok1 := batch[0].(*pgproto3.Parse)
	b, ok2 := batch[1].(*pgproto3.Bind)
	e, ok3 := batch[len(batch)-2].(*pgproto3.Execute)
	if !ok1 || !ok2 || !ok3 || p.Name != "" || b.PreparedStatement != "" || b.DestinationPortal != "" || e.Portal != "" || len(b.Parameters) > 0 {
		return "", nil, false
	}
	pre := []pgproto3.BackendMessage{&pgproto3.ParseComplete{}, &pgproto3.BindComplete{}}
	if len(batch) == 5 {
		if ds, ok := batch[2].(*pgproto3.Describe); !ok || ds.ObjectType != 'P' || ds.Name != "" {
			return "", nil, false
		}
		pre = append(pre, &pgproto3.NoData{})
	}
	ts := txStmt(p.Query)
	if ts == nil {
		return "", nil, false
	}
	switch ts.Kind {
	case pg.TransactionStmtKind_TRANS_STMT_COMMIT, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK:
		if ts.Chain {
			return "", nil, false
		}
	case pg.TransactionStmtKind_TRANS_STMT_SAVEPOINT, pg.TransactionStmtKind_TRANS_STMT_RELEASE, pg.TransactionStmtKind_TRANS_STMT_ROLLBACK_TO:
		if d.org == "" {
			return "", nil, false
		}
	default:
		return "", nil, false
	}
	if ok, dead := s.q.waitEmpty(); !ok || dead {
		return "", nil, false
	}
	return p.Query, pre, true
}

// extBegin: the batch starts with BEGIN by the extended protocol (Parse unnamed, Bind of it to
// the unnamed portal, Describe of the portal maybe, Execute), as JDBC sends it before the
// transaction's first statement with autocommit off: it is deferred as directTxQuery defers a
// simple BEGIN; the client gets its answers (ParseComplete, BindComplete, NoData,
// CommandComplete), rest is what follows it (its Sync at least).
func (s *session) extBegin(batch []pgproto3.FrontendMessage, isSync, unsynced bool) ([]pgproto3.FrontendMessage, bool) {
	if s.direct.tx != nil || !isSync || unsynced || len(batch) < 4 || s.txStatus() != 'I' || !s.directTxPossible() {
		return nil, false
	}
	p, ok1 := batch[0].(*pgproto3.Parse)
	b, ok2 := batch[1].(*pgproto3.Bind)
	if !ok1 || !ok2 || p.Name != "" || b.PreparedStatement != "" || b.DestinationPortal != "" || len(b.Parameters) > 0 {
		return nil, false
	}
	ts := txStmt(p.Query)
	if ts == nil || ts.Kind != pg.TransactionStmtKind_TRANS_STMT_BEGIN && ts.Kind != pg.TransactionStmtKind_TRANS_STMT_START {
		return nil, false
	}
	at := 2
	describe := false
	if ds, ok := batch[at].(*pgproto3.Describe); ok {
		if ds.ObjectType != 'P' || ds.Name != "" {
			return nil, false
		}
		describe, at = true, at+1
	}
	if e, ok := batch[at].(*pgproto3.Execute); !ok || e.Portal != "" || at+1 >= len(batch) {
		return nil, false
	}
	if ok, dead := s.q.waitEmpty(); !ok || dead {
		return nil, false
	}
	s.setTx(&directTx{begin: p.Query})
	s.forgetUnnamed()
	s.tenant.apply(s.analysis(p.Query).Events, nil)
	s.toClient(&pgproto3.ParseComplete{})
	s.toClient(&pgproto3.BindComplete{})
	if describe {
		s.toClient(&pgproto3.NoData{})
	}
	s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
	return batch[at+1:], true
}

// forgetUnnamed: the client's unnamed statement and portal are a transaction statement the proxy
// answered itself (extBegin, extTxStmt) and the cloud never saw: a Bind or Describe of them
// without a Parse gets "does not exist" from the proxy rather than running the cloud's older one
// (block 9's review).
func (s *session) forgetUnnamed() {
	delete(s.stmts, "")
	delete(s.portals, "")
}

// The cooperative mode by set_config (block 9, from block 10's queue): SELECT
// set_config('lokadb.tenant', $1 or a constant, true) at the start of a deferred transaction, the
// form pgx, ORMs and connection hooks send, is held with the BEGIN as its SET LOCAL is
// (tenantLocal): the cloud runs it as SET LOCAL when the transaction opens (txGate) or goes the
// cloud's way (materialize), and the client gets set_config's row from the proxy, the value it
// set (a text). Before, it sent the transaction the cloud's way and taught nothing: such
// transactions never went direct (DP21, UT10 of block 18).

// tenantConfigLocal: sql is SELECT set_config(<setting>, <value>, true) alone, to a value the
// proxy takes (ts.Param > 0: a parameter's).
func tenantConfigLocal(sql string, cfg TenantConfig) (*TenantSet, bool) {
	ts, err := stmtTenant(sql, cfg)
	if err != nil || ts == nil || !ts.Local || ts.All {
		return nil, false
	}
	tree, perr := pg.Parse(sql)
	if perr != nil || len(tree.Stmts) != 1 || tree.Stmts[0].Stmt.GetSelectStmt() == nil {
		return nil, false
	}
	return ts, true
}

// holdTenant: lokadb.tenant set for the deferred transaction, held with its BEGIN as SET LOCAL.
func (s *session) holdTenant(ts TenantSet) {
	setting := s.tenantCfg().Setting
	if setting == "" {
		setting = TenantSetting
	}
	d := s.direct.tx
	d.tenant = append(d.tenant, "SET LOCAL "+setting+" = "+sqlString(ts.Value))
	s.tenant.set(ts)
}

// setConfigRow: set_config's answer, a text: its description (desc; formats: the Bind's
// result formats), then its row.
func (s *session) setConfigRow(v string, desc bool, formats []int16) {
	if desc {
		var f int16
		if len(formats) > 0 {
			f = formats[0]
		}
		s.toClient(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("set_config"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1, Format: f}}})
	}
	s.toClient(&pgproto3.DataRow{Values: [][]byte{[]byte(v)}}) // a text is the same in either format
}

// extTenantLocal: the batch is set_config(…, true) by the extended protocol (pgx's Exec with a
// parameter: Bind of the statement it prepared, Describe of the portal maybe, Execute; or the
// whole of it with an unnamed Parse) in a deferred transaction: held with the BEGIN, answered
// here. false: the batch is something else, or a value the cloud's way refuses.
func (s *session) extTenantLocal(batch []pgproto3.FrontendMessage) bool {
	var parse *pgproto3.Parse
	var b *pgproto3.Bind
	describe := false
	for i, m := range batch {
		switch m := m.(type) {
		case *pgproto3.Parse:
			if i != 0 || m.Name != "" {
				return false
			}
			parse = m
		case *pgproto3.Bind:
			if b != nil || m.DestinationPortal != "" || (m.PreparedStatement == "") != (parse != nil) {
				return false
			}
			b = m
		case *pgproto3.Describe:
			if m.ObjectType != 'P' || m.Name != "" || b == nil {
				return false
			}
			describe = true
		case *pgproto3.Execute:
			if b == nil || m.Portal != "" || i != len(batch)-2 {
				return false
			}
		case *pgproto3.Sync:
		default:
			return false
		}
	}
	if b == nil {
		return false
	}
	st := s.stmts[b.PreparedStatement]
	if parse != nil {
		st = &stmtInfo{sql: parse.Query}
		st.tset, st.terr = stmtTenant(parse.Query, s.tenantCfg())
	}
	if st == nil || st.tset == nil || st.terr != nil {
		return false
	}
	if _, ok := tenantConfigLocal(st.sql, s.tenantCfg()); !ok {
		return false
	}
	ts, err := boundTenant(st, &portalInfo{params: b.Parameters, formats: b.ParameterFormatCodes})
	if err != nil || ts == nil {
		return false
	}
	if ok, dead := s.q.waitEmpty(); !ok || dead {
		return false
	}
	s.holdTenant(*ts)
	if parse != nil {
		s.toClient(&pgproto3.ParseComplete{})
		s.forgetUnnamed() // the cloud has not got it
	}
	s.toClient(&pgproto3.BindComplete{})
	s.setConfigRow(ts.Value, describe, b.ResultFormatCodes)
	s.toClient(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
	return true
}

// beginFor: the BEGIN the direct transaction has not sent on dc yet (txGate leaves it to go in
// front of the first statement there: one exchange less, item 9), taken: the caller sends it
// with the statement (withBegin) and reads its answer first; nil: none.
func (s *session) beginFor(dc *directConn) *pgproto3.Query {
	if d := s.direct.tx; d != nil && d.dc == dc && d.beginPending {
		d.beginPending = false
		return &pgproto3.Query{String: d.begin}
	}
	return nil
}

// withBegin puts begin in front of msgs so that its error skips them (block 9's review: a write
// after a failed BEGIN would commit on its own): in the same query string as a simple query
// ("BEGIN; …"), or by the extended protocol before an extended one, with no Sync between. Its
// answer ends with CommandComplete BEGIN (the reader takes everything up to it as the BEGIN's).
func withBegin(begin string, msgs []pgproto3.FrontendMessage) []pgproto3.FrontendMessage {
	if q, ok := msgs[0].(*pgproto3.Query); ok {
		return append([]pgproto3.FrontendMessage{&pgproto3.Query{String: begin + "; " + q.String}}, msgs[1:]...)
	}
	return append([]pgproto3.FrontendMessage{&pgproto3.Parse{Query: begin}, &pgproto3.Bind{}, &pgproto3.Execute{}}, msgs...)
}

// beginDone: m ends the BEGIN's part of an answer withBegin made.
func beginDone(m pgproto3.BackendMessage) bool {
	c, ok := m.(*pgproto3.CommandComplete)
	return ok && strings.EqualFold(string(c.CommandTag), "BEGIN")
}

// commitBoth commits a direct transaction that did nothing of its own in the cloud (reads and
// UPDATEs at the organization: the cloud holds the pause and the gate's checks alone): no
// deferred constraint there, the order of the commits is the organization's alone, and the
// cloud's COMMIT goes at once with the organization's (item 9: one exchange's wait instead of
// three). A cloud COMMIT that fails loses nothing (the pause is released with it).
func (s *session) commitBoth() {
	d := s.direct.tx
	c := s.cloudStart("COMMIT")
	_, _, failed, err := s.orgWrite(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: "COMMIT"}}, false, true)
	<-c.done
	if c.err != nil {
		s.log.Warn("a direct transaction without work in the cloud: its COMMIT there failed (the pause is released)", "org", d.org, "err", c.err)
	}
	if err != nil || failed != nil {
		if err != nil { // nothing of it in the cloud: the client alone does not know the outcome (review PR-1)
			s.dropDirect(d.dc)
			failed = errUnknown(d.org, err)
		}
		s.endTx()
		s.toClient(failed)
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
		return
	}
	if len(d.sets) > 0 { // the session settings it ran on both sides stay, as on the server
		s.direct.mu.Lock()
		s.direct.sets = append(s.direct.sets, d.sets...)
		s.direct.mu.Unlock()
		d.dc.applied += len(d.sets)
	}
	s.endTx()
	s.answer("COMMIT", nil)
}

// onlySetConfig: sel is SELECT set_config(...) [, set_config(...)] with constant arguments and
// nothing else: a setting, which runs on both sides. A SELECT that also reads a table or calls
// something else is a statement, not a setting (review of the protocol, PR-13: it went to the
// organization's database whole).
func onlySetConfig(sel *pg.SelectStmt) bool {
	if len(sel.FromClause) > 0 || sel.WhereClause != nil || len(sel.GroupClause) > 0 || sel.HavingClause != nil ||
		len(sel.ValuesLists) > 0 || sel.Larg != nil || sel.WithClause != nil || len(sel.TargetList) == 0 {
		return false
	}
	constant := func(n *pg.Node) bool {
		if tc := n.GetTypeCast(); tc != nil {
			n = tc.Arg
		}
		return n.GetAConst() != nil
	}
	for _, t := range sel.TargetList {
		fc := t.GetResTarget().GetVal().GetFuncCall()
		if fc == nil || len(fc.Funcname) == 0 || strings.ToLower(fc.Funcname[len(fc.Funcname)-1].GetString_().GetSval()) != "set_config" ||
			len(fc.Funcname) == 2 && fc.Funcname[0].GetString_().GetSval() != "pg_catalog" || len(fc.Funcname) > 2 {
			return false
		}
		for _, a := range fc.Args {
			if !constant(a) {
				return false
			}
		}
	}
	return true
}
