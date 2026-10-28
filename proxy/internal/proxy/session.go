package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
)

// expectKind identifies a message sent upstream whose responses are still pending.
type expectKind byte

const (
	kQuery expectKind = 'Q'
	kParse expectKind = 'P'
	kBind  expectKind = 'B'
	kDescS expectKind = 'S'
	kDescP expectKind = 'D'
	kExec  expectKind = 'E'
	kClose expectKind = 'C'
	kSync  expectKind = 'Y'
	kFunc  expectKind = 'F' // FunctionCall: answered like a simple query, up to ReadyForQuery
)

// mode says what to do with the responses of a message.
type mode int

const (
	mFwd     mode = iota // client's own message: relay responses to the client
	mDrop                // injected by the proxy: swallow responses
	mCapture             // proxy's own query: collect rows
	mHold                // client's query run inside the proxy's transaction: relay all but ReadyForQuery
)

type capture struct {
	rows [][]string
	err  error
	done chan struct{}
	once sync.Once
	// untilSync: captureExt ending with a Sync: the capture ends at its ReadyForQuery, and an
	// error is the proxy's own (the client's messages are not affected)
	untilSync bool
	oids      []uint32                // ParameterDescription (describeTypes)
	cols      []uint32                // RowDescription's column types (resultTypes); empty, not nil, after NoData
	resp      *pgproto3.ErrorResponse // the error err stands for (its SQLSTATE: the direct path's gate)
}

func (c *capture) finish() { c.once.Do(func() { close(c.done) }) }

// fail ends the capture with err, unless it ended already (the queue closed with the session:
// abort): one writer of c.err.
func (c *capture) fail(err error) {
	c.once.Do(func() {
		c.err = err
		close(c.done)
	})
}

// abort: the session is over: the capture ends with an error, unless it ended already (what its
// reader got stays).
func (c *capture) abort() {
	c.once.Do(func() {
		c.err = io.ErrUnexpectedEOF
		close(c.done)
	})
}

type expect struct {
	kind expectKind
	mode mode
	cap  *capture
	// onOK runs (relay goroutine) once the message has run successfully: an Execute at its
	// CommandComplete, a query or Sync at its ReadyForQuery without an error before.
	onOK func()
	// notify gets true or false when the message is answered (or will never be);
	// wantTag: true needs this command tag (COMMIT: "ROLLBACK" means the transaction had failed).
	notify  chan bool
	wantTag string
	failed  bool
	tag     string
	// tagAs: the command tag the client gets instead of the server's (indexTag)
	tagAs string
	// mu, ended: the relay finishes an expect while the session closes the queue (the client went
	// away) with the expect still in it: whoever comes first reports the outcome, once
	mu    sync.Mutex
	ended bool
}

// finish reports the outcome of the message once.
func (e *expect) finish(ok bool) { e.end(ok, true) }

// abort: the session is over, nothing is to run for the message (no onOK), its waiter gets false.
func (e *expect) abort() { e.end(false, false) }

func (e *expect) end(ok, run bool) {
	e.mu.Lock()
	if e.ended {
		e.mu.Unlock()
		return
	}
	e.ended = true
	onOK, notify := e.onOK, e.notify
	e.onOK, e.notify = nil, nil
	if ok && e.wantTag != "" && e.tag != e.wantTag {
		ok = false
	}
	e.mu.Unlock()
	if ok && run && onOK != nil {
		onOK()
	}
	if notify != nil {
		select {
		case notify <- ok:
		default:
		}
	}
}

// queue is the ordered list of upstream messages awaiting responses.
type queue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []*expect
	closed bool
	// dead: an error ended an extended-protocol batch that had no Sync. The server now
	// ignores everything until a Sync: what is queued gets no response, and nothing the
	// proxy would send of its own (set_config, lookups) would run.
	dead bool
}

func newQueue() *queue {
	q := &queue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *queue) push(e []*expect) {
	q.mu.Lock()
	q.items = append(q.items, e...)
	q.mu.Unlock()
}

func (q *queue) head() *expect {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

func (q *queue) empty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) == 0
}

func (q *queue) pop() {
	q.mu.Lock()
	if len(q.items) > 0 { // close() may have emptied it meanwhile (the client went away)
		q.items = q.items[1:]
	}
	if len(q.items) == 0 {
		q.cond.Broadcast()
	}
	q.mu.Unlock()
}

// hasSync reports whether a Sync is queued (the server's skipping ends there).
func (q *queue) hasSync() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.syncQueued()
}

func (q *queue) syncQueued() bool { // under mu
	for _, e := range q.items {
		if e.kind == kSync {
			return true
		}
	}
	return false
}

// skipping: the server skips what comes next, up to a Sync the client has not sent yet. Once
// that Sync is on its way the server reads the next batch normally, though its ReadyForQuery
// (which ends dead) may still be coming.
func (q *queue) skipping() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dead && !q.syncQueued()
}

// dropNextSync forgets the first queued Sync: the server ignores a Sync it reads in copy-in mode.
func (q *queue) dropNextSync() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, e := range q.items {
		if e.kind == kSync {
			q.items = append(q.items[:i:i], q.items[i+1:]...)
			return
		}
	}
}

func (q *queue) setDead(d bool) {
	q.mu.Lock()
	q.dead = d
	q.cond.Broadcast()
	q.mu.Unlock()
}

func (q *queue) isDead() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dead
}

// waitEmpty blocks until every pending response has arrived. ok is false if the session
// died; dead, if what is pending will not be answered before the client's next Sync.
func (q *queue) waitEmpty() (ok, dead bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	// dead with a Sync queued ends with that Sync's ReadyForQuery: worth waiting for
	for len(q.items) > 0 && !q.closed && (!q.dead || q.syncQueued()) {
		q.cond.Wait()
	}
	return !q.closed, q.dead && !q.syncQueued()
}

func (q *queue) close() {
	q.mu.Lock()
	q.closed = true
	for _, e := range q.items {
		if e.cap != nil {
			e.cap.abort()
		}
		e.abort()
	}
	q.items = nil
	q.cond.Broadcast()
	q.mu.Unlock()
}

type stmtInfo struct {
	sql  string
	oids []uint32
	sent string // the text Parsed upstream under the client's name
	// an: the analysis against meta (the metadata it was made with); nil while the session
	// had no metadata (prepared before enable). See current.
	an   *Analysis
	meta *Meta
	path string // pathNow when an was made
	// alias: a statement of the proxy's own with the text the analysis wants, when sent is
	// not it any more (see relink); ptypes: the parameter types the server gave the client's
	// statement (pg_prepared_statements), which the alias keeps, since the client encodes
	// its parameters for them.
	alias  string
	ptypes []uint32
	// forgets, forgetsAll: what running it drops (DEALLOCATE name, DEALLOCATE ALL, DISCARD ALL).
	forgets    []string
	forgetsAll bool
	seq        int  // order of the Parse in the session (forget drops only what came before)
	unlistens  bool // it ends the proxy's LISTEN (dropsListen): listen again after it
	holds      bool // it declares a cursor WITH HOLD (session.cursors)
	// tset: it sets lokadb.tenant (the cooperative mode, tenant.go); terr: it would, but the
	// proxy refuses it (a value that is not an organization).
	tset *TenantSet
	terr error
	// rtypes: the result's column types the cloud gives the statement (resultTypes, the
	// direct path); nil until asked
	rtypes []uint32
	rmeta  *Meta // the metadata rtypes were asked under
	// unnamed: the client's unnamed statement Parsed in a batch that goes the direct path
	// (unnamedStmt): its Parse and Binds go to the organization's database as the client sent them
	unnamed bool
}

type portalInfo struct {
	stmt    string
	st      *stmtInfo // the statement the portal was bound from (nil: unknown to the proxy)
	an      *Analysis // the analysis its Bind used (nil: bound before the session had metadata)
	params  [][]byte
	formats []int16
	batch   int
	bindIdx int
	// org: lokadb.org set right before the portal's Bind (initial partition pruning ran with
	// it); its Execute needs the same value, whatever ran in between. orgSet: org is known.
	org    string
	orgSet bool
	// tset: the lokadb.tenant its Execute sets, the parameter read (stmtInfo.tset)
	tset *TenantSet
	// up: the statement of the proxy's own it is bound to upstream (a keyedStmt's name), if
	// any: that one is not closed while the portal may run (keyedBind)
	up string
}

type outMsg struct {
	msg     pgproto3.FrontendMessage
	mode    mode
	cap     *capture
	onOK    func()
	notify  chan bool
	wantTag string
	tagAs   string
}

type session struct {
	srv *Server
	db  string
	log *slog.Logger

	cliConn net.Conn
	cliIn   *pgproto3.Backend // read by the main goroutine
	cliOut  *clientOut        // written under cliMu (rawrelay.go)
	cliMu   sync.Mutex
	pending int                // bytes buffered for the client, under cliMu
	upIn    *pgproto3.Frontend // read by the relay goroutine until it holds no bytes of its own; then upRaw
	upConn  net.Conn
	upRaw   *frameReader       // the cloud's frames, read by the relay (rawrelay.go)
	upOut   *pgproto3.Frontend // written by the main goroutine

	meta      *Meta
	metaDirty bool
	// metaRole: the role the session's metadata were read under (current_user, the shared
	// cache's key); metaRoleMoved: a SET ROLE, SET SESSION AUTHORIZATION, RESET ALL or DISCARD
	// ALL ran since (or one the proxy could not tell what it did to the role): the next look
	// asks the cloud who the session is (meta_cache.go: recheck needs the role as it is)
	metaRole      string
	metaRoleMoved bool
	// svcPrep: the proxy's own statements (set_config of lokadb.org, the pause lock) the upstream
	// connection is known to have prepared (svcStmt bits), set when the server answered the Parse;
	// svcEpoch: counts the DEALLOCATE ALL / DISCARD ALL of the client, which drop them (a
	// confirmation of an older Parse is not one); svcSent: the ones this batch sends the Parse of
	// keyTexts, keyedStmts: statements with the partition key's condition of an organization
	// (keycond.go, F7): the simple protocol's texts, the extended one's statements Parsed
	// under names of the proxy's own (keyedN numbers them)
	keyTexts   map[string]string
	keyedStmts map[string]*keyedStmt
	keyedUse   uint64 // counts the Binds of keyedStmts: keyedStmt.used, the least recently used goes first
	// inlineStmts: the connection's statements that set lokadb.org from a refs table within the
	// statement's exchange (inlinelookup.go), by refs table
	inlineStmts map[string]*keyedStmt
	keyedN      int
	svcPrep     atomic.Uint32
	// prep: the direct path's statements of its own prepared on the cloud connection
	// (direct_prep.go); svcEpoch tells when the client's DISCARD ALL dropped them
	prep     prepCache
	svcEpoch atomic.Uint32
	svcSent  uint32
	// metaNotified: a lokadb command changed the metadata (NOTIFY lokadb_meta, set by the relay)
	metaNotified atomic.Bool
	metaVer      int64     // the database's schema version the metadata was read at
	metaAt       time.Time // when it was read
	metaFails    int       // reads in a row that failed (metaBackoff)
	q            *queue
	tx           atomic.Uint32 // status byte of the last ReadyForQuery
	// the response being timed (metrics.go): the organization of its last statement on LokaDB
	// tables, when its first message came, whether it held an error
	mStat  atomic.Pointer[orgStat]
	mStart atomic.Int64
	mErr   atomic.Bool

	stmts   map[string]*stmtInfo
	portals map[string]*portalInfo
	batch   []pgproto3.FrontendMessage
	batchNo int
	// batchBytes: about what the held batch takes (maxBatchBytes)
	batchBytes int
	aliasN     int // the last lokadb_v<n> statement name given (relink)
	parseN     int // the last stmtInfo.seq given
	// forgotten: what DEALLOCATE / DISCARD ALL dropped upstream, applied by the main goroutine
	// (the relay reports their success), under forgetMu.
	forgetMu  sync.Mutex
	forgotten []forgetting
	// unsynced: the last batch went with a Flush, not a Sync: upstream an implicit
	// transaction may be open, and a query of the proxy's own would end it.
	unsynced bool
	// copyExt: a COPY FROM STDIN through Execute began (the relay saw its CopyInResponse): the
	// server ignored the Sync after the Execute, so the COPY's implicit transaction is open until
	// the client's next Sync, though the batch ended with one (second review of the protocol,
	// P3: a reload at that Sync went with Syncs of its own and committed the COPY early, and an
	// error of the proxy's query rolled it back under the client's CommandComplete).
	copyExt atomic.Bool
	// inPipeline: the batch being handled follows one cut by Flush, so an implicit transaction
	// may be open upstream: the proxy's own queries go through captureExt (see internal)
	inPipeline bool
	// cursors: the cursors over LokaDB tables declared in the open transaction (orgset.go).
	// Postgres runs a cursor's query at each FETCH and fills a cursor WITH HOLD at the COMMIT,
	// under the lokadb.org then in force, which may be another statement's organization: the
	// proxy sets the cursor's own right before them.
	cursors  map[string]cursorAt
	prepared map[string]string // statements PREPAREd with SQL: name → text (execute.go)
	// createdPending: a function or view over LokaDB tables created here (created.go): the
	// metadata of every session is read again once this one's transaction is over
	createdPending bool
	upOrg          string      // what lokadb.org was last set to upstream ("?" = unknown)
	orgDirty       atomic.Bool // an error since: upOrg may have been rolled back (set by the relay)
	// DDL the cloud ran in the open transaction, not replayed yet (see ddl.go); written by
	// the relay goroutine, under ddlMu.
	ddlMu      sync.Mutex
	ddlPending []string
	savepts    []savept
	ddlOpen    bool                         // DDL was sent in this transaction (its responses may still be on the way)
	orgCache   map[string]map[string]string // refs table -> id -> tenant: the session's own (orgcache.go: ownOrgs, or no process cache)
	ownOrgs    bool                         // the open transaction changed what lookups answer: the session's own cache until it ends
	orgVer     int64                        // orgVersion(db) the cache is valid for; orgAt: when it was emptied
	orgAt      time.Time
	anCache    *byteCache[string, *Analysis] // by text, bounded in bytes (bytecache.go)
	anSeen     seenSet                       // texts met once: kept from the second sighting
	track      *txTracker                    // nil unless the workload is recorded
	fanout     *Fanout                       // policy for statements that name no organization
	// tenant: the organization the application named (lokadb.tenant, the cooperative mode,
	// tenant.go), as the statements sent so far leave it; tenantImpl: an implicit transaction
	// of a pipeline is open in its view (SET LOCAL lasts until its Sync).
	tenant     tenantState
	tenantImpl bool
	user       string            // the role the client connected as (Tenant.AllRoles)
	params     map[string]string // the client's startup parameters (the direct connections take them)
	direct     directState       // the direct path, profile fdw+direct (direct_session.go)
	// directBusy: the direct connection a statement of the session runs on now (cancelDirect,
	// read by the goroutine that got the CancelRequest)
	directBusy atomic.Pointer[directConn]
	// ungating: the last release of the direct path's pause lock (ungate) and its organization:
	// sent again when it failed (ungateAgain)
	ungating    *capture
	ungatingOrg string
	cancelKey   string // in Server.sessions
	// cancelPID, cancelSecret: the key the client got (the proxy's own when connections are
	// cached); upPID, upSecret: the connection's, which a cancel is sent with (forwardCancel)
	cancelPID, upPID       uint32
	cancelSecret, upSecret []byte
	// the cache of cloud connections (pool.go): pc, the connection when it is one of the cache;
	// parking, the reset sent (the relay stops after its answer); discardOK, the reset ran;
	// parked, the connection goes back; evict, Postgres refused the role something on it
	pc                        *pooledConn
	parking, discardOK, evict atomic.Bool
	// cancels: the session's cancels on their way upstream (forwardCancel dials up to 5 s);
	// released: its connections went back to the pools (releasing), and a cancel that comes
	// now goes nowhere: the connection may run another client's statement (second review of
	// the protocol, P9). Under cancelMu.
	cancelMu  sync.Mutex
	cancels   int
	released  bool
	parked    bool
	relayDone chan struct{}
	sp        pathState // the search_path (schemas_path.go)
}

func newSession(srv *Server, db string, c net.Conn, be *pgproto3.Backend, hj *pgconn.HijackedConn, meta *Meta, params map[string]string) *session {
	s := &session{
		upOrg: "?", // what ALTER ROLE / DATABASE … SET lokadb.org or a pooler left is not known (review P8)
		srv:   srv, db: db, log: srv.log.With("db", db),
		cliConn: c, cliIn: be, cliOut: newClientOut(c),
		upIn: hj.Frontend, upConn: hj.Conn, upOut: pgproto3.NewFrontend(hj.Conn, hj.Conn),
		meta: meta, q: newQueue(), fanout: srv.fanoutFor(db),
		stmts: map[string]*stmtInfo{}, portals: map[string]*portalInfo{},
		orgCache: map[string]map[string]string{},
	}
	budget := srv.cfg.CacheBytes
	if budget <= 0 {
		budget = defaultCacheBytes
	}
	total := int64(srv.cfg.CacheBytesTotal)
	if total <= 0 {
		total = defaultCacheBytesTotal
	}
	s.anCache = newByteCache[string, *Analysis](budget/2).shared(&srv.cacheBytes, total)
	s.direct.kinds = newByteCache[string, settingKind](budget/8).shared(&srv.cacheBytes, total)
	s.direct.info = newByteCache[*Analysis, *directInfo](budget*3/16).shared(&srv.cacheBytes, total)
	s.direct.infoNamed = newByteCache[*Analysis, *directInfo](budget*3/16).shared(&srv.cacheBytes, total)
	s.tx.Store('I')
	s.metaDirty = meta != nil && meta.Retry // the read at connect failed: again before the first statement
	s.user, s.params = params["user"], params
	s.upPID, s.upSecret = hj.PID, hj.SecretKey
	s.cancelPID, s.cancelSecret = hj.PID, hj.SecretKey
	if srv.pool != nil { // the client gets a key of the proxy's (forwardCancel)
		s.cancelPID, s.cancelSecret = proxyCancelKey()
	}
	s.cancelKey = cancelKey(s.cancelPID, s.cancelSecret)
	s.relayDone = make(chan struct{})
	srv.sessions.Store(s.cancelKey, s)
	s.tenant.session, _ = startupTenant(params, s.tenantCfg().Setting) // a bad value is refused at the connection (handle)
	s.metaVer, s.metaAt = srv.schemaVersion(db).Load(), metaLook()
	if srv.rec != nil {
		s.track = newTxTracker(srv.rec, db)
	}
	return s
}

func (s *session) run() {
	go s.relay()
	defer s.q.close()
	defer s.closeDirect()
	defer s.srv.sessions.CompareAndDelete(s.cancelKey, s)
	defer s.releaseCaches()
	defer func() { // a bug in one session must not take down the proxy and every other session
		if r := recover(); r != nil {
			s.log.Error("session aborted by a proxy bug", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	for {
		msg, err := s.cliIn.Receive()
		if err != nil {
			s.parked = s.park()
			return
		}
		if s.track != nil {
			s.track.observe(msg)
		}
		s.ungateAgain()
		s.closeIdleDirect()
		if s.mStart.Load() == 0 {
			s.mStart.Store(time.Now().UnixNano())
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			s.handleQuery(m.String)
		case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe, *pgproto3.Execute, *pgproto3.Close:
			s.batch = append(s.batch, clientNames(clone(msg))) // the proxy's names are its own (ownnames.go)
			s.batchBytes += msgBytes(msg)
			if len(s.batch) >= maxBatch || s.batchBytes >= maxBatchBytes { // a huge pipeline goes on in pieces, not all held here
				s.handleBatch(&pgproto3.Flush{})
			}
		case *pgproto3.Sync:
			s.handleBatch(&pgproto3.Sync{})
		case *pgproto3.Flush:
			s.handleBatch(&pgproto3.Flush{})
		case *pgproto3.Terminate:
			if s.parked = s.park(); s.parked { // the connection stays for the next client (pool.go)
				return
			}
			_ = s.send([]outMsg{{msg: &pgproto3.Terminate{}}})
			return
		default: // CopyData, CopyDone, CopyFail, FunctionCall
			if s.direct.copy != nil { // a COPY on the direct path (direct_copy.go)
				s.copyMsg(msg)
				continue
			}
			if len(s.batch) > 0 { // what the client sent before goes first
				s.handleBatch(&pgproto3.Flush{})
			}
			out := []outMsg{{msg: msg}}
			if fc, ok := msg.(*pgproto3.FunctionCall); ok && s.meta != nil {
				// fastpath, by the function's oid: the proxy cannot tell what it reads (set_config,
				// a function of the application's over LokaDB tables). The large objects' alone run,
				// under no organization: they read no LokaDB row (review of routing, R7)
				// The proxy's own statements go by the extended protocol: a simple query would destroy
				// the client's unnamed statement, a fastpath call does not (second review of the
				// protocol, P6)
				if !largeObjectFuncs[fc.Function] {
					_ = s.send(append(raise(raiseSQLCode("0A000",
						fmt.Sprintf("lokadb: a fastpath function call (oid %d) is refused in a database with LokaDB tables: the proxy cannot route it; the large object functions alone are taken", fc.Function))),
						outMsg{msg: &pgproto3.Sync{}}))
					continue
				}
				if d := s.direct.tx; d != nil && d.org == "" {
					s.materialize() // the large objects run in the client's transaction, the cloud's (review PR-2)
				}
				if s.upOrg != noRows { // by the extended protocol, with a Sync of its own: an error of it must not skip the call
					s.svcSent = 0
					out = append(append(s.setOrg(noRows), outMsg{msg: &pgproto3.Sync{}, mode: mDrop}), out...)
					s.upOrg = noRows
				}
			} else if ok {
				s.upOrg = "?" // it may be set_config by its oid
			}
			if err := s.send(out); err != nil {
				return
			}
		}
	}
}

func (s *session) txStatus() byte { return byte(s.tx.Load()) }

// maxBatch, maxBatchBytes: extended-protocol messages held before they are sent on with a
// Flush. Pieces after the first run inside the pipeline's implicit transaction, where the
// proxy cannot look up organizations (P3): a pgx SendBatch or JDBC executeBatch of thousands of
// statements stays in one piece.
const (
	maxBatch      = 100000
	maxBatchBytes = 64 << 20
)

// msgBytes: about what a held message takes.
func msgBytes(m pgproto3.FrontendMessage) int {
	n := 64
	switch m := m.(type) {
	case *pgproto3.Parse:
		n += len(m.Query) + 4*len(m.ParameterOIDs)
	case *pgproto3.Bind:
		for _, p := range m.Parameters {
			n += len(p) + 24
		}
	}
	return n
}

// lookupMsg refuses a statement whose organization the refs tables would tell, in a pipeline
// without Sync (P3).
const lookupMsg = "lokadb: the organization of this statement is found in the refs tables, which the proxy cannot read inside a pipeline without Sync (cut by Flush, or longer than the proxy holds): send a Sync before it, or name the organization column"

func (s *session) analysis(sql string) *Analysis {
	path, key := defaultPath, sql
	if s.meta != nil && s.meta.SchemasUsed { // a bare name depends on the search path (schemas.go)
		path = s.searchPath()
		key = pathKey(path) + "\x02" + sql
	}
	if a, ok := s.anCache.get(key); ok {
		return a
	}
	a := analyzeIn(sql, s.meta, path)
	// kept from the second sighting of the text, within the session's byte budget: a text of
	// inlined literals is met once, and a huge VALUES list is rarely sent twice (PF7)
	if size := textBytes(key); size <= s.anCache.max() && s.anSeen.again(key) {
		s.anCache.put(key, a, size)
	}
	return a
}

// pathNow: the key of the search path the session's analyses are made with ("" while no bare
// name depends on it).
func (s *session) pathNow() string {
	if s.meta == nil || !s.meta.SchemasUsed {
		return ""
	}
	return pathKey(s.searchPath())
}

// current is st's analysis against the metadata in force, made again when that changed since
// it was made: a statement prepared before enable (none), or before a schema change or a move
// (columns, types, foreign partitions of another age).
func (s *session) current(st *stmtInfo) *Analysis {
	if p := s.pathNow(); st.an == nil || st.meta != s.meta || st.path != p {
		st.an, st.meta, st.path = s.analysis(st.sql), s.meta, p
	}
	return st.an
}

// newStmt records a statement Parsed by the client (its text sent upstream as is).
func (s *session) newStmt(m *pgproto3.Parse) *stmtInfo {
	s.parseN++
	st := &stmtInfo{sql: m.Query, oids: m.ParameterOIDs, sent: m.Query, seq: s.parseN}
	st.forgets, st.forgetsAll = deallocates(m.Query)
	st.unlistens = dropsListen(m.Query)
	st.holds = declaresHold(m.Query)
	st.tset, st.terr = stmtTenant(m.Query, s.tenantCfg())
	return st
}

// wants: the text st's analysis wants run; relink is needed when it is not what was sent.
func wants(st *stmtInfo, an *Analysis) string {
	if an.Rewritten != "" {
		return an.Rewritten
	}
	return st.sql
}

// relink points the client's Bind at the text the analysis wants when the text Parsed
// upstream under the client's name is another (prepared before enable, or before a schema
// change: an upsert to rewrite, casts of other types). The text goes under a statement name of
// the proxy's own, one per client statement, parsed again once per batch (closing it would end
// the portals bound from it in the same batch); the client's own statement is left alone.
//
// The alias is parsed with the parameter types the server gave the client's statement (the
// client encodes for them; the text's own casts convert). Without them (the unnamed
// statement after a failed batch, an aborted transaction) the client's statement runs as it was
// prepared.
func (s *session) relink(inject map[int][]outMsg, idx int, b *pgproto3.Bind, st *stmtInfo, an *Analysis, relinked map[*stmtInfo]bool) {
	want := wants(st, an)
	if want == st.sent || st.ptypes == nil {
		return
	}
	if st.alias == "" {
		s.aliasN++
		st.alias = fmt.Sprintf("lokadb_v%d", s.aliasN)
	}
	if !relinked[st] {
		inject[idx] = append(inject[idx],
			outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: st.alias}, mode: mDrop},
			outMsg{msg: &pgproto3.Parse{Name: st.alias, Query: want, ParameterOIDs: st.ptypes}, mode: mDrop})
		relinked[st] = true
	}
	b.PreparedStatement = st.alias
}

// paramTypes reads the parameter types the server gave the client's prepared statement
// name (idle only: a query of the proxy's own). nil if it cannot tell.
func (s *session) paramTypes(name string) []uint32 {
	if name == "" { // the unnamed statement is not in pg_prepared_statements: Describe it
		return s.describeTypes()
	}
	rows, err := s.internal("SELECT parameter_types::oid[]::text FROM pg_prepared_statements WHERE name = " + sqlString(name))
	if err != nil || len(rows) != 1 {
		return nil
	}
	list := strings.Trim(rows[0][0], "{}")
	types := []uint32{}
	if list == "" {
		return types
	}
	for _, f := range strings.Split(list, ",") {
		n, err := strconv.ParseUint(f, 10, 32)
		if err != nil {
			return nil
		}
		types = append(types, uint32(n))
	}
	return types
}

// relinkIf relinks unless the transaction is aborted (it refuses the Parse; the client's
// Bind fails there anyway).
func (s *session) relinkIf(tx byte, inject map[int][]outMsg, idx int, b *pgproto3.Bind, st *stmtInfo, an *Analysis, relinked map[*stmtInfo]bool) {
	if tx != 'E' {
		s.relink(inject, idx, b, st, an, relinked)
	}
}

// forgetting is a DEALLOCATE or DISCARD ALL that ran: what it dropped, of what was Parsed
// before it (seq).
type forgetting struct {
	names []string
	all   bool
	seq   int
}

// forgetOnOK: an onOK that records, once the server ran it, what the statement dropped.
// seq: the last Parse before it, in message order.
func (s *session) forgetOnOK(names []string, all bool, seq int) func() {
	if !all && len(names) == 0 {
		return nil
	}
	if all { // the statements of the proxy's own go too: prepared again from the next use (svc)
		s.svcEpoch.Add(1)
		s.svcPrep.Store(0)
		s.svcSent = 0
	}
	f := forgetting{names: names, all: all, seq: seq}
	return func() {
		s.forgetMu.Lock()
		s.forgotten = append(s.forgotten, f)
		s.forgetMu.Unlock()
	}
}

// forget drops (in the main goroutine) what DEALLOCATE or DISCARD ALL dropped upstream, so that
// a later Bind of such a name is refused as Postgres would, not run through an alias.
func (s *session) forget() {
	s.forgetMu.Lock()
	fs := s.forgotten
	s.forgotten = nil
	s.forgetMu.Unlock()
	for _, f := range fs {
		for name, st := range s.stmts {
			if st.seq <= f.seq && (f.all || slices.Contains(f.names, name)) {
				delete(s.stmts, name)
			}
		}
		if f.all {
			s.portals = map[string]*portalInfo{} // DISCARD ALL drops them (after DEALLOCATE ALL they are unusable anyway)
		}
	}
}

// deallocates: the prepared statements sql drops (DEALLOCATE name), or all of them
// (DEALLOCATE ALL, DISCARD ALL, which also drops the portals).
func deallocates(sql string) (names []string, all bool) {
	if !hasFold(sql, "deallocate") && !hasFold(sql, "discard") {
		return nil, false
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil, false
	}
	for _, raw := range tree.Stmts {
		if d := raw.Stmt.GetDeallocateStmt(); d != nil {
			if d.Isall {
				all = true
			} else {
				names = append(names, d.Name)
			}
		}
		if d := raw.Stmt.GetDiscardStmt(); d != nil && d.Target == pg.DiscardMode_DISCARD_ALL {
			all = true
		}
	}
	return names, all
}

func (s *session) handleQuery(sql string) {
	// a pipeline cut by Flush before it: the cloud has its implicit transaction open, which
	// a statement on the direct path would not close (review of block 9, 5)
	wasUnsynced := s.unsynced || s.copyExt.Swap(false)
	// a query of the proxy's own before the client's must not end that implicit transaction
	// (review P3): through captureExt with Flush, and no reload in front of it
	s.unsynced, s.inPipeline = false, wasUnsynced
	s.direct.lostUnnamed = nil // a simple query destroys the unnamed statement
	s.forget()
	names, all := deallocates(sql)
	forgets := s.forgetOnOK(names, all, s.parseN)
	if dropsListen(sql) {
		defer s.relisten()
	}
	if s.meta == nil {
		if s.txStatus() == 'I' && s.q.empty() && !wasUnsynced {
			s.refresh('I') // enabled since this connection was opened?
		}
		if s.meta == nil {
			_ = s.send([]outMsg{{msg: &pgproto3.Query{String: sql}, onOK: chainOK(forgets, s.noteSettings(sql, s.txStatus() != 'I'))}})
			return
		}
	}
	ok, dead := s.q.waitEmpty()
	if !ok {
		return
	}
	if dead { // the server skips to the next Sync: nothing of the proxy's would run
		_ = s.send([]outMsg{{msg: &pgproto3.Query{String: sql}, onOK: chainOK(forgets, s.noteSettings(sql, true))}})
		return
	}
	tx := s.txStatus()
	if !wasUnsynced { // at the next query: not inside the client's implicit transaction (review P3)
		s.refresh(tx)
	}
	if s.meta == nil { // disabled since: as before enable
		_ = s.send([]outMsg{{msg: &pgproto3.Query{String: sql}, onOK: forgets}})
		return
	}
	if s.orgDirty.Swap(false) {
		s.upOrg = "?"
	}
	if tx == 'I' {
		s.cursors = nil
	}
	an := s.analysis(sql)
	if msg := an.rejection(); msg != "" {
		s.respondError(msg, tx)
		return
	}
	if an.MayResetOrg { // whatever is set below, the string may change it (review P8)
		defer func() { s.upOrg = "?" }()
	}
	if an.Lokadb && an.ResetsOrg {
		s.respondError(orgSetMsg, tx)
		return
	}
	// EXECUTE of a statement PREPAREd with SQL: routed by that statement (execute.go)
	vals := constOnly
	if ex, v, msg := s.executeAnalysis(sql, false); msg != "" {
		s.respondErrorCode("0A000", msg, tx)
		return
	} else if ex != nil {
		an, vals = ex, v
	}
	s.notePrepare(sql)
	s.noteCreated(sql)
	s.learnNonLokadb(sql, an)
	// a deferred or direct transaction (direct_tx.go, 8a.2): its statements, or its BEGIN sent
	// to the cloud first
	if s.direct.tx != nil || an.TxControl {
		if s.directTxQuery(sql, an, wasUnsynced) {
			return
		}
		tx = s.txStatus()
	}
	// The cooperative mode (tenant.go): the organization the application named for the
	// string's statements on LokaDB tables, the value after its last SET lokadb.tenant.
	tevs, tsets, tlast, terr := tenantString(sql, s.tenantCfg())
	if terr != nil {
		s.respondErrorCode(tenantCode(terr), "lokadb: "+terr.Error(), tx)
		return
	}
	if len(tsets) > 0 && an.Lokadb {
		for _, st := range an.Stmts[:tlast] {
			if s.analysis(st).Lokadb {
				s.respondError("lokadb: a query string names the organization (SET lokadb.tenant) after a statement on LokaDB tables: name it first, or in a query of its own", tx)
				return
			}
		}
	}
	implicit := tx == 'I' && !hasEvent(an.Events, 'B') // the string is a transaction of its own
	named := s.tenant.Value()
	if len(tsets) > 0 {
		c := s.tenant.clone()
		if implicit {
			c.begin()
		}
		k := 0
		for i, e := range tevs {
			if e.Kind == 'N' {
				k = i
			}
		}
		c.apply(tevs[:k+1], tsets)
		named = c.Value()
	}
	evs := an.Events
	if s.refuseNamed(sql, an, false, named) {
		_ = s.send([]outMsg{{msg: &pgproto3.Query{String: s.fanout.raiseSQL(an)}}})
		return
	}
	// profile fdw+direct: straight to the organization's database (direct_session.go; a COPY
	// FROM STDIN of the organization the application named: direct_copy.go)
	if tx == 'I' && len(tsets) == 0 && !wasUnsynced && (s.directCopyStart(sql, an) || s.directQuery(sql, an)) {
		return
	}
	var out []outMsg
	var unlocks []outMsg // release the session locks of an autocommit query (orgLock, upsertLock)
	fetchOrg, fetching := "", false
	keyOrg := "" // the organization of the statement the partition key's condition is added for
	if !an.Lokadb {
		var err error
		if fetchOrg, fetching, err = s.fetchOrg(sql, named); err != nil {
			s.respondErrorCode("0A000", "lokadb: "+err.Error(), tx)
			return
		}
	}
	if !an.Lokadb && !fetching { // closed by default (openOrg): no LokaDB row unless named or allowed
		fetchOrg = s.openOrg(sql, named)
	}
	switch {
	case an.Lokadb && (tx == 'E' || an.OrgReverted):
		// lokadb.org is set by a query sent before this one: a failed transaction rejects it,
		// a ROLLBACK inside the string undoes it, and the LokaDB statements would run under
		// whatever organization was set before.
		s.respondError("lokadb: a ROLLBACK and statements on LokaDB tables in one query string: send the ROLLBACK as a query of its own", tx)
		return
	case an.ResetsOrg:
		// the client's SET, RESET, set_config of lokadb.org or DISCARD ALL (no LokaDB table, or
		// refused above): what reads rows after it in the string would run under its value
		// (empty: every organization), what reads rows before it under the one set here
		// (second review of routing: closed by default on every way)
		if s.readsAfter(an, func(sa *Analysis) bool { return sa.ResetsOrg }) {
			s.respondError(orgSetMsg, tx)
			return
		}
		if tx != 'E' && s.readsRows(an) && s.upOrg != fetchOrg {
			out = append(out, outMsg{msg: &pgproto3.Query{String: "SELECT set_config('lokadb.org', " + sqlString(fetchOrg) + ", false)"}, mode: mDrop})
		}
		s.upOrg = "?"
	case an.Utility: // nothing to set, and SET TRANSACTION must stay first in the transaction
	case !an.Lokadb && (tx == 'E' || an.TxControl):
		// A string with a transaction's statement: one of its other statements reads rows under
		// the lokadb.org set before the string (closed by default, openOrg), not under the one
		// a statement before left (second review of routing: '' on a new connection, every
		// organization). After a ROLLBACK (TO) inside the client's transaction, or any end of
		// an aborted one, the value is the one the transaction began or saved with: a statement
		// that reads rows there is refused, as the same string with LokaDB tables is.
		if s.readsAfter(an, func(sa *Analysis) bool { return sa.TxControl && (sa.Reverts || tx == 'E') }) && tx != 'I' {
			s.respondError("lokadb: a ROLLBACK (or the end of an aborted transaction) and statements after it in one query string: send the ROLLBACK as a query of its own", tx)
			return
		}
		org, set := fetchOrg, tx != 'E' && s.readsRows(an)
		if h, ok := s.holdOrg(); ok && tx == 'T' && an.Commit { // cursors WITH HOLD are filled at the COMMIT
			org, set = h, true
		}
		if set && s.upOrg != org {
			out = append(out, outMsg{msg: &pgproto3.Query{String: "SELECT set_config('lokadb.org', " + sqlString(org) + ", false)"}, mode: mDrop})
		}
		s.upOrg = "?"
		if set && (tx == 'I' || !an.Reverts) { // a session value, kept unless the string's ROLLBACK undoes it
			s.upOrg = org
		}
	case an.Lokadb || s.upOrg != fetchOrg || fetching:
		org := fetchOrg
		if an.Lokadb {
			if named != "" && named != TenantAll { // named by the application: reconciled, not guessed
				org = named
				if err := s.tenantOrg(sql, an, named, vals); err != nil {
					code, msg := tenantError(err)
					s.respondErrorCode(code, msg, s.txStatus())
					return
				}
			} else {
				var unknown bool
				var err error
				org, _, unknown, err = s.resolveOrg(an, vals, true)
				if err != nil {
					s.respondError("lokadb: "+err.Error(), s.txStatus())
					return
				}
				if unknown && s.refuseNamed(sql, an, true, named) {
					_ = s.send([]outMsg{{msg: &pgproto3.Query{String: s.fanout.raiseSQL(an)}}})
					return
				}
			}
		}
		if an.Lokadb {
			if err := s.noteCursors(sql, org, named); err != nil {
				s.respondErrorCode("0A000", "lokadb: "+err.Error(), s.txStatus())
				return
			}
			if why := s.directOnlyRefusal(sql, an, org, named != "" && named != TenantAll); why != "" { // profile direct: no cloud way
				if s.txStatus() != 'I' { // the server raises it: its transaction is aborted, as Postgres's would be (block 9's review)
					_ = s.send([]outMsg{{msg: &pgproto3.Query{String: raiseSQLCode("0A000", "lokadb: "+why)}}})
					return
				}
				s.respondErrorCode("0A000", "lokadb: "+why, s.txStatus())
				return
			}
			s.learnStmt(sql, org, named != "" && named != TenantAll) // a transaction learnt for the direct path (direct_tx.go)
		}
		setOrg := ""
		if s.upOrg != org { // already this one upstream: not set again (block 9, review PF3)
			setOrg = "set_config('lokadb.org', " + sqlString(org) + ", false)"
		}
		s.upOrg = org
		// the pause lock in the same query as set_config: one statement of the cloud's, not two
		// (third review of performance, perf3-12)
		lockFn := ""
		if key, ok := s.orgLock(an, org); ok {
			lock := s.lockCall(key)
			switch {
			case tx == 'I' && an.TxControl:
				// A string that opens a transaction itself: a session lock might outlive it
				// (an aborted transaction refuses the unlock). Its later statements lock.
			case tx == 'I':
				// Each query string is a transaction of its own: a session lock, released
				// right after it (a transaction lock taken here would end with this query).
				lockFn = "pg_advisory_lock_shared(" + lock + ")"
				unlocks = append(unlocks, outMsg{msg: &pgproto3.Query{String: "SELECT pg_advisory_unlock_shared(" + lock + ")"}, mode: mDrop})
			default:
				lockFn = "pg_advisory_xact_lock_shared(" + lock + ")"
			}
		}
		if q := strings.Join(slices.DeleteFunc([]string{setOrg, lockFn}, func(x string) bool { return x == "" }), ", "); q != "" {
			out = append(out, outMsg{msg: &pgproto3.Query{String: "SELECT " + q}, mode: mDrop})
		}
		if tx != 'E' { // an aborted transaction would refuse it and skip the client's ROLLBACK
			for _, p := range an.Prelude {
				out = append(out, outMsg{msg: &pgproto3.Query{String: p}, mode: mDrop})
			}
		}
		part := s.foreignPart(an, org)
		if key := upsertLock(an, org, part); key != "" && tx != 'E' {
			if tx == 'I' { // as the pause lock: a transaction lock would end with this query
				out = append(out, outMsg{msg: &pgproto3.Query{String: "SELECT pg_advisory_lock(" + upsertLockArgs(sqlString(key)) + ")"}, mode: mDrop})
				unlocks = append(unlocks, outMsg{msg: &pgproto3.Query{String: "SELECT pg_advisory_unlock(" + upsertLockArgs(sqlString(key)) + ")"}, mode: mDrop})
			} else {
				out = append(out, outMsg{msg: &pgproto3.Query{String: "SELECT pg_advisory_xact_lock(" + upsertLockArgs(sqlString(key)) + ")"}, mode: mDrop})
			}
		}
		if part != "" {
			sql = an.targetSQL(part, org)
			an = &Analysis{Lokadb: true, Rewritten: sql, Target: part}
		} else if an.Lokadb && len(an.Stmts) == 1 && an.Rewritten == "" {
			keyOrg = org // the partition key's condition (keycond.go, F7); a statement the proxy rewrites goes as it is
		}
		if an.Lokadb {
			s.trace(sql, an, org)
		}
		if an.Reverts && tx != 'I' { // the ROLLBACK also undoes the set_config sent inside this transaction
			s.upOrg = "?"
		}
	}
	// what the string leaves of lokadb.tenant (an implicit transaction drops its SET LOCAL)
	if implicit {
		s.tenant.begin()
	}
	if tevs != nil {
		s.tenant.apply(tevs, tsets)
	} else {
		s.tenant.apply(evs, nil)
	}
	if implicit {
		s.tenant.commit()
	}
	q := sql
	if an.Rewritten != "" {
		q = an.Rewritten
	}
	if keyOrg != "" {
		q = s.keyedText(q, keyOrg)
	}
	final := outMsg{msg: &pgproto3.Query{String: q}, onOK: forgets, tagAs: indexTag(an)}
	if len(evs) > 0 {
		final.onOK = func() {
			s.applyEvents(evs)
			if forgets != nil {
				forgets()
			}
		}
	}
	final.onOK = chainOK(final.onOK, s.noteSettings(sql, tx != 'I' || hasEvent(evs, 'B')))
	final.onOK = chainOK(final.onOK, s.learnEnd(sql, an, tx))
	ddl, commit := hasEvent(evs, 'D'), hasEvent(evs, 'C')
	switch {
	case ddl && tx == 'I':
		s.autocommitDDL(out, final, an)
		return
	case commit && tx == 'T' && (ddl || s.ddlOpen || s.hasPending()):
		if len(an.Stmts) != 1 {
			s.respondError("lokadb: this transaction changed the schema of LokaDB tables: send its COMMIT as a query of its own", tx)
			return
		}
		s.commitQuery(out, final)
		return
	}
	if ddl {
		s.ddlOpen = true
	}
	out = append(out, final)
	out = append(out, unlocks...)
	_ = s.send(out)
}

// batchStep is a Bind or an Execute of a batch, with what it binds or runs.
type batchStep struct {
	idx  int  // position in the batch: injected messages go right before it
	bind bool // Bind; otherwise Execute
	st   *stmtInfo
	p    *portalInfo
}

func (s *session) handleBatch(final pgproto3.FrontendMessage) {
	batch := append(s.batch, final)
	s.batch, s.batchBytes = nil, 0
	s.batchNo++
	s.svcSent = 0
	_, isSync := final.(*pgproto3.Sync)
	s.forget()
	unsynced := s.unsynced || s.copyExt.Swap(false) // as the previous batch left the upstream connection
	s.inPipeline = unsynced
	// A batch cut by Flush leaves an implicit transaction open upstream only when it ran
	// something of the client's outside a transaction block.
	ranSome, execs := false, 0
	for _, m := range batch {
		if _, ok := m.(*pgproto3.Execute); ok {
			ranSome = true
			execs++
		}
	}
	s.unsynced = !isSync && (unsynced || ranSome && s.txStatus() == 'I')
	// onOK of the Executes of DEALLOCATE / DISCARD ALL: what they drop is forgotten once they ran
	forgets := map[int]func(){}
	// listenAt: Executes of UNLISTEN * / DISCARD ALL, after which the proxy listens again
	listenAt := map[int]bool{}
	raw := func() {
		out := make([]outMsg, 0, len(batch))
		for i, m := range batch {
			out = append(out, outMsg{msg: m, onOK: forgets[i]})
			if listenAt[i] {
				out = append(out, listenMsgs()...)
			}
		}
		_ = s.send(out)
	}
	// Fresh metadata before anything of the batch is analysed: enable ran since the
	// connection opened, or the schema changed. Not inside a pipeline cut by Flush (a query of
	// the proxy's own would end the client's implicit transaction), but at its start, Flush or
	// Sync: a client that always cuts its pipelines with Flush must not keep old metadata.
	if !unsynced && (s.meta == nil || s.refreshDue()) {
		if s.meta == nil {
			if s.txStatus() == 'I' && s.q.empty() {
				s.refresh('I') // enabled since this connection was opened?
			}
		} else {
			ok, dead := s.q.waitEmpty()
			if !ok {
				return
			}
			if !dead {
				s.refresh(s.txStatus())
			}
		}
	}
	// The server skips everything up to the next Sync after an error: the batch goes as it
	// is (its statements and portals will not exist upstream either).
	if s.q.skipping() {
		if isSync && s.tenantImpl { // the failed implicit transaction drops what it set
			s.tenant.rollback()
			s.tenantImpl = false
		}
		raw()
		return
	}
	// No metadata (not enabled, or enabled since): the batch goes as it is, but its statements
	// and portals are recorded, so that they are analysed when bound after enable (a pool's
	// statement cache outlives enable).
	if s.meta == nil {
		for i, msg := range batch {
			switch m := msg.(type) {
			case *pgproto3.Parse:
				s.stmts[m.Name] = s.newStmt(m)
			case *pgproto3.Bind: // not its parameters: nothing reads them later
				s.portals[m.DestinationPortal] = &portalInfo{stmt: m.PreparedStatement, st: s.stmts[m.PreparedStatement]}
			case *pgproto3.Close:
				if m.ObjectType == 'S' {
					delete(s.stmts, m.Name)
				} else {
					delete(s.portals, m.Name)
				}
			case *pgproto3.Execute:
				if p := s.portals[m.Portal]; p != nil && p.st != nil {
					forgets[i] = s.forgetOnOK(p.st.forgets, p.st.forgetsAll, s.parseN)
					listenAt[i] = p.st.unlistens
				}
			}
		}
		raw()
		return
	}
	// BEGIN by the extended protocol at the batch's head (JDBC with autocommit off): deferred as a
	// simple one is (direct_tx.go), the rest of the batch goes on in the deferred transaction
	if rest, ok := s.extBegin(batch, isSync, unsynced); ok {
		if batch = rest; len(batch) == 1 { // the Sync alone
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return
		}
	}
	// profile fdw+direct: a statement straight to the organization's database (direct_session.go),
	// in a deferred or direct transaction too (direct_tx.go)
	if s.direct.tx != nil {
		if s.directTxBatch(batch, isSync, unsynced) {
			return
		}
	} else if isSync && !unsynced && s.directBatch(batch) {
		return
	}

	// lokadb.org is decided per Bind (initial partition pruning runs there) and restored before
	// each Execute when something else set it in between: interleaved portals (Bind P1,
	// Bind P2, Execute P1, Execute P2) or a portal bound in an earlier batch.
	var steps []batchStep
	needIdle := false
	inject := map[int][]outMsg{}
	// pre: lokadb.tenant as the batch goes (the cooperative mode): a statement under a named
	// organization needs no lookup (tenantOrg)
	pre := s.tenant.clone()
	if s.txStatus() == 'I' && !unsynced && !s.tenantImpl {
		pre.begin()
	}
	seqAt := map[int]int{} // the last Parse before each Execute (forget)
	closeAlias := func(i int, st *stmtInfo) {
		if st != nil && st.alias != "" {
			inject[i] = append(inject[i], outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: st.alias}, mode: mDrop})
		}
	}
	// the unnamed statement a direct batch took (the cloud never had it) and the client uses
	// again without a Parse: Parsed in the cloud before its first use here (PR-14)
	if lost := s.direct.lostUnnamed; lost != nil && s.stmts[""] == nil {
	scan:
		for i, msg := range batch {
			uses := false
			switch m := msg.(type) {
			case *pgproto3.Parse:
				if m.Name == "" {
					break scan // a Parse of its own: the statement is the client's new one
				}
			case *pgproto3.Bind:
				uses = m.PreparedStatement == ""
			case *pgproto3.Describe:
				uses = m.ObjectType == 'S' && m.Name == ""
			}
			if uses {
				p := *lost
				st := s.newStmt(&p)
				if an := s.current(st); an.Rewritten != "" {
					p.Query = an.Rewritten
				}
				st.sent = p.Query
				s.stmts[""] = st
				inject[i] = append(inject[i], outMsg{msg: &p, mode: mDrop})
				break
			}
		}
	}
	if s.stmts[""] != nil || len(batch) > 0 && batchParsesUnnamed(batch) {
		s.direct.lostUnnamed = nil // an unnamed statement is there again (or this batch makes one): the lost one is gone for good
	}
	for i, msg := range batch {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			closeAlias(i, s.stmts[m.Name]) // a statement replaced under the same name
			st := s.newStmt(m)
			if an := s.current(st); an.Rewritten != "" {
				m.Query = an.Rewritten
			}
			st.sent = m.Query
			s.stmts[m.Name] = st
		case *pgproto3.Bind:
			st := s.stmts[m.PreparedStatement]
			p := &portalInfo{stmt: m.PreparedStatement, st: st, params: m.Parameters,
				formats: m.ParameterFormatCodes, batch: s.batchNo, bindIdx: i}
			s.portals[m.DestinationPortal] = p
			steps = append(steps, batchStep{idx: i, bind: true, st: st, p: p})
			if st == nil {
				continue
			}
			an := s.current(st)
			if named := pre.Value(); an.Lokadb && !an.TxControl && (named == "" || named == TenantAll) {
				if _, incomplete, _, _ := s.resolveOrg(an, p.values, false); incomplete {
					if _, _, inline := s.inlineKey(an, p.values, batch, i, m, s.txStatus()); !inline { // found within its exchange (inlinelookup.go)
						needIdle = true
					}
				}
			}
			p.tset, _ = boundTenant(st, p)
			if wants(st, an) != st.sent {
				if st.ptypes == nil {
					needIdle = true // its parameter types, for the alias (relink)
				}
				if st.alias == "" { // named now: a Close later in the batch closes it too
					s.aliasN++
					st.alias = fmt.Sprintf("lokadb_v%d", s.aliasN)
				}
			}
		case *pgproto3.Close:
			if m.ObjectType == 'S' {
				closeAlias(i, s.stmts[m.Name])
				delete(s.stmts, m.Name)
			} else {
				delete(s.portals, m.Name)
			}
		case *pgproto3.Execute:
			seqAt[i] = s.parseN
			p := s.portals[m.Portal]
			if p == nil || p.st == nil {
				continue
			}
			listenAt[i] = p.st.unlistens
			steps = append(steps, batchStep{idx: i, st: p.st, p: p})
			pre.apply(s.current(p.st).Events, nil)
			if p.tset != nil {
				pre.set(*p.tset)
			}
		}
	}

	// lookups in the refs tables wait for every pending response; inside a pipeline (1b) they
	// go through the extended protocol, which keeps its implicit transaction open (internal)
	idle := needIdle
	if idle {
		ok, dead := s.q.waitEmpty()
		if !ok {
			return
		}
		if dead { // an earlier batch failed after it was analysed: idle work is impossible now
			idle = false
		}
	}
	tx := s.txStatus()
	txNow := tx // as the batch goes: a ROLLBACK (TO) in it ends an aborted state
	if tx == 'I' && !unsynced {
		s.cursors = nil
	}
	relinked := map[*stmtInfo]bool{}
	refused := false
	if s.orgDirty.Swap(false) {
		s.upOrg = "?"
	}
	cur := s.upOrg // lokadb.org upstream at this point of the batch ("?" = unknown)
	// a deferred transaction (direct_tx.go): the cloud is outside one, the client inside; its
	// SET LOCAL lokadb.tenant lasts to its end, not to this Sync (TestDirectTxTenantExtended)
	if tx == 'I' && !unsynced && !s.tenantImpl && s.direct.tx == nil { // SET LOCAL lokadb.tenant lasts until the Sync
		s.tenant.begin()
		s.tenantImpl = true
	}
	for _, e := range steps {
		if !e.bind {
			an := e.p.an
			if an == nil { // bound before the session had metadata, in the same pipeline: as bound
				continue
			}
			s.tenantApply(an.Events, e.p.tset)
			forgets[e.idx] = s.forgetOnOK(e.st.forgets, e.st.forgetsAll, seqAt[e.idx])
			// a setting the direct connections repeat: known to have stayed only when it is
			// the batch's one statement outside a transaction (a later error would roll it back)
			forgets[e.idx] = chainOK(forgets[e.idx], s.noteSettings(e.st.sql, txNow != 'I' || unsynced || !isSync || execs > 1))
			if an.TxControl {
				if org, ok := s.holdOrg(); ok && an.Commit && txNow != 'E' && cur != org {
					inject[e.idx] = append(inject[e.idx], s.setOrg(org)...) // see cursors
				}
				if an.Reverts && txNow == 'E' {
					txNow = 'T'
				}
				// A rollback also reverts set_config, so the upstream value is unknown afterwards.
				cur = "?"
				continue
			}
			if an.ResetsOrg { // the client's SET lokadb.org runs here: a later Execute sets it again
				cur = "?"
				continue
			}
			if e.p.orgSet && cur != e.p.org {
				inject[e.idx] = append(inject[e.idx], s.setOrg(e.p.org)...)
				cur = e.p.org
			}
			if an.MayResetOrg { // it may change lokadb.org itself (review P8)
				cur = "?"
			}
			continue
		}
		b := batch[e.idx].(*pgproto3.Bind)
		if e.st == nil {
			// Not Parsed by this client through the proxy (SQL PREPARE, or a name never
			// prepared): it would run unanalysed. Postgres refuses an unknown name the same way.
			inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode("26000",
				fmt.Sprintf("lokadb: prepared statement %q does not exist here (statements prepared with SQL PREPARE cannot be bound through the proxy)", b.PreparedStatement)))...)
			refused = true
			continue
		}
		an := s.current(e.st) // against the metadata in force: prepared before enable, a schema change
		e.p.an = an
		if _, _, msg := s.executeAnalysis(e.st.sql, true); msg != "" { // EXECUTE of a SQL PREPARE on LokaDB tables (execute.go)
			inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode("0A000", msg))...)
			refused = true
			continue
		}
		s.notePrepare(e.st.sql)
		s.noteCreated(e.st.sql)
		if e.st.ptypes == nil && idle && txNow != 'E' && wants(e.st, an) != e.st.sent {
			e.st.ptypes = s.paramTypes(b.PreparedStatement)
		}
		// Refused by the proxy (LokaDB's own objects, DDL it cannot replay): the server raises
		// the error before the client's Bind and skips to Sync, Flush or not.
		if msg := an.rejection(); msg != "" {
			inject[e.idx] = append(inject[e.idx], raise(raiseSQL(msg))...)
			refused = true
			continue
		}
		if an.Lokadb && an.ResetsOrg { // set_config('lokadb.org', …) in a statement on LokaDB tables
			inject[e.idx] = append(inject[e.idx], raise(raiseSQL(orgSetMsg))...)
			refused = true
			continue
		}
		// the cooperative mode: a statement that sets lokadb.tenant, its value checked here;
		// the organization the application named for this one
		ts, terr := boundTenant(e.st, e.p)
		if terr != nil {
			inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode(tenantCode(terr), "lokadb: "+terr.Error()))...)
			refused = true
			continue
		}
		e.p.tset = ts
		named := s.tenant.Value()
		if an.TxControl {
			// An aborted transaction rejects everything but ROLLBACK: an injected
			// statement would fail and make the server skip the client's ROLLBACK.
			continue
		}
		// A refused statement: the server raises the error before the client's Bind and skips the rest of the batch.
		if s.refuseNamed(e.st.sql, an, false, named) {
			inject[e.idx] = append(inject[e.idx], raise(s.fanout.raiseSQL(an))...)
			refused = true
			continue
		}
		if an.ResetsOrg {
			cur = "?" // the client sets or resets lokadb.org itself
			s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
			continue
		}
		forg, fetching, ferr := s.fetchOrg(e.st.sql, named)
		if ferr != nil && !an.Lokadb {
			inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode("0A000", "lokadb: "+ferr.Error()))...)
			refused = true
			continue
		}
		if fetching && !an.Lokadb { // a FETCH or MOVE: its cursor's organization
			if cur != forg {
				inject[e.idx] = append(inject[e.idx], s.setOrg(forg)...)
			}
			cur = forg
			e.p.org, e.p.orgSet = forg, true
			s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
			continue
		}
		open := "" // closed by default (openOrg): no LokaDB row unless named or allowed
		if !an.Lokadb {
			open = s.openOrg(e.st.sql, named)
		}
		if an.Utility || !an.Lokadb && cur == open {
			e.p.org, e.p.orgSet = cur, !an.Utility
			s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
			continue
		}
		org := open
		if an.Lokadb {
			var unknown, incomplete bool
			var err error
			if named != "" && named != TenantAll { // named by the application: reconciled, not guessed
				org = named
				if err = s.tenantOrg(e.st.sql, an, named, e.p.values); err != nil {
					code, msg := tenantError(err)
					inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode(code, msg))...)
					refused = true
					continue
				}
			} else if il, ok := s.inlineLookup(an, e.p.values, batch, e.idx, b, txNow); ok {
				// the cold key's organization set by a statement of the proxy's own ahead of the
				// Bind, in the same exchange (inlinelookup.go): the proxy does not know it
				inject[e.idx] = append(inject[e.idx], il...)
				cur = "?"
				e.p.org, e.p.orgSet = "", false
				s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
				s.trace(e.st.sql, an, "(its key's)")
				continue
			} else {
				org, incomplete, unknown, err = s.resolveOrg(an, e.p.values, idle && txNow != 'E')
			}
			if err != nil { // the statement must not run under a made-up organization
				inject[e.idx] = append(inject[e.idx], raise(raiseSQL("lokadb: "+err.Error()))...)
				refused = true
				continue
			}
			// Inside a pipeline without Sync (P3) the refs cannot be read: the statement would
			// run in every organization's database, which is the fanout policy's to allow.
			if incomplete && txNow != 'E' && s.fanout.decide(e.st.sql, an, true) == FanoutRefused {
				s.log.Warn("refused: its organization needs a lookup inside a pipeline", "table", fanoutTable(an), "sql", brief(e.st.sql))
				inject[e.idx] = append(inject[e.idx], raise(raiseSQL(lookupMsg))...)
				refused = true
				continue
			}
			if unknown && s.refuseNamed(e.st.sql, an, true, named) {
				inject[e.idx] = append(inject[e.idx], raise(s.fanout.raiseSQL(an))...)
				refused = true
				continue
			}
		}
		if an.Lokadb {
			if err := s.noteCursors(e.st.sql, org, named); err != nil {
				inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode("0A000", "lokadb: "+err.Error()))...)
				refused = true
				continue
			}
			if why := s.directOnlyRefusal(e.st.sql, an, org, named != "" && named != TenantAll); why != "" { // profile direct: no cloud way
				inject[e.idx] = append(inject[e.idx], raise(raiseSQLCode("0A000", "lokadb: "+why))...)
				refused = true
				continue
			}
			s.learnStmt(e.st.sql, org, named != "" && named != TenantAll) // a transaction learnt for the direct path (direct_tx.go)
		}
		if cur != org { // lokadb.org is already this one upstream: not set again (block 9, review PF3)
			inject[e.idx] = append(inject[e.idx], s.setOrg(org)...)
		}
		if key, ok := s.orgLock(an, org); ok {
			// up to Sync (or COMMIT) the batch is one transaction: the lock holds as long
			inject[e.idx] = append(inject[e.idx], s.lockOrg(key, s.meta.uuidOrgs())...)
		}
		if txNow != 'E' {
			inject[e.idx] = append(inject[e.idx], prelude(an.Prelude)...)
		}
		part := s.foreignPart(an, org)
		if key := upsertLock(an, org, part); key != "" && txNow != 'E' {
			// up to Sync (or COMMIT) the batch is one transaction, as for the pause lock
			inject[e.idx] = append(inject[e.idx], lockUpsert(key)...)
		}
		cur = org
		e.p.org, e.p.orgSet = org, true
		traced := an
		// UPDATE/DELETE of one organization in its own database: bind the client's
		// portal to the same statement aimed at the foreign partition (see Analysis.target).
		if part != "" {
			inject[e.idx] = append(inject[e.idx],
				outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_alt"}, mode: mDrop},
				outMsg{msg: &pgproto3.Parse{Name: "lokadb_alt", Query: an.targetSQL(part, org), ParameterOIDs: e.st.oids}, mode: mDrop})
			b.PreparedStatement = "lokadb_alt"
			traced = &Analysis{Lokadb: true, Rewritten: "partition", Target: part}
		} else if !an.Lokadb || txNow == 'E' || an.Rewritten != "" { // a statement the proxy rewrites: relinked as it is
			s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
		} else if keyed := s.keyedBind(e.st, e.st.sql, b, org); keyed != nil { // the partition key's condition (keycond.go, F7)
			inject[e.idx] = append(inject[e.idx], keyed...)
			e.p.up = b.PreparedStatement
		} else {
			s.relinkIf(txNow, inject, e.idx, b, e.st, an, relinked)
		}
		if an.Lokadb {
			s.trace(e.st.sql, traced, org)
		}
	}
	s.upOrg = cur
	if refused { // what follows the error is skipped: lokadb.org was not set by it
		s.upOrg = "?"
	}
	if isSync && s.tenantImpl { // the Sync ends the implicit transaction: its SET LOCAL ends
		if refused {
			s.tenant.rollback()
		} else {
			s.tenant.commit()
		}
		s.tenantImpl = false
	}

	// The commit point of DDL to replay (ddl.go): the COMMIT's Execute, or the Sync of an
	// implicit transaction (the batch started outside a transaction and opens none).
	events := map[int][]TxEvent{}
	commitAt, beginSeen, ddlBefore := -1, false, false
	tagAt := map[int]string{} // Executes whose command tag the client gets changed (indexTag)
	for _, e := range steps {
		if !e.bind && e.p.an != nil {
			if t := indexTag(e.p.an); t != "" {
				tagAt[e.idx] = t
			}
		}
	}
	ends := map[int]func(){} // learnEnd of a COMMIT's Execute
	for _, e := range steps {
		if e.bind || e.p.an == nil || len(e.p.an.Events) == 0 {
			continue
		}
		evs := e.p.an.Events
		events[e.idx] = evs
		if e.p.an.TxControl && e.p.st != nil { // the COMMIT of a watched transaction (direct_tx.go)
			ends[e.idx] = s.learnEnd(e.p.st.sql, e.p.an, s.txStatus())
		}
		beginSeen = beginSeen || hasEvent(evs, 'B')
		if commitAt < 0 && hasEvent(evs, 'D') {
			ddlBefore = true
		}
		if commitAt < 0 && hasEvent(evs, 'C') {
			commitAt = e.idx
		}
	}
	if ddlBefore {
		s.ddlOpen = true
	}
	cutAt, isExec := -1, false
	switch replay := ddlBefore || s.ddlOpen || s.hasPending(); {
	case !replay || tx == 'E':
	case commitAt >= 0:
		cutAt, isExec = commitAt, true
	case isSync && tx == 'I' && !beginSeen:
		cutAt = len(batch) - 1
	}

	out := make([]outMsg, 0, len(batch)+7*len(inject))
	cutOut, commitOut := -1, -1
	for i, m := range batch {
		if i == cutAt {
			cutOut = len(out)
		}
		out = append(out, inject[i]...)
		o := outMsg{msg: m, onOK: forgets[i], tagAs: tagAt[i]}
		if evs := events[i]; evs != nil {
			f := forgets[i]
			o.onOK = func() {
				s.applyEvents(evs)
				if f != nil {
					f()
				}
			}
		}
		o.onOK = chainOK(o.onOK, ends[i])
		if i == cutAt {
			commitOut = len(out)
		}
		out = append(out, o)
		if listenAt[i] {
			out = append(out, listenMsgs()...)
		}
	}
	if cutAt >= 0 {
		s.commitInBatch(out, cutOut, commitOut, isExec)
		return
	}
	_ = s.send(out)
}

// relisten sends LISTEN lokadb_meta again after the client's UNLISTEN * or DISCARD ALL.
func (s *session) relisten() {
	_ = s.send([]outMsg{{msg: &pgproto3.Query{String: "LISTEN " + metaChannel}, mode: mDrop}})
}

// listenMsgs: LISTEN lokadb_meta inside a batch, through a statement and portal of the proxy's own.
func listenMsgs() []outMsg {
	return []outMsg{
		{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_n"}, mode: mDrop},
		{msg: &pgproto3.Parse{Name: "lokadb_n", Query: "LISTEN " + metaChannel}, mode: mDrop},
		{msg: &pgproto3.Bind{DestinationPortal: "lokadb_np", PreparedStatement: "lokadb_n"}, mode: mDrop},
		{msg: &pgproto3.Execute{Portal: "lokadb_np"}, mode: mDrop},
	}
}

// indexTag: CREATE UNIQUE INDEX on a LokaDB table runs as SELECT lokadb.create_index(…)
// (analyze.go): the client gets the tag of what it sent, not SELECT 1. Only for a statement
// of its own (in a longer string the tag would be the client's own SELECT's as well).
func indexTag(an *Analysis) string {
	if len(an.Stmts) == 1 && strings.HasPrefix(an.Rewritten, "SELECT lokadb.create_index(") {
		return "CREATE INDEX"
	}
	if len(an.Stmts) == 1 && an.Tag != "" {
		return an.Tag
	}
	return ""
}

// park resets the session's connection for the next client of its key (pool.go) when it is
// clean: the cloud outside a transaction, nothing pending or held, no COPY, no transaction of
// the proxy's own. true: the reset ran and the relay goroutine has stopped; false: the
// connection is to be closed.
func (s *session) park() bool {
	if s.pc == nil || s.evict.Load() || time.Since(s.pc.born) > s.srv.pool.maxAge {
		return false
	}
	// what is still on the way is the proxy's own as a rule (the direct path's ungate is sent
	// without waiting): its answers are waited for a little; a statement of the client's still
	// running is not, the connection is closed
	if !s.q.empty() {
		done := make(chan bool, 1)
		go func() { ok, dead := s.q.waitEmpty(); done <- ok && !dead }()
		select {
		case ok := <-done:
			if !ok {
				return false
			}
		case <-time.After(2 * time.Second):
			return false
		}
	}
	if !s.q.empty() || s.q.isDead() || len(s.batch) > 0 || s.unsynced || s.copyExt.Load() || s.ddlOpen || s.direct.tx != nil || s.txStatus() != 'I' {
		return false
	}
	if !s.releasing() {
		return false
	}
	s.parking.Store(true)
	if err := s.send([]outMsg{{msg: &pgproto3.Query{String: "DISCARD ALL"}, mode: mDrop, onOK: func() { s.discardOK.Store(true) }}}); err != nil {
		return false
	}
	select {
	case <-s.relayDone:
	case <-time.After(10 * time.Second):
		return false
	}
	if s.upRaw != nil && s.upRaw.buffered() > 0 { // read past the reset's answer: the next session would not have it
		return false
	}
	return s.discardOK.Load() && s.txStatus() == 'I' && !s.evict.Load()
}

// releaseCaches: the session's caches no longer count in the process's bound (byteCache.total).
func (s *session) releaseCaches() {
	s.anCache.reset()
	s.direct.kinds.reset()
	s.direct.info.reset()
	s.direct.infoNamed.reset()
}

// releasing: the session's upstream connections may go back to the pools for other clients: no
// cancel of its is on the way (it would reach their statements), and none starts after this.
// false: one is: the connections are closed instead.
func (s *session) releasing() bool {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.cancels > 0 {
		return false
	}
	s.released = true
	return true
}

// cancelBegin: a cancel of the session may go upstream (cancelEnd when it is sent); false: its
// connections are back in the pools, the cancel goes nowhere.
func (s *session) cancelBegin() bool {
	s.cancelMu.Lock()
	defer s.cancelMu.Unlock()
	if s.released {
		return false
	}
	s.cancels++
	return true
}

func (s *session) cancelEnd() {
	s.cancelMu.Lock()
	s.cancels--
	s.cancelMu.Unlock()
}

// upstreamError: an error of a query of the proxy's own, with its SQLSTATE (meta.go: denied).
type upstreamError struct{ code, msg string }

func (e *upstreamError) Error() string { return e.msg }

// orgSetMsg refuses a query string or statement that sets lokadb.org itself and works on LokaDB
// tables: the proxy's own value, set right before, would not hold for those statements.
const orgSetMsg = "lokadb: lokadb.org is set by the proxy: a query string or statement that sets it (SET, RESET, set_config, DISCARD ALL) cannot also work on LokaDB tables; send it as a query of its own"

// foreignPart is the partition a statement can address directly: an UPDATE/DELETE/SELECT
// (Analysis.target) confined to one organization that lives in its own database, its
// foreign partition; an upsert (TargetLocal) of a cloud organization, its local partition
// or DEFAULT, when the session's role may write it.
func (s *session) foreignPart(an *Analysis, org string) string {
	if an.Target == "" || org == "" || org == "-1" || org == noOrg {
		return ""
	}
	part := s.partitionOf(an, org)
	if part == "" || an.targetText() == "" { // the statement is built only for an organization that has a partition (PF4)
		return ""
	}
	return part
}

// partitionOf: the partition foreignPart aims an analysed statement of org at, "" for none.
func (s *session) partitionOf(an *Analysis, org string) string {
	if an.Write && s.meta.StmtTriggers[an.Target] { // fired for the parent only
		return ""
	}
	if an.TargetLocal {
		// a cloud organization's upsert, natively in its own partition or DEFAULT; one in
		// its own database keeps the rewritten upsert (ON CONFLICT DO UPDATE is not there
		// for a foreign table)
		if s.meta.Foreign[an.Target][org] != "" {
			return ""
		}
		part := s.meta.Local[an.Target][org]
		if part == "" && s.meta.Tables[an.Target] != nil {
			part = s.meta.Tables[an.Target].Cloud
		}
		if !s.meta.Writable[part] {
			return ""
		}
		return part
	}
	part := s.meta.Foreign[an.Target][org]
	if !s.meta.Direct[part] { // the role may not do there what it may on the parent
		return ""
	}
	return part
}

// raiseSQL is a statement that fails with msg.
func raiseSQL(msg string) string { return raiseSQLCode("0A000", msg) }

// raiseSQLCode is a statement that fails with msg and SQLSTATE code.
func raiseSQLCode(code, msg string) string {
	return fmt.Sprintf("DO $lokadb$ BEGIN RAISE EXCEPTION USING ERRCODE = '%s', MESSAGE = %s; END $lokadb$", code, sqlString(msg))
}

// orgLock is the pause lock a statement takes (lokadb.org_lock): move-org holds a lock
// exclusively while it splits an organization out of DEFAULT, so the organization's
// statements wait and every other organization's run. key: the organization, or "NULL" for
// the lock of every organization (several, rows of none, ids not found, an UPDATE moving rows
// between organizations), which the move holds too. ok false: none (no LokaDB table; DDL,
// which its own table locks order against a move; a database without lokadb.org_lock).
func (s *session) orgLock(an *Analysis, org string) (key string, ok bool) {
	if s.meta == nil || !s.meta.OrgLock || !an.Lokadb || an.Utility || len(an.DDLTables) > 0 || an.OwnLock {
		return "", false
	}
	if an.MovesOrg || !realOrg(org) {
		return "NULL", true
	}
	return org, true
}

// lockCall: the lock key of orgLock's key in SQL text: lokadb.org_lock(NULL), of an integer
// organization lokadb.org_lock(N), of one numbered by uuid lokadb.org_lock_v('…') (block 18).
func (s *session) lockCall(key string) string {
	if key == "NULL" || !s.meta.uuidOrgs() {
		return "lokadb.org_lock(" + key + ")"
	}
	return "lokadb.org_lock_v(" + sqlString(key) + ")"
}

// lockOrg takes the pause lock key in the current transaction (see orgLock); asText: the
// organizations are numbered by uuid (lokadb.org_lock_v, block 18).
func (s *session) lockOrg(key string, asText bool) []outMsg {
	var p []byte
	if key != "NULL" {
		p = []byte(key)
	}
	if asText && key != "NULL" {
		return s.svc(svcLockTextStmt, "lokadb_lp", p)
	}
	return s.svc(svcLockBigStmt, "lokadb_lp", p)
}

// upsertLock is the key of the exclusive lock a rewritten upsert runs under (block 2, item 9
// (b)): its NOT EXISTS does not see another transaction's uncommitted insert of the same key,
// and its INSERT then fails with a unique violation instead of updating. The lock is per
// table and organization (the key values of an INSERT … SELECT or of a bound statement are
// not known before it runs): upserts of one table and organization run one at a time, the
// rest of its statements do not wait. "": none (the native upsert in a local partition, part
// set by foreignPart, is atomic by itself).
func upsertLock(an *Analysis, org, part string) string {
	if an.UpsertLock == "" || part != "" {
		return ""
	}
	return an.UpsertLock + "/" + org
}

// upsertLockArgs: the two int4 keys of upsertLock, a space of their own (lokadb.org_lock and
// move-org use the one bigint key). key is an SQL expression of text.
func upsertLockArgs(key string) string {
	return "hashtext('lokadb.upsert'), hashtext(" + key + ")"
}

// lockUpsert takes the upsertLock key in the current transaction.
func lockUpsert(key string) []outMsg {
	return []outMsg{
		{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_us"}, mode: mDrop},
		{msg: &pgproto3.Close{ObjectType: 'P', Name: "lokadb_up"}, mode: mDrop},
		{msg: &pgproto3.Parse{Name: "lokadb_us", Query: "SELECT pg_advisory_xact_lock(" + upsertLockArgs("$1::text") + ")"}, mode: mDrop},
		{msg: &pgproto3.Bind{DestinationPortal: "lokadb_up", PreparedStatement: "lokadb_us", Parameters: [][]byte{[]byte(key)}}, mode: mDrop},
		{msg: &pgproto3.Execute{Portal: "lokadb_up"}, mode: mDrop},
	}
}

// setOrg sets lokadb.org through a self-contained named statement/portal so the
// client's unnamed statement and portal are never clobbered.
// prelude runs Analysis.Prelude inside a batch: a statement and a portal of its own for
// each (a Query there would commit the client's implicit transaction and drop its unnamed
// statement and portal).
func prelude(stmts []string) []outMsg {
	var out []outMsg
	for _, q := range stmts {
		out = append(out,
			outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: "lokadb_pre"}, mode: mDrop},
			outMsg{msg: &pgproto3.Close{ObjectType: 'P', Name: "lokadb_prep"}, mode: mDrop},
			outMsg{msg: &pgproto3.Parse{Name: "lokadb_pre", Query: q}, mode: mDrop},
			outMsg{msg: &pgproto3.Bind{DestinationPortal: "lokadb_prep", PreparedStatement: "lokadb_pre"}, mode: mDrop},
			outMsg{msg: &pgproto3.Execute{Portal: "lokadb_prep"}, mode: mDrop})
	}
	return out
}

func (s *session) setOrg(org string) []outMsg {
	return s.svc(svcSetOrgStmt, "lokadb_p", []byte(org))
}

// The proxy's own statements of every statement it routes (block 9, review PF3): they were
// Closed, Parsed, Bound and Executed each time, so the cloud parsed and planned them for every
// statement of the client. Prepared once on the connection (the Parse goes until the server has
// answered one, svcPrep), then only the portal's Close, Bind and Execute.
type svcStmt struct {
	bit         uint32
	name, query string
}

const (
	svcSetOrg uint32 = 1 << iota
	svcLockBig
	svcLockText
)

var (
	svcSetOrgStmt   = svcStmt{svcSetOrg, "lokadb_s", "SELECT set_config('lokadb.org', $1, false)"}
	svcLockBigStmt  = svcStmt{svcLockBig, "lokadb_ls", "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock($1::bigint))"}
	svcLockTextStmt = svcStmt{svcLockText, "lokadb_lsv", "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock_v($1::text))"}
)

// svc: the messages that run st with the parameter p (nil: NULL) through the named portal. Not
// known to be prepared on the connection (and not already Parsed by this batch): Closed and
// Parsed first, as before; the bit is set when the server's ParseComplete comes (a Parse that an
// error skipped, or that came before a DISCARD ALL, confirms nothing).
func (s *session) svc(st svcStmt, portal string, p []byte) []outMsg {
	var out []outMsg
	if s.svcPrep.Load()&st.bit == 0 && s.svcSent&st.bit == 0 {
		s.svcSent |= st.bit
		e0, bit := s.svcEpoch.Load(), st.bit
		out = append(out,
			outMsg{msg: &pgproto3.Close{ObjectType: 'S', Name: st.name}, mode: mDrop},
			outMsg{msg: &pgproto3.Parse{Name: st.name, Query: st.query}, mode: mDrop, onOK: func() {
				if s.svcEpoch.Load() == e0 {
					s.svcPrep.Or(bit)
				}
			}})
	}
	return append(out,
		outMsg{msg: &pgproto3.Close{ObjectType: 'P', Name: portal}, mode: mDrop},
		outMsg{msg: &pgproto3.Bind{DestinationPortal: portal, PreparedStatement: st.name, Parameters: [][]byte{p}}, mode: mDrop},
		outMsg{msg: &pgproto3.Execute{Portal: portal}, mode: mDrop})
}

var intRe = regexp.MustCompile(`^-?[0-9]{1,19}$`)

// valuesFunc returns the texts a ValueRef stands for, read as values of its kind (v.Kind,
// keys.go) in their canonical form: one for a literal or a scalar parameter, every element
// for an array parameter; false if they cannot be read (or are not values of that kind).
type valuesFunc func(ValueRef) ([]string, bool)

func constOnly(v ValueRef) ([]string, bool) {
	switch {
	case v.Null:
		return []string{nullValue}, true
	case v.Param != 0:
		return nil, false
	}
	x, ok := canonicalKey(v.Kind, v.Const)
	if !ok {
		return nil, false
	}
	return []string{x}, true
}

// noOrg is the organization of rows that have none (NULL in the organization column, a
// child row without a parent or under a parent that has none): they stay in the cloud, in
// <table>__none, and lokadb.org = -2 makes them the statement's rows. -1 is a different
// thing: ids that do not exist.
const noOrg = "-2"

// nullValue is what a valuesFunc returns for NULL.
const nullValue = "<NULL>"

// values reads literals and bound parameters, text or binary, as values of v.Kind: a binary
// parameter by the key's type (a uuid of 16 bytes, text as its bytes), not by its length alone.
func (p *portalInfo) values(v ValueRef) ([]string, bool) {
	xs, ok := p.raw(v)
	if !ok {
		return nil, false
	}
	for i, x := range xs {
		if x == nullValue {
			continue
		}
		if xs[i], ok = canonicalKey(v.Kind, x); !ok {
			return nil, false
		}
	}
	return xs, true
}

// raw: the texts of values (above) before they are made canonical.
func (p *portalInfo) raw(v ValueRef) ([]string, bool) {
	if v.Null {
		return []string{nullValue}, true
	}
	if v.Param == 0 {
		return []string{v.Const}, true
	}
	i := v.Param - 1
	if i >= len(p.params) {
		return nil, false
	}
	if p.params[i] == nil {
		if v.Array {
			return nil, false
		}
		return []string{nullValue}, true
	}
	b := p.params[i]
	var format int16
	switch {
	case len(p.formats) == 1:
		format = p.formats[0]
	case i < len(p.formats):
		format = p.formats[i]
	}
	switch {
	case v.Array && format == 0:
		return parseArrayText(string(b))
	case v.Array:
		return binaryArray(b)
	case format == 0:
		return []string{string(b)}, true
	}
	if x, ok := binaryKey(v.Kind, b); ok {
		return []string{x}, true
	}
	return nil, false
}

func binaryInt(b []byte) (string, bool) {
	switch len(b) {
	case 2:
		return strconv.Itoa(int(int16(binary.BigEndian.Uint16(b)))), true
	case 4:
		return strconv.Itoa(int(int32(binary.BigEndian.Uint32(b)))), true
	case 8:
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(b)), 10), true
	}
	return "", false
}

// binaryArray reads a one-dimensional array in Postgres binary form (array_send): ndim,
// has-null flag, element type, then size and lower bound per dimension, then each element
// as length + bytes. Integer and uuid elements are decoded; any other type is taken as text.
// A NULL element or several dimensions: not ok.
func binaryArray(b []byte) ([]string, bool) {
	if len(b) < 12 {
		return nil, false
	}
	ndim, elem := int32(binary.BigEndian.Uint32(b)), binary.BigEndian.Uint32(b[8:])
	b = b[12:]
	if ndim == 0 {
		return nil, true
	}
	if ndim != 1 || len(b) < 8 {
		return nil, false
	}
	n := int(int32(binary.BigEndian.Uint32(b)))
	b = b[8:]
	if n < 0 || n > len(b)/4 {
		return nil, false
	}
	out := make([]string, 0, n)
	for k := 0; k < n; k++ {
		if len(b) < 4 {
			return nil, false
		}
		l := int(int32(binary.BigEndian.Uint32(b)))
		b = b[4:]
		if l < 0 || l > len(b) {
			return nil, false
		}
		e := b[:l]
		b = b[l:]
		switch elem {
		case 20, 21, 23: // int8, int2, int4
			x, ok := binaryInt(e)
			if !ok {
				return nil, false
			}
			out = append(out, x)
		case 2950: // uuid
			x, ok := binaryKey(kindUUID, e)
			if !ok {
				return nil, false
			}
			out = append(out, x)
		default:
			out = append(out, string(e))
		}
	}
	return out, true
}

// resolveOrg returns the single tenant a statement is confined to, "" when it spans
// tenants (no restriction: all partitions are visible), or "-1" when nothing can match
// anywhere: every referenced id is absent from the refs tables, or the arrays are empty.
// err: the refs lookup failed, so it is not known whether the ids exist.
// incomplete reports that refs lookups were needed but not allowed; unknown, that a value
// could not be read as a value of its key's kind (the statement names no organization after
// all).
func (s *session) resolveOrg(an *Analysis, vals valuesFunc, lookup bool) (org string, incomplete, unknown bool, err error) {
	s.freshOrgs(an)
	if an.Unhinted || an.ParseError {
		return "", false, false, nil
	}
	if org, ok := s.tenantNamed(an, vals); ok {
		return org, false, false, nil
	}
	for pass := 0; pass < 2; pass++ {
		orgs := map[string]bool{}
		missing := map[string][]string{}
		seen, hinted := 0, false
		for _, h := range an.Hints {
			refs, ok := s.meta.Lookups[h.Table][h.Column]
			if !ok {
				continue
			}
			hinted = true
			// canonical: '007' and 7 are the same id, so are 'A0EEBC99…' and '{a0eebc99…}'
			kind := s.meta.keyKind(refs)
			for _, v := range h.Values {
				v.Kind = kind
				xs, ok := vals(v)
				if !ok {
					return "", false, true, nil
				}
				for _, x := range xs {
					if x == nullValue { // no organization: the row has none, or nothing matches
						seen++
						orgs[noOrg] = true
						continue
					}
					seen++
					if refs == "" {
						orgs[x] = true
					} else if o, ok := s.orgOf(refs, x); ok {
						orgs[o] = true
					} else {
						missing[refs] = append(missing[refs], x)
					}
				}
			}
		}
		// lookups of several columns (a composite key, tuples.go): each row's values, a product
		// of them where a parameter is an array
		for _, h := range an.Tuples {
			refs, ok := s.meta.Lookups[h.Table][tupleName(h.Columns)]
			kinds := s.meta.tupleKinds(refs)
			if !ok || len(kinds) != len(h.Columns) {
				continue
			}
			hinted = true
			for _, row := range h.Rows {
				keys := [][]string{nil}
				for i, v := range row {
					v.Kind = kinds[i]
					xs, ok := vals(v)
					if !ok {
						return "", false, true, nil
					}
					var next [][]string
					for _, k := range keys {
						for _, x := range xs {
							next = append(next, append(append([]string{}, k...), x))
						}
					}
					if len(next) > maxTuples {
						return "", false, true, nil
					}
					keys = next
				}
				for _, k := range keys {
					seen++
					if slices.Contains(k, nullValue) { // part of the key NULL: no row
						orgs[noOrg] = true
						continue
					}
					x := tupleKey(k)
					if o, ok := s.orgOf(refs, x); ok {
						orgs[o] = true
					} else {
						missing[refs] = append(missing[refs], x)
					}
				}
			}
		}
		if len(missing) > 0 && pass == 0 {
			if !lookup {
				return "", true, false, nil
			}
			if err := s.lookup(missing); err != nil {
				return "", false, false, err
			}
			continue
		}
		switch {
		case len(orgs) == 1:
			for o := range orgs {
				return o, false, false, nil
			}
		case len(orgs) == 0 && (len(missing) > 0 || hinted && seen == 0):
			return "-1", false, false, nil
		case len(orgs) > 1: // values of several organizations: a fanout, the policy's (review of routing R5)
			return "", false, true, nil
		}
		return "", false, false, nil
	}
	return "", false, false, nil
}

// tenantNamed: the one organization the statement's organization columns name
// (organization_id = 1, IN of one value): its keys are not looked up in __refs (review of
// performance PF2: an exchange with the cloud per new key). Under lokadb.org of that
// organization every LokaDB table of the statement shows that organization's rows alone (the
// policy lokadb_tenant), so a key of another organization finds nothing; a write of one is
// refused by __refs (its primary key), the parent's trigger lokadb_parent or the RLS check. false:
// no organization column named, values of several organizations or of none, or unreadable.
func (s *session) tenantNamed(an *Analysis, vals valuesFunc) (string, bool) {
	org := ""
	for _, h := range an.Hints {
		if refs, ok := s.meta.Lookups[h.Table][h.Column]; !ok || refs != "" {
			continue
		}
		for _, v := range h.Values {
			v.Kind = s.meta.keyKind("")
			xs, ok := vals(v)
			if !ok {
				return "", false
			}
			for _, x := range xs {
				if x == nullValue || org != "" && x != org {
					return "", false
				}
				org = x
			}
		}
	}
	return org, org != ""
}

// readsRows: a statement of an's string may read or write rows (not a transaction's
// statement, not a utility such as SET, SHOW, LISTEN).
func (s *session) readsRows(an *Analysis) bool {
	for _, st := range an.Stmts {
		if sa := s.analysis(st); !sa.TxControl && !sa.Utility && !sa.ResetsOrg {
			return true
		}
	}
	return false
}

// readsAfter: a statement that may read rows comes after one that mark matches in an's string.
func (s *session) readsAfter(an *Analysis, mark func(*Analysis) bool) bool {
	seen := false
	for _, st := range an.Stmts {
		sa := s.analysis(st)
		switch {
		case mark(sa):
			seen = true
		case seen && !sa.TxControl && !sa.Utility && !sa.ResetsOrg:
			return true
		}
	}
	return false
}

// orgVersions[db]: counts the statements through this proxy that may change what lookups
// answer (Analysis.ChangesLookups). orgCacheTTL bounds what other proxies and direct writes
// can leave stale.
var orgVersions sync.Map

const orgCacheTTL = time.Minute

func orgVersion(db string) *atomic.Int64 {
	if v, ok := orgVersions.Load(db); ok { // see schemaVersion
		return v.(*atomic.Int64)
	}
	v, _ := orgVersions.LoadOrStore(db, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// freshOrgs empties the organization cache when a statement here or in another session of
// the database may have changed a row's organization or a key, and every orgCacheTTL (P7).
func (s *session) freshOrgs(an *Analysis) {
	v := orgVersion(s.db)
	if an.ChangesLookups {
		v.Add(1)
	}
	switch inTx := s.txStatus() != 'I' || s.direct.tx != nil || s.direct.deferred.Load(); { // a deferred BEGIN is the client's transaction too
	case !inTx:
		s.ownOrgs = false // a transaction of its own: what it reads is committed
	case an.ChangesLookups && !s.ownOrgs: // its changes are its own until the commit (orgcache.go)
		s.ownOrgs = true
		s.orgCache, s.orgVer, s.orgAt = map[string]map[string]string{}, v.Load(), time.Now()
	}
	if s.orgAt.IsZero() { // the session's first statement: its cache is new
		s.orgVer, s.orgAt = v.Load(), time.Now()
	}
	if now := v.Load(); now != s.orgVer || time.Since(s.orgAt) > orgCacheTTL {
		s.orgCache, s.orgVer, s.orgAt = map[string]map[string]string{}, now, time.Now()
	}
}

// lookup reads the tenants of ids from the refs tables into orgCache. In a failed
// transaction nothing can be read; the statement fails there anyway.
func (s *session) lookup(missing map[string][]string) error {
	tx := s.txStatus()
	if tx == 'E' {
		return nil
	}
	n := 0
	for _, c := range s.orgCache {
		n += len(c)
	}
	if n > 100000 { // bounded: ids are looked up again when needed
		s.orgCache = map[string]map[string]string{}
	}
	ver := orgVersion(s.db).Load() // what the lookups read is kept only if nothing changed meanwhile
	for refs, ids := range missing {
		if kinds := s.meta.tupleKinds(refs); kinds != nil { // a composite key (tuples.go)
			if err := s.lookupTuples(refs, kinds, ids, tx, ver); err != nil {
				return err
			}
			continue
		}
		// An integer key's values go as numbers (canonicalInt: digits alone): one past int8 is
		// then found nowhere, as a quoted one would fail the lookup (22003). A uuid or text key
		// and a key's map (its id is text) take literals, read as the type of id (block 17).
		_, keyMap := keyRefs(refs)
		var quoted []string
		for _, id := range ids {
			if s.meta.keyKind(refs) == kindInt && !keyMap {
				quoted = append(quoted, id)
			} else {
				quoted = append(quoted, sqlString(id))
			}
		}
		tenant, from, list := "tenant", qrel(refs), strings.Join(quoted, ",")
		if tc := s.meta.cloud(refs); tc != "" { // a cloud table that knows the organization
			tenant = quoteIdent(tc)
		}
		if k, ok := keyRefs(refs); ok { // a key's map (routing.keys): text values
			from = "lokadb." + quoteIdent(k)
		}
		q := fmt.Sprintf("SELECT id::text, coalesce(%s::text, %s) FROM %s WHERE id IN (%s)", tenant, sqlString(noOrg), from, list)
		rows, err := s.lookupQuery(q, tx == 'T')
		if err != nil {
			s.log.Warn("refs lookup failed", "refs", refs, "err", err)
			return fmt.Errorf("could not find the organization of the referenced rows: %w", err)
		}
		got := make(map[string]string, len(rows))
		for _, r := range rows {
			got[r[0]] = r[1]
		}
		s.keepOrgs(refs, ver, got)
	}
	return nil
}

// lookupTuples reads the organizations of composite keys (tupleKey) from refs table refs,
// whose columns id_1 … id_n hold them.
func (s *session) lookupTuples(refs string, kinds []string, keys []string, tx byte, ver int64) error {
	var rows, cols, sel []string
	for i := range kinds {
		cols = append(cols, fmt.Sprintf("id_%d", i+1))
		sel = append(sel, fmt.Sprintf("id_%d::text", i+1))
	}
	for _, key := range keys {
		vals, ok := tupleVals(key)
		if !ok || len(vals) != len(kinds) {
			continue
		}
		var lits []string
		for i, v := range vals {
			if kinds[i] == kindInt {
				lits = append(lits, v) // canonical digits: one past int8 is found nowhere
			} else {
				lits = append(lits, sqlString(v))
			}
		}
		rows = append(rows, "("+strings.Join(lits, ", ")+")")
	}
	if len(rows) == 0 {
		return nil
	}
	q := fmt.Sprintf("SELECT %s, coalesce(tenant::text, %s) FROM %s WHERE (%s) IN (%s)",
		strings.Join(sel, ", "), sqlString(noOrg), qrel(refs), strings.Join(cols, ", "), strings.Join(rows, ", "))
	got, err := s.lookupQuery(q, tx == 'T')
	if err != nil {
		s.log.Warn("refs lookup failed", "refs", refs, "err", err)
		return fmt.Errorf("could not find the organization of the referenced rows: %w", err)
	}
	found := make(map[string]string, len(got))
	for _, r := range got {
		found[tupleKey(r[:len(kinds)])] = r[len(kinds)]
	}
	s.keepOrgs(refs, ver, found)
	return nil
}

// lookupQuery runs a refs lookup; inside the client's transaction under a savepoint, so
// that its failure (a lock timeout, say) does not abort the client's transaction.
//
// Inside a pipeline there is no such protection: after an error the server skips everything
// up to the client's Sync, ROLLBACK TO included. The failure then ends the client's pipeline
// with that error, as an error of its own statement would (route passes it on).
func (s *session) lookupQuery(q string, inTx bool) ([][]string, error) {
	if !inTx || s.inPipeline {
		return s.internal(q)
	}
	// one exchange (review of performance PF14): the savepoint, the lookup and its release up
	// to one Sync; a failed lookup skips the release, rolled back to the savepoint after it
	c := s.cloudRun("SAVEPOINT lokadb_lookup", q, "RELEASE SAVEPOINT lokadb_lookup")
	if c.err != nil {
		s.cloudRun("ROLLBACK TO SAVEPOINT lokadb_lookup", "RELEASE SAVEPOINT lokadb_lookup")
		return nil, c.err
	}
	return c.rows, nil
}

func canonicalInt(x string) string {
	if v, err := strconv.ParseInt(x, 10, 64); err == nil {
		return strconv.FormatInt(v, 10)
	}
	return x
}

// internal runs a proxy-internal query (captureExt): with a Sync between pipelines, with a
// Flush inside one.
func (s *session) internal(sql string) ([][]string, error) {
	return s.captureExt(sql, s.inPipeline)
}

// internalFlush runs a proxy-internal query with a Flush, never a Sync: for a point where the
// client's messages before it are already sent without their Sync (an implicit transaction
// may be open, a portal of the client's may wait for its Execute), such as the DDL replay
// between a batch's prefix and its COMMIT.
func (s *session) internalFlush(sql string) ([][]string, error) {
	return s.captureExt(sql, true)
}

// captureExt runs a proxy-internal query with Parse, Bind and Execute through a statement and
// portal of the proxy's own, then a Sync, or a Flush inside the client's pipeline (where a
// Sync would commit its implicit transaction). A simple query would also drop the client's
// unnamed statement, which it may Bind in its next batch (pgx: Parse, Describe, Sync, then
// Bind, Execute, Sync). Call only when the queue is empty and not dead.
func (s *session) captureExt(sql string, flush bool) ([][]string, error) {
	c := &capture{done: make(chan struct{}), untilSync: !flush}
	if err := s.send(s.extMsgs(c,
		&pgproto3.Close{ObjectType: 'S', Name: "lokadb_q"},
		&pgproto3.Close{ObjectType: 'P', Name: "lokadb_qp"},
		&pgproto3.Parse{Name: "lokadb_q", Query: sql},
		&pgproto3.Bind{DestinationPortal: "lokadb_qp", PreparedStatement: "lokadb_q"},
		&pgproto3.Execute{Portal: "lokadb_qp"})); err != nil {
		return nil, err
	}
	<-c.done
	return c.rows, c.err
}

// extMsgs: ms captured by c, then a Sync (c.untilSync) or a Flush.
func (s *session) extMsgs(c *capture, ms ...pgproto3.FrontendMessage) []outMsg {
	out := make([]outMsg, 0, len(ms)+1)
	for _, m := range ms {
		out = append(out, outMsg{msg: m, mode: mCapture, cap: c})
	}
	if c.untilSync {
		return append(out, outMsg{msg: &pgproto3.Sync{}, mode: mCapture, cap: c})
	}
	return append(out, outMsg{msg: &pgproto3.Flush{}})
}

// describeTypes: the parameter types of the client's unnamed statement (Describe), nil if it
// cannot tell.
func (s *session) describeTypes() []uint32 {
	c := &capture{done: make(chan struct{}), untilSync: !s.inPipeline}
	if err := s.send(s.extMsgs(c, &pgproto3.Describe{ObjectType: 'S', Name: ""})); err != nil {
		return nil
	}
	<-c.done
	if c.err != nil || c.oids == nil {
		return nil
	}
	return c.oids
}

// resultTypes: the column types of the client's prepared statement name as the cloud
// describes it (empty: it returns no rows), nil if it cannot tell. Idle only.
func (s *session) resultTypes(name string) []uint32 {
	c := &capture{done: make(chan struct{}), untilSync: !s.inPipeline}
	if err := s.send(s.extMsgs(c, &pgproto3.Describe{ObjectType: 'S', Name: name})); err != nil {
		return nil
	}
	<-c.done
	if c.err != nil {
		return nil
	}
	return c.cols
}

// capture runs a proxy-internal query on the client's upstream connection (inside its
// transaction, so it also sees the client's uncommitted rows). Call only when idle.
func (s *session) capture(sql string) ([][]string, error) {
	c := &capture{done: make(chan struct{})}
	if err := s.send([]outMsg{{msg: &pgproto3.Query{String: sql}, mode: mCapture, cap: c}}); err != nil {
		return nil, err
	}
	<-c.done
	return c.rows, c.err
}

// refuse applies the fanout policy: true if the statement must not run. unknown: the
// statement has an organization restriction, but its values are not organization numbers.
func (s *session) refuse(sql string, an *Analysis, unknown bool) bool {
	v := s.fanout.Decide(sql, an)
	if unknown {
		v = s.fanout.decide(sql, an, true)
	}
	switch v {
	case FanoutRefused:
		s.log.Warn("refused: names no organization", "table", fanoutTable(an), "policy", s.fanout.Mode, "gap", an.GapText(), "sql", brief(sql))
		return true
	case FanoutRun:
		if s.fanout.Mode == FanoutLog {
			s.log.Warn("runs in every organization's database", "table", fanoutTable(an), "sql", brief(sql))
		}
	}
	return false
}

// brief is sql fit for the log: literals replaced by $n (they are the organizations' data),
// whitespace collapsed, cut at 160 bytes.
func brief(sql string) string {
	sql = redact(sql)
	if len(sql) > 160 {
		sql = sql[:160] + "..."
	}
	return strings.Join(strings.Fields(sql), " ")
}

// redact replaces every literal of sql by a $n placeholder (pg_query normalization). SQL that
// does not parse is not logged at all.
func redact(sql string) string {
	n, err := pg.Normalize(sql)
	if err != nil {
		return fmt.Sprintf("<%d bytes that do not parse>", len(sql))
	}
	return n
}

func (s *session) trace(sql string, an *Analysis, org string) {
	s.countStatement(org, false)
	level, ok := s.srv.routeLevel()
	if !ok || !s.log.Enabled(context.Background(), level) {
		return
	}
	route := "tenant " + org
	switch org {
	case "":
		route = "all tenants"
	case "-1":
		route = "no tenant (ids not found)"
	case noOrg:
		route = "rows of no organization"
	}
	if an.logged == "" { // normalizing parses the SQL again: once per cached analysis
		an.logged = brief(sql)
	}
	s.log.Log(context.Background(), level, "route", "to", route, "rewritten", an.Rewritten != "", "sql", an.logged)
}

func (s *session) respondError(msg string, tx byte) { s.respondErrorCode("0A000", msg, tx) }

func (s *session) respondErrorCode(code, msg string, tx byte) {
	s.log.Warn("rejected", "err", msg)
	s.cliMu.Lock()
	defer s.cliMu.Unlock()
	s.mErr.Store(true)
	s.cliOut.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: msg})
	if d := s.direct.tx; d != nil {
		d.aborted = true // an error ends a deferred or direct transaction, as the server's would
	}
	s.endRound()
	s.cliOut.Send(s.clientRFQ(tx))
	_ = s.cliOut.Flush()
}

func (s *session) send(out []outMsg) error {
	var ex []*expect
	for _, o := range out {
		if k := kindOf(o.msg); k != 0 {
			ex = append(ex, &expect{kind: k, mode: o.mode, cap: o.cap, onOK: o.onOK, notify: o.notify, wantTag: o.wantTag, tagAs: o.tagAs})
		}
	}
	s.q.push(ex)
	for _, o := range out {
		s.upOut.Send(o.msg)
	}
	return s.upOut.Flush()
}

// toClient buffers a message for the client; flushClient sends what is buffered.
func (s *session) toClient(m pgproto3.BackendMessage) {
	n := 32
	switch m := m.(type) {
	case *pgproto3.DataRow:
		for _, v := range m.Values {
			n += len(v) + 4
		}
	case *pgproto3.CopyData:
		n += len(m.Data)
	}
	switch m.(type) {
	case *pgproto3.ErrorResponse:
		s.mErr.Store(true)
	case *pgproto3.ReadyForQuery:
		s.endRound()
	}
	s.cliMu.Lock()
	if s.direct.owedParse.CompareAndSwap(true, false) {
		s.cliOut.Send(&pgproto3.ParseComplete{})
	}
	s.cliOut.Send(m)
	s.pending += n
	s.cliMu.Unlock()
}

func (s *session) flushClient() {
	s.cliMu.Lock()
	if s.pending > 0 {
		_ = s.cliOut.Flush()
		s.pending = 0
	}
	s.cliMu.Unlock()
}

const flushBytes = 64 << 10

// relay reads upstream responses, matches them to pending messages and routes them.
// Writes to the client are batched: one flush per response (ReadyForQuery) instead of one
// per row, except where the client may be waiting without a ReadyForQuery coming.
func (s *session) relay() {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("session aborted by a proxy bug", "panic", r, "stack", string(debug.Stack()))
		}
		s.flushClient()
		s.q.close()
		s.cliConn.Close()
		close(s.relayDone)
	}()
	skip := false // after an error the server ignores messages until Sync
	for {
		var msg pgproto3.BackendMessage
		if s.upRaw == nil {
			m, err := s.upIn.Receive()
			if err != nil {
				return
			}
			if s.upIn.ReadBufferLen() == 0 { // nothing of the stream left in the Frontend: frames from here on
				s.upRaw = newFrameReader(s.upConn)
			}
			msg = m
		} else {
			typ, body, err := s.upRaw.next()
			if err != nil {
				return
			}
			if s.passRaw(typ, skip) { // a row or copied data of the client's: its bytes as they came
				s.toClientRaw(typ, body)
				s.cliMu.Lock()
				big := s.pending > flushBytes
				s.cliMu.Unlock()
				if big || s.q.empty() {
					s.flushClient()
				}
				continue
			}
			if msg, err = decodeBackend(typ, body); err != nil {
				s.log.Error("cloud's stream unreadable", "err", err)
				return
			}
		}
		s.route(msg, &skip)
		if s.parking.Load() && s.q.empty() { // the reset's answer is in: the connection goes back (park)
			return
		}
		switch msg.(type) {
		case *pgproto3.ReadyForQuery,
			// the client may be waiting without a ReadyForQuery coming: an error after Flush,
			// COPY handing over the turn, an Execute with a row limit, async notifications
			*pgproto3.ErrorResponse, *pgproto3.CopyInResponse, *pgproto3.CopyOutResponse,
			*pgproto3.CopyBothResponse, *pgproto3.PortalSuspended, *pgproto3.NotificationResponse,
			// a NOTICE of a long statement is the client's progress: at once, as Postgres sends it
			*pgproto3.NoticeResponse:
			s.flushClient()
			continue
		}
		s.cliMu.Lock()
		big := s.pending > flushBytes
		s.cliMu.Unlock()
		// nothing else pending: the client sent Flush instead of Sync, or the server spoke on its own
		if big || s.q.empty() {
			s.flushClient()
		}
	}
}

func (s *session) route(msg pgproto3.BackendMessage, skip *bool) {
	switch m := msg.(type) {
	case *pgproto3.NotificationResponse:
		if m.Channel == metaChannel { // the proxy's own (metaChannel): not the client's business
			s.metaNotified.Store(true)
			s.srv.credentialsNotified()
			return
		}
		s.toClient(msg)
		return
	case *pgproto3.NoticeResponse, *pgproto3.ParameterStatus:
		s.toClient(msg)
		return
	}
	if *skip {
		for h := s.q.head(); h != nil && h.kind != kSync; h = s.q.head() {
			h.finish(false) // skipped by the server: it never ran
			s.q.pop()
		}
		*skip = false
	}
	h := s.q.head()
	if h == nil {
		s.toClient(msg)
		return
	}
	done := false
	switch m := msg.(type) {
	case *pgproto3.ErrorResponse:
		if s.pc != nil && (strings.HasPrefix(m.Code, "28") || m.Code == "42501") {
			s.evict.Store(true) // Postgres refused the role something: the cache of its key goes (pool.go)
		}
		s.orgDirty.Store(true) // the failed transaction undoes the set_config sent in it
		if s.direct.deferred.Load() && (h.mode != mCapture || !h.cap.untilSync) {
			s.direct.deferredErr.Store(true) // an error the client gets in its deferred transaction aborts it (relayRFQ, txFailed: review PR-4)
		}
		// A simple query or FunctionCall ends with ReadyForQuery anyway; so does a Sync whose
		// own processing failed (the implicit transaction's COMMIT: a deferred FK, a
		// serialization failure): nothing is skipped, the ReadyForQuery that follows ends it.
		if h.kind == kQuery || h.kind == kFunc || h.kind == kSync {
			h.failed = true
			s.deliver(h, msg)
			return
		}
		switch h.mode {
		case mDrop:
			s.toClient(msg) // the client's own messages were skipped: it must see why
		case mCapture: // captureExt inside a pipeline: the same, and the proxy learns of it
			if !h.cap.untilSync {
				s.toClient(msg)
			}
			s.deliver(h, msg)
		default:
			s.deliver(h, msg)
		}
		h.finish(false)
		s.q.pop()
		*skip = true
		if !s.q.hasSync() {
			s.q.setDead(true) // until the client's Sync: see queue.dead
		}
		return
	case *pgproto3.CopyInResponse:
		if h.kind == kExec {
			// COPY FROM STDIN through Execute: the server ignores the Sync that followed it
			// (it is in copy-in mode when it reads it); the client sends another after CopyDone.
			s.q.dropNextSync()
			s.copyExt.Store(true)
		}
	case *pgproto3.ReadyForQuery:
		if prev := s.tx.Swap(uint32(m.TxStatus)); prev != 'I' && m.TxStatus == 'I' {
			s.pathTxEnd()
		}
		done = h.kind == kQuery || h.kind == kSync || h.kind == kFunc
		if h.kind == kSync {
			s.q.setDead(false)
		}
	case *pgproto3.ParseComplete:
		done = h.kind == kParse
	case *pgproto3.BindComplete:
		done = h.kind == kBind
	case *pgproto3.CloseComplete:
		done = h.kind == kClose
	case *pgproto3.NoData, *pgproto3.RowDescription:
		done = h.kind == kDescS || h.kind == kDescP
	case *pgproto3.CommandComplete, *pgproto3.EmptyQueryResponse, *pgproto3.PortalSuspended:
		done = h.kind == kExec
		if cc, ok := m.(*pgproto3.CommandComplete); ok {
			h.tag = string(cc.CommandTag)
		}
	}
	s.deliver(h, msg)
	if done {
		ok := !h.failed // a query, Sync or FunctionCall: no error before its ReadyForQuery
		if h.kind == kExec {
			_, ok = msg.(*pgproto3.CommandComplete)
		}
		h.finish(ok)
		s.q.pop()
	}
}

func (s *session) deliver(h *expect, msg pgproto3.BackendMessage) {
	if _, ok := msg.(*pgproto3.CommandComplete); ok && h.tagAs != "" && h.mode != mCapture {
		msg = &pgproto3.CommandComplete{CommandTag: []byte(h.tagAs)}
	}
	switch h.mode {
	case mFwd:
		if r, ok := msg.(*pgproto3.ReadyForQuery); ok {
			msg = s.relayRFQ(r.TxStatus) // the client's status in a deferred transaction (direct_tx.go)
		}
		s.toClient(msg)
	case mHold:
		if _, ok := msg.(*pgproto3.ReadyForQuery); !ok {
			s.toClient(msg)
		}
	case mCapture:
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			row := make([]string, len(m.Values))
			for i, v := range m.Values {
				row[i] = string(v)
			}
			h.cap.rows = append(h.cap.rows, row)
		case *pgproto3.ErrorResponse:
			h.cap.err = &upstreamError{code: m.Code, msg: m.Message}
			r := *m
			h.cap.resp = &r
			if h.kind != kQuery && !h.cap.untilSync { // captureExt with Flush: no ReadyForQuery comes
				h.cap.finish()
			}
		case *pgproto3.CommandComplete, *pgproto3.EmptyQueryResponse:
			if h.kind == kExec && !h.cap.untilSync {
				h.cap.finish()
			}
		case *pgproto3.ParameterDescription:
			h.cap.oids = append([]uint32{}, m.ParameterOIDs...)
		case *pgproto3.NoData, *pgproto3.RowDescription:
			h.cap.cols = []uint32{}
			if rd, ok := m.(*pgproto3.RowDescription); ok {
				for _, f := range rd.Fields {
					h.cap.cols = append(h.cap.cols, f.DataTypeOID)
				}
			}
			if h.kind == kDescS && !h.cap.untilSync {
				h.cap.finish()
			}
		case *pgproto3.ReadyForQuery:
			h.cap.finish()
		}
	}
}

func kindOf(m pgproto3.FrontendMessage) expectKind {
	switch m := m.(type) {
	case *pgproto3.Query:
		return kQuery
	case *pgproto3.Parse:
		return kParse
	case *pgproto3.Bind:
		return kBind
	case *pgproto3.Describe:
		if m.ObjectType == 'S' {
			return kDescS
		}
		return kDescP
	case *pgproto3.Execute:
		return kExec
	case *pgproto3.Close:
		return kClose
	case *pgproto3.Sync:
		return kSync
	case *pgproto3.FunctionCall:
		return kFunc
	}
	return 0
}

// clone copies a received message: pgproto3 reuses its buffers on the next Receive.
func clone(m pgproto3.FrontendMessage) pgproto3.FrontendMessage {
	switch m := m.(type) {
	case *pgproto3.Parse:
		return &pgproto3.Parse{Name: m.Name, Query: m.Query, ParameterOIDs: append([]uint32(nil), m.ParameterOIDs...)}
	case *pgproto3.Bind:
		c := &pgproto3.Bind{DestinationPortal: m.DestinationPortal, PreparedStatement: m.PreparedStatement,
			ParameterFormatCodes: append([]int16(nil), m.ParameterFormatCodes...),
			ResultFormatCodes:    append([]int16(nil), m.ResultFormatCodes...),
			Parameters:           make([][]byte, len(m.Parameters))}
		for i, p := range m.Parameters {
			if p != nil {
				c.Parameters[i] = append([]byte{}, p...)
			}
		}
		return c
	case *pgproto3.Describe:
		c := *m
		return &c
	case *pgproto3.Execute:
		c := *m
		return &c
	case *pgproto3.Close:
		c := *m
		return &c
	}
	return m
}

// batchParsesUnnamed: the batch has a Parse of the unnamed statement.
func batchParsesUnnamed(batch []pgproto3.FrontendMessage) bool {
	for _, m := range batch {
		if p, ok := m.(*pgproto3.Parse); ok && p.Name == "" {
			return true
		}
	}
	return false
}

// noRows: lokadb.org of a statement that reads no LokaDB row: no organization's rows (review of
// routing, R1, R7).
const noRows = "-1"

// largeObjectFuncs: the built-in oids of the large object functions a fastpath call may run
// (pgjdbc's LargeObjectManager, Hibernate's @Lob, libpq's lo_*): lo_open, lo_close, loread,
// lowrite, lo_lseek, lo_creat, lo_tell, lo_unlink, lo_truncate, lo_create, lo_lseek64,
// lo_tell64, lo_truncate64, lo_from_bytea, lo_get (two), lo_put. Not lo_import and lo_export:
// they read and write the server's files.
var largeObjectFuncs = map[uint32]bool{952: true, 953: true, 954: true, 955: true, 956: true, 957: true, 958: true, 964: true,
	1004: true, 715: true, 3170: true, 3171: true, 3172: true, 3457: true, 3458: true, 3459: true, 3460: true}
