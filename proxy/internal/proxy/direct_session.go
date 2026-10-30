package proxy

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	pg "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/lokadb/lokadb/proxy/internal/creds"
	"github.com/lokadb/lokadb/proxy/internal/rel"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// The direct path of the profile fdw+direct, stage 8a (tasks/direct-path.md): a statement of
// one moved organization, outside a transaction block, goes to the organization's database
// on a connection of the session's own instead of through the cloud and postgres_fdw. 8a
// takes reads and UPDATEs that change neither the key nor the organization column (nothing
// the refs tables hold); everything else goes the cloud's way, as in the profile fdw.
//
// The pause: move-org, move-back and org-profile hold org_lock exclusively. Before each
// direct statement the cloud connection takes it shared (the gate), reads the profile again in
// a statement of its own (a snapshot taken after the lock: a switch committed while it waited
// is seen) and releases it once the organization's database answered. A statement that
// waited out a move-back finds the organization in the cloud and goes the cloud's way.

const ProfileFDWDirect = "fdw+direct"

// directState: the session's direct connections and what they need from the cloud session.
type directState struct {
	conns map[string]*directConn // organization -> its connection
	// idleCheck: when closeIdleDirect last looked at conns
	idleCheck time.Time
	info      *byteCache[*Analysis, *directInfo]
	// infoNamed: the same under an organization the application named (the cooperative mode:
	// the statement's own conditions need not confine it)
	infoNamed *byteCache[*Analysis, *directInfo]
	// off: why the session's direct path is off for good ("" on): a setting the proxy cannot
	// repeat on the organization's connection (set_config(), SET ROLE, a SET in a transaction).
	off string
	mu  sync.Mutex // sets: appended by the relay goroutine (onOK)
	// sets: the session's SET / RESET / DISCARD ALL that ran in the cloud, in order; each
	// direct connection repeats those it has not yet before its next statement.
	sets []directSet
	// kinds: directSetting of the texts seen (most contain "set" as UPDATE … SET)
	kinds *byteCache[string, settingKind]
	// unresolved: organizations whose gate found a table missing by its name (logged once)
	unresolved map[string]bool
	// tx: the deferred or direct transaction (direct_tx.go, 8a.2); deferred: tx is deferred,
	// read by the relay goroutine (relayRFQ)
	tx       *directTx
	deferred atomic.Bool
	// deferredErr: the relay passed the client an error of the cloud while tx was deferred (a
	// batch without Execute: a Parse that does not parse, a Bind of no statement): the client's
	// transaction is aborted, as Postgres's would be (review PR-4)
	deferredErr atomic.Bool
	learn       *learnTx // the cloud transaction being learnt for the direct path
	// copy: a COPY FROM STDIN streaming into the organization's database (direct_copy.go): the
	// client's CopyData, CopyDone and CopyFail go there, not to the cloud
	copy *directCopy
	// unnamed: the client's unnamed statements Parsed in a batch (unnamedStmt), by text and
	// parameter types
	unnamed map[string]*stmtInfo
	// lostUnnamed: the client's last unnamed Parse a direct batch took (the cloud never had it):
	// a Bind or a Describe of the unnamed statement that comes without a Parse of its own
	// Parses it in the cloud first (block 9, review PR-14); cleared by the next Parse "" or
	// simple query, which destroy the unnamed statement in Postgres too
	lostUnnamed *pgproto3.Parse
	// owedParse: the client's Parse of its unnamed statement was taken out of a write's batch
	// (its text is rewritten there): its ParseComplete goes before whatever the client gets next
	owedParse atomic.Bool
}

type settingKind struct {
	sets    []directSet // the statements of the text to repeat, each cut out of it
	unknown bool
}

// resetsRole: the text resets every setting (RESET ALL, DISCARD ALL): the role among them.
func (k settingKind) resetsRole() bool {
	for _, d := range k.sets {
		if d.discard || d.name == "" {
			return true
		}
	}
	return false
}

// directSet: one settings statement to repeat: its own text (a SET, RESET or DISCARD ALL cut
// out of the client's string by the parser's location, never the whole string: review of
// block 9, 3), the setting ("" for all of them), DISCARD ALL.
type directSet struct {
	sql, name string
	discard   bool
	// cloud: a set_config(name, …) of the client's; its value is the cloud's current_setting
	// at the time of the repeat (sql empty)
	cloud bool
}

type directConn struct {
	org     string
	addr    string // CancelRequest (cancelDirect)
	pid     uint32
	secret  []byte
	tls     *tls.Config
	plainOK bool
	conn    net.Conn
	fe      *pgproto3.Frontend
	applied int                  // directState.sets repeated on it
	names   map[*stmtInfo]string // the client's statements Parsed on it, under names of the proxy's own
	n       int                  // the last lokadb_d<n> given
	used    time.Time            // its last answer (probeDirect)
	path    string               // its search_path, in a database with schemas (directConnFor): "" unknown
	// the pool of direct connections (direct_pool.go): poolKey "" when the pool is off; born,
	// when it was opened; evict, the server refused its role something
	poolKey string
	born    time.Time
	evict   bool
	dsn     string // the role's DSN it was opened with (customersApp): another one now, it is closed
	// dbID: the organization's database id the check of the database saw it with (DBIDs, when
	// it opened): another one now (repoint, the address unchanged), it is closed, pooled or not
	dbID string
}

// directInfo: what the direct path needs of a statement besides Direct's verdict.
type directInfo struct {
	ok        bool // Direct's verdict, and the statement is a read or an UPDATE 8a takes
	qualified bool // it names a LokaDB table of public as public.t (another schema in the organization's database)
	tables    []string
	// update, setCols: the table an UPDATE changes and the columns it sets ("" for a read)
	update  string
	setCols []string
	// ops: the operators the statement names (=, %, <->…), which the gate requires to be the
	// server's own: an extension's (pg_trgm in the cloud's public) is not found in the
	// organization's database under its schema
	ops []string
	// gateSQL: the gate's check of the statement (gateCheck), its organization and partitions
	// parameters: one text, prepared once, for every organization (perf3-11)
	gateSQL string
	// write: an INSERT or a DELETE (stage 8b, direct_write.go); nil for a read or an UPDATE
	write *directWrite
	// why: the first reason the direct path does not take it ("" when ok): Direct's verdict, or
	// one of the Shape* below (DirectRefusal: check -profile judges by it)
	why string
}

// Why the direct path does not take a statement Direct lets through (its form: directShape).
const (
	ShapeTwoInserts       = "inserts_two_tables"         // an INSERT into two LokaDB tables (a data-modifying CTE)
	ShapeStatements       = "several_statements"         // more than one statement in the string
	ShapeAmbiguousName    = "ambiguous_name"             // a bare name of several schemas, the search path unknown
	ShapeStatementForm    = "statement_form"             // not a read, an UPDATE, an INSERT or a DELETE the direct path writes (SELECT INTO, upsert forms it cannot)
	ShapeModifyingCTE     = "data_modifying_cte"         // a data-modifying CTE
	ShapeMerge            = "merge"                      // MERGE
	ShapeRowTrigger       = "cloud_row_trigger"          // a row trigger of the cloud on the UPDATE's table (an FK onto a cloud table among them)
	ShapeKeyUpdate        = "update_key_or_organization" // an UPDATE of the key or the organization column: the refs tables
	ShapeOperator         = "extension_operator"         // an operator off the server's own (an extension's in the cloud)
	ShapeUserType         = "application_type"           // a column of a type of the application's: another OID in each database
	ShapeStatementTrigger = "cloud_statement_trigger"    // a statement trigger of the cloud on the table
	ShapeChangesLookups   = "changes_lookups"            // it changes a column the proxy finds organizations by
)

// DirectRefusal: why the direct path does not take sql by its form, one of the Shape* ("" when
// it does, or when Direct refuses it: its Reason): the proxy's own decision (directShape),
// which check -profile reports instead of a copy of it.
func DirectRefusal(sql string, m *Meta, named bool, path []string) string {
	if !DirectIn(sql, m, named, path).OK {
		return "" // Direct's own reason (DirectVerdict.Reason)
	}
	return directShape(sql, analyzeIn(sql, m, path), m, named, path).why
}

// directRetry: after a failed connection to an organization's database the proxy sends its
// statements the cloud's way for this long before it tries again.
const directRetry = 10 * time.Second

// directLink: an open connection to an organization's database; addr, pid, secret: where
// and with which key a CancelRequest for it goes (addr "": nowhere).
type directLink struct {
	conn   net.Conn
	fe     *pgproto3.Frontend
	addr   string
	pid    uint32
	secret []byte
	// tls: the DSN's TLS (nil: none), which a CancelRequest goes through too; plainOK: the
	// DSN falls back to a connection without it (sslmode=prefer, allow)
	tls     *tls.Config
	plainOK bool
}

// directDialer opens a connection to an organization's database (Server.dialDirect: tests).
type directDialer func(ctx context.Context, dsn string, params map[string]string) (*directLink, error)

func pgDial(ctx context.Context, dsn string, params map[string]string) (*directLink, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	for k, v := range params {
		cfg.RuntimeParams[k] = v
	}
	cfg.DialFunc = tunnel.DialProving(directNetDial) // a silent network ends the connection (direct_net.go, PR-6); a relay's port: proven first (R01)
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	hj, err := conn.Hijack()
	if err != nil {
		return nil, err
	}
	l := &directLink{conn: hj.Conn, fe: hj.Frontend, addr: net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)), pid: hj.PID, secret: hj.SecretKey, tls: cfg.TLSConfig}
	for _, f := range cfg.Fallbacks {
		l.plainOK = l.plainOK || f.TLSConfig == nil
	}
	return l, nil
}

// cancelDirect sends the client's CancelRequest on to the statement its session runs on a
// direct connection, if one runs (forwardCancel; through the relay and agent, as a
// connection of its own, like libpq's), with the TLS of the connection's DSN: a relay that
// requires TLS for the organization takes no cancel in clear (review of block 9, B).
func (s *session) cancelDirect() {
	dc := s.directBusy.Load()
	if dc == nil || dc.addr == "" {
		return
	}
	if err := sendCancel(dc.addr, dc.tls, dc.plainOK, dc.pid, dc.secret); err != nil {
		s.log.Warn("cancel not sent to the organization's database", "org", dc.org, "err", err)
	}
}

func sendCancel(addr string, tc *tls.Config, plainOK bool, pid uint32, secret []byte) error {
	raw, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
	var c net.Conn = raw
	if tc != nil {
		req, _ := (&pgproto3.SSLRequest{}).Encode(nil)
		if _, err := raw.Write(req); err != nil {
			return err
		}
		b := make([]byte, 1)
		if _, err := io.ReadFull(raw, b); err != nil {
			return err
		}
		switch {
		case b[0] == 'S':
			t := tls.Client(raw, tc)
			if err := t.Handshake(); err != nil {
				return err
			}
			c = t
		case !plainOK:
			return fmt.Errorf("the server refused TLS (%q), which the DSN requires", b[0])
		}
	}
	buf, err := (&pgproto3.CancelRequest{ProcessID: pid, SecretKey: secret}).Encode(nil)
	if err != nil {
		return err
	}
	_, err = c.Write(buf)
	return err
}

// cancelKey: a session's key in Server.sessions, the BackendKeyData the client got.
func cancelKey(pid uint32, secret []byte) string { return fmt.Sprintf("%d/%x", pid, secret) }

// directOff turns the session's direct path off for good.
func (s *session) directOff(why string) {
	if s.direct.off == "" {
		s.direct.off = why
		if s.meta != nil && len(s.meta.Profiles) > 0 {
			s.log.Info("direct path off for this session: its statements go through the cloud", "reason", why)
		}
	}
}

// closeDirect closes the session's direct connections (the session ends), or gives the clean
// ones back to the pool (direct_pool.go).
func (s *session) closeDirect() {
	for _, dc := range s.direct.conns {
		if !s.directStale(dc) && s.releasing() && s.parkDirect(dc) { // not one of an old address or database, no cancel on its way (review P9)
			s.srv.directPool.put(dc.poolKey, dc, dc.born)
			continue
		}
		if dc.evict && s.srv.directPool != nil {
			s.srv.directPool.evict(dc.poolKey)
		}
		closeDirectConn(dc)
	}
	s.direct.conns = nil
}

// directSafe: the session settings a direct connection takes from the client (SET, RESET,
// set_config, startup parameters), a white list as for functions: how values are shown and
// parsed and how long a statement may wait. Any other (row_security,
// session_replication_role, default_transaction_read_only, an extension's) turns the direct
// path of the session off (coordinator's decision of 25.10). search_path is not taken: the
// direct connection keeps the organization's schema (directConnFor).
var directSafe = map[string]bool{
	"timezone": true, "datestyle": true, "intervalstyle": true, "client_encoding": true,
	"extra_float_digits": true, "application_name": true, "statement_timeout": true, "lock_timeout": true,
	"idle_in_transaction_session_timeout": true, "standard_conforming_strings": true, "bytea_output": true,
	"client_min_messages": true, // Rails sets it at every connection; which notices the client gets (block 9's review)
}

// directSetting: what sql does to the session's settings. sets: its SET and RESET of
// directSafe settings, RESET ALL and DISCARD ALL at session level, which a direct connection
// repeats, each with its own text alone, and set_config of a directSafe setting, repeated
// with the cloud's value; unknown: what it cannot repeat (any other setting, SET ROLE, SET
// SESSION AUTHORIZATION, DISCARD of a part). lokadb.* settings are the proxy's and the
// cloud's, never repeated; search_path is not repeated (directSafe).
func directSetting(sql string) (sets []directSet, unknown bool) {
	if !hasFold(sql, "set_config") && !settingStmt.MatchString(sql) {
		return nil, false // no SET, RESET, DISCARD or set_config: no parse (review of performance, PF12: UPDATE … SET, OFFSET)
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return nil, false
	}
	for _, raw := range tree.Stmts {
		text := func() string {
			from, n := int(raw.StmtLocation), int(raw.StmtLen)
			if from < 0 || from > len(sql) {
				return ""
			}
			if n == 0 || from+n > len(sql) {
				n = len(sql) - from
			}
			return strings.TrimSpace(sql[from : from+n])
		}
		switch {
		case raw.Stmt.GetVariableSetStmt() != nil:
			vs := raw.Stmt.GetVariableSetStmt()
			n := strings.ToLower(vs.Name)
			switch {
			case vs.IsLocal || strings.HasPrefix(n, "lokadb."):
			case n == "transaction" || n == "transaction_snapshot": // the transaction's own
			case n == "search_path":
				sets = append(sets, directSet{name: n}) // noted, not repeated
			case vs.Kind == pg.VariableSetKind_VAR_RESET_ALL || directSafe[n]:
				if t := text(); t != "" {
					sets = append(sets, directSet{sql: t, name: n})
				} else {
					unknown = true
				}
			default: // SET ROLE, SESSION AUTHORIZATION, SESSION CHARACTERISTICS, row_security…
				unknown = true
			}
		case raw.Stmt.GetDiscardStmt() != nil:
			if raw.Stmt.GetDiscardStmt().Target != pg.DiscardMode_DISCARD_ALL {
				unknown = true // DISCARD PLANS / SEQUENCES / TEMP: no settings, but not ours to judge
			} else if t := text(); t != "" {
				sets = append(sets, directSet{sql: t, discard: true})
			}
		}
		Walk(raw, func(msg proto.Message) {
			f, ok := msg.(*pg.FuncCall)
			if !ok || len(f.Funcname) == 0 || f.Funcname[len(f.Funcname)-1].GetString_().GetSval() != "set_config" {
				return
			}
			// set_config('TimeZone', $1, false) (Django at every connection): the name is known,
			// the value comes from the cloud when the proxy repeats it, whatever the call computed;
			// a name that is not a constant or not directSafe turns the direct path off
			var name string
			if len(f.Args) > 0 {
				a := f.Args[0]
				if tc := a.GetTypeCast(); tc != nil { // 'TimeZone'::text
					a = tc.Arg
				}
				if c := a.GetAConst(); c != nil && c.GetSval() != nil {
					name = strings.ToLower(strings.TrimSpace(c.GetSval().GetSval()))
				}
			}
			switch {
			case strings.HasPrefix(name, "lokadb."):
			case name == "transaction_isolation" || name == "transaction_read_only" || name == "transaction_deferrable":
			case name == "search_path":
				sets = append(sets, directSet{name: name}) // noted, not repeated
			case directSafe[name]:
				sets = append(sets, directSet{name: name, cloud: true})
			default:
				unknown = true
			}
		})
	}
	return sets, unknown
}

// noteSetting records a settings statement that ran in the cloud (onOK of its message) for the
// direct connections; one inside a transaction block (rolled back or not) or one the proxy
// cannot repeat turns the direct path off.
func (s *session) noteSetting(sql string, inTx bool) func() {
	k, ok := s.direct.kinds.get(sql)
	if !ok {
		k.sets, k.unknown = directSetting(sql)
		s.direct.kinds.put(sql, k, textBytes(sql)/4+128)
	}
	if k.unknown || k.resetsRole() {
		s.metaRoleMoved = true // the role may be another now (the shared metadata are by role)
	}
	switch {
	case k.unknown:
		s.directOff("a setting the proxy cannot repeat on the organization's connection (one off its white list, SET ROLE, set_config with a computed name)")
	case len(k.sets) > 0 && inTx:
		s.directOff("a session setting changed inside a transaction")
	case len(k.sets) > 0:
		return func() {
			s.direct.mu.Lock()
			s.direct.sets = append(s.direct.sets, k.sets...)
			s.direct.mu.Unlock()
		}
	}
	return nil
}

// chainOK runs a, then b (either may be nil).
func chainOK(a, b func()) func() {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	return func() { a(); b() }
}

// info: what the direct path needs of an analysed statement, worked out once (under the
// organization the application named, if it named one: namedOrg).
func (s *session) directInfo(sql string, an *Analysis) *directInfo {
	named := s.namedOrg() != ""
	cache := s.direct.info
	if named {
		cache = s.direct.infoNamed
	}
	if di, ok := cache.get(an); ok {
		return di
	}
	di := directShape(sql, an, s.meta, named, s.pathFor())
	cache.put(an, di, textBytes(sql)) // the key keeps the analysis alive
	return di
}

// namedOrg: the organization the application named (lokadb.tenant, the cooperative mode) for
// what runs now; "" for none and for '*'.
func (s *session) namedOrg() string {
	if v := s.tenant.Value(); v != TenantAll {
		return v
	}
	return ""
}

// directShape: what the direct path takes of sql, by its form alone (the session's
// directInfo caches it; check -profile asks it through DirectTakes, so its estimate is the
// proxy's behaviour).
func directShape(sql string, an *Analysis, m *Meta, named bool, path []string) *directInfo {
	di := &directInfo{}
	if v, done := directRefusedBy(an); done {
		di.why = v.Reason
		return di
	}
	// one parse for the verdict and the shape (second review of performance, F9: two more
	// parses of every new text, some 190 µs)
	tree, err := pg.Parse(sql)
	if err != nil {
		di.why = DirectRefused
		return di
	}
	v := directInTree(tree, an, m, named, path) // an: sql's analysis by path (the caller's)
	if !v.OK || len(v.Inserts) > 1 {
		di.why = v.Reason
		if v.OK {
			di.why = ShapeTwoInserts
		}
		return di
	}
	di.tables = v.Lokadb
	if len(tree.Stmts) != 1 {
		di.why = ShapeStatements
		return di
	}
	top := tree.Stmts[0].Stmt
	// public.t of a LokaDB table, as written: in the organization's database public's tables
	// may be in another schema (the rest are by key once qualify gave the bare names theirs;
	// qualify never writes public, so the tree directInTree qualified still shows what was written)
	Walk(tree.Stmts[0], func(msg proto.Message) {
		if x, ok := msg.(*pg.RangeVar); ok && x.Schemaname == "public" && m.table(x) != nil {
			di.qualified = true
		}
	})
	if qualify(top, m, path) != "" {
		di.why = ShapeAmbiguousName
		return di // a name of several schemas, the path unknown (block 4's review: check asks DirectTakesIn)
	}
	ok := true
	no := func(why string) { // the first reason stays
		if ok {
			di.why = why
		}
		ok = false
	}
	if top.GetInsertStmt() != nil || top.GetDeleteStmt() != nil { // 8b: direct_write.go
		if di.write = buildWrite(top, m); di.write == nil {
			no(ShapeStatementForm)
		}
	} else if !(top.GetSelectStmt() != nil && top.GetSelectStmt().IntoClause == nil || top.GetUpdateStmt() != nil) {
		no(ShapeStatementForm)
	}
	Walk(tree.Stmts[0], func(msg proto.Message) {
		switch x := msg.(type) {
		case *pg.InsertStmt:
			if x != top.GetInsertStmt() {
				no(ShapeModifyingCTE)
			}
		case *pg.DeleteStmt:
			if x != top.GetDeleteStmt() {
				no(ShapeModifyingCTE)
			}
		case *pg.MergeStmt:
			no(ShapeMerge)
		case *pg.UpdateStmt:
			t := m.table(x.Relation)
			switch {
			case t == nil || x != top.GetUpdateStmt():
				no(ShapeModifyingCTE)
				return
			case m.rowTrigger(t.Name, trigUpdate):
				no(ShapeRowTrigger) // a row trigger of the cloud on UPDATE would not fire there (stage 8b)
				return
			}
			di.update = t.Name
			for _, n := range x.TargetList {
				col := n.GetResTarget().GetName()
				kc, composite := tupleCols(t.Key)
				if col == t.Key || t.Key == "" && col == "id" || col == t.TenantCol || composite && slices.Contains(kc, col) {
					no(ShapeKeyUpdate) // the row changes its key or organization: the refs tables
				}
				di.setCols = append(di.setCols, col)
			}
		case *pg.A_Expr:
			if !di.addOp(x.Name) {
				no(ShapeOperator)
			}
		case *pg.SortBy:
			if !di.addOp(x.UseOp) {
				no(ShapeOperator)
			}
		case *pg.SubLink:
			if !di.addOp(x.OperName) {
				no(ShapeOperator)
			}
		}
	})
	for _, t := range v.Lokadb {
		switch tb := m.Tables[t]; {
		case tb == nil, tb.UserTypes:
			no(ShapeUserType) // another type OID in each database
		case m.StmtTriggers[t]:
			no(ShapeStatementTrigger) // a statement trigger of the cloud's parent
		}
	}
	// a DELETE frees keys (ChangesLookups): resolveOrg has told the other sessions already
	if an.ChangesLookups && !(di.write != nil && di.write.del) {
		no(ShapeChangesLookups)
	}
	di.ok = ok
	return di
}

// DirectTakes: the direct path takes sql by its form (Direct, then the forms of 8a and 8b:
// reads, UPDATEs that keep key and organization, INSERT … VALUES and DELETE of 8b, no table
// with a row or statement trigger of the cloud, no application type); whether a statement
// goes direct also depends on its organization at run time. named: the application named the
// organization (lokadb.tenant), the statement's own conditions need not (the cooperative mode,
// the second step of 8a.2).
func DirectTakes(sql string, m *Meta, named bool) bool {
	return DirectTakesIn(sql, m, named, defaultPath)
}

// DirectTakesIn is DirectTakes with the bare names resolved by search path path.
func DirectTakesIn(sql string, m *Meta, named bool, path []string) bool {
	return directShape(sql, analyzeIn(sql, m, path), m, named, path).ok
}

// directOrg: the organization whose database sql goes to straight, "" for the cloud's way.
// vals reads the statement's values; ptypes: the parameter types the client encodes for (nil
// for a simple query). Call only when the queue is empty and not dead: it may look up ids.
func (s *session) directOrg(sql string, an *Analysis, vals valuesFunc, ptypes []uint32) (string, *directInfo) {
	org := s.directOrgOf(sql, an, vals, ptypes)
	if org == "" {
		return "", nil
	}
	return org, s.directInfo(sql, an)
}

func (s *session) directOrgOf(sql string, an *Analysis, vals valuesFunc, ptypes []uint32) string {
	if s.direct.off != "" || s.meta == nil || len(s.meta.Profiles) == 0 || s.txStatus() != 'I' && s.direct.tx == nil {
		return ""
	}
	if !an.Lokadb || an.TxControl || an.ResetsOrg || an.Utility ||
		len(an.Events) > 0 || len(an.Prelude) > 0 || an.rejection() != "" || len(an.Stmts) > 1 {
		return ""
	}
	for _, t := range ptypes {
		if t >= 16384 { // a type of its own: another OID in the organization's database
			return ""
		}
	}
	org := s.namedOrg() // the organization first: the shape is parsed only for one of a direct profile (review F9)
	if org != "" {
		// the cooperative mode: the organization the application named, the statement's values
		// reconciled with it as the cloud's way does (tenantOrg), not looked up; rows of another
		// organization the proxy does not know of are not in this organization's database, as
		// the cloud's policy lokadb_tenant hides them
		if s.tenantOrg(sql, an, org, vals) != nil {
			return "" // the cloud's way answers the mismatch
		}
	} else {
		var incomplete, unknown bool
		var err error
		org, incomplete, unknown, err = s.resolveOrg(an, vals, true)
		if err != nil || incomplete || unknown {
			return ""
		}
	}
	if !realOrg(org) {
		return ""
	}
	// the profile and the config by the organization's number (one numbered by uuid: block 18)
	num := s.meta.orgNum(org)
	if num == "" || !directProfile(s.meta.Profiles[num]) || s.customersApp(org) == "" {
		return ""
	}
	di := s.directInfo(sql, an)
	if !di.ok {
		return ""
	}
	if s.meta.Profiles[num] == ProfileDirect && s.namedOrg() == "" && di.write == nil && di.update == "" {
		return "" // a read of profile direct names its organization (direct_only.go): refused there
	}
	if d := s.direct.tx; d != nil && d.org != "" && d.org != org {
		return "" // another organization's statement in a direct transaction: refused (direct_tx.go)
	}
	for _, t := range di.tables {
		if s.meta.Foreign[t][org] == "" { // not moved with the organization
			return ""
		}
	}
	if schema := s.meta.Schemas[num]; schema != "" && schema != "public" {
		switch {
		case di.qualified:
			return "" // public.t: in the organization's database t is in its schema
		case !s.meta.SchemasUsed && (s.params["search_path"] != "" || s.setsSearchPath()):
			return "" // the connection's search_path is the organization's schema alone
		}
	}
	return org
}

func (s *session) setsSearchPath() bool {
	s.direct.mu.Lock()
	defer s.direct.mu.Unlock()
	for _, d := range s.direct.sets {
		if d.name == "search_path" {
			return true
		}
	}
	return false
}

// customersApp: the DSN of the direct path's role in organization org's database (the
// config's customers_app, by the organization's number: orgNum, block 18); "" for none.
func (s *session) customersApp(org string) string {
	if dsn := s.srv.creds.dsn(s.db, s.meta.orgNum(org), creds.Direct); dsn != "" { // the control plane's (stage 9a)
		return dsn
	}
	return s.srv.cfg.CustomersApp[s.db][s.meta.orgNum(org)]
}

// orgDBID: the id the cloud records for org's database (lokadb.orgs.db_id; "" none).
func (s *session) orgDBID(org string) string {
	if s.meta == nil {
		return ""
	}
	return s.meta.DBIDs[s.meta.orgNum(org)]
}

// directStale: dc was opened for another address or checked against another database than
// the organization has now (repoint).
func (s *session) directStale(dc *directConn) bool {
	return dc.dsn != s.customersApp(dc.org) || dc.dbID != s.orgDBID(dc.org)
}

// directConnFor: the session's connection to org's database, opened on first use, with the
// session's settings repeated on it. An error: the statement goes the cloud's way.
func (s *session) directConnFor(org string) (*directConn, error) {
	dc := s.direct.conns[org]
	if dc != nil && s.directStale(dc) && (s.direct.tx == nil || s.direct.tx.dc != dc) {
		// the organization's address or database changed (repoint): the next statement opens
		// the new one; a transaction already on dc ends there (repoint paused the organization
		// for it)
		closeDirectConn(dc)
		delete(s.direct.conns, org)
		dc = nil
	}
	if dc == nil {
		params := map[string]string{}
		for k, v := range s.params {
			switch lk := strings.ToLower(k); {
			case lk == "user" || lk == "database" || strings.HasPrefix(lk, "lokadb."):
			case lk == "options" && strings.Contains(strings.ToLower(v), "lokadb."):
				return nil, fmt.Errorf("the startup options name lokadb settings")
			case lk == "search_path": // the organization's schema, below
			case lk == "options" && !directOptions(v), lk != "options" && !directSafe[lk]:
				s.directOff("a startup parameter the proxy does not take to the organization's connection: " + k)
				return nil, fmt.Errorf("startup parameter %s", k)
			default:
				params[k] = v
			}
		}
		// the organization's schema, whatever search_path the session sets in the cloud: names
		// on the direct path resolve there (and in pg_catalog), the gate checks that they name
		// the LokaDB tables in the cloud
		schema := s.meta.Schemas[s.meta.orgNum(org)]
		if schema == "" {
			schema = "public"
		}
		params["search_path"] = quoteIdent(schema)
		if s.meta.SchemasUsed { // the session's path, public replaced by the organization's schema
			params["search_path"] = s.orgSearchPath(schema)
		}
		dial := s.srv.dialDirect
		if dial == nil {
			dial = pgDial
		}
		key := s.db + "/" + org
		if t, ok := s.srv.directDown.Load(key); ok && time.Since(t.(time.Time)) < directRetry {
			return nil, fmt.Errorf("unreachable %s ago", time.Since(t.(time.Time)).Round(time.Second))
		}
		// a connection of the pool (direct_pool.go): reset, its role checked when it opened; the
		// session's settings are repeated on it from the start (below)
		poolKey := ""
		if p := s.srv.directPool; p != nil {
			poolKey = s.directPoolKeyFor(org, params)
			pooled, born, ok := p.get(poolKey)
			for ok && pooled.dbID != s.orgDBID(org) { // checked against another database (repoint): not this one
				closeDirectConn(pooled)
				pooled, born, ok = p.get(poolKey)
			}
			if ok {
				pooled.applied, pooled.names, pooled.n, pooled.used, pooled.evict = 0, map[*stmtInfo]string{}, 0, time.Now(), false
				pooled.path = params["search_path"] // DISCARD ALL took it back to the startup value (directSettings sets the session's)
				pooled.poolKey, pooled.born, pooled.dsn = poolKey, born, s.customersApp(org)
				if s.direct.conns == nil {
					s.direct.conns = map[string]*directConn{}
				}
				s.direct.conns[org] = pooled
				return s.directSettings(pooled)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		link, err := dial(ctx, s.customersApp(org), params)
		if err != nil {
			s.srv.directDown.Store(key, time.Now()) // the sessions of the proxy do not each wait for it
			return nil, err
		}
		dc = &directConn{org: org, conn: link.conn, fe: link.fe, addr: link.addr, pid: link.pid, secret: link.secret, tls: link.tls, plainOK: link.plainOK, names: map[*stmtInfo]string{}, used: time.Now(), path: params["search_path"], poolKey: poolKey, born: time.Now(), dsn: s.customersApp(org), dbID: s.orgDBID(org)}
		if why, err := s.directRoleCheck(dc); why != "" || err != nil {
			dc.conn.Close()
			s.srv.directDown.Store(key, time.Now())
			if err == nil {
				err = fmt.Errorf("customers_app of organization %s: the role is not one of DML alone: %s", org, why)
			}
			return nil, err
		}
		if err := s.directIdentity(dc, key); err != nil {
			return nil, err
		}
		s.srv.directDown.Delete(key)
		if s.direct.conns == nil {
			s.direct.conns = map[string]*directConn{}
		}
		s.direct.conns[org] = dc
	}
	return s.directSettings(dc)
}

// directSettings repeats on dc the session's settings it has not run yet (directConnFor).
func (s *session) directSettings(dc *directConn) (*directConn, error) {
	s.direct.mu.Lock()
	sets := s.direct.sets[dc.applied:]
	s.direct.mu.Unlock()
	for _, d := range sets {
		if d.sql == "" && !d.cloud && !d.discard { // search_path: noted, not repeated
			dc.applied++
			continue
		}
		if d.cloud {
			v, err := s.cloudSetting(d.name)
			if err != nil {
				s.dropDirect(dc)
				return nil, fmt.Errorf("the cloud's %s: %w", d.name, err)
			}
			d.sql = "SELECT set_config(" + sqlString(d.name) + ", " + sqlString(v) + ", false)"
		}
		dc.fe.Send(&pgproto3.Query{String: d.sql})
		if err := dc.fe.Flush(); err != nil {
			s.dropDirect(dc)
			return nil, err
		}
		for {
			m, err := dc.fe.Receive()
			if err != nil {
				s.dropDirect(dc)
				return nil, err
			}
			if e, ok := m.(*pgproto3.ErrorResponse); ok {
				s.dropDirect(dc)
				return nil, fmt.Errorf("repeating %q: %s", brief(d.sql), e.Message)
			}
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
		if d.discard { // DISCARD ALL dropped its statements
			dc.names = map[*stmtInfo]string{}
		}
		dc.applied++
	}
	if s.meta.SchemasUsed {
		// tables outside public (block 4): bare names resolve on the connection as in the
		// cloud, by the session's search path with public replaced by the organization's schema
		// (a table of another schema is in that schema there too)
		schema := s.meta.Schemas[s.meta.orgNum(dc.org)]
		if schema == "" {
			schema = "public"
		}
		if want := s.orgSearchPath(schema); dc.path != want {
			if err := s.directExec(dc, "SET search_path = "+want); err != nil {
				s.dropDirect(dc)
				return nil, fmt.Errorf("search_path: %w", err)
			}
			dc.path = want
		}
	}
	return dc, nil
}

// directExec runs q on dc, its answer the proxy's own.
func (s *session) directExec(dc *directConn, q string) error {
	dc.fe.Send(&pgproto3.Query{String: q})
	if err := dc.fe.Flush(); err != nil {
		return err
	}
	var failed error
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return err
		}
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			failed = fmt.Errorf("%s", m.Message)
		case *pgproto3.ReadyForQuery:
			return failed
		}
	}
}

// cloudSetting: the session's value of a setting in the cloud (a set_config of the client's to
// repeat on a direct connection), asked as gate asks, before it.
func (s *session) cloudSetting(name string) (string, error) {
	c := &capture{done: make(chan struct{}), untilSync: true}
	if err := s.send(s.extMsgs(c,
		&pgproto3.Close{ObjectType: 'S', Name: "lokadb_gs"}, &pgproto3.Close{ObjectType: 'P', Name: "lokadb_gsp"},
		&pgproto3.Parse{Name: "lokadb_gs", Query: "SELECT current_setting(" + sqlString(name) + ")"},
		&pgproto3.Bind{DestinationPortal: "lokadb_gsp", PreparedStatement: "lokadb_gs"}, &pgproto3.Execute{Portal: "lokadb_gsp"})); err != nil {
		return "", err
	}
	<-c.done
	if c.err != nil {
		return "", c.err
	}
	if len(c.rows) != 1 || len(c.rows[0]) != 1 {
		return "", fmt.Errorf("no value")
	}
	return c.rows[0][0], nil
}

// directRoleSQL: what the direct connection's role may do beyond DML on the organization's
// tables, one row each (none: DML alone). The direct path runs the application's statements
// under it, so it owns nothing, creates nothing and passes by nothing (review of block 9, 6;
// the owner's DSN, customers, is for DDL replay).
const directRoleSQL = `SELECT /* lokadb: role */ w FROM (
SELECT 'superuser, BYPASSRLS, CREATEROLE, CREATEDB or REPLICATION' AS w FROM pg_roles
  WHERE rolname = current_user AND (rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication)
UNION ALL SELECT 'CREATE on the database' WHERE has_database_privilege(current_database(), 'CREATE')
UNION ALL (SELECT 'CREATE on schema ' || nspname FROM pg_namespace
  WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema' AND has_schema_privilege(oid, 'CREATE') LIMIT 1)
UNION ALL (SELECT 'owns ' || relname FROM pg_class
  WHERE relkind IN ('r', 'p', 'v', 'm', 'f') AND pg_has_role(current_user, relowner, 'USAGE')
  AND relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace) LIMIT 1)
/* tables */) w`

// directRoleCheck: "" when dc's role is one of DML alone and has it on each of the
// organization's tables, else what is wrong (org-profile -direct-role grants it).
func (s *session) directRoleCheck(dc *directConn) (string, error) {
	schema := s.meta.Schemas[s.meta.orgNum(dc.org)]
	if schema == "" {
		schema = "public"
	}
	var need []string
	for t, parts := range s.meta.Foreign {
		if parts[dc.org] != "" {
			rel := sqlString(orgRel(t, schema))
			need = append(need, "UNION ALL SELECT 'no SELECT, INSERT, UPDATE, DELETE on ' || "+rel+
				" WHERE NOT coalesce(has_table_privilege(to_regclass("+rel+"), 'SELECT, INSERT, UPDATE, DELETE'), false)")
		}
	}
	sort.Strings(need)
	dc.fe.Send(&pgproto3.Query{String: strings.Replace(directRoleSQL, "/* tables */", strings.Join(need, "\n"), 1)})
	if err := dc.fe.Flush(); err != nil {
		return "", err
	}
	var why []string
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return "", err
		}
		switch m := m.(type) {
		case *pgproto3.DataRow:
			if len(m.Values) > 0 {
				why = append(why, string(m.Values[0]))
			}
		case *pgproto3.ErrorResponse:
			why = append(why, "the check failed: "+m.Message)
		case *pgproto3.ReadyForQuery:
			return strings.Join(why, "; "), nil
		}
	}
}

// orgRel: the table of key t in the organization's database: public's tables in its schema,
// another schema's in that schema.
func orgRel(t, schema string) string {
	s, n := rel.SplitKey(t)
	if s == "" || s == "public" {
		s = schema
	}
	return quoteIdent(s) + "." + quoteIdent(n)
}

func (s *session) dropDirect(dc *directConn) {
	dc.conn.Close()
	delete(s.direct.conns, dc.org)
}

// gateCheck: the gate's second statement for di's statement in org. It reads, in a snapshot
// taken after the pause lock: the organization's profile (lokadb.orgs, which enable lets
// PUBLIC read; no row: moved back); that each
// table's partition of org is still a foreign table (a switch that keeps the row); that the
// client's role may run the statement (the FDW path checks its privileges on the cloud's
// parent, the organization's database knows only the -customer-app role: review of block 9,
// A); that no row level security policy of the application's own is on the tables (the
// organization's database has none of them).
func (s *session) gateCheck(di *directInfo, org string) cloudStmt {
	// $1: the organization's number in lokadb.orgs (NULL: none), $2…: the names of its partitions
	// of the statement's tables, in di.tables' order
	params := [][]byte{nil}
	if num := s.meta.orgNum(org); num != "" {
		params[0] = []byte(num)
	}
	for _, t := range di.tables {
		_, partName := rel.SplitKey(s.meta.Foreign[t][org])
		params = append(params, []byte(partName))
	}
	if di.gateSQL != "" {
		return prepared(di.gateSQL, params...)
	}
	// The tables by the names the statement uses, resolved by the session's search_path as
	// the statement's own are (not a fixed public.: review of block 9, C); the partition in
	// its parent's schema. found: every one of them resolved (false: the direct path is off
	// for the statement, and the log says why).
	var conds, found []string
	for i, t := range di.tables {
		rel := s.gateRel(t)
		part := "(SELECT oid FROM pg_class WHERE relname = $" + itoa(i+2) +
			" AND relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = " + rel + "))"
		found = append(found, rel+" IS NOT NULL", part+" IS NOT NULL")
		conds = append(conds,
			"coalesce(has_table_privilege("+rel+", 'SELECT'), false)",
			"coalesce((SELECT relkind = 'f' FROM pg_class WHERE oid = "+part+"), false)",
			"NOT EXISTS (SELECT FROM pg_policy WHERE polrelid = "+rel+" AND polname <> 'lokadb_tenant')")
	}
	if di.update != "" {
		rel := s.gateRel(di.update)
		cols := []string{"false"}
		if len(di.setCols) > 0 {
			cols = nil
			for _, c := range di.setCols {
				cols = append(cols, "coalesce(has_column_privilege("+rel+", "+sqlString(c)+", 'UPDATE'), false)")
			}
		}
		conds = append(conds, "(coalesce(has_table_privilege("+rel+", 'UPDATE'), false) OR ("+strings.Join(cols, " AND ")+"))")
	}
	if w := di.write; w != nil { // the privilege the cloud's parent checks for it
		priv := "'INSERT'"
		if w.del {
			priv = "'DELETE'"
		}
		rel := s.gateRel(w.table.Name)
		conds = append(conds, "coalesce(has_table_privilege("+rel+", "+priv+"), false)")
		if w.doUpdate { // an upsert's UPDATE half: UPDATE of its SET columns, as for an UPDATE
			cols := []string{"false"}
			if len(w.setCols) > 0 {
				cols = nil
				for _, c := range w.setCols {
					cols = append(cols, "coalesce(has_column_privilege("+rel+", "+sqlString(c)+", 'UPDATE'), false)")
				}
			}
			conds = append(conds, "(coalesce(has_table_privilege("+rel+", 'UPDATE'), false) OR ("+strings.Join(cols, " AND ")+"))")
		}
	}
	if len(di.ops) > 0 { // the operators are the server's own, none an extension's or the application's
		var names []string
		for _, o := range di.ops {
			names = append(names, sqlString(o))
		}
		conds = append(conds, "NOT EXISTS (SELECT FROM pg_operator o WHERE o.oprname IN ("+strings.Join(names, ", ")+
			") AND (o.oprnamespace <> 'pg_catalog'::regnamespace OR EXISTS (SELECT FROM pg_depend d WHERE d.classid = 'pg_operator'::regclass AND d.objid = o.oid AND d.deptype = 'e')))")
	}
	// lokadb.orgs by the organization's number
	di.gateSQL = "SELECT coalesce((SELECT profile FROM lokadb.orgs WHERE org = $1), ''), (" + strings.Join(conds, " AND ") + ")::text, (" +
		strings.Join(found, " AND ") + ")::text"
	return prepared(di.gateSQL, params...)
}

// gateKey: the gate's check of di in org, as a key (a transaction's checks passed: directTx.checked).
func (s *session) gateKey(di *directInfo, org string) string {
	g := s.gateCheck(di, org)
	key := g.sql
	for _, p := range g.params {
		key += "\x00" + string(p)
	}
	return key
}

// gateRel: the cloud's table of key t in the gate's SQL. Without schemas by the name the
// statement uses (the session's search_path resolves both alike: review of block 9, C); with
// them by the key qualify resolved the name to, whatever the session's path is.
func (s *session) gateRel(t string) string {
	if s.meta.SchemasUsed {
		return "to_regclass(" + sqlString(qfull(t)) + ")"
	}
	return "to_regclass(" + sqlString(qrel(t)) + ")"
}

// gate takes the pause lock of org on the cloud connection and runs gateCheck after it: true
// when the organization is still in fdw+direct and the statement may run there, the lock
// then held until ungate. refused: the gate's error is the client's statement's (cancelled
// while it waited for a move, lock_timeout, a deadlock): it goes neither way, the client gets
// it (review of block 9, 4). Any other failure: the cloud's way.
func (s *session) gate(org string, di *directInfo) (ok bool, refused *pgproto3.ErrorResponse) {
	return s.gateAll(org, []*directInfo{di})
}

// gateAll is gate for the statements of a pipeline (item 6): the pause and each one's check
// in one round trip; every one of them must be allowed.
func (s *session) gateAll(org string, dis []*directInfo) (ok bool, refused *pgproto3.ErrorResponse) {
	qs := []cloudStmt{s.lockStmt("SELECT FROM pg_advisory_lock_shared", org)}
	for _, di := range dis {
		qs = append(qs, s.gateCheck(di, org))
	}
	c := s.cloudRunStmts(qs...) // prepared once on the connection (direct_prep.go)
	locked := len(c.rows) > 0   // the lock's statement ran
	allowed := c.err == nil && len(c.rows) == 1+len(dis)
	for i := 0; allowed && i < len(dis); i++ {
		allowed = len(c.rows[1+i]) == 3 && s.gateAllows(c.rows[1+i], org, dis[i])
	}
	if allowed {
		return true, nil
	}
	if locked {
		s.ungate(org)
	}
	if r := c.resp; r != nil && (strings.HasPrefix(r.Code, "57") || r.Code == "55P03" || r.Code == "40P01") {
		return false, r
	}
	if c.err != nil {
		s.log.Warn("direct path gate failed: the statement goes through the cloud", "org", org, "err", c.err)
	}
	return false, nil
}

// gateAllows reads the gate check's row (profile, allowed, found): the statement may go the
// direct path. A table the gate did not find by its name is logged once a session.
func (s *session) gateAllows(r []string, org string, di *directInfo) bool {
	if directProfile(r[0]) && r[1] == "true" && r[2] == "true" {
		return true
	}
	if directProfile(r[0]) && r[2] != "true" && !s.direct.unresolved[org] {
		if s.direct.unresolved == nil {
			s.direct.unresolved = map[string]bool{}
		}
		s.direct.unresolved[org] = true // once a session
		s.log.Warn("direct path off for a statement: a table of it or the organization's partition is not found by its name under the session's search_path (the tables in another schema?); it goes through the cloud",
			"org", org, "tables", strings.Join(di.tables, ","))
	}
	return false
}

// refuseDirect answers the client's statement with the gate's error.
func (s *session) refuseDirect(r *pgproto3.ErrorResponse) {
	s.toClient(r)
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

// ungate releases the pause lock gate took; its answers are dropped.
//
// It is not waited for, and its answers are the proxy's own (a capture): the client, answered
// already, never gets its error as that of its next statement (review PR-3). One that failed
// is sent again before the session's next message (ungateAgain): the shared lock must not
// stay with the session, a move of the organization waits for it.
func (s *session) ungate(org string) {
	s.ungating, s.ungatingOrg = s.cloudStartStmts(s.lockStmt("SELECT pg_advisory_unlock_shared", org)), org
}

// ungateAgain sends the unlock of the last ungate again when it failed (one that has not
// answered yet is left to it).
func (s *session) ungateAgain() {
	c := s.ungating
	if c == nil {
		return
	}
	select {
	case <-c.done:
	default:
		return
	}
	s.ungating = nil
	if c.err != nil && !s.q.isDead() {
		s.log.Warn("the pause lock of the direct path was not released: again", "org", s.ungatingOrg, "err", c.err)
		s.ungate(s.ungatingOrg)
	}
}

// runDirect sends msgs (ending with Sync, or a simple Query) on dc and relays the answers to
// the client up to its ReadyForQuery. parse: msgs start with a Parse of the proxy's own,
// whose ParseComplete the client does not get; parsed reports that it succeeded. A broken
// connection is an error to the client (the statement may or may not have run, as through the
// FDW), and the connection is dropped. retry: the connection broke before a single answer
// and canRetry (a read): nothing went to the client, the caller runs it once more on a new
// connection (relay, agent or NAT drop idle connections; review of block 9).
//
// The answer goes to the client message by message (toClient), as everywhere in the session:
// the protocol lets the relay goroutine's asynchronous messages of the cloud (a notice, a
// notification) come between any two of them, so a long statement on the direct path holds
// none of them back (review of block 9, A). Its ReadyForQuery carries the cloud's status:
// the direct path leaves s.tx alone ('I').
func (s *session) runDirect(dc *directConn, msgs []pgproto3.FrontendMessage, canRetry bool) (parsed, retry bool) {
	k, retry := s.runDirectN(dc, msgs, canRetry)
	return k > 0, retry
}

// runDirectN is runDirect for a pipeline (item 6). The Parses among msgs under a name are the
// proxy's own (lokadb_d<n>): their ParseCompletes are not the client's; the unnamed ones are the
// client's (unnamedStmt), theirs go to it. parsed: how many of the proxy's the server took, in
// order (an error skips the rest up to the Sync).
func (s *session) runDirectN(dc *directConn, msgs []pgproto3.FrontendMessage, canRetry bool) (parsed int, retry bool) {
	var ours []bool // each Parse of msgs in order: the proxy's own
	for _, m := range msgs {
		if p, ok := m.(*pgproto3.Parse); ok {
			ours = append(ours, p.Name != "")
		}
	}
	begin := s.beginFor(dc) // a direct transaction's BEGIN with its first statement (item 9)
	if begin != nil {
		msgs = withBegin(begin.String, msgs) // its error skips the statement
	}
	for _, m := range msgs {
		dc.fe.Send(m)
	}
	s.directBusy.Store(dc)
	defer s.directBusy.Store(nil)
	got := false
	fail := func(err error) (int, bool) {
		s.dropDirect(dc)
		if d := s.direct.tx; d != nil && d.dc == dc { // the organization's side of the transaction is gone
			d.aborted, d.dc = true, nil
		}
		if !got && canRetry {
			s.log.Info("organization's database connection found broken: the read runs on a new one", "org", dc.org, "err", err)
			return 0, true
		}
		s.log.Warn("organization's database connection lost", "org", dc.org, "err", err)
		s.toClient(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006",
			Message: "lokadb: the connection to organization " + dc.org + "'s database was lost: " + err.Error()})
		s.toClient(s.clientRFQ(s.txStatus()))
		s.flushClient()
		return 0, false
	}
	if err := dc.fe.Flush(); err != nil {
		return fail(err)
	}
	inBegin := begin != nil // the BEGIN's answer first: the proxy's own
	var beginErr *pgproto3.ErrorResponse
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return fail(err)
		}
		got = true
		if inBegin { // up to the BEGIN's CommandComplete (withBegin)
			switch m := m.(type) {
			case *pgproto3.ErrorResponse:
				r := *m
				beginErr, inBegin = &r, false
			default:
				inBegin = !beginDone(m)
			}
			if beginErr == nil {
				continue
			}
		}
		if beginErr != nil { // the server skipped the statement: the client gets BEGIN's error
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				if d := s.direct.tx; d != nil {
					d.aborted = true
				}
				s.toClient(beginErr)
				s.toClient(s.clientRFQ(s.txStatus()))
				s.flushClient()
				return 0, false
			}
			continue
		}
		switch m.(type) {
		case *pgproto3.ParseComplete:
			own := len(ours) > 0 && ours[0]
			if len(ours) > 0 {
				ours = ours[1:]
			}
			if own {
				parsed++
				continue
			}
		case *pgproto3.ErrorResponse:
			ours = nil
			dc.markEvict(m.(*pgproto3.ErrorResponse).Code) // the pool (direct_pool.go)
		case *pgproto3.ParameterStatus, *pgproto3.NotificationResponse:
			continue // the organization's server, not the client's session
		case *pgproto3.ReadyForQuery:
			dc.used = time.Now()
			if d := s.direct.tx; d != nil && m.(*pgproto3.ReadyForQuery).TxStatus == 'E' {
				d.aborted = true // the statement failed in the organization's transaction
			}
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return parsed, false
		}
		s.toClient(m)
		s.cliMu.Lock()
		big := s.pending > flushBytes
		s.cliMu.Unlock()
		if big {
			s.flushClient()
		}
	}
}

// directQuery runs a simple query on the direct path; false: it goes the cloud's way.
func (s *session) directQuery(sql string, an *Analysis) bool {
	org, di := s.directOrg(sql, an, constOnly, nil)
	if org == "" {
		return false
	}
	dc, err := s.directConnFor(org)
	if err != nil {
		s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err)
		return false
	}
	if di.write != nil {
		return s.writeDirect(dc, org, di, constOnly, sql, nil, nil, an)
	}
	if d := s.direct.tx; d != nil { // a deferred or direct transaction (8a.2): no retry, the transaction is the connection's
		if _, ok, sent := s.txGate(org, di, dc); !ok {
			return sent
		}
		d.setFirst(sql)
		s.traceDirect(sql, an, org)
		s.runDirect(d.dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: sql}}, false)
		return true
	}
	ok, refused := s.gate(org, di)
	if refused != nil {
		s.refuseDirect(refused)
		return true
	}
	if !ok {
		return false
	}
	s.traceDirect(sql, an, org)
	for try := 0; ; try++ {
		if _, retry := s.runDirect(dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: sql}}, try == 0 && di.update == ""); !retry {
			break
		}
		if dc, err = s.directConnFor(org); err != nil {
			s.directLost(org, err)
			break
		}
	}
	s.ungate(org)
	return true
}

// directLost: a retried read found no new connection: the client's error, as a lost one.
func (s *session) directLost(org string, err error) {
	s.toClient(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006",
		Message: "lokadb: the connection to organization " + org + "'s database was lost: " + err.Error()})
	s.toClient(s.clientRFQ(s.txStatus()))
	s.flushClient()
}

// directBatch runs a batch on the direct path: one Bind of a named statement to the unnamed
// portal, its Execute, maybe a Describe and a Close of that portal, then Sync (pgx, PDO,
// JDBC once a statement is server-prepared). false: it goes the cloud's way, untouched.
func (s *session) directBatch(batch []pgproto3.FrontendMessage) bool {
	if s.direct.off != "" || s.meta == nil || len(s.meta.Profiles) == 0 || s.tenantImpl && s.direct.tx == nil {
		return false
	}
	var b *pgproto3.Bind
	var parse *pgproto3.Parse // the client's unnamed statement, Parsed in the batch
	execs, binds := 0, 0
	for _, msg := range batch {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			if m.Name != "" {
				return false // a named one is Parsed in the cloud: its later Binds may go there
			}
			parse = m
		case *pgproto3.Bind:
			if m.DestinationPortal != "" || m.PreparedStatement == "" && parse == nil {
				return false
			}
			b = m
			binds++
		case *pgproto3.Describe:
			if m.ObjectType != 'P' || m.Name != "" || b == nil {
				return false
			}
		case *pgproto3.Execute:
			if m.Portal != "" || m.MaxRows != 0 || b == nil {
				return false
			}
			execs++
		case *pgproto3.Close:
			if m.ObjectType != 'P' || m.Name != "" {
				return false
			}
		case *pgproto3.Sync:
		default:
			return false
		}
	}
	if execs > 1 {
		return s.directPipeline(batch)
	}
	if b == nil || execs != 1 || binds != 1 {
		return false
	}
	if ok, dead := s.q.waitEmpty(); !ok || dead {
		return false
	}
	if parse != nil {
		if b.PreparedStatement != "" || batch[0] != parse {
			return false
		}
		st, failed := s.unnamedStmt(parse)
		if failed {
			return true
		}
		if st == nil {
			return false
		}
		s.stmts[""] = st // what the client's Parse makes it (the cloud's way Parses it again)
		if !s.directOne(b, batch) {
			return false
		}
		s.direct.lostUnnamed = parse
		s.forgetUnnamed() // the cloud has not got it
		return true
	}
	return s.directOne(b, batch)
}

// directOne: directBatch's one statement, bound by b (the client's unnamed statement Parsed in
// the batch too: st.unnamed).
func (s *session) directOne(b *pgproto3.Bind, batch []pgproto3.FrontendMessage) bool {
	st := s.stmts[b.PreparedStatement]
	if st == nil || st.tset != nil || st.terr != nil || st.holds || st.unlistens || st.forgetsAll || len(st.forgets) > 0 {
		return false
	}
	an := s.current(st)
	if s.txStatus() != 'I' && s.direct.tx == nil {
		return false
	}
	if st.ptypes == nil {
		if st.ptypes = s.stmtParamTypes(st, b.PreparedStatement); st.ptypes == nil { // the process's cache first (perf3-14)
			return false
		}
	}
	p := &portalInfo{stmt: b.PreparedStatement, st: st, an: an, params: b.Parameters, formats: b.ParameterFormatCodes}
	org, di := s.directOrg(st.sql, an, p.values, st.ptypes)
	if org == "" {
		return false
	}
	// the result's column types as the cloud describes the statement (the client may have
	// cached them): a type of the application's has another OID there
	if st.rtypes == nil || st.rmeta != s.meta { // a schema change since may have changed them
		if st.rtypes, st.rmeta = s.stmtResultTypes(st, b.PreparedStatement), s.meta; st.rtypes == nil {
			return false
		}
	}
	for _, t := range st.rtypes {
		if t >= 16384 {
			return false
		}
	}
	dc, err := s.directConnFor(org)
	if err != nil {
		s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err)
		return false
	}
	if di.write != nil {
		wb := batch
		if st.unnamed { // its text is rewritten: Parsed by the proxy, the client's ParseComplete owed
			wb = withoutParse(batch)
			s.direct.owedParse.Store(true)
		}
		if !s.writeDirect(dc, org, di, p.values, "", wb, st.ptypes, an) {
			s.direct.owedParse.Store(false)
			return false
		}
		delete(s.portals, "")
		return true
	}
	if d := s.direct.tx; d != nil { // a deferred or direct transaction (8a.2)
		if _, ok, sent := s.txGate(org, di, dc); !ok {
			return sent
		}
		d.setFirst(st.sql)
		s.traceDirect(st.sql, an, org)
		msgs, name, parse := s.directMsgs(d.dc, st, batch)
		if parsed, _ := s.runDirect(d.dc, msgs, false); parsed && parse {
			d.dc.names[st] = name
		}
		delete(s.portals, "")
		return true
	}
	ok, refused := s.gate(org, di)
	if refused != nil {
		delete(s.portals, "")
		s.refuseDirect(refused)
		return true
	}
	if !ok {
		return false
	}
	s.traceDirect(st.sql, an, org)
	for try := 0; ; try++ {
		msgs, name, parse := s.directMsgs(dc, st, batch)
		parsed, retry := s.runDirect(dc, msgs, try == 0 && di.update == "")
		if parsed && parse {
			dc.names[st] = name
		}
		if !retry {
			break
		}
		if dc, err = s.directConnFor(org); err != nil {
			s.directLost(org, err)
			break
		}
	}
	delete(s.portals, "")
	s.ungate(org)
	return true
}

// directMsgs: the batch for dc: the client's statement under a name of the proxy's own,
// Parsed before its first Bind there (parse), then the client's messages bound to it.
func (s *session) directMsgs(dc *directConn, st *stmtInfo, batch []pgproto3.FrontendMessage) (msgs []pgproto3.FrontendMessage, name string, parse bool) {
	if st.unnamed { // the client's Parse and Bind of its unnamed statement, as it sent them
		return batch, "", false
	}
	name = dc.names[st]
	parse = name == ""
	if parse {
		if len(dc.names) >= 1000 { // statements the client dropped long ago stay prepared there
			s.runDirectQuiet(dc, &pgproto3.Query{String: "DEALLOCATE ALL"})
			dc.names = map[*stmtInfo]string{}
		}
		dc.n++
		name = fmt.Sprintf("lokadb_d%d", dc.n)
		msgs = append(msgs, &pgproto3.Parse{Name: name, Query: st.sql, ParameterOIDs: st.ptypes})
	}
	for _, msg := range batch {
		if m, ok := msg.(*pgproto3.Bind); ok {
			c := *m
			c.PreparedStatement = name
			msg = &c
		}
		msgs = append(msgs, msg)
	}
	return msgs, name, parse
}

// runDirectQuiet runs a query of the proxy's own on dc, its answers dropped.
func (s *session) runDirectQuiet(dc *directConn, m pgproto3.FrontendMessage) {
	dc.fe.Send(m)
	if dc.fe.Flush() != nil {
		return
	}
	for {
		r, err := dc.fe.Receive()
		if err != nil {
			return
		}
		if _, ok := r.(*pgproto3.ReadyForQuery); ok {
			return
		}
	}
}

func (s *session) traceDirect(sql string, an *Analysis, org string) {
	s.countStatement(org, true)
	level, ok := s.srv.routeLevel()
	if !ok || !s.log.Enabled(context.Background(), level) {
		return
	}
	if an.logged == "" {
		an.logged = brief(sql)
	}
	s.log.Log(context.Background(), level, "route", "to", "tenant "+org, "path", "direct", "sql", an.logged)
}

// directOptions: the startup options name only directSafe settings (-c name=value,
// --name=value).
func directOptions(v string) bool {
	if strings.Contains(v, `\`) {
		return false
	}
	f := strings.Fields(v)
	for i := 0; i < len(f); i++ {
		kv := ""
		switch t := f[i]; {
		case t == "-c" && i+1 < len(f):
			i++
			kv = f[i]
		case strings.HasPrefix(t, "-c"):
			kv = t[2:]
		case strings.HasPrefix(t, "--"):
			kv = t[2:]
		default:
			return false
		}
		name, _, ok := strings.Cut(kv, "=")
		if !ok || !directSafe[strings.ToLower(strings.ReplaceAll(name, "-", "_"))] {
			return false
		}
	}
	return true
}

// addOp records an operator the statement names; one qualified with a schema other than
// pg_catalog (OPERATOR(public.%)) is not taken at all.
func (di *directInfo) addOp(name []*pg.Node) bool {
	if len(name) == 0 {
		return true
	}
	if len(name) > 1 && name[0].GetString_().GetSval() != "pg_catalog" {
		return false
	}
	op := name[len(name)-1].GetString_().GetSval()
	if op == "" || strings.ToUpper(op) == op && strings.ToLower(op) != op { // BETWEEN, SIMILAR…: keywords, not operators
		return true
	}
	for _, o := range di.ops {
		if o == op {
			return true
		}
	}
	di.ops = append(di.ops, op)
	return true
}

// pipeUnit: one statement of a pipeline: the client's Bind and what follows it up to its Execute
// (and Close of the portal).
type pipeUnit struct {
	parse *pgproto3.Parse // the client's unnamed statement it binds, Parsed earlier in the batch
	p     *portalInfo     // its Bind's values
	st    *stmtInfo
	an    *Analysis
	di    *directInfo
	msgs  []pgproto3.FrontendMessage
}

// directPipeline runs a pipeline of Bind/Execute of the client's prepared statements (item 6:
// pgx.Batch, JDBC executeBatch with autocommit) on the direct path when every one of them is
// direct for one organization: one gate in the cloud (the pause and each statement's check in
// one round trip) and one exchange with the organization's database, which runs them as
// Postgres runs a pipeline (one implicit transaction up to the Sync; an error skips the rest).
// A pipeline of reads alone is run again on a new connection when the old one is found broken
// before any answer; one with an UPDATE is not (it may have run). Reads and UPDATEs, outside a
// transaction or in a deferred or direct one (pgx's tx.SendBatch:
// the transaction's gate then, txGateAll); a write of 8b goes the cloud's way as before, and
// BEGIN/COMMIT in the pipeline too. false: nothing ran.
func (s *session) directPipeline(batch []pgproto3.FrontendMessage) bool {
	var units []*pipeUnit
	var cur *pipeUnit
	var unnamed *pgproto3.Parse // the client's last Parse of its unnamed statement in the batch
	bindNext := false           // it starts the unit to come (its Bind next)
	execd := false
	for i, msg := range batch {
		switch m := msg.(type) {
		case *pgproto3.Parse:
			if m.Name != "" || cur != nil && !execd || bindNext {
				return false
			}
			unnamed, bindNext = m, true
			cur, execd = &pipeUnit{parse: m}, false
			units = append(units, cur)
		case *pgproto3.Bind:
			if m.DestinationPortal != "" || m.PreparedStatement == "" && unnamed == nil {
				return false
			}
			if bindNext { // the Bind of the unit its Parse started
				if m.PreparedStatement != "" {
					return false
				}
				bindNext = false
				break
			}
			if cur != nil && !execd {
				return false
			}
			cur, execd = &pipeUnit{}, false
			if m.PreparedStatement == "" {
				cur.parse = unnamed
			}
			units = append(units, cur)
		case *pgproto3.Describe:
			if m.ObjectType != 'P' || m.Name != "" || cur == nil || execd || bindNext {
				return false
			}
		case *pgproto3.Execute:
			if m.Portal != "" || m.MaxRows != 0 || cur == nil || execd || bindNext {
				return false
			}
			execd = true
		case *pgproto3.Close:
			if m.ObjectType != 'P' || m.Name != "" || cur == nil || !execd {
				return false
			}
		case *pgproto3.Sync:
			if i != len(batch)-1 {
				return false
			}
			continue
		default:
			return false
		}
		cur.msgs = append(cur.msgs, msg)
	}
	if len(units) < 2 || !execd || bindNext {
		return false
	}
	if ok, dead := s.q.waitEmpty(); !ok || dead {
		return false
	}
	if s.txStatus() != 'I' && s.direct.tx == nil {
		return false
	}
	org := ""
	anyUnnamed := false
	var lastUnnamed *pgproto3.Parse // the client's last unnamed Parse of the batch (lostUnnamed)
	writes := false                 // a write of 8b among them (pipeWrite)
	for _, u := range units {
		var b *pgproto3.Bind
		for _, m := range u.msgs {
			if x, ok := m.(*pgproto3.Bind); ok {
				b = x
			}
		}
		st := s.stmts[b.PreparedStatement]
		if u.parse != nil {
			var failed bool
			if st, failed = s.unnamedStmt(u.parse); failed {
				return true
			}
			anyUnnamed = true
			lastUnnamed = u.parse
		}
		if st == nil || st.tset != nil || st.terr != nil || st.holds || st.unlistens || st.forgetsAll || len(st.forgets) > 0 {
			return false
		}
		if st.ptypes == nil {
			if st.ptypes = s.stmtParamTypes(st, b.PreparedStatement); st.ptypes == nil { // the process's cache first (perf3-14)
				return false
			}
		}
		an := s.current(st)
		p := &portalInfo{stmt: b.PreparedStatement, st: st, an: an, params: b.Parameters, formats: b.ParameterFormatCodes}
		o, di := s.directOrg(st.sql, an, p.values, st.ptypes)
		if o == "" || org != "" && o != org {
			return false // another organization's: the whole pipeline the cloud's way
		}
		org = o
		writes = writes || di.write != nil
		if st.rtypes == nil || st.rmeta != s.meta {
			if st.rtypes, st.rmeta = s.stmtResultTypes(st, b.PreparedStatement), s.meta; st.rtypes == nil {
				return false
			}
		}
		for _, t := range st.rtypes {
			if t >= 16384 {
				return false
			}
		}
		u.st, u.an, u.di, u.p = st, an, di, p
	}
	dc, err := s.directConnFor(org)
	if err != nil {
		s.log.Warn("organization's database not reachable directly: through the cloud", "org", org, "err", err)
		return false
	}
	var dis []*directInfo
	seen := map[string]bool{} // by the check's text: 1000 Binds of one statement, one check (block 9's review)
	reads := true
	for _, u := range units {
		if q := s.gateKey(u.di, org); !seen[q] {
			seen[q] = true
			dis = append(dis, u.di)
		}
		reads = reads && u.di.update == ""
	}
	if writes {
		if !s.pipeWrite(org, units, dis, dc) {
			return false
		}
		delete(s.portals, "")
		if anyUnnamed {
			s.direct.lostUnnamed = lastUnnamed
			s.forgetUnnamed() // the cloud has not got it
		}
		return true
	}
	if d := s.direct.tx; d != nil { // a deferred or direct transaction (8a.2): its gate, its connection
		if _, ok, sent := s.txGateAll(org, dis, dc); !ok {
			return sent
		}
		d.setFirst(units[0].st.sql)
		for _, u := range units {
			s.traceDirect(u.st.sql, u.an, org)
		}
		msgs, parsing, names := s.pipeMsgs(d.dc, units)
		parsed, _ := s.runDirectN(d.dc, msgs, false)
		for _, st := range parsing[:parsed] {
			d.dc.names[st] = names[st]
		}
		delete(s.portals, "")
		if anyUnnamed {
			s.direct.lostUnnamed = lastUnnamed
			s.forgetUnnamed() // the cloud has not got it
		}
		return true
	}
	ok, refused := s.gateAll(org, dis)
	if refused != nil {
		delete(s.portals, "")
		s.refuseDirect(refused)
		return true
	}
	if !ok {
		return false
	}
	for _, u := range units {
		s.traceDirect(u.st.sql, u.an, org)
	}
	for try := 0; ; try++ {
		msgs, parsing, names := s.pipeMsgs(dc, units)
		parsed, retry := s.runDirectN(dc, msgs, try == 0 && reads)
		for _, st := range parsing[:parsed] {
			dc.names[st] = names[st]
		}
		if !retry {
			break
		}
		if dc, err = s.directConnFor(org); err != nil {
			s.directLost(org, err)
			break
		}
	}
	delete(s.portals, "")
	if anyUnnamed {
		s.direct.lostUnnamed = lastUnnamed
		s.forgetUnnamed() // the cloud has not got it
	}
	s.ungate(org)
	return true
}

// pipeMsgs: the pipeline for dc: each statement under a name of the proxy's own, Parsed before
// its first Bind there (parsing, in order; names: the names given), the client's messages bound
// to them, one Sync.
func (s *session) pipeMsgs(dc *directConn, units []*pipeUnit) (msgs []pgproto3.FrontendMessage, parsing []*stmtInfo, names map[*stmtInfo]string) {
	if len(dc.names)+len(units) > 1000 { // statements the client dropped long ago stay prepared there
		s.runDirectQuiet(dc, &pgproto3.Query{String: "DEALLOCATE ALL"})
		dc.names = map[*stmtInfo]string{}
	}
	names = map[*stmtInfo]string{}
	for _, u := range units {
		if u.st.unnamed { // the client's Parse and Bind of its unnamed statement, as it sent them
			msgs = append(msgs, u.msgs...)
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
			parsing = append(parsing, u.st)
			msgs = append(msgs, &pgproto3.Parse{Name: name, Query: u.st.sql, ParameterOIDs: u.st.ptypes})
		}
		for _, m := range u.msgs {
			if b, ok := m.(*pgproto3.Bind); ok {
				c := *b
				c.PreparedStatement = name
				m = &c
			}
			msgs = append(msgs, m)
		}
	}
	return append(msgs, &pgproto3.Sync{}), parsing, names
}

// unnamedStmt: the client's unnamed statement Parsed in a batch for the direct path (JDBC before
// prepareThreshold, pgx's exec mode, any driver that Parses every statement unnamed): one
// stmtInfo per text and parameter types for the session, with the parameter and result types
// the cloud gives it (one Describe there per text, again after a change of the metadata). The
// cloud's unnamed statement becomes this text, which the client's Parse would make it anyway.
// nil: the cloud's way. failed: the cloud refused the text inside a transaction it holds open
// (a direct one): the client has the error, the transaction is aborted on both sides as it
// would be on the cloud's way.
func (s *session) unnamedStmt(p *pgproto3.Parse) (st *stmtInfo, failed bool) {
	key := p.Query + "\x00" + fmt.Sprint(p.ParameterOIDs)
	if st := s.direct.unnamed[key]; st != nil && st.rmeta == s.meta {
		return st, false
	}
	// another session described the text already (the process's cache, perf3-14): no exchange
	if ptypes, rtypes := s.cachedTypes(p.Query, p.ParameterOIDs); ptypes != nil && rtypes != nil {
		return s.newUnnamed(p, key, ptypes, rtypes), false
	}
	c := &capture{done: make(chan struct{}), untilSync: true}
	if err := s.send(s.extMsgs(c, &pgproto3.Parse{Query: p.Query, ParameterOIDs: p.ParameterOIDs},
		&pgproto3.Describe{ObjectType: 'S'})); err != nil {
		return nil, false
	}
	<-c.done
	if c.err != nil || c.oids == nil || c.cols == nil {
		if d := s.direct.tx; d != nil && d.org != "" && c.resp != nil { // the cloud's transaction is aborted now
			d.aborted, d.errSent = true, true
			s.toClient(c.resp)
			s.toClient(s.clientRFQ(s.txStatus()))
			s.flushClient()
			return nil, true
		}
		return nil, false
	}
	s.storeTypes(p.Query, p.ParameterOIDs, c.oids, c.cols)
	return s.newUnnamed(p, key, c.oids, c.cols), false
}

// newUnnamed: the session's record of the client's unnamed statement p, with its types.
func (s *session) newUnnamed(p *pgproto3.Parse, key string, ptypes, rtypes []uint32) *stmtInfo {
	st := &stmtInfo{sql: p.Query, oids: p.ParameterOIDs, sent: p.Query, unnamed: true,
		ptypes: ptypes, rtypes: rtypes, rmeta: s.meta}
	st.forgets, st.forgetsAll = deallocates(p.Query)
	st.unlistens = dropsListen(p.Query)
	st.holds = declaresHold(p.Query)
	st.tset, st.terr = stmtTenant(p.Query, s.tenantCfg())
	if s.direct.unnamed == nil || len(s.direct.unnamed) >= 1000 {
		s.direct.unnamed = map[string]*stmtInfo{}
	}
	s.direct.unnamed[key] = st
	return st
}

// withoutParse: batch without the client's Parse (a write's text is Parsed by the proxy).
func withoutParse(batch []pgproto3.FrontendMessage) []pgproto3.FrontendMessage {
	out := make([]pgproto3.FrontendMessage, 0, len(batch))
	for _, m := range batch {
		if _, ok := m.(*pgproto3.Parse); !ok {
			out = append(out, m)
		}
	}
	return out
}

// settingStmt: a statement of the string starts with SET, RESET or DISCARD (comments before it
// too): directSetting parses only those, and those with set_config.
var settingStmt = stmtStart{stmtStartRe("set|reset|discard"), []string{"set", "reset", "discard"}} // by its first word as a rule (createStart)
