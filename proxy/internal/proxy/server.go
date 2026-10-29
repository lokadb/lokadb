package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// MetaCacheConfig: the section meta_cache of the proxy's config.
type MetaCacheConfig struct {
	Off bool `json:"off,omitempty"` // every session reads the metadata itself
	// Verify (a duration, "0": always ask): a new connection that comes within this time of one
	// that verified its role's entry takes the entry without asking the cloud for the stamp
	// (20-200 ms of the cloud's work at a thousand organizations, 1-3 ms at few: the bulk of a
	// connect through a connection cache). enable, a move, disable, a GRANT then reach NEW
	// connections up to this late; live sessions hear them by NOTIFY as before.
	Verify string `json:"verify,omitempty"`
}

// Config of the proxy.
type Config struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"` // libpq DSN of the cloud Postgres without user/dbname/password
	// Customers[database][org] = DSN of that organization's own database; used to replay DDL.
	// org: its number (one numbered by uuid: the one org-add gave it, block 18).
	Customers map[string]map[string]string `json:"customers"`
	// CustomersApp[database][org] = DSN of the role the direct path (profile fdw+direct) runs
	// the application's statements under in that organization's database: DML on its tables
	// alone, no owner, no CREATE, no superuser (the proxy checks it on connecting). Without an
	// entry the organization's statements go through the cloud.
	CustomersApp map[string]map[string]string `json:"customers_app,omitempty"`
	// ControlPlane: DSN of the control plane database whose lokadb_cp.org_credentials hold the
	// organizations' credentials (credentials.go, stage 9a): they win over Customers and
	// CustomersApp. CredentialsKey: the file of the key they are sealed with (32 bytes, 0600);
	// CredentialsKeyOld: the one before it, while the rows are resealed (key rotation).
	ControlPlane      string `json:"control_plane,omitempty"`
	CredentialsKey    string `json:"credentials_key,omitempty"`
	CredentialsKeyOld string `json:"credentials_key_old,omitempty"`
	// Record: file the proxy keeps the observed workload in, for `lokadb check` (off when empty).
	Record string `json:"record,omitempty"`
	// MaxConnections: clients served at once (0 = 1000); more get "too many connections".
	MaxConnections int `json:"max_connections,omitempty"`
	// CacheBytes: what the caches of one session (analyses of the texts it met, the direct path's
	// shapes of them) may hold, estimated in bytes (default 8 MiB; block 9, review PF7)
	CacheBytes int `json:"cache_bytes,omitempty"`
	// CacheBytesTotal: what the caches of every session together may hold (default 512 MiB;
	// BACKLOG block 9, item 13): a session whose caches would go past it empties its own.
	CacheBytesTotal int `json:"cache_bytes_total,omitempty"`
	// OrgCacheBytes: what the organizations of ids, shared by the sessions, may hold (default 64
	// MiB; orgcache.go).
	OrgCacheBytes int `json:"org_cache_bytes,omitempty"`
	// KeyedStatements: the statements with the partition key's condition one cloud connection
	// keeps prepared, a statement of the client and an organization each (default 1024;
	// keycond.go). Each holds about 25 KB of the cloud backend's memory.
	KeyedStatements int `json:"keyed_statements,omitempty"`
	// RouteLog: the log line "route" of every statement on LokaDB tables at level Info. Off by
	// default (the user's decision of 26.10: block 14 measured it at 20-25 % of the proxy's
	// throughput with 32 clients, one line a statement through the log's one mutex), the line
	// is then at level Debug (LOKADB_DEBUG). e2e, the defects checks and the playground read it
	// and set it. NoRouteLog (the setting of before) turns it off at every level.
	RouteLog   bool `json:"route_log,omitempty"`
	NoRouteLog bool `json:"no_route_log,omitempty"`
	// MetricsListen: where /metrics (Prometheus text: statements, errors and response times per
	// organization, metrics.go) and /healthz are served; "" serves none. Bind it to an address only
	// the monitoring reaches: the counters carry organization numbers.
	MetricsListen string `json:"metrics_listen,omitempty"`
	// MaxMessageBytes: the largest protocol message a client may send (0 = 256 MiB).
	MaxMessageBytes int `json:"max_message_bytes,omitempty"`
	// Fanout[database]: what to do with statements that name no organization ("*": the other
	// databases; without an entry: reject). See fanout.go.
	Fanout map[string]FanoutPolicy `json:"fanout,omitempty"`
	// UpstreamPooling: "session" when a pooler in session mode (pgbouncer pool_mode = session)
	// stands between the proxy and Postgres. Without it the proxy refuses clients when it sees
	// a pooler (see checkPooler): in transaction mode the proxy's set_config and the client's
	// statement may run on different server connections, under another client's organization.
	UpstreamPooling string `json:"upstream_pooling,omitempty"`
	// UpstreamPool: the cache of cloud connections (pool.go): a client's connection is reset
	// and kept for the next client with the same key. nil (the default) or an object: on for every
	// database (the user's decision of 27.10, block 14's measurement: 16 clients, a connection per
	// read, 874 ops/s through the proxy without it, 2795 with it, 1321 straight to Postgres; before
	// it a client connecting per request paid the cloud's backend, its catalog reads and its
	// postgres_fdw connection to the organization again each time); false: off, every client a
	// connection of its own.
	UpstreamPool *PoolConfig `json:"upstream_pool,omitempty"`
	// DirectPool: the pool of direct connections to the organizations' databases (profile
	// fdw+direct, direct_pool.go): a session's connection is reset and kept for the next
	// session. nil: every session opens its own, closed when it ends.
	DirectPool *PoolConfig `json:"direct_pool,omitempty"`
	// MetaCache: the shared cache of the lokadb metadata by database and role (meta_cache.go):
	// a session gets what its role read before, while the database's stamp is unchanged. On
	// unless {"off": true} (block 9, review PF1: sessions reading their own cost the cloud 0.3-1.8 s
	// of work each at a thousand organizations, and again every metaTTL).
	MetaCache *MetaCacheConfig `json:"meta_cache,omitempty"`
	// Tenant: the cooperative mode, the application naming the organization (lokadb.tenant,
	// tenant_session.go); nil: lokadb.tenant only, a statement naming another organization fails.
	Tenant *TenantConfig `json:"tenant,omitempty"`
	// TLS between the applications and the proxy (tls.go); nil: none, SSLRequest answered 'N'.
	TLS *TLSConfig `json:"tls,omitempty"`
}

type Server struct {
	cfg    Config
	log    *slog.Logger
	rec    *recorder
	fanout map[string]*Fanout // compiled Config.Fanout
	slots  chan struct{}      // one per client being served (MaxConnections)
	spare  chan struct{}      // connections over the limit looked at for a cancel request (cancelOnly)
	schema sync.Map           // database -> *atomic.Int64: schema changes replayed (see schemaVersion)
	tls    *proxyTLS          // nil: no TLS
	// dialDirect opens a direct connection to an organization's database (nil: pgDial; tests)
	dialDirect directDialer
	directDown sync.Map // "database/org" -> time.Time of the last failed direct connection
	metrics    *metrics // counters per organization (metrics.go)
	sessions   sync.Map // cancelKey -> *session (a CancelRequest for a statement on the direct path)
	upOnce     sync.Once
	upCfg      *pgconn.Config // the upstream DSN parsed (upstreamConfig)
	upErr      error
	// cloudFirst: db/fingerprint of the first statements of transactions the direct path could
	// not take whole: those go the cloud's way (direct_tx.go)
	cloudFirst cloudFirst
	// directOK: db/fingerprint of transactions' first statements learnt for the direct path:
	// two cloud transactions in a row that started so committed with every statement one the
	// direct path takes (direct_tx.go)
	directOK   directOK
	pool       *connPool              // the cache of cloud connections (pool.go); nil: off
	orgs       *orgCache              // the organizations of ids, for every session (orgcache.go); nil: each session its own
	cacheBytes atomic.Int64           // what the caches of every session hold (byteCache.total)
	directPool *idlePool[*directConn] // the pool of direct connections (direct_pool.go); nil: off
	metaCache  *metaCache             // the shared cache of the metadata (meta_cache.go); nil: off
	types      typeCache              // the statements' types of the direct path, for every session (direct_types.go)
	creds      credStore              // the organizations' credentials of the control plane (credentials.go)
	seen       roleSeen               // the roles that connected (seen.go)
}

func NewServer(cfg Config, log *slog.Logger) *Server {
	s := &Server{cfg: cfg, log: log, orgs: newOrgCache(cfg.OrgCacheBytes), metrics: newMetrics()}
	if cfg.Record != "" {
		s.rec = newRecorder(cfg.Record, log)
		log.Info("recording the workload", "file", cfg.Record)
	}
	return s
}

func (s *Server) ListenAndServe() error {
	s.fanout = map[string]*Fanout{}
	for db := range s.cfg.Fanout {
		f, err := s.cfg.FanoutFor(db)
		if err != nil {
			return err
		}
		s.fanout[db] = f
		s.log.Info("fanout policy", "db", db, "mode", f.Mode, "allowed_statements", f.Allowed)
	}
	if s.fanout["*"] == nil {
		s.fanout["*"], _ = s.cfg.FanoutFor("*")
	}
	var err error
	if s.tls, err = s.cfg.TLS.load(s.log); err != nil {
		return err
	}
	if err = s.startCredentials(); err != nil {
		return err
	}
	s.serveMetrics()
	switch up := s.cfg.UpstreamPool; {
	case up != nil && up.Off:
		s.log.Info("cloud connections not cached: upstream_pool is false in the config")
	default:
		cfg, why := up, "upstream_pool in the config"
		if up == nil {
			cfg, why = &PoolConfig{}, "the default (\"upstream_pool\": false turns it off)"
		}
		if s.pool, err = newConnPool(cfg, s.log); err != nil {
			return err
		}
		s.log.Info("cloud connections cached", "for", why, "max_idle", s.pool.maxIdle, "max_idle_total", s.pool.maxTotal, "idle_ttl", s.pool.idleTTL, "max_age", s.pool.maxAge)
	}
	if s.cfg.MetaCache == nil || !s.cfg.MetaCache.Off {
		s.metaCache = newMetaCache()
		if v := s.cfg.MetaCache; v != nil && v.Verify != "" {
			d, err := time.ParseDuration(v.Verify)
			if err != nil || d < 0 {
				return fmt.Errorf("meta_cache.verify: bad duration %q", v.Verify)
			}
			s.metaCache.verifyFor = d
		}
		s.log.Info("lokadb metadata cached by database and role")
	}
	if s.cfg.DirectPool != nil {
		if s.directPool, err = newDirectPool(s.cfg.DirectPool); err != nil {
			return err
		}
		s.log.Info("direct connections pooled", "max_idle", s.directPool.maxIdle, "max_idle_total", s.directPool.maxTotal, "idle_ttl", s.directPool.idleTTL, "max_age", s.directPool.maxAge)
	}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	if s.pool != nil || s.directPool != nil || s.metaCache != nil {
		go s.flushOnHangup()
	}
	max := s.cfg.MaxConnections
	if max <= 0 {
		max = 1000
	}
	s.slots = make(chan struct{}, max)
	s.spare = make(chan struct{}, spareSlots)
	s.log.Info("listening", "addr", s.cfg.Listen, "max_connections", max, "tls", s.tls != nil, "tls_required", s.tls != nil && s.tls.require)
	backoff := 5 * time.Millisecond
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// too many open files and the like: wait it out, the proxy must not stop for everybody
			s.log.Warn("accept failed, retrying", "err", err, "in", backoff)
			time.Sleep(backoff)
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 5 * time.Millisecond
		select {
		case s.slots <- struct{}{}:
			go func() {
				defer func() { <-s.slots }()
				s.handle(c)
			}()
		default:
			// a full proxy still takes a cancel request: it is what stops a runaway statement
			// when the proxy is full (block 9, review PR-9; Postgres takes it in its postmaster)
			select {
			case s.spare <- struct{}{}:
				go func() {
					defer func() { <-s.spare }()
					s.cancelOnly(c)
				}()
			default:
				go tooMany(c)
			}
		}
	}
}

// spareSlots: connections over MaxConnections that are looked at for a cancel request, at most.
const spareSlots = 64

// cancelOnly serves a connection over MaxConnections: a cancel request is forwarded (as the
// startup of a session would, TLS first when the client asks for it), anything else is turned
// away as tooMany does. Short: the whole of it is bounded by 5 s.
func (s *Server) cancelOnly(c net.Conn) {
	defer c.Close()
	defer s.survive("cancel request over the limit")
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	be := pgproto3.NewBackend(c, c)
	for {
		m, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch m := m.(type) {
		case *pgproto3.SSLRequest:
			if c = s.tls.upgrade(c); c == nil {
				return
			}
			be = pgproto3.NewBackend(c, c)
		case *pgproto3.GSSEncRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.CancelRequest:
			s.forwardCancel(m)
			return
		default:
			be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "53300", Message: "lokadb: too many connections to the proxy"})
			_ = be.Flush()
			return
		}
	}
}

// tooMany answers a client over MaxConnections as Postgres does, and hangs up.
func tooMany(c net.Conn) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	be := pgproto3.NewBackend(c, c)
	be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "53300", Message: "lokadb: too many connections to the proxy"})
	_ = be.Flush()
}

// phases: where the time of a client's connect goes (block 9, the connect latency: 22-25 ms through
// the proxy against 4.6 ms straight to Postgres in block 14's trial): one Debug line a connection.
type phases struct {
	last time.Time
	f    []any
}

func newPhases() *phases { return &phases{last: time.Now()} }

// mark closes the phase name: the microseconds since the previous mark.
func (p *phases) mark(name string) {
	n := time.Now()
	p.f = append(p.f, name+"_us", n.Sub(p.last).Microseconds())
	p.last = n
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	ph := newPhases()
	// A bug while a client connects (the metadata read, a cancel request) must not take down
	// the proxy and every other session: that client loses its connection, nothing else.
	defer s.survive("connection setup")
	// a client that connects and says nothing must not hold a connection slot forever
	_ = c.SetDeadline(time.Now().Add(30 * time.Second))
	be := pgproto3.NewBackend(c, c)
	maxMsg := s.cfg.MaxMessageBytes
	if maxMsg <= 0 {
		maxMsg = 256 << 20
	}
	be.SetMaxBodyLen(maxMsg)
	var sm *pgproto3.StartupMessage
	for sm == nil {
		m, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch m := m.(type) {
		case *pgproto3.SSLRequest:
			// TLS (tls.go): the rest of the startup, the password included, runs inside it
			if c = s.tls.upgrade(c); c == nil {
				return
			}
			be = pgproto3.NewBackend(c, c)
			be.SetMaxBodyLen(maxMsg)
		case *pgproto3.GSSEncRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.CancelRequest:
			s.forwardCancel(m)
			return
		case *pgproto3.StartupMessage:
			if s.tls.refuseClear(c, be) {
				return
			}
			sm = m
		default:
			return
		}
	}
	ph.mark("startup") // the client's startup message, TLS included
	user, db := sm.Parameters["user"], sm.Parameters["database"]
	if db == "" {
		db = user
	}
	if name := startupLokadb(sm.Parameters, s.cfg.Tenant != nil); name != "" {
		s.log.Error("refused: a lokadb setting in the startup parameters", "db", db, "user", user, "setting", name)
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "42501", Message: "lokadb: the connection sets " + name + " in its startup parameters; lokadb.* settings are the proxy's, an application cannot set them",
			Hint: "remove " + name + " from the connection string (options=-c … or a parameter of its own)"})
		_ = be.Flush()
		return
	}
	if s.cfg.Tenant != nil { // the cooperative mode: the organization the connection names
		v, err := startupTenant(sm.Parameters, s.cfg.Tenant.Setting, "")
		if err == nil && v != "" {
			err = s.cfg.Tenant.sessionRefused(TenantSet{Value: v}, TenantSetting)
		}
		if err != nil {
			s.log.Error("refused: the organization in the startup parameters", "db", db, "user", user, "err", err)
			be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: tenantCode(err), Message: "lokadb: the connection string: " + err.Error()})
			_ = be.Flush()
			return
		}
	}

	// Ask the client for its password and reuse it upstream: the proxy keeps no secrets.
	be.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := be.Flush(); err != nil {
		return
	}
	if err := be.SetAuthType(pgproto3.AuthTypeCleartextPassword); err != nil {
		return
	}
	m, err := be.Receive()
	if err != nil {
		return
	}
	pw, ok := m.(*pgproto3.PasswordMessage)
	if !ok {
		return
	}
	ph.mark("password") // the exchange with the client for its password
	s.seen.note(user, db)

	cfg, err := s.upstreamConfig()
	if err != nil {
		s.log.Error("bad upstream dsn", "err", err)
		return
	}
	cfg.User, cfg.Password, cfg.Database = user, pw.Password, db
	for k, v := range sm.Parameters {
		if k != "user" && k != "database" {
			cfg.RuntimeParams[k] = v
		}
	}
	// The fanout policy, for the triggers that stand in for the FKs from LokaDB tables to cloud
	// tables (admin/fkcloud.go): a change of a cloud row referenced by them runs in every moved
	// organization's database or is refused. The client's own value, as a parameter or as -c in
	// options, is overridden: ours is set both ways, the last -c of options winning.
	mode := s.fanoutFor(db).Mode
	cfg.RuntimeParams["lokadb.fanout"] = mode
	opts := "-c lokadb.fanout=" + mode
	if o := cfg.RuntimeParams["options"]; o != "" {
		opts = o + " " + opts
	}
	cfg.RuntimeParams["options"] = opts
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// a connection of the cache (pool.go), or a new one, checked once
	var pc *pooledConn
	var key string
	listened := false // LISTEN of the metadata channel went with the checks of a new connection
	if s.pool != nil {
		key = s.pool.key(db, user, pw.Password, cfg.RuntimeParams)
		pc = s.pool.getConn(key)
	}
	if pc == nil {
		conn, err := pgconn.ConnectConfig(ctx, cfg)
		if err != nil {
			if s.pool != nil && authError(err) {
				s.pool.evict(key) // the password or the role no longer let in: the cached ones go too
			}
			sendError(be, err)
			return
		}
		s.seen.notePID(conn.PID()) // the backend is the proxy's (the setup wizard tells it from a direct one)
		ph.mark("upstream")        // the connection to Postgres: TCP, authentication, the server's startup
		poolErr, roleErr, listening := s.checkNew(ctx, conn)
		s.learnInstallation(ctx, conn) // once per process (credentials.go)
		listened = listening
		ph.mark("checks") // the pooler, the role and LISTEN, in one exchange
		if err := poolErr; err != nil {
			s.log.Error("refused: a pooler between the proxy and Postgres", "db", db, "err", err)
			be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "08004", Message: err.Error()})
			_ = be.Flush()
			conn.Close(ctx)
			return
		}
		if err := roleErr; err != nil {
			s.log.Error("refused: the role bypasses row level security", "db", db, "user", user, "err", err)
			be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000", Message: err.Error(),
				Hint: "ALTER ROLE " + quoteIdent(user) + " NOSUPERUSER NOBYPASSRLS NOREPLICATION (and REVOKE membership of a role that has them), or connect as a separate application role; lokadb check -app-role shows it in advance (finding app_role_bypasses_rls)"})
			_ = be.Flush()
			conn.Close(ctx)
			return
		}
		ph.mark("role")
		hj, err := conn.Hijack()
		if err != nil {
			sendError(be, err)
			return
		}
		pc = &pooledConn{hj: hj, key: key, born: time.Now()}
	}
	kept := false
	defer func() {
		if !kept {
			closePooled(pc)
		}
	}()
	hj := pc.hj
	q := func(sql string) ([][]string, error) { return frontQuery(hj, sql) }
	// lokadb commands that change the metadata (enable, move-org, disable) notify this channel at
	// their commit: the session reloads it at once instead of after metaTTL (see metaChannel)
	ph.mark("hijack") // a connection of the cache, or the hijack of a new one
	if !listened {    // a connection of the cache (its DISCARD ALL dropped it), or a failed check exchange
		if _, err := q("LISTEN " + metaChannel); err != nil {
			s.log.Warn("cannot listen for metadata changes: they are seen within "+metaTTL.String(), "db", db, "err", err)
		}
	}
	ph.mark("listen")
	meta, metaRole, err := s.loadMetaAtConnect(db, user, sm.Parameters, q)
	if err != nil {
		// enabled (the catalog says so) yet unread: statements on LokaDB tables are refused
		// until a read goes through, not passed unrouted (Meta.Retry)
		meta = unreadMeta(q)
		s.log.Warn("lokadb metadata not loaded", "db", db, "err", err, "refusing", meta != nil)
	} else if meta != nil && meta.Denied != "" {
		s.log.Warn("lokadb metadata not readable under the role: statements on LokaDB tables are refused", "db", db, "user", user)
	}
	ph.mark("meta") // the stamp and, when the entry is old or none, the read
	// the search path the session starts with, when a bare table name depends on it (schemas.go;
	// otherwise read when it first does)
	var pathRow []string
	if meta != nil && meta.SchemasUsed {
		if r, err := q(`SELECT current_setting('search_path'), current_user::text`); err == nil && len(r) == 1 && len(r[0]) == 2 {
			pathRow = r[0]
		}
	}
	ph.mark("path")
	sess := newSession(s, db, c, be, hj, meta, sm.Parameters)
	sess.metaRole = metaRole
	be.Send(&pgproto3.AuthenticationOk{})
	for k, v := range hj.ParameterStatuses {
		be.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	be.Send(&pgproto3.BackendKeyData{ProcessID: sess.cancelPID, SecretKey: sess.cancelSecret})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := be.Flush(); err != nil {
		s.sessions.CompareAndDelete(sess.cancelKey, sess)
		return
	}
	ph.mark("reply")
	if s.log.Enabled(context.Background(), slog.LevelDebug) {
		s.log.Debug("connect", ph.f...)
	}
	_ = c.SetDeadline(time.Time{})
	if pathRow != nil {
		sess.initPath(pathRow[0], pathRow[1])
	}
	if s.pool != nil {
		sess.pc = pc
	}
	sess.run()
	switch {
	case sess.parked:
		s.pool.putConn(pc)
		kept = true
	case sess.evict.Load():
		s.pool.evict(key)
	}
}

// authError: Postgres refused the role at connecting (SQLSTATE class 28).
func authError(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && strings.HasPrefix(pe.Code, "28")
}

// checkPooler refuses an upstream connection that goes through a pooler (pgbouncer, Odyssey,
// RDS Proxy…), unless Config.UpstreamPooling says it pools whole sessions: such a pooler sends
// its own BackendKeyData, so the process id the proxy got differs from pg_backend_pid(). The
// proxy relies on its connection being one server session: lokadb.org set before a statement,
// the session lock of an autocommit statement, LISTEN lokadb_meta, prepared statements.
func (s *Server) checkPooler(ctx context.Context, conn *pgconn.PgConn) error {
	if s.cfg.UpstreamPooling == "session" {
		return nil
	}
	rows, err := execRows(ctx, conn, "SELECT pg_backend_pid()::text")
	if err != nil {
		return nil // cannot tell: an error of its own shows up with the client's first statement
	}
	return poolerRefusal(conn, rows)
}

func poolerRefusal(conn *pgconn.PgConn, rows [][]string) error {
	if len(rows) != 1 {
		return nil // cannot tell
	}
	if rows[0][0] == strconv.FormatUint(uint64(conn.PID()), 10) {
		return nil
	}
	return errors.New(`lokadb: the proxy connects to Postgres through a pooler (its process id is not the server's); only a direct connection or a pooler in session mode is supported: set "upstream_pooling": "session" in the proxy config if it pools whole sessions`)
}

// checkNew runs the checks of a new upstream connection in one exchange (block 9, the connect
// latency: three round trips, each a transaction with its own parse and plan, became one): the
// pooler's, the role's, and LISTEN of the metadata channel, which the session would send next.
// listening: the channel is listened to. An error of the exchange (a statement that failed): the
// checks again one by one, as before, and no LISTEN yet.
func (s *Server) checkNew(ctx context.Context, conn *pgconn.PgConn) (poolErr, roleErr error, listening bool) {
	pooled := s.cfg.UpstreamPooling != "session"
	var stmts []string
	if pooled {
		stmts = append(stmts, "SELECT pg_backend_pid()::text")
	}
	stmts = append(stmts, checkRoleSQL, "LISTEN "+metaChannel)
	res, err := conn.Exec(ctx, strings.Join(stmts, "; ")).ReadAll()
	if err != nil || len(res) != len(stmts) {
		return s.checkPooler(ctx, conn), checkRole(ctx, conn), false
	}
	rowsOf := func(r *pgconn.Result) [][]string {
		var out [][]string
		for _, row := range r.Rows {
			vals := make([]string, len(row))
			for i, v := range row {
				vals[i] = string(v)
			}
			out = append(out, vals)
		}
		return out
	}
	at := 0
	if pooled {
		poolErr = poolerRefusal(conn, rowsOf(res[0]))
		at = 1
	}
	return poolErr, roleRefusal(rowsOf(res[at])), true
}

// startupLokadb returns the lokadb.* setting the client's startup parameters set, as a
// parameter of its own or in options (-c name=value, --name=value), or "". They go upstream
// as they are: a session would start with lokadb.org = N the proxy does not know of, and what
// the proxy does not set itself (a function, a view, what the analysis misses) would run under
// N, the same as SET lokadb.org in a query string (refused, see orgset.go). Names are compared
// without case, as Postgres does. With allowTenant (the proxy config has a section "tenant":
// the cooperative mode) lokadb.tenant is the application's to set and is skipped; any other
// lokadb.* is refused still, also next to it.
func startupLokadb(params map[string]string, allowTenant bool) string {
	proxys := func(name string) bool {
		return strings.HasPrefix(strings.ToLower(name), "lokadb.") && !(allowTenant && strings.EqualFold(name, TenantSetting))
	}
	for k, v := range params {
		if proxys(k) {
			return k
		}
		if strings.ToLower(k) != "options" {
			continue
		}
		for _, opt := range splitOptions(v) {
			opt = strings.TrimPrefix(opt, "-c")
			opt = strings.TrimPrefix(opt, "--")
			name, _, _ := strings.Cut(opt, "=")
			if name = strings.TrimSpace(name); proxys(name) {
				return name
			}
		}
	}
	return ""
}

// splitOptions splits the startup parameter options as Postgres does (pg_split_opts): on
// whitespace, a backslash taking the next character literally. "-c" and its value may come
// as one word or two; the name is then the next word.
func splitOptions(s string) []string {
	var out []string
	var cur strings.Builder
	esc, in := false, false
	for _, r := range s {
		switch {
		case esc:
			cur.WriteRune(r)
			esc, in = false, true
		case r == '\\':
			esc, in = true, true
		case r == ' ' || r == '\t' || r == '\n' || r == '\r':
			if in {
				out = append(out, cur.String())
				cur.Reset()
				in = false
			}
		default:
			cur.WriteRune(r)
			in = true
		}
	}
	if in {
		out = append(out, cur.String())
	}
	return out
}

// checkRoleSQL (block 9, review of 4af18c4, PR-11): the roles the session is, was logged in as or
// can SET ROLE to that see past row level security: a superuser, BYPASSRLS, REPLICATION (logical
// decoding, pg_logical_slot_get_changes, streams every organization's changes). A login as a
// superuser with options=-c role=app (current_user is app, session_user is not: SET ROLE back to it),
// and SET ROLE into a role the login role is a member of, were let through by a check of
// current_user alone. Attributes are not inherited: it is the right to SET ROLE (PG 16; before,
// membership) that gives them.
const checkRoleSQL = `SELECT current_user::text, r.rolname::text,
    CASE WHEN r.rolname = current_user THEN 'current' WHEN r.rolname = session_user THEN 'session' ELSE 'member' END,
    r.rolsuper::text, r.rolbypassrls::text, r.rolreplication::text
  FROM pg_roles r
  WHERE (r.rolsuper OR r.rolbypassrls OR r.rolreplication)
    AND (r.rolname IN (current_user, session_user)
      OR pg_has_role(current_user, r.oid, CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'SET' ELSE 'MEMBER' END)
      OR pg_has_role(session_user, r.oid, CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'SET' ELSE 'MEMBER' END))
  ORDER BY 3 DESC, 2 LIMIT 1`

// checkRole refuses a session that bypasses row level security (a superuser, BYPASSRLS or
// REPLICATION role, as the login, behind options=-c role=…, or one it can SET ROLE to): the
// policies lokadb_tenant are the second wall behind the proxy's routing (a statement the proxy
// let through unrouted, a function of the database), and such a role sees and changes every
// organization's rows past them. The table owner is no exception only because enable sets
// FORCE ROW LEVEL SECURITY. Checked in every database, enabled or not: enable may come later.
func checkRole(ctx context.Context, conn *pgconn.PgConn) error {
	rows, err := execRows(ctx, conn, checkRoleSQL)
	if err != nil {
		return nil // cannot tell: an error of its own shows up with the client's first statement
	}
	return roleRefusal(rows)
}

// roleRefusal: the refusal checkRoleSQL's rows call for, nil for none.
func roleRefusal(rows [][]string) error {
	if len(rows) != 1 || len(rows[0]) != 6 {
		return nil // none
	}
	r := rows[0]
	var attrs []string
	if r[3] == "true" {
		attrs = append(attrs, "is a superuser")
	}
	if r[4] == "true" {
		attrs = append(attrs, "has BYPASSRLS")
	}
	if r[5] == "true" {
		attrs = append(attrs, "has REPLICATION (logical decoding reads every organization's changes)")
	}
	why := strings.Join(attrs, ", ")
	switch r[2] {
	case "session":
		why = "was logged in as " + r[1] + ", which " + why
	case "member":
		why = "can SET ROLE to " + r[1] + ", which " + why
	}
	return fmt.Errorf("lokadb: role %s bypasses row level security (it %s): it would see and change every organization's rows past the policies lokadb_tenant, so the proxy does not let it in", r[0], why)
}

// survive, deferred in a goroutine, turns a panic in it into an error in the log.
func (s *Server) survive(what string) {
	if r := recover(); r != nil {
		s.log.Error("proxy bug: "+what+" aborted, the proxy goes on", "panic", r, "stack", string(debug.Stack()))
	}
}

func (s *Server) fanoutFor(db string) *Fanout {
	if f := s.fanout[db]; f != nil {
		return f
	}
	if f := s.fanout["*"]; f != nil {
		return f
	}
	f, _ := Config{}.FanoutFor(db) // reject
	return f
}

// upstreamConfig: the upstream DSN parsed (once: ParseConfig reads the environment, .pgpass and
// the service file, at every connection before; third review of performance, perf3-26), a copy
// each connection may change.
func (s *Server) upstreamConfig() (*pgconn.Config, error) {
	s.upOnce.Do(func() { s.upCfg, s.upErr = pgconn.ParseConfig(s.cfg.Upstream) })
	if s.upErr != nil {
		return nil, s.upErr
	}
	return s.upCfg.Copy(), nil
}

// routeLevel: the level of the log line "route" (Config.RouteLog); false: none.
func (s *Server) routeLevel() (slog.Level, bool) {
	switch {
	case s.cfg.NoRouteLog:
		return 0, false
	case s.cfg.RouteLog:
		return slog.LevelInfo, true
	}
	return slog.LevelDebug, true
}

func (s *Server) forwardCancel(m *pgproto3.CancelRequest) {
	v, ok := s.sessions.Load(cancelKey(m.ProcessID, m.SecretKey))
	if ok {
		// the session's connections stay out of the pools until the cancel is sent; a session
		// whose connections went back already cancels nothing (second review of the protocol, P9)
		sess := v.(*session)
		if !sess.cancelBegin() {
			return
		}
		defer sess.cancelEnd()
	}
	if ok && v.(*session).directBusy.Load() != nil {
		// a statement of the session on the direct path: the cloud runs nothing of it, and a
		// cancel arriving there late would hit the proxy's own release of the pause (review PR-3)
		v.(*session).cancelDirect()
		return
	}
	if s.pool != nil {
		// the client holds the proxy's key (proxyCancelKey): only a live session's statement is
		// cancelled, with its connection's own key; a client gone away cancels nothing, though its
		// connection may serve another client now
		if !ok {
			return
		}
		sess := v.(*session)
		m = &pgproto3.CancelRequest{ProcessID: sess.upPID, SecretKey: sess.upSecret}
	}
	cfg, err := s.upstreamConfig()
	if err != nil {
		return
	}
	up, err := net.DialTimeout("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))), 5*time.Second)
	if err != nil {
		return
	}
	defer up.Close()
	if buf, err := m.Encode(nil); err == nil {
		_, _ = up.Write(buf)
	}
}

func execRows(ctx context.Context, conn *pgconn.PgConn, sql string) ([][]string, error) {
	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	var out [][]string
	for _, r := range res {
		for _, row := range r.Rows {
			vals := make([]string, len(row))
			for i, v := range row {
				vals[i] = string(v)
			}
			out = append(out, vals)
		}
	}
	return out, nil
}

func sendError(be *pgproto3.Backend, err error) {
	msg := &pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: err.Error()}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		msg = &pgproto3.ErrorResponse{Severity: pgErr.Severity, Code: pgErr.Code, Message: pgErr.Message}
	}
	be.Send(msg)
	_ = be.Flush()
}

// FlushCaches drops everything the proxy keeps between clients: the idle cloud connections and the
// direct ones (their sessions were opened with the password and the rights of their day), the
// shared metadata. The next connection of each client logs in to Postgres again (block 9, review
// PR-7: after ALTER ROLE … PASSWORD, NOLOGIN, VALID UNTIL, REVOKE CONNECT or a change of pg_hba,
// the old password lets in up to max_age otherwise).
func (s *Server) FlushCaches() (cloud, direct int) {
	if s.pool != nil {
		cloud = s.pool.flush()
	}
	if s.directPool != nil {
		direct = s.directPool.flush()
	}
	if s.metaCache != nil {
		s.metaCache.flush()
	}
	return cloud, direct
}

// flushOnHangup: SIGHUP flushes the caches (FlushCaches): the way to end the window of a changed
// password at once.
func (s *Server) flushOnHangup() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP)
	for range ch {
		cloud, direct := s.FlushCaches()
		s.log.Info("caches flushed on SIGHUP", "cloud_connections", cloud, "direct_connections", direct)
	}
}
