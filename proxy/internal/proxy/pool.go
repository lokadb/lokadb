package proxy

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// The cache of cloud connections (block 9, item 5; tasks/proxy-pooling.md, project of 25.10):
// when a client goes away, its server connection is reset (DISCARD ALL) and kept for the next
// client with the same key instead of being closed, so that a client that connects for every
// request (PHP, serverless) does not pay a connection, an authentication and the checks each
// time. On by default for every database (the user's decision of 27.10); "upstream_pool": false
// turns it off.
//
// What one client leaves must not reach the next: only a clean connection is kept (the cloud
// outside a transaction, nothing pending, no COPY, the client's answer read to the end), and
// DISCARD ALL drops the settings, SET ROLE, temporary tables, prepared statements, cursors,
// LISTEN, advisory locks; the proxy's caches belong to the session, not the connection. The
// key holds the client's password only as an HMAC under a key of the process (never written
// anywhere): another password misses, and Postgres judges it on a new connection.

// PoolConfig: the cache of cloud connections (Config.UpstreamPool). In the config an object, or
// false (Off: no cache) or true (an empty object).
type PoolConfig struct {
	MaxIdle      int    `json:"max_idle,omitempty"`       // idle connections per key (10)
	MaxIdleTotal int    `json:"max_idle_total,omitempty"` // idle connections of the proxy (200)
	IdleTTL      string `json:"idle_ttl,omitempty"`       // an idle connection older is closed (60s)
	MaxAge       string `json:"max_age,omitempty"`        // a connection older is closed, not kept (5m)
	Off          bool   `json:"-"`
}

// UnmarshalJSON takes false and true besides the object.
func (c *PoolConfig) UnmarshalJSON(b []byte) error {
	switch strings.TrimSpace(string(b)) {
	case "false":
		*c = PoolConfig{Off: true}
		return nil
	case "true":
		*c = PoolConfig{}
		return nil
	}
	type plain PoolConfig
	return json.Unmarshal(b, (*plain)(c))
}

// pooledConn: a cloud connection the cache hands out: opened, checked (checkPooler,
// checkRole) once, reset before it is kept.
type pooledConn struct {
	hj   *pgconn.HijackedConn
	key  string
	born time.Time
}

// idlePool: idle connections of any kind by key (the cloud's, pool.go; the organizations'
// direct ones, direct_pool.go): handed out newest first, one past maxAge closed, one idle past
// probeAfter probed; idleTTL and maxAge enforced by a janitor; maxIdle per key, maxTotal in all.
type idlePool[T any] struct {
	mu                sync.Mutex
	idle              map[string][]*idleEntry[T]
	n                 int
	maxIdle, maxTotal int
	idleTTL, maxAge   time.Duration
	close             func(T)
	probe             func(T) bool
}

type idleEntry[T any] struct {
	v          T
	born, idle time.Time
}

// probeAfter: an idle connection kept longer answers a Sync before it is handed out (the
// server may have closed it: idle_session_timeout, a restart).
const probeAfter = 5 * time.Second

func newIdlePool[T any](c *PoolConfig, name string, close func(T), probe func(T) bool) (*idlePool[T], error) {
	p := &idlePool[T]{idle: map[string][]*idleEntry[T]{}, maxIdle: 10, maxTotal: 200, idleTTL: time.Minute, maxAge: 5 * time.Minute, close: close, probe: probe}
	if c.MaxIdle > 0 {
		p.maxIdle = c.MaxIdle
	}
	if c.MaxIdleTotal > 0 {
		p.maxTotal = c.MaxIdleTotal
	}
	for _, d := range []struct {
		s   string
		dst *time.Duration
	}{{c.IdleTTL, &p.idleTTL}, {c.MaxAge, &p.maxAge}} {
		if d.s == "" {
			continue
		}
		v, err := time.ParseDuration(d.s)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("%s: bad duration %q", name, d.s)
		}
		*d.dst = v
	}
	go p.janitor()
	return p, nil
}

// get: an idle connection of key and when it was opened; ok false for none.
func (p *idlePool[T]) get(key string) (v T, born time.Time, ok bool) {
	for {
		p.mu.Lock()
		l := p.idle[key]
		if len(l) == 0 {
			p.mu.Unlock()
			return v, born, false
		}
		e := l[len(l)-1]
		p.idle[key] = l[:len(l)-1]
		if len(p.idle[key]) == 0 {
			delete(p.idle, key)
		}
		p.n--
		p.mu.Unlock()
		if time.Since(e.born) > p.maxAge {
			p.close(e.v)
			continue
		}
		if time.Since(e.idle) > probeAfter && !p.probe(e.v) {
			p.close(e.v)
			continue
		}
		return e.v, e.born, true
	}
}

// put keeps a reset connection, or closes it (too old, too many).
func (p *idlePool[T]) put(key string, v T, born time.Time) {
	if time.Since(born) > p.maxAge {
		p.close(v)
		return
	}
	p.mu.Lock()
	if len(p.idle[key]) >= p.maxIdle || p.n >= p.maxTotal {
		p.mu.Unlock()
		p.close(v)
		return
	}
	p.idle[key] = append(p.idle[key], &idleEntry[T]{v: v, born: born, idle: time.Now()})
	p.n++
	p.mu.Unlock()
}

// evict closes every idle connection of key: the server refused the role something on one of
// them (an authorization error, a privilege of the role), so the others may be stale too.
func (p *idlePool[T]) evict(key string) {
	p.mu.Lock()
	l := p.idle[key]
	delete(p.idle, key)
	p.n -= len(l)
	p.mu.Unlock()
	for _, e := range l {
		p.close(e.v)
	}
}

// evictPrefix closes every idle connection of the keys that start with prefix (an
// organization's direct connections under a DSN it no longer has: directPoolKey).
func (p *idlePool[T]) evictPrefix(prefix string) {
	var gone []*idleEntry[T]
	p.mu.Lock()
	for k, l := range p.idle {
		if strings.HasPrefix(k, prefix) {
			gone = append(gone, l...)
			p.n -= len(l)
			delete(p.idle, k)
		}
	}
	p.mu.Unlock()
	for _, e := range gone {
		p.close(e.v)
	}
}

// flush closes every idle connection, whatever its key (a changed password, a revoked role): how
// many.
func (p *idlePool[T]) flush() int {
	p.mu.Lock()
	var all []T
	for _, l := range p.idle {
		for _, e := range l {
			all = append(all, e.v)
		}
	}
	p.idle = map[string][]*idleEntry[T]{}
	p.n = 0
	p.mu.Unlock()
	for _, v := range all {
		p.close(v)
	}
	return len(all)
}

// janitor closes idle connections past idleTTL or maxAge.
func (p *idlePool[T]) janitor() {
	for range time.Tick(10 * time.Second) {
		var old []T
		p.mu.Lock()
		for k, l := range p.idle {
			kept := l[:0]
			for _, e := range l {
				if time.Since(e.idle) > p.idleTTL || time.Since(e.born) > p.maxAge {
					old = append(old, e.v)
				} else {
					kept = append(kept, e)
				}
			}
			p.n -= len(l) - len(kept)
			if len(kept) == 0 {
				delete(p.idle, k)
			} else {
				p.idle[k] = kept
			}
		}
		p.mu.Unlock()
		for _, v := range old {
			p.close(v)
		}
	}
}

// connPool: the cache of cloud connections, by the client's key (key).
type connPool struct {
	*idlePool[*pgconn.HijackedConn]
	secret [32]byte // the key of the password's HMAC: in memory alone
}

func newConnPool(c *PoolConfig, log *slog.Logger) (*connPool, error) {
	ip, err := newIdlePool(c, "upstream_pool", closeHijacked, probe)
	if err != nil {
		return nil, err
	}
	p := &connPool{idlePool: ip}
	if _, err := rand.Read(p.secret[:]); err != nil {
		return nil, err
	}
	return p, nil
}

// key: the cache's key of a client: the database, the role, the startup parameters the proxy
// connects with (sorted) and the password's HMAC.
func (p *connPool) key(db, user, password string, params map[string]string) string {
	names := make([]string, 0, len(params))
	for k := range params {
		names = append(names, k)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, s := range []string{db, user} {
		b.WriteString(s)
		b.WriteByte(0)
	}
	for _, k := range names {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(params[k])
		b.WriteByte(0)
	}
	m := hmac.New(sha256.New, p.secret[:])
	m.Write([]byte(password))
	b.WriteString(hex.EncodeToString(m.Sum(nil)))
	return b.String()
}

// getConn: an idle cloud connection of key, nil for none.
func (p *connPool) getConn(key string) *pooledConn {
	hj, born, ok := p.get(key)
	if !ok {
		return nil
	}
	return &pooledConn{hj: hj, key: key, born: born}
}

// putConn keeps a reset cloud connection, or closes it.
func (p *connPool) putConn(pc *pooledConn) { p.put(pc.key, pc.hj, pc.born) }

func closeHijacked(hj *pgconn.HijackedConn) { closePooled(&pooledConn{hj: hj}) }

func closePooled(pc *pooledConn) {
	pc.hj.Frontend.Send(&pgproto3.Terminate{})
	_ = pc.hj.Frontend.Flush()
	pc.hj.Conn.Close()
}

// probe: the connection answers a Sync with ReadyForQuery outside a transaction.
func probe(hj *pgconn.HijackedConn) bool {
	_ = hj.Conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer hj.Conn.SetDeadline(time.Time{})
	hj.Frontend.Send(&pgproto3.Sync{})
	if hj.Frontend.Flush() != nil {
		return false
	}
	for {
		m, err := hj.Frontend.Receive()
		if err != nil {
			return false
		}
		if r, ok := m.(*pgproto3.ReadyForQuery); ok {
			return r.TxStatus == 'I'
		}
	}
}

// frontQuery runs a simple query of the proxy's own on a hijacked connection before a session
// takes it (LISTEN, the metadata): its rows, and its error with the SQLSTATE (upstreamError).
func frontQuery(hj *pgconn.HijackedConn, sql string) ([][]string, error) {
	_ = hj.Conn.SetDeadline(time.Now().Add(15 * time.Second))
	defer hj.Conn.SetDeadline(time.Time{})
	hj.Frontend.Send(&pgproto3.Query{String: ownsql.Tag(sql)})
	if err := hj.Frontend.Flush(); err != nil {
		return nil, err
	}
	var rows [][]string
	var qerr error
	for {
		m, err := hj.Frontend.Receive()
		if err != nil {
			return nil, err
		}
		switch m := m.(type) {
		case *pgproto3.DataRow:
			row := make([]string, len(m.Values))
			for i, v := range m.Values {
				row[i] = string(v)
			}
			rows = append(rows, row)
		case *pgproto3.ErrorResponse:
			if qerr == nil {
				qerr = &upstreamError{code: m.Code, msg: m.Message}
			}
		case *pgproto3.ReadyForQuery:
			if qerr != nil {
				return nil, qerr
			}
			return rows, nil
		}
	}
}

// proxyCancelKey: the BackendKeyData the proxy gives a client when connections are cached: the
// server's key would let a client gone away cancel the next one's statements on the same
// connection. Server.sessions finds the session by it while the session lives.
func proxyCancelKey() (uint32, []byte) {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.BigEndian.Uint32(b[:4]) &^ (1 << 31), b[4:]
}
