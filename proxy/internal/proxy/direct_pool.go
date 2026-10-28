package proxy

import (
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The pool of direct connections (block 10, item 7; tasks/proxy-pooling.md, project of 25.10):
// a session's connection to an organization's database (profile fdw+direct) outlives the
// session: when the session ends it is reset (DISCARD ALL) and kept for the next session of the
// same database, organization, role DSN and startup parameters. Off unless the config has a
// section direct_pool.
//
// What one session leaves must not reach the next: only a connection outside a transaction,
// not running a statement and with no error of the role is kept; DISCARD ALL drops its
// settings, the proxy's statements (lokadb_d<n>), temporary tables, locks; the next session
// repeats its own settings on it from the start. The role is checked (directRoleCheck) once,
// when the connection opens: maxAge bounds how long a change of its rights goes unseen.

// directPoolKey: the pool's key of a direct connection: the database, the organization, the
// role's DSN (customers_app, from the config: no secret of the client's) and the startup
// parameters it is opened with (sorted).
func directPoolKey(db, org, dsn string, params map[string]string) string {
	names := make([]string, 0, len(params))
	for k := range params {
		names = append(names, k)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, s := range []string{db, org, dsn} {
		b.WriteString(s)
		b.WriteByte(0)
	}
	for _, k := range names {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(params[k])
		b.WriteByte(0)
	}
	return b.String()
}

// directPoolKeyFor: the pool's key of the session's connection to org: the DSN as
// customersApp finds it (an organization named by a uuid is configured by its number).
func (s *session) directPoolKeyFor(org string, params map[string]string) string {
	return directPoolKey(s.db, org, s.customersApp(org), params)
}

func newDirectPool(c *PoolConfig) (*idlePool[*directConn], error) {
	return newIdlePool(c, "direct_pool", closeDirectConn, probeDirectConn)
}

func closeDirectConn(dc *directConn) {
	dc.fe.Send(&pgproto3.Terminate{})
	_ = dc.fe.Flush()
	dc.conn.Close()
}

// probeDirectConn: the connection answers a Sync with ReadyForQuery outside a transaction.
func probeDirectConn(dc *directConn) bool {
	_ = dc.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer dc.conn.SetDeadline(time.Time{})
	dc.fe.Send(&pgproto3.Sync{})
	if dc.fe.Flush() != nil {
		return false
	}
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return false
		}
		if r, ok := m.(*pgproto3.ReadyForQuery); ok {
			return r.TxStatus == 'I'
		}
	}
}

// parkDirect resets dc for the next session (the session ends): only a connection outside a
// transaction, not running a statement, without a refused privilege and younger than maxAge.
// true: reset (DISCARD ALL answered, outside a transaction); false: to be closed.
func (s *session) parkDirect(dc *directConn) bool {
	p := s.srv.directPool
	if p == nil || dc.poolKey == "" || dc.evict || s.directBusy.Load() == dc || time.Since(dc.born) > p.maxAge {
		return false
	}
	if d := s.direct.tx; d != nil && d.dc == dc { // a transaction the client left open: its server rolls it back
		return false
	}
	return resetDirect(dc)
}

// resetDirect: DISCARD ALL on dc and its answer read, up to 5 s: true when the connection is
// clean and can go back to the pool. It touches the connection alone (not the session's state),
// so it may run in a goroutine of its own once the session has let go of dc.
func resetDirect(dc *directConn) bool {
	_ = dc.conn.SetDeadline(time.Now().Add(5 * time.Second))
	defer dc.conn.SetDeadline(time.Time{})
	dc.fe.Send(&pgproto3.Query{String: "DISCARD ALL"}) // refused inside a transaction block, as it should be
	if dc.fe.Flush() != nil {
		return false
	}
	// the reset's own answer: its tag, then ReadyForQuery outside a transaction. A ReadyForQuery
	// before the tag belongs to an exchange left unread (an aborted read): the connection is not
	// clean, and its leftovers would reach the next session (block 10's review)
	discarded := false
	for {
		m, err := dc.fe.Receive()
		if err != nil {
			return false
		}
		switch m := m.(type) {
		case *pgproto3.CommandComplete:
			discarded = string(m.CommandTag) == "DISCARD ALL"
		case *pgproto3.ErrorResponse:
			return false
		case *pgproto3.ReadyForQuery:
			return discarded && m.TxStatus == 'I'
		}
	}
}

// markEvict: an error of the organization's server on dc that may concern its role (an
// authorization error, a refused privilege): dc is not kept, and the pool forgets its key.
func (dc *directConn) markEvict(code string) {
	if strings.HasPrefix(code, "28") || code == "42501" {
		dc.evict = true
	}
}

// directIdleClose: a session's direct connection unused this long goes back to the pool, or is
// closed (review of performance, PF11). Without it a session kept one connection to each
// organization it ever touched to its end: an application's pool of P connections, each serving
// every organization, held P connections at every organization's database.
var directIdleClose = 60 * time.Second

// closeIdleDirect, before each message of the client: the session's direct connections unused
// for directIdleClose, but one its transaction or COPY holds. A look every few seconds at most,
// and one connection a look. The reset (DISCARD ALL, up to 5 s) runs in a goroutine of its own, so
// the client's message waits for none (third review of performance, perf3-21).
func (s *session) closeIdleDirect() {
	if len(s.direct.conns) == 0 || s.direct.copy != nil {
		return
	}
	now := time.Now()
	if now.Sub(s.direct.idleCheck) < min(5*time.Second, directIdleClose) {
		return
	}
	s.direct.idleCheck = now
	for org, dc := range s.direct.conns {
		if d := s.direct.tx; d != nil && d.dc == dc || now.Sub(dc.used) < directIdleClose {
			continue
		}
		delete(s.direct.conns, org)
		// the reset (DISCARD ALL, up to 5 s on a slow or unreachable database) runs in a
		// goroutine of its own: the session has let go of dc, and the client's message does not
		// wait for it (third review of performance, perf3-21). What the session owns is read here.
		pool, canPark := s.srv.directPool, s.directBusy.Load() != dc && dc.poolKey != "" && !dc.evict && s.srv.directPool != nil && time.Since(dc.born) <= s.srv.directPool.maxAge
		go func() {
			if canPark && resetDirect(dc) {
				pool.put(dc.poolKey, dc, dc.born)
				return
			}
			if dc.evict && pool != nil {
				pool.evict(dc.poolKey)
			}
			closeDirectConn(dc)
		}()
		return
	}
}
