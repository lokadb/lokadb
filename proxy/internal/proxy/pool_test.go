package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// poolFake: a server that counts its connections, the DISCARD ALL it got and the cancel
// requests (with the key they carried), and keeps a transaction status per connection.
type poolFake struct {
	mu       sync.Mutex
	conns    int
	discards int
	cancels  []uint32 // process ids the cancel requests named
	addr     string
}

func newPoolFake(t *testing.T) *poolFake {
	f := &poolFake{}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	f.addr = l.Addr().String()
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go f.serve(c)
		}
	}()
	return f
}

func (f *poolFake) count() (conns, discards, cancels int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.conns, f.discards, len(f.cancels)
}

func (f *poolFake) serve(c net.Conn) {
	defer c.Close()
	be := pgproto3.NewBackend(c, c)
	m, err := be.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if cr, ok := m.(*pgproto3.CancelRequest); ok {
		f.mu.Lock()
		f.cancels = append(f.cancels, cr.ProcessID)
		f.mu.Unlock()
		return
	}
	f.mu.Lock()
	f.conns++
	pid := uint32(1000 + f.conns)
	f.mu.Unlock()
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "18.0"})
	be.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: []byte{1, 2, 3, 4}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()
	tx := byte('I')
	text := pgproto3.FieldDescription{DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}
	row := func(vals ...string) {
		fs := make([]pgproto3.FieldDescription, len(vals))
		bs := make([][]byte, len(vals))
		for i, v := range vals {
			fs[i], bs[i] = text, []byte(v)
		}
		be.Send(&pgproto3.RowDescription{Fields: fs})
		be.Send(&pgproto3.DataRow{Values: bs})
	}
	for {
		m, err := be.Receive()
		if err != nil {
			return
		}
		unmark(m)
		switch m := m.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Sync:
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
		case *pgproto3.Query:
			q := m.String
			switch {
			case strings.Contains(q, "pg_backend_pid"):
				row(strconv.Itoa(int(pid)))
			case strings.Contains(q, "rolbypassrls"):
				row("app", "false", "false")
			case strings.Contains(q, "to_regclass('lokadb.tables')"):
				row("false")
			case q == "SELECT slow": // an answer still on the way when the client goes
				time.Sleep(300 * time.Millisecond)
			case q == "BEGIN":
				tx = 'T'
			case q == "COMMIT" || q == "ROLLBACK":
				tx = 'I'
			case q == "DISCARD ALL":
				f.mu.Lock()
				f.discards++
				f.mu.Unlock()
			case strings.Contains(q, "forbidden"):
				be.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "permission denied for table forbidden"})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
				_ = be.Flush()
				continue
			}
			be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			be.Send(&pgproto3.ReadyForQuery{TxStatus: tx})
		}
		_ = be.Flush()
	}
}

// poolProxy: a proxy in front of f, its cloud connections cached when pool is set.
func poolProxy(t *testing.T, f *poolFake, pool *PoolConfig) string {
	addr, _ := poolProxyS(t, f, pool)
	return addr
}

func poolProxyS(t *testing.T, f *poolFake, pool *PoolConfig) (string, *Server) {
	host, port, _ := net.SplitHostPort(f.addr)
	return startPoolerProxyS(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable", UpstreamPool: pool})
}

// parked: the client's session is over and its connection is in the pool (not only its reset
// sent: the next client would otherwise open one of its own).
func parked(srv *Server) bool {
	live := false
	srv.sessions.Range(func(_, _ any) bool { live = true; return false })
	srv.pool.mu.Lock()
	n := srv.pool.n
	srv.pool.mu.Unlock()
	return !live && n == 1
}

func poolConnect(t *testing.T, addr, extra string) *pgconn.PgConn {
	h, p, _ := net.SplitHostPort(addr)
	c, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app dbname=app sslmode=disable connect_timeout=5 "+extra)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// waitFor polls cond for up to 10 s (the proxy resets a connection after its client is gone;
// a loaded machine, a full go test, takes its time: TestPoolReuse and TestPoolCancelKey failed
// once each within 2 s).
func waitFor(cond func() bool) bool {
	for i := 0; i < 500; i++ {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// Block 9, item 5: a client's cloud connection is reset (DISCARD ALL) and handed to the next
// client of the same key; the client gets a cancel key of the proxy's, not the server's.
func TestPoolReuse(t *testing.T) {
	f := newPoolFake(t)
	addr, srv := poolProxyS(t, f, &PoolConfig{})
	a := poolConnect(t, addr, "password=pw")
	if a.PID() >= 1000 && a.PID() < 2000 {
		t.Errorf("the client got the server's key %d, not the proxy's", a.PID())
	}
	if _, err := a.Exec(t.Context(), "SELECT 1").ReadAll(); err != nil {
		t.Fatal(err)
	}
	a.Close(t.Context())
	if !waitFor(func() bool { _, d, _ := f.count(); return d == 1 }) {
		t.Fatalf("no DISCARD ALL after the client went")
	}
	if !waitFor(func() bool { return parked(srv) }) {
		t.Fatalf("the connection is not in the pool after its reset")
	}
	b := poolConnect(t, addr, "password=pw")
	defer b.Close(t.Context())
	if _, err := b.Exec(t.Context(), "SELECT 2").ReadAll(); err != nil {
		t.Fatal(err)
	}
	if conns, _, _ := f.count(); conns != 1 {
		t.Errorf("the second client opened a connection of its own: %d connections", conns)
	}
}

// "upstream_pool": false: every client its own connection, the server's key, no reset.
func TestPoolOff(t *testing.T) {
	f := newPoolFake(t)
	addr := poolProxy(t, f, &PoolConfig{Off: true})
	a := poolConnect(t, addr, "password=pw")
	if a.PID() < 1000 || a.PID() >= 2000 {
		t.Errorf("without the cache the client should hold the server's key, got %d", a.PID())
	}
	a.Close(t.Context())
	time.Sleep(100 * time.Millisecond)
	b := poolConnect(t, addr, "password=pw")
	b.Close(t.Context())
	if conns, d, _ := f.count(); conns != 2 || d != 0 {
		t.Errorf("connections %d, resets %d; want 2 and 0", conns, d)
	}
}

// The key: another password or other startup parameters never get a cached connection; a
// connection a client left inside a transaction, or after Postgres refused its role something,
// is closed, not kept.
func TestPoolNotShared(t *testing.T) {
	for _, c := range []struct {
		name         string
		first, then  string
		firstSQL     string
		wantConns    int
		wantDiscards int
	}{
		{"another password", "password=pw", "password=other", "SELECT 1", 2, 1},
		{"another application_name", "password=pw application_name=a", "password=pw application_name=b", "SELECT 1", 2, 1},
		{"left inside a transaction", "password=pw", "password=pw", "BEGIN", 2, 0},
		{"a privilege refused", "password=pw", "password=pw", "SELECT * FROM forbidden", 2, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := newPoolFake(t)
			addr := poolProxy(t, f, &PoolConfig{})
			a := poolConnect(t, addr, c.first)
			_, _ = a.Exec(t.Context(), c.firstSQL).ReadAll()
			a.Close(t.Context())
			time.Sleep(200 * time.Millisecond)
			b := poolConnect(t, addr, c.then)
			b.Close(t.Context())
			time.Sleep(100 * time.Millisecond)
			if conns, d, _ := f.count(); conns != c.wantConns || d < c.wantDiscards || c.wantDiscards == 0 && d > 1 {
				t.Errorf("connections %d, resets %d; want %d connections", conns, d, c.wantConns)
			}
		})
	}
}

// A cancel goes to the server with the connection's own key while its session lives; the key
// of a client gone away cancels nothing, though its connection serves another client now.
func TestPoolCancelKey(t *testing.T) {
	f := newPoolFake(t)
	addr, srv := poolProxyS(t, f, &PoolConfig{})
	a := poolConnect(t, addr, "password=pw")
	pid, secret := a.PID(), append([]byte{}, a.SecretKey()...)
	if err := a.CancelRequest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { _, _, n := f.count(); return n == 1 }) {
		t.Fatal("the live session's cancel did not reach the server")
	}
	f.mu.Lock()
	got := f.cancels[0]
	f.mu.Unlock()
	if got != 1001 {
		t.Errorf("the cancel named process %d, want the connection's 1001", got)
	}
	a.Close(t.Context())
	if !waitFor(func() bool { return parked(srv) }) { // the session gone: its key is no live session's
		t.Fatal("the connection is not in the pool after the client went")
	}
	b := poolConnect(t, addr, "password=pw")
	defer b.Close(t.Context())
	// the gone client's key, sent as libpq would
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	buf, _ := (&pgproto3.CancelRequest{ProcessID: pid, SecretKey: secret}).Encode(nil)
	_, _ = c.Write(buf)
	c.Close()
	time.Sleep(200 * time.Millisecond)
	if _, _, n := f.count(); n != 1 {
		t.Errorf("the key of a client gone away cancelled on the connection another client holds now (%d cancels)", n)
	}
	if bytes.Equal(b.SecretKey(), secret) {
		t.Errorf("two clients got the same cancel key")
	}
}

// A client that goes while an answer is still on the way (the direct path's ungate is sent
// without waiting, so is any statement whose answer the client did not wait for): the answer
// is waited for a little, then the connection is reset and kept.
func TestPoolAnswerOnTheWay(t *testing.T) {
	f := newPoolFake(t)
	addr := poolProxy(t, f, &PoolConfig{})
	h, p, _ := net.SplitHostPort(addr)
	c, err := net.Dial("tcp", net.JoinHostPort(h, p))
	if err != nil {
		t.Fatal(err)
	}
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app", "database": "app"}})
	_ = fe.Flush()
	for { // the password, then up to ReadyForQuery
		m, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := m.(*pgproto3.AuthenticationCleartextPassword); ok {
			fe.Send(&pgproto3.PasswordMessage{Password: "pw"})
			_ = fe.Flush()
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	fe.Send(&pgproto3.Query{String: "SELECT slow"})
	_ = fe.Flush()
	c.Close() // gone before the answer
	if !waitFor(func() bool { _, d, _ := f.count(); return d == 1 }) {
		t.Fatal("the connection was not reset and kept after the answer on the way came")
	}
	b := poolConnect(t, addr, "password=pw")
	defer b.Close(t.Context())
	if conns, _, _ := f.count(); conns != 1 {
		t.Errorf("%d connections: the first one was closed", conns)
	}
}

// Block 9, review PR-7: the idle connections of every key go at once (SIGHUP after a changed
// password), not by key as evict does on a refused login.
func TestPoolFlush(t *testing.T) {
	var closed atomic.Int32
	p, err := newIdlePool(&PoolConfig{}, "t", func(int) { closed.Add(1) }, func(int) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	p.put("a", 1, now)
	p.put("a", 2, now)
	p.put("b", 3, now)
	if n := p.flush(); n != 3 || closed.Load() != 3 {
		t.Fatalf("flush: %d reported, %d closed, want 3", n, closed.Load())
	}
	if _, _, ok := p.get("a"); ok {
		t.Fatal("a connection of a survived the flush")
	}
	if p.n != 0 {
		t.Fatalf("count after flush: %d", p.n)
	}
	p.put("a", 4, now) // usable again
	if v, _, ok := p.get("a"); !ok || v != 4 {
		t.Fatalf("after a flush: %v %v", v, ok)
	}
}

// The default (no upstream_pool in the config): the cache serves every database (the user's
// decision of 27.10), one without lokadb metadata too (the fake has none): the second client gets
// the first one's connection, reset.
func TestPoolDefault(t *testing.T) {
	f := newPoolFake(t)
	addr := poolProxy(t, f, nil)
	a := poolConnect(t, addr, "password=pw")
	if a.PID() >= 1000 && a.PID() < 2000 { // the cache is there: the client holds a key of the proxy's
		t.Errorf("the default: the client got the server's key %d, as without the cache", a.PID())
	}
	a.Close(t.Context())
	time.Sleep(200 * time.Millisecond)
	b := poolConnect(t, addr, "password=pw")
	b.Close(t.Context())
	time.Sleep(100 * time.Millisecond)
	if conns, d, _ := f.count(); conns != 1 || d < 1 {
		t.Errorf("the default: connections %d, resets %d; want 1 and at least 1", conns, d)
	}
	var cfg Config
	for raw, want := range map[string]string{`{"upstream_pool": false}`: "off", `{"upstream_pool": true}`: "on", `{"upstream_pool": {"max_idle": 3}}`: "on3", `{}`: "default"} {
		cfg = Config{}
		if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatal(err)
		}
		got := "default"
		switch up := cfg.UpstreamPool; {
		case up == nil:
		case up.Off:
			got = "off"
		case up.MaxIdle == 3:
			got = "on3"
		default:
			got = "on"
		}
		if got != want {
			t.Errorf("%s: %s, want %s", raw, got, want)
		}
	}
}
