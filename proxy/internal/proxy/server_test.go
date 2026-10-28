package proxy

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// fakePostgres accepts connections and answers like a server whose BackendKeyData carries
// process id 1 while pg_backend_pid() says pid (a pooler when they differ), and whose role
// app has rolsuper and rolbypassrls as given.
func fakePostgres(t *testing.T, pid string, super, bypass bool) string {
	var risky [][]string // checkRole's answer: the role the session is, how, and its attributes
	if super || bypass {
		risky = [][]string{{"app", "app", "current", strconv.FormatBool(super), strconv.FormatBool(bypass), "false"}}
	}
	return fakePostgresRoles(t, pid, risky)
}

// fakePostgresRoles: fakePostgres whose role check (checkRoleSQL) answers risky (none: the
// session sees past no policy).
func fakePostgresRoles(t *testing.T, pid string, risky [][]string) string {
	addr, _ := fakePostgresQueries(t, pid, risky)
	return addr
}

// fakePostgresQueries: fakePostgresRoles that also reports the query strings it received, in
// order (a string of several statements is one query; each answers with its own result).
func fakePostgresQueries(t *testing.T, pid string, risky [][]string) (string, func() []string) {
	var mu sync.Mutex
	var queries []string
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				be := pgproto3.NewBackend(c, c)
				if _, err := be.ReceiveStartupMessage(); err != nil {
					return
				}
				be.Send(&pgproto3.AuthenticationOk{})
				be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 2}})
				be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				_ = be.Flush()
				for {
					m, err := be.Receive()
					if err != nil {
						return
					}
					unmark(m)
					q, ok := m.(*pgproto3.Query)
					if !ok {
						continue
					}
					mu.Lock()
					queries = append(queries, q.String)
					mu.Unlock()
					for _, stmt := range strings.Split(q.String, "; ") {
						if strings.Contains(stmt, "pg_backend_pid") {
							be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("pid"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}}})
							be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(pid)}})
						}
						if strings.Contains(stmt, "rolbypassrls") {
							f := pgproto3.FieldDescription{DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}
							be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{f, f, f, f, f, f}})
							for _, r := range risky {
								vals := make([][]byte, len(r))
								for i, v := range r {
									vals[i] = []byte(v)
								}
								be.Send(&pgproto3.DataRow{Values: vals})
							}
						}
						tag := "SELECT 1"
						if strings.HasPrefix(stmt, "LISTEN") {
							tag = "LISTEN"
						}
						be.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
					}
					be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
					_ = be.Flush()
				}
			}()
		}
	}()
	return l.Addr().String(), func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), queries...)
	}
}

// startPoolerProxy serves cfg on a free port and returns its address.
func startPoolerProxy(t *testing.T, cfg Config) string {
	addr, _ := startPoolerProxyS(t, cfg)
	return addr
}

// startPoolerProxyS also returns the server, whose pool a test may watch.
func startPoolerProxyS(t *testing.T, cfg Config) (string, *Server) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	cfg.Listen = addr
	srv := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = srv.ListenAndServe() }()
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return addr, srv
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("proxy did not start")
	return "", nil
}

// Block 9, item 6 (P9): a pooler between the proxy and Postgres. In transaction mode the
// proxy's set_config and the client's statement may run on different server connections,
// under another client's organization; the proxy did not notice. It refuses now unless the
// config says the pooler keeps whole sessions.
func TestUpstreamPooler(t *testing.T) {
	for _, c := range []struct {
		name, pid, pooling string
		ok                 bool
	}{
		{"direct", "1", "", true},
		{"pooler", "4242", "", false},
		{"pooler in session mode, declared", "4242", "session", true},
	} {
		up := fakePostgres(t, c.pid, false, false)
		host, port, _ := net.SplitHostPort(up)
		cfgAddr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable", UpstreamPooling: c.pooling})
		h, p, _ := net.SplitHostPort(cfgAddr)
		conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5")
		if c.ok != (err == nil) {
			t.Errorf("%s: connect error %v, want ok=%v", c.name, err, c.ok)
		}
		if err == nil {
			conn.Close(t.Context())
		} else if !strings.Contains(err.Error(), "pooler") {
			t.Errorf("%s: error %v does not say why", c.name, err)
		}
	}
}

// Block 9, item 10: the proxy connected to the cloud under whatever role the client named. A
// superuser or a BYPASSRLS role sees and changes every organization's rows past the policies
// lokadb_tenant (the proxy's routing is the only wall left); the proxy refuses it now, with the
// fix in the hint.
func TestRoleBypassesRLS(t *testing.T) {
	for _, c := range []struct {
		name          string
		super, bypass bool
		why           string
	}{
		{"application role", false, false, ""},
		{"superuser", true, false, "superuser"},
		{"BYPASSRLS", false, true, "BYPASSRLS"},
	} {
		up := fakePostgres(t, "1", c.super, c.bypass)
		host, port, _ := net.SplitHostPort(up)
		addr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable"})
		h, p, _ := net.SplitHostPort(addr)
		conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5")
		if c.why == "" {
			if err != nil {
				t.Errorf("%s: refused: %v", c.name, err)
			} else {
				conn.Close(t.Context())
			}
			continue
		}
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			t.Errorf("%s: connect error %v, want a refusal", c.name, err)
			continue
		}
		if pe.Code != "28000" || !strings.Contains(pe.Message, "role app bypasses row level security") || !strings.Contains(pe.Message, c.why) ||
			!strings.Contains(pe.Hint, "NOBYPASSRLS") || !strings.Contains(pe.Hint, "-app-role") {
			t.Errorf("%s: %s %q hint %q", c.name, pe.Code, pe.Message, pe.Hint)
		}
	}
}

// Block 9, review PR-11: the role check looks past current_user: a login as a superuser behind
// options=-c role=app (session_user), a role the session can SET ROLE to, and REPLICATION (logical
// decoding reads every organization's changes) bypass the policies as much as BYPASSRLS does.
func TestRoleBypassesRLSBehindTheLogin(t *testing.T) {
	for _, c := range []struct {
		name string
		row  []string
		want []string // pieces of the refusal
	}{
		{"REPLICATION", []string{"app", "app", "current", "false", "false", "true"}, []string{"role app bypasses", "has REPLICATION"}},
		{"a superuser behind -c role", []string{"app", "boss", "session", "true", "false", "false"}, []string{"role app bypasses", "was logged in as boss", "superuser"}},
		{"a member of a BYPASSRLS role", []string{"app", "reports", "member", "false", "true", "false"}, []string{"role app bypasses", "can SET ROLE to reports", "BYPASSRLS"}},
	} {
		up := fakePostgresRoles(t, "1", [][]string{c.row})
		host, port, _ := net.SplitHostPort(up)
		addr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable"})
		h, p, _ := net.SplitHostPort(addr)
		conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5")
		var pe *pgconn.PgError
		if !errors.As(err, &pe) {
			if err == nil {
				conn.Close(t.Context())
			}
			t.Errorf("%s: connect error %v, want a refusal", c.name, err)
			continue
		}
		for _, w := range c.want {
			if pe.Code != "28000" || !strings.Contains(pe.Message, w) {
				t.Errorf("%s: %s %q does not say %q", c.name, pe.Code, pe.Message, w)
			}
		}
		if !strings.Contains(pe.Hint, "NOREPLICATION") {
			t.Errorf("%s: hint %q", c.name, pe.Hint)
		}
	}
}

// Block 9, B9.10 (startup): the client's startup parameters other than user and database go
// upstream as they are. options=-c lokadb.org=2 started the session under organization 2,
// unknown to the proxy (it took lokadb.org for empty): what it does not set itself (a
// function, a view, what the analysis misses) ran under 2. The proxy refuses such a startup.
func TestStartupLokadb(t *testing.T) {
	for _, c := range []struct {
		params map[string]string
		want   string
	}{
		{map[string]string{"application_name": "lokadb.app", "options": "-c search_path=public"}, ""},
		{map[string]string{"options": "-c lokadb.org=2"}, "lokadb.org"},
		{map[string]string{"options": "-c  statement_timeout=0   -c LOKADB.Org=2"}, "LOKADB.Org"},
		{map[string]string{"options": "-clokadb.org=2"}, "lokadb.org"},
		{map[string]string{"options": "--lokadb.org=2"}, "lokadb.org"},
		{map[string]string{"options": `-c application_name=a\ -c\ lokadb.org=2`}, ""}, // one escaped value
		{map[string]string{"options": "-c lokadb.tenant=42"}, "lokadb.tenant"},        // without the cooperative mode
		{map[string]string{"options": "-cLOKADB.TENANT=42"}, "LOKADB.TENANT"},
		{map[string]string{"options": "--Lokadb.Tenant=42"}, "Lokadb.Tenant"},
		{map[string]string{"options": "-c  lokadb.tenant=42"}, "lokadb.tenant"},
		{map[string]string{"lokadb.tenant": "42"}, "lokadb.tenant"},
		{map[string]string{"LOKADB.Tenant": "42"}, "LOKADB.Tenant"},
		{map[string]string{"lokadb.org": "2"}, "lokadb.org"},
		{map[string]string{"Lokadb.Org": "2"}, "Lokadb.Org"},
	} {
		if got := startupLokadb(c.params, false); got != c.want {
			t.Errorf("%v: %q, want %q", c.params, got, c.want)
		}
	}
	// Block 10: with a section "tenant" in the config lokadb.tenant is the application's; any
	// other lokadb.* is refused still, also in the same options
	for _, c := range []struct {
		params map[string]string
		want   string
	}{
		{map[string]string{"options": "-c lokadb.tenant=42"}, ""},
		{map[string]string{"options": "-c LOKADB.Tenant=42"}, ""},
		{map[string]string{"lokadb.tenant": "42"}, ""},
		{map[string]string{"options": "-clokadb.tenant=42 --LOKADB.TENANT=42"}, ""},
		{map[string]string{"options": "-c lokadb.tenant=1 -c lokadb.org=2"}, "lokadb.org"},
		{map[string]string{"options": "--lokadb.org=2 -c lokadb.tenant=1"}, "lokadb.org"},
		{map[string]string{"options": "-c lokadb.tenant=1 -clokadb.fanout=allow"}, "lokadb.fanout"},
		{map[string]string{"options": `-c lokadb.tenant=1\ -c\ lokadb.org=2`}, ""}, // one value of lokadb.tenant: startupTenant refuses it
		{map[string]string{"options": "-c lokadb.fanout=allow"}, "lokadb.fanout"},
		{map[string]string{"lokadb.fanout": "allow"}, "lokadb.fanout"},
		{map[string]string{"options": "-c lokadb.tenant=1", "lokadb.org": "2"}, "lokadb.org"},
	} {
		if got := startupLokadb(c.params, true); got != c.want {
			t.Errorf("cooperative %v: %q, want %q", c.params, got, c.want)
		}
	}
	up := fakePostgres(t, "1", false, false)
	host, port, _ := net.SplitHostPort(up)
	addr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable"})
	h, p, _ := net.SplitHostPort(addr)
	for opts, ok := range map[string]bool{"": true, " options='-c lokadb.org=2'": false} {
		conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5"+opts)
		if ok {
			if err != nil {
				t.Errorf("%q: refused: %v", opts, err)
			} else {
				conn.Close(t.Context())
			}
			continue
		}
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "42501" || !strings.Contains(pe.Message, "lokadb.org") {
			t.Errorf("%q: %v, want a refusal naming lokadb.org", opts, err)
		}
	}
	// the cooperative mode: lokadb.tenant connects, lokadb.fanout is refused still
	addr = startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable", Tenant: sessionTenant()})
	h, p, _ = net.SplitHostPort(addr)
	for opts, refused := range map[string]string{" options='-c lokadb.tenant=1'": "", " options='-c lokadb.fanout=allow'": "lokadb.fanout"} {
		conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5"+opts)
		if refused == "" {
			if err != nil {
				t.Errorf("cooperative %q: refused: %v", opts, err)
			} else {
				conn.Close(t.Context())
			}
			continue
		}
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "42501" || !strings.Contains(pe.Message, refused) {
			t.Errorf("cooperative %q: %v, want a refusal naming %s", opts, err, refused)
		}
	}
	// tenant.local_only: no organization for the whole session from the connection string
	lo := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable", Tenant: &TenantConfig{}})
	lh, lp, _ := net.SplitHostPort(lo)
	_, err := pgconn.Connect(t.Context(), "host="+lh+" port="+lp+" user=app password=app dbname=app sslmode=disable connect_timeout=5 options='-c lokadb.tenant=1'")
	if pe := (*pgconn.PgError)(nil); !errors.As(err, &pe) || pe.Code != "0A000" || !strings.Contains(pe.Message, "local_only") {
		t.Errorf("local_only, lokadb.tenant at startup: %v, want a refusal 0A000", err)
	}
	if conn, err := pgconn.Connect(t.Context(), "host="+lh+" port="+lp+" user=app password=app dbname=app sslmode=disable connect_timeout=5"); err != nil {
		t.Errorf("local_only, no lokadb.tenant: %v", err)
	} else {
		conn.Close(t.Context())
	}
	// a value that is not an organization (here the escaped "-c lokadb.org=2" inside it; the
	// connection string takes one backslash off) is refused at the connection
	for _, opts := range []string{` options='-c lokadb.tenant=acme'`, ` options='-c lokadb.tenant=1\\ -c\\ lokadb.org=2'`, ` options='-c lokadb.tenant=1 -c lokadb.tenant=2'`} {
		_, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5"+opts)
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "22023" {
			t.Errorf("cooperative %q: %v, want a refusal 22023", opts, err)
		}
	}
}

// Block 9, review PR-9: a full proxy (MaxConnections) took no cancel request: it answered 53300
// and the runaway statement ran on, just when a cancel is needed. A connection over the limit now
// gets its first message looked at: a cancel request is forwarded (and answered with nothing, as
// Postgres does), anything else is turned away with 53300.
func TestCancelOverTheLimit(t *testing.T) {
	up := fakePostgres(t, "1", false, false)
	host, port, _ := net.SplitHostPort(up)
	addr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable", MaxConnections: 1})
	// takes the only slot: starts up and is asked for its password (the readiness probe of
	// startPoolerProxy may hold the slot for a moment: tried again until it is ours)
	var hold net.Conn
	for i := 0; ; i++ {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		hf := pgproto3.NewFrontend(c, c)
		hf.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app", "database": "app"}})
		_ = hf.Flush()
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		m, err := hf.Receive()
		if _, ok := m.(*pgproto3.AuthenticationCleartextPassword); err == nil && ok {
			hold = c
			break
		}
		c.Close()
		if i > 50 {
			t.Fatalf("no client got the slot: %T %v", m, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	defer hold.Close()
	read := func(c net.Conn) []byte {
		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
		b, _ := io.ReadAll(c)
		return b
	}
	cancel, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = cancel.Write([]byte{0, 0, 0, 16, 0x04, 0xd2, 0x16, 0x2e, 0, 0, 0, 7, 0, 0, 0, 9}) // CancelRequest, pid 7
	if b := read(cancel); len(b) != 0 {
		t.Errorf("a cancel request over the limit was answered: %q", b)
	}
	cancel.Close()
	start, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	be := pgproto3.NewFrontend(start, start)
	be.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app", "database": "app"}})
	_ = be.Flush()
	if b := read(start); !strings.Contains(string(b), "too many connections") {
		t.Errorf("a startup over the limit: %q, want 53300", b)
	}
	start.Close()
}

// Block 9, the connect latency: a new upstream connection is checked in one exchange (the
// pooler, the role, LISTEN of the metadata channel), not three; a refusal comes out of it as
// before.
func TestConnectChecksInOneExchange(t *testing.T) {
	up, queries := fakePostgresQueries(t, "1", nil)
	host, port, _ := net.SplitHostPort(up)
	addr := startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable"})
	h, p, _ := net.SplitHostPort(addr)
	conn, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close(t.Context())
	var checks []string
	for _, q := range queries() {
		if strings.Contains(q, "pg_backend_pid") || strings.Contains(q, "rolbypassrls") || strings.HasPrefix(q, "LISTEN") {
			checks = append(checks, q)
		}
	}
	if len(checks) != 1 || !strings.Contains(checks[0], "pg_backend_pid") || !strings.Contains(checks[0], "rolbypassrls") || !strings.Contains(checks[0], "LISTEN "+metaChannel) {
		t.Fatalf("the checks of a new connection went as %d queries %q, want one with the pooler's, the role's and LISTEN", len(checks), checks)
	}
	// the pooler's refusal still comes out of the one exchange
	up, _ = fakePostgresQueries(t, "4242", nil)
	host, port, _ = net.SplitHostPort(up)
	addr = startPoolerProxy(t, Config{Upstream: "host=" + host + " port=" + port + " sslmode=disable"})
	h, p, _ = net.SplitHostPort(addr)
	if _, err := pgconn.Connect(t.Context(), "host="+h+" port="+p+" user=app password=app dbname=app sslmode=disable connect_timeout=5"); err == nil || !strings.Contains(err.Error(), "pooler") {
		t.Fatalf("a pooler between the proxy and Postgres: %v", err)
	}
}
