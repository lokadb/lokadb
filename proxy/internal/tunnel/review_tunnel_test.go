package tunnel

// Review 2 of the tunnel (tasks/review/tunnel.md): the findings fixed, each with the test
// that proved it, turned around.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

var gssRequest = func() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b, 8)
	binary.BigEndian.PutUint32(b[4:], codeGSS)
	return b
}()

// tlsOrg: a relay serving organization 1 with org_tls require, its agent connected to db.
func tlsOrg(t *testing.T, db func(cert tls.Certificate) string) (*testRelay, string) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{poll: 100 * time.Millisecond}, nil)
	tr.store.SetEndpoint("1", Endpoint{Listen: org1, TLS: "require"})
	waitFor(t, "served", func() bool { return tr.r.serves("1") && tr.r.tlsRequired("1") })
	cert, err := tls.LoadX509KeyPair(filepath.Join(tr.dir, "relay.crt"), filepath.Join(tr.dir, "relay.key"))
	if err != nil {
		t.Fatal(err)
	}
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), db(cert), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	return tr, org1
}

// V-2: every GSSENCRequest was declined and requireTLS called itself again, without a limit:
// one client grew the relay's stack until the process died (every organization with it).
// Now one is declined, a second one refused and the connection closed.
func TestGSSRequestsBounded(t *testing.T) {
	_, org1 := tlsOrg(t, func(tls.Certificate) string { return echoServer(t, "db:") })
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	go func() {
		for i := 0; i < 1000; i++ {
			if _, err := c.Write(gssRequest); err != nil {
				return
			}
		}
	}()
	ans, _ := io.ReadAll(c)
	if len(ans) == 0 || ans[0] != 'N' {
		t.Fatalf("the first GSSENCRequest: %q", ans)
	}
	if len(ans) < 2 || ans[1] != 'E' {
		t.Fatalf("the second GSSENCRequest is not refused with an error: %q", ans)
	}
}

// S-1: an SSLRequest after the declined GSSENCRequest (libpq with a Kerberos ticket cache)
// reached the organization's database's answer only to find the connection closed. Now it goes
// on: the client gets the database's 'S' through the tunnel.
func TestGSSThenSSL(t *testing.T) {
	_, org1 := tlsOrg(t, func(cert tls.Certificate) string { return fakeDB(t, 'S', cert) })
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write(gssRequest)
	ans := make([]byte, 1)
	if _, err := io.ReadFull(c, ans); err != nil || ans[0] != 'N' {
		t.Fatalf("GSSENCRequest answer: %q %v", ans, err)
	}
	_, _ = c.Write(sslRequest)
	if _, err := io.ReadFull(c, ans); err != nil || ans[0] != 'S' {
		t.Fatalf("SSLRequest after GSSENCRequest: %q %v", ans, err)
	}
}

// The negotiation has one deadline: a client that sends nothing after its GSSENCRequest is
// closed, it does not hold the connection by writing a request every few seconds.
func TestNegotiationDeadline(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for the 10 s deadline")
	}
	_, org1 := tlsOrg(t, func(tls.Certificate) string { return echoServer(t, "db:") })
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t0 := time.Now()
	_ = c.SetDeadline(time.Now().Add(20 * time.Second))
	_, _ = c.Write(gssRequest)
	_, _ = io.ReadAll(c)
	if d := time.Since(t0); d > 12*time.Second {
		t.Fatalf("closed after %v", d)
	}
}

// renewWith calls /v1/renew presenting cert, as the agent does.
func (tr *testRelay) renewWith(t *testing.T, cert tls.Certificate) (int, enrollResponse) {
	t.Helper()
	pool, err := loadPool(filepath.Join(tr.dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	cl := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{cert}}}}
	body, _ := json.Marshal(enrollRequest{CSR: csr(t)})
	resp, err := cl.Post("https://"+tr.enroll+"/v1/renew", "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out enrollResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

// V-1: the relay superseded the certificate a renewal was asked with at once. When the
// answer never reached the agent (a timeout, a dropped connection, a failed write of its
// state) the agent kept the old certificate, was refused every 10 s and, once its session
// dropped, could not reconnect without a new token. Now the old certificate stays valid
// until the new one is presented: the retried renewal is answered, the agent restarted with
// its old state connects; once the new certificate is used, the old one opens nothing.
func TestLostRenewalRetried(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, EnrollFailPerMin: -1}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), echoServer(t, "db:"), true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	if code, out := tr.renewWith(t, *st.cert); code != http.StatusOK { // its answer is lost
		t.Fatalf("first renewal: %d %q", code, out.Error)
	}
	code, out := tr.renewWith(t, *st.cert)
	if code != http.StatusOK {
		t.Fatalf("retried renewal: %d %q", code, out.Error)
	}
	a := tr.agent(t, state, "", echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "the agent with its old certificate reconnects", func() bool { return tr.session("1") != "" })

	// the new certificate is used (a session or a renewal with it): the old one is done
	b, _ := pem.Decode([]byte(out.Cert))
	child, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := tr.store.Confirm(context.Background(), child.SerialNumber.Text(16)); err != nil {
		t.Fatal(err)
	}
	if code, out := tr.renewWith(t, *st.cert); code != http.StatusForbidden {
		t.Fatalf("renewal with the old certificate after the new one was used: %d %q", code, out.Error)
	}
}

// S-2: revoking the agent's current serial revoked it and its renewals, not the certificate
// its live session was opened with: the session kept serving. Now a serial revokes the whole
// chain of its agent, and the relay closes that session within its 2 s check.
func TestRevokeCurrentSerialClosesSession(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, CertTTL: "3s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	parent := tr.session("1")
	waitFor(t, "renewal", func() bool { return a.Serial() != parent })
	tr.store.RevokeSerial(a.Serial())
	deadline := time.Now().Add(5 * time.Second)
	for tr.session("1") == parent && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if tr.session("1") == parent {
		t.Fatalf("the session on %s still serves after its agent was revoked", parent)
	}
}

type cloudCountStore struct {
	*MemStore
	calls atomic.Int64
}

func (c *cloudCountStore) CloudWindow(ctx context.Context, org string) (CloudGrant, error) {
	c.calls.Add(1)
	return c.MemStore.CloudWindow(ctx, org)
}

// S-3: every stream an agent opened towards the cloud was one registry query, and nothing
// limited how many streams it opened. Now the answer is kept for a second per organization,
// and one session has at most max_cloud_streams open: the next one is refused.
func TestCloudStreamsBounded(t *testing.T) {
	const ping = "ping\n"
	org1 := freeAddr(t)
	var cs *cloudCountStore
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, Cloud: pgCloud(t), CloudAccess: "window", MaxCloudStreams: 8},
		func(m *MemStore) Store { cs = &cloudCountStore{MemStore: m}; return cs })
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return tr.session("1") != "" && len(a.sess) > 0
	})
	// the window closed: 300 streams, a few registry queries
	for i := 0; i < 300; i++ {
		st, err := a.openCloud()
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		_, _ = st.Write([]byte(ping))
		_ = st.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = io.ReadAll(st)
		st.Close()
	}
	if n := cs.calls.Load(); n > 10 {
		t.Fatalf("300 streams with the window closed made %d registry queries", n)
	}
	// the window open: 8 streams to the cloud at once, the 9th refused
	tr.store.SetCloud("1", true, "mover", "movedb")
	time.Sleep(1100 * time.Millisecond) // past the kept answer
	var held []net.Conn
	for i := 0; i < 9; i++ {
		st, err := a.openCloud()
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, st)
		_ = st.SetDeadline(time.Now().Add(5 * time.Second))
		if i < 8 {
			_, _ = st.Write(moverStartup())
		} else { // the refusal answers a Postgres client's startup message
			b, _ := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u"}}).Encode(nil)
			_, _ = st.Write(b)
		}
		buf := make([]byte, 256)
		n, _ := st.Read(buf)
		got := string(buf[:n])
		switch {
		case i < 8 && got != moverGreeting:
			t.Fatalf("stream %d: %q", i+1, got)
		case i == 8 && !strings.Contains(got, "too many connections to the cloud"):
			t.Fatalf("stream 9 beyond max_cloud_streams: %q", got)
		}
	}
	for _, st := range held {
		st.Close()
	}
}

// S-4: no ceiling on connections. An organization's endpoint took any number (each one a
// stream and, at the agent, a connection to the organization's database before Postgres
// authenticated it), the agent port any number of half-open handshakes. Now: max_org_conns
// per endpoint, max_handshakes on the agent port, max_conns at the agent.
func TestConnectionCeilings(t *testing.T) {
	const ping = "ping\n"
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, MaxOrgConns: 3, MaxHandshakes: 2}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false) // keeps its connections
	a.cfg.MaxConns = 2
	a.conns = slots(2)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	startup, _ := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u"}}).Encode(nil)
	dial := func() (net.Conn, string) {
		c, err := net.Dial("tcp", org1)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = c.Write([]byte(ping))
		buf := make([]byte, 256)
		n, _ := c.Read(buf)
		return c, string(buf[:n])
	}
	var held []net.Conn
	for i := 0; i < 2; i++ { // the agent's two connections to the database
		c, got := dial()
		if got != "db:"+ping {
			t.Fatalf("connection %d: %q", i+1, got)
		}
		held = append(held, c)
	}
	// the third: the relay lets it through (3 per endpoint), the agent refuses it
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write(startup)
	if b, _ := io.ReadAll(c); !strings.Contains(string(b), "too many connections to the organization's database") {
		t.Fatalf("beyond the agent's max_conns: %q", b)
	}
	c.Close()
	// the fourth beyond max_org_conns, with the third held open at the relay
	third, _ := net.Dial("tcp", org1)
	defer third.Close()
	time.Sleep(200 * time.Millisecond)
	c, err = net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write(startup)
	if b, _ := io.ReadAll(c); !strings.Contains(string(b), "too many connections open") {
		t.Fatalf("beyond max_org_conns: %q", b)
	}
	for _, c := range held {
		c.Close()
	}
	// the agent port: two connections that never finish their handshake, the third is closed at once
	var shaking []net.Conn
	for i := 0; i < 2; i++ {
		s, err := net.Dial("tcp", tr.relayAddr)
		if err != nil {
			t.Fatal(err)
		}
		shaking = append(shaking, s)
	}
	time.Sleep(200 * time.Millisecond)
	s, err := net.Dial("tcp", tr.relayAddr)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.SetDeadline(time.Now().Add(3 * time.Second))
	t0 := time.Now()
	if _, err := s.Read(make([]byte, 1)); err == nil || time.Since(t0) > 2*time.Second {
		t.Fatalf("the third handshake beyond max_handshakes: %v after %v", err, time.Since(t0))
	}
	for _, s := range append(shaking, s) {
		s.Close()
	}
}

type enrollFailStore struct {
	*MemStore
	fail atomic.Bool
}

func (f *enrollFailStore) EnrollCert(ctx context.Context, h, remote, serial, org string, notAfter time.Time) error {
	if f.fail.Load() {
		return errors.New("registry down")
	}
	return f.MemStore.EnrollCert(ctx, h, remote, serial, org, notAfter)
}

// N-4: the token was used, then the certificate recorded, two registry calls: a failure
// between them burned the token. Now both are one transaction: a failed write leaves the
// token for the next try.
func TestEnrollRegistryFailureKeepsToken(t *testing.T) {
	var fs *enrollFailStore
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}}, func(m *MemStore) Store {
		fs = &enrollFailStore{MemStore: m}
		return fs
	})
	tok := tr.store.CreateToken("1")
	fs.fail.Store(true)
	if code, out := tr.post(t, "/v1/enroll", enrollRequest{Token: tok, CSR: csr(t)}); code != http.StatusServiceUnavailable {
		t.Fatalf("registry down: %d %q", code, out.Error)
	}
	fs.fail.Store(false)
	if code, out := tr.post(t, "/v1/enroll", enrollRequest{Token: tok, CSR: csr(t)}); code != http.StatusOK {
		t.Fatalf("the same token after the registry is back: %d %q", code, out.Error)
	}
}

// S-5: the relay read its certificate and the CA once, at start: a rotation was a restart
// (every session dropped). Now a whole CA rotation runs with the relay and the agent up: both
// CAs in the bundle, a relay certificate of the new CA, the new CA first in the bundle with
// its key. The agent reconnects through each step, new certificates come from the new CA.
func TestRotationWithoutRestart(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, CertTTL: "3s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	read := func(dir, f string) []byte {
		b, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	write := func(f string, b []byte) {
		if err := os.WriteFile(filepath.Join(tr.dir, f), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	through := func() bool { // a round trip through the tunnel
		c, err := net.DialTimeout("tcp", org1, time.Second)
		if err != nil {
			return false
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = c.Write([]byte("ping"))
		b, _ := io.ReadAll(c)
		return string(b) == "db:ping"
	}
	reconnect := func(step string) { // drop the agent's session: it comes back through the new files
		tr.r.mu.Lock()
		for _, as := range tr.r.sessions["1"] {
			as.sess.Close()
		}
		tr.r.mu.Unlock()
		at := time.Now()
		waitFor(t, step+": the agent reconnects", func() bool { // a session begun after the change, and through it
			for _, si := range tr.r.Sessions() {
				if !si.Retiring && si.Since.After(at) {
					return through()
				}
			}
			return false
		})
	}
	next := t.TempDir()
	if err := GenerateCerts(next, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	ca1, ca2 := read(tr.dir, "ca.crt"), read(next, "ca.crt")
	write("ca.crt", append(append([]byte{}, ca1...), ca2...)) // 1. both CAs
	reconnect("both CAs")
	write("relay.key", read(next, "relay.key")) // 2. the relay certificate of the new CA
	write("relay.crt", read(next, "relay.crt"))
	reconnect("relay certificate of the new CA")
	write("ca.key", read(next, "ca.key")) // 3. the new CA signs
	write("ca.crt", append(append([]byte{}, ca2...), ca1...))
	first := a.Serial()
	waitFor(t, "a renewal signed by the new CA", func() bool {
		leaf := a.creds.leaf()
		return a.Serial() != first && leaf != nil && leaf.CheckSignatureFrom(mustLeaf(t, ca2)) == nil
	})
	reconnect("agent certificate of the new CA")
}

func mustLeaf(t *testing.T, pemBytes []byte) *x509.Certificate {
	b, _ := pem.Decode(pemBytes)
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// K-1, the tunnel's part: the agent's -cloud-listen reaches the cloud database only while a
// move's window is open, and a connection opened in the window ends when it closes (it was
// left open before: a customer could keep reading the cloud database after the move).
func TestCloudWindowClosesStreams(t *testing.T) {
	const q = "q\n"
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, Cloud: pgCloud(t), CloudAccess: "window", poll: 200 * time.Millisecond}, nil)
	cl := freeAddr(t)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	a.cfg.CloudListen, a.cfg.CloudAllow = cl, []string{"127.0.0.1"}
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { // both sides: the relay lists the session before the agent's ping returns
		a.mu.Lock()
		defer a.mu.Unlock()
		return tr.session("1") != "" && len(a.sess) > 0
	})
	line := func(c net.Conn) (string, error) {
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Write([]byte(q)); err != nil {
			return "", err
		}
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		return string(buf[:n]), err
	}
	// no window: refused
	if msg := pgError(t, cl); !strings.Contains(msg, "not open to organization 1") {
		t.Fatalf("without a window: %q", msg)
	}
	tr.store.SetCloud("1", true, "mover", "movedb")
	time.Sleep(1100 * time.Millisecond) // past a kept answer
	var c net.Conn
	waitFor(t, "a connection in the window", func() bool {
		var err error
		if c, err = net.Dial("tcp", cl); err != nil {
			return false
		}
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = c.Write(moverStartup())
		greet := make([]byte, 64)
		if n, _ := c.Read(greet); string(greet[:n]) != moverGreeting {
			c.Close()
			return false
		}
		if got, _ := line(c); got != "cloud:"+q {
			c.Close()
			return false
		}
		return true
	})
	defer c.Close()
	tr.store.SetCloud("1", false) // move-org's end
	waitFor(t, "the connection closed with the window", func() bool {
		_, err := line(c)
		return err != nil
	})
}

// N-2: failures were counted by client host alone: agents behind one NAT shared ten a minute,
// one agent refused every 10 s used them up for all. Now a client with a certificate counts
// as its agent; an IPv6 host counts by its /64.
func TestFailuresCountPerAgent(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}}, nil)
	certOf := func() tls.Certificate {
		state := t.TempDir()
		if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
			t.Fatal(err)
		}
		st, err := loadState(state)
		if err != nil {
			t.Fatal(err)
		}
		return *st.cert
	}
	bad, good := certOf(), certOf()
	leaf, _ := x509.ParseCertificate(bad.Certificate[0])
	tr.store.RevokeSerial(leaf.SerialNumber.Text(16))
	codes := map[int]int{}
	for i := 0; i < 12; i++ {
		code, _ := tr.renewWith(t, bad)
		codes[code]++
	}
	if codes[http.StatusTooManyRequests] == 0 {
		t.Fatalf("a revoked agent renewing 12 times: %v", codes)
	}
	if code, out := tr.renewWith(t, good); code != http.StatusOK {
		t.Fatalf("another agent from the same host: %d %q", code, out.Error)
	}
	for in, want := range map[string]string{"[2001:db8:1:2:3:4:5:6]:7444": "2001:db8:1:2::/64", "10.0.0.1:7444": "10.0.0.1"} {
		if got := hostOf(in); got != want {
			t.Errorf("hostOf(%s) = %s, want %s", in, got, want)
		}
	}
}
