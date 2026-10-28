package tunnel

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/jackc/pgx/v5/pgproto3"
)

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func echoServer(t *testing.T, prefix string) string {
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
				buf := make([]byte, 64)
				n, _ := c.Read(buf)
				_, _ = c.Write(append([]byte(prefix), buf[:n]...))
			}()
		}
	}()
	return l.Addr().String()
}

// roundTrip sends "ping" and returns what came back.
func roundTrip(t *testing.T, addr string) string {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := c.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(c)
	return string(b)
}

// pgError connects like a Postgres client and returns the server's error message.
func pgError(t *testing.T, addr string) string {
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := fe.Receive()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := m.(*pgproto3.ErrorResponse)
	if !ok {
		t.Fatalf("expected ErrorResponse, got %T", m)
	}
	return e.Message
}

// waitFor polls cond for up to 30 s: it returns as soon as cond holds, the margin is for a
// loaded machine (a renewal of a 3 s certificate, a reconnect after it).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 600; i++ {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", what)
}

func TestEnrollmentTunnelRenewalRevocation(t *testing.T) {
	dir, rogue := t.TempDir(), t.TempDir()
	// The CA, the relay certificate and a static org-1 certificate signed by the right CA
	// but never issued by enrollment.
	if err := GenerateCerts(dir, "localhost", []string{"1"}); err != nil {
		t.Fatal(err)
	}
	if err := GenerateCerts(rogue, "localhost", nil); err != nil { // another CA
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := NewMemStore()
	db, cloudDB := echoServer(t, "db:"), echoServer(t, "cloud:")
	relayAddr, enrollAddr, org1 := freeAddr(t), freeAddr(t), freeAddr(t)
	r := NewRelay(RelayConfig{
		Listen: relayAddr, EnrollListen: enrollAddr,
		Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"),
		CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"), CertTTL: "3s",
		Orgs: map[string]string{"1": org1}, Cloud: cloudDB, OrgAllow: []string{"127.0.0.0/8"},
	}, store, log)
	go func() { _ = r.Run() }()
	waitFor(t, "relay up", func() bool {
		c, err := net.Dial("tcp", org1)
		if err == nil {
			c.Close()
		}
		return err == nil
	})
	session := func() string { return r.serialOf("1") }
	newAgent := func(state, ca, token string, enrollOnly bool, cloudListen string) *Agent {
		tf := filepath.Join(t.TempDir(), "token")
		if token != "" {
			if err := writeFile(tf, token); err != nil {
				t.Fatal(err)
			}
		}
		return NewAgent(AgentConfig{Relay: relayAddr, Enroll: enrollAddr, ServerName: "localhost", CA: ca,
			StateDir: state, TokenFile: tf, DB: db, CloudListen: cloudListen, EnrollOnly: enrollOnly}, log)
	}

	if msg := pgError(t, org1); !strings.Contains(msg, "not connected") {
		t.Fatalf("no agent yet: %q", msg)
	}
	if err := newAgent(t.TempDir(), filepath.Join(dir, "ca.crt"), "made-up-token", true, "").Run(); err == nil {
		t.Fatal("enrollment with an unknown token succeeded")
	}
	token := store.CreateToken("1")
	if err := newAgent(t.TempDir(), filepath.Join(rogue, "ca.crt"), token, true, "").Run(); err == nil {
		t.Fatal("agent trusted a relay certificate from another CA")
	}

	// Enrollment: the agent creates its key and gets a certificate.
	state := agentDir(t)
	cloudListen := freeAddr(t)
	agent := newAgent(state, filepath.Join(dir, "ca.crt"), token, false, cloudListen)
	go func() { _ = agent.Run() }()
	waitFor(t, "agent connected", func() bool { return session() != "" })
	first := agent.Serial()
	if session() != first || store.Count("enroll") != 1 {
		t.Fatalf("session serial %q, agent serial %q, enrollments %d", session(), first, store.Count("enroll"))
	}
	if got := roundTrip(t, org1); got != "db:ping" {
		t.Fatalf("cloud -> customer db: %q", got)
	}
	// The cloud database is closed to the organization until a move opens a window.
	if msg := pgError(t, cloudListen); !strings.Contains(msg, "not open to organization 1") {
		t.Fatalf("customer -> cloud db without a window: %q", msg)
	}
	store.SetCloud("1", true, "mover", "movedb") // the move's role and database (R3-13)
	cc, err := net.DialTimeout("tcp", cloudListen, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = cc.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = cc.Write(moverStartup())
	got, _ := io.ReadAll(cc)
	cc.Close()
	if !strings.HasPrefix(string(got), "cloud:") {
		t.Fatalf("customer -> cloud db: %q", got)
	}

	// The token is single use, even with the right CA and a fresh key.
	if err := newAgent(t.TempDir(), filepath.Join(dir, "ca.crt"), token, true, "").Run(); err == nil {
		t.Fatal("a used token enrolled again")
	}
	// A restarted agent reuses its stored certificate: no token needed.
	if err := newAgent(state, filepath.Join(dir, "ca.crt"), "", true, "").Run(); err != nil {
		t.Fatalf("restart with the stored certificate: %v", err)
	}

	// A certificate from the right CA that enrollment never issued is refused.
	static, err := tls.LoadX509KeyPair(filepath.Join(dir, "org-1.crt"), filepath.Join(dir, "org-1.key"))
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := loadPool(filepath.Join(dir, "ca.crt"))
	c, err := tls.Dial("tcp", relayAddr, &tls.Config{Certificates: []tls.Certificate{static}, RootCAs: pool, ServerName: "localhost"})
	if err == nil {
		sess, _ := yamux.Client(c, muxConfig())
		if _, perr := sess.Ping(); perr == nil {
			t.Fatal("relay accepted a certificate not issued by enrollment")
		}
		c.Close()
	}

	// Renewal: certificates live 3s; the agent replaces its own before expiry.
	waitFor(t, "renewal", func() bool { return store.Count("renew") >= 1 && agent.Serial() != first })
	time.Sleep(3500 * time.Millisecond) // the first certificate is now expired
	agent.Disconnect()
	// gone, then back: since renewals roll the session over, the serial alone does not tell
	// the closed session from the new one
	waitFor(t, "the dropped session gone", func() bool { return session() == "" })
	waitFor(t, "reconnect with the renewed certificate", func() bool { s := session(); return s != "" && s != first })
	if got := roundTrip(t, org1); got != "db:ping" {
		t.Fatalf("after renewal: %q", got)
	}
	if _, ok, _ := store.CertValid(context.Background(), first); ok {
		t.Fatal("the certificate a renewal replaced still opens sessions")
	}

	// Revocation: the live session is dropped and reconnections are refused.
	store.RevokeOrg("1")
	waitFor(t, "revoked session closed", func() bool { return session() == "" })
	time.Sleep(3 * time.Second)
	if session() != "" {
		t.Fatal("a revoked agent reconnected")
	}
	if msg := pgError(t, org1); !strings.Contains(msg, "not connected") {
		t.Fatalf("after revocation: %q", msg)
	}

	// A new token brings the organization back; the agent recognizes it as new.
	again := newAgent(state, filepath.Join(dir, "ca.crt"), store.CreateToken("1"), false, "")
	go func() { _ = again.Run() }()
	waitFor(t, "re-enrolled agent connected", func() bool { s := session(); return s != "" && s == again.Serial() })
	if store.Count("enroll") != 2 {
		t.Fatalf("enrollments: %d", store.Count("enroll"))
	}
}

// agentDir: a state directory for an agent that outlives its test (agents have no stop): it
// keeps renewing into it, so its removal is best effort, not t.TempDir's check that the
// directory empties.
func agentDir(t *testing.T) string {
	d, err := os.MkdirTemp("", "lokadb-agent-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return d
}

func TestAllowList(t *testing.T) {
	a, err := newAllowList([]string{"10.1.0.0/16", "192.168.5.7", "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	for addr, want := range map[string]bool{
		"10.1.200.3:5432": true, "10.2.0.1:5432": false, "192.168.5.7:1": true, "192.168.5.8:1": false,
		"127.0.0.1:9": true, // localhost, by name
	} {
		tcp, _ := net.ResolveTCPAddr("tcp", addr)
		if got := a.allows(tcp); got != want {
			t.Errorf("%s: allowed %v, want %v", addr, got, want)
		}
	}
	if e, _ := newAllowList(nil); !e.allows(&net.TCPAddr{IP: net.ParseIP("8.8.8.8")}) {
		t.Error("an empty list must allow everybody")
	}
	if _, err := newAllowList([]string{"10.0.0.0/99"}); err == nil {
		t.Error("a bad CIDR was accepted")
	}
}

// A renewal replaces the certificate it was requested with; revoking a certificate by its
// serial revokes the renewals made from it.
func TestRevokeChain(t *testing.T) {
	ctx := context.Background()
	m := NewMemStore()
	_ = m.RecordCert(ctx, "a", "1", time.Now(), "enroll", "", "")
	_ = m.RecordCert(ctx, "b", "1", time.Now(), "renew", "", "a")
	_ = m.RecordCert(ctx, "c", "1", time.Now(), "renew", "", "b")
	_ = m.RecordCert(ctx, "x", "1", time.Now(), "enroll", "", "")
	// a renewal keeps its parent valid until the new certificate is presented (review V-1)
	for serial, want := range map[string]bool{"a": true, "b": true, "c": true, "x": true} {
		if _, ok, _ := m.CertValid(ctx, serial); ok != want {
			t.Errorf("before use: %s valid %v, want %v", serial, ok, want)
		}
	}
	_ = m.Confirm(ctx, "b")
	_ = m.Confirm(ctx, "c")
	for serial, want := range map[string]bool{"a": false, "b": false, "c": true, "x": true} {
		if _, ok, _ := m.CertValid(ctx, serial); ok != want {
			t.Errorf("%s valid %v, want %v", serial, ok, want)
		}
	}
	m.RevokeSerial("a")
	if got, _ := m.Revoked(ctx, []string{"a", "b", "c", "x"}); len(got) != 3 {
		t.Errorf("revoked %v, want a, b and c (x is another enrollment)", got)
	}
}
