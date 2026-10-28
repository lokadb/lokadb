package tunnel

// Review 3 of the tunnel (tasks/review3/tunnel.md). Every test here proves one finding by
// showing the behaviour of main as it is: a test that passes means the weakness is still there.
// When a finding is fixed its test fails on purpose; turn the assertion around then.
//
// R3-1  TestReview3_RemoteIssuerFrozenAtCARotation fixed
// R3-2  TestReview3_RevokeRacesInFlightRenewal        (needs Postgres: LOKADB_REVIEW3_PG) fixed
// R3-3  TestReview3_ExpiredAgentNeverRecovers fixed
// R3-4  TestReview3_HandshakeSlotsStarvedByIdleConnections fixed in part
// R3-5  TestReview3_EnrollPortHasNoConnectionCeiling fixed
// R3-6  TestReview3_StalledClientsPinMuxWindows fixed in part
// R3-7  TestReview3_GraceReadmitsRevokedCertificate fixed
// R3-8  TestReview3_RelayCannotStartWithoutRegistry fixed
// R3-9  TestReview3_AgentSessionOutlivesSupersessionAndExpiry fixed
// R3-10 TestReview3_OrgTLSFailsOpenWhileRegistryIsDownAtStart fixed
// R3-11 TestReview3_NegativeMaxCloudStreamsCrashesRelay fixed

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/jackc/pgx/v5/pgproto3"
)

func r3read(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func r3write(t *testing.T, path string, b ...[]byte) {
	t.Helper()
	var all []byte
	for _, x := range b {
		all = append(all, x...)
	}
	if err := os.WriteFile(path, all, 0o600); err != nil {
		t.Fatal(err)
	}
}

// r3dial opens a tunnel session to the relay with cert, as an agent does, and pings it: a nil
// error means the relay admitted the session.
func (tr *testRelay) r3dial(cert tls.Certificate) (*yamux.Session, error) {
	pool, err := loadPool(filepath.Join(tr.dir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	c, err := tls.Dial("tcp", tr.relayAddr, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool,
		ServerName: "localhost", MinVersion: tls.VersionTLS13})
	if err != nil {
		return nil, err
	}
	s, err := yamux.Client(c, muxConfig())
	if err != nil {
		c.Close()
		return nil, err
	}
	if _, err := s.Ping(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

// R3-1. The relay with issuer_url builds its client of the issuing service once, in Run, from
// the relay certificate and the CA bundle of that moment (tunnel.go:239,247). The rotation of
// README (the section on CA rotation without a restart) reloads the bundle in the issuer and in the relay's
// listeners, but not here: after step 3 (the new CA signs) the issuer signs, the relay checks
// the answer against the old bundle (issuer.go:186) and every enrollment and every renewal
// ends with 500 "certificate signing failed" until the relay is restarted.
func TestReview3_RemoteIssuerFrozenAtCARotation(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	ca, caKey := filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key")
	issAddr := freeAddr(t)
	is := NewIssuer(IssuerConfig{Listen: issAddr, Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"),
		CA: ca, CAKey: caKey, CertTTL: "1h", Relays: []string{"localhost"}}, testLog())
	go func() { _ = is.Run() }()
	waitFor(t, "issuer up", func() bool {
		c, err := net.Dial("tcp", issAddr)
		if err == nil {
			c.Close()
		}
		return err == nil
	})
	_, port, _ := net.SplitHostPort(issAddr)
	url := "https://localhost:" + port
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: ca, IssuerURL: url}, nil)
	enroll := func(tr *testRelay) (int, enrollResponse) {
		return tr.post(t, "/v1/enroll", enrollRequest{Token: tr.store.CreateToken("1"), CSR: csr(t)})
	}
	if code, out := enroll(tr); code != http.StatusOK {
		t.Fatalf("before the rotation: %d %q", code, out.Error)
	}

	// README steps 1 and 3: both CAs in the bundle, then the new CA first with its key.
	next := t.TempDir()
	if err := GenerateCerts(next, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	ca1, ca2 := r3read(t, ca), r3read(t, filepath.Join(next, "ca.crt"))
	r3write(t, ca, ca1, ca2)
	r3write(t, caKey, r3read(t, filepath.Join(next, "ca.key")))
	r3write(t, ca, ca2, ca1)

	// fixed: the running relay takes the new bundle for its issuer client too
	if code, out := enroll(tr); code != http.StatusOK {
		t.Fatalf("the running relay after the rotation: %d %q", code, out.Error)
	}
	// a restarted relay signs through the same issuer with the same files
	tr2 := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: ca, IssuerURL: url}, nil)
	if code, out := enroll(tr2); code != http.StatusOK {
		t.Fatalf("a relay started after the rotation: %d %q", code, out.Error)
	}
}

// pauseStore stops a renewal between the signature and the record of its certificate.
type r3PauseStore struct {
	*PGStore
	arm           atomic.Bool
	hit, released chan struct{}
}

func (p *r3PauseStore) RecordCert(ctx context.Context, serial, org string, notAfter time.Time, kind, remote, parent string) error {
	if kind == "renew" && p.arm.CompareAndSwap(true, false) {
		close(p.hit)
		<-p.released
	}
	return p.PGStore.RecordCert(ctx, serial, org, notAfter, kind, remote, parent)
}

// R3-2. A renewal is checked (CertValid of the parent), signed and only then recorded
// (tunnel.go:500-517, enroll.go:240). Revoke (enroll.go:290) revokes the rows that exist when its
// UPDATE runs; the child recorded after it has revoked_at NULL and CertValid does not look at
// its parent, so the agent that was just revoked holds a valid certificate, opens a session and
// can renew again. An attacker with a stolen certificate keeps renewals in flight all the time.
func TestReview3_RevokeRacesInFlightRenewal(t *testing.T) {
	dsn := os.Getenv("LOKADB_REVIEW3_PG")
	if dsn == "" {
		t.Skip("LOKADB_REVIEW3_PG: a Postgres DSN for a scratch registry")
	}
	ctx := context.Background()
	pg, err := OpenPGStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	org := fmt.Sprintf("r3rv%d", time.Now().UnixNano())
	ps := &r3PauseStore{PGStore: pg, hit: make(chan struct{}), released: make(chan struct{})}
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{org: freeAddr(t)}, EnrollFailPerMin: -1}, func(*MemStore) Store { return ps })
	tok, err := pg.CreateToken(ctx, org, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	state := agentDir(t)
	if err := tr.agent(t, state, tok, echoServer(t, "db:"), true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	parent := st.leaf.SerialNumber.Text(16)

	// the renewal is in flight: signed, not yet recorded
	ps.arm.Store(true)
	key, csrPEM, err := newKeyAndCSR()
	if err != nil {
		t.Fatal(err)
	}
	type answer struct {
		code int
		out  enrollResponse
	}
	done := make(chan answer, 1)
	go func() {
		pool, _ := loadPool(filepath.Join(tr.dir, "ca.crt"))
		cl := &http.Client{Timeout: 20 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
			TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost", Certificates: []tls.Certificate{*st.cert}}}}
		body, _ := json.Marshal(enrollRequest{CSR: csrPEM})
		resp, err := cl.Post("https://"+tr.enroll+"/v1/renew", "application/json", strings.NewReader(string(body)))
		if err != nil {
			done <- answer{code: -1}
			return
		}
		defer resp.Body.Close()
		var out enrollResponse
		_ = json.NewDecoder(resp.Body).Decode(&out)
		done <- answer{resp.StatusCode, out}
	}()
	select {
	case <-ps.hit:
	case <-time.After(10 * time.Second):
		t.Fatal("the renewal never reached the registry")
	}

	// the operator revokes the agent: `lokadb revoke -org <org> -serial <parent>`
	certs, _, err := pg.Revoke(ctx, org, parent)
	if err != nil || certs != 1 {
		t.Fatalf("revoke: %d certificates, %v", certs, err)
	}
	close(ps.released)
	ans := <-done
	if ans.code != http.StatusOK {
		t.Fatalf("the in-flight renewal: %d %q", ans.code, ans.out.Error)
	}
	blk, _ := pem.Decode([]byte(ans.out.Cert))
	child, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	childSerial := child.SerialNumber.Text(16)
	if _, ok, _ := pg.CertValid(ctx, parent); ok {
		t.Fatal("the revoked parent is still valid")
	}
	// fixed: a renewal of a revoked certificate is revoked with it, whenever it was written
	if _, ok, err := pg.CertValid(ctx, childSerial); err != nil || ok {
		t.Fatalf("the child of a revoked certificate is valid (%v, %v)", ok, err)
	}
	if got, err := pg.Revoked(ctx, []string{childSerial}); err != nil || len(got) != 1 {
		t.Fatalf("Revoked of the child: %v %v", got, err)
	}
	cert, err := tls.X509KeyPair([]byte(ans.out.Cert), key)
	if err != nil {
		t.Fatal(err)
	}
	if sess, err := tr.r3dial(cert); err == nil {
		sess.Close()
		t.Fatal("the child of a revoked certificate opened a session")
	}

	// revoke -org with an enrollment in flight: its transaction began before the revocation,
	// it commits after; the barrier of org_revocations revokes what it writes
	if _, _, err := pg.Revoke(ctx, org, ""); err != nil {
		t.Fatal(err)
	}
	late := fmt.Sprintf("late%d", time.Now().UnixNano())
	if _, err := pg.db.Exec(ctx, `INSERT INTO lokadb_cp.agent_certs (serial, org, kind, not_after, issued_at)
		VALUES ($1, $2, 'enroll', now() + interval '1 hour', now() - interval '1 second')`, late, org); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := pg.CertValid(ctx, late); ok {
		t.Fatal("an enrollment begun before revoke -org and written after it is valid")
	}
	fresh := fmt.Sprintf("fresh%d", time.Now().UnixNano())
	if _, err := pg.db.Exec(ctx, `INSERT INTO lokadb_cp.agent_certs (serial, org, kind, not_after) VALUES ($1, $2, 'enroll', now() + interval '1 hour')`, fresh, org); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := pg.CertValid(ctx, fresh); !ok {
		t.Fatal("an enrollment after the revocation (a new token) is not valid")
	}
}

type r3FailRenew struct {
	*MemStore
	fail atomic.Bool
}

func (s *r3FailRenew) RecordCert(ctx context.Context, serial, org string, notAfter time.Time, kind, remote, parent string) error {
	if kind == "renew" && s.fail.Load() {
		return errors.New("registry down")
	}
	return s.MemStore.RecordCert(ctx, serial, org, notAfter, kind, remote, parent)
}

// R3-3. The agent renews at two thirds of the certificate's life and retries every 10 s
// (tunnel.go:1375-1394); the relay takes a renewal only with a certificate that is still valid
// (tunnel.go:493, TLS). One failed attempt on the last third (a registry or relay down for
// longer than a third of cert_ttl, README's example is 1 h) and the agent holds a certificate
// that expires; the session it has keeps running (by design), but the first reconnect after the
// expiry fails for good, renewals fail too, and the agent only logs. Only a new token and a
// restart bring it back.
func TestReview3_ExpiredAgentNeverRecovers(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a certificate to expire")
	}
	var fs *r3FailRenew
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CertTTL: "3s", EnrollFailPerMin: -1}, func(m *MemStore) Store {
		fs = &r3FailRenew{MemStore: m}
		return fs
	})
	fs.fail.Store(true) // the registry write of a renewal fails: the relay answers 500
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	first := a.Serial()

	time.Sleep(4 * time.Second) // the certificate (3 s) is over; the registry is back:
	fs.fail.Store(false)
	a.Disconnect() // one network blip
	// fixed: the relay renews a certificate that ended a short while ago (renew_expired), the
	// agent comes back by itself
	waitFor(t, "the agent renewed its ended certificate and reconnected", func() bool {
		return tr.store.Count("renew") > 0 && a.Serial() != first && tr.session("1") == a.Serial()
	})
}

// R3-3, the other half: a certificate that ended longer ago than renew_expired is not renewed,
// and the agent says so and stops with an error instead of running on without a tunnel.
func TestExpiredBeyondGraceStopsAgent(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a certificate to expire")
	}
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CertTTL: "3s", RenewExpired: "1s", EnrollFailPerMin: -1}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), echoServer(t, "db:"), true).Run(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second) // ended 2 s ago, renew_expired 1 s
	err := tr.agent(t, state, "", echoServer(t, "db:"), false).Run()
	if err == nil || !strings.Contains(err.Error(), "issue a new token") {
		t.Fatalf("an agent whose certificate ended beyond renew_expired: %v", err)
	}
}

// R3-4. max_handshakes (tunnel.go:591) is one counter for the whole agent port, with no share
// per source and no short deadline for a connection that has not started TLS: MaxHandshakes
// idle TCP connections hold every slot for 10 s each (tunnel.go:596), and while they are
// renewed no agent of any organization gets in.
func TestReview3_HandshakeSlotsStarvedByIdleConnections(t *testing.T) {
	// fixed, in part: an idle connection holds its place 5 s, not 10, and one host takes at most
	// maxHandshakesPerHost places; many hosts together can still fill max_handshakes for a while
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, MaxHandshakes: 64}, nil)
	from := &net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.2")}}
	var idle []net.Conn
	for i := 0; i < maxHandshakesPerHost; i++ {
		c, err := from.Dial("tcp", tr.relayAddr)
		if err != nil {
			t.Skipf("no 127.0.0.2 here: %v", err)
		}
		idle = append(idle, c)
	}
	defer func() {
		for _, c := range idle {
			c.Close()
		}
	}()
	time.Sleep(200 * time.Millisecond)
	extra, err := from.Dial("tcp", tr.relayAddr)
	if err != nil {
		t.Fatal(err)
	}
	_ = extra.SetDeadline(time.Now().Add(3 * time.Second))
	t0 := time.Now()
	if _, err := extra.Read(make([]byte, 1)); err == nil || time.Since(t0) > 2*time.Second {
		t.Fatalf("a handshake beyond the host's share: %v after %v", err, time.Since(t0))
	}
	extra.Close()
	// an agent from another host is not held up by them
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "an agent of another host gets in at once", func() bool { return tr.session("1") != "" })
	// and the idle ones are dropped after 5 s without being closed by their side
	_ = idle[0].SetDeadline(time.Now().Add(8 * time.Second))
	if _, err := idle[0].Read(make([]byte, 1)); err == nil {
		t.Fatal("an idle connection got an answer")
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("an idle connection was held past the handshake deadline")
	}
}

// R3-5. The ceilings added for the agent port do not exist on the enrollment port: it is an
// http.Server on a plain tls listener (tunnel.go:301-316) that takes every connection and keeps
// each one for the handshake timeout (5 s) and, after it, for ReadTimeout. The same flood on
// the agent port is closed at the ceiling.
func TestReview3_EnrollPortHasNoConnectionCeiling(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, MaxHandshakes: 2, MaxEnrollConns: 10}, nil)
	const n = 300
	flood := func(addr string) (held int) {
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for i := 0; i < n; i++ {
			c, err := net.Dial("tcp", addr)
			if err != nil {
				break
			}
			conns = append(conns, c)
		}
		time.Sleep(500 * time.Millisecond)
		for _, c := range conns {
			_ = c.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
			if _, err := c.Read(make([]byte, 1)); err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					held++ // still open on the relay's side
				}
			}
		}
		return held
	}
	agentPort, enrollPort := flood(tr.relayAddr), flood(tr.enroll)
	t.Logf("held open of %d idle connections: agent port %d (max_handshakes 2), enrollment port %d", n, agentPort, enrollPort)
	// fixed: the enrollment port holds at most max_enroll_conns
	if agentPort > 10 || enrollPort > 20 {
		t.Fatalf("agent port %d (max_handshakes 2), enrollment port %d (max_enroll_conns 10) of %d", agentPort, enrollPort, n)
	}
}

// R3-6. Each stream of the mux buffers up to its receive window (4 MiB, muxConfig) when the
// reader falls behind, and the number of streams is max_org_conns (1000 by default) per
// organization with no cap for the relay as a whole: a client that reads nothing pins the
// window of every connection it opens. N stalled connections hold about N x 4 MiB of heap.
func TestReview3_StalledClientsPinMuxWindows(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() { // a database that answers every connection with 16 MiB at once
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				chunk := make([]byte, 64<<10)
				for i := 0; i < 256; i++ {
					if _, err := c.Write(chunk); err != nil {
						return
					}
				}
			}()
		}
	}()
	// fixed in part: each stalled connection still pins its window, but the relay as a whole
	// takes at most max_streams of them; memory is about max_streams x the window
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, MaxStreams: 8}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), l.Addr().String(), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	var conns []net.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	for i := 0; i < 8; i++ {
		c, err := net.Dial("tcp", org1)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c) // never read
	}
	time.Sleep(500 * time.Millisecond)
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	startup, _ := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "u"}}).Encode(nil)
	_, _ = c.Write(startup)
	if b, _ := io.ReadAll(c); !strings.Contains(string(b), "too many connections open on the relay") {
		t.Fatalf("a connection beyond max_streams: %q", b)
	}
}

// R3-7. watchRevocations closes the session of a revoked certificate but does not forget the
// registry answer remembered for registry_grace (tunnel.go:827-831 skips it for revoked
// serials): until the grace is over a registry that goes down admits the certificate again, a
// certificate the relay itself had just cut.
func TestReview3_GraceReadmitsRevokedCertificate(t *testing.T) {
	var ds *downStore
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, RegistryGrace: "1m"}, func(m *MemStore) Store {
		ds = &downStore{MemStore: m}
		return ds
	})
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := tr.r3dial(*st.cert)
	if err != nil {
		t.Fatal(err)
	}
	tr.store.RevokeSerial(st.leaf.SerialNumber.Text(16))
	waitFor(t, "the relay cuts the revoked session", sess.IsClosed)
	// the holder of the revoked certificate does not try again while the registry is up (a try
	// would make the relay forget the answer: Relay.agent calls remember(serial, org, false));
	// it waits for the registry to go down
	ds.down.Store(true)
	// fixed: the relay forgets the answer of a certificate it cut for revocation
	if again, err := tr.r3dial(*st.cert); err == nil {
		again.Close()
		t.Fatal("registry_grace let a revoked certificate back in while the registry is down")
	}
}

// R3-8. main.go opens the registry before the relay starts (OpenPGStore applies the schema).
// A relay restarted while the registry is down does not start at all, even though its config
// file lists organizations and agents would reconnect on registry_grace: "a registry outage
// closes no port" (endpoints.go) holds only for a relay that was already running.
func TestReview3_RelayCannotStartWithoutRegistry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// a store for other commands still needs the registry
	if _, err := OpenPGStore(ctx, "postgres://u:p@127.0.0.1:1/db?connect_timeout=2"); err == nil {
		t.Fatal("a registry store opens without the registry")
	}
	// fixed: the relay's store opens without it, and its calls fail until the registry is there
	s, err := OpenPGStoreForRelay(ctx, "postgres://u:p@127.0.0.1:1/db?connect_timeout=2", testLog())
	if err != nil {
		t.Fatalf("the relay's store without the registry: %v", err)
	}
	defer s.Close()
	if _, _, err := s.CertValid(ctx, "x"); err == nil {
		t.Fatal("a registry call without the registry succeeded")
	}
}

// R3-9. A live session is never cut by supersession or by expiry (README: a live session is not cut
// when its certificate expires; watchRevocations asks Revoked only). The thief of a key who has a
// session keeps it for as long as the TCP connection lives, however short cert_ttl is, and
// whatever the owner's renewals did to the certificate; only revocation ends it.
func TestReview3_AgentSessionOutlivesSupersessionAndExpiry(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a certificate to expire")
	}
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CertTTL: "3s", EnrollFailPerMin: -1,
		superseded: time.Second}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	stolen := *st.cert
	sess, err := tr.r3dial(stolen) // the thief's session
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	// the owner renews and presents the new certificate: the stolen one is superseded
	code, out := tr.renewWith(t, stolen)
	if code != http.StatusOK {
		t.Fatalf("renewal: %d %q", code, out.Error)
	}
	b, _ := pem.Decode([]byte(out.Cert))
	child, _ := x509.ParseCertificate(b.Bytes)
	if err := tr.store.Confirm(context.Background(), child.SerialNumber.Text(16)); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := tr.store.CertValid(context.Background(), st.leaf.SerialNumber.Text(16)); ok {
		t.Fatal("the stolen certificate is still valid")
	}
	time.Sleep(6 * time.Second) // twice the certificate's life
	if _, err := tr.r3dial(stolen); err == nil {
		t.Fatal("the superseded and expired certificate opened a new session")
	}
	// fixed: the relay closes a session on a certificate its agent replaced (and on one that
	// ended longer ago than renew_expired)
	if !sess.IsClosed() && tr.session("1") != "" {
		t.Fatal("the session of a superseded, expired certificate still runs")
	}
}

// R3-9, the agent's side: after a renewal the agent opens a session on the new certificate
// and lets the old one finish its streams: a long connection survives the renewal, and the
// relay's session for the organization is on the new certificate.
func TestRenewalRollsSessionOver(t *testing.T) {
	const nl = "\n"
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, CertTTL: "3s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	first := tr.session("1")
	long, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer long.Close()
	say := func(what string) {
		t.Helper()
		_ = long.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := long.Write([]byte(what + nl)); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		buf := make([]byte, 64)
		n, err := long.Read(buf)
		if err != nil || string(buf[:n]) != "db:"+what+nl {
			t.Fatalf("%s: %q %v", what, buf[:n], err)
		}
	}
	say("before")
	waitFor(t, "a session on the renewed certificate", func() bool { s := tr.session("1"); return s != "" && s != first })
	say("after the renewal") // the old session still carries it
	if got := roundTrip(t, org1); got != "" && !strings.HasPrefix(got, "db:") {
		t.Fatalf("a new connection after the renewal: %q", got)
	}
}

type r3EndpointsDown struct {
	*MemStore
	down atomic.Bool
}

func (s *r3EndpointsDown) Endpoints(ctx context.Context) (map[string]Endpoint, error) {
	if s.down.Load() {
		return nil, errors.New("registry down")
	}
	return s.MemStore.Endpoints(ctx)
}

// R3-10. The per-organization "tls": "require" lives in the registry only (relay_orgs.tls); the
// relay reads it in wantedEndpoints (endpoints.go:44-58). An organization that is also listed
// in relay.json "orgs" is served from the file when the registry does not answer, and until it
// does tlsOrgs is empty: tlsRequired is false and the endpoint passes clear-text connections,
// startup message and password included, to the organization's database, for as long as the
// registry's answer to this one query fails (a relay restarted in a registry outage, or a
// registry that is slow or fails only this query).
func TestReview3_OrgTLSFailsOpenWhileRegistryIsDownAtStart(t *testing.T) {
	org1 := freeAddr(t)
	var rs *r3EndpointsDown
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, poll: 100 * time.Millisecond}, func(m *MemStore) Store {
		rs = &r3EndpointsDown{MemStore: m}
		rs.down.Store(true)
		return rs
	})
	tr.store.SetEndpoint("1", Endpoint{Listen: org1, TLS: "require"}) // what the registry says
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	plain := func() string { // eight clear-text bytes that are not an SSLRequest
		c, err := net.DialTimeout("tcp", org1, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = c.Write([]byte("pingpong"))
		b, _ := io.ReadAll(c)
		return string(b)
	}
	// fixed: until the registry says, the organization's connections are refused
	if got := plain(); strings.Contains(got, "db:") {
		t.Fatalf("clear text reached the organization's database while the registry is down: %q", got)
	}
	rs.down.Store(false)
	waitFor(t, "the registry answers", func() bool { return tr.r.tlsRequired("1") })
	if got := plain(); strings.Contains(got, "db:") {
		t.Fatalf("clear text still passes with the registry up: %q", got)
	}
}

// R3-11. agent() makes the cloud-stream semaphore with make(chan struct{}, MaxCloudStreams)
// (tunnel.go:696). NewRelay maps 0 to the default but leaves a negative value, which the
// neighbouring options (max_org_conns, max_handshakes, enroll_fail_per_min) read as "no
// limit": the panic is in a goroutine, so the first agent that connects ends the relay process
// (every organization with it). Nothing validates the config at start.
func TestReview3_NegativeMaxCloudStreamsCrashesRelay(t *testing.T) {
	if os.Getenv("LOKADB_R3_CHILD") == "1" {
		tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, MaxCloudStreams: -1}, nil)
		a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
		go func() { _ = a.Run() }()
		waitFor(t, "agent", func() bool { return tr.session("1") != "" })
		time.Sleep(time.Second)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestReview3_NegativeMaxCloudStreamsCrashesRelay$")
	cmd.Env = append(os.Environ(), "LOKADB_R3_CHILD=1")
	out, err := cmd.CombinedOutput()
	// fixed: -1 is no limit, as for the other ceilings
	if err != nil || strings.Contains(string(out), "makechan") {
		t.Fatalf("max_cloud_streams -1: %v\n%.600s", err, out)
	}
}

// R3-14: an agent's -ca file had only the old CA, and nothing brought it the new one: after
// step 2 of the rotation (a relay certificate of the new CA) it lost its tunnel. Now the relay
// sends its bundle with every certificate and the agent keeps it in its state.
func TestRotatedCAReachesAgent(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CertTTL: "3s"}, nil)
	own := filepath.Join(t.TempDir(), "ca.crt") // the agent's -ca: the old CA only
	r3write(t, own, r3read(t, filepath.Join(tr.dir, "ca.crt")))
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	a.cfg.CA = own
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	next := t.TempDir()
	if err := GenerateCerts(next, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	ca1, ca2 := r3read(t, filepath.Join(tr.dir, "ca.crt")), r3read(t, filepath.Join(next, "ca.crt"))
	r3write(t, filepath.Join(tr.dir, "ca.crt"), ca1, ca2) // 1. both CAs on the relay
	first := a.Serial()
	waitFor(t, "a renewal brings the bundle", func() bool { return a.Serial() != first })
	r3write(t, filepath.Join(tr.dir, "relay.key"), r3read(t, filepath.Join(next, "relay.key"))) // 2. the relay certificate of the new CA
	r3write(t, filepath.Join(tr.dir, "relay.crt"), r3read(t, filepath.Join(next, "relay.crt")))
	switched := time.Now()
	a.Disconnect()
	waitFor(t, "the agent connects to the relay of the new CA", func() bool {
		for _, si := range tr.r.Sessions() {
			if !si.Retiring && si.Since.After(switched) {
				return true // a TLS handshake against the new relay certificate succeeded
			}
		}
		return false
	})
	if got := r3read(t, own); string(got) != string(ca1) {
		t.Fatal("the agent's -ca file was changed")
	}
}

// R3-16: the agent had no stop: SIGTERM cut every query. Drain lets an open connection finish
// and takes no new one.
func TestAgentDrain(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	open, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	_ = open.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = open.Write([]byte("a\n"))
	buf := make([]byte, 16)
	if n, err := open.Read(buf); err != nil || string(buf[:n]) != "db:a\n" {
		t.Fatalf("before: %q %v", buf[:n], err)
	}
	done := make(chan struct{})
	go func() { a.Drain(3 * time.Second); close(done) }()
	time.Sleep(300 * time.Millisecond)
	_, _ = open.Write([]byte("b\n")) // the open one still works while draining
	if n, err := open.Read(buf); err != nil || string(buf[:n]) != "db:b\n" {
		t.Fatalf("while draining: %q %v", buf[:n], err)
	}
	if msg := pgError(t, org1); !strings.Contains(msg, "not connected") && !strings.Contains(msg, "failed") {
		t.Fatalf("a new connection while the agent drains: %q", msg)
	}
	<-done
}

// R3-17: the registry knew a certificate by its serial alone: whoever could sign (the CA key,
// the issuing service) made one with a known serial and CN=org-1 and the relay let it in. Now
// the session must present the key the serial was issued with.
func TestForgedSerialRefused(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	iss, err := loadIssuer(filepath.Join(tr.dir, "ca.crt"), filepath.Join(tr.dir, "ca.key"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := *st.leaf // the same serial, subject and dates, another key
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, iss.ca, &key.PublicKey, iss.key)
	if err != nil {
		t.Fatal(err)
	}
	forged := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	if s, err := tr.r3dial(forged); err == nil {
		s.Close()
		t.Fatal("a certificate with a known serial and another key opened a session")
	}
	if s, err := tr.r3dial(*st.cert); err != nil {
		t.Fatalf("the real one: %v", err)
	} else {
		s.Close()
	}
}

// R3-18: a certificate renewed without limit, each renewal a signature and a registry row.
// Now a certificate renews once a third of its life is gone, at most maxRenewalsPerCert times.
func TestRenewalRate(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CertTTL: "9s", EnrollFailPerMin: -1, strictRenewTest: true}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := tr.renewWith(t, *st.cert); code != http.StatusTooManyRequests {
		t.Fatalf("a renewal right after the enrollment: %d", code)
	}
	time.Sleep(3500 * time.Millisecond) // a third of 9 s (and the 0.9 s it starts earlier for clock skew)
	for i := 0; i < maxRenewalsPerCert; i++ {
		if code, out := tr.renewWith(t, *st.cert); code != http.StatusOK {
			t.Fatalf("renewal %d: %d %q", i+1, code, out.Error)
		}
	}
	if code, _ := tr.renewWith(t, *st.cert); code != http.StatusTooManyRequests {
		t.Fatalf("renewal %d of one certificate: %d", maxRenewalsPerCert+1, code)
	}
}

// R3-19: nothing showed which agents of an organization are connected now. GET /sessions on
// the health port lists them.
func TestSessionsListed(t *testing.T) {
	health := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, HealthListen: health}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	resp, err := http.Get("http://" + health + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []SessionInfo
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil || len(got) != 1 || got[0].Org != "1" || got[0].Serial != tr.session("1") {
		t.Fatalf("sessions: %+v %v", got, err)
	}
}

// R3-12: without org_allow the relay served its organizations' ports to whoever reached them
// (an error in the log). Now it does not start.
func TestOrgAllowRequired(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	r := NewRelay(RelayConfig{Listen: freeAddr(t), EnrollListen: freeAddr(t), Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"),
		CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"), Orgs: map[string]string{"1": freeAddr(t)}}, NewMemStore(), testLog())
	done := make(chan error, 1)
	go func() { done <- r.Run() }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "org_allow is required") {
			t.Fatalf("a relay without org_allow: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a relay without org_allow started")
	}
}

// Block 8 (cloud metrics): /sessions names the agent's version, which the agent tells in its
// ALPN name; an agent that tells none (older) is listed without one, and the ALPN name of an
// older relay's format still parses.
func TestSessionsShowAgentVersion(t *testing.T) {
	health := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, HealthListen: health}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	a.cfg.Version = "0.9.1-test"
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	resp, err := http.Get("http://" + health + "/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []SessionInfo
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil || len(got) != 1 || got[0].Version != "0.9.1-test" {
		t.Fatalf("sessions: %+v %v", got, err)
	}
	for _, c := range []struct{ proto, set, slot, version string }{
		{"lokadb-s/ab12/3/0.9.1-test", "ab12", "3", "0.9.1-test"},
		{"lokadb-s/ab12/3", "ab12", "3", ""},
		{"h2", "", "", ""},
	} {
		set, slot, v := parseSet(c.proto)
		if set != c.set || slot != c.slot || v != c.version {
			t.Errorf("parseSet(%q) = %q %q %q", c.proto, set, slot, v)
		}
	}
	if p := setProto("ab12", 3, "v 1/2"); p != "lokadb-s/ab12/3/v12" {
		t.Errorf("a version with a space and a slash: %q", p)
	}
}

// Block 8 (cloud metrics): the agent's /status names its state, relay, certificate and the time
// to its database; it posts the same JSON to the service with its secret, and stops when the
// service refuses the secret.
func TestAgentStatus(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}}, nil)
	db := echoServer(t, "db:")
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), db, false)
	a.cfg.Version = "0.9.2-test"
	a.cfg.StatusListen = freeAddr(t)
	got := make(chan AgentStatus, 4)
	auth := make(chan string, 4)
	svc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var st AgentStatus
		_ = json.NewDecoder(r.Body).Decode(&st)
		auth <- r.Header.Get("Authorization")
		got <- st
		_, _ = w.Write([]byte(`{"notices":[],"interval_s":30}`))
	}))
	defer svc.Close()
	tf := filepath.Join(t.TempDir(), "las")
	if err := os.WriteFile(tf, []byte("las_test\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	a.cfg.StatusURL, a.cfg.StatusTokenFile = svc.URL, tf
	a.reportEvery = 200 * time.Millisecond
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	var st AgentStatus
	waitFor(t, "status connected with the database probed", func() bool {
		resp, err := http.Get("http://" + a.cfg.StatusListen + "/status")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		st = AgentStatus{}
		return json.NewDecoder(resp.Body).Decode(&st) == nil && st.State == "connected" && st.DB.OK && st.Org == "1"
	})
	if st.Version != "0.9.2-test" || len(st.Relays) != 1 || st.Relays[0].Sessions != 1 || st.CertEnds.IsZero() {
		t.Fatalf("status: %+v", st)
	}
	select {
	case h := <-auth:
		if h != "Bearer las_test" {
			t.Fatalf("authorization %q", h)
		}
		if p := <-got; p.State != "connected" || p.Org != "1" {
			t.Fatalf("posted: %+v", p)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no status posted")
	}
}
