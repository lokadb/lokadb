package tunnel

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

type testRelay struct {
	r                 *Relay
	store             *MemStore
	dir               string
	relayAddr, enroll string
	log               *slog.Logger
}

// startRelay runs a relay over a MemStore (wrapped by wrap, if given) with cfg's Orgs.
func startRelay(t *testing.T, cfg RelayConfig, wrap func(*MemStore) Store) *testRelay {
	t.Helper()
	dir := filepath.Dir(cfg.CA) // certificates made by the caller (cfg.CA set), or new ones
	if cfg.CA == "" {
		dir = t.TempDir()
		if err := GenerateCerts(dir, "localhost", nil); err != nil {
			t.Fatal(err)
		}
	}
	tr := &testRelay{store: NewMemStore(), dir: dir, relayAddr: freeAddr(t), enroll: freeAddr(t),
		log: testLog()}
	cfg.Listen, cfg.EnrollListen = tr.relayAddr, tr.enroll
	if len(cfg.OrgAllow) == 0 {
		cfg.OrgAllow = []string{"127.0.0.0/8"} // required; the tests connect from loopback
	}
	cfg.lenientRenew = !cfg.strictRenewTest // the tests renew at will; TestRenewalRate checks the rule
	cfg.Cert, cfg.Key = filepath.Join(dir, "relay.crt"), filepath.Join(dir, "relay.key")
	cfg.CA = filepath.Join(dir, "ca.crt")
	if cfg.IssuerURL == "" {
		cfg.CAKey = filepath.Join(dir, "ca.key")
	}
	var store Store = tr.store
	if wrap != nil {
		store = wrap(tr.store)
	}
	tr.r = NewRelay(cfg, store, tr.log)
	go func() { _ = tr.r.Run() }()
	waitFor(t, "enrollment API up", func() bool {
		c, err := net.Dial("tcp", tr.enroll)
		if err == nil {
			c.Close()
		}
		return err == nil
	})
	return tr
}

func (tr *testRelay) agent(t *testing.T, state, token, db string, enrollOnly bool) *Agent {
	tf := filepath.Join(t.TempDir(), "token")
	if token != "" {
		if err := writeFile(tf, token); err != nil {
			t.Fatal(err)
		}
	}
	return NewAgent(AgentConfig{Relay: tr.relayAddr, Enroll: tr.enroll, ServerName: "localhost", CA: filepath.Join(tr.dir, "ca.crt"),
		StateDir: state, TokenFile: tf, DB: db, EnrollOnly: enrollOnly}, tr.log)
}

// post calls the enrollment API directly and returns the status and the answer.
func (tr *testRelay) post(t *testing.T, path string, req enrollRequest) (int, enrollResponse) {
	t.Helper()
	pool, err := loadPool(filepath.Join(tr.dir, "ca.crt"))
	if err != nil {
		t.Fatal(err)
	}
	cl := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"}, DisableKeepAlives: true}}
	body, _ := json.Marshal(req)
	resp, err := cl.Post("https://"+tr.enroll+path, "application/json", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out enrollResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (tr *testRelay) session(org string) string { return tr.r.serialOf(org) }

// serialOf returns the certificate serials of the organization's sessions, sorted and
// comma-separated ("" without one).
func (r *Relay) serialOf(org string) string {
	var s []string
	for _, as := range r.agentsOf(org) {
		s = append(s, as.serial)
	}
	sort.Strings(s)
	return strings.Join(s, ",")
}

func csr(t *testing.T) string {
	_, c, err := newKeyAndCSR()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// lineEcho answers every line with prefix+line, on the same connection, until it closes.
func lineEcho(t *testing.T, prefix string) string {
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
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					if _, err := c.Write([]byte(prefix + sc.Text() + "\n")); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

// Item 8 and 10: a refusal for a reason that is not the token's (an organization this relay
// does not serve, a bad CSR) leaves the token usable, and the CSR error the client gets is
// generic.
func TestEnrollRefusalsKeepToken(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, EnrollFailPerMin: -1}, nil)
	tok := tr.store.CreateToken("9") // not served yet
	code, out := tr.post(t, "/v1/enroll", enrollRequest{Token: tok, CSR: csr(t)})
	if code != http.StatusForbidden || !strings.Contains(out.Error, "not served") {
		t.Fatalf("organization not served: %d %q", code, out.Error)
	}
	if !tr.store.TokenUsable(tok) {
		t.Fatal("a token for an organization the relay does not serve was burned")
	}

	// A CSR whose signature does not match its key: the token is not used, the reason stays in the log.
	blk, _ := pem.Decode([]byte(csr(t)))
	blk.Bytes[len(blk.Bytes)-1] ^= 1 // the last byte of the DER belongs to the signature
	tok1 := tr.store.CreateToken("1")
	code, out = tr.post(t, "/v1/enroll", enrollRequest{Token: tok1, CSR: string(pem.EncodeToMemory(blk))})
	if code != http.StatusBadRequest || out.Error != errBadCSR.Error() {
		t.Fatalf("bad CSR: %d %q, want 400 %q", code, out.Error, errBadCSR.Error())
	}
	if !tr.store.TokenUsable(tok1) {
		t.Fatal("a request with a bad CSR burned the token")
	}
	code, out = tr.post(t, "/v1/enroll", enrollRequest{Token: tok1, CSR: "not a csr"})
	if code != http.StatusBadRequest || out.Error != errBadCSR.Error() {
		t.Fatalf("not a CSR: %d %q", code, out.Error)
	}

	// The organization gets an endpoint: the same token now enrolls.
	tr.store.SetEndpoint("9", Endpoint{Listen: freeAddr(t)})
	waitFor(t, "organization 9 served", func() bool { return tr.r.serves("9") })
	if code, out = tr.post(t, "/v1/enroll", enrollRequest{Token: tok, CSR: csr(t)}); code != http.StatusOK || out.Org != "9" {
		t.Fatalf("enrollment after the endpoint appeared: %d %q", code, out.Error)
	}
}

// Item 6: failed attempts are limited per host; the registry is not asked once the limit is hit.
func TestEnrollFailureLimit(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, EnrollFailPerMin: 3}, nil)
	for i := 0; i < 3; i++ {
		if code, _ := tr.post(t, "/v1/enroll", enrollRequest{Token: "guess", CSR: csr(t)}); code != http.StatusForbidden {
			t.Fatalf("attempt %d: %d", i, code)
		}
	}
	code, out := tr.post(t, "/v1/enroll", enrollRequest{Token: tr.store.CreateToken("1"), CSR: csr(t)})
	if code != http.StatusTooManyRequests {
		t.Fatalf("after 3 failures: %d %q, want 429", code, out.Error)
	}
	if n := tr.store.Lookups(); n != 3 {
		t.Fatalf("the registry was asked %d times, want 3", n)
	}
	// the bucket refills: a third of a minute later one more attempt is allowed
	tr.r.limit.mu.Lock()
	tr.r.limit.now = func() time.Time { return time.Now().Add(21 * time.Second) }
	tr.r.limit.mu.Unlock()
	if code, _ := tr.post(t, "/v1/enroll", enrollRequest{Token: tr.store.CreateToken("1"), CSR: csr(t)}); code != http.StatusOK {
		t.Fatalf("after the refill: %d", code)
	}
}

type hangStore struct {
	*MemStore
	hang atomic.Bool
}

func (h *hangStore) CertValid(ctx context.Context, serial string) (string, bool, error) {
	if h.hang.Load() {
		<-ctx.Done()
		return "", false, ctx.Err()
	}
	return h.MemStore.CertValid(ctx, serial)
}

// Item 9: a registry that does not answer does not hold an agent connection forever.
func TestAgentHandshakeRegistryTimeout(t *testing.T) {
	var hs *hangStore
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, registryTimeout: 300 * time.Millisecond}, func(m *MemStore) Store {
		hs = &hangStore{MemStore: m}
		return hs
	})
	state := t.TempDir()
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := loadPool(filepath.Join(tr.dir, "ca.crt"))
	hs.hang.Store(true)
	c, err := tls.Dial("tcp", tr.relayAddr, &tls.Config{Certificates: []tls.Certificate{*st.cert}, RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = c.Read(make([]byte, 1))
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("the relay still holds the connection 3s after the registry stopped answering")
	}
}

// 1.5: organizations come and go without a restart; the others' connections survive.
func TestOrgEndpointsWithoutRestart(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "relay.json")
	org1 := freeAddr(t)
	if err := os.WriteFile(cfgPath, []byte(`{"orgs": {"1": "`+org1+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, ConfigPath: cfgPath, poll: 100 * time.Millisecond}, nil)
	db1, db2 := lineEcho(t, "db1:"), lineEcho(t, "db2:")
	a1 := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), db1, false)
	go func() { _ = a1.Run() }()
	waitFor(t, "agent 1 connected", func() bool { return tr.session("1") != "" })

	// a long-lived connection of organization 1, used before, during and after the changes
	live, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	rd := bufio.NewReader(live)
	say := func(what string) {
		t.Helper()
		_ = live.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := live.Write([]byte(what + "\n")); err != nil {
			t.Fatalf("organization 1's live connection, %s: %v", what, err)
		}
		if got, err := rd.ReadString('\n'); err != nil || got != "db1:"+what+"\n" {
			t.Fatalf("organization 1's live connection, %s: %q %v", what, got, err)
		}
	}
	say("before")

	// organization 2 through the registry
	org2 := freeAddr(t)
	tr.store.SetEndpoint("2", Endpoint{Listen: org2})
	waitFor(t, "organization 2 served", func() bool { return tr.r.serves("2") })
	a2 := tr.agent(t, agentDir(t), tr.store.CreateToken("2"), db2, false)
	go func() { _ = a2.Run() }()
	waitFor(t, "agent 2 connected", func() bool { return tr.session("2") != "" })
	c2, err := net.Dial("tcp", org2)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c2.Write([]byte("hi\n"))
	_ = c2.SetDeadline(time.Now().Add(3 * time.Second))
	if got, _ := bufio.NewReader(c2).ReadString('\n'); got != "db2:hi\n" {
		t.Fatalf("organization 2: %q", got)
	}
	c2.Close()
	say("after org 2 joined")

	// organization 3 through the config file
	org3 := freeAddr(t)
	if err := os.WriteFile(cfgPath, []byte(`{"orgs": {"1": "`+org1+`", "3": "`+org3+`"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "organization 3 served", func() bool { return tr.r.serves("3") })
	if msg := pgError(t, org3); !strings.Contains(msg, "organization 3 is not connected") {
		t.Fatalf("organization 3 without an agent: %q", msg)
	}
	say("after org 3 joined")

	// removing organization 2 closes its port and its tunnel, and nothing else
	tr.store.SetEndpoint("2", Endpoint{})
	waitFor(t, "organization 2 closed", func() bool {
		c, err := net.Dial("tcp", org2)
		if err == nil {
			c.Close()
		}
		return err != nil && tr.session("2") == ""
	})
	say("after org 2 left")

	// a broken config file keeps the last good list
	if err := os.WriteFile(cfgPath, []byte(`{"orgs": `), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if !tr.r.serves("3") || !tr.r.serves("1") {
		t.Fatal("an unreadable config file closed endpoints")
	}
	say("after a broken config file")
	if s := tr.session("1"); s != a1.Serial() {
		t.Fatalf("organization 1's session changed: %q, agent %q", s, a1.Serial())
	}
}

// The token hash lives in the key file: one rename replaces the key, the certificate and
// the hash together. A state written by an older agent (token.sha256) still loads.
// The operator starts the agent right after lokadb org-endpoint; the relay opens the port on its
// next poll. The agent waits for it with the token still valid (e2e on a fast machine: the
// agent was ahead of the poll by 0.8 s and exited).
func TestAgentWaitsForOrgEndpoint(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{}, poll: 100 * time.Millisecond}, nil)
	tok := tr.store.CreateToken("3")
	db := lineEcho(t, "db3:")

	// enroll-only does not wait: it reports at once, the token is kept
	t0 := time.Now()
	if err := tr.agent(t, agentDir(t), tok, db, true).Run(); err == nil || !errors.Is(err, errNotServed) {
		t.Fatalf("enroll-only for an organization not served: %v", err)
	}
	if time.Since(t0) > 2*time.Second {
		t.Fatalf("enroll-only waited %v", time.Since(t0))
	}

	a := tr.agent(t, agentDir(t), tok, db, false)
	a.cfg.EnrollWait = 10 * time.Second
	done := make(chan error, 1)
	go func() { done <- a.Run() }()
	time.Sleep(1500 * time.Millisecond) // the agent has been refused at least once
	select {
	case err := <-done:
		t.Fatalf("the agent gave up before the endpoint existed: %v", err)
	default:
	}
	org3 := freeAddr(t)
	tr.store.SetEndpoint("3", Endpoint{Listen: org3})
	waitFor(t, "agent connected once the endpoint appeared", func() bool { return tr.session("3") != "" })
	select {
	case err := <-done:
		t.Fatalf("agent stopped: %v", err)
	default:
	}
}

func TestAgentStateOneFile(t *testing.T) {
	dir := t.TempDir()
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}}, nil)
	if err := tr.agent(t, dir, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "token.sha256")); err == nil {
		t.Fatal("the token hash is still a separate file")
	}
	st, err := loadState(dir)
	if err != nil || st.tokenHash == "" || st.cert == nil {
		t.Fatalf("state: %+v %v", st, err)
	}
	pair, _ := os.ReadFile(filepath.Join(dir, "agent.pem"))
	keyPEM := pair[strings.Index(string(pair), "-----BEGIN EC PRIVATE KEY"):]
	keyPEM = keyPEM[:strings.Index(string(keyPEM), "-----END EC PRIVATE KEY-----")+len("-----END EC PRIVATE KEY-----\n")]
	certPEM := pair[:strings.Index(string(pair), "-----BEGIN EC PRIVATE KEY")]
	// a renewal (tokenHash "") keeps the hash
	if err := saveState(dir, keyPEM, certPEM, ""); err != nil {
		t.Fatal(err)
	}
	if st2, _ := loadState(dir); st2.tokenHash != st.tokenHash {
		t.Fatalf("renewal lost the token hash: %q", st2.tokenHash)
	}
	// an older agent's state
	old := t.TempDir()
	_ = os.WriteFile(filepath.Join(old, "agent.pem"), append(append([]byte{}, certPEM...), keyPEM...), 0o600)
	_ = os.WriteFile(filepath.Join(old, "token.sha256"), []byte("abc\n"), 0o600)
	if st3, err := loadState(old); err != nil || st3.tokenHash != "abc" {
		t.Fatalf("older state: %q %v", st3.tokenHash, err)
	}
}

// fakeDB answers an SSLRequest with answer; with 'S' it then speaks TLS and echoes lines.
func fakeDB(t *testing.T, answer byte, cert tls.Certificate) string {
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
				head := make([]byte, 8)
				if _, err := io.ReadFull(c, head); err != nil {
					return
				}
				_, _ = c.Write([]byte{answer})
				if answer != 'S' {
					return
				}
				tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}})
				sc := bufio.NewScanner(tc)
				for sc.Scan() {
					_, _ = tc.Write([]byte("tls:" + sc.Text() + "\n"))
				}
			}()
		}
	}()
	return l.Addr().String()
}

var sslRequest = []byte{0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f}

// S8: with TLS required (here by the organizations' registry entries; org_tls "require"
// does it for all) only TLS to the organization's database crosses the relay.
func TestOrgTLSRequire(t *testing.T) {
	org1, org2 := freeAddr(t), freeAddr(t)
	tr := startRelay(t, RelayConfig{poll: 100 * time.Millisecond}, nil)
	tr.store.SetEndpoint("1", Endpoint{Listen: org1, TLS: "require"})
	tr.store.SetEndpoint("2", Endpoint{Listen: org2, TLS: "require"})
	waitFor(t, "organizations served", func() bool { return tr.r.serves("1") && tr.r.serves("2") && tr.r.tlsRequired("2") })
	cert, err := tls.LoadX509KeyPair(filepath.Join(tr.dir, "relay.crt"), filepath.Join(tr.dir, "relay.key"))
	if err != nil {
		t.Fatal(err)
	}
	for org, db := range map[string]string{"1": fakeDB(t, 'S', cert), "2": fakeDB(t, 'N', cert)} {
		a := tr.agent(t, agentDir(t), tr.store.CreateToken(org), db, false)
		go func() { _ = a.Run() }()
		waitFor(t, "agent "+org, func() bool { return tr.session(org) != "" })
	}

	// a client that starts in clear is refused before anything reaches the agent
	if msg := pgError(t, org1); !strings.Contains(msg, "accepts only TLS") {
		t.Fatalf("clear startup: %q", msg)
	}

	// TLS end to end: the relay forwards the SSLRequest, the database says 'S'
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write(sslRequest)
	ans := make([]byte, 1)
	if _, err := io.ReadFull(c, ans); err != nil || ans[0] != 'S' {
		t.Fatalf("SSLRequest answer: %q %v", ans, err)
	}
	tc := tls.Client(c, &tls.Config{InsecureSkipVerify: true})
	_, _ = tc.Write([]byte("ping\n"))
	if got, err := bufio.NewReader(tc).ReadString('\n'); err != nil || got != "tls:ping\n" {
		t.Fatalf("through TLS: %q %v", got, err)
	}

	// a database that does not offer TLS: the client asked for it and is refused, not downgraded
	c2, err := net.Dial("tcp", org2)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	_ = c2.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c2.Write(sslRequest)
	if _, err := io.ReadFull(c2, ans); err != nil || ans[0] != 'N' {
		t.Fatalf("SSLRequest answer from a database without TLS: %q %v", ans, err)
	}
	fe := pgproto3.NewFrontend(c2, c2)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "app"}})
	_ = fe.Flush()
	m, err := fe.Receive()
	if e, ok := m.(*pgproto3.ErrorResponse); !ok || !strings.Contains(e.Message, "does not offer TLS") {
		t.Fatalf("clear startup after a declined SSLRequest: %#v %v", m, err)
	}
}

// Item 3: several agents of one organization share its streams; a reconnection replaces only
// the same agent's session; max_agents_per_org caps them.
func TestSeveralAgentsPerOrg(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, MaxAgentsPerOrg: 2}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "a:"), false)
	b := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "b:"), false)
	go func() { _ = a.Run() }()
	go func() { _ = b.Run() }()
	waitFor(t, "two agents connected", func() bool { return strings.Count(tr.session("1"), ",") == 1 })

	type conn struct {
		c  net.Conn
		rd *bufio.Reader
	}
	ask := func(c conn, what string) (string, error) {
		_ = c.c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.c.Write([]byte(what + "\n")); err != nil {
			return "", err
		}
		return c.rd.ReadString('\n')
	}
	var conns []conn
	via := map[string][]conn{}
	for i := 0; i < 4; i++ {
		c, err := net.Dial("tcp", org1)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		cc := conn{c, bufio.NewReader(c)}
		got, err := ask(cc, "hi")
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, cc)
		via[got[:2]] = append(via[got[:2]], cc)
	}
	if len(via["a:"]) != 2 || len(via["b:"]) != 2 {
		t.Fatalf("streams per agent: a %d, b %d (want 2 and 2)", len(via["a:"]), len(via["b:"]))
	}

	// agent a drops and reconnects: only its streams break, b's go on
	a.Disconnect()
	for _, cc := range via["b:"] {
		if got, err := ask(cc, "still"); err != nil || got != "b:still\n" {
			t.Fatalf("agent b's stream after agent a dropped: %q %v", got, err)
		}
	}
	waitFor(t, "agent a back", func() bool {
		return strings.Count(tr.session("1"), ",") == 1 && strings.Contains(tr.session("1"), a.Serial())
	})
	for _, cc := range via["b:"] {
		if got, err := ask(cc, "again"); err != nil || got != "b:again\n" {
			t.Fatalf("agent b's stream after agent a came back: %q %v", got, err)
		}
	}

	// a third agent is over max_agents_per_org: refused, the two keep their sessions
	before := tr.session("1")
	c3 := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "c:"), false)
	go func() { _ = c3.Run() }()
	time.Sleep(time.Second)
	if tr.session("1") != before {
		t.Fatalf("a third agent changed the sessions: %q -> %q", before, tr.session("1"))
	}
}

// A renewal keeps the agent id of the certificate it replaces; an enrollment starts a new one.
func TestAgentIDAcrossRenewals(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	iss, err := loadIssuer(filepath.Join(dir, "ca.crt"), filepath.Join(dir, "ca.key"), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	issue := func(agent string) *x509.Certificate {
		req, err := parseCSR([]byte(csr(t)))
		if err != nil {
			t.Fatal(err)
		}
		p, _, _, err := iss.sign(req, "1", agent)
		if err != nil {
			t.Fatal(err)
		}
		blk, _ := pem.Decode(p)
		c, err := x509.ParseCertificate(blk.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	first := issue("")
	if agentID(first) != first.SerialNumber.Text(16) {
		t.Fatalf("enrollment: agent id %q, serial %s", agentID(first), first.SerialNumber.Text(16))
	}
	renewed := issue(agentID(first))
	if agentID(renewed) != agentID(first) || renewed.SerialNumber.Cmp(first.SerialNumber) == 0 {
		t.Fatalf("renewal: agent id %q, want %q", agentID(renewed), agentID(first))
	}
	if other := issue(""); agentID(other) == agentID(first) {
		t.Fatal("two enrollments got the same agent id")
	}
}

type downStore struct {
	*MemStore
	down atomic.Bool
}

func (d *downStore) CertValid(ctx context.Context, serial string) (string, bool, error) {
	if d.down.Load() {
		return "", false, errors.New("registry down")
	}
	return d.MemStore.CertValid(ctx, serial)
}

// Item 7: with registry_grace a known agent reconnects while the registry is down; without
// it (the default) it does not, and an unknown certificate never does.
func TestRegistryGrace(t *testing.T) {
	for _, grace := range []string{"", "1m"} {
		var ds *downStore
		tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, RegistryGrace: grace}, func(m *MemStore) Store {
			ds = &downStore{MemStore: m}
			return ds
		})
		a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "a:"), false)
		runErr := make(chan error, 1)
		go func() { runErr <- a.Run() }()
		var ended error
		waitFor(t, "agent connected", func() bool {
			select {
			case ended = <-runErr: // it stopped: say why instead of waiting out the margin
				return true
			default:
			}
			return tr.session("1") != ""
		})
		if ended != nil {
			t.Fatalf("registry_grace %q: the agent stopped before it connected: %v", grace, ended)
		}
		// a second agent enrolled but never connected: unknown to the relay
		other := t.TempDir()
		if err := tr.agent(t, other, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
			t.Fatal(err)
		}
		ds.down.Store(true)
		a.Disconnect()
		want := grace != ""
		if want {
			// it retries after 1 s: wait for the event, however slow the machine is
			waitFor(t, "the known agent reconnected", func() bool { return tr.session("1") != "" })
		} else {
			time.Sleep(3 * time.Second) // nothing to wait for: the absence of a reconnection
		}
		back := tr.session("1") != ""
		if back != want {
			t.Fatalf("registry_grace %q: the known agent reconnected %v, want %v", grace, back, want)
		}
		st, _ := loadState(other)
		pool, _ := loadPool(filepath.Join(tr.dir, "ca.crt"))
		c, err := tls.Dial("tcp", tr.relayAddr, &tls.Config{Certificates: []tls.Certificate{*st.cert}, RootCAs: pool, ServerName: "localhost"})
		if err == nil {
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, err := c.Read(make([]byte, 1))
			if ne, ok := err.(net.Error); err == nil || ok && ne.Timeout() {
				t.Fatalf("registry_grace %q: an unknown certificate got a session without the registry", grace)
			}
			c.Close()
		}
	}
}

// Item 5: the relay holds no CA key; the issuing service signs, for the relays it knows only.
func TestIssuingService(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	start := func(relays ...string) string {
		addr := freeAddr(t)
		is := NewIssuer(IssuerConfig{Listen: addr, Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"),
			CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"), CertTTL: "3s", Relays: relays}, log)
		go func() { _ = is.Run() }()
		waitFor(t, "issuer up", func() bool {
			c, err := net.Dial("tcp", addr)
			if err == nil {
				c.Close()
			}
			return err == nil
		})
		_, port, _ := net.SplitHostPort(addr)
		return "https://localhost:" + port // the name in the certificate
	}
	// the relay's certificate is CN=localhost: the issuer serves it
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: filepath.Join(dir, "ca.crt"),
		IssuerURL: start("localhost")}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "a:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent connected", func() bool { return tr.session("1") != "" })
	first := a.Serial()
	waitFor(t, "renewal through the issuer", func() bool { return tr.store.Count("renew") >= 1 && a.Serial() != first })

	// an issuer that does not list this relay refuses it: the relay answers 500, and the
	// token, not used before the signature, stays valid
	tr2 := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: filepath.Join(dir, "ca.crt"),
		IssuerURL: start("some-other-relay")}, nil)
	tok := tr2.store.CreateToken("1")
	code, out := tr2.post(t, "/v1/enroll", enrollRequest{Token: tok, CSR: csr(t)})
	if code != http.StatusInternalServerError || out.Error != "certificate signing failed" {
		t.Fatalf("relay unknown to the issuer: %d %q", code, out.Error)
	}
	if !tr2.store.TokenUsable(tok) {
		t.Fatal("a failed signature burned the token")
	}
}

// testLog discards the relay's log unless LOKADB_TEST_LOG is set.
func testLog() *slog.Logger {
	if os.Getenv("LOKADB_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Drain (SIGTERM): the health check turns red first, then no new connections, while open
// streams finish; the relay is done when they are.
func TestDrain(t *testing.T) {
	org1, health := freeAddr(t), freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, HealthListen: health,
		DrainDelay: "300ms", DrainTimeout: "5s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "a:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent connected", func() bool { return tr.session("1") != "" })
	status := func() int {
		resp, err := http.Get("http://" + health + "/healthz")
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	waitFor(t, "health up", func() bool { return status() == http.StatusOK })

	live, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	rd := bufio.NewReader(live)
	say := func(what string) error {
		_ = live.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := live.Write([]byte(what + "\n")); err != nil {
			return err
		}
		got, err := rd.ReadString('\n')
		if err == nil && got != "a:"+what+"\n" {
			err = fmt.Errorf("got %q", got)
		}
		return err
	}
	if err := say("before"); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { tr.r.Drain(); close(done) }()
	waitFor(t, "health red", func() bool { return status() == http.StatusServiceUnavailable })
	time.Sleep(500 * time.Millisecond) // past drain_delay: the port is closed
	if c, err := net.DialTimeout("tcp", org1, time.Second); err == nil {
		c.Close()
		t.Fatal("a draining relay took a new connection")
	}
	if err := say("while draining"); err != nil {
		t.Fatalf("an open stream broke while draining: %v", err)
	}
	select {
	case <-done:
		t.Fatal("drain ended with a stream still open")
	case <-time.After(300 * time.Millisecond):
	}
	live.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not end after the last stream closed")
	}
}

// Item 2: an agent keeps a session to every relay, so any relay reaches the organization;
// stopping one relay leaves the connections through the other untouched.
func TestAgentSeveralRelays(t *testing.T) {
	r1 := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, DrainDelay: "0s", DrainTimeout: "1s"}, nil)
	org2 := freeAddr(t)
	r2 := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org2}, CA: filepath.Join(r1.dir, "ca.crt")},
		func(*MemStore) Store { return r1.store }) // one registry, one CA
	a := NewAgent(AgentConfig{Relay: r1.relayAddr + "," + r2.relayAddr, Enroll: r1.enroll, ServerName: "localhost",
		CA: filepath.Join(r1.dir, "ca.crt"), StateDir: t.TempDir(), TokenFile: func() string {
			tf := filepath.Join(t.TempDir(), "token")
			_ = writeFile(tf, r1.store.CreateToken("1"))
			return tf
		}(), DB: lineEcho(t, "db:")}, testLog())
	go func() { _ = a.Run() }()
	waitFor(t, "a session to each relay", func() bool { return r1.session("1") != "" && r2.session("1") != "" })

	talk := func(addr string) (net.Conn, *bufio.Reader) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		return c, bufio.NewReader(c)
	}
	via2, rd2 := talk(org2)
	defer via2.Close()
	ask := func(what string) error {
		_ = via2.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := via2.Write([]byte(what + "\n")); err != nil {
			return err
		}
		got, err := rd2.ReadString('\n')
		if err == nil && got != "db:"+what+"\n" {
			err = fmt.Errorf("got %q", got)
		}
		return err
	}
	if err := ask("via relay 2"); err != nil {
		t.Fatal(err)
	}
	via1, _ := talk(r1.r.cfg.Orgs["1"])
	via1.Close()

	r1.r.Drain() // relay 1 stops
	if err := ask("relay 1 is gone"); err != nil {
		t.Fatalf("the connection through relay 2 broke when relay 1 stopped: %v", err)
	}
	c, rd := talk(org2)
	defer c.Close()
	_, _ = c.Write([]byte("new\n"))
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	if got, err := rd.ReadString('\n'); err != nil || got != "db:new\n" {
		t.Fatalf("a new connection through relay 2: %q %v", got, err)
	}
	if r2.session("1") == "" {
		t.Fatal("the agent lost its session to relay 2")
	}
}
