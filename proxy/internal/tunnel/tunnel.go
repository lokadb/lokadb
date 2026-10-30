// Package tunnel connects the cloud to organizations' own databases without any
// inbound connection into the customer network.
//
// The agent, running next to the customer database, dials the relay (mutual TLS) and
// keeps one multiplexed session open. The relay maps each organization, identified by
// the agent's client certificate, to a TCP port inside the cloud network: every
// connection to that port (postgres_fdw, the proxy, admin tools) becomes a stream to
// the agent, which forwards it to its configured database address only. The agent can
// also expose a local port that is forwarded, the other way, to the cloud database
// (used by the logical-replication subscription during a move).
//
// Agents get their certificates by enrollment: the agent generates its key, sends a CSR
// with a one-time token and receives a short-lived certificate, renewed before it expires.
// The relay accepts only certificates issued this way and not revoked, and drops sessions
// whose certificate gets revoked.
//
// Bytes are relayed as they are: the Postgres protocol, authentication included, runs
// end to end between the cloud client and the customer database.
package tunnel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/yamux"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/lokadb/lokadb/proxy/internal/buildinfo"
)

func muxConfig() *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = true
	c.KeepAliveInterval = 10 * time.Second
	c.LogOutput = io.Discard
	// The receive window caps each stream at window/RTT: yamux's default 256 KiB gave
	// 10.8 MB/s at +20 ms, 4 MiB gave 93 MB/s (16 MiB: 99). A stream buffers up to the
	// window only when its reader falls behind. LOKADB_MUX_WINDOW_KB overrides it.
	c.MaxStreamWindowSize = 4 << 20
	// at most 64 MiB: a larger value overflowed uint32 and yamux refused every session
	if kb, err := strconv.Atoi(os.Getenv("LOKADB_MUX_WINDOW_KB")); err == nil && kb >= 256 && kb <= 64<<10 {
		c.MaxStreamWindowSize = uint32(kb) * 1024
	}
	return c
}

// pipe copies both ways until either side ends, then closes both.
func pipe(a, b net.Conn) {
	done := make(chan struct{}, 2)
	cp := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		done <- struct{}{}
	}
	go cp(a, b)
	go cp(b, a)
	<-done
	a.Close()
	b.Close()
	<-done
}

// refuse answers a Postgres client with a FATAL error instead of a bare disconnect, so
// the application sees why its organization's database is unreachable.
func refuse(c net.Conn, msg string) {
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	be := pgproto3.NewBackend(c, c)
	for {
		m, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		switch m.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				return
			}
		case *pgproto3.StartupMessage:
			be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "08006", Message: msg})
			_ = be.Flush()
			return
		default:
			return
		}
	}
}

func loadPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("%s: no certificate", path)
	}
	return pool, nil
}

// RelayConfig configures the cloud side.
type RelayConfig struct {
	Listen       string `json:"listen"`        // where agents connect (mutual TLS)
	EnrollListen string `json:"enroll_listen"` // enrollment and renewal API (TLS)
	Cert         string `json:"cert"`
	Key          string `json:"key"`
	CA           string `json:"ca"`     // CA certificate
	CAKey        string `json:"ca_key"` // CA key: signs agents' certificate requests
	// CertNames: the relay makes the CA and its own certificate itself when they are missing,
	// for these names (comma separated, the first is the common name): cert, key, ca and ca_key
	// must then be relay.crt, relay.key, ca.crt and ca.key of one directory, which only the
	// relay mounts (the CA key never leaves it). Existing files are never replaced.
	CertNames string `json:"cert_names"`
	// CAPublic: where the relay copies the public ca.crt for the agent, the runner and the proxy
	// (written once, never replaced).
	CAPublic string `json:"ca_public"`
	// IssuerURL: the issuing service (lokadb issuer) that signs instead, so that the relay
	// holds no CA key; the relay authenticates to it with its own certificate.
	IssuerURL    string `json:"issuer_url"`
	CertTTL      string `json:"cert_ttl"`
	ControlPlane string `json:"control_plane"` // DSN of the registry database
	// Orgs[org] = address inside the cloud network that leads to that organization's database.
	// The registry table lokadb_cp.relay_orgs adds more; both are re-read every few seconds.
	Orgs map[string]string `json:"orgs"`
	// ConfigPath, if set, is the file the relay re-reads "orgs" from (lokadb relay -config).
	ConfigPath string `json:"-"`
	// EnrollFailPerMin: failed enrollments and renewals allowed per client host and minute
	// before the relay answers 429 (default 10, negative = no limit).
	EnrollFailPerMin int `json:"enroll_fail_per_min"`
	// MaxAgentsPerOrg: agents of one organization connected at once (default 4). Each agent
	// enrolls with its own token; a reconnection of the same agent replaces its session.
	MaxAgentsPerOrg int `json:"max_agents_per_org"`
	// MaxCloudStreams: streams one agent session may have open to the cloud database at once
	// (default 64; a move's subscription takes one or two). Beyond it a stream is refused.
	MaxCloudStreams int `json:"max_cloud_streams"`
	// MaxOrgConns: connections to one organization's endpoint at once (default 1000,
	// negative = no limit); beyond it a connection is refused before it reaches the agent
	// and a slot of the organization's database.
	MaxOrgConns int `json:"max_org_conns"`
	// MaxHandshakes: agent connections still in their TLS handshake and certificate check
	// at once (default 256, negative = no limit); beyond it one is closed at once. The agent
	// port faces the internet: half-open handshakes must not use up the relay's descriptors.
	MaxHandshakes int `json:"max_handshakes"`
	// MaxEnrollConns: connections to the enrollment port at once (default 256, negative = no
	// limit); beyond it a connection is closed at once.
	MaxEnrollConns int `json:"max_enroll_conns"`
	// MaxStreams: connections through the relay to all organizations at once (default 2048,
	// negative = no limit). A connection whose reader stalls buffers up to the mux window
	// (4 MiB), so the relay's memory in the worst case is about max_streams x 4 MiB: size it
	// to the host (review 3 of the tunnel, R3-6).
	MaxStreams int `json:"max_streams"`
	// RegistryGrace: while the registry does not answer, a certificate the registry called
	// valid at most this long ago may still open a session (default "" = 0: no session
	// without the registry). The price: a certificate revoked meanwhile can connect.
	RegistryGrace string `json:"registry_grace"`
	// RenewExpired: a certificate that ended at most this long ago may still renew (only
	// renew; default "24h", "0" off). An agent whose renewals failed through the last third of
	// its certificate (the relay or the registry down) comes back by itself instead of needing a
	// new token (review 3 of the tunnel, R3-3). The registry still has to call it valid.
	RenewExpired string `json:"renew_expired"`
	// HealthListen: address of GET /healthz for a load balancer ("" = none); DrainDelay and
	// DrainTimeout shape the stop on SIGTERM (drain.go; defaults 0 and 5s: with one relay a
	// long drain only lengthens the outage; behind a balancer with several relays give the
	// balancer drain_delay to notice and the pools drain_timeout to move).
	HealthListen string `json:"health_listen"`
	DrainDelay   string `json:"drain_delay"`
	DrainTimeout string `json:"drain_timeout"`

	poll            time.Duration // how often the organization list is re-read (2s)
	superseded      time.Duration // supersededGrace (tests)
	lenientRenew    bool          // tests: renewals at any time and any number (renewTooOften off)
	noDrainSignal   bool          // tests: a relay of before the drain signal
	strictRenewTest bool          // tests: startRelay keeps renewTooOften on
	registryTimeout time.Duration // bound of every registry call made for a connection (5s)
	// Cloud is the cloud database address agents may reach through the tunnel ("" = none).
	Cloud string `json:"cloud"`
	// CloudAccess: "window" (default) lets an organization's agent reach Cloud only while the
	// control plane has a window open for it (move-org -cp opens one for the move);
	// "always" lets every connected agent reach it at any time.
	CloudAccess string `json:"cloud_access"`
	// OrgAllow: who may connect to the organizations' endpoints (IPs, CIDRs, host names);
	// empty = anybody who can reach the port.
	OrgAllow []string `json:"org_allow"`
	// OrgTLS: "require" lets through the organizations' endpoints only connections that
	// negotiate TLS with the organization's own database (end to end: the relay then sees
	// only ciphertext); "" passes connections as they are.
	OrgTLS string `json:"org_tls"`
	// OrgPin: "off" lets org_allow in as before, without the SaaS database's proven addresses
	// (pin.go); "" (on) takes the organizations' connections from those only once one is proven
	OrgPin string `json:"org_pin"`
}

type agentSession struct {
	sess    *yamux.Session
	serial  string
	id      string // agentID: the same agent across reconnections and renewals
	org     string
	ends    time.Time // its certificate's NotAfter
	remote  string
	since   time.Time
	set     string // the agent's session set (sets.go): its sessions to this relay, renewed together
	slot    string // which of the set
	version string // the agent's version, as it told in its ALPN name ("" from an agent without)
}

type Relay struct {
	started   time.Time
	cfg       RelayConfig
	store     Store
	log       *slog.Logger
	orgAllow  *allowList
	limit     *failLimiter
	grace     time.Duration
	expired   time.Duration // RenewExpired
	draining  atomic.Bool
	agentLn   net.Listener // where agents connect (closed by Drain)
	mu        sync.Mutex
	sessions  map[string]map[string]*agentSession // org -> agent id -> its session
	checked   map[string]checkedCert              // last registry answers (registry_grace)
	served    map[string]string                   // organizations served now (org -> listen)
	endpoints map[string]*orgEndpoint             // their open listeners
	tlsOrgs   map[string]bool                     // organizations whose registry entry requires TLS
	tlsKnown  bool                                // the registry's list was read at least once (tlsOrgs holds)
	pin       pinState                            // the SaaS database's addresses (pin.go)
	orgConns  map[net.Conn]string                 // open connections to the organizations' ports, by source address
	touched   map[string]time.Time                // proven addresses marked in use (pin.go)
	cloudSeen map[string]cloudAnswer              // the registry's last cloud_access answer per organization
	shaking   chan struct{}                       // agent handshakes in progress (MaxHandshakes)
	shakers   map[string]int                      // of them, per source host (maxHandshakesPerHost)
	streams   chan struct{}                       // connections through the relay, every organization (MaxStreams)
	retiring  map[*agentSession]bool              // sessions replaced by their agent's newer one, finishing their streams
	renewals  map[string]int                      // successful renewals per presented serial (renewTooOften)
	// last good organization list of each source (only one sync runs at a time)
	fileOrgs, regOrgs map[string]string
	wmu               sync.Mutex
	warned            map[string]bool
}

func NewRelay(cfg RelayConfig, store Store, log *slog.Logger) *Relay {
	if cfg.poll == 0 {
		cfg.poll = 2 * time.Second
	}
	if cfg.registryTimeout == 0 {
		cfg.registryTimeout = 5 * time.Second
	}
	if cfg.MaxAgentsPerOrg == 0 {
		cfg.MaxAgentsPerOrg = 4
	}
	if cfg.MaxCloudStreams == 0 {
		cfg.MaxCloudStreams = 64
	}
	if cfg.MaxOrgConns == 0 {
		cfg.MaxOrgConns = 1000
	}
	if cfg.MaxHandshakes == 0 {
		cfg.MaxHandshakes = 256
	}
	if cfg.MaxEnrollConns == 0 {
		cfg.MaxEnrollConns = 256
	}
	if cfg.MaxStreams == 0 {
		cfg.MaxStreams = 2048
	}
	return &Relay{started: time.Now(), cfg: cfg, store: store, log: log, sessions: map[string]map[string]*agentSession{}, cloudSeen: map[string]cloudAnswer{},
		shaking: slots(cfg.MaxHandshakes), streams: slots(cfg.MaxStreams),
		served: map[string]string{}, endpoints: map[string]*orgEndpoint{}, fileOrgs: cfg.Orgs, warned: map[string]bool{}}
}

func (r *Relay) Run() error {
	certs, err := newReloadable("relay certificate", r.log, func() (tls.Certificate, error) { return keyPair(r.cfg.Cert, r.cfg.Key) }, r.cfg.Cert, r.cfg.Key)
	if err != nil {
		return err
	}
	pools, err := newReloadable("CA bundle", r.log, func() (*x509.CertPool, error) { return loadPool(r.cfg.CA) }, r.cfg.CA)
	if err != nil {
		return err
	}
	go r.expiryWarn(certs)
	var iss signer
	ttl := "issuer's"
	if r.cfg.IssuerURL != "" {
		if r.cfg.CAKey != "" {
			r.log.Warn("ca_key is ignored: certificates are signed by the issuing service", "issuer_url", r.cfg.IssuerURL)
		}
		if iss, err = newRemoteIssuer(r.cfg.IssuerURL, certs, pools); err != nil {
			return err
		}
	} else {
		d, err := parseTTL(r.cfg.CertTTL)
		if err != nil {
			return err
		}
		local, err := newReloadable("CA key", r.log, func() (*issuer, error) { return loadIssuer(r.cfg.CA, r.cfg.CAKey, d) }, r.cfg.CA, r.cfg.CAKey)
		if err != nil {
			return err
		}
		iss = reloadingSigner{local}
		ttl = d.String()
	}
	switch r.cfg.CloudAccess {
	case "":
		r.cfg.CloudAccess = "window"
	case "window", "always":
	default:
		return fmt.Errorf("cloud_access: %q, want \"window\" or \"always\"", r.cfg.CloudAccess)
	}
	r.expired = 24 * time.Hour
	if r.cfg.RenewExpired != "" {
		if r.expired, err = time.ParseDuration(r.cfg.RenewExpired); err != nil || r.expired < 0 {
			return fmt.Errorf("renew_expired: %q", r.cfg.RenewExpired)
		}
	}
	if r.cfg.RegistryGrace != "" {
		if r.grace, err = time.ParseDuration(r.cfg.RegistryGrace); err != nil || r.grace < 0 {
			return fmt.Errorf("registry_grace: %q", r.cfg.RegistryGrace)
		}
		if r.grace > 0 {
			r.log.Warn("registry_grace: while the registry is down, known certificates connect, revoked ones included", "grace", r.grace)
		}
	}
	if r.cfg.OrgTLS != "" && r.cfg.OrgTLS != "require" {
		return fmt.Errorf("org_tls: %q, want \"require\" or nothing", r.cfg.OrgTLS)
	}
	if r.orgAllow, err = newAllowList(r.cfg.OrgAllow); err != nil {
		return err
	}
	switch {
	case r.cfg.EnrollFailPerMin == 0:
		r.limit = newFailLimiter(10)
	case r.cfg.EnrollFailPerMin > 0:
		r.limit = newFailLimiter(r.cfg.EnrollFailPerMin)
	}
	if r.orgAllow.empty() {
		// required (review 3 of the tunnel, R3-12): without it anybody who reaches the relay's
		// organization ports gets a connection to that organization's database through its
		// agent, with only the organization's Postgres password in the way
		return errors.New("org_allow is required: list the addresses of the cloud that reach the organizations (the proxy, the cloud Postgres for postgres_fdw, the hosts that run lokadb check and move-org): IPs, CIDRs or host names")
	}
	ln, err := tls.Listen("tcp", r.cfg.Listen, relayTLS(&tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}, certs, pools))
	if err != nil {
		return err
	}
	raw, err := net.Listen("tcp", r.cfg.EnrollListen)
	if err != nil {
		return err
	}
	// capped under TLS: http.Server needs the *tls.Conn itself to fill req.TLS
	eln := tls.NewListener(capListener(raw, r.cfg.MaxEnrollConns), relayTLS(&tls.Config{
		// enrollment: none yet; renewal: the current one, verified here so that one ended at
		// most renew_expired ago passes too (Go's own check refuses any expired one)
		ClientAuth:            tls.RequestClientCert,
		VerifyPeerCertificate: func(raw [][]byte, _ [][]*x509.Certificate) error { return r.verifyRenewing(raw, pools.get()) },
		MinVersion:            tls.VersionTLS13,
	}, certs, pools))
	go func() {
		srv := &http.Server{Handler: r.api(iss), ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute,
			ErrorLog: slog.NewLogLogger(r.log.Handler(), slog.LevelWarn)}
		r.log.Info("enrollment API", "listen", r.cfg.EnrollListen, "cert_ttl", ttl)
		if err := srv.Serve(eln); err != nil {
			r.log.Error("enrollment API stopped", "err", err)
		}
	}()
	// A port of the config file that cannot be opened at start is a configuration error, as
	// before; later, and for the registry, it is logged and retried.
	if err := r.syncEndpoints(r.wantedEndpoints()); err != nil {
		for org, addr := range r.cfg.Orgs {
			if r.listening(org, addr) {
				continue
			}
			return err
		}
	}
	go r.watchEndpoints()
	go r.watchRevocations()
	r.serveHealth()
	r.mu.Lock()
	r.agentLn = ln
	r.mu.Unlock()
	r.log.Info("waiting for agents", "listen", r.cfg.Listen)
	err = acceptLoop(ln, r.log, func(c net.Conn) { r.agent(c.(*tls.Conn)) })
	if r.draining.Load() {
		select {} // Drain closed the listener and ends the process when it is done
	}
	return err
}

// maxHandshakesPerHost: agent handshakes at once from one source host (an IPv6 /64): one
// host cannot take every place of max_handshakes (review 3 of the tunnel, R3-4). The agents of
// one site behind its NAT are a handful.
const maxHandshakesPerHost = 16

func (r *Relay) shakeFrom(host string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shakers == nil {
		r.shakers = map[string]int{}
	}
	if r.shakers[host] >= maxHandshakesPerHost {
		return false
	}
	r.shakers[host]++
	return true
}

func (r *Relay) shook(host string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shakers[host]--; r.shakers[host] <= 0 {
		delete(r.shakers, host)
	}
}

// slots: a counting semaphore of n (nil, no limit, for n < 0); take does not wait.
func slots(n int) chan struct{} {
	if n < 0 {
		return nil
	}
	return make(chan struct{}, n)
}

func take(s chan struct{}) bool {
	if s == nil {
		return true
	}
	select {
	case s <- struct{}{}:
		return true
	default:
		return false
	}
}

func give(s chan struct{}) {
	if s != nil {
		<-s
	}
}

// acceptLoop serves ln until it is closed. Other accept errors (too many open files, a
// connection reset before it was accepted) are waited out, not fatal: one bad moment must
// not stop the listener for every organization.
func acceptLoop(ln net.Listener, log *slog.Logger, serve func(net.Conn)) error {
	backoff := 5 * time.Millisecond
	for {
		c, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			log.Warn("accept failed, retrying", "listen", ln.Addr(), "err", err, "in", backoff)
			time.Sleep(backoff)
			if backoff < time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = 5 * time.Millisecond
		go serve(c)
	}
}

type enrollRequest struct {
	Token string `json:"token,omitempty"`
	CSR   string `json:"csr"`
}

type enrollResponse struct {
	Cert     string    `json:"cert,omitempty"`
	Org      string    `json:"org,omitempty"`
	NotAfter time.Time `json:"not_after,omitempty"`
	Error    string    `json:"error,omitempty"`
	// NotServed: the organization has no endpoint on this relay (yet); the token is kept.
	NotServed bool `json:"not_served,omitempty"`
	// CA: the relay's CA bundle now. The agent keeps it (ca.pem in its state) and trusts it
	// with -ca: a rotated CA reaches agents that nobody can reach (review 3, R3-14).
	CA string `json:"ca,omitempty"`
}

func (r *Relay) api(iss signer) http.Handler {
	reply := func(w http.ResponseWriter, code int, resp enrollResponse) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	}
	// refused counts a failed attempt against the client host and answers it.
	refused := func(w http.ResponseWriter, req *http.Request, code int, msg string) {
		r.limit.fail(limitKey(req))
		reply(w, code, enrollResponse{Error: msg})
	}
	// issue signs, then records the certificate, an enrollment's together with the use of
	// its token (tokenHash, one registry transaction): a failed signature (the issuing
	// service down) or registry write burns no token, and a certificate whose token turned
	// out used is never recorded, so it opens no session.
	issue := func(w http.ResponseWriter, req *http.Request, csr *x509.CertificateRequest, org, kind string, parent *x509.Certificate,
		tokenHash string) {
		agent, parentSerial := "", ""
		if parent != nil {
			agent, parentSerial = agentID(parent), parent.SerialNumber.Text(16)
		}
		certPEM, serial, notAfter, err := iss.sign(csr, org, agent)
		if err != nil {
			r.log.Error("signing a certificate failed", "org", org, "kind", kind, "err", err)
			reply(w, http.StatusInternalServerError, enrollResponse{Error: "certificate signing failed"})
			return
		}
		if tokenHash != "" {
			ctx, cancel := context.WithTimeout(req.Context(), r.cfg.registryTimeout)
			err := r.store.EnrollCert(ctx, tokenHash, req.RemoteAddr, serial, org, notAfter)
			cancel()
			if !r.tokenUsed(w, req, err) {
				return
			}
		} else if err := r.store.RecordCert(req.Context(), serial, org, notAfter, kind, req.RemoteAddr, parentSerial); err != nil {
			reply(w, http.StatusInternalServerError, enrollResponse{Error: "registry unavailable"})
			return
		}
		if blk, _ := pem.Decode(certPEM); blk != nil {
			if c, err := x509.ParseCertificate(blk.Bytes); err == nil {
				if err := r.store.SetCertKey(req.Context(), serial, keyHash(c)); err != nil {
					r.log.Warn("registry: the certificate's key is not recorded (its serial alone identifies it)", "serial", serial, "err", err)
				}
			}
		}
		r.log.Info("certificate issued", "org", org, "kind", kind, "serial", serial, "not_after", notAfter.Format(time.RFC3339))
		bundle, _ := os.ReadFile(r.cfg.CA)
		reply(w, http.StatusOK, enrollResponse{Cert: string(certPEM), Org: org, NotAfter: notAfter, CA: string(bundle)})
	}
	// decode reads the request and checks its CSR before any token is used, so a malformed
	// request burns nothing.
	decode := func(w http.ResponseWriter, req *http.Request) (enrollRequest, *x509.CertificateRequest, bool) {
		var in enrollRequest
		if r.limit.blocked(limitKey(req)) {
			r.log.Warn("enrollment refused: too many failed attempts", "remote", req.RemoteAddr, "path", req.URL.Path)
			reply(w, http.StatusTooManyRequests, enrollResponse{Error: "too many failed attempts, retry later"})
			return in, nil, false
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 64<<10)).Decode(&in); err != nil {
			refused(w, req, http.StatusBadRequest, "bad request")
			return in, nil, false
		}
		csr, err := parseCSR([]byte(in.CSR))
		if err != nil {
			r.log.Warn("enrollment refused: bad certificate request", "remote", req.RemoteAddr, "path", req.URL.Path, "err", err)
			refused(w, req, http.StatusBadRequest, errBadCSR.Error())
			return in, nil, false
		}
		return in, csr, true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/enroll", func(w http.ResponseWriter, req *http.Request) {
		in, csr, ok := decode(w, req)
		if !ok {
			return
		}
		ctx, cancel := context.WithTimeout(req.Context(), r.cfg.registryTimeout)
		defer cancel()
		// The organization is checked before the token is used: a token for an organization
		// this relay does not serve (yet) stays valid.
		h := hashToken(in.Token)
		org, err := r.store.TokenOrg(ctx, h)
		if err == nil && !r.serves(org) {
			r.log.Warn("enrollment refused: organization not served by this relay, token kept", "org", org, "remote", req.RemoteAddr)
			reply(w, http.StatusForbidden, enrollResponse{Error: "organization not served by this relay (the token is still valid)", NotServed: true})
			return
		}
		if !r.tokenUsed(w, req, err) {
			return
		}
		issue(w, req, csr, org, "enroll", nil, h)
	})
	mux.HandleFunc("POST /v1/renew", func(w http.ResponseWriter, req *http.Request) {
		_, csr, ok := decode(w, req)
		if !ok {
			return
		}
		if req.TLS == nil || len(req.TLS.PeerCertificates) == 0 {
			refused(w, req, http.StatusUnauthorized, "renewal needs the current certificate")
			return
		}
		peer := req.TLS.PeerCertificates[0] // verified against the CA (verifyRenewing): valid, or ended at most renew_expired ago
		ctx, cancel := context.WithTimeout(req.Context(), r.cfg.registryTimeout)
		defer cancel()
		org, valid, err := r.store.CertValid(ctx, peer.SerialNumber.Text(16))
		if err != nil {
			r.log.Error("renewal: registry unavailable", "err", err)
			reply(w, http.StatusServiceUnavailable, enrollResponse{Error: "registry unavailable"})
			return
		}
		if !valid || peer.Subject.CommonName != "org-"+org {
			r.log.Warn("renewal refused", "cn", peer.Subject.CommonName, "remote", req.RemoteAddr)
			refused(w, req, http.StatusForbidden, "certificate revoked or not issued by enrollment")
			return
		}
		if !r.serves(org) {
			r.log.Warn("renewal refused: organization not served by this relay", "org", org, "remote", req.RemoteAddr)
			reply(w, http.StatusForbidden, enrollResponse{Error: "organization not served by this relay"})
			return
		}
		if why := r.renewTooOften(peer); why != "" {
			r.log.Warn("renewal refused: "+why, "serial", peer.SerialNumber.Text(16), "remote", req.RemoteAddr)
			reply(w, http.StatusTooManyRequests, enrollResponse{Error: why})
			return
		}
		r.confirm(peer.SerialNumber.Text(16))
		issue(w, req, csr, org, "renew", peer, "")
	})
	return mux
}

type cloudAnswer struct {
	g      CloudGrant
	repeat bool // closed twice within a second
	at     time.Time
}

// cloudOpen: the registry's cloud_access for org. An open answer is kept for a second; a
// closed one only once it repeats within a second (a burst of streams with the window
// closed), so the first stream after a move opens the window is not refused by an old answer.
// An error is not kept.
func (r *Relay) cloudOpen(org string) (CloudGrant, error) {
	r.mu.Lock()
	a, ok := r.cloudSeen[org]
	r.mu.Unlock()
	fresh := ok && time.Since(a.at) < time.Second
	if fresh && (a.g.Open || a.repeat) {
		return a.g, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	defer cancel()
	g, err := r.store.CloudWindow(ctx, org)
	if err == nil {
		r.mu.Lock()
		r.cloudSeen[org] = cloudAnswer{g: g, repeat: !g.Open && fresh && !a.g.Open, at: time.Now()}
		r.mu.Unlock()
	}
	return g, err
}

// cloudStartup reads the startup of a connection the agent opens to the cloud database while
// the organization's window is open (review 3 of the tunnel, R3-13): only the window's role,
// to its database, for replication (a move's subscription and its table copies) gets
// through; the startup message is returned to be sent on. Anything else is refused (FATAL
// 28000) and nil returned. A request for TLS or GSS encryption is declined: the agent-relay
// leg is mutual TLS already, and the relay must read the startup to check it.
func (r *Relay) cloudStartup(org string, c net.Conn, g CloudGrant) []byte {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	be := pgproto3.NewBackend(c, c)
	deny := func(why string) []byte {
		r.log.Warn("cloud connection refused: "+why, "org", org, "role", g.Role, "database", g.DB)
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000",
			Message: fmt.Sprintf("lokadb relay: the cloud window of organization %s admits only the move's role %q to database %q for replication: %s", org, g.Role, g.DB, why)})
		_ = be.Flush()
		c.Close()
		return nil
	}
	for asked := 0; asked < 3; asked++ {
		m, err := be.ReceiveStartupMessage()
		if err != nil {
			c.Close()
			return nil
		}
		switch m := m.(type) {
		case *pgproto3.SSLRequest, *pgproto3.GSSEncRequest:
			if _, err := c.Write([]byte{'N'}); err != nil {
				c.Close()
				return nil
			}
		case *pgproto3.StartupMessage:
			user, db := m.Parameters["user"], m.Parameters["database"]
			if db == "" {
				db = user
			}
			switch {
			case g.Role == "" || g.DB == "":
				return deny("the window names no role and database")
			case user != g.Role:
				return deny("role " + user)
			case db != g.DB:
				return deny("database " + db)
			case m.Parameters["replication"] != "database":
				return deny("not a replication connection")
			}
			_ = c.SetDeadline(time.Time{})
			b, err := m.Encode(nil)
			if err != nil {
				c.Close()
				return nil
			}
			return b
		default:
			return deny("not a startup message")
		}
	}
	return deny("too many encryption requests")
}

// windowCheck: how often the open streams to the cloud are checked against the window.
func (r *Relay) windowCheck() time.Duration {
	if r.cfg.poll > 0 {
		return r.cfg.poll
	}
	return 2 * time.Second
}

// verifyRenewing checks a client certificate on the enrollment port: none (an enrollment), or
// one of the CA for client authentication, valid now or ended at most r.expired ago.
func (r *Relay) verifyRenewing(raw [][]byte, roots *x509.CertPool) error {
	if len(raw) == 0 {
		return nil
	}
	certs := make([]*x509.Certificate, 0, len(raw))
	for _, b := range raw {
		c, err := x509.ParseCertificate(b)
		if err != nil {
			return err
		}
		certs = append(certs, c)
	}
	inter := x509.NewCertPool()
	for _, c := range certs[1:] {
		inter.AddCert(c)
	}
	at := time.Now()
	if leaf := certs[0]; at.After(leaf.NotAfter) && at.Sub(leaf.NotAfter) <= r.expired {
		at = leaf.NotAfter.Add(-time.Second) // its chain as it was on its last second
	}
	_, err := certs[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter, CurrentTime: at,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	return err
}

// maxRenewalsPerCert: renewals one certificate may ask for. An agent asks once, again when an
// answer was lost (V-1); more is a holder renewing at will (review 3 of the tunnel, R3-18).
const maxRenewalsPerCert = 3

// renewTooOften: why a renewal with cert is refused now ("": it is not). A certificate renews
// once a third of its life is gone (the agent renews at two thirds), and at most
// maxRenewalsPerCert times.
func (r *Relay) renewTooOften(cert *x509.Certificate) string {
	if r.cfg.lenientRenew {
		return ""
	}
	if life := cert.NotAfter.Sub(cert.NotBefore); time.Since(cert.NotBefore) < life/3 {
		return "renewal too early: a certificate renews once a third of its life is gone"
	}
	serial := cert.SerialNumber.Text(16)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.renewals == nil {
		r.renewals = map[string]int{}
	}
	if r.renewals[serial] >= maxRenewalsPerCert {
		return "renewal refused: this certificate was renewed already"
	}
	r.renewals[serial]++
	if len(r.renewals) > 100000 { // a relay running for long: forget; a refusal is a nicety, not a guard
		r.renewals = map[string]int{serial: r.renewals[serial]}
	}
	return ""
}

// keyHash: sha256 of a certificate's public key (SubjectPublicKeyInfo), hex.
func keyHash(c *x509.Certificate) string {
	h := sha256.Sum256(c.RawSubjectPublicKeyInfo)
	return hex.EncodeToString(h[:])
}

// confirm: the agent presented serial, the certificate it was renewed from is done with
// (Store.Confirm). A registry error leaves the parent valid a little longer: logged only.
func (r *Relay) confirm(serial string) {
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	defer cancel()
	if err := r.store.Confirm(ctx, serial); err != nil {
		r.log.Warn("registry: the renewed certificate's parent stays valid for now", "serial", serial, "err", err)
	}
}

// tokenUsed answers a failed token lookup or use (and counts a bad token against the client).
func (r *Relay) tokenUsed(w http.ResponseWriter, req *http.Request, err error) bool {
	reply := func(code int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(enrollResponse{Error: msg})
	}
	switch {
	case err == nil:
		return true
	case errors.Is(err, errBadToken):
		r.log.Warn("enrollment refused", "remote", req.RemoteAddr, "err", err)
		r.limit.fail(limitKey(req))
		reply(http.StatusForbidden, errBadToken.Error())
	default:
		r.log.Error("enrollment: registry unavailable", "err", err)
		reply(http.StatusServiceUnavailable, "registry unavailable")
	}
	return false
}

// agent authenticates one agent connection and serves its session until it drops.
func (r *Relay) agent(c *tls.Conn) {
	host := hostOf(c.RemoteAddr().String())
	if !r.shakeFrom(host) {
		r.log.Warn("agent connection closed: too many handshakes in progress from its host", "remote", c.RemoteAddr(), "max", maxHandshakesPerHost)
		c.Close()
		return
	}
	if !take(r.shaking) {
		r.shook(host)
		r.log.Warn("agent connection closed: max_handshakes in progress", "remote", c.RemoteAddr(), "max", r.cfg.MaxHandshakes)
		c.Close()
		return
	}
	// 5 s for the whole handshake: a connection that sends nothing frees its place soon
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	err := c.Handshake()
	r.shook(host)
	if err != nil {
		give(r.shaking)
		r.log.Warn("agent rejected", "remote", c.RemoteAddr(), "err", err)
		c.Close()
		return
	}
	_ = c.SetDeadline(time.Time{})
	peer := c.ConnectionState().PeerCertificates[0]
	serial := peer.SerialNumber.Text(16)
	// Bounded: a registry that hangs must not hold the connection (and its goroutine) forever.
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	org, valid, err := r.store.CertValid(ctx, serial)
	cancel()
	give(r.shaking)
	if err != nil {
		org, valid = r.graceCheck(serial, err)
	} else {
		r.remember(serial, org, valid)
	}
	if !valid || peer.Subject.CommonName != "org-"+org {
		r.log.Warn("agent rejected: certificate revoked or not issued by enrollment", "cn", peer.Subject.CommonName, "serial", serial, "err", err)
		c.Close()
		return
	}
	if !r.serves(org) {
		r.log.Warn("agent rejected: organization not configured", "org", org)
		c.Close()
		return
	}
	if err == nil && valid {
		kctx, kcancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
		k, kerr := r.store.CertKey(kctx, serial)
		kcancel()
		if kerr == nil && k != "" && k != keyHash(peer) {
			r.log.Error("agent rejected: the certificate's key is not the one its serial was issued with", "serial", serial, "remote", c.RemoteAddr())
			c.Close()
			return
		}
		r.confirm(serial)
	}
	sess, err := yamux.Server(c, muxConfig())
	if err != nil {
		r.log.Error("agent session not started", "org", org, "err", err)
		c.Close()
		return
	}
	as := &agentSession{sess: sess, serial: serial, id: agentID(peer), org: org, ends: peer.NotAfter,
		remote: c.RemoteAddr().String(), since: time.Now()}
	as.set, as.slot, as.version = parseSet(c.ConnectionState().NegotiatedProtocol)
	r.mu.Lock()
	if _, ok := r.served[org]; !ok { // the organization was removed while it connected
		r.mu.Unlock()
		sess.Close()
		return
	}
	if r.draining.Load() { // connected while Drain took its snapshot: not served (review 3, R3-16)
		r.mu.Unlock()
		sess.Close()
		return
	}
	agents := r.sessions[org]
	if agents == nil {
		agents = map[string]*agentSession{}
		r.sessions[org] = agents
	}
	old, why := placeSession(agents, as, r.cfg.MaxAgentsPerOrg)
	if why != "" {
		r.mu.Unlock()
		r.log.Warn("agent rejected: "+why, "org", org, "agent", as.id, "max", r.cfg.MaxAgentsPerOrg)
		sess.Close()
		return
	}
	n := len(agents)
	r.mu.Unlock()
	// the same agent's earlier set (a renewal, a restart) or the same slot reconnected: the
	// newest take the new streams, the old ones finish their own
	for _, o := range old {
		r.retire(o)
	}
	r.log.Info("agent connected", "org", org, "agent", as.id, "serial", serial, "agents", n,
		"cert_expires", peer.NotAfter.Format(time.RFC3339), "remote", c.RemoteAddr())

	// Streams opened by the agent go to the cloud database (and nowhere else), at most
	// MaxCloudStreams at once; the window is read from the registry at most once a second per
	// organization (review of the tunnel, S-3: every stream was a registry query, unbounded).
	// A window that closes (move-org's end, its expiry) also closes the streams opened while
	// it was open: a connection kept past the move must not keep reading the cloud
	// database (review of the tunnel, K-1, the tunnel's part).
	var liveMu sync.Mutex
	live := map[net.Conn]bool{}
	if r.cfg.CloudAccess == "window" {
		go func() {
			var failingSince time.Time
			for {
				select {
				case <-sess.CloseChan():
					return
				case <-time.After(r.windowCheck()):
				}
				liveMu.Lock()
				n := len(live)
				liveMu.Unlock()
				if n == 0 {
					continue
				}
				g, err := r.cloudOpen(org)
				open := g.Open
				if err != nil {
					if failingSince.IsZero() {
						failingSince = time.Now()
					}
					if time.Since(failingSince) < 10*time.Second {
						continue
					}
					// the registry has not answered for 10 s: the window may have closed meanwhile;
					// the streams go, a move's subscription reconnects (review 3, R3-15)
					open = false
				} else {
					failingSince = time.Time{}
				}
				if !open {
					r.log.Warn("cloud window closed: closing the organization's streams to the cloud database", "org", org, "agent", as.id, "streams", n)
					liveMu.Lock()
					for c := range live {
						c.Close()
					}
					liveMu.Unlock()
				}
			}
		}()
	}
	go func() {
		cloudSlots := slots(r.cfg.MaxCloudStreams) // negative: no limit, as the other ceilings (review 3, R3-11)
		for {
			st, err := sess.Accept()
			if err != nil {
				return
			}
			if r.cfg.Cloud == "" {
				st.Close()
				continue
			}
			if !take(cloudSlots) {
				r.log.Warn("cloud stream refused: the agent has max_cloud_streams open", "org", org, "agent", as.id, "max", r.cfg.MaxCloudStreams)
				// not in the accept loop: a silent stream would stop it for 5 s (review 3, R3-15)
				go refuse(st, fmt.Sprintf("lokadb relay: organization %s has too many connections to the cloud database open", org))
				continue
			}
			go func() {
				defer give(cloudSlots)
				var first []byte
				if r.cfg.CloudAccess == "window" {
					g, err := r.cloudOpen(org)
					if err != nil || !g.Open {
						r.log.Warn("cloud database closed to the organization", "org", org, "err", err)
						refuse(st, fmt.Sprintf("lokadb relay: the cloud database is not open to organization %s (no move in progress)", org))
						return
					}
					if first = r.cloudStartup(org, st, g); first == nil {
						return
					}
				}
				up, err := net.DialTimeout("tcp", r.cfg.Cloud, 5*time.Second)
				if err != nil {
					r.log.Warn("cloud database unreachable", "org", org, "err", err)
					st.Close()
					return
				}
				if first != nil {
					if _, err := up.Write(first); err != nil {
						up.Close()
						st.Close()
						return
					}
				}
				liveMu.Lock()
				live[st] = true
				liveMu.Unlock()
				pipe(st, up)
				liveMu.Lock()
				delete(live, st)
				liveMu.Unlock()
			}()
		}
	}()

	<-sess.CloseChan()
	r.mu.Lock()
	current := false
	if agents := r.sessions[org]; agents[as.key()] == as {
		current = true
		delete(agents, as.key())
		if len(agents) == 0 {
			delete(r.sessions, org)
		}
	}
	r.mu.Unlock()
	if !current { // replaced by the agent's newer session (a renewal, a reconnect): no disconnection
		r.log.Info("agent session replaced and closed", "org", org, "agent", as.id, "serial", serial)
		return
	}
	r.log.Warn("agent disconnected", "org", org, "agent", as.id)
}

type checkedCert struct {
	org string
	at  time.Time
}

// remember keeps the registry's answer for a certificate (registry_grace); a certificate
// found invalid is forgotten.
func (r *Relay) remember(serial, org string, valid bool) {
	if r.grace <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.checked == nil {
		r.checked = map[string]checkedCert{}
	}
	if !valid {
		delete(r.checked, serial)
		return
	}
	now := time.Now()
	r.checked[serial] = checkedCert{org: org, at: now}
	for s, c := range r.checked { // keep the map to the certificates seen within the grace
		if now.Sub(c.at) > r.grace {
			delete(r.checked, s)
		}
	}
}

// graceCheck decides without the registry: valid only if it called the certificate valid
// at most registry_grace ago. (The TLS handshake has already checked the expiry.)
func (r *Relay) graceCheck(serial string, regErr error) (string, bool) {
	r.mu.Lock()
	c, ok := r.checked[serial]
	r.mu.Unlock()
	if r.grace <= 0 || !ok || time.Since(c.at) > r.grace {
		r.log.Warn("agent rejected: the registry is unavailable", "serial", serial, "err", regErr)
		return "", false
	}
	r.log.Warn("registry unavailable: agent admitted on an earlier answer (registry_grace)", "org", c.org, "serial", serial,
		"answered", time.Since(c.at).Round(time.Second), "err", regErr)
	return c.org, true
}

// watchRevocations drops sessions whose certificate has been revoked. (Expiry is checked
// at connection time: a live session outlasting its certificate is not cut, so queries in
// flight are not broken every certificate lifetime; a stolen key cannot open new ones.)
// retireWait: how long a replaced session may finish its streams before it is closed.
const retireWait = 10 * time.Minute

// retire takes a session out of service: GoAway (no new streams either way), closed once its
// streams end or after retireWait; watchRevocations still watches it.
func (r *Relay) retire(as *agentSession) {
	_ = as.sess.GoAway()
	r.mu.Lock()
	if r.retiring == nil {
		r.retiring = map[*agentSession]bool{}
	}
	r.retiring[as] = true
	r.mu.Unlock()
	go func() {
		for deadline := time.Now().Add(retireWait); as.sess.NumStreams() > 0 && time.Now().Before(deadline) && !as.sess.IsClosed(); {
			time.Sleep(time.Second)
		}
		as.sess.Close()
		r.mu.Lock()
		delete(r.retiring, as)
		r.mu.Unlock()
	}()
}

// supersededGrace: a session on a certificate replaced longer ago than this is closed: its
// agent rolled over to a session on the new one, so it is somebody else's (review 3, R3-9).
// RelayConfig.superseded overrides it (tests).
const supersededGrace = time.Minute

func (r *Relay) watchRevocations() {
	for range time.Tick(2 * time.Second) {
		r.mu.Lock()
		bySerial := map[string]*agentSession{}
		var serials, active []string
		for _, agents := range r.sessions {
			for _, as := range agents {
				bySerial[as.serial] = as
				serials = append(serials, as.serial)
				active = append(active, as.serial)
			}
		}
		for as := range r.retiring {
			bySerial[as.serial] = as
			serials = append(serials, as.serial)
		}
		r.mu.Unlock()
		// a session whose certificate ended longer ago than it could still be renewed
		for s, as := range bySerial {
			if time.Since(as.ends) > r.expired {
				r.log.Warn("certificate ended: closing the agent session", "serial", s, "ended", as.ends.Format(time.RFC3339))
				as.sess.Close()
			}
		}
		if len(serials) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		revoked, err := r.store.Revoked(ctx, serials)
		cancel()
		if err != nil {
			r.log.Warn("revocation check failed", "err", err)
			continue
		}
		sctx, scancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
		grace := supersededGrace
		if r.cfg.superseded > 0 {
			grace = r.cfg.superseded
		}
		old, err := r.store.Superseded(sctx, active, grace)
		scancel()
		if err == nil {
			for _, s := range old {
				r.log.Warn("certificate replaced by a renewal its agent uses: closing the session still on it", "serial", s)
				bySerial[s].sess.Close()
			}
		}
		gone := map[string]bool{}
		for _, s := range revoked {
			r.log.Warn("certificate revoked: closing the agent session", "serial", s)
			bySerial[s].sess.Close()
			r.remember(s, "", false) // registry_grace must not let it back in (review 3, R3-7)
			gone[s] = true
		}
		for s, as := range bySerial {
			if !gone[s] {
				r.remember(s, as.org, true)
			}
		}
	}
}

// serveOrg turns each connection to an organization's endpoint into a stream to its agent.
func (r *Relay) serveOrg(org string, l net.Listener) {
	conns := slots(r.cfg.MaxOrgConns)
	refusing := slots(32) // refusals of connections beyond max_org_conns being answered (review 3, R3-5)
	_ = acceptLoop(l, r.log, func(c net.Conn) {
		if !r.orgAllow.allows(c.RemoteAddr()) {
			r.log.Warn("organization endpoint: connection refused, not in org_allow", "org", org, "remote", c.RemoteAddr())
			c.Close()
			return
		}
		if !take(conns) {
			r.log.Warn("organization endpoint: connection refused, max_org_conns open", "org", org, "remote", c.RemoteAddr(), "max", r.cfg.MaxOrgConns)
			if take(refusing) { // a refusal with its reason, a few at a time; beyond them a bare close
				refuse(c, fmt.Sprintf("lokadb relay: organization %s has too many connections open", org))
				give(refusing)
			} else {
				c.Close()
			}
			return
		}
		defer give(conns)
		if !take(r.streams) {
			r.log.Warn("organization endpoint: connection refused, max_streams open on the relay", "org", org, "remote", c.RemoteAddr(), "max", r.cfg.MaxStreams)
			refuse(c, "lokadb relay: too many connections open on the relay")
			return
		}
		defer give(r.streams)
		// the first message: a probe of lokadb pin is answered here; anything else is checked
		// against the SaaS database's addresses, then read again by what follows
		if r.pins() != nil {
			msg := firstMessage(c)
			if msg == nil {
				c.Close()
				return
			}
			code, admit := probeCode(msg)
			switch {
			case code != "" && admit: // one of LokaDB's components, this connection only
				if c = r.admitted(org, c, code); c == nil {
					return
				}
			case code != "":
				r.answerProbe(org, c, code)
				return
			default:
				c = &replayConn{Conn: c, first: msg}
				if !r.pinAllows(org, c) {
					return
				}
				defer r.trackOrgConn(c)()
			}
		}
		if len(r.agentsOf(org)) == 0 {
			refuse(c, fmt.Sprintf("lokadb relay: the agent of organization %s is not connected", org))
			return
		}
		if r.tlsUnknown() {
			r.log.Warn("organization endpoint: connection refused, the registry has not said yet whether the organization requires TLS", "org", org)
			refuse(c, fmt.Sprintf("lokadb relay: organization %s is not available until the relay reads the registry", org))
			return
		}
		var first []byte
		if r.tlsRequired(org) {
			if first = r.requireTLS(org, c); first == nil {
				return
			}
		}
		st, err := r.openStream(org)
		if err != nil {
			refuse(c, fmt.Sprintf("lokadb relay: tunnel to organization %s failed: %v", org, err))
			return
		}
		if first != nil && !r.tlsAccepted(org, c, st, first) {
			return
		}
		pipe(c, st)
	})
}

// agentsOf returns the organization's sessions, the least busy first.
func (r *Relay) agentsOf(org string) []*agentSession {
	r.mu.Lock()
	out := make([]*agentSession, 0, len(r.sessions[org]))
	for _, as := range r.sessions[org] {
		out = append(out, as)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].sess.NumStreams() < out[j].sess.NumStreams() })
	return out
}

// openStream opens a stream to one of the organization's agents: the least busy one, or the
// next if that session has just closed.
func (r *Relay) openStream(org string) (net.Conn, error) {
	err := errors.New("the agent is not connected")
	for _, as := range r.agentsOf(org) {
		var st net.Conn
		if st, err = as.sess.Open(); err == nil {
			return st, nil
		}
	}
	return nil, err
}

// Postgres startup codes (the 4 bytes after the length of the first message).
const (
	codeCancel = 80877102
	codeSSL    = 80877103
	codeGSS    = 80877104
)

// requireTLS reads the client's first message (org_tls "require"): an SSLRequest or a
// CancelRequest goes on (returned, to be forwarded), anything else is refused, so a
// password or a query never crosses the relay in clear. One GSSENCRequest is declined, as
// Postgres does, and the client may then ask for TLS (libpq with a Kerberos ticket); a second
// one is refused. The whole negotiation has one deadline (review of the tunnel, V-2, S-1:
// unbounded recursion on GSSENCRequests, and an SSLRequest after one closed the connection).
func (r *Relay) requireTLS(org string, c net.Conn) []byte {
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	refuse := func() []byte {
		r.log.Warn("organization endpoint: connection without TLS refused (org_tls require)", "org", org, "remote", c.RemoteAddr())
		be := pgproto3.NewBackend(c, c)
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000",
			Message: fmt.Sprintf("lokadb relay: organization %s accepts only TLS connections (use sslmode=require or stricter)", org)})
		_ = be.Flush()
		c.Close()
		return nil
	}
	gss := false
	for {
		head := make([]byte, 8)
		if _, err := io.ReadFull(c, head); err != nil {
			c.Close()
			return nil
		}
		n, code := binary.BigEndian.Uint32(head), binary.BigEndian.Uint32(head[4:])
		switch {
		case n == 8 && code == codeSSL:
			_ = c.SetDeadline(time.Time{})
			return head
		case n == 16 && code == codeCancel: // a cancel carries no secret but the key it cancels
			rest := make([]byte, 8)
			if _, err := io.ReadFull(c, rest); err != nil {
				c.Close()
				return nil
			}
			_ = c.SetDeadline(time.Time{})
			return append(head, rest...)
		case n == 8 && code == codeGSS && !gss: // GSSENCRequest: decline, the client may then ask for TLS
			gss = true
			if _, err := c.Write([]byte{'N'}); err != nil {
				c.Close()
				return nil
			}
		default:
			return refuse()
		}
	}
}

// tlsAccepted forwards the first message and, for an SSLRequest, checks that the
// organization's database agreed to TLS; if it declined, the client is refused instead of
// continuing in clear.
func (r *Relay) tlsAccepted(org string, c, st net.Conn, first []byte) bool {
	if _, err := st.Write(first); err != nil {
		st.Close()
		refuse(c, fmt.Sprintf("lokadb relay: tunnel to organization %s failed: %v", org, err))
		return false
	}
	if binary.BigEndian.Uint32(first[4:]) != codeSSL {
		return true // a cancel: nothing comes back
	}
	_ = st.SetReadDeadline(time.Now().Add(10 * time.Second))
	ans := make([]byte, 1)
	_, err := io.ReadFull(st, ans)
	_ = st.SetReadDeadline(time.Time{})
	if err == nil && ans[0] == 'S' {
		if _, err := c.Write(ans); err != nil {
			st.Close()
			c.Close()
			return false
		}
		return true
	}
	st.Close()
	r.log.Warn("organization endpoint: the organization's database does not offer TLS (org_tls require)", "org", org, "err", err)
	// The client asked for TLS: decline it, then refuse the clear startup that follows.
	if _, err := c.Write([]byte{'N'}); err != nil {
		c.Close()
		return false
	}
	refuse(c, fmt.Sprintf("lokadb relay: the database of organization %s does not offer TLS, and this relay requires it (org_tls)", org))
	return false
}

// AgentConfig configures the customer side.
type AgentConfig struct {
	// StatusListen: a local address that serves GET /status (agentstatus.go). StatusURL and
	// StatusTokenFile: where the status is posted every 60 s (the service, with the agent's las_
	// secret read from the file); LOKADB_AGENT_STATUS=off stops the posting.
	StatusListen    string
	StatusURL       string
	StatusTokenFile string
	// Version: what the agent tells the relay about its build (default: the binary's, buildinfo);
	// the relay lists it in /sessions.
	Version string
	// Relay: relay tunnel addresses, host:port[,host:port...]. A name with several addresses
	// (a DNS name with one per relay) counts as several relays: the agent keeps a session to
	// each, so any relay can reach the organization (tasks/relay-ha.md).
	Relay      string
	Enroll     string // relay enrollment API address (host:port)
	ServerName string // expected name in the relay certificates ("": the host of each address)
	CA         string // CA that signs the relay certificate
	StateDir   string // where the agent keeps its key and certificate
	TokenFile  string // one-time enrollment token (only read when it is a new one)
	DB         string // the only address cloud streams are forwarded to
	// CloudListen, if set, is a local address forwarded through the tunnel to the cloud
	// database (for the subscription that copies the organization's rows).
	CloudListen string
	// CloudAllow: who may connect to CloudListen (IPs, CIDRs, host names). Empty: the host
	// of DB (the database that subscribes), loopback and this host's own addresses only.
	CloudAllow []string
	EnrollOnly bool // enroll (if needed) and exit
	// MaxConns: connections the agent keeps open to DB at once (default 200, negative = no
	// limit): a client that reaches the organization's endpoint must not take every slot
	// of the organization's database before Postgres has authenticated it.
	MaxConns int
	// Sessions: sessions to every relay (default 1, at most 8): the organization's
	// connections spread over several TCP connections (sets.go).
	Sessions int
	// EnrollWait: how long enrollment keeps trying while the relay does not serve the
	// organization yet (its endpoint is opened within a poll, 2 s, of lokadb org-endpoint). The
	// token is not used meanwhile. Default 30 s; EnrollOnly never waits.
	EnrollWait time.Duration
}

// errNotServed: the relay has no endpoint for the token's organization yet; the token is intact.
const enrollRetry = time.Second

var errNotServed = errors.New("organization not served by this relay (the token is still valid)")

type Agent struct {
	cfg   AgentConfig
	log   *slog.Logger
	roots *x509.CertPool
	creds *credentials
	mu    sync.Mutex
	sess  map[string]*yamux.Session // relay address -> its session
	// resolveEvery: how often the relay names are resolved again (new relays, gone ones)
	resolveEvery time.Duration
	reportEvery  time.Duration             // how often the status is posted (agentstatus.go; default 1 minute)
	conns        chan struct{}             // connections to DB open (MaxConns)
	rolls        map[chan struct{}]bool    // one per session: closed when a renewal asks for a new session
	replaced     map[string]*yamux.Session // per relay: the session a renewal replaces, retired once the new one is up
	stats        *agentStats
	set          string // the session set now (sets.go; a renewal starts a new one)
}

func NewAgent(cfg AgentConfig, log *slog.Logger) *Agent {
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 200
	}
	cfg.Sessions = min(max(cfg.Sessions, 1), maxSessionsPerSet)
	if cfg.Version == "" {
		cfg.Version = buildinfo.Version()
	}
	return &Agent{cfg: cfg, log: log, creds: &credentials{}, stats: newAgentStats(), sess: map[string]*yamux.Session{}, resolveEvery: 30 * time.Second, reportEvery: time.Minute,
		conns: slots(cfg.MaxConns), set: newSetID()}
}

func (a *Agent) Run() error {
	pool, err := a.caPool()
	if err != nil {
		return err
	}
	a.roots = pool
	if err := a.loadOrEnroll(); err != nil {
		return err
	}
	if a.cfg.EnrollOnly {
		return nil
	}
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
		// Always the current certificate: a renewed one is used from the next connection.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return a.creds.get(), nil },
	}
	if a.cfg.CloudListen != "" {
		allow := a.cfg.CloudAllow
		self := len(allow) == 0
		if self {
			host, _, _ := net.SplitHostPort(a.cfg.DB)
			allow = []string{host, "127.0.0.1", "::1"}
		}
		list, err := newAllowList(allow)
		if err != nil {
			return err
		}
		// a database on the agent's host that reaches -cloud-listen by the host's address comes
		// from that address, not loopback (block 14's stand: a logical move failed with 08006)
		list.self = self
		l, err := net.Listen("tcp", a.cfg.CloudListen)
		if err != nil {
			return err
		}
		a.log.Info("forwarding to the cloud database", "listen", a.cfg.CloudListen, "allow", allow)
		go a.serveCloud(l, list)
	}
	go a.probeDB()
	if a.cfg.StatusListen != "" {
		a.serveStatus(a.cfg.StatusListen)
	}
	if a.cfg.StatusURL != "" {
		go a.reportStatus()
	}
	fatal := make(chan error, 1)
	go a.renewLoop(fatal)
	go a.keepRelays(tlsCfg)
	return <-fatal
}

// relayTargets resolves the relay addresses: address -> the name its certificate must carry.
func (a *Agent) relayTargets() (map[string]string, error) {
	out := map[string]string{}
	var firstErr error
	for _, entry := range strings.Split(a.cfg.Relay, ",") {
		host, port, err := net.SplitHostPort(strings.TrimSpace(entry))
		if err != nil {
			return nil, fmt.Errorf("relay address %q: %w", entry, err)
		}
		name := a.cfg.ServerName
		if name == "" {
			name = host
		}
		ips, err := net.LookupHost(host)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, ip := range ips {
			out[net.JoinHostPort(ip, port)] = name
		}
	}
	if len(out) == 0 {
		return nil, firstErr
	}
	return out, nil
}

// keepRelays keeps one session to every relay, and follows the relay list as it changes.
func (a *Agent) keepRelays(tlsCfg *tls.Config) {
	running := map[string]chan struct{}{}
	for {
		targets, err := a.relayTargets()
		if err != nil {
			a.log.Warn("relay addresses do not resolve, keeping the current sessions", "relay", a.cfg.Relay, "err", err)
		}
		for addr, name := range targets {
			if running[addr] == nil {
				stop := make(chan struct{})
				running[addr] = stop
				cfg := tlsCfg.Clone()
				cfg.ServerName = name
				for slot := 0; slot < a.cfg.Sessions; slot++ {
					go a.keep(addr, slot, cfg, stop)
				}
			}
		}
		for addr, stop := range running {
			if _, ok := targets[addr]; !ok && err == nil {
				a.log.Info("relay gone from the relay list", "relay", addr)
				close(stop)
				delete(running, addr)
			}
		}
		time.Sleep(a.resolveEvery)
	}
}

// keep reconnects to one relay until stop.
func (a *Agent) keep(addr string, slot int, tlsCfg *tls.Config, stop chan struct{}) {
	backoff := time.Second
	var quickUntil time.Time // after a drain signal: retry every second
	for {
		err := a.session(addr, slot, tlsCfg, stop)
		select {
		case <-stop:
			return
		default:
		}
		if errors.Is(err, errRolled) {
			backoff = time.Second
			continue // the new certificate's session, at once
		}
		if errors.Is(err, errDrained) {
			backoff, quickUntil = time.Second, time.Now().Add(time.Minute)
			continue // at once; then every second while the relay restarts
		}
		if errors.Is(err, errConnectedOnce) {
			backoff = time.Second // it was up: retry quickly
		}
		// ±25 %: hundreds of agents of a restarted relay do not come back in one wave, each
		// with a registry query (review of the tunnel, N-1)
		wait := backoff + time.Duration(rand.Int64N(int64(backoff/2))) - backoff/4
		a.stats.down(addr, err)
		a.log.Warn("tunnel down, reconnecting", "relay", addr, "err", err, "in", wait.Round(time.Millisecond))
		select {
		case <-stop:
			return
		case <-time.After(wait):
		}
		backoff = min(backoff*2, 5*time.Second) // 1, 2, 4, 5 s: a relay back is found within 5 s
		if time.Now().Before(quickUntil) {
			backoff = time.Second
		}
	}
}

var errConnectedOnce = errors.New("session ended")

// session keeps one outbound connection to a relay open and serves it.
func (a *Agent) session(relay string, slot int, tlsCfg *tls.Config, stop chan struct{}) error {
	tlsCfg = tlsCfg.Clone()
	if p, err := a.caPool(); err == nil { // a rotated CA bundle without a restart (rotate.go)
		tlsCfg.RootCAs = p
	}
	a.mu.Lock()
	tlsCfg.NextProtos = []string{setProto(a.set, slot, a.cfg.Version)} // the session set and slot (sets.go)
	a.mu.Unlock()
	addr := relay // where it dials
	key := relay + "#" + strconv.Itoa(slot)
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 10 * time.Second}, "tcp", addr, tlsCfg)
	if err != nil {
		return err
	}
	sess, err := yamux.Client(c, muxConfig())
	if err != nil {
		c.Close()
		return err
	}
	// The TLS handshake succeeds with any certificate from the CA; the relay then checks
	// the registry and closes the connection if it refuses it. Only a ping answer means
	// the session was accepted (and only then is the retry delay reset).
	if _, err := sess.Ping(); err != nil {
		sess.Close()
		return fmt.Errorf("the relay refused the session (certificate revoked or not issued by enrollment?): %w", err)
	}
	roll := make(chan struct{})
	a.mu.Lock()
	a.sess[key] = sess
	a.stats.up(relay)
	if a.rolls == nil {
		a.rolls = map[chan struct{}]bool{}
	}
	a.rolls[roll] = true
	prev := a.replaced[key] // the session this one replaces after a renewal: it retires now, not before
	delete(a.replaced, key)
	a.mu.Unlock()
	if prev != nil {
		retireSession(prev)
	}
	a.log.Info("tunnel up", "relay", addr, "serial", a.creds.serial())
	go func() {
		select {
		case <-stop:
			sess.Close()
		case <-sess.CloseChan():
		}
	}()
	drained := make(chan struct{})
	var drainOnce sync.Once
	go a.serveStreams(sess, func() { drainOnce.Do(func() { close(drained) }) })
	release := func() {
		a.mu.Lock()
		if a.sess[key] == sess {
			delete(a.sess, key)
		}
		delete(a.rolls, roll)
		a.mu.Unlock()
	}
	select {
	case <-sess.CloseChan():
		release()
		return errConnectedOnce
	case <-drained:
		// the relay drains: a new session now (behind a balancer or a DNS name another relay
		// takes it), this one finishes its streams as the relay lets it
		release()
		a.mu.Lock()
		if a.replaced == nil {
			a.replaced = map[string]*yamux.Session{}
		}
		if old := a.replaced[key]; old != nil && old != sess {
			go retireSession(old)
		}
		a.replaced[key] = sess
		a.mu.Unlock()
		return errDrained
	case <-roll:
		// a renewed certificate: a new session on it (keep dials at once); this one keeps
		// working until that one is up, then takes no new streams and ends once its own are
		// done (review 3 of the tunnel, R3-9). Retiring it first left a moment when the relay
		// opened streams on a session the agent refused.
		a.mu.Lock()
		delete(a.rolls, roll)
		if a.replaced == nil {
			a.replaced = map[string]*yamux.Session{}
		}
		if old := a.replaced[key]; old != nil && old != sess {
			go retireSession(old)
		}
		a.replaced[key] = sess
		a.mu.Unlock()
		return errRolled
	}
}

// retireSession: GoAway, then closed once its streams end (at most retireWait).
func retireSession(sess *yamux.Session) {
	_ = sess.GoAway()
	go func() {
		for deadline := time.Now().Add(retireWait); sess.NumStreams() > 0 && time.Now().Before(deadline) && !sess.IsClosed(); {
			time.Sleep(time.Second)
		}
		sess.Close()
	}()
}

// errRolled: the session was replaced by one on a renewed certificate; reconnect at once.
var errRolled = errors.New("renewed certificate: a new session")

// errDrained: the relay said it drains; reconnect at once, and every second for a minute (a
// restarted relay listens again within seconds: a backoff grown meanwhile would wait on).
var errDrained = errors.New("the relay drains: a new session")

// rollover: after a renewal every session moves to the new certificate (session).
func (a *Agent) rollover() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.set = newSetID() // the sessions on the new certificate are a new set: the relay retires the old one
	for r := range a.rolls {
		close(r)
		delete(a.rolls, r)
	}
}

// serveStreams forwards the relay's streams of sess to the organization's database.
func (a *Agent) serveStreams(sess *yamux.Session, onDrain func()) {
	for {
		st, err := sess.Accept()
		if err != nil {
			return
		}
		if !take(a.conns) {
			a.stats.refusedCeiling.Add(1)
			a.log.Warn("connection refused: the agent has its maximum of connections to the database open", "db", a.cfg.DB, "max", a.cfg.MaxConns)
			go refuse(st, "lokadb agent: too many connections to the organization's database open")
			continue
		}
		go func() {
			defer give(a.conns)
			// a Postgres client speaks first: what it sends first, or the relay's drain signal
			// (written at once, it arrives in one read)
			head := make([]byte, 512)
			_ = st.SetReadDeadline(time.Now().Add(30 * time.Second))
			n, err := st.Read(head)
			if err != nil {
				st.Close()
				return
			}
			head = head[:n]
			_ = st.SetReadDeadline(time.Time{})
			if bytes.Equal(head, drainSignal) {
				st.Close()
				a.log.Info("the relay drains: dialling again now", "serial", a.creds.serial())
				onDrain()
				return
			}
			db, err := net.DialTimeout("tcp", a.cfg.DB, 5*time.Second)
			if err != nil {
				a.log.Warn("database unreachable", "db", a.cfg.DB, "err", err)
				// the cloud client learns that, not the organization's addresses (review of the tunnel, N-3)
				refuse(st, "lokadb agent: the organization's database is unreachable (the agent's log has the reason)")
				return
			}
			if _, err := db.Write(head); err != nil {
				db.Close()
				st.Close()
				return
			}
			pipe(st, db)
		}()
	}
}

func (a *Agent) serveCloud(l net.Listener, allow *allowList) {
	_ = acceptLoop(l, a.log, func(c net.Conn) {
		if !allow.allows(c.RemoteAddr()) {
			a.stats.refusedAllow.Add(1)
			a.log.Warn("cloud forwarding: connection refused, not allowed (-cloud-allow)", "remote", c.RemoteAddr())
			// an error the subscription shows, not "server closed the connection unexpectedly"
			refuse(c, fmt.Sprintf("lokadb agent: connection from %s to the cloud window refused; add it to -cloud-allow", c.RemoteAddr()))
			return
		}
		st, err := a.openCloud()
		if err != nil {
			refuse(c, "lokadb agent: "+err.Error())
			return
		}
		pipe(c, st)
	})
}

// openCloud opens a stream to the cloud database through the least busy relay session (a
// draining relay refuses new streams: the next one is tried).
func (a *Agent) openCloud() (net.Conn, error) {
	a.mu.Lock()
	all := make([]*yamux.Session, 0, len(a.sess))
	for _, s := range a.sess {
		all = append(all, s)
	}
	a.mu.Unlock()
	if len(all) == 0 {
		return nil, errors.New("the tunnel to the cloud is down")
	}
	sort.Slice(all, func(i, j int) bool { return all[i].NumStreams() < all[j].NumStreams() })
	var err error
	for _, s := range all {
		var st net.Conn
		if st, err = s.Open(); err == nil {
			return st, nil
		}
	}
	return nil, fmt.Errorf("tunnel failed: %w", err)
}

// Disconnect drops the current tunnel sessions (tests: forces a reconnection).
// Drain (SIGTERM): no new streams on any session (GoAway), the open ones finish for up to
// timeout, then the sessions close (review 3 of the tunnel, R3-16: a stop cut every query).
func (a *Agent) Drain(timeout time.Duration) {
	a.mu.Lock()
	var all []*yamux.Session
	for _, s := range a.sess {
		all = append(all, s)
	}
	a.mu.Unlock()
	for _, s := range all {
		_ = s.GoAway()
	}
	a.log.Warn("draining: no new connections, waiting for the open ones", "timeout", timeout)
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		open := 0
		for _, s := range all {
			if !s.IsClosed() {
				open += s.NumStreams()
			}
		}
		if open == 0 {
			break
		}
	}
	for _, s := range all {
		s.Close()
	}
}

func (a *Agent) Disconnect() {
	a.mu.Lock()
	for _, s := range a.sess {
		s.Close()
	}
	a.mu.Unlock()
}

// Serial is the serial number of the certificate the agent currently holds.
func (a *Agent) Serial() string { return a.creds.serial() }

func (a *Agent) apiClient(withCert bool) *http.Client {
	name := a.cfg.ServerName
	if name == "" {
		name, _, _ = net.SplitHostPort(a.cfg.Enroll)
	}
	roots := a.roots
	if p, err := a.caPool(); err == nil {
		roots = p
	}
	tlsCfg := &tls.Config{RootCAs: roots, ServerName: name, MinVersion: tls.VersionTLS13}
	if withCert {
		tlsCfg.Certificates = []tls.Certificate{*a.creds.get()}
	}
	// a client per call: no keep-alive, or each renewal would leave a connection open
	return &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{TLSClientConfig: tlsCfg, DisableKeepAlives: true}}
}

func (a *Agent) call(path string, withCert bool, req enrollRequest) (enrollResponse, error) {
	body, _ := json.Marshal(req)
	resp, err := a.apiClient(withCert).Post("https://"+a.cfg.Enroll+path, "application/json", strings.NewReader(string(body)))
	if err != nil {
		return enrollResponse{}, err
	}
	defer resp.Body.Close()
	var out enrollResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, fmt.Errorf("%s: %s", path, resp.Status)
	}
	if resp.StatusCode != http.StatusOK {
		if out.NotServed {
			return out, fmt.Errorf("%s: %w", path, errNotServed)
		}
		return out, &refusal{code: resp.StatusCode, msg: fmt.Sprintf("%s: %s", path, out.Error)}
	}
	return out, nil
}

// loadOrEnroll uses the stored certificate, or enrolls with a token the agent has not used yet.
func (a *Agent) loadOrEnroll() error {
	tok := ""
	if a.cfg.TokenFile != "" {
		if b, err := os.ReadFile(a.cfg.TokenFile); err == nil {
			tok = strings.TrimSpace(string(b))
		}
	}
	st, _ := loadState(a.cfg.StateDir)
	if tok != "" && hashToken(tok) != st.tokenHash {
		return a.enroll(tok)
	}
	if st.cert != nil && time.Now().Before(st.leaf.NotAfter) {
		a.creds.set(st.cert, st.leaf)
		a.log.Info("using the stored certificate", "org", st.leaf.Subject.CommonName, "serial", a.creds.serial(),
			"expires", st.leaf.NotAfter.Format(time.RFC3339))
		return nil
	}
	if st.cert != nil { // ended: the relay renews one that ended a short while ago (renew_expired)
		a.creds.set(st.cert, st.leaf)
		a.log.Warn("the stored certificate has ended, renewing it", "serial", a.creds.serial(), "ended", st.leaf.NotAfter.Format(time.RFC3339))
		if err := a.renew(); err != nil {
			return fmt.Errorf("the stored certificate ended at %s and the relay does not renew it (%v): issue a new token", st.leaf.NotAfter.Format(time.RFC3339), err)
		}
		return nil
	}
	if tok != "" {
		return errors.New("the stored certificate is missing or expired and this enrollment token was already used: issue a new token")
	}
	return errors.New("no certificate yet: provide a one-time enrollment token")
}

func (a *Agent) enroll(tok string) error {
	key, csr, err := newKeyAndCSR()
	if err != nil {
		return err
	}
	out, err := a.call("/v1/enroll", false, enrollRequest{Token: tok, CSR: csr})
	// The operator starts the agent right after lokadb org-endpoint, and the relay opens the
	// endpoint on its next poll: wait for it instead of exiting with the token still valid.
	wait := a.cfg.EnrollWait
	if wait == 0 {
		wait = 30 * time.Second
	}
	for deadline := time.Now().Add(wait); errors.Is(err, errNotServed) && !a.cfg.EnrollOnly && time.Now().Before(deadline); {
		a.log.Info("the relay does not serve the organization yet, retrying", "in", enrollRetry)
		time.Sleep(enrollRetry)
		out, err = a.call("/v1/enroll", false, enrollRequest{Token: tok, CSR: csr})
	}
	if err != nil {
		return fmt.Errorf("enrollment: %w", err)
	}
	if err := a.store(key, out.Cert, hashToken(tok)); err != nil {
		return err
	}
	a.keepCA(out.CA)
	a.log.Info("enrolled", "org", out.Org, "serial", a.creds.serial(), "expires", out.NotAfter.Format(time.RFC3339))
	return nil
}

// renewLoop replaces the certificate (and the key) once two thirds of its lifetime are gone.
// renew replaces the certificate (and the key) with one the relay signs, presenting the
// current one (valid, or ended a short while ago: renew_expired).
func (a *Agent) renew() error {
	key, csr, err := newKeyAndCSR()
	if err != nil {
		return err
	}
	out, err := a.call("/v1/renew", true, enrollRequest{CSR: csr})
	if err != nil {
		return err
	}
	if err := a.store(key, out.Cert, ""); err != nil {
		return err
	}
	a.keepCA(out.CA)
	a.log.Info("certificate renewed", "serial", a.creds.serial(), "expires", out.NotAfter.Format(time.RFC3339))
	a.rollover()
	return nil
}

// renewLoop renews at two thirds of the certificate's life, retrying every 10 s. Once the
// certificate has ended and the relay refuses its renewal (not a network error: a refusal or
// a TLS alert) the agent cannot come back by itself: fatal gets the reason, Run returns it and
// the process exits with an error a supervisor sees (review 3 of the tunnel, R3-3).
func (a *Agent) renewLoop(fatal chan<- error) {
	for {
		leaf := a.creds.leaf()
		at := leaf.NotBefore.Add(leaf.NotAfter.Sub(leaf.NotBefore) * 2 / 3)
		time.Sleep(time.Until(at))
		err := a.renew()
		a.stats.renewed(err)
		if err == nil {
			continue
		}
		if leaf := a.creds.leaf(); time.Now().After(leaf.NotAfter) && refusedByRelay(err) {
			a.log.Error("the certificate has ended and the relay refuses to renew it: the agent stops; issue a new token",
				"serial", a.creds.serial(), "ended", leaf.NotAfter.Format(time.RFC3339), "err", err)
			fatal <- fmt.Errorf("the certificate ended at %s and the relay refuses to renew it (%v): issue a new token", leaf.NotAfter.Format(time.RFC3339), err)
			return
		}
		a.log.Warn("renewal failed, retrying in 10s", "err", err, "cert_ends", leaf.NotAfter.Format(time.RFC3339))
		time.Sleep(10 * time.Second)
	}
}

// refusedByRelay: the relay answered (a refusal of the request or a TLS alert on the
// certificate), not a network failure or an unavailable registry, which pass.
func refusedByRelay(err error) bool {
	var r *refusal
	return errors.As(err, &r) && (r.code == http.StatusUnauthorized || r.code == http.StatusForbidden) ||
		strings.Contains(err.Error(), "remote error: tls: ")
}

// refusal: the enrollment API answered with an error status.
type refusal struct {
	code int
	msg  string
}

func (r *refusal) Error() string { return r.msg }

// caPool: the CAs the agent trusts: -ca, and the bundle the relay last sent (keepCA).
func (a *Agent) caPool() (*x509.CertPool, error) {
	b, err := os.ReadFile(a.cfg.CA)
	if err != nil {
		return nil, err
	}
	if kept, err := os.ReadFile(filepath.Join(a.cfg.StateDir, "ca.pem")); err == nil {
		b = append(append(b, '\n'), kept...)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("%s: no certificate", a.cfg.CA)
	}
	return pool, nil
}

// keepCA saves the CA bundle of a relay's answer: it came over TLS checked with the CAs the
// agent already trusts.
func (a *Agent) keepCA(bundle string) {
	if bundle == "" || !x509.NewCertPool().AppendCertsFromPEM([]byte(bundle)) {
		return
	}
	path := filepath.Join(a.cfg.StateDir, "ca.pem")
	if old, err := os.ReadFile(path); err == nil && string(old) == bundle {
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(bundle), 0o600); err == nil && os.Rename(tmp, path) == nil {
		a.log.Info("CA bundle from the relay kept", "path", path)
	}
}

func (a *Agent) store(keyPEM []byte, certPEM, tokenHash string) error {
	cert, err := tls.X509KeyPair([]byte(certPEM), keyPEM)
	if err != nil {
		return err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	if err := saveState(a.cfg.StateDir, keyPEM, []byte(certPEM), tokenHash); err != nil {
		return err
	}
	a.creds.set(&cert, leaf)
	return nil
}

type credentials struct {
	mu   sync.Mutex
	cert *tls.Certificate
	l    *x509.Certificate
}

func (c *credentials) set(cert *tls.Certificate, leaf *x509.Certificate) {
	c.mu.Lock()
	c.cert, c.l = cert, leaf
	c.mu.Unlock()
}

func (c *credentials) get() *tls.Certificate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cert
}

func (c *credentials) leaf() *x509.Certificate {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.l
}

func (c *credentials) serial() string {
	if l := c.leaf(); l != nil {
		return l.SerialNumber.Text(16)
	}
	return ""
}
