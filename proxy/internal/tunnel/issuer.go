package tunnel

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"time"
)

// The CA key does not have to live on the relay: "issuer_url" in relay.json sends the
// checked CSR to a separate issuing service (lokadb issuer), which alone holds the key (and
// is where a KMS or an HSM plugs in: it signs through a crypto.Signer). The relay keeps
// checking tokens and certificates against the registry and records what was issued; a
// compromised relay can still ask for agent certificates, but cannot take the CA key away,
// sign anything but agent client certificates, or hide an issuance from the issuer's log.

// signer issues agent certificates: the relay's own CA key or the issuing service.
type signer interface {
	sign(csr *x509.CertificateRequest, org, agent string) (certPEM []byte, serialHex string, notAfter time.Time, err error)
}

type signRequest struct {
	CSR   string `json:"csr"`
	Org   string `json:"org"`
	Agent string `json:"agent,omitempty"`
}

type signResponse struct {
	Cert  string `json:"cert,omitempty"`
	Error string `json:"error,omitempty"`
}

// IssuerConfig configures the issuing service.
type IssuerConfig struct {
	Listen  string `json:"listen"`
	Cert    string `json:"cert"` // its TLS server certificate and key
	Key     string `json:"key"`
	CA      string `json:"ca"`     // CA certificate: signs agents' certificates, verifies relays
	CAKey   string `json:"ca_key"` // the CA key: only here
	CertTTL string `json:"cert_ttl"`
	// Relays: common names of the relay client certificates (from the same CA) allowed to
	// ask for signatures.
	Relays []string `json:"relays"`
}

type Issuer struct {
	cfg IssuerConfig
	log *slog.Logger
}

func NewIssuer(cfg IssuerConfig, log *slog.Logger) *Issuer { return &Issuer{cfg: cfg, log: log} }

func parseTTL(s string) (time.Duration, error) {
	if s == "" {
		return time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("cert_ttl: %q", s)
	}
	return d, nil
}

func (is *Issuer) Run() error {
	ttl, err := parseTTL(is.cfg.CertTTL)
	if err != nil {
		return err
	}
	local, err := newReloadable("CA key", is.log, func() (*issuer, error) { return loadIssuer(is.cfg.CA, is.cfg.CAKey, ttl) }, is.cfg.CA, is.cfg.CAKey)
	if err != nil {
		return err
	}
	if len(is.cfg.Relays) == 0 {
		return errors.New("relays: list the relay certificate names allowed to request signatures")
	}
	certs, err := newReloadable("issuer certificate", is.log, func() (tls.Certificate, error) { return keyPair(is.cfg.Cert, is.cfg.Key) }, is.cfg.Cert, is.cfg.Key)
	if err != nil {
		return err
	}
	pools, err := newReloadable("CA bundle", is.log, func() (*x509.CertPool, error) { return loadPool(is.cfg.CA) }, is.cfg.CA)
	if err != nil {
		return err
	}
	ln, err := tls.Listen("tcp", is.cfg.Listen, relayTLS(&tls.Config{
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}, certs, pools))
	if err != nil {
		return err
	}
	is.log.Info("issuing service", "listen", is.cfg.Listen, "cert_ttl", ttl, "relays", is.cfg.Relays)
	srv := &http.Server{Handler: is.handler(reloadingSigner{local}), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: time.Minute,
		ErrorLog: slog.NewLogLogger(is.log.Handler(), slog.LevelWarn)}
	return srv.Serve(ln)
}

func (is *Issuer) handler(iss signer) http.Handler {
	reply := func(w http.ResponseWriter, code int, resp signResponse) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sign", func(w http.ResponseWriter, req *http.Request) {
		cn := req.TLS.PeerCertificates[0].Subject.CommonName // verified against the CA
		if !slices.Contains(is.cfg.Relays, cn) {
			is.log.Warn("signature refused: not a relay of this issuer", "cn", cn, "remote", req.RemoteAddr)
			reply(w, http.StatusForbidden, signResponse{Error: "not an allowed relay"})
			return
		}
		var in signRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, req.Body, 64<<10)).Decode(&in); err != nil || in.Org == "" {
			reply(w, http.StatusBadRequest, signResponse{Error: "bad request"})
			return
		}
		csr, err := parseCSR([]byte(in.CSR))
		if err != nil {
			is.log.Warn("signature refused: bad certificate request", "relay", cn, "err", err)
			reply(w, http.StatusBadRequest, signResponse{Error: errBadCSR.Error()})
			return
		}
		certPEM, serial, notAfter, err := iss.sign(csr, in.Org, in.Agent)
		if err != nil {
			is.log.Error("signing failed", "relay", cn, "org", in.Org, "err", err)
			reply(w, http.StatusInternalServerError, signResponse{Error: "certificate signing failed"})
			return
		}
		is.log.Info("certificate issued", "relay", cn, "remote", req.RemoteAddr, "org", in.Org, "agent", in.Agent,
			"serial", serial, "not_after", notAfter.Format(time.RFC3339))
		reply(w, http.StatusOK, signResponse{Cert: string(certPEM)})
	})
	return mux
}

// remoteIssuer is the relay's client of the issuing service. It checks what comes back: a
// client certificate of the CA, for the organization and the key it asked for.
// remoteIssuer: the relay's client of the issuing service. Its certificate and the CA bundle
// are the relay's of the moment (rotate.go), read on every signature: a CA rotation needs no
// relay restart (review 3 of the tunnel, R3-1: they were fixed at start, and after the new CA
// began to sign every enrollment and renewal failed with 500).
type remoteIssuer struct {
	url, host string
	cert      *reloadable[tls.Certificate]
	roots     *reloadable[*x509.CertPool]
}

func newRemoteIssuer(rawURL string, cert *reloadable[tls.Certificate], roots *reloadable[*x509.CertPool]) (*remoteIssuer, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("issuer_url: %q, want https://host:port", rawURL)
	}
	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		host = u.Host
	}
	return &remoteIssuer{url: u.String(), host: host, cert: cert, roots: roots}, nil
}

func (ri *remoteIssuer) sign(csr *x509.CertificateRequest, org, agent string) ([]byte, string, time.Time, error) {
	body, _ := json.Marshal(signRequest{CSR: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr.Raw})), Org: org, Agent: agent})
	roots := ri.roots.get()
	// a client per signature (renewals are rare): the certificate and the bundle of now
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{ri.cert.get()}, RootCAs: roots, ServerName: ri.host, MinVersion: tls.VersionTLS13}}}
	resp, err := client.Post(ri.url+"/v1/sign", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("issuing service: %w", err)
	}
	defer resp.Body.Close()
	var out signResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
		return nil, "", time.Time{}, fmt.Errorf("issuing service: %s %s", resp.Status, out.Error)
	}
	blk, _ := pem.Decode([]byte(out.Cert))
	if blk == nil {
		return nil, "", time.Time{}, errors.New("issuing service: no certificate in the answer")
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, "", time.Time{}, fmt.Errorf("issuing service: %w", err)
	}
	if _, err := c.Verify(x509.VerifyOptions{Roots: roots, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return nil, "", time.Time{}, fmt.Errorf("issuing service: the certificate does not verify: %w", err)
	}
	pub, err := x509.MarshalPKIXPublicKey(c.PublicKey)
	if err != nil || c.Subject.CommonName != "org-"+org || !bytes.Equal(pub, csr.RawSubjectPublicKeyInfo) ||
		agent != "" && agentID(c) != agent {
		return nil, "", time.Time{}, errors.New("issuing service: the certificate is not the one requested")
	}
	return []byte(out.Cert), c.SerialNumber.Text(16), c.NotAfter, nil
}
