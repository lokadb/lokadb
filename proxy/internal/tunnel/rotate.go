package tunnel

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// Rotation without a restart (review of the tunnel, S-5). The relay reads its certificate,
// the CA bundle and the CA key again when one of their files changes on disk (checked on each
// handshake and signature: a stat per file); the agent reads the CA bundle again on every
// connection. A file that does not load keeps the last good one, logged once.
//
// Rotating the CA, nobody restarted:
//  1. ca.crt = old CA + new CA everywhere (relays, issuer, agents): both verify.
//  2. A relay certificate signed by the new CA in relay.crt/relay.key.
//  3. ca.crt = new CA first + old CA, ca.key = the new key: certificates are signed by the
//     first CA of the bundle from now on; agents get one at their next renewal.
//  4. Once every agent certificate has been renewed (cert_ttl), the old CA out of the bundles.

// reloadable: a value loaded from files, loaded again when their size or mtime changes.
type reloadable[T any] struct {
	what   string
	paths  []string
	load   func() (T, error)
	log    *slog.Logger
	mu     sync.Mutex
	stamp  string // of the loaded files
	failed string // stamp that did not load (logged once)
	v      T
}

func newReloadable[T any](what string, log *slog.Logger, load func() (T, error), paths ...string) (*reloadable[T], error) {
	r := &reloadable[T]{what: what, paths: paths, load: load, log: log, stamp: fileStamp(paths)}
	v, err := load()
	if err != nil {
		return nil, err
	}
	r.v = v
	return r, nil
}

func fileStamp(paths []string) string {
	var b strings.Builder
	for _, p := range paths {
		if fi, err := os.Stat(p); err == nil {
			fmt.Fprintf(&b, "%s %d %d;", p, fi.Size(), fi.ModTime().UnixNano())
		} else {
			fmt.Fprintf(&b, "%s -;", p)
		}
	}
	return b.String()
}

func (r *reloadable[T]) get() T {
	stamp := fileStamp(r.paths)
	r.mu.Lock()
	defer r.mu.Unlock()
	if stamp == r.stamp || stamp == r.failed {
		return r.v
	}
	v, err := r.load()
	if err != nil {
		r.failed = stamp
		r.log.Error("rotation: the new files do not load, keeping the previous ones", "what", r.what, "err", err)
		return r.v
	}
	r.v, r.stamp, r.failed = v, stamp, ""
	r.log.Warn("rotation: loaded again", "what", r.what)
	return r.v
}

// reloadingSigner signs with the CA key of the moment (a local issuer).
type reloadingSigner struct{ r *reloadable[*issuer] }

func (s reloadingSigner) sign(csr *x509.CertificateRequest, org, agent string) ([]byte, string, time.Time, error) {
	return s.r.get().sign(csr, org, agent)
}

// relayTLS: the relay's listener configuration with the certificate and the CA bundle of the
// moment; base holds the rest.
func relayTLS(base *tls.Config, cert *reloadable[tls.Certificate], pool *reloadable[*x509.CertPool]) *tls.Config {
	return &tls.Config{MinVersion: base.MinVersion, GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
		c := base.Clone()
		kp := cert.get()
		c.Certificates = []tls.Certificate{kp}
		c.ClientCAs = pool.get()
		if base.ClientAuth == tls.RequireAndVerifyClientCert { // the agent port: its session set (sets.go)
			for _, p := range hello.SupportedProtos {
				if strings.HasPrefix(p, alpnSet) {
					c.NextProtos = []string{p}
					break
				}
			}
		}
		return c, nil
	}}
}

// expiryWarn: certificates of the relay that end within 30 days, logged at start and daily.
func (r *Relay) expiryWarn(cert *reloadable[tls.Certificate]) {
	for {
		var ends []string
		if kp := cert.get(); kp.Leaf != nil && time.Until(kp.Leaf.NotAfter) < 30*24*time.Hour {
			ends = append(ends, "relay certificate "+kp.Leaf.NotAfter.Format(time.RFC3339))
		}
		if b, err := os.ReadFile(r.cfg.CA); err == nil {
			for rest := b; ; {
				var blk *pem.Block
				if blk, rest = pem.Decode(rest); blk == nil {
					break
				}
				if c, err := x509.ParseCertificate(blk.Bytes); err == nil && time.Until(c.NotAfter) < 30*24*time.Hour {
					ends = append(ends, "CA "+c.Subject.CommonName+" "+c.NotAfter.Format(time.RFC3339))
				}
			}
		}
		if len(ends) > 0 {
			r.log.Warn("certificates end within 30 days: rotate them (the relay reads new files without a restart)", "ends", strings.Join(ends, "; "))
		}
		time.Sleep(24 * time.Hour)
	}
}

// keyPair: tls.LoadX509KeyPair with the leaf parsed (its expiry is read).
func keyPair(cert, key string) (tls.Certificate, error) {
	kp, err := tls.LoadX509KeyPair(cert, key)
	if err == nil && kp.Leaf == nil && len(kp.Certificate) > 0 {
		kp.Leaf, err = x509.ParseCertificate(kp.Certificate[0])
	}
	return kp, err
}
