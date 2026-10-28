package proxy

import (
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// TLS between the applications and the proxy (block 8, S8). The proxy answers an
// SSLRequest with 'S' and continues the startup inside TLS, as Postgres does; the
// cleartext password the proxy asks for then travels encrypted. With "require" a client
// that starts without TLS is refused before it sends its password.

// TLSConfig of the proxy listener ("tls" in lokadb.json).
type TLSConfig struct {
	Cert string `json:"cert"` // PEM certificate chain and key; replaced files are picked up without a restart
	Key  string `json:"key"`
	// Require: refuse clients that do not ask for TLS (FATAL 28000). Off: TLS when asked.
	Require bool `json:"require,omitempty"`
}

type proxyTLS struct {
	cfg     *tls.Config
	require bool
}

// load checks the key pair now and returns a config that re-reads it when a file changes
// (certificate rotation without a restart).
func (t *TLSConfig) load(log *slog.Logger) (*proxyTLS, error) {
	if t == nil {
		return nil, nil
	}
	if t.Cert == "" || t.Key == "" {
		return nil, fmt.Errorf("tls: cert and key are required")
	}
	kp := &keyPair{cert: t.Cert, key: t.Key, log: log}
	if _, err := kp.get(); err != nil {
		return nil, fmt.Errorf("tls: %w", err)
	}
	return &proxyTLS{require: t.Require, cfg: &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return kp.get()
		},
	}}, nil
}

type keyPair struct {
	cert, key string
	log       *slog.Logger
	mu        sync.Mutex
	loaded    *tls.Certificate
	stamp     string // modification times of both files when loaded
	checked   time.Time
}

// get returns the key pair, re-read when one of the files changed (checked at most once
// a second); a broken new pair keeps the old one in use.
func (k *keyPair) get() (*tls.Certificate, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.loaded != nil && time.Since(k.checked) < time.Second {
		return k.loaded, nil
	}
	k.checked = time.Now()
	stamp := ""
	for _, p := range []string{k.cert, k.key} {
		fi, err := os.Stat(p)
		if err != nil {
			if k.loaded != nil {
				return k.loaded, nil
			}
			return nil, err
		}
		stamp += fi.ModTime().String() + "|"
	}
	if k.loaded != nil && stamp == k.stamp {
		return k.loaded, nil
	}
	c, err := tls.LoadX509KeyPair(k.cert, k.key)
	if err != nil {
		if k.loaded != nil {
			k.log.Warn("tls: new key pair unreadable, keeping the current one", "cert", k.cert, "err", err)
			return k.loaded, nil
		}
		return nil, err
	}
	if k.loaded != nil {
		k.log.Info("tls: key pair reloaded", "cert", k.cert)
	}
	k.loaded, k.stamp = &c, stamp
	return k.loaded, nil
}

// upgrade answers an SSLRequest: 'N' without TLS configured (c returned as is), else 'S'
// and the handshake (under the connection's current deadline). nil: the client is gone.
func (t *proxyTLS) upgrade(c net.Conn) net.Conn {
	if t == nil {
		if _, err := c.Write([]byte{'N'}); err != nil {
			return nil
		}
		return c
	}
	if _, err := c.Write([]byte{'S'}); err != nil {
		return nil
	}
	tc := tls.Server(c, t.cfg)
	if err := tc.Handshake(); err != nil {
		return nil
	}
	return tc
}

// refuseClear reports whether a startup without TLS must be refused, and refuses it.
func (t *proxyTLS) refuseClear(c net.Conn, be *pgproto3.Backend) bool {
	if t == nil || !t.require {
		return false
	}
	if _, ok := c.(*tls.Conn); ok {
		return false
	}
	be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000",
		Message: "lokadb: the proxy accepts only TLS connections (use sslmode=require or stricter)"})
	_ = be.Flush()
	return true
}
