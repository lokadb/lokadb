package proxy

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// selfSigned writes a certificate for localhost and its key into dir.
func selfSigned(t *testing.T, dir, cn string) (string, string) {
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(time.Now().UnixNano()), Subject: pkix.Name{CommonName: cn},
		DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kd, _ := x509.MarshalECPrivateKey(key)
	cert, keyPath := filepath.Join(dir, "proxy.crt"), filepath.Join(dir, "proxy.key")
	_ = os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	_ = os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
	return cert, keyPath
}

func startProxy(t *testing.T, tlsCfg *TLSConfig) string {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	l.Close()
	dead, _ := net.Listen("tcp", "127.0.0.1:0") // an upstream that is not there
	deadAddr := dead.Addr().(*net.TCPAddr)
	dead.Close()
	s := NewServer(Config{Listen: addr, Upstream: "host=127.0.0.1 port=" + strconv.Itoa(deadAddr.Port) + " sslmode=disable", TLS: tlsCfg},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = s.ListenAndServe() }()
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("proxy did not start")
	return ""
}

// connectErr connects with libpq-style options and returns the error text.
func connectErr(t *testing.T, addr, opts string) string {
	host, port, _ := net.SplitHostPort(addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := pgconn.Connect(ctx, "host="+host+" port="+port+" user=app password=pw dbname=app "+opts)
	if err == nil {
		t.Fatalf("%s: connected to a proxy without an upstream", opts)
	}
	return err.Error()
}

// S8: the proxy speaks TLS when asked, can require it, and says 'N' without a certificate.
func TestProxyTLS(t *testing.T) {
	cert, key := selfSigned(t, t.TempDir(), "proxy")
	pem, _ := os.ReadFile(cert)
	root := filepath.Join(t.TempDir(), "root.crt")
	_ = os.WriteFile(root, pem, 0o600)

	// TLS when asked: the handshake and the startup succeed, the error is the upstream's
	addr := startProxy(t, &TLSConfig{Cert: cert, Key: key})
	_, port, _ := net.SplitHostPort(addr)
	msg := connectErr(t, "localhost:"+port, "sslmode=verify-full sslrootcert="+root)
	if strings.Contains(msg, "tls") || strings.Contains(msg, "SSL") || !strings.Contains(msg, "connect") {
		t.Fatalf("TLS to the proxy: %s", msg)
	}
	// without require, a client in clear still gets in (to the same upstream error)
	if msg := connectErr(t, addr, "sslmode=disable"); strings.Contains(msg, "accepts only TLS") {
		t.Fatalf("clear without require: %s", msg)
	}

	// require: a client in clear is refused before its password
	req := startProxy(t, &TLSConfig{Cert: cert, Key: key, Require: true})
	if msg := connectErr(t, req, "sslmode=disable"); !strings.Contains(msg, "accepts only TLS") {
		t.Fatalf("clear with require: %s", msg)
	}
	_, rport, _ := net.SplitHostPort(req)
	if msg := connectErr(t, "localhost:"+rport, "sslmode=require"); strings.Contains(msg, "accepts only TLS") {
		t.Fatalf("TLS with require: %s", msg)
	}

	// no TLS configured: sslmode=require fails on the proxy's 'N', as with Postgres without ssl
	none := startProxy(t, nil)
	if msg := connectErr(t, none, "sslmode=require"); !strings.Contains(strings.ToLower(msg), "ssl") && !strings.Contains(msg, "tls") {
		t.Fatalf("sslmode=require against a proxy without TLS: %s", msg)
	}

	// a broken key pair is refused at start
	if _, err := (&TLSConfig{Cert: cert, Key: cert}).load(slog.Default()); err == nil {
		t.Fatal("a certificate as its own key was accepted")
	}
}

// The key pair is re-read when the files change: rotation without a restart.
func TestProxyTLSReload(t *testing.T) {
	dir := t.TempDir()
	cert, key := selfSigned(t, dir, "first")
	pt, err := (&TLSConfig{Cert: cert, Key: key}).load(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	cn := func() string {
		c, err := pt.cfg.GetCertificate(nil)
		if err != nil {
			t.Fatal(err)
		}
		leaf, _ := x509.ParseCertificate(c.Certificate[0])
		return leaf.Subject.CommonName
	}
	if cn() != "first" {
		t.Fatal(cn())
	}
	time.Sleep(1100 * time.Millisecond)
	selfSigned(t, dir, "second")
	future := time.Now().Add(time.Minute)
	_ = os.Chtimes(cert, future, future) // a distinct modification time even on coarse clocks
	if got := cn(); got != "second" {
		t.Fatalf("after rotation: %s", got)
	}
}
