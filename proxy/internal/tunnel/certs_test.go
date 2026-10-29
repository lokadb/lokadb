package tunnel

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// GenerateCertsTo keeps the CA key out of the certificates directory.
func TestGenerateCertsToSplitsTheCAKey(t *testing.T) {
	certs, keys := filepath.Join(t.TempDir(), "certs"), filepath.Join(t.TempDir(), "ca-private")
	if err := GenerateCertsTo(certs, keys, "relay,10.9.8.7", []string{"3"}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(certs, "ca.key")); err == nil {
		t.Fatal("ca.key is in the certificates directory")
	}
	fi, err := os.Stat(filepath.Join(keys, "ca.key"))
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("ca.key: %v %v", fi, err)
	}
	if st, _ := os.Stat(keys); st.Mode().Perm() != 0o700 {
		t.Errorf("the key directory has mode %v", st.Mode().Perm())
	}
	// the relay certificate carries both the name and the IP
	names, err := certNames(filepath.Join(certs, "relay.crt"))
	if err != nil || strings.Join(names, ",") != "relay,10.9.8.7" {
		t.Fatalf("relay certificate names: %v %v", names, err)
	}
	if _, err := os.Stat(filepath.Join(certs, "org-3.crt")); err != nil {
		t.Fatal(err)
	}
	// it verifies against the CA for the IP
	b, _ := os.ReadFile(filepath.Join(certs, "relay.crt"))
	blk, _ := pem.Decode(b)
	leaf, _ := x509.ParseCertificate(blk.Bytes)
	pool := x509.NewCertPool()
	cb, _ := os.ReadFile(filepath.Join(certs, "ca.crt"))
	pool.AppendCertsFromPEM(cb)
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: "10.9.8.7", KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		t.Fatalf("relay certificate for its IP: %v", err)
	}
	_ = net.IP{}
}

// EnsureCerts never replaces a CA or a relay certificate, and says when the name differs.
func TestEnsureCertsKeepsWhatExists(t *testing.T) {
	certs, keys := t.TempDir(), filepath.Join(t.TempDir(), "k")
	st, err := EnsureCerts(certs, keys, "relay")
	if err != nil || st.CA != "created" || st.Relay != "created" {
		t.Fatalf("first: %+v %v", st, err)
	}
	ca, _ := os.ReadFile(filepath.Join(certs, "ca.crt"))
	key, _ := os.ReadFile(filepath.Join(keys, "ca.key"))
	relay, _ := os.ReadFile(filepath.Join(certs, "relay.crt"))
	st, err = EnsureCerts(certs, keys, "relay")
	if err != nil || st.CA != "kept" || st.Relay != "kept" {
		t.Fatalf("second: %+v %v", st, err)
	}
	st, err = EnsureCerts(certs, keys, "relay,other")
	if err != nil || !strings.HasPrefix(st.Relay, "kept, but it is for relay and not other") {
		t.Fatalf("a name the certificate lacks: %+v %v", st, err)
	}
	ca2, _ := os.ReadFile(filepath.Join(certs, "ca.crt"))
	key2, _ := os.ReadFile(filepath.Join(keys, "ca.key"))
	relay2, _ := os.ReadFile(filepath.Join(certs, "relay.crt"))
	if !bytes.Equal(ca, ca2) || !bytes.Equal(key, key2) || !bytes.Equal(relay, relay2) {
		t.Fatal("a file was rewritten")
	}
	// the relay certificate removed: issued again from the same CA
	os.Remove(filepath.Join(certs, "relay.crt"))
	os.Remove(filepath.Join(certs, "relay.key"))
	st, err = EnsureCerts(certs, keys, "relay")
	if err != nil || st.CA != "kept" || st.Relay != "created" {
		t.Fatalf("relay certificate reissued: %+v %v", st, err)
	}
	// the CA key lost
	os.Remove(filepath.Join(keys, "ca.key"))
	if _, err := EnsureCerts(certs, keys, "relay"); err == nil || !strings.Contains(err.Error(), "CA key is lost") {
		t.Fatalf("lost CA key: %v", err)
	}
}

// org_allow takes IPs, CIDRs and names: a Docker network's addresses change, a CIDR covers them.
func TestOrgAllowTakesCIDRs(t *testing.T) {
	a, err := newAllowList([]string{"172.16.0.0/12", "10.1.2.3", "fd00::/8"})
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{"172.18.0.7": true, "172.32.0.1": false, "10.1.2.3": true, "10.1.2.4": false, "fd00::5": true, "8.8.8.8": false} {
		if got := a.allows(&net.TCPAddr{IP: net.ParseIP(ip)}); got != want {
			t.Errorf("%s: allowed %v, want %v", ip, got, want)
		}
	}
}

// TestPrepareCerts: with cert_names the relay makes its CA and certificate itself, writes the
// public CA once, and keeps what exists on the next start.
func TestPrepareCerts(t *testing.T) {
	dir := t.TempDir()
	pub := filepath.Join(dir, "shared", "ca.crt")
	if err := os.MkdirAll(filepath.Dir(pub), 0o755); err != nil {
		t.Fatal(err)
	}
	rd := filepath.Join(dir, "relay")
	cfg := RelayConfig{CertNames: "relay.example.com,lokadb-relay", CA: rd + "/ca.crt", CAKey: rd + "/ca.key", Cert: rd + "/relay.crt", Key: rd + "/relay.key", CAPublic: pub}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := cfg.PrepareCerts(log); err != nil {
		t.Fatal(err)
	}
	ca, key := mustRead(t, rd+"/ca.crt"), mustRead(t, rd+"/ca.key")
	if string(mustRead(t, pub)) != string(ca) {
		t.Fatal("the public CA is not the relay's")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(pub), "ca.key")); err == nil {
		t.Fatal("the CA key is in the shared directory")
	}
	if names, _ := certNames(rd + "/relay.crt"); strings.Join(names, ",") != "relay.example.com,lokadb-relay" {
		t.Fatalf("names: %v", names)
	}
	if err := cfg.PrepareCerts(log); err != nil {
		t.Fatal(err)
	}
	if string(mustRead(t, rd+"/ca.crt")) != string(ca) || string(mustRead(t, rd+"/ca.key")) != string(key) {
		t.Fatal("the CA was replaced on a second start")
	}
	// a public copy that differs is kept
	if err := os.WriteFile(pub, []byte("other"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := cfg.PrepareCerts(log); err != nil || string(mustRead(t, pub)) != "other" {
		t.Fatalf("a different public CA: %v", err)
	}
	// the CA certificate without its key: no new CA
	os.Remove(rd + "/ca.key")
	if err := cfg.PrepareCerts(log); err == nil || !strings.Contains(err.Error(), "CA key is lost") {
		t.Fatalf("lost key: %v", err)
	}
	// files that are not one directory's
	bad := cfg
	bad.Cert = filepath.Join(dir, "elsewhere", "relay.crt")
	if err := bad.PrepareCerts(log); err == nil {
		t.Fatal("scattered files accepted")
	}
	// no cert_names: nothing is made
	if err := (RelayConfig{}).PrepareCerts(log); err != nil {
		t.Fatal(err)
	}
}

func mustRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
