package tunnel

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GenerateCerts writes a demo CA (certificate and key: the relay signs agents' enrollment
// requests with it), a relay server certificate, and optionally static client certificates
// for orgs (CN org-<N>; the relay rejects them unless they were issued by enrollment).
// The CA key goes next to the certificates; GenerateCertsTo puts it elsewhere.
// In production the CA key belongs in a KMS/HSM of the control plane.
func GenerateCerts(dir, relayHost string, orgs []string) error {
	return GenerateCertsTo(dir, dir, relayHost, orgs)
}

// GenerateCertsTo is GenerateCerts with the CA key in caKeyDir (0700): a directory that
// containers mount must hold ca.crt and the relay's certificate only, the CA key belongs to
// the relay alone (the runner, the proxy and the agents need ca.crt).
func GenerateCertsTo(certDir, caKeyDir, relayHost string, orgs []string) error {
	ca, caKey, err := newCA(certDir, caKeyDir)
	if err != nil {
		return err
	}
	if err := issueRelayCert(certDir, ca, caKey, relayHost); err != nil {
		return err
	}
	for _, org := range orgs {
		if org == "" {
			continue
		}
		if err := issueCert(certDir, "org-"+org, ca, caKey, &x509.Certificate{
			Subject:     pkix.Name{CommonName: "org-" + org},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}); err != nil {
			return err
		}
	}
	return nil
}

// CertsState tells what EnsureCerts found and did: "created", "kept" or, for the relay
// certificate, "kept, but it is for <names> and not <name>" when it lacks the name asked for.
type CertsState struct {
	CA, Relay  string
	RelayNames []string // the names (DNS and IP) of the relay certificate now
}

// EnsureCerts makes what is missing and keeps what is there: an existing CA (certificate and
// key) is never replaced, nor an existing relay certificate (a changed name is reported, the
// certificate is not touched: its files are removed to issue it again). The CA key is in
// caKeyDir. A CA certificate without its key is an error: a new CA would orphan every agent.
func EnsureCerts(certDir, caKeyDir, relayHost string) (CertsState, error) {
	var st CertsState
	caCrt, caKeyPath := filepath.Join(certDir, "ca.crt"), filepath.Join(caKeyDir, "ca.key")
	_, errCrt := os.Stat(caCrt)
	_, errKey := os.Stat(caKeyPath)
	var ca *x509.Certificate
	var caKey *ecdsa.PrivateKey
	var err error
	switch {
	case errCrt == nil && errKey == nil:
		if ca, caKey, err = loadCA(caCrt, caKeyPath); err != nil {
			return st, err
		}
		st.CA = "kept"
	case errCrt == nil:
		return st, fmt.Errorf("%s exists but %s does not: the CA key is lost or elsewhere (-ca-key-dir); a new CA would leave every agent without a valid certificate, so none is made", caCrt, caKeyPath)
	default:
		if ca, caKey, err = newCA(certDir, caKeyDir); err != nil {
			return st, err
		}
		st.CA = "created"
	}
	relayCrt, relayKey := filepath.Join(certDir, "relay.crt"), filepath.Join(certDir, "relay.key")
	_, e1 := os.Stat(relayCrt)
	_, e2 := os.Stat(relayKey)
	if e1 == nil && e2 == nil {
		st.Relay = "kept"
		if names, err := certNames(relayCrt); err == nil {
			st.RelayNames = names
			for _, w := range splitHosts(relayHost) {
				found := false
				for _, n := range names {
					found = found || n == w
				}
				if !found {
					st.Relay = "kept, but it is for " + strings.Join(names, ", ") + " and not " + w
					break
				}
			}
		}
		return st, nil
	}
	if err := issueRelayCert(certDir, ca, caKey, relayHost); err != nil {
		return st, err
	}
	st.Relay, st.RelayNames = "created", splitHosts(relayHost)
	return st, nil
}

func splitHosts(h string) []string {
	var out []string
	for _, p := range strings.Split(h, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// certNames: the DNS names and IPs of a certificate file.
func certNames(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil {
		return nil, fmt.Errorf("%s: no certificate", path)
	}
	c, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return nil, err
	}
	names := append([]string{}, c.DNSNames...)
	for _, ip := range c.IPAddresses {
		names = append(names, ip.String())
	}
	return names, nil
}

func loadCA(crtPath, keyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	cb, err := os.ReadFile(crtPath)
	if err != nil {
		return nil, nil, err
	}
	kb, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, nil, err
	}
	cblk, _ := pem.Decode(cb)
	kblk, _ := pem.Decode(kb)
	if cblk == nil || kblk == nil {
		return nil, nil, fmt.Errorf("%s or %s is not PEM", crtPath, keyPath)
	}
	ca, err := x509.ParseCertificate(cblk.Bytes)
	if err != nil {
		return nil, nil, err
	}
	key, err := x509.ParseECPrivateKey(kblk.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return ca, key, nil
}

// newCA writes ca.crt to certDir and ca.key to caKeyDir.
func newCA(certDir, caKeyDir string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	if err := os.MkdirAll(certDir, 0o755); err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(caKeyDir, 0o700); err != nil {
		return nil, nil, err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          serial(),
		Subject:               pkix.Name{CommonName: "lokadb demo CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	ca, _ := x509.ParseCertificate(caDER)
	if err := writePEM(filepath.Join(certDir, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return nil, nil, err
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return nil, nil, err
	}
	if err := writePEM(filepath.Join(caKeyDir, "ca.key"), "EC PRIVATE KEY", caKeyDER); err != nil {
		return nil, nil, err
	}
	return ca, caKey, nil
}

func issueCert(dir, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, tmpl *x509.Certificate) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	tmpl.SerialNumber = serial()
	tmpl.NotBefore = time.Now().Add(-time.Hour)
	tmpl.NotAfter = time.Now().AddDate(1, 0, 0)
	tmpl.KeyUsage = x509.KeyUsageDigitalSignature
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, name+".crt"), "CERTIFICATE", der); err != nil {
		return err
	}
	return writePEM(filepath.Join(dir, name+".key"), "EC PRIVATE KEY", keyDER)
}

// issueRelayCert: the relay's certificate for relayHost, a name or an IP or several, comma
// separated (the first is the common name); a client certificate too (the relay authenticates
// with it to the issuing service: lokadb issuer).
func issueRelayCert(dir string, ca *x509.Certificate, caKey *ecdsa.PrivateKey, relayHost string) error {
	hosts := splitHosts(relayHost)
	if len(hosts) == 0 {
		return fmt.Errorf("the relay needs a name")
	}
	tmpl := &x509.Certificate{
		Subject:     pkix.Name{CommonName: hosts[0]},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	return issueCert(dir, "relay", ca, caKey, tmpl)
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

// writePEM writes a certificate world-readable and a private key readable by its owner only.
func writePEM(path, typ string, der []byte) error {
	mode := os.FileMode(0o644)
	if strings.Contains(typ, "PRIVATE KEY") {
		mode = 0o600
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer f.Close()
	if err := f.Chmod(mode); err != nil { // an existing file keeps its mode otherwise
		return fmt.Errorf("%s: %w", path, err)
	}
	return pem.Encode(f, &pem.Block{Type: typ, Bytes: der})
}
