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
	"os"
	"path/filepath"
	"strings"
	"time"
)

// GenerateCerts writes a demo CA (certificate and key: the relay signs agents' enrollment
// requests with it), a relay server certificate, and optionally static client certificates
// for orgs (CN org-<N>; the relay rejects them unless they were issued by enrollment).
// In production the CA key belongs in a KMS/HSM of the control plane.
func GenerateCerts(dir, relayHost string, orgs []string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
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
		return err
	}
	ca, _ := x509.ParseCertificate(caDER)
	if err := writePEM(filepath.Join(dir, "ca.crt"), "CERTIFICATE", caDER); err != nil {
		return err
	}
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return err
	}
	if err := writePEM(filepath.Join(dir, "ca.key"), "EC PRIVATE KEY", caKeyDER); err != nil {
		return err
	}
	issue := func(name string, tmpl *x509.Certificate) error {
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
	if err := issue("relay", &x509.Certificate{
		Subject:  pkix.Name{CommonName: relayHost},
		DNSNames: []string{relayHost},
		// client too: the relay authenticates with it to the issuing service (lokadb issuer)
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return err
	}
	for _, org := range orgs {
		if org == "" {
			continue
		}
		if err := issue("org-"+org, &x509.Certificate{
			Subject:     pkix.Name{CommonName: "org-" + org},
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}); err != nil {
			return err
		}
	}
	return nil
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
