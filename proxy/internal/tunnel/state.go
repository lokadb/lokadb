package tunnel

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
)

// newKeyAndCSR creates the agent's private key, which never leaves the agent, and a
// certificate request for it.
func newKeyAndCSR() (keyPEM []byte, csrPEM string, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, "", err
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: "lokadb-agent"},
	}, key)
	if err != nil {
		return nil, "", err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, "", err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})), nil
}

type agentState struct {
	cert      *tls.Certificate
	leaf      *x509.Certificate
	tokenHash string // the enrollment token already used (so it is never presented twice)
}

// tokenBlock is the PEM block of agent.pem that holds the hash of the enrollment token
// (tls.X509KeyPair skips it).
const tokenBlock = "LokaDB ENROLLMENT TOKEN SHA256"

// readTokenHash finds the token hash in agent.pem, or in token.sha256 of an older agent.
func readTokenHash(dir string, pair []byte) string {
	for rest := pair; ; {
		var blk *pem.Block
		if blk, rest = pem.Decode(rest); blk == nil {
			break
		}
		if blk.Type == tokenBlock {
			return string(blk.Bytes)
		}
	}
	if b, err := os.ReadFile(filepath.Join(dir, "token.sha256")); err == nil {
		return strings.TrimSpace(string(b))
	}
	return ""
}

func loadState(dir string) (agentState, error) {
	var st agentState
	// One file holds the key, the certificate and the token hash, so they are replaced together.
	pair, err := os.ReadFile(filepath.Join(dir, "agent.pem"))
	st.tokenHash = readTokenHash(dir, pair)
	if err != nil {
		return st, err
	}
	cert, err := tls.X509KeyPair(pair, pair)
	if err != nil {
		return st, err
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return st, err
	}
	st.cert, st.leaf = &cert, leaf
	return st, nil
}

// saveState writes the key pair and the token hash in one atomic rename (owner-only), so a
// crash leaves either the old state or the new one; tokenHash "" keeps the stored one.
func saveState(dir string, keyPEM, certPEM []byte, tokenHash string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	path := filepath.Join(dir, "agent.pem")
	if tokenHash == "" {
		old, _ := os.ReadFile(path)
		tokenHash = readTokenHash(dir, old)
	}
	data := append(append([]byte{}, certPEM...), keyPEM...)
	if tokenHash != "" {
		data = append(data, pem.EncodeToMemory(&pem.Block{Type: tokenBlock, Bytes: []byte(tokenHash)})...)
	}
	tmp := filepath.Join(dir, ".agent.pem.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	// on disk before the rename: a power cut must not leave an empty agent.pem (review of
	// the tunnel, N-6)
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil { // the rename itself
		_ = d.Sync()
		d.Close()
	}
	return nil
}

func writeFile(path, s string) error { return os.WriteFile(path, []byte(s), 0o600) }
