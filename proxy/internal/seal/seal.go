// Package seal encrypts a short secret to a public key, so that a party in the middle (the
// LokaDB Cloud service) carries it without being able to read it: the runner's key for the
// customer's role passwords, the installer's one-time key for the relay enrollment token.
//
// X25519 with an ephemeral key, HKDF-SHA256 (info "lokadb-seal-v1", salt the two public keys),
// AES-256-GCM. A sealed box is: version byte 1, the ephemeral public key (32 bytes), the GCM
// nonce (12 bytes), the ciphertext with its tag. Standard library only.
package seal

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
)

const (
	version = 1
	info    = "lokadb-seal-v1"
	keyLen  = 32
	nonce   = 12
)

// Key: an X25519 private key.
type Key struct{ k *ecdh.PrivateKey }

// NewKey makes a random key.
func NewKey() (*Key, error) {
	k, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &Key{k}, nil
}

// ParseKey reads a private key from Private's encoding.
func ParseKey(s string) (*Key, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("seal key: %w", err)
	}
	k, err := ecdh.X25519().NewPrivateKey(b)
	if err != nil {
		return nil, fmt.Errorf("seal key: %w", err)
	}
	return &Key{k}, nil
}

// Private: the private key, base64 (keep it in a file of mode 600).
func (k *Key) Private() string { return base64.StdEncoding.EncodeToString(k.k.Bytes()) }

// Public: the public key, base64: what is handed out.
func (k *Key) Public() string { return base64.StdEncoding.EncodeToString(k.k.PublicKey().Bytes()) }

// Fingerprint of a public key (base64): "SHA256:" and the first 16 bytes of its hash, in
// base64, as people compare it.
func Fingerprint(public string) (string, error) {
	b, err := base64.StdEncoding.DecodeString(public)
	if err != nil || len(b) != keyLen {
		return "", errors.New("seal: not a public key")
	}
	sum := sha256.Sum256(b)
	return "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:16]), nil
}

// Seal encrypts plaintext to the public key (base64); the result is base64.
func Seal(public string, plaintext []byte) (string, error) {
	pb, err := base64.StdEncoding.DecodeString(public)
	if err != nil {
		return "", fmt.Errorf("seal: public key: %w", err)
	}
	to, err := ecdh.X25519().NewPublicKey(pb)
	if err != nil {
		return "", fmt.Errorf("seal: public key: %w", err)
	}
	eph, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return "", err
	}
	shared, err := eph.ECDH(to)
	if err != nil {
		return "", err
	}
	aead, err := gcm(shared, eph.PublicKey().Bytes(), pb)
	if err != nil {
		return "", err
	}
	out := make([]byte, 1+keyLen+nonce, 1+keyLen+nonce+len(plaintext)+aead.Overhead())
	out[0] = version
	copy(out[1:], eph.PublicKey().Bytes())
	if _, err := rand.Read(out[1+keyLen:]); err != nil {
		return "", err
	}
	out = aead.Seal(out, out[1+keyLen:], plaintext, out[:1+keyLen])
	return base64.StdEncoding.EncodeToString(out), nil
}

// Open decrypts a box Seal made to this key.
func (k *Key) Open(box string) ([]byte, error) {
	b, err := base64.StdEncoding.DecodeString(box)
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	if len(b) < 1+keyLen+nonce+16 || b[0] != version {
		return nil, errors.New("seal: not a sealed box of this version")
	}
	eph, err := ecdh.X25519().NewPublicKey(b[1 : 1+keyLen])
	if err != nil {
		return nil, fmt.Errorf("seal: %w", err)
	}
	shared, err := k.k.ECDH(eph)
	if err != nil {
		return nil, err
	}
	aead, err := gcm(shared, b[1:1+keyLen], k.k.PublicKey().Bytes())
	if err != nil {
		return nil, err
	}
	out, err := aead.Open(nil, b[1+keyLen:1+keyLen+nonce], b[1+keyLen+nonce:], b[:1+keyLen])
	if err != nil {
		return nil, errors.New("seal: the box does not open with this key")
	}
	return out, nil
}

func gcm(shared, ephPub, toPub []byte) (cipher.AEAD, error) {
	salt := append(append([]byte{}, ephPub...), toPub...)
	key, err := hkdf.Key(sha256.New, shared, salt, info, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
