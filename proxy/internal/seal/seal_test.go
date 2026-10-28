package seal

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestSealOpen(t *testing.T) {
	k, err := NewKey()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte(`{"app_password":"p'w\\x","dml_password":"q"}`)
	box, err := Seal(k.Public(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(box, "p'w") {
		t.Fatal("plaintext in the box")
	}
	got, err := k.Open(box)
	if err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("Open = %q, %v", got, err)
	}
	// the private key survives its encoding
	k2, err := ParseKey(k.Private())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := k2.Open(box); err != nil || !bytes.Equal(got, msg) {
		t.Fatalf("parsed key: %q, %v", got, err)
	}
	// two seals of one message differ (ephemeral key, nonce)
	box2, _ := Seal(k.Public(), msg)
	if box2 == box {
		t.Fatal("same box twice")
	}
}

func TestOpenRefuses(t *testing.T) {
	k, _ := NewKey()
	other, _ := NewKey()
	box, _ := Seal(k.Public(), []byte("secret"))
	if _, err := other.Open(box); err == nil {
		t.Fatal("another key opened the box")
	}
	b, _ := base64.StdEncoding.DecodeString(box)
	for _, i := range []int{0, 5, 40, len(b) - 1} { // version, ephemeral key, nonce, tag
		c := append([]byte{}, b...)
		c[i] ^= 1
		if _, err := k.Open(base64.StdEncoding.EncodeToString(c)); err == nil {
			t.Fatalf("a box changed at byte %d opened", i)
		}
	}
	if _, err := k.Open("bm90IGEgYm94"); err == nil {
		t.Fatal("garbage opened")
	}
	if _, err := Seal("bad", []byte("x")); err == nil {
		t.Fatal("sealed to a bad key")
	}
}

func TestFingerprint(t *testing.T) {
	k, _ := NewKey()
	f1, err := Fingerprint(k.Public())
	if err != nil || !strings.HasPrefix(f1, "SHA256:") {
		t.Fatalf("%q %v", f1, err)
	}
	f2, _ := Fingerprint(k.Public())
	other, _ := NewKey()
	f3, _ := Fingerprint(other.Public())
	if f1 != f2 || f1 == f3 {
		t.Fatal("fingerprint not stable or not distinct")
	}
	if _, err := Fingerprint("eA=="); err == nil {
		t.Fatal("short key accepted")
	}
}
