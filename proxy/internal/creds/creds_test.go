package creds

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func key(b byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = b
	}
	return k
}

// A sealed value opens with its key for its own row alone: another key, another organization,
// kind or database, or a changed byte fail.
func TestSealOpen(t *testing.T) {
	k := key(1)
	blob, err := Seal(k, "c1", "app", "7", Direct, "host=org7 user=dml password=s")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "password") {
		t.Fatal("the plain text is in the blob")
	}
	if got, err := Open(k, "c1", "app", "7", Direct, blob); err != nil || got != "host=org7 user=dml password=s" {
		t.Fatalf("open: %q %v", got, err)
	}
	for name, open := range map[string]func() (string, error){
		"another key":          func() (string, error) { return Open(key(2), "c1", "app", "7", Direct, blob) },
		"another organization": func() (string, error) { return Open(k, "c1", "app", "8", Direct, blob) },
		"another kind":         func() (string, error) { return Open(k, "c1", "app", "7", Owner, blob) },
		"another database":     func() (string, error) { return Open(k, "c1", "other", "7", Direct, blob) },
		"another cloud":        func() (string, error) { return Open(k, "c2", "app", "7", Direct, blob) },
		"before A14":           func() (string, error) { return Open(k, "", "app", "7", Direct, blob) },
		"a changed byte": func() (string, error) {
			b := append([]byte{}, blob...)
			b[len(b)-1] ^= 1
			return Open(k, "c1", "app", "7", Direct, b)
		},
	} {
		if _, err := open(); err == nil {
			t.Errorf("%s: opened", name)
		}
	}
	b2, _ := Seal(k, "c1", "app", "7", Direct, "host=org7 user=dml password=s")
	if string(b2) == string(blob) {
		t.Error("two seals of one value are equal: the nonce is not fresh")
	}
	if KeyID(k) == KeyID(key(2)) || len(KeyID(k)) != 16 {
		t.Errorf("key ids: %s %s", KeyID(k), KeyID(key(2)))
	}
}

// The key file: 32 bytes or 64 hexadecimal digits; readable by others: refused (not on Windows).
func TestLoadKey(t *testing.T) {
	dir := t.TempDir()
	raw, hexf, bad := filepath.Join(dir, "raw"), filepath.Join(dir, "hex"), filepath.Join(dir, "bad")
	_ = os.WriteFile(raw, key(3), 0o600)
	_ = os.WriteFile(hexf, []byte(strings.Repeat("0a", 32)+"\n"), 0o600)
	_ = os.WriteFile(bad, []byte("short"), 0o600)
	if k, err := LoadKey(raw); err != nil || len(k) != 32 {
		t.Errorf("raw: %v", err)
	}
	if k, err := LoadKey(hexf); err != nil || k[0] != 0x0a {
		t.Errorf("hex: %v", err)
	}
	if _, err := LoadKey(bad); err == nil {
		t.Error("a short key loaded")
	}
	if runtime.GOOS != "windows" {
		open := filepath.Join(dir, "open")
		_ = os.WriteFile(open, key(4), 0o644)
		if _, err := LoadKey(open); err == nil || !strings.Contains(err.Error(), "0600") {
			t.Errorf("a key others may read: %v", err)
		}
	}
}

// The coordinator's condition for 9a: a ciphertext moved to another organization's row (an
// administrator of the control plane swapping two rows) opens for neither; each opens for its
// own row alone.
func TestNoSwapBetweenOrganizations(t *testing.T) {
	k := key(5)
	b1, _ := Seal(k, "c1", "app", "1", Direct, "host=org1 user=dml password=one")
	b2, _ := Seal(k, "c1", "app", "2", Direct, "host=org2 user=dml password=two")
	if _, err := Open(k, "c1", "app", "2", Direct, b1); err == nil {
		t.Error("organization 1's credentials opened as organization 2's")
	}
	if _, err := Open(k, "c1", "app", "1", Direct, b2); err == nil {
		t.Error("organization 2's credentials opened as organization 1's")
	}
	if got, err := Open(k, "c1", "app", "1", Direct, b1); err != nil || !strings.Contains(got, "password=one") {
		t.Errorf("its own row: %q %v", got, err)
	}
	// the error names neither the key nor the plain text
	_, err := Open(k, "c1", "app", "2", Direct, b1)
	if err == nil || strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), string(k)) {
		t.Errorf("the error: %v", err)
	}
}

// Review of admin, A14: two clouds with a database of the same name, one control plane and one
// key: a row of one cloud does not open as the other's, nor as a row of before A14.
func TestNoSwapBetweenInstallations(t *testing.T) {
	k := key(1)
	b1, _ := Seal(k, "7301", "app", "1", Direct, "host=a user=dml password=one")
	if _, err := Open(k, "7302", "app", "1", Direct, b1); err == nil {
		t.Fatal("a row of cloud 7301 opened as cloud 7302's")
	}
	if _, err := Open(k, "", "app", "1", Direct, b1); err == nil {
		t.Fatal("a row of cloud 7301 opened as one of before A14")
	}
	old, _ := Seal(k, "", "app", "1", Direct, "host=a user=dml password=old")
	if got, err := Open(k, "", "app", "1", Direct, old); err != nil || !strings.Contains(got, "password=old") {
		t.Fatalf("a row of before A14: %q, %v", got, err)
	}
	if _, err := Open(k, "7301", "app", "1", Direct, old); err == nil {
		t.Fatal("a row of before A14 opened as cloud 7301's: its seal names no cloud")
	}
}
