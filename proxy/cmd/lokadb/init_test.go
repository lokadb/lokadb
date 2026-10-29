package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveRef(t *testing.T) {
	t.Setenv("LOKADB_T_DSN", "host=x")
	t.Setenv("LOKADB_T_EMPTY", "")
	f := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(f, []byte(" host=y password='p'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for in, want := range map[string]string{"plain": "plain", "": "", "env:LOKADB_T_DSN": "host=x", "file:" + f: "host=y password='p'"} {
		if got, err := resolveRef("upstream", in); err != nil || got != want {
			t.Errorf("%q: %q %v", in, got, err)
		}
	}
	if _, err := resolveRef("upstream", "env:LOKADB_T_EMPTY"); err == nil || !strings.Contains(err.Error(), "LOKADB_T_EMPTY is empty") {
		t.Errorf("an empty variable: %v", err)
	}
	if _, err := resolveRef("control_plane", "file:/nonexistent/dsn"); err == nil {
		t.Error("a missing file")
	}
}

func TestInside(t *testing.T) {
	for _, c := range []struct {
		path, dir string
		want      bool
	}{{"/etc/lokadb/certs", "/etc/lokadb/certs", true}, {"/etc/lokadb/certs/k", "/etc/lokadb/certs", true}, {"/etc/lokadb/ca-private", "/etc/lokadb/certs", false}, {"/etc/lokadb", "/etc/lokadb/certs", false}} {
		if got := inside(c.path, c.dir); got != c.want {
			t.Errorf("inside(%s, %s) = %v", c.path, c.dir, got)
		}
	}
}
