package initcmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

func read(t *testing.T, path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Without an admin DSN: the certificates, the key and the two configs are made, the CA key is
// apart from what containers mount, nothing is overwritten on a second run, and the configs
// load as what the relay and the proxy read.
func TestInitWithoutDatabase(t *testing.T) {
	dir := t.TempDir()
	var out bytes.Buffer
	o := Options{Dir: dir, RelayHost: "relay,10.1.2.3", Out: &out}
	if err := Run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"ca.crt", "relay/ca.crt", "relay/relay.crt", "relay/relay.key", "relay/ca.key", "credentials.key", "relay.json", "proxy.json"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.key")); err == nil {
		t.Fatal("ca.key is in the directory the agent, the runner and the proxy mount")
	}
	if string(read(t, filepath.Join(dir, "ca.crt"))) != string(read(t, filepath.Join(dir, "relay", "ca.crt"))) {
		t.Fatal("the public ca.crt is not the CA of relay/")
	}
	for f, want := range map[string]os.FileMode{"relay/ca.key": 0o600, "credentials.key": 0o600, "relay/relay.key": 0o600, "ca.crt": 0o644} {
		if fi, _ := os.Stat(filepath.Join(dir, f)); fi.Mode().Perm() != want {
			t.Errorf("%s has mode %v", f, fi.Mode().Perm())
		}
	}
	if fi, _ := os.Stat(filepath.Join(dir, "relay")); fi.Mode().Perm() != 0o700 {
		t.Errorf("relay/ has mode %v", fi.Mode().Perm())
	}
	var rc tunnel.RelayConfig
	if err := json.Unmarshal(read(t, filepath.Join(dir, "relay.json")), &rc); err != nil {
		t.Fatal(err)
	}
	if rc.CA != "/etc/lokadb/relay/ca.crt" || rc.CAKey != "/etc/lokadb/relay/ca.key" || rc.Cert != "/etc/lokadb/relay/relay.crt" || rc.ControlPlane != "file:/etc/lokadb/control-plane.dsn" || rc.HealthListen != ":7446" {
		t.Fatalf("relay.json: %+v", rc)
	}
	var pc proxy.Config
	if err := json.Unmarshal(read(t, filepath.Join(dir, "proxy.json")), &pc); err != nil {
		t.Fatal(err)
	}
	if pc.Upstream != "env:LOKADB_UPSTREAM" || pc.CredentialsKey != "/etc/lokadb/credentials.key" || pc.ControlPlane != "file:/etc/lokadb/control-plane.dsn" {
		t.Fatalf("proxy.json: %+v", pc)
	}
	if !strings.Contains(out.String(), "not made: give -admin") || !strings.Contains(out.String(), "docker run -d --name lokadb-relay") {
		t.Fatalf("output:\n%s", out.String())
	}

	// a second run keeps every file byte for byte
	before := map[string][]byte{}
	for _, f := range []string{"ca.crt", "relay/ca.crt", "relay/ca.key", "relay/relay.crt", "credentials.key", "relay.json", "proxy.json"} {
		before[f] = read(t, filepath.Join(dir, f))
	}
	out.Reset()
	if err := Run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	for f, b := range before {
		if !bytes.Equal(read(t, filepath.Join(dir, f)), b) {
			t.Errorf("%s was rewritten", f)
		}
	}
	if strings.Count(out.String(), "(kept") < 6 {
		t.Fatalf("second run:\n%s", out.String())
	}

	// another relay name: the certificate is kept, and the output says it lacks the name
	out.Reset()
	o.RelayHost = "other.example.com"
	if err := Run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "kept, but it is for") || !bytes.Equal(read(t, filepath.Join(dir, "relay/relay.crt")), before["relay/relay.crt"]) {
		t.Fatalf("a changed relay name:\n%s", out.String())
	}
}

// The CA key lost: a new CA is not made.
func TestInitRefusesANewCA(t *testing.T) {
	dir := t.TempDir()
	o := Options{Dir: dir, RelayHost: "relay"}
	if err := Run(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "relay", "ca.key")); err != nil {
		t.Fatal(err)
	}
	before := read(t, filepath.Join(dir, "relay", "ca.crt"))
	err := Run(context.Background(), o)
	if err == nil || !strings.Contains(err.Error(), "CA key is lost") {
		t.Fatalf("init after the CA key is lost: %v", err)
	}
	if !bytes.Equal(read(t, filepath.Join(dir, "relay", "ca.crt")), before) {
		t.Fatal("the CA certificate was replaced")
	}
}

// With an admin DSN (INIT_TEST_DSN=postgres://user:pass@host:5432/postgres, a superuser): the
// control plane's role and database are made with a random password that is not printed, kept on
// a second run, and the file works.
func TestInitControlPlane(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	ctx := context.Background()
	suffix := fmt.Sprint(time.Now().UnixNano())
	role, db := "initt_"+suffix, "initt_"+suffix
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" WITH (FORCE)")
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+role)
	})
	dir := t.TempDir()
	var out bytes.Buffer
	o := Options{Dir: dir, RelayHost: "relay", AdminDSN: dsn, CPRole: role, CPDB: db, CPHost: "pgsql", CPSSLMode: "disable", Out: &out}
	if err := Run(ctx, o); err != nil {
		t.Fatal(err)
	}
	file := string(read(t, filepath.Join(dir, "control-plane.dsn")))
	if !strings.Contains(file, "host='pgsql'") || !strings.Contains(file, "sslmode=disable") || !strings.Contains(file, "user='"+role+"'") {
		t.Fatalf("control-plane.dsn: %s", file)
	}
	pw := strings.TrimSuffix(strings.SplitN(strings.SplitN(file, "password='", 2)[1], "'", 2)[0], "")
	if len(pw) < 24 || strings.Contains(out.String(), pw) {
		t.Fatalf("the password is %d characters, and printed: %v", len(pw), strings.Contains(out.String(), pw))
	}
	if fi, _ := os.Stat(filepath.Join(dir, "control-plane.dsn")); fi.Mode().Perm() != 0o600 {
		t.Errorf("control-plane.dsn has mode %v", fi.Mode().Perm())
	}
	// the role owns the database and the schema is there
	var n int
	if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba WHERE d.datname = $1 AND r.rolname = $2`, db, role).Scan(&n); err != nil || n != 1 {
		t.Fatalf("database owner: %d %v", n, err)
	}
	// a second run keeps the role, the database and the file
	out.Reset()
	if err := Run(ctx, o); err != nil {
		t.Fatal(err)
	}
	if string(read(t, filepath.Join(dir, "control-plane.dsn"))) != file || !strings.Contains(out.String(), "(kept") {
		t.Fatalf("second run:\n%s", out.String())
	}
	// the role exists but its file is lost: init does not change a password it did not set
	if err := os.Remove(filepath.Join(dir, "control-plane.dsn")); err != nil {
		t.Fatal(err)
	}
	if err := Run(ctx, o); err == nil || !strings.Contains(err.Error(), "its password is unknown") {
		t.Fatalf("role without its file: %v", err)
	}
}
