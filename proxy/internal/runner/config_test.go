package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

func write(t *testing.T, path, s string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestLoadConfig: only the JSON and service stop the runner; the rest is for Check.
func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	mountInfo = filepath.Join(dir, "none")
	t.Cleanup(func() { mountInfo = "/proc/self/mountinfo" })
	if _, err := LoadConfig(write(t, filepath.Join(dir, "bad.json"), `{"service": `)); err == nil || !strings.Contains(err.Error(), "not valid JSON") {
		t.Fatalf("bad JSON: %v", err)
	}
	if _, err := LoadConfig(write(t, filepath.Join(dir, "nosvc.json"), `{"db": "x"}`)); err == nil || !strings.Contains(err.Error(), "service required") {
		t.Fatalf("no service: %v", err)
	}
	if _, err := LoadConfig(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("a missing runner.json loaded")
	}
	c, err := LoadConfig(write(t, filepath.Join(dir, "min.json"), `{"service": "https://api.example.com/"}`))
	if err != nil || c.Service != "https://api.example.com" || c.State != "/state" {
		t.Fatalf("minimal: %+v %v", c, err)
	}
	keys := map[string]bool{}
	for _, p := range c.Check(context.Background()) {
		keys[p.Key] = true
		if p.Fix == "" {
			t.Errorf("no fix for %q", p.Message)
		}
	}
	for _, k := range []string{"db", "spec", "owner", "control_plane", "relay.agent", "relay.ca", "relay.cloud_host"} {
		if !keys[k] {
			t.Errorf("no problem for the missing %s: %v", k, keys)
		}
	}
}

// TestCheckFiles: each file the config names is a problem of its own, with the path in the
// container and on the host (from the mounts); a database DSN from an empty variable too.
func TestCheckFiles(t *testing.T) {
	dir := t.TempDir()
	etc := filepath.ToSlash(filepath.Join(dir, "etc", "lokadb"))
	mountInfo = write(t, filepath.Join(dir, "mountinfo"), strings.Join([]string{
		"1 0 0:50 / / rw,relatime - overlay overlay rw,lowerdir=/var/lib/docker/x",
		"2 1 8:1 /srv/lokadb\\040conf " + etc + " ro,relatime - ext4 /dev/sda1 rw",
		"3 1 8:1 /var/lib/docker/volumes/lokadb-runner/_data /state rw - ext4 /dev/sda1 rw",
	}, "\n"))
	t.Cleanup(func() { mountInfo = "/proc/self/mountinfo" })
	os.Unsetenv("LOKADB_TEST_EMPTY")
	c := &Config{Service: "https://x", Owner: "app", Relay: Relay{Agent: "relay:7443", CloudHost: "relay", PortMin: 15000, PortMax: 15999,
		CA: etc + "/relay-ca.crt"}, Spec: etc + "/spec.json", ProxyConfig: write(t, etc+"/proxy.json", `{"control_plane": "env:LOKADB_TEST_EMPTY", "credentials_key": "/opt/keys/cred.key"}`)}
	c.dbRaw = "env:LOKADB_TEST_EMPTY"
	byKey := map[string]cloudproto.Problem{}
	for _, p := range c.Check(context.Background()) {
		byKey[p.Key] = p
	}
	spec := byKey["spec"]
	if spec.Path != etc+"/spec.json" || spec.Host != "/srv/lokadb conf/spec.json" || spec.Mounted == nil || !*spec.Mounted ||
		!strings.Contains(spec.Message, "no such file") || !strings.Contains(spec.Message, "on the host: /srv/lokadb conf/spec.json") ||
		!strings.Contains(spec.Fix, "-write-spec "+etc+"/spec.json") {
		t.Fatalf("spec: %+v", spec)
	}
	if ca := byKey["relay.ca"]; ca.Host != "/srv/lokadb conf/relay-ca.crt" {
		t.Fatalf("relay.ca: %+v", ca)
	}
	key := byKey["credentials_key"]
	if key.Mounted == nil || *key.Mounted || !strings.Contains(key.Message, "/opt/keys is not mounted") || !strings.Contains(key.Fix, "-v /opt/keys/cred.key:/opt/keys/cred.key:ro") {
		t.Fatalf("credentials_key: %+v", key)
	}
	if db := byKey["db"]; !strings.Contains(db.Message, "LOKADB_TEST_EMPTY is empty") {
		t.Fatalf("db: %+v", db)
	}
	if cp := byKey["control_plane"]; !strings.Contains(cp.Message, "LOKADB_TEST_EMPTY") {
		t.Fatalf("control_plane: %+v", cp)
	}

	// fixed: the spec written, valid; a broken one says so
	write(t, etc+"/spec.json", `{"tables": [`)
	byKey = map[string]cloudproto.Problem{}
	for _, p := range c.Check(context.Background()) {
		byKey[p.Key] = p
	}
	if !strings.Contains(byKey["spec"].Message, "not valid JSON") {
		t.Fatalf("broken spec: %+v", byKey["spec"])
	}
	write(t, etc+"/spec.json", `{"tenant_column": "org_id", "tables": []}`)
	for _, p := range c.Check(context.Background()) {
		if p.Key == "spec" {
			t.Fatalf("spec still a problem: %+v", p)
		}
	}
}

// TestHostPath: outside a container (the root is not an overlay) there is no host path to give.
func TestHostPath(t *testing.T) {
	dir := t.TempDir()
	mountInfo = write(t, filepath.Join(dir, "mountinfo"), "1 0 8:1 / / rw - ext4 /dev/sda1 rw\n2 1 8:2 / /etc/lokadb rw - ext4 /dev/sdb1 rw\n")
	t.Cleanup(func() { mountInfo = "/proc/self/mountinfo" })
	if _, _, ok := hostPath("/etc/lokadb/spec.json"); ok {
		t.Fatal("a host's own mounts taken for a container's")
	}
	write(t, mountInfo, "1 0 0:50 / / rw - overlay overlay rw\n")
	if host, mounted, ok := hostPath("/etc/lokadb/spec.json"); !ok || mounted || host != "" {
		t.Fatalf("unmounted: %q %v %v", host, mounted, ok)
	}
}

// TestNoProxyConfig: control_plane and credentials_key in runner.json do without proxy_config.
func TestNoProxyConfig(t *testing.T) {
	dir := t.TempDir()
	mountInfo = filepath.Join(dir, "none")
	t.Cleanup(func() { mountInfo = "/proc/self/mountinfo" })
	c, err := LoadConfig(write(t, filepath.Join(dir, "runner.json"), `{"service": "https://x", "control_plane": "env:LOKADB_TEST_CP",
		"credentials_key": "`+filepath.ToSlash(filepath.Join(dir, "cred.key"))+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOKADB_TEST_CP", "host=127.0.0.1 port=1 dbname=cp user=cp sslmode=disable connect_timeout=2")
	byKey := map[string]cloudproto.Problem{}
	for _, p := range c.Check(context.Background()) {
		byKey[p.Key] = p
	}
	if _, ok := byKey["proxy_config"]; ok {
		t.Fatalf("proxy_config asked for: %+v", byKey["proxy_config"])
	}
	if p := byKey["credentials_key"]; !strings.Contains(p.Message, "cred.key: no such file") {
		t.Fatalf("credentials_key: %+v", p)
	}
	if p := byKey["control_plane"]; !strings.Contains(p.Message, "the control plane at host 127.0.0.1 does not answer") {
		t.Fatalf("control_plane: %+v", p)
	}
	write(t, filepath.Join(dir, "cred.key"), "k")
	for _, p := range c.Check(context.Background()) {
		if p.Key == "credentials_key" {
			t.Fatalf("still: %+v", p)
		}
	}
}

// TestConnProblem: a database that does not answer is said in words, with what to do.
func TestConnProblem(t *testing.T) {
	for _, tc := range []struct{ dsn, err, msg, fix string }{
		{"host=pgsql dbname=controlplane user=cp_role", "failed to connect to `user=cp_role database=controlplane`: hostname resolving error: lookup pgsql on 127.0.0.11:53: no such host",
			"the control plane at host pgsql cannot be resolved from the runner's container", "--network NAME"},
		{"host=db user=app password=secret dbname=x", "failed to connect: password authentication failed for user \"app\"", "does not answer", "password='…'"},
		{"host=db user=app dbname=x", "failed to connect: server refused TLS connection", "does not answer", "sslmode=disable"},
		{"host=db user=app dbname=x", "failed to connect: no pg_hba.conf entry for host \"10.0.0.5\"", "does not answer", "pg_hba.conf"},
		{"postgres://u:p@h:5432/d", "cannot parse `postgres://u:xxxxx@h:5432/d%`: failed to parse as URL", "not a valid DSN", "percent-encoded"},
	} {
		p := connProblem("control_plane", "the control plane", tc.dsn, errors.New(tc.err))
		if !strings.Contains(p.Message, tc.msg) || !strings.Contains(p.Fix, tc.fix) || strings.Contains(p.Message, "secret") {
			t.Errorf("%s: %+v", tc.err, p)
		}
	}
	if p := resolveProblem(context.Background(), "relay.cloud_host", "the relay", "no-such-host.invalid"); p == nil || !strings.Contains(p.Message, "cannot be resolved") {
		t.Errorf("resolve: %+v", p)
	}
	if p := resolveProblem(context.Background(), "relay.cloud_host", "the relay", "127.0.0.1"); p != nil {
		t.Errorf("an address taken for a name: %+v", p)
	}
}
