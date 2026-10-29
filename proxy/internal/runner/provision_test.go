package runner

import (
	"bytes"
	"context"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// useDir points the LokaDB directory at a temporary one.
func useDir(t *testing.T) string {
	t.Helper()
	old := Dir
	Dir = filepath.ToSlash(t.TempDir())
	mountInfo = filepath.Join(Dir, "none")
	t.Cleanup(func() { Dir = old; mountInfo = "/proc/self/mountinfo" })
	return Dir
}

// TestDefaults: runner.json names no path and every file is the directory's; the path fields
// stay as overrides, and control_plane/credentials_key given directly leave the proxy config out.
func TestDefaults(t *testing.T) {
	dir := useDir(t)
	c, err := LoadConfig(write(t, filepath.Join(dir, "runner.json"), `{"service": "https://api.example.com"}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec != dir+"/spec.json" || c.ProxyConfig != dir+"/proxy.json" || c.Relay.CA != dir+"/ca.crt" || c.Relay.CloudHost != "lokadb-relay" {
		t.Fatalf("defaults: spec %q proxy %q ca %q cloud_host %q", c.Spec, c.ProxyConfig, c.Relay.CA, c.Relay.CloudHost)
	}
	c, err = LoadConfig(write(t, filepath.Join(dir, "r2.json"), `{"service": "https://x", "spec": "/a/spec.json", "proxy_config": "/a/p.json",
		"relay": {"ca": "/a/ca.pem", "cloud_host": "relay.internal"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Spec != "/a/spec.json" || c.ProxyConfig != "/a/p.json" || c.Relay.CA != "/a/ca.pem" || c.Relay.CloudHost != "relay.internal" {
		t.Fatalf("overrides lost: %+v", c)
	}
	c, err = LoadConfig(write(t, filepath.Join(dir, "r3.json"), `{"service": "https://x", "control_plane": "env:CP"}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.ProxyConfig != "" || c.CredentialsKey != dir+"/credentials.key" {
		t.Fatalf("direct control plane: proxy %q key %q", c.ProxyConfig, c.CredentialsKey)
	}
	// a runner.json that is not there says how to mount the directory
	if _, err := LoadConfig(filepath.Join(dir, "none.json")); err == nil {
		t.Fatal("no error")
	}
	if _, err := LoadConfig(dir + "/runner.json.absent"); err == nil || !strings.Contains(err.Error(), "docker run -v HOSTDIR:"+dir) {
		t.Fatalf("missing runner.json: %v", err)
	}
}

// TestProvisionDry: runner -check says what it would create, makes nothing, and does not call
// those files missing.
func TestProvisionDry(t *testing.T) {
	dir := useDir(t)
	write(t, dir+"/proxy.json", `{}`) // there already: not listed
	c, err := LoadConfig(write(t, dir+"/runner.json", `{"service": "https://x", "db": "host=127.0.0.1 port=1 user=a dbname=d sslmode=disable connect_timeout=1",
		"owner": "app", "relay": {"agent": "relay.example.com:7443"}}`))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	todo, err := c.Provision(context.Background(), true, &out)
	if err != nil || strings.Join(todo, ",") != "credentials.key,control-plane.dsn,relay.json" || !strings.Contains(out.String(), "WOULD CREATE") {
		t.Fatalf("dry: %v %v\n%s", todo, err, out.String())
	}
	for _, f := range todo {
		if _, err := os.Stat(filepath.Join(dir, f)); err == nil {
			t.Fatalf("a dry run made %s", f)
		}
	}
	if _, ok := os.Stat(dir + "/relay.json"); ok == nil {
		t.Fatal("relay.json made")
	}
	c.willMake[dir+"/proxy.json"] = true
	for _, p := range c.Check(context.Background()) {
		if p.Key == "proxy_config" || p.Key == "credentials_key" {
			t.Errorf("a file it would create is a problem: %+v", p)
		}
	}
	// no DSN or no relay address: nothing to make yet, Check says what is missing
	c2, _ := LoadConfig(write(t, dir+"/r2.json", `{"service": "https://x"}`))
	if todo, err := c2.Provision(context.Background(), false, &out); todo != nil || err != nil {
		t.Fatalf("without db and relay: %v %v", todo, err)
	}
	// a proxy config of its own: the directory's files are not made
	c3, _ := LoadConfig(write(t, dir+"/r3.json", `{"service": "https://x", "db": "x", "proxy_config": "/elsewhere/p.json", "relay": {"agent": "r:7443"}}`))
	if todo, _ := c3.Provision(context.Background(), true, &out); todo != nil {
		t.Fatalf("an override still provisioned: %v", todo)
	}
}

// TestProvision (INIT_TEST_DSN, a superuser): the files are made once with a control plane role
// whose password is not printed, and nothing that exists is overwritten.
func TestProvision(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	dir := useDir(t)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS lokadb_cp WITH (FORCE)")
		_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS lokadb_cp")
	})
	write(t, dir+"/proxy.json", `{"listen": ":1"}`) // the user's own: kept
	c, err := LoadConfig(write(t, dir+"/runner.json", `{"service": "https://x", "db": "`+dsn+`", "relay": {"agent": "relay.example.com:7443"}, "network": "app_default"}`))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if _, err := c.Provision(ctx, false, &out); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	for _, f := range []string{"credentials.key", "control-plane.dsn", "relay.json"} {
		if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	if string(read(t, dir+"/proxy.json")) != `{"listen": ":1"}` {
		t.Fatal("proxy.json was overwritten")
	}
	if !strings.Contains(out.String(), "--network app_default") || !strings.Contains(out.String(), "docker run -d --name lokadb-relay") {
		t.Fatalf("no docker run lines:\n%s", out.String())
	}
	if strings.Contains(out.String(), string(read(t, dir+"/credentials.key"))[:20]) {
		t.Fatal("the key was printed")
	}
	// again: nothing to make
	if todo, err := c.Provision(ctx, false, &out); todo != nil || err != nil {
		t.Fatalf("second run: %v %v", todo, err)
	}
}

func read(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestEmbeddedSettings: a runner with no files finds its database in the application's
// environment and takes the rest from the panel's settings; the spec lands in the volume once.
func TestEmbeddedSettings(t *testing.T) {
	dir := useDir(t)
	mountInfo = write(t, dir+"/mountinfo", "1 0 0:50 / / rw - overlay overlay rw\n") // in a container
	t.Setenv("LOKADB_DB", "")
	c, err := NewEmbedded(env(map[string]string{"LOKADB_TOKEN": "lrr_x", "DB_HOST": "pgsql", "DB_DATABASE": "coders", "DB_USERNAME": "forge", "DB_PASSWORD": "pw"}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Embedded || c.Token != "lrr_x" || c.Owner != "forge" || c.Relay.CloudHost != "lokadb" || c.State != dir+"/state" || c.Service != DefaultService {
		t.Fatalf("embedded config: %+v", c)
	}
	if !strings.Contains(c.Found(), "pgsql:5432/coders as forge") || strings.Contains(c.Found(), "pw") {
		t.Fatalf("found: %s", c.Found())
	}
	// before the panel says anything: the missing pieces are said in the panel's words
	var keys []string
	for _, p := range c.Check(context.Background()) {
		keys = append(keys, p.Key)
		if p.Key == "relay.agent" && !strings.Contains(p.Fix, "panel") {
			t.Errorf("the fix does not send to the panel: %s", p.Fix)
		}
	}
	if !strings.Contains(strings.Join(keys, ","), "relay.agent") {
		t.Fatalf("no problem for the relay address: %v", keys)
	}
	s := &cloudproto.Settings{RelayPublic: "relay.example.com:7443", Owner: "app", Spec: []byte(`{"tables":[{"name":"docs","tenant_column":"team_id"}]}`)}
	if changed, err := c.ApplySettings(s); err != nil || !changed {
		t.Fatalf("apply: %v %v", changed, err)
	}
	if c.Relay.Agent != "relay.example.com:7443" || c.Relay.Enroll != "relay.example.com:7444" || c.Owner != "app" || len(c.spec.Tables) != 1 {
		t.Fatalf("applied: %+v %+v", c.Relay, c.spec)
	}
	if string(read(t, dir+"/spec.json")) == "" {
		t.Fatal("no spec.json")
	}
	if changed, _ := c.ApplySettings(s); changed {
		t.Fatal("the same settings applied twice are a change")
	}
}

// TestEmbeddedDatabaseHints: a database that does not answer is said in the words of someone who
// runs a container: localhost in the application's settings is the container itself, and a
// database in another compose project needs that project's network.
func TestEmbeddedDatabaseHints(t *testing.T) {
	useDir(t)
	mk := func(dsn string) *Config {
		t.Helper()
		t.Setenv("LOKADB_DB", dsn)
		c := &Config{Service: "https://x", Embedded: true, DB: "env:LOKADB_DB", Owner: "forge", Relay: Relay{Agent: "relay.example.com:7443"}}
		if err := c.finish(); err != nil {
			t.Fatal(err)
		}
		return c
	}
	problem := func(c *Config) cloudproto.Problem {
		for _, p := range c.Check(context.Background()) {
			if p.Key == "db" {
				return p
			}
		}
		t.Fatal("no problem for the database")
		return cloudproto.Problem{}
	}
	p := problem(mk("host=127.0.0.1 port=1 dbname=d user=u password=pw sslmode=disable connect_timeout=2"))
	if !strings.Contains(p.Message, "127.0.0.1") || !strings.Contains(p.Message, "container itself") ||
		!strings.Contains(p.Fix, "host.docker.internal") || !strings.Contains(p.Fix, "host-gateway") || strings.Contains(p.Message+p.Fix, "pw") {
		t.Fatalf("localhost: %+v", p)
	}
	p = problem(mk("host=no-such-service.invalid port=5432 dbname=d user=u password=pw sslmode=disable connect_timeout=2"))
	if !strings.Contains(p.Fix, "external: true") || !strings.Contains(p.Fix, "docker network ls") {
		t.Fatalf("another compose project: %+v", p)
	}
}

// LOKADB_DIR moves the directory (a systemd service of its own user has no /etc/lokadb).
func TestDirFromEnv(t *testing.T) {
	t.Setenv("LOKADB_DIR", "")
	if dirFromEnv() != "/etc/lokadb" {
		t.Fatalf("default: %s", dirFromEnv())
	}
	t.Setenv("LOKADB_DIR", "/var/lib/lokadb/")
	if dirFromEnv() != "/var/lib/lokadb" {
		t.Fatalf("set: %s", dirFromEnv())
	}
}

// The relay's name in the runner's own network: the compose service in a container, the loopback
// on a plain host (no Docker, a systemd service).
func TestEmbeddedRelayName(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { mountInfo = "/proc/self/mountinfo" })
	for root, want := range map[string]string{"overlay": "lokadb", "ext4": "127.0.0.1"} {
		mountInfo = write(t, filepath.Join(dir, root), "1 0 0:50 / / rw - "+root+" x rw\n")
		c := &Config{Service: "https://x", Embedded: true}
		if err := c.finish(); err != nil || c.Relay.CloudHost != want {
			t.Errorf("%s: %q %v, want %s", root, c.Relay.CloudHost, err, want)
		}
	}
}

// After step 5 the application's settings name LokaDB's own proxy: a restart must still know the
// database it found before.
func TestEmbeddedRestartAfterPointing(t *testing.T) {
	useDir(t)
	t.Setenv("LOKADB_DB", "")
	before := env(map[string]string{"LOKADB_TOKEN": "lrr_x", "DB_HOST": "postgres", "DB_DATABASE": "coders", "DB_USERNAME": "forge", "DB_PASSWORD": "pw"})
	if _, err := NewEmbedded(before, nil); err != nil {
		t.Fatal(err)
	}
	after := env(map[string]string{"LOKADB_TOKEN": "lrr_x", "DB_HOST": "lokadb", "DB_PORT": "6432", "DB_DATABASE": "coders", "DB_USERNAME": "forge", "DB_PASSWORD": "pw"})
	c, err := NewEmbedded(after, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.found.Host != "postgres" || c.found.Port != "5432" || !strings.Contains(os.Getenv("LOKADB_UPSTREAM"), "host='postgres'") {
		t.Fatalf("the earlier database was not kept: %+v upstream %s", c.found, os.Getenv("LOKADB_UPSTREAM"))
	}
	// no earlier record, and the settings already name the proxy: a plain answer, not a loop
	useDir(t)
	if _, err := NewEmbedded(after, nil); err == nil || !strings.Contains(err.Error(), "LOKADB_DB") {
		t.Fatalf("no record: %v", err)
	}
}
