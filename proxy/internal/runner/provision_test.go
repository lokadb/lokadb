package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	old := resolves
	t.Cleanup(func() { resolves = old })
	resolves = func(string) bool { return true } // in a container its own name resolves
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

// The relay's name in the runner's own network: the container's name where it resolves (compose,
// Kubernetes, docker run), the loopback where it does not (a plain host).
func TestEmbeddedRelayName(t *testing.T) {
	old := resolves
	t.Cleanup(func() { resolves = old })
	for want, ok := range map[string]bool{"lokadb": true, "127.0.0.1": false} {
		resolves = func(string) bool { return ok }
		c := &Config{Service: "https://x", Embedded: true}
		if err := c.finish(); err != nil || c.Relay.CloudHost != want {
			t.Errorf("resolves=%v: %q %v, want %s", ok, c.Relay.CloudHost, err, want)
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

// A managed database: TLS is required by default, a refused connection names the address the
// database sees (it is in its own error), a silent one says to look at the provider's firewall.
func TestManagedDatabase(t *testing.T) {
	if p := ManagedProvider("mydb.abc123.eu-west-1.rds.amazonaws.com"); p != "Amazon RDS or Aurora" {
		t.Fatalf("rds: %q", p)
	}
	if ManagedProvider("db.internal") != "" || ManagedProvider("postgres") != "" {
		t.Fatal("a private name taken for a managed service")
	}
	f, err := DiscoverDB(env(map[string]string{"DATABASE_URL": "postgres://u:p@proj.db.ondigitalocean.com:25060/app"}))
	if err != nil || f.SSL != "require" {
		t.Fatalf("managed host sslmode: %+v %v", f, err)
	}
	if f, _ := DiscoverDB(env(map[string]string{"DATABASE_URL": "postgres://u:p@db.mycompany.example/app"})); f.SSL != "prefer" {
		t.Fatalf("another server: %s", f.SSL)
	}
	if f, _ := DiscoverDB(env(map[string]string{"DATABASE_URL": "postgres://u:p@proj.db.ondigitalocean.com:25060/app?sslmode=verify-full"})); f.SSL != "verify-full" {
		t.Fatalf("an explicit sslmode is kept: %s", f.SSL)
	}
	c := &Config{Embedded: true, DB: "host=mydb.abc.eu-west-1.rds.amazonaws.com port=5432 dbname=d user=u password=pw sslmode=require"}
	plain := &Config{Embedded: true, DB: "host=mydb.abc.eu-west-1.rds.amazonaws.com port=5432 dbname=d user=u password=pw sslmode=disable"}
	if p := plain.embeddedDBProblem(cloudproto.Problem{Key: "db"}, errors.New(`failed to connect: server error (FATAL: no pg_hba.conf entry for host "10.0.0.5", user "u", database "d", no encryption (SQLSTATE 28000))`)); !strings.Contains(p.Message, "accepts only TLS") || !strings.Contains(p.Fix, "sslmode=require") || !strings.Contains(p.Fix, "10.0.0.5") {
		t.Fatalf("a server that wants TLS: %+v", p)
	}
	p := c.embeddedDBProblem(cloudproto.Problem{Key: "db"}, errors.New(`failed to connect: server error (FATAL: no pg_hba.conf entry for host "203.0.113.7", user "u", database "d", no encryption (SQLSTATE 28000))`))
	if !strings.Contains(p.Message, "203.0.113.7") || !strings.Contains(p.Fix, "203.0.113.7") || strings.Contains(p.Message+p.Fix, "pw") {
		t.Fatalf("pg_hba: %+v", p)
	}
	p = c.embeddedDBProblem(cloudproto.Problem{Key: "db"}, errors.New("failed to connect: dial tcp 10.1.2.3:5432: i/o timeout"))
	if !strings.Contains(p.Message, "Amazon RDS or Aurora") || !strings.Contains(p.Fix, "security group") || !strings.Contains(p.Fix, "checkip.amazonaws.com") {
		t.Fatalf("timeout: %+v", p)
	}
}

// The public proxy (an application on a platform): TLS required, a certificate for the public name
// made once, only the allowed addresses; never the whole internet; turning it off puts proxy.json back.
func TestPublicProxy(t *testing.T) {
	dir := useDir(t)
	embeddedDone.Store(false)
	write(t, dir+"/proxy.json", `{"listen": ":6432", "upstream": "env:LOKADB_UPSTREAM", "fanout": {"*": {"mode": "reject"}}}`)
	c := &Config{}
	pub := &cloudproto.PublicProxy{Name: "lokadb.example.com", Allow: []string{"203.0.113.7", "198.51.100.0/24"}}
	if _, err := c.ApplySettings(&cloudproto.Settings{ProxyPublic: pub}); err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(read(t, dir+"/proxy.json"), &m); err != nil {
		t.Fatal(err)
	}
	tls, _ := m["tls"].(map[string]any)
	if tls["require"] != true || tls["cert"] != dir+"/proxy.crt" || m["listen"] != ":6432" || len(m["allow"].([]any)) != 2 {
		t.Fatalf("proxy.json: %v", m)
	}
	crt := read(t, dir+"/proxy.crt")
	if fi, _ := os.Stat(dir + "/proxy.key"); fi.Mode().Perm() != 0o600 {
		t.Errorf("proxy.key mode %v", fi.Mode().Perm())
	}
	if c.RestartRequested() {
		t.Error("a restart asked for before the proxy ran")
	}
	// the same again: nothing changes, the certificate is kept
	if c.applyPublicProxy() || !bytes.Equal(read(t, dir+"/proxy.crt"), crt) {
		t.Error("the same setting changed something")
	}
	// a restart: before the panel's settings come, what was applied last stands
	fresh := &Config{}
	if fresh.applyPublicProxy() || !bytes.Contains(read(t, dir+"/proxy.json"), []byte("allow")) {
		t.Error("a new process wiped the public proxy setting before the panel's settings arrived")
	}
	// the proxy runs: a change asks for a new process
	embeddedDone.Store(true)
	t.Cleanup(func() { embeddedDone.Store(false) })
	pub.Allow = []string{"203.0.113.9"}
	c.public = pub
	if !c.applyPublicProxy() || !c.RestartRequested() {
		t.Error("a new allow list did not ask for a restart")
	}
	// refused: no addresses, the whole internet, a name with a port; the file stays as it is
	before := read(t, dir+"/proxy.json")
	for _, bad := range []*cloudproto.PublicProxy{{Name: "x.example.com"}, {Name: "x.example.com", Allow: []string{"0.0.0.0/0"}}, {Name: "x.example.com:6432", Allow: []string{"1.2.3.4"}}, {Name: "", Allow: []string{"1.2.3.4"}}} {
		if ValidatePublicProxy(bad) == nil {
			t.Errorf("accepted %+v", bad)
		}
		c.public = bad
		if c.applyPublicProxy() || !bytes.Equal(read(t, dir+"/proxy.json"), before) {
			t.Errorf("a refused setting changed proxy.json: %+v", bad)
		}
	}
	// off: back to the private proxy
	c.public = nil
	if !c.applyPublicProxy() {
		t.Fatal("turning it off changed nothing")
	}
	m = nil
	_ = json.Unmarshal(read(t, dir+"/proxy.json"), &m)
	if m["tls"] != nil || m["allow"] != nil || m["listen"] != ":6432" {
		t.Fatalf("not put back: %v", m)
	}
}

// TestAppRoleRisk: the application's role is reported when the proxy would refuse it (a superuser,
// BYPASSRLS, REPLICATION, or one that can SET ROLE to such a role), and not otherwise; the cluster's
// first role (POSTGRES_USER of the image) is told apart, since Postgres will not demote it.
func TestAppRoleRisk(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	roles := []string{"rr_plain", "rr_super", "rr_bypass", "rr_repl", "rr_member"}
	drop := func() {
		for _, r := range roles {
			_, _ = admin.Exec(ctx, "DROP ROLE IF EXISTS "+r)
		}
	}
	drop()
	t.Cleanup(drop)
	for _, q := range []string{
		"CREATE ROLE rr_plain LOGIN", "CREATE ROLE rr_super LOGIN SUPERUSER", "CREATE ROLE rr_bypass LOGIN BYPASSRLS",
		"CREATE ROLE rr_repl LOGIN REPLICATION", "CREATE ROLE rr_member LOGIN", "GRANT rr_bypass TO rr_member WITH SET TRUE",
	} {
		if _, err := admin.Exec(ctx, q); err != nil {
			if strings.Contains(q, "WITH SET") { // before Postgres 16 a plain membership is the same
				_, err = admin.Exec(ctx, "GRANT rr_bypass TO rr_member")
			}
			if err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	for _, c := range []struct{ role, want string }{
		{"rr_plain", ""}, {"rr_super", "is a superuser"}, {"rr_bypass", "has BYPASSRLS"}, {"rr_repl", "has REPLICATION"},
		{"rr_member", "can SET ROLE to rr_bypass, which has BYPASSRLS"}, {"nobody_here", ""},
	} {
		why, boot, err := appRoleRisk(ctx, dsn, c.role)
		if err != nil || boot || (c.want == "" && why != "") || !strings.HasPrefix(why, c.want) {
			t.Errorf("%s: %q bootstrap %v, %v; want %q", c.role, why, boot, err, c.want)
		}
	}
	var first string
	if err := admin.QueryRow(ctx, "SELECT rolname FROM pg_roles WHERE oid = 10").Scan(&first); err != nil {
		t.Fatal(err)
	}
	if why, boot, err := appRoleRisk(ctx, dsn, first); err != nil || !boot || !strings.Contains(why, "superuser") {
		t.Errorf("the cluster's first role %s: %q bootstrap %v, %v", first, why, boot, err)
	}
}
