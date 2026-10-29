// Package runner: lokadb runner, the SaaS's side of LokaDB Cloud. It dials the service, takes
// jobs (check, enable, move-org, move-back, the agent's enrollment and credentials) and runs
// them here with the open module, against the SaaS database and its control plane, whose
// passwords never leave this process. See private/cloud/DESIGN.md §4 (the protocol is
// package cloudproto).
package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Duration reads "30s" from JSON.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	d.Duration = v
	return err
}

// Dir: the one directory of the install, mounted at the same place in every LokaDB container
// (docker run -v HOSTDIR:/etc/lokadb): runner.json, spec.json, proxy.json, control-plane.dsn,
// credentials.key, ca.crt, relay.json. The files of runner.json default to their names here;
// the path fields below stay as overrides. A variable for the tests.
var Dir = dirFromEnv()

// dirFromEnv: LOKADB_DIR where the program runs without Docker (a systemd service of its own user has
// no /etc/lokadb to write to); /etc/lokadb, the path every container mounts, otherwise.
func dirFromEnv() string {
	if d := os.Getenv("LOKADB_DIR"); d != "" {
		return filepath.Clean(d)
	}
	return "/etc/lokadb"
}

// RelayName: the relay container's name, and so its host name on a Docker network; the default
// of relay.cloud_host.
const RelayName = "lokadb-relay"

// Config: runner.json. Every secret may be "env:NAME" or "file:PATH". Paths default to the
// files of Dir.
type Config struct {
	// Service: LokaDB Cloud's API (https://api.lokadb.com).
	Service string `json:"service"`
	// Name: how the panel shows this runner (default: the host name).
	Name string `json:"name"`
	// State: the directory of runner.json (the secret, the key): a volume (default /state).
	State string `json:"state"`
	// Token: the one-time registration token (lrr_…) of the first start; default
	// env LOKADB_RUNNER_TOKEN.
	Token string `json:"token"`

	DB    string `json:"db"`    // admin DSN of the SaaS database (as lokadb move-org -db)
	Spec  string `json:"spec"`  // the spec enable was given (default Dir/spec.json)
	Owner string `json:"owner"` // the role owning the application's tables (move-org -owner)
	// ProxyConfig: the proxy's config (default Dir/proxy.json): check reads its fanout policy;
	// control_plane and credentials_key default from it. Naming control_plane or
	// credentials_key here instead leaves the proxy config out (default none).
	ProxyConfig       string `json:"proxy_config"`
	ControlPlane      string `json:"control_plane"`
	CredentialsKey    string `json:"credentials_key"`
	CredentialsKeyOld string `json:"credentials_key_old"`

	// Network: the Docker network the relay and the proxy run on, for the docker run lines the
	// runner prints ("host" for --network host; "" none). Image: their image (default the
	// runner's own release).
	Network string `json:"network"`
	Image   string `json:"image"`

	// Embedded: one process with the relay and the proxy, files made in the volume, the rest
	// from the panel's settings (set when there is no runner.json and the paste gave a token).
	Embedded bool `json:"embedded"`

	Relay Relay `json:"relay"`
	// ProxyStatus: the proxies' status URLs (block 8), forwarded in the status snapshots.
	ProxyStatus []string `json:"proxy_status"`
	// SendStatus: false sends no status snapshots (jobs still work; the panel says monitoring
	// is off). Default true.
	SendStatus *bool `json:"status"`

	LockTimeout Duration `json:"lock_timeout"` // move-org's (default 5s)
	MaxPause    Duration `json:"max_pause"`    // move-org's (default 10s)

	// loaded: dbRaw and cpRaw as written (env:…), the rest by Check
	dbRaw, cpRaw, keyRaw, keyOldRaw string
	spec                            admin.Spec
	proxyCfg                        *proxy.Config
	found                           *Found               // the application's database and role, from its environment
	adminDSN                        string               // the DSN the runner works as (the application's, or LOKADB_DB's)
	settings                        *cloudproto.Settings // the panel's, as last applied
	public                          *cloudproto.PublicProxy
	publicKnown                     bool // the panel's settings have arrived: until then proxy.json (what was applied last) stands
	restart                         bool
	info                            map[string]string
	Log                             *slog.Logger
	willMake                        map[string]bool // files a dry Provision found it would create
}

type Relay struct {
	// Agent: the relay tunnel address(es) agents dial from outside, host:port[,host:port].
	Agent      string `json:"agent"`
	Enroll     string `json:"enroll"`      // default: the first agent host, port 7444
	ServerName string `json:"server_name"` // name in the relay certificates
	CA         string `json:"ca"`          // the CA certificate file (public), handed to installers (default Dir/ca.crt)
	// CloudHost: the relay as this network reaches an organization's port (the proxy, move-org);
	// default lokadb-relay, the name of the relay container the runner's docker run line gives.
	CloudHost string `json:"cloud_host"`
	PortMin   int    `json:"port_min"` // organizations' ports (default 15000-15999)
	PortMax   int    `json:"port_max"`
	TLS       string `json:"tls"`     // org-endpoint -tls: "require" or ""
	SSLMode   string `json:"sslmode"` // of connections to an organization's database (default prefer)
	// Status: the relays' health_listen URLs (/sessions today, /status with block 8).
	Status []string `json:"status"`
}

// LoadConfig reads runner.json: only what registering and polling need must be right here
// (the JSON itself and service); the rest is Check's, which the runner repeats until it passes.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w%s%s", err, whereOnHost(path), noDirHint(path))
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%s: not valid JSON: %w", path, err)
	}
	return &c, c.finish()
}

// whereOnHost: for a file error, where the path is on the host, when the runner is in a container.
func whereOnHost(path string) string {
	host, mounted, ok := hostPath(path)
	switch {
	case !ok:
		return ""
	case mounted:
		return fmt.Sprintf(" (in the container; on the host: %s)", host)
	}
	return fmt.Sprintf(" (in the container, where %s is not mounted from the host)", filepath.Dir(path))
}

func (c *Config) finish() error {
	var err error
	if c.Token == "" {
		c.Token = os.Getenv("LOKADB_RUNNER_TOKEN")
	}
	if c.Token == "" {
		c.Token = os.Getenv("LOKADB_TOKEN")
	}
	if c.Token, err = Secret(c.Token); err != nil {
		return err
	}
	c.Service = strings.TrimRight(c.Service, "/")
	if c.Service == "" {
		return fmt.Errorf("runner config: service required (the LokaDB API, https://api.lokadb.com)")
	}
	if c.Spec == "" {
		c.Spec = filepath.Join(Dir, "spec.json")
	}
	if c.Relay.CA == "" {
		c.Relay.CA = filepath.Join(Dir, "ca.crt")
	}
	if c.Relay.CloudHost == "" {
		c.Relay.CloudHost = RelayName
		if c.Embedded {
			// the name the paste gives the container (a compose service, a Service of Kubernetes, the
			// name of docker run) resolves to itself, and the database's own container reaches the
			// relay by it; where it does not resolve (a plain host, or a container that runs
			// systemd), the loopback is the relay's address
			c.Relay.CloudHost = "127.0.0.1"
			if resolves("lokadb") {
				c.Relay.CloudHost = "lokadb"
			}
		}
	}
	switch {
	case c.ProxyConfig == "" && c.ControlPlane == "" && c.CredentialsKey == "":
		c.ProxyConfig = filepath.Join(Dir, "proxy.json")
	case c.ProxyConfig == "" && c.CredentialsKey == "":
		c.CredentialsKey = filepath.Join(Dir, "credentials.key")
	}
	c.dbRaw, c.cpRaw, c.keyRaw, c.keyOldRaw = c.DB, c.ControlPlane, c.CredentialsKey, c.CredentialsKeyOld
	if c.State == "" {
		c.State = "/state"
	}
	if c.Name == "" {
		c.Name, _ = os.Hostname()
	}
	if c.Relay.PortMin == 0 {
		c.Relay.PortMin, c.Relay.PortMax = 15000, 15999
	}
	if c.Relay.SSLMode == "" {
		c.Relay.SSLMode = "prefer"
	}
	if c.Relay.Enroll == "" && c.Relay.Agent != "" {
		first := strings.Split(c.Relay.Agent, ",")[0]
		host, _, err := net.SplitHostPort(first)
		if err != nil {
			host = first
		}
		c.Relay.Enroll = net.JoinHostPort(host, "7444")
	}
	if c.LockTimeout.Duration == 0 {
		c.LockTimeout.Duration = 5 * time.Second
	}
	if c.MaxPause.Duration == 0 {
		c.MaxPause.Duration = 10 * time.Second
	}
	return nil
}

// specFix: how a missing spec is made (Install on your database, step 3).
const specFix = `write it on this machine: lokadb check -db "$LOKADB_DB" -org-table TABLE -write-spec %s ` +
	`(TABLE: your organizations table; lokadb check -db "$LOKADB_DB" -org-candidates lists the likely ones). ` +
	`See https://docs.lokadb.com/getting-started/install/#3-write-the-spec`

// Check: everything else the runner needs, each failure a problem in words: the required keys,
// the files (the spec, the proxy config and the key files it names, the relay CA), the database
// and the control plane reachable. None: the config is loaded (the spec and the proxy config read).
func (c *Config) Check(ctx context.Context) []cloudproto.Problem {
	var out []cloudproto.Problem
	add := func(key, msg, fix string) { out = append(out, cloudproto.Problem{Key: key, Message: msg, Fix: fix}) }
	file := func(p *cloudproto.Problem) bool {
		if p != nil {
			out = append(out, *p)
		}
		return p == nil
	}
	for _, kv := range [][3]string{
		{"db", c.dbRaw, "the administrator DSN of your SaaS database, best as env:LOKADB_DB with -e LOKADB_DB in docker run"},
		{"spec", c.Spec, "the path of your spec.json: the tables that move with an organization"},
		{"owner", c.Owner, "the role that owns your application's tables (as lokadb move-org -owner; psql's \\dt shows it)"},
		{"relay.agent", c.Relay.Agent, "host:port of your relay that your customers' agents connect to (port 7443)"},
		{"relay.ca", c.Relay.CA, "the path of your relay's public CA certificate (default /etc/lokadb/ca.crt, which the relay writes)"},
		{"relay.cloud_host", c.Relay.CloudHost, "how this network reaches the relay's organization ports"},
	} {
		if kv[1] == "" && c.Embedded {
			add(kv[0], kv[0]+": not chosen yet", "enter it on the panel's setup page, step 1: "+kv[2])
		} else if kv[1] == "" {
			add(kv[0], kv[0]+": not set in runner.json", "set it: "+kv[2])
		}
	}
	// an embedded runner with no relay address yet: that is the one thing to do now (the panel's
	// step 1); the files, the relay and the proxy follow from it, so they are not problems of their own
	if c.Embedded && c.Relay.Agent == "" {
		return humanize(out)
	}
	if c.Relay.PortMin <= 0 || c.Relay.PortMax < c.Relay.PortMin || c.Relay.PortMax > 65535 {
		add("relay.port_min", "relay.port_min/port_max: not a range of ports", "two ports, port_min up to port_max (default 15000-15999)")
	}
	var err error
	dbOK := c.dbRaw != ""
	if c.DB, err = Secret(c.dbRaw); err != nil {
		add("db", "db: "+err.Error(), "pass it to the container: export LOKADB_DB='host=… dbname=… user=… password=…', then docker run -e LOKADB_DB …")
		dbOK = false
	}
	// an embedded runner takes the tables from the panel (step 2), which needs the runner ready: a
	// missing spec is no problem there, the jobs that need it say so
	if _, noSpec := os.Stat(c.Spec); c.Spec != "" && !(c.Embedded && noSpec != nil && len(c.spec.Tables) == 0) {
		var sp admin.Spec
		if file(c.readFile("spec", c.Spec, &sp, fmt.Sprintf(specFix, c.Spec))) {
			c.spec = sp.Sorted()
		}
	}
	// the control plane and the credentials key: runner.json's, else the proxy config's (which
	// is optional: without it check applies the default fanout policy, reject)
	cp, key, keyOld := c.cpRaw, c.keyRaw, c.keyOldRaw
	c.proxyCfg = nil
	if c.ProxyConfig != "" {
		pc := &proxy.Config{}
		if file(c.readFile("proxy_config", c.ProxyConfig, pc, c.proxyFix())) {
			c.proxyCfg = pc
			if cp == "" {
				cp = pc.ControlPlane
			}
			if key == "" {
				key, keyOld = pc.CredentialsKey, pc.CredentialsKeyOld
			}
		}
	}
	if c.ProxyConfig == "" || c.proxyCfg != nil {
		var miss []string
		if cp == "" {
			miss = append(miss, "control_plane")
		}
		if key == "" {
			miss = append(miss, "credentials_key")
		}
		if len(miss) > 0 {
			where := "runner.json does not name it, and has no proxy_config"
			if c.ProxyConfig != "" {
				where = "neither runner.json nor " + c.ProxyConfig + " names it"
			}
			add(miss[0], strings.Join(miss, " and ")+": "+where, "the organizations' credentials live there (install step 4): give proxy_config, your proxy's config that has both, "+
				"or control_plane (best as env:LOKADB_CONTROL_PLANE) and credentials_key (the key file) in runner.json. https://docs.lokadb.com/getting-started/install/#4-the-control-plane-and-the-credentials-key")
		}
	}
	cpOK := false
	if cp != "" {
		if c.ControlPlane, err = Secret(cp); err != nil {
			add("control_plane", "control_plane: "+err.Error(), "pass it to the container: docker run -e "+strings.TrimPrefix(cp, "env:")+" …")
		} else {
			cpOK = true
		}
	}
	c.CredentialsKey, c.CredentialsKeyOld = key, keyOld
	for _, kv := range [][2]string{{"credentials_key", key}, {"credentials_key_old", keyOld}} {
		if kv[1] != "" {
			file(c.readFile(kv[0], kv[1], nil, "the credentials key, /etc/lokadb/credentials.key in the LokaDB directory, the same file your proxy uses"))
		}
	}
	if c.Relay.CA != "" {
		file(c.readFile("relay.ca", c.Relay.CA, nil, c.caFix()))
	}
	if h := c.Relay.CloudHost; h != "" {
		if p := resolveProblem(ctx, "relay.cloud_host", "the relay", h); p != nil {
			if h == RelayName {
				p.Fix = "the relay container is named " + RelayName + ": start it on this network with the docker run line the runner printed (docker logs lokadb-runner; the panel shows it too), then the name resolves. " + p.Fix
			}
			out = append(out, *p)
		}
	}
	if dbOK {
		if err := pingErr(ctx, c.DB); err != nil {
			out = append(out, c.embeddedDBProblem(connProblem("db", "the SaaS database", c.DB, err), err))
		} else if c.Embedded {
			c.probeDB(ctx)
		}
	}
	if cpOK {
		if err := pingErr(ctx, c.ControlPlane); err != nil {
			out = append(out, connProblem("control_plane", "the control plane", c.ControlPlane, err))
		}
	}
	return humanize(out)
}

// dockerFix: what to do when a host name does not resolve in the runner's container.
func dockerFix(host string) string {
	return fmt.Sprintf("if %s runs in Docker on this machine, start the runner on its network: docker run --network NAME … "+
		"(docker inspect -f '{{json .NetworkSettings.Networks}}' CONTAINER shows NAME; a docker-compose project's is usually PROJECT_default) "+
		"and use the service's name as the host, usually with sslmode=disable; otherwise use an address the container can reach", host)
}

// loopback: localhost and its addresses.
func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// localhostFix: in a container, localhost is the container itself.
const localhostFix = "localhost in the runner's container is the container itself: start the runner with --network host " +
	"(the database listens on this machine), or use an address the container can reach"

// resolveProblem: the host name resolves from here, or a problem that says what to do.
func resolveProblem(ctx context.Context, key, what, host string) *cloudproto.Problem {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if net.ParseIP(host) != nil || host == "localhost" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := net.DefaultResolver.LookupHost(ctx, host); err != nil {
		return &cloudproto.Problem{Key: key, Message: fmt.Sprintf("%s: %s at host %s cannot be resolved from the runner's container (%v)", key, what, host, err),
			Fix: dockerFix(host)}
	}
	return nil
}

// connProblem: why a database does not answer, in words, with what to do.
func connProblem(key, what, dsn string, err error) cloudproto.Problem {
	s := Redact(err.Error())
	host := ""
	if cfg, e := pgconn.ParseConfig(dsn); e == nil {
		host = cfg.Host
	}
	_, _, inContainer := hostPath("/")
	p := cloudproto.Problem{Key: key, Message: fmt.Sprintf("%s: %s at host %s does not answer: %s", key, what, host, s),
		Fix: fmt.Sprintf("%s must be reachable from the runner's container at that address", what)}
	var dns *net.DNSError
	switch {
	case strings.Contains(s, "cannot parse"):
		p.Message = fmt.Sprintf("%s: not a valid DSN: %s", key, s)
		p.Fix = "key=value: host=… port=5432 dbname=… user=… password=… sslmode=… (a password with spaces or quotes as password='…'); or a postgres:// URL with the password percent-encoded"
	case errors.As(err, &dns) || strings.Contains(s, "no such host") || strings.Contains(s, "hostname resolving error"):
		p.Message = fmt.Sprintf("%s: %s at host %s cannot be resolved from the runner's container (%s)", key, what, host, s)
		p.Fix = dockerFix(host)
	case strings.Contains(s, "password authentication failed"):
		p.Fix = "the user or the password in the DSN is wrong for this server: check them with psql. In key=value form write password='…' (a ' or \\ inside as \\' and \\\\); in a postgres:// URL, percent-encode the password"
	case strings.Contains(s, "no pg_hba.conf entry"):
		p.Fix = "the server's pg_hba.conf does not let this user in from the runner's address: add a host line for it (scram-sha-256) and reload Postgres"
	case strings.Contains(s, "does not exist"):
		p.Fix = "the database or the user named in the DSN does not exist on that server"
	case loopback(host) && inContainer:
		p.Fix = localhostFix
	case strings.Contains(s, "connection refused"):
		p.Fix = "nothing listens at that address and port as the runner's container sees it: check the host and the port; " + dockerFix(host)
	case strings.Contains(s, "SSL") || strings.Contains(s, "TLS"):
		p.Fix = "the server and the DSN disagree on TLS: a server without TLS (often inside a Docker network) needs sslmode=disable; sslmode=require needs TLS on the server"
	case strings.Contains(s, "timeout") || strings.Contains(s, "deadline"):
		p.Fix = "no answer in 10 seconds: a firewall, or an address the container cannot route to; " + dockerFix(host)
	}
	return p
}

// readFile reads a file the config names (as JSON into v, unless v is nil); a failure is a
// problem that names the path in the container and on the host.
func (c *Config) readFile(key, path string, v any, fix string) *cloudproto.Problem {
	if c.willMake[path] {
		return nil
	}
	b, err := os.ReadFile(path)
	if err == nil {
		if v == nil {
			return nil
		}
		if err := json.Unmarshal(b, v); err != nil {
			return &cloudproto.Problem{Key: key, Path: path, Message: fmt.Sprintf("%s: %s: not valid JSON: %v", key, path, err),
				Fix: "correct the file; the runner reads it again by itself"}
		}
		return nil
	}
	what := "cannot be read: " + err.Error()
	switch {
	case errors.Is(err, fs.ErrNotExist):
		what = "no such file"
	case errors.Is(err, fs.ErrPermission):
		what = "not readable by the runner (permission denied)"
	}
	p := &cloudproto.Problem{Key: key, Path: path, Message: fmt.Sprintf("%s: %s: %s", key, path, what), Fix: fix}
	if host, mounted, ok := hostPath(path); ok {
		p.Mounted = &mounted
		if mounted {
			p.Host = host
			p.Message += fmt.Sprintf(" (in the container; on the host: %s)", host)
		} else {
			p.Message += fmt.Sprintf(" (in the container, where %s is not mounted from the host)", filepath.Dir(path))
			p.Fix = fmt.Sprintf("keep the file in the LokaDB directory (docker run -v HOSTDIR:%s, the one mount of every LokaDB container), or add -v %s:%s:ro to docker run. %s", Dir, path, path, fix)
		}
	}
	return p
}

// pingErr: the database answers, or why not.
func pingErr(ctx context.Context, dsn string) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	return conn.Ping(ctx)
}

// mountInfo: the file of the runner's mounts (a test sets another).
var mountInfo = "/proc/self/mountinfo"

// hostPath: where a path the runner sees is on the host, from its container's mounts. ok: the
// runner runs in a container (its root is an overlay); mounted: the path is under a mount from
// the host (a -v bind or a volume), host its place there.
func hostPath(path string) (host string, mounted, ok bool) {
	b, err := os.ReadFile(mountInfo)
	if err != nil {
		return "", false, false
	}
	path = filepath.Clean(path)
	best := ""
	for _, line := range strings.Split(string(b), "\n") {
		// id parent major:minor root mountpoint options [optional…] - fstype source super
		f := strings.Fields(line)
		if len(f) < 5 {
			continue
		}
		root, point := unescape(f[3]), unescape(f[4])
		if point == "/" {
			if fsType(f) != "overlay" {
				return "", false, false // not a container's root: the paths are the host's
			}
			ok = true
			continue
		}
		if (path == point || strings.HasPrefix(path, point+"/")) && len(point) > len(best) {
			best, host = point, filepath.Join(root, strings.TrimPrefix(path, point))
		}
	}
	if !ok {
		return "", false, false
	}
	return host, best != "", true
}

func fsType(f []string) string {
	for i, x := range f {
		if x == "-" && i+1 < len(f) {
			return f[i+1]
		}
	}
	return ""
}

// unescape: mountinfo writes a space as \040 (a tab, a newline and a backslash in octal too).
func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+3 < len(s) {
			if n, err := strconv.ParseUint(s[i+1:i+4], 8, 8); err == nil {
				b.WriteByte(byte(n))
				i += 3
				continue
			}
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// statusOn: status snapshots are sent.
func (c *Config) statusOn() bool { return c.SendStatus == nil || *c.SendStatus }

// Secret resolves "env:NAME" and "file:PATH"; anything else is the value itself.
func Secret(v string) (string, error) {
	switch {
	case strings.HasPrefix(v, "env:"):
		name := strings.TrimPrefix(v, "env:")
		if os.Getenv(name) == "" {
			return "", fmt.Errorf("%s: the environment variable %s is empty", v, name)
		}
		return os.Getenv(name), nil
	case strings.HasPrefix(v, "file:"):
		b, err := os.ReadFile(strings.TrimPrefix(v, "file:"))
		return strings.TrimSpace(string(b)), err
	}
	return v, nil
}

// noDirHint: for a runner.json that is not there, the mount that puts the LokaDB directory in.
func noDirHint(path string) string {
	if filepath.Dir(path) != Dir {
		return ""
	}
	return fmt.Sprintf(". Every LokaDB container mounts the directory with the configs once: docker run -v HOSTDIR:%s … (HOSTDIR holds %s)", Dir, filepath.Base(path))
}

// proxyFix: what to do about a proxy config that is not there.
func (c *Config) proxyFix() string {
	if c.ProxyConfig == filepath.Join(Dir, "proxy.json") {
		return "your LokaDB proxy's config, proxy.json in the LokaDB directory. The runner makes it, with the control plane and the key, when the directory is mounted without :ro and LOKADB_DB may create roles and databases; or run lokadb init -dir HOSTDIR"
	}
	return "your LokaDB proxy's config, the file you start the proxy with"
}

// caFix: what to do about a relay CA that is not there.
func (c *Config) caFix() string {
	if c.Relay.CA == filepath.Join(Dir, "ca.crt") {
		return "the relay makes the CA and writes the public ca.crt here when it starts: start it with the docker run line the runner printed (docker logs lokadb-runner; the panel shows it too)"
	}
	return "your relay's public CA certificate"
}

// plain: how a problem's subject is named in its first line, in words and not in runner.json's
// keys (the panel and the log show Message first).
var plain = map[string]string{
	"db":                  "Your database",
	"spec":                "The list of tables to move (spec.json)",
	"owner":               "The owner role",
	"relay.agent":         "The relay's public address",
	"relay.ca":            "The relay's public certificate (ca.crt)",
	"relay.cloud_host":    "The relay's name on your network",
	"relay.port_min":      "The relay's port range",
	"proxy_config":        "The proxy's settings (proxy.json)",
	"control_plane":       "The control plane database",
	"credentials_key":     "The key for stored passwords (credentials.key)",
	"credentials_key_old": "The old key for stored passwords",
}

// humanize replaces the leading "key: " of each message by its plain name; a message that
// already says what it is about ("the control plane at host …") only gets a capital.
func humanize(ps []cloudproto.Problem) []cloudproto.Problem {
	for i, p := range ps {
		rest, ok := strings.CutPrefix(p.Message, p.Key+": ")
		if !ok {
			continue
		}
		name := plain[p.Key]
		switch {
		case name == "":
		case strings.HasPrefix(rest, "the "):
			ps[i].Message = strings.ToUpper(rest[:1]) + rest[1:]
		default:
			ps[i].Message = name + ": " + rest
		}
	}
	return ps
}

// resolves: the name has an address from here (a test replaces it).
var resolves = func(name string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupHost(ctx, name)
	return err == nil && len(addrs) > 0
}
