package runner

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// Embedded mode (tasks/panel-install.md): one container, one process: the runner, the relay and
// the proxy. The database comes from the application's environment, the files are made in the
// volume, and what the person chose in the panel (the relay's public address, the owner role, the
// tables) arrives with the runner's polls (cloudproto.Settings). Nothing is typed on the server
// except the paste the panel gives.

// DefaultService: the LokaDB API an embedded runner connects to unless LOKADB_SERVICE says otherwise.
const DefaultService = "https://api.lokadb.com"

// NewEmbedded is the Config of a runner that has no runner.json: it needs the token of the
// panel's paste (LOKADB_TOKEN) and the application's environment. get reads a variable.
func NewEmbedded(get func(string) string, log *slog.Logger) (*Config, error) {
	tok := strings.TrimSpace(get("LOKADB_TOKEN"))
	if tok == "" {
		tok = strings.TrimSpace(get("LOKADB_RUNNER_TOKEN"))
	}
	svc := get("LOKADB_SERVICE")
	if svc == "" {
		svc = DefaultService
	}
	// the application's own database and role come from its environment; LOKADB_DB, when it is
	// there, is the administrator the runner works as (an application role often may not create
	// a role, a database or an extension) and changes nothing about which role is the application's
	f, err := DiscoverDB(func(k string) string {
		if k == "LOKADB_DB" {
			return ""
		}
		return get(k)
	})
	// The application's settings file is the one the wizard's step 5 edits to point it at LokaDB's own
	// proxy. Read after that (a restart), it names LokaDB itself as the database: so what was found
	// before is kept in the volume, and used when the settings now name our own proxy.
	if err == nil {
		if selfLoop(f) {
			old, lerr := loadFound()
			if lerr != nil {
				return nil, fmt.Errorf("your application's settings now point at LokaDB's own proxy (%s:%s), and LokaDB has no earlier record of your database in %s: put the database's real address in LOKADB_DB", f.Host, f.Port, filepath.Join(Dir, "state"))
			}
			f = old
		} else {
			saveFound(f)
		}
	}
	admin := f.DSN
	if v := strings.TrimSpace(get("LOKADB_DB")); v != "" {
		a, aerr := fromDSN(v, "LOKADB_DB")
		if aerr != nil {
			return nil, aerr
		}
		if err != nil { // no application environment: the administrator's database is the one
			f, err = a, nil
		}
		admin = a.DSN
	}
	if err != nil {
		return nil, err
	}
	c := &Config{Service: svc, Token: tok, Embedded: true, found: &f, adminDSN: admin, Log: log}
	c.State = filepath.Join(Dir, "state")
	// the DSNs with their passwords stay in this process: the config names them by variable
	os.Setenv("LOKADB_DB", admin)
	os.Setenv("LOKADB_UPSTREAM", f.Upstream())
	c.DB = "env:LOKADB_DB"
	if _, _, inContainer := hostPath("/"); inContainer { // the wizard words step 5 by it
		c.setInfo("where", "container")
	} else {
		c.setInfo("where", "host")
	}
	c.Owner = f.User // the application's role owns its tables in nearly every setup; the panel can change it
	return c, c.finish()
}

// Found: what the runner found in the environment, without the password.
func (c *Config) Found() string {
	if c.found == nil {
		return ""
	}
	return c.found.Describe()
}

// ApplySettings makes the panel's settings the config's: the relay's public address, its name in
// this network, the owner role, and the spec (written to the volume as spec.json). It reports
// whether anything changed.
func (c *Config) ApplySettings(s *cloudproto.Settings) (bool, error) {
	if s == nil {
		return false, nil
	}
	changed := false
	cp := *s
	c.settings = &cp
	set := func(dst *string, v string) {
		if v != "" && *dst != v {
			*dst, changed = v, true
		}
	}
	c.public, c.publicKnown = s.ProxyPublic, true
	c.fanoutMode = s.Fanout
	if c.applyFanout() {
		changed = true
	}
	if c.applyPublicProxy() {
		changed = true
	}
	set(&c.Relay.Agent, s.RelayPublic)
	set(&c.Relay.CloudHost, s.RelayName)
	set(&c.Owner, s.Owner)
	if s.RelayPublic != "" {
		c.Relay.Enroll = "" // derived from the agent address again
		if h := hostOf(s.RelayPublic); h != "" {
			c.Relay.Enroll = h + ":7444"
		}
	}
	if len(s.Spec) > 0 && string(s.Spec) != "null" {
		path := filepath.Join(Dir, "spec.json")
		if old, err := os.ReadFile(path); err != nil || !jsonEqual(old, s.Spec) {
			if err := os.MkdirAll(Dir, 0o755); err != nil {
				return changed, err
			}
			if err := os.WriteFile(path, append(append([]byte{}, s.Spec...), '\n'), 0o644); err != nil {
				return changed, fmt.Errorf("spec.json: %w", err)
			}
			changed = true
		}
		var sp admin.Spec
		if err := json.Unmarshal(s.Spec, &sp); err != nil {
			return changed, fmt.Errorf("the spec from the panel: %w", err)
		}
		c.spec = sp.Sorted()
	}
	return changed, nil
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xb) == string(yb)
}

func hostOf(addr string) string {
	if i := strings.LastIndex(addr, ":"); i > 0 && !strings.Contains(addr[i:], "]") {
		return addr[:i]
	}
	return addr
}

var embeddedOnce sync.Once

// startEmbedded starts the relay and the proxy of this process once their files are there
// (provisioning made them): the same code as lokadb relay and lokadb proxy.
func (c *Config) startEmbedded(ctx context.Context) error {
	for _, f := range []string{"relay.json", "proxy.json", "control-plane.dsn", "credentials.key"} {
		if _, err := os.Stat(filepath.Join(Dir, f)); err != nil {
			return nil // not yet: the next check provisions and tries again
		}
	}
	var err error
	c.applyPublicProxy() // before the proxy reads its file
	c.applyFanout()
	embeddedOnce.Do(func() { err = c.runEmbedded(ctx); embeddedDone.Store(err == nil) })
	return err
}

func (c *Config) runEmbedded(ctx context.Context) error {
	log := c.Log
	if log == nil {
		log = slog.Default()
	}
	var rc tunnel.RelayConfig
	rpath := filepath.Join(Dir, "relay.json")
	b, err := os.ReadFile(rpath)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, &rc); err != nil {
		return fmt.Errorf("%s: %w", rpath, err)
	}
	rc.ConfigPath = rpath
	if err := rc.PrepareCerts(log); err != nil {
		return err
	}
	if rc.ControlPlane, err = Secret(rc.ControlPlane); err != nil {
		return err
	}
	store, err := tunnel.OpenPGStoreForRelay(ctx, rc.ControlPlane, log)
	if err != nil {
		return fmt.Errorf("the control plane: %w", err)
	}
	rl := tunnel.NewRelay(rc, store, log)
	c.setInfo("relay", "up")
	go func() {
		if err := rl.Run(); err != nil {
			log.Error("relay", "err", err)
			c.setInfo("relay", "stopped: "+Redact(err.Error()))
		}
	}()
	go func() { <-ctx.Done(); rl.Drain() }()

	var pc proxy.Config
	ppath := filepath.Join(Dir, "proxy.json")
	if b, err = os.ReadFile(ppath); err != nil {
		return err
	}
	if err := json.Unmarshal(b, &pc); err != nil {
		return fmt.Errorf("%s: %w", ppath, err)
	}
	if pc.Upstream, err = Secret(pc.Upstream); err != nil {
		return err
	}
	if pc.ControlPlane, err = Secret(pc.ControlPlane); err != nil {
		return err
	}
	if err := pc.Tenant.Validate(); err != nil {
		return err
	}
	c.setInfo("proxy", "up")
	c.setInfo("proxy_listen", pc.Listen)
	srv := proxy.NewServer(pc, log)
	go c.watchApp(ctx, srv)
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Error("proxy", "err", err)
			c.setInfo("proxy", "stopped: "+Redact(err.Error()))
		}
	}()
	log.Info("relay and proxy started in this process", "relay_agents", rc.Listen, "proxy", pc.Listen)
	return nil
}

func settingsEqual(a, b *cloudproto.Settings) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// applySettings makes the panel's latest settings the config's (the runner calls it before
// each check of its config and before a check or enable job).
func (r *Runner) applySettings() {
	r.mu.Lock()
	s := r.settings
	r.mu.Unlock()
	if s == nil {
		return
	}
	changed, err := r.Cfg.ApplySettings(s)
	if err != nil {
		r.Log.Warn("the panel's settings could not be applied", "err", err)
		return
	}
	if changed {
		r.Log.Info("the panel's settings applied", "relay", s.RelayPublic, "owner", s.Owner, "spec", len(s.Spec) > 0)
	}
	if r.Cfg.RestartRequested() && r.Stop != nil {
		r.Log.Warn("ending the process to start it again with the new proxy setting")
		r.Stop()
	}
}

// infoMu guards the facts a runner reports of itself (the poll reads them while checks run).
var infoMu sync.Mutex

// Info: facts for the setup wizard, sent with each poll: what was found in the environment and
// what is started in this process. Nothing secret.
func (c *Config) Info() map[string]string {
	infoMu.Lock()
	defer infoMu.Unlock()
	out := map[string]string{}
	if c.found != nil {
		out["database"] = c.found.Describe()
	}
	if c.Embedded {
		out["embedded"] = "yes"
	}
	var names []string
	for _, t := range c.spec.Tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	out["spec"] = strings.Join(names, ",") // the tables the runner works with: the panel waits for the saved choice
	// the relay as the agents dial it, and its CA (a public certificate): the service checks from the
	// internet that customers reach it, and that it is this relay that answers
	if c.Relay.Agent != "" {
		out["relay_agent"], out["relay_enroll"] = c.Relay.Agent, c.Relay.Enroll
		if c.Relay.ServerName != "" {
			out["relay_server_name"] = c.Relay.ServerName
		}
		if b, err := os.ReadFile(c.Relay.CA); err == nil && c.Relay.CA != "" {
			out["relay_ca"] = string(b)
		}
	}
	for k, v := range c.info {
		out[k] = v
	}
	return out
}

func (c *Config) setInfo(k, v string) {
	infoMu.Lock()
	defer infoMu.Unlock()
	if c.info == nil {
		c.info = map[string]string{}
	}
	c.info[k] = v
}

// watchApp tells, every few seconds, whether the application goes through LokaDB: the proxy of
// this process counts the sessions of the application's role ("proxy_sessions"), and the database
// says how many connections of that role do not come from this container ("direct_connections",
// pg_stat_activity). The wizard ticks "Your application goes through LokaDB" only when the first is
// more than 0 and the second is 0. Both are facts, not what the person says they did.
func (c *Config) watchApp(ctx context.Context, srv *proxy.Server) {
	if c.found == nil {
		return
	}
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	var prevDirect int64
	for {
		c.watchRole(ctx)
		list, over := srv.Unrouted()
		mine := list[:0:0] // the application's database only: the proxy may serve others
		for _, u := range list {
			if u.DB == "" || u.DB == c.found.DBName {
				mine = append(mine, u)
			}
		}
		c.collectUnrouted(mine, over)
		var via int64
		var last time.Time
		for _, u := range srv.Seen() {
			if u.User == c.found.User {
				via += u.Total
				if u.Last.After(last) {
					last = u.Last
				}
			}
		}
		c.setInfo("proxy_sessions", strconv.FormatInt(via, 10))
		if !last.IsZero() {
			c.setInfo("proxy_last", last.UTC().Format(time.RFC3339))
		}
		// two samples in a row: a connection of the runner's own work (as the application's role,
		// when no administrator was given) lasts seconds; the application's pool stays
		if n, err := directConnections(ctx, c.adminDSN, c.found.User, c.found.DBName, srv.UpstreamPIDs()); err != nil {
			c.setInfo("direct_connections", "")
			prevDirect = 0
		} else {
			shown := n
			if prevDirect == 0 {
				shown = 0
			}
			prevDirect = n
			c.setInfo("direct_connections", strconv.FormatInt(shown, 10))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// appRoleRisk: why the proxy would refuse the application's role (it is, or can SET ROLE to, a
// superuser, a BYPASSRLS or a REPLICATION role: the proxy's own words), "" when it would not.
// Row level security does not apply to such a role, so the proxy lets none in; the official postgres
// image makes POSTGRES_USER a superuser, and an application that uses it is refused at step 5.
//
// bootstrap: the role is the one the cluster was made with (POSTGRES_USER of the postgres image).
// Postgres will not take SUPERUSER from it ("The bootstrap superuser must have the SUPERUSER
// attribute"), so for it the fix is a new application role, not ALTER ROLE.
func appRoleRisk(ctx context.Context, dsn, role string) (why string, bootstrap bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, err := ownsql.Connect(ctx, dsn)
	if err != nil {
		return "", false, err
	}
	defer conn.Close(context.Background())
	var name string
	var super, bypass, repl, self bool
	err = conn.QueryRow(ctx, `SELECT r.rolname::text, r.rolsuper, r.rolbypassrls, r.rolreplication, r.oid = a.oid
		FROM pg_roles a JOIN pg_roles r ON r.oid = a.oid
		  OR pg_has_role(a.oid, r.oid, CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'SET' ELSE 'MEMBER' END)
		WHERE a.rolname = $1 AND (r.rolsuper OR r.rolbypassrls OR r.rolreplication)
		ORDER BY (r.oid = a.oid) DESC, 1 LIMIT 1`, role).Scan(&name, &super, &bypass, &repl, &self)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	why = proxy.RoleAttrs(super, bypass, repl)
	if !self {
		why = "can SET ROLE to " + name + ", which " + why
	}
	if self && super {
		err = conn.QueryRow(ctx, `SELECT oid = 10 FROM pg_roles WHERE rolname = $1`, role).Scan(&bootstrap)
	}
	return why, bootstrap, err
}

// watchRole keeps the panel's view of the application's role current (the owner fixes it while the
// runner runs): "app_role" and, when the proxy would refuse it, "app_role_risk" with the reason.
func (c *Config) watchRole(ctx context.Context) {
	if c.found == nil {
		return
	}
	why, bootstrap, err := appRoleRisk(ctx, c.adminDSN, c.found.User)
	if err != nil {
		return // keep what was known
	}
	c.setInfo("app_role", c.found.User)
	c.setInfo("app_role_risk", why)
	if bootstrap {
		c.setInfo("app_role_bootstrap", "yes")
	} else {
		c.setInfo("app_role_bootstrap", "")
	}
}

// directConnections: connections of role to database whose backend is not one the proxy opened
// (proxyPIDs), whatever address they come from: on a plain host the application and the proxy are
// on the same machine.
func directConnections(ctx context.Context, dsn, role, db string, proxyPIDs []int32) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := ownsql.Connect(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	var n int64
	err = conn.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND datname = $2 AND backend_type = 'client backend'
		AND pid <> pg_backend_pid() AND NOT (pid = ANY($3::int[]))`, role, db, proxyPIDs).Scan(&n)
	return n, err
}

// embeddedDBProblem says, for a runner in a container, what a database that does not answer
// usually means there: the application's settings name localhost (which is the container itself
// in here), or the database is on a Docker network the container is not on.
func (c *Config) embeddedDBProblem(p cloudproto.Problem, cause error) cloudproto.Problem {
	if !c.Embedded {
		return p
	}
	if cfg, err := pgconn.ParseConfig(c.DB); err == nil && loopback(cfg.Host) {
		p.Message = fmt.Sprintf("Your application's settings put the database at %s. Inside LokaDB's container that address is the container itself, not your server, so nothing answers there.", cfg.Host)
		p.Fix = "Use the name the database has on the Docker network: the Postgres service of your docker-compose.yml (for example DB_HOST=postgres). " +
			"For a Postgres installed on this server itself (not in Docker), give the LokaDB container extra_hosts: [\"host.docker.internal:host-gateway\"] and use host.docker.internal; " +
			"Postgres must listen on that address and its pg_hba.conf must let the container in. Or use the server's own address. LokaDB reads the same settings file as your application, so if the application itself runs outside Docker, give the container its own LOKADB_DB with the address it can reach."
		return p
	}
	if m := pgHBA.FindStringSubmatch(cause.Error()); m != nil {
		p.Message = fmt.Sprintf("The database refused LokaDB: it sees it at the address %s and has no rule for it.", m[1])
		p.Fix = fmt.Sprintf("Allow %s: in the provider's firewall or security group, and in pg_hba.conf if you run the server (a host line for this address, user and database, scram-sha-256, and a reload).", m[1])
		if strings.Contains(cause.Error(), "no encryption") {
			// the server says the same for an unlisted address and for a plain connection to a server that wants TLS
			if cfg, err := pgconn.ParseConfig(c.DB); err == nil && cfg.TLSConfig == nil {
				p.Message = fmt.Sprintf("The database has no rule for LokaDB (it sees it at %s) for a connection that is not encrypted. Either it accepts only TLS, or that address is not allowed.", m[1])
				p.Fix = "First add sslmode=require to the database settings (DATABASE_URL gets ?sslmode=require; or DB_SSLMODE=require, or sslmode=require in LOKADB_DB): most managed services accept nothing else. If it still refuses, allow " + m[1] + " in the provider's firewall or security group, and in pg_hba.conf if you run the server. A host name without dots is taken for a Docker service, which has no TLS."
			}
		}
		return p
	}
	if cfg, err := pgconn.ParseConfig(c.DB); err == nil && ManagedProvider(cfg.Host) != "" && (strings.Contains(cause.Error(), "timeout") || strings.Contains(cause.Error(), "deadline") || strings.Contains(cause.Error(), "i/o timeout")) {
		p.Message = fmt.Sprintf("%s did not answer in time. Usually its firewall does not let this server in.", ManagedProvider(cfg.Host))
		p.Fix = "Allow this server's outgoing address in the provider's firewall (an RDS security group, Cloud SQL authorized networks, an Azure firewall rule, Supabase network restrictions...). " +
			"This server's own addresses are " + localAddresses() + "; behind a NAT (most cloud servers) the one the database sees is the public address of the NAT or of the server, which your cloud console shows as the instance's public IP (or run curl https://checkip.amazonaws.com on the server yourself). When LokaDB can connect, this page shows the address the database sees."
		return p
	}
	if strings.Contains(p.Message, "cannot be resolved") || strings.Contains(Redact(cause.Error()), "no such host") {
		p.Fix = "LokaDB's container is on your application's docker-compose network only. If Postgres runs in another docker-compose project, put the lokadb service on that project's network too: " +
			"networks: [default, DBPROJECT_default] under lokadb, and at the bottom of the file networks: { DBPROJECT_default: { external: true } } (docker network ls shows the network's name). " +
			"If Postgres is on another machine, use its address in the settings, not a service name."
	}
	return p
}

// selfLoop: the database the settings name is this program's own proxy (the application was
// pointed at LokaDB, step 5 of the wizard).
func selfLoop(f Found) bool {
	if f.Port != "6432" {
		return false
	}
	h, _ := os.Hostname()
	switch f.Host {
	case "127.0.0.1", "localhost", "::1", "lokadb", h:
		return true
	}
	return false
}

func foundPath() string { return filepath.Join(Dir, "state", "found.json") }

// saveFound keeps the database found in the environment (with its password, mode 0600, in the
// directory that already holds the credentials key) for the restart after the application was
// pointed at the proxy.
func saveFound(f Found) {
	if err := os.MkdirAll(filepath.Dir(foundPath()), 0o700); err != nil {
		return
	}
	if b, err := json.Marshal(f); err == nil {
		_ = os.WriteFile(foundPath(), b, 0o600)
	}
}

func loadFound() (Found, error) {
	var f Found
	b, err := os.ReadFile(foundPath())
	if err != nil {
		return f, err
	}
	return f, json.Unmarshal(b, &f)
}

// managedHosts: the name endings of the managed Postgres services, and what the provider calls
// the administrator role (the panel says which one to use).
var managedHosts = []struct{ suffix, provider string }{
	{".rds.amazonaws.com", "Amazon RDS or Aurora"},
	{".rds.amazonaws.com.cn", "Amazon RDS or Aurora"},
	{".database.azure.com", "Azure Database for PostgreSQL"},
	{".postgres.database.azure.com", "Azure Database for PostgreSQL"},
	{".supabase.co", "Supabase"},
	{".pooler.supabase.com", "Supabase"},
	{".neon.tech", "Neon"},
	{".db.ondigitalocean.com", "DigitalOcean"},
	{".cloudsql.google.com", "Google Cloud SQL"},
	{".aivencloud.com", "Aiven"},
	{".render.com", "Render"},
	{".railway.app", "Railway"},
	{".rlwy.net", "Railway"},
	{".crunchybridge.com", "Crunchy Bridge"},
}

// ManagedProvider: the managed service a host name belongs to, "" for any other.
func ManagedProvider(host string) string {
	h := strings.ToLower(host)
	for _, m := range managedHosts {
		if strings.HasSuffix(h, m.suffix) {
			return m.provider
		}
	}
	return ""
}

var pgHBA = regexp.MustCompile(`no pg_hba\.conf entry for host "([^"]+)"`)

// probeDB tells the panel two facts about the database the runner is connected to: the address the
// database sees LokaDB at (what a firewall or allowlist of a managed service must let in: from the
// database itself, no third party is asked), and whether the role LokaDB works as is an
// administrator (enable needs to create an extension and roles).
func (c *Config) probeDB(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	conn, err := ownsql.Connect(cctx, c.DB)
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	var addr *string
	var admin bool
	err = conn.QueryRow(cctx, `SELECT host(inet_client_addr()),
		coalesce((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false) OR EXISTS (
		  SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid JOIN pg_roles u ON u.oid = m.member
		  WHERE u.rolname = current_user AND r.rolname IN ('rds_superuser', 'cloudsqlsuperuser', 'azure_pg_admin', 'supabase_admin', 'neon_superuser', 'doadmin'))`).Scan(&addr, &admin)
	if err != nil {
		return
	}
	if addr != nil {
		c.setInfo("outgoing_address", *addr)
	}
	if admin {
		c.setInfo("rights", "administrator")
	} else {
		c.setInfo("rights", "limited")
	}
	c.watchRole(ctx)
	if cfg, err := pgconn.ParseConfig(c.DB); err == nil {
		c.setInfo("db_user", cfg.User)
		if p := ManagedProvider(cfg.Host); p != "" {
			c.setInfo("managed", p)
		}
	}
	var addrs []string
	if ifs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifs {
			if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && !ipn.IP.IsLinkLocalUnicast() {
				addrs = append(addrs, ipn.IP.String())
			}
		}
	}
	c.setInfo("local_addresses", strings.Join(addrs, ", "))
}

func localAddresses() string {
	var addrs []string
	if ifs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifs {
			if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && !ipn.IP.IsLinkLocalUnicast() {
				addrs = append(addrs, ipn.IP.String())
			}
		}
	}
	if len(addrs) == 0 {
		return "not known here"
	}
	return strings.Join(addrs, ", ")
}

// publicProxyJSON: the keys of proxy.json that the public proxy owns.
type publicKeys struct {
	TLS   json.RawMessage `json:"tls,omitempty"`
	Allow []string        `json:"allow,omitempty"`
}

// ValidatePublicProxy: see cloudproto.PublicProxy.Validate.
func ValidatePublicProxy(p *cloudproto.PublicProxy) error { return p.Validate() }

// applyPublicProxy makes proxy.json say what the panel chose: TLS required with a certificate for the
// public name (made once, self-signed, in the directory; a certificate of your own put there as
// proxy.crt and proxy.key is used) and the allow list; or neither, when the panel turned the public
// proxy off. It reports a change. A proxy that is already running is restarted (the process exits,
// and its supervisor starts it again: a second of reconnecting for the application).
func (c *Config) applyPublicProxy() bool {
	if !c.publicKnown {
		return false // a restart must not wipe what was applied before the panel's settings come
	}
	path := filepath.Join(Dir, "proxy.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return false // not made yet: Prepare calls this again after it is
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(b, &m) != nil {
		return false
	}
	want := map[string]json.RawMessage{}
	if p := c.public; p != nil {
		if err := ValidatePublicProxy(p); err != nil {
			if c.Log != nil {
				c.Log.Warn("the public proxy's settings are not used", "err", err)
			}
			return false
		}
		crt, key := filepath.Join(Dir, "proxy.crt"), filepath.Join(Dir, "proxy.key")
		if err := ensureSelfSigned(crt, key, strings.TrimSpace(p.Name)); err != nil {
			if c.Log != nil {
				c.Log.Warn("the proxy's certificate could not be made", "err", err)
			}
			return false
		}
		want["tls"], _ = json.Marshal(map[string]any{"cert": crt, "key": key, "require": true})
		allow := make([]string, 0, len(p.Allow))
		for _, e := range p.Allow {
			allow = append(allow, strings.TrimSpace(e))
		}
		sort.Strings(allow)
		want["allow"], _ = json.Marshal(allow)
	}
	same := true
	for _, k := range []string{"tls", "allow"} {
		if !jsonEqual(m[k], want[k]) && !(m[k] == nil && want[k] == nil) {
			same = false
		}
	}
	if same {
		return false
	}
	for _, k := range []string{"tls", "allow"} {
		if v, ok := want[k]; ok {
			m[k] = v
		} else {
			delete(m, k)
		}
	}
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return false
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, append(out, '\n'), 0o644) != nil || os.Rename(tmp, path) != nil {
		return false
	}
	if c.Log != nil {
		c.Log.Warn("the proxy's public setting changed: LokaDB restarts to apply it", "public", c.public != nil)
	}
	c.restart = embeddedStarted()
	return true
}

// RestartRequested: a setting that needs a new process was applied (the caller ends the process).
func (c *Config) RestartRequested() bool { return c.restart }

var embeddedDone atomic.Bool

func embeddedStarted() bool { return embeddedDone.Load() }

// ensureSelfSigned makes a certificate and key for name if neither file is there.
func ensureSelfSigned(crt, key, name string) error {
	if _, err := os.Stat(crt); err == nil {
		if _, err := os.Stat(key); err == nil {
			return nil
		}
	}
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: name}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(1, 0, 0),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	if ip := net.ParseIP(name); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{name}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &k.PublicKey, k)
	if err != nil {
		return err
	}
	kd, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		return err
	}
	if err := os.WriteFile(crt, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return err
	}
	return os.WriteFile(key, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd}), 0o600)
}

// applyFanout makes proxy.json say the panel's fanout policy for the application's database: the
// mode of its entry in "fanout". The entry's allow list, the entries of other databases and every
// other key stay as they are (they may be the person's own edits). Nothing is chosen: nothing is
// touched. A proxy that is already running is restarted, as for the public setting. It reports a change.
func (c *Config) applyFanout() bool {
	if c.fanoutMode == "" || c.found == nil || c.found.DBName == "" {
		return false
	}
	path := filepath.Join(Dir, "proxy.json")
	b, err := os.ReadFile(path)
	if err != nil {
		return false // not made yet: Prepare calls this again after it is
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(b, &m) != nil {
		return false
	}
	fan := map[string]json.RawMessage{}
	if m["fanout"] != nil && json.Unmarshal(m["fanout"], &fan) != nil {
		return false // not an object: the person's file is not ours to rewrite
	}
	entry := map[string]json.RawMessage{}
	if e := fan[c.found.DBName]; e != nil {
		_ = json.Unmarshal(e, &entry)
	}
	if string(entry["mode"]) == strconv.Quote(c.fanoutMode) {
		return false
	}
	entry["mode"], _ = json.Marshal(c.fanoutMode)
	fan[c.found.DBName], _ = json.Marshal(entry)
	m["fanout"], _ = json.Marshal(fan)
	out, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return false
	}
	tmp := path + ".tmp"
	if os.WriteFile(tmp, append(out, '\n'), 0o644) != nil || os.Rename(tmp, path) != nil {
		return false
	}
	if c.Log != nil {
		c.Log.Warn("the fanout policy changed: LokaDB restarts to apply it", "database", c.found.DBName, "mode", c.fanoutMode)
	}
	if embeddedStarted() {
		c.restart = true
	}
	return true
}
