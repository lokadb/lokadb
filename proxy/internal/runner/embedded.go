package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	embeddedOnce.Do(func() { err = c.runEmbedded(ctx) })
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
