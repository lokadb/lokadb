// Package initcmd is `lokadb init`: the files a first install needs, made in one command, so
// that nobody types a password or a path by hand: the control plane's role and database with
// a random password, the credentials key, the CA and the relay certificate, relay.json and a
// skeleton of proxy.json. It is idempotent: what exists is kept, never overwritten, a key and
// a CA least of all.
package initcmd

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/creds"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

type Options struct {
	Dir          string // where the files are written (on this machine)
	ContainerDir string // where the containers see Dir (paths in the configs); default /etc/lokadb
	RelayHost    string // the name (or IP) agents dial the relay by; several, comma separated
	AdminDSN     string // creates the control plane role and database; "" leaves that to you
	CPRole       string // default lokadb_cp
	CPDB         string // default lokadb_cp
	CPHost       string // the control plane's host as the containers reach it (default: the admin DSN's)
	CPPort       string
	CPSSLMode    string // default: the admin DSN's, else require
	OrgAllow     []string
	Network      string // docker network for the printed docker run lines ("" none)
	Image        string // default ghcr.io/lokadb/lokadb:latest
	Out          io.Writer
}

const (
	defaultContainerDir = "/etc/lokadb"
	defaultImage        = "ghcr.io/lokadb/lokadb:latest"
)

// Run makes what is missing. It prints what it did (created, kept, skipped), never a password.
func Run(ctx context.Context, o Options) error {
	if o.Out == nil {
		o.Out = io.Discard
	}
	if o.Dir == "" || o.RelayHost == "" {
		return errors.New("init: -dir and -relay-host are needed")
	}
	if o.ContainerDir == "" {
		o.ContainerDir = defaultContainerDir
	}
	if o.CPRole == "" {
		o.CPRole = "lokadb_cp"
	}
	if o.CPDB == "" {
		o.CPDB = "lokadb_cp"
	}
	if o.Image == "" {
		o.Image = defaultImage
	}
	if len(o.OrgAllow) == 0 {
		// the private ranges: container addresses change, a CIDR covers them
		o.OrgAllow = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	}
	say := func(format string, a ...any) { fmt.Fprintf(o.Out, format+"\n", a...) }
	// the layout the docs describe: relay/ holds the CA (certificate and key) and the relay's
	// certificate and only the relay mounts it; the public ca.crt is copied to Dir for the agent,
	// the runner and the proxy
	relayDir := filepath.Join(o.Dir, "relay")
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return err
	}

	// 1. the CA and the relay certificate; the CA key apart from what containers mount
	if err := os.MkdirAll(relayDir, 0o700); err != nil { // the CA key is in it
		return err
	}
	st, err := tunnel.EnsureCerts(relayDir, relayDir, o.RelayHost)
	if err != nil {
		return fmt.Errorf("init: certificates: %w", err)
	}
	say("CA key            %s  (%s; only the relay mounts relay/)", filepath.Join(relayDir, "ca.key"), st.CA)
	say("relay certificate %s  (%s; names: %s)", filepath.Join(relayDir, "relay.crt"), st.Relay, strings.Join(st.RelayNames, ", "))
	pub := filepath.Join(o.Dir, "ca.crt")
	caPEM, err := os.ReadFile(filepath.Join(relayDir, "ca.crt"))
	if err != nil {
		return err
	}
	if old, err := os.ReadFile(pub); err == nil && string(old) == string(caPEM) {
		say("CA certificate    %s  (kept; the public copy for the agent, the runner and the proxy)", pub)
	} else if err == nil {
		say("CA certificate    %s  (kept, but it is NOT the CA of relay/ca.crt: copy relay/ca.crt over it if relay/ is the CA you mean)", pub)
	} else {
		if err := writeNew(pub, caPEM, 0o644); err != nil {
			return err
		}
		say("CA certificate    %s  (created: the public copy for the agent, the runner and the proxy)", pub)
	}

	// 2. the credentials key
	keyPath := filepath.Join(o.Dir, "credentials.key")
	if _, err := os.Stat(keyPath); err == nil {
		if _, err := creds.LoadKey(keyPath); err != nil {
			return fmt.Errorf("init: %s exists but is not a key: %w (it is not replaced: sealed credentials depend on it)", keyPath, err)
		}
		say("credentials key   %s  (kept)", keyPath)
	} else {
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		if err := writeNew(keyPath, []byte(hex.EncodeToString(b)+"\n"), 0o600); err != nil {
			return err
		}
		say("credentials key   %s  (created, 0600)", keyPath)
	}

	// 3. the control plane: a role with a random password and a database of its own
	dsnPath := filepath.Join(o.Dir, "control-plane.dsn")
	if err := controlPlane(ctx, &o, dsnPath, say); err != nil {
		return err
	}

	// 4. the configs
	cd := o.ContainerDir
	relayCfg := map[string]any{}
	relayJSON := orderedJSON{
		{"listen", ":7443"}, {"enroll_listen", ":7444"}, {"health_listen", ":7446"},
		{"cert", cd + "/relay/relay.crt"}, {"key", cd + "/relay/relay.key"}, {"ca", cd + "/relay/ca.crt"},
		{"ca_key", cd + "/relay/ca.key"}, {"control_plane", "file:" + cd + "/control-plane.dsn"},
		{"orgs", relayCfg}, {"org_allow", o.OrgAllow},
	}
	proxyJSON := orderedJSON{
		{"listen", ":6432"}, {"upstream", "env:LOKADB_UPSTREAM"},
		{"control_plane", "file:" + cd + "/control-plane.dsn"}, {"credentials_key", cd + "/credentials.key"},
		{"fanout", map[string]any{"*": map[string]any{"mode": "reject"}}},
	}
	for _, f := range []struct {
		name string
		v    orderedJSON
	}{{"relay.json", relayJSON}, {"proxy.json", proxyJSON}} {
		path := filepath.Join(o.Dir, f.name)
		if _, err := os.Stat(path); err == nil {
			say("%-17s %s  (kept)", f.name, path)
			continue
		}
		b, err := f.v.marshal()
		if err != nil {
			return err
		}
		if err := writeNew(path, b, 0o644); err != nil {
			return err
		}
		say("%-17s %s  (created)", f.name, path)
	}

	// 5. what to run
	net := ""
	if o.Network != "" {
		net = " --network " + o.Network
	}
	d := o.Dir
	say(`
Warnings
  - %s holds the CA key: mount it into the relay only. The agent, the runner and the proxy need %s only (the public CA certificate).
  - org_allow in relay.json is %s: the addresses that may reach an organization's port on the relay. A CIDR is fine (container addresses change); narrow it to your proxy's network.

Next
  export LOKADB_UPSTREAM='host=<cloud database> port=5432 dbname=<db> user=<role> password=<...> sslmode=<...>'   # the proxy's upstream, not stored in a file

  docker run -d --name lokadb-relay%s -p 7443:7443 -p 7444:7444 -p 7446:7446 \
    -v %s/relay.json:%s/relay.json:ro -v %s/relay:%s/relay:ro \
    -v %s/control-plane.dsn:%s/control-plane.dsn:ro %s relay -config %s/relay.json

  docker run -d --name lokadb-proxy%s -p 6432:6432 -e LOKADB_UPSTREAM \
    -v %s/proxy.json:%s/proxy.json:ro -v %s/credentials.key:%s/credentials.key:ro \
    -v %s/control-plane.dsn:%s/control-plane.dsn:ro %s proxy -config %s/proxy.json

  Ports: 7443 agents (mutual TLS), 7444 enrollment, 7446 health and /sessions /status.`,
		d+"/relay", d+"/ca.crt", strings.Join(o.OrgAllow, ", "),
		net, d, cd, d, cd, d, cd, o.Image, cd,
		net, d, cd, d, cd, d, cd, o.Image, cd)
	return nil
}

// writeNew creates path and refuses to replace it.
func writeNew(path string, b []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Chmod(mode); err != nil {
		return err
	}
	_, err = f.Write(b)
	return err
}

type kv struct {
	k string
	v any
}

// orderedJSON: a JSON object with its keys in the order given (a file a person reads).
type orderedJSON []kv

func (o orderedJSON) marshal() ([]byte, error) {
	var sb strings.Builder
	sb.WriteString("{\n")
	for i, e := range o {
		v, err := json.Marshal(e.v)
		if err != nil {
			return nil, err
		}
		sep := ","
		if i == len(o)-1 {
			sep = ""
		}
		fmt.Fprintf(&sb, "  %q: %s%s\n", e.k, v, sep)
	}
	sb.WriteString("}\n")
	return []byte(sb.String()), nil
}

// controlPlane makes the role and the database, or keeps them, and writes the DSN file the
// relay and the proxy read ("file:"), 0600. Without an admin DSN it only reports what is left to do.
func controlPlane(ctx context.Context, o *Options, dsnPath string, say func(string, ...any)) error {
	_, statErr := os.Stat(dsnPath)
	haveFile := statErr == nil
	if o.AdminDSN == "" {
		if haveFile {
			say("control plane    %s  (kept)", dsnPath)
		} else {
			say("control plane    not made: give -admin DSN (a role that may create roles and databases), or create a role and a database yourself and write their DSN to %s (0600)", dsnPath)
		}
		return nil
	}
	cfg, err := pgconn.ParseConfig(o.AdminDSN)
	if err != nil {
		return fmt.Errorf("init: -admin: %w", err)
	}
	host, port, ssl := o.CPHost, o.CPPort, o.CPSSLMode
	if host == "" {
		host = cfg.Host
	}
	if port == "" {
		port = fmt.Sprint(cfg.Port)
	}
	if ssl == "" {
		ssl = sslModeOf(o.AdminDSN)
	}
	admin, err := pgx.Connect(ctx, o.AdminDSN)
	if err != nil {
		return fmt.Errorf("init: -admin: %w", scrub(err, cfg.Password))
	}
	defer admin.Close(ctx)
	var roleExists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, o.CPRole).Scan(&roleExists); err != nil {
		return err
	}
	var password string
	switch {
	case roleExists && haveFile:
		say("control plane    role %s and %s (kept; the password stays in %s)", o.CPRole, dsnPath, dsnPath)
		password, err = passwordIn(dsnPath)
		if err != nil {
			return err
		}
	case roleExists:
		return fmt.Errorf("init: the role %s exists but %s does not: its password is unknown, and init does not change a password it did not set. Write the DSN to that file yourself, or drop the role (DROP ROLE %s) to have it made again", o.CPRole, dsnPath, o.CPRole)
	default:
		b := make([]byte, 24)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		password = base64.RawURLEncoding.EncodeToString(b)
		if _, err := admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{o.CPRole}.Sanitize()+" LOGIN PASSWORD "+quoteLit(password)); err != nil {
			return fmt.Errorf("init: CREATE ROLE %s: %w", o.CPRole, err)
		}
		say("control plane    role %s (created, random password, not printed)", o.CPRole)
	}
	var dbExists bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, o.CPDB).Scan(&dbExists); err != nil {
		return err
	}
	if dbExists {
		say("control plane    database %s (kept)", o.CPDB)
	} else {
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{o.CPDB}.Sanitize()+" OWNER "+pgx.Identifier{o.CPRole}.Sanitize()); err != nil {
			return fmt.Errorf("init: CREATE DATABASE %s: %w", o.CPDB, err)
		}
		say("control plane    database %s (created, owner %s)", o.CPDB, o.CPRole)
	}
	// the new role connects (through the admin DSN's host: the one this machine reaches), and its schema is applied
	check := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s", dsnQuote(cfg.Host), cfg.Port, dsnQuote(o.CPDB), dsnQuote(o.CPRole), dsnQuote(password), ssl)
	store, err := tunnel.OpenPGStore(ctx, check)
	if err != nil {
		return fmt.Errorf("init: the control plane role cannot use its database: %w", scrub(err, password))
	}
	store.Close()
	say("control plane    schema lokadb_cp applied")
	if !haveFile {
		dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=%s\n", dsnQuote(host), port, dsnQuote(o.CPDB), dsnQuote(o.CPRole), dsnQuote(password), ssl)
		if err := writeNew(dsnPath, []byte(dsn), 0o600); err != nil {
			return err
		}
		say("control plane    %s (created, 0600; host %s as the containers reach it, sslmode=%s)", dsnPath, host, ssl)
	}
	return nil
}

// sslModeOf: sslmode of a DSN (keyword/value or URL), "require" when it names none.
func sslModeOf(dsn string) string {
	if strings.Contains(dsn, "://") {
		if u, err := url.Parse(dsn); err == nil {
			if v := u.Query().Get("sslmode"); v != "" {
				return v
			}
		}
		return "require"
	}
	for _, f := range strings.Fields(dsn) {
		if v, ok := strings.CutPrefix(f, "sslmode="); ok {
			return strings.Trim(v, "'")
		}
	}
	return "require"
}

func quoteLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// dsnQuote: a value of a libpq key=value DSN.
func dsnQuote(v string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(v) + "'"
}

// passwordIn: the password of a key=value DSN file.
func passwordIn(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	cfg, err := pgconn.ParseConfig(strings.TrimSpace(string(b)))
	if err != nil {
		return "", fmt.Errorf("init: %s: %w", path, err)
	}
	return cfg.Password, nil
}

// scrub: err without the password in its text.
func scrub(err error, password string) error {
	if password == "" || err == nil {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), password, "***"))
}
