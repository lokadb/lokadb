// Package runner: lokadb runner, the SaaS's side of LokaDB Cloud. It dials the service, takes
// jobs (check, enable, move-org, move-back, the agent's enrollment and credentials) and runs
// them here with the open module, against the SaaS database and its control plane, whose
// passwords never leave this process. See private/cloud/DESIGN.md §4 (the protocol is
// package cloudproto).
package runner

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
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

// Config: runner.json. Every secret may be "env:NAME" or "file:PATH".
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
	Spec  string `json:"spec"`  // the spec enable was given
	Owner string `json:"owner"` // the role owning the application's tables (move-org -owner)
	// ProxyConfig: the proxy's config: check reads its fanout policy; control_plane and
	// credentials_key default from it.
	ProxyConfig       string `json:"proxy_config"`
	ControlPlane      string `json:"control_plane"`
	CredentialsKey    string `json:"credentials_key"`
	CredentialsKeyOld string `json:"credentials_key_old"`

	Relay Relay `json:"relay"`
	// ProxyStatus: the proxies' status URLs (block 8), forwarded in the status snapshots.
	ProxyStatus []string `json:"proxy_status"`
	// SendStatus: false sends no status snapshots (jobs still work; the panel says monitoring
	// is off). Default true.
	SendStatus *bool `json:"status"`

	LockTimeout Duration `json:"lock_timeout"` // move-org's (default 5s)
	MaxPause    Duration `json:"max_pause"`    // move-org's (default 10s)

	// loaded
	spec     admin.Spec
	proxyCfg *proxy.Config
}

type Relay struct {
	// Agent: the relay tunnel address(es) agents dial from outside, host:port[,host:port].
	Agent      string `json:"agent"`
	Enroll     string `json:"enroll"`      // default: the first agent host, port 7444
	ServerName string `json:"server_name"` // name in the relay certificates
	CA         string `json:"ca"`          // the CA certificate file (public), handed to installers
	// CloudHost: the relay as this network reaches an organization's port (the proxy, move-org).
	CloudHost string `json:"cloud_host"`
	PortMin   int    `json:"port_min"` // organizations' ports (default 15000-15999)
	PortMax   int    `json:"port_max"`
	TLS       string `json:"tls"`     // org-endpoint -tls: "require" or ""
	SSLMode   string `json:"sslmode"` // of connections to an organization's database (default prefer)
	// Status: the relays' health_listen URLs (/sessions today, /status with block 8).
	Status []string `json:"status"`
}

// LoadConfig reads runner.json, its secrets, the spec and the proxy config.
func LoadConfig(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, c.finish()
}

func (c *Config) finish() error {
	var err error
	if c.Token == "" {
		c.Token = os.Getenv("LOKADB_RUNNER_TOKEN")
	}
	for _, p := range []*string{&c.DB, &c.ControlPlane, &c.Token} {
		if *p, err = Secret(*p); err != nil {
			return err
		}
	}
	c.Service = strings.TrimRight(c.Service, "/")
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
	var missing []string
	for _, kv := range [][2]string{{"service", c.Service}, {"db", c.DB}, {"spec", c.Spec}, {"owner", c.Owner}, {"proxy_config", c.ProxyConfig},
		{"relay.agent", c.Relay.Agent}, {"relay.ca", c.Relay.CA}, {"relay.cloud_host", c.Relay.CloudHost}} {
		if kv[1] == "" {
			missing = append(missing, kv[0])
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("runner config: %s required", strings.Join(missing, ", "))
	}
	if c.Relay.PortMin <= 0 || c.Relay.PortMax < c.Relay.PortMin || c.Relay.PortMax > 65535 {
		return fmt.Errorf("runner config: relay.port_min/port_max: a range of ports")
	}
	if err := readJSON(c.Spec, &c.spec); err != nil {
		return fmt.Errorf("spec: %w", err)
	}
	c.spec = c.spec.Sorted()
	c.proxyCfg = &proxy.Config{}
	if err := readJSON(c.ProxyConfig, c.proxyCfg); err != nil {
		return fmt.Errorf("proxy_config: %w", err)
	}
	if c.ControlPlane == "" {
		c.ControlPlane = c.proxyCfg.ControlPlane
	}
	if c.CredentialsKey == "" {
		c.CredentialsKey, c.CredentialsKeyOld = c.proxyCfg.CredentialsKey, c.proxyCfg.CredentialsKeyOld
	}
	if c.ControlPlane == "" || c.CredentialsKey == "" {
		return fmt.Errorf("runner config: control_plane and credentials_key (or the proxy config's): the organizations' credentials live there")
	}
	return nil
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

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
