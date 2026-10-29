package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/initcmd"
)

// provisioned: the files of the LokaDB directory the runner makes when they are missing (the
// work of lokadb init); everything else in it the user writes (spec.json) or the relay makes
// (the CA).
var provisioned = []string{"credentials.key", "control-plane.dsn", "relay.json", "proxy.json"}

// provisioning: the runner makes the directory's files only when runner.json leaves them at
// their defaults (the proxy config is the directory's own).
func (c *Config) provisioning() bool {
	return c.ProxyConfig == filepath.Join(Dir, "proxy.json")
}

// missing: the provisioned files that are not there.
func missing() []string {
	var out []string
	for _, f := range provisioned {
		if _, err := os.Stat(filepath.Join(Dir, f)); err != nil {
			out = append(out, f)
		}
	}
	return out
}

// relayNames: the names the relay's certificate gets: the public hosts of relay.agent and the
// name the runner's network reaches it by.
func (c *Config) relayNames() string {
	var names []string
	seen := map[string]bool{}
	add := func(h string) {
		if h = strings.TrimSpace(h); h != "" && !seen[h] {
			seen[h] = true
			names = append(names, h)
		}
	}
	for _, a := range strings.Split(c.Relay.Agent, ",") {
		a = strings.TrimSpace(a)
		if i := strings.LastIndex(a, ":"); i > 0 && !strings.Contains(a[i:], "]") {
			a = a[:i]
		}
		add(a)
	}
	h := c.Relay.CloudHost
	if i := strings.LastIndex(h, ":"); i > 0 {
		h = h[:i]
	}
	add(h)
	return strings.Join(names, ",")
}

// Provision makes the files of the LokaDB directory that are missing, the work of lokadb init:
// the control plane's role and database in the SaaS database (a random password, never
// printed), credentials.key, relay.json and proxy.json (the relay makes the CA and its
// certificate itself). It never overwrites a file, and prints the docker run lines of the relay
// and the proxy to out. dry: only says what it would make (runner -check) and remembers it, so
// that Check does not call those files missing. It returns the names it made or would make.
func (c *Config) Provision(ctx context.Context, dry bool, out io.Writer) ([]string, error) {
	if !c.provisioning() {
		return nil, nil
	}
	todo := missing()
	if len(todo) == 0 {
		return nil, nil
	}
	if c.dbRaw == "" || c.Relay.Agent == "" {
		return nil, nil // Check says what is missing first
	}
	db, err := Secret(c.dbRaw)
	if err != nil {
		return nil, nil // Check reports it
	}
	if dry {
		c.willMake = map[string]bool{}
		for _, f := range todo {
			c.willMake[filepath.Join(Dir, f)] = true
		}
		fmt.Fprintf(out, "WOULD CREATE  in %s: %s\n", Dir, strings.Join(todo, ", "))
		fmt.Fprintf(out, "              (the runner makes them when it starts, the work of lokadb init: the role lokadb_cp and its database in your database with a random password that is never printed, a credentials key, relay.json and proxy.json; it never overwrites a file; the relay makes the CA and the relay certificate when it starts)\n")
		return todo, nil
	}
	if err := writable(Dir); err != nil {
		return nil, fmt.Errorf("%s is not writable (%v): mount the LokaDB directory without :ro to let the runner make %s, or run lokadb init -dir HOSTDIR yourself", Dir, err, strings.Join(todo, ", "))
	}
	host := Dir
	if h, mounted, ok := hostPath(Dir); ok && mounted {
		host = h
	} else if ok {
		host = "HOSTDIR"
	}
	var allow []string
	if c.Embedded {
		// the proxy and the runner are this process: loopback; the database's own container
		// (postgres_fdw) and the others of the compose project: the private ranges
		allow = []string{"127.0.0.0/8", "10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
	}
	err = initcmd.Run(ctx, initcmd.Options{
		Dir: Dir, ContainerDir: Dir, HostDir: host, RelayHost: c.relayNames(),
		AdminDSN: db, Network: networkFlag(c.Network), Image: c.Image, Out: out, OrgAllow: allow, NoNext: c.Embedded,
	})
	if err != nil {
		return nil, err
	}
	return todo, nil
}

// networkFlag: the value of --network for runner.json's network.
func networkFlag(n string) string { return strings.TrimSpace(n) }

// writable: a file can be made in dir.
func writable(dir string) error {
	f, err := os.CreateTemp(dir, ".lokadb-write-test-*")
	if err != nil {
		return err
	}
	name := f.Name()
	f.Close()
	return os.Remove(name)
}

// Prepare is what the runner repeats until it is ready: Provision (for real), then Check. A
// failure to provision is a problem of its own.
func (c *Config) Prepare(ctx context.Context) []cloudproto.Problem {
	var probs []cloudproto.Problem
	if _, err := c.Provision(ctx, false, os.Stdout); err != nil {
		p := cloudproto.Problem{Key: "provision", Message: "the LokaDB directory's files could not be made: " + Redact(err.Error()),
			Fix: "LOKADB_DB must be a role that may create roles and databases, and " + Dir + " must be mounted writable (docker run -v HOSTDIR:" + Dir + "); or make the files yourself: lokadb init -dir HOSTDIR -admin \"$LOKADB_DB\" -relay-host NAME"}
		if strings.Contains(err.Error(), "permission denied to create") {
			// the usual case with a managed service: the application's own role is all LokaDB found
			p.Message = "LokaDB needs an administrator role once, to make its own role and database, and the role your application connects as is not one."
			p.Fix = "Give LokaDB an administrator of your Postgres in the variable LOKADB_DB (host=… dbname=… user=… password=… sslmode=require): on a managed service the role the provider made for you (postgres on Amazon RDS, doadmin on DigitalOcean, cloudsqlsuperuser members on Google Cloud SQL…). The password stays on your server. The table on the setup page lists the role of each provider."
		}
		probs = append(probs, p)
	}
	if c.Embedded {
		if err := c.startEmbedded(ctx); err != nil {
			probs = append(probs, cloudproto.Problem{Key: "embedded", Message: "The relay and the proxy could not start in this container: " + Redact(err.Error()),
				Fix: "see the container's log (docker logs); the setup page shows the same"})
		}
	}
	return append(probs, c.Check(ctx)...)
}
