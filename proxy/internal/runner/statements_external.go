package runner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Statements without a customer for a runner whose proxy runs outside it (runner.json with proxy_status):
// the proxy serves them at /unrouted next to its /status, the runner looks every few seconds, and what it saves
// of the person's decisions goes into the proxy's config (proxy_config), the file that proxy reads.

// appDB is the application's database as the proxy sees it: found in the environment (an embedded runner),
// else the database of the runner's own DSN.
func (c *Config) appDB() string {
	if c.found != nil && c.found.DBName != "" {
		return c.found.DBName
	}
	dsn, err := Secret(c.DB)
	if err != nil || dsn == "" {
		return ""
	}
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return ""
	}
	return cc.Database
}

// proxyConfigPath is the config of the proxy whose decisions these are: proxy_config, else the runner's own.
func (c *Config) proxyConfigPath() string {
	if c.ProxyConfig != "" {
		return c.ProxyConfig
	}
	return filepath.Join(Dir, "proxy.json")
}

// unroutedURL: where a proxy serves its statements, from the URL of its /status.
func unroutedURL(status string) string {
	return strings.TrimSuffix(strings.TrimRight(status, "/"), "/status") + "/unrouted"
}

// externalProxy: this runner does not run its proxy; it can list the statements only when it knows where to ask.
func (c *Config) externalProxy() bool { return !c.Embedded }

// watchExternal asks every proxy named in proxy_status for the statements that name no customer.
func (c *Config) watchExternal(ctx context.Context) {
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		c.lookExternal(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (c *Config) lookExternal(ctx context.Context) {
	db := c.appDB()
	byFP := map[string]proxy.Unrouted{} // the same shape at two proxies is one: their counts add up
	var over int64
	var problem string
	for _, u := range c.ProxyStatus {
		items, o, err := fetchUnrouted(ctx, unroutedURL(u))
		if err != nil {
			problem = Redact(err.Error())
			continue
		}
		over += o
		for _, x := range items {
			if x.DB != "" && x.DB != db {
				continue
			}
			if have, ok := byFP[x.Fingerprint]; ok {
				have.Count += x.Count
				have.Refused = have.Refused || x.Refused
				byFP[x.Fingerprint] = have
				continue
			}
			byFP[x.Fingerprint] = x
		}
	}
	if problem != "" && len(byFP) == 0 {
		c.setInfo("unrouted_visible", "no: "+problem)
		return
	}
	c.setInfo("unrouted_visible", "yes")
	list := make([]proxy.Unrouted, 0, len(byFP))
	for _, x := range byFP {
		list = append(list, x)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Fingerprint < list[j].Fingerprint })
	c.collectUnrouted(list, over)
}

func fetchUnrouted(ctx context.Context, url string) ([]proxy.Unrouted, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, &statusError{url: url, status: resp.Status}
	}
	var out struct {
		Items    []proxy.Unrouted `json:"items"`
		Overflow int64            `json:"overflow"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(&out); err != nil {
		return nil, 0, err
	}
	return out.Items, out.Overflow, nil
}

type statusError struct{ url, status string }

func (e *statusError) Error() string {
	return e.url + ": " + e.status + " (this proxy is older than the runner: update it, then the runner can list the queries that name no customer)"
}
