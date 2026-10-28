package runner

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

// statusLoop sends a status snapshot every interval (DESIGN §9.1); a changed inventory (an
// organization moved by hand, a new extension) is sent along.
func (r *Runner) statusLoop(ctx context.Context) {
	for {
		r.mu.Lock()
		d := r.interval
		r.mu.Unlock()
		sleep(ctx, d)
		if ctx.Err() != nil {
			return
		}
		st := r.snapshot(ctx)
		if _, err := r.call(ctx, "", "/v1/runner/status", st, nil); err != nil && ctx.Err() == nil {
			r.Log.Warn("sending the status", "err", err)
		}
	}
}

func (r *Runner) snapshot(ctx context.Context) cloudproto.Status {
	r.mu.Lock()
	st := cloudproto.Status{At: time.Now().UTC(), Runner: cloudproto.RunnerState{Version: r.Version, UptimeS: int64(time.Since(r.started).Seconds()),
		JobsRunning: len(r.running), LastJobError: r.lastErr}, Relays: []cloudproto.Component{}, Proxies: []cloudproto.Component{}, Orgs: []cloudproto.OrgStatus{}}
	r.mu.Unlock()
	st.Installation.DBOK = ping(ctx, r.Cfg.DB)
	st.Installation.ControlPlaneOK = ping(ctx, r.Cfg.ControlPlane)
	inv, err := r.ops.Inventory(ctx)
	if err != nil {
		st.Installation.Error = Redact(err.Error())
	} else {
		r.mu.Lock()
		changed := inv.Hash != r.invHash
		r.mu.Unlock()
		if changed {
			r.sendInventory(ctx, &inv)
		}
		st.Installation.OrgsCustomer = len(inv.Orgs)
		b, _ := json.Marshal(inv.Extensions)
		st.Installation.ExtensionsHash = hashOf(b)
	}
	for _, u := range r.Cfg.Relay.Status {
		st.Relays = append(st.Relays, fetchStatus(ctx, u))
	}
	for _, u := range r.Cfg.ProxyStatus {
		st.Proxies = append(st.Proxies, fetchStatus(ctx, u))
	}
	if err == nil {
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		for _, o := range inv.Orgs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				p := r.ops.Probe(ctx, o.Org)
				mu.Lock()
				st.Orgs = append(st.Orgs, cloudproto.OrgStatus{Org: o.Org, Location: o.Location, Profile: o.Profile, Probe: p})
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	return st
}

func ping(ctx context.Context, dsn string) bool {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return false
	}
	defer c.Close(context.Background())
	return c.Ping(ctx) == nil
}

// fetchStatus: a relay's or a proxy's status document as it serves it (JSON), or why not.
func fetchStatus(ctx context.Context, url string) cloudproto.Component {
	c := cloudproto.Component{Addr: url}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		c.Error = err.Error()
		return c
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.Error = err.Error()
		return c
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	switch {
	case err != nil:
		c.Error = err.Error()
	case resp.StatusCode != http.StatusOK:
		c.Error = resp.Status
	case !json.Valid(b):
		c.Error = "not JSON"
	default:
		c.Status = b
	}
	return c
}
