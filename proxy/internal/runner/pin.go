package runner

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// pinLoop keeps the relays' list of the SaaS database's addresses true (tunnel/pin.go, R01): a
// probe from the database every 30 s, and at once when a relay holds a connection from an
// address it does not know (the database came back at a new address: a pod, a container); the
// relay lets that connection through when the probe proves it, so the database's queries go on.
// What it finds goes to the panel in the runner's info: the addresses, and the connections
// refused (a copy of the database tried to reach a customer).
func (r *Runner) pinLoop(ctx context.Context) {
	last := time.Time{}
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		due := time.Since(last) >= 30*time.Second
		if !due {
			held, err := r.ops.Tun.RefusedSources(ctx, 5*time.Second)
			for _, h := range held {
				due = due || h.Pending
			}
			if err != nil || !due {
				continue
			}
		}
		last = time.Now()
		r.pinOnce(ctx)
	}
}

func (r *Runner) pinOnce(ctx context.Context) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, err := ownsql.Connect(cctx, r.Cfg.DB)
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	res, err := admin.Pin(cctx, conn, r.ops.Tun)
	if err != nil {
		r.Cfg.setInfo("cloud_pin", "error: "+Redact(err.Error()))
		return
	}
	if len(res.Relays) == 0 {
		r.Cfg.setInfo("cloud_pin", "") // no organization moved: nothing to protect yet
		return
	}
	r.Cfg.setInfo("cloud_pin", pinWords(res))
	refused, _ := r.ops.Tun.RefusedSources(cctx, 7*24*time.Hour)
	var rs []string
	for _, x := range refused {
		if !x.Pending || time.Since(x.Last) > 10*time.Second {
			rs = append(rs, fmt.Sprintf("%s org %s ×%d last %s", x.Addr, x.Org, x.Count, x.Last.UTC().Format(time.RFC3339)))
		}
	}
	r.Cfg.setInfo("cloud_refused", strings.Join(rs, "; "))
	// a copy in another database of the same cluster has the cluster's address: the relays let it
	// through, so it is looked for (every 10 minutes) and said
	if time.Since(r.copiesAt) >= 10*time.Minute {
		r.copiesAt = time.Now()
		copies, err := admin.FindCopies(cctx, r.Cfg.DB, res.Relays)
		if err == nil {
			var cs []string
			for _, c := range copies {
				cs = append(cs, c.Database+" ("+strings.Join(c.Servers, ", ")+")")
			}
			r.Cfg.setInfo("cloud_copies", strings.Join(cs, "; "))
		}
	}
}

// pinWords: the runner's info "cloud_pin": "proven <address>" (only those pass), "seen <address>
// since …" (trusted, not proven: a warning), each relay a probe did not get through to.
func pinWords(res admin.PinResult) string {
	var proven, seen []string
	for _, s := range res.Sources {
		switch {
		case s.Valid:
			proven = append(proven, s.Addr)
		case s.How == "seen":
			seen = append(seen, s.Addr+" since "+s.First.UTC().Format(time.RFC3339))
		}
	}
	var out []string
	if len(proven) > 0 {
		out = append(out, "proven "+strings.Join(proven, ","))
	}
	if len(seen) > 0 {
		out = append(out, "seen "+strings.Join(seen, ","))
	}
	var failed []string
	for a, why := range res.Failed {
		failed = append(failed, a+": "+Redact(why))
	}
	sort.Strings(failed)
	if len(failed) > 0 {
		out = append(out, "probe failed "+strings.Join(failed, "; "))
	}
	return strings.Join(out, " | ")
}

// applyCloudSources: the addresses confirmed in the panel become the relays' (manual), once each.
func (r *Runner) applyCloudSources(addrs []string) {
	if r.ops == nil || r.ops.Tun == nil || len(addrs) == 0 {
		return
	}
	r.mu.Lock()
	if r.confirmed == nil {
		r.confirmed = map[string]bool{}
	}
	var todo []string
	for _, a := range addrs {
		if net.ParseIP(a) != nil && !r.confirmed[a] {
			todo = append(todo, a)
		}
	}
	r.mu.Unlock()
	for _, a := range todo {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := r.ops.Tun.ProveSource(ctx, a, "manual")
		cancel()
		if err == nil {
			r.mu.Lock()
			r.confirmed[a] = true
			r.mu.Unlock()
			r.Log.Info("an address confirmed in the panel as the SaaS database's", "addr", a)
		}
	}
}

// rotateLoop gives the moved organizations' roles new passwords every hour (admin.RotateDue, R01:
// the password in a dump of the SaaS database opens nothing an hour later), not while a job of
// this runner runs (a move holds the old one). An organization whose agent is not connected keeps
// its password until the next try; the runner's info cred_rotation says each one.
func (r *Runner) rotateLoop(ctx context.Context) {
	tick := time.NewTicker(5 * time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		r.mu.Lock()
		busy := len(r.running) > 0
		r.mu.Unlock()
		if busy || r.Cfg.CredentialsKey == "" || r.Cfg.ControlPlane == "" {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		reps, err := admin.RotateDue(cctx, admin.RotateOptions{CloudDSN: r.Cfg.DB, ControlPlane: r.Cfg.ControlPlane, KeyFile: r.Cfg.CredentialsKey,
			KeyFileOld: r.Cfg.CredentialsKeyOld, Log: r.Log}, time.Hour)
		var said []string
		for _, rep := range reps {
			if rep.Err != nil {
				said = append(said, "org "+rep.Org+" kept its password: "+Redact(rep.Err.Error()))
			}
		}
		at, _ := admin.Rotated(cctx, r.Cfg.ControlPlane)
		cancel()
		if err != nil {
			said = append(said, "error: "+Redact(err.Error()))
		}
		var last []string
		for org, t := range at {
			last = append(last, "org "+org+" "+t.UTC().Format(time.RFC3339))
		}
		sort.Strings(last)
		if len(last) > 0 || len(said) > 0 {
			r.Cfg.setInfo("cred_rotation", strings.Join(append(last, said...), "; "))
		}
	}
}
