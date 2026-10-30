package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"os"
	"sort"
	"strings"
	"time"
)

// Organization endpoints change without restarting the relay: every poll (2 s) it
// rebuilds the wanted set from the "orgs" of its config file (re-read when the relay knows
// the file) and the registry table lokadb_cp.relay_orgs (lokadb org-endpoint), opens the
// ports of new organizations, moves changed ones and closes removed ones. Connections of the
// other organizations are not touched.

type orgEndpoint struct {
	addr string
	ln   net.Listener
}

// wantedEndpoints merges the config file and the registry. The config file wins on a
// conflict (it is what the operator of this relay wrote); a source that cannot be read
// keeps its last good list, so a registry outage closes no port.
func (r *Relay) wantedEndpoints() map[string]string {
	if r.cfg.ConfigPath != "" {
		var c struct {
			Orgs map[string]string `json:"orgs"`
		}
		b, err := os.ReadFile(r.cfg.ConfigPath)
		if err == nil {
			err = json.Unmarshal(b, &c)
		}
		if err != nil {
			r.warnOnce("config", "organization list: config file unreadable, keeping the last one", "path", r.cfg.ConfigPath, "err", err)
		} else {
			r.fileOrgs = c.Orgs
			r.warnOnce("config", "")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	reg, err := r.store.Endpoints(ctx)
	cancel()
	if err != nil {
		r.warnOnce("registry", "organization list: registry unavailable, keeping the last one", "err", err)
	} else {
		r.regOrgs = map[string]string{}
		tlsOrgs := map[string]bool{}
		for org, ep := range reg {
			r.regOrgs[org] = ep.Listen
			tlsOrgs[org] = ep.TLS == "require"
		}
		r.mu.Lock()
		r.tlsOrgs, r.tlsKnown = tlsOrgs, true
		r.mu.Unlock()
		r.warnOnce("registry", "")
		r.loadPins()
	}
	want := maps.Clone(r.regOrgs)
	if want == nil {
		want = map[string]string{}
	}
	for org, addr := range r.fileOrgs {
		if a, ok := want[org]; ok && a != addr {
			r.warnOnce("conflict:"+org, "organization endpoint: the config file and the registry disagree, using the config file",
				"org", org, "config", addr, "registry", a)
		} else {
			r.warnOnce("conflict:"+org, "")
		}
		want[org] = addr
	}
	return want
}

// syncEndpoints opens, moves and closes organization endpoints to match want. It returns
// the first listen error (the endpoint is retried on the next pass).
func (r *Relay) syncEndpoints(want map[string]string) error {
	if r.draining.Load() {
		return nil // a draining relay opens no port again
	}
	var first error
	r.mu.Lock()
	r.served = want
	var drop []*agentSession
	for org, ep := range r.endpoints {
		if addr, ok := want[org]; ok && addr == ep.addr {
			continue
		}
		ep.ln.Close()
		delete(r.endpoints, org)
		if _, ok := want[org]; !ok {
			r.log.Info("organization endpoint closed", "org", org, "listen", ep.addr)
			for _, as := range r.sessions[org] {
				drop = append(drop, as) // an organization no longer served keeps no tunnel
			}
		} else {
			r.log.Info("organization endpoint moved", "org", org, "from", ep.addr, "to", want[org])
		}
	}
	r.mu.Unlock()
	for _, as := range drop {
		as.sess.Close()
	}
	for org, addr := range want {
		r.mu.Lock()
		_, open := r.endpoints[org]
		r.mu.Unlock()
		if open {
			continue
		}
		l, err := net.Listen("tcp", addr)
		if err != nil {
			r.warnOnce("listen:"+org, "organization endpoint: cannot listen, retrying", "org", org, "listen", addr, "err", err)
			if first == nil {
				first = fmt.Errorf("organization %s: %w", org, err)
			}
			continue
		}
		r.warnOnce("listen:"+org, "")
		r.mu.Lock()
		if r.draining.Load() { // Drain closed the ports meanwhile: this one stays closed (review 3, R3-16)
			r.mu.Unlock()
			l.Close()
			return first
		}
		r.endpoints[org] = &orgEndpoint{addr: addr, ln: l}
		r.mu.Unlock()
		r.log.Info("organization endpoint", "org", org, "listen", addr)
		go r.serveOrg(org, l)
	}
	return first
}

func (r *Relay) watchEndpoints() {
	last, was := time.Time{}, ""
	for range time.Tick(r.cfg.poll) {
		_ = r.syncEndpoints(r.wantedEndpoints())
		if now := r.portList(); now != was || time.Since(last) > 5*time.Minute {
			last, was = time.Now(), now
			r.publishPorts()
		}
	}
}

func (r *Relay) portList() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ps []string
	for _, ep := range r.endpoints {
		ps = append(ps, ep.addr)
	}
	sort.Strings(ps)
	return strings.Join(ps, ",")
}

// publishPorts writes the organizations' ports this relay serves to the registry: LokaDB's
// components prove their address before they connect to one (self.go).
func (r *Relay) publishPorts() {
	ps := r.pins()
	if ps == nil {
		return
	}
	r.mu.Lock()
	var ports []string
	for _, ep := range r.endpoints {
		if _, p, err := net.SplitHostPort(ep.addr); err == nil {
			ports = append(ports, p)
		}
	}
	r.mu.Unlock()
	if len(ports) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	defer cancel()
	_ = ps.SetRelayPorts(ctx, ports)
}

// listening reports whether the organization's endpoint is open on addr.
func (r *Relay) listening(org, addr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ep := r.endpoints[org]
	return ep != nil && ep.addr == addr
}

// tlsRequired reports whether the organization's endpoint lets through TLS only: org_tls
// of the relay, or the organization's own setting in the registry.
func (r *Relay) tlsRequired(org string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.OrgTLS == "require" || r.tlsOrgs[org]
}

// tlsUnknown: the registry has not answered since the relay started, so whether an
// organization requires TLS is not known yet (org_tls "require" settles it for all): its
// connections are refused, not let through in clear (review 3 of the tunnel, R3-10).
func (r *Relay) tlsUnknown() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cfg.OrgTLS != "require" && !r.tlsKnown
}

// serves reports whether the relay serves the organization now.
func (r *Relay) serves(org string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.served[org]
	return ok
}

// warnOnce logs a condition when it starts (msg != "") and forgets it when it clears
// (msg == ""), so a poll every two seconds does not repeat the same warning.
func (r *Relay) warnOnce(key, msg string, args ...any) {
	r.wmu.Lock()
	defer r.wmu.Unlock()
	if msg == "" {
		delete(r.warned, key)
		return
	}
	if r.warned[key] {
		return
	}
	r.warned[key] = true
	r.log.Warn(msg, args...)
}
