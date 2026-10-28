package tunnel

import (
	"encoding/json"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/buildinfo"
)

// Health and draining, for a relay behind a TCP load balancer (several relays, see
// tasks/relay-ha.md). GET /healthz on health_listen answers 200 while the relay takes
// connections and 503 once it drains. Drain (SIGTERM) turns the health check red, waits
// drain_delay for the balancer to notice, stops taking connections (organization ports,
// agents), tells the agents to open no more streams to it (yamux GoAway), waits for the
// open streams to end for up to drain_timeout, then closes the rest.

// SessionInfo: one agent session of the relay (GET /sessions on health_listen).
type SessionInfo struct {
	Org      string    `json:"org"`
	Agent    string    `json:"agent"`
	Serial   string    `json:"serial"`
	Remote   string    `json:"remote"`
	Since    time.Time `json:"since"`
	Ends     time.Time `json:"cert_ends"`
	Streams  int       `json:"streams"`
	Version  string    `json:"version,omitempty"`  // the agent's build, from its ALPN name
	Retiring bool      `json:"retiring,omitempty"` // replaced by the agent's newer session, finishing its streams
}

// Sessions lists the relay's agent sessions, by organization and age.
func (r *Relay) Sessions() []SessionInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []SessionInfo
	add := func(as *agentSession, retiring bool) {
		out = append(out, SessionInfo{Org: as.org, Agent: as.id, Serial: as.serial, Remote: as.remote, Since: as.since,
			Ends: as.ends, Streams: as.sess.NumStreams(), Version: as.version, Retiring: retiring})
	}
	for _, agents := range r.sessions {
		for _, as := range agents {
			add(as, false)
		}
	}
	for as := range r.retiring {
		add(as, true)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Org != out[j].Org {
			return out[i].Org < out[j].Org
		}
		return out[i].Since.Before(out[j].Since)
	})
	return out
}

// RelayState is the JSON of GET /status on health_listen.
type RelayState struct {
	Version  string        `json:"version"`
	UptimeS  int64         `json:"uptime_s"`
	Draining bool          `json:"draining"`
	Orgs     []string      `json:"orgs"` // the organizations it serves
	Sessions []SessionInfo `json:"sessions"`
}

// Status: the relay's state now.
func (r *Relay) Status() RelayState {
	st := RelayState{Version: buildinfo.Version(), UptimeS: int64(time.Since(r.started).Seconds()), Draining: r.draining.Load(),
		Orgs: []string{}, Sessions: r.Sessions()}
	r.mu.Lock()
	for org := range r.served {
		st.Orgs = append(st.Orgs, org)
	}
	r.mu.Unlock()
	sort.Strings(st.Orgs)
	return st
}

func (r *Relay) serveHealth() {
	if r.cfg.HealthListen == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if r.draining.Load() {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	// the agents connected now, and the sessions finishing after a renewal (review 3, R3-19)
	mux.HandleFunc("GET /sessions", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Sessions())
	})
	// the relay's state for the cloud service's statistics (DESIGN v3 §9): counters and states only
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(r.Status())
	})
	l, err := net.Listen("tcp", r.cfg.HealthListen)
	if err != nil {
		r.log.Error("health endpoint", "listen", r.cfg.HealthListen, "err", err)
		return
	}
	r.log.Info("health endpoint", "listen", r.cfg.HealthListen)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(l) }()
}

func durationOr(s string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(s); err == nil && d >= 0 {
		return d
	}
	return def
}

// drainSignal: the first 8 bytes of a stream the relay opens to an agent to say it drains: a
// length 8 and a code no Postgres client sends (the startup codes are 80877102-80877104 and
// the protocol versions). The agent re-dials at once instead of waiting for the session to
// close; an agent that does not know it forwards 8 bytes its database refuses.
var drainSignal = []byte{0, 0, 0, 8, 0xb7, 0x0d, 0xd8, 0x41}

func signalDrain(sess interface{ Open() (net.Conn, error) }) {
	st, err := sess.Open()
	if err != nil {
		return
	}
	_ = st.SetDeadline(time.Now().Add(time.Second))
	_, _ = st.Write(drainSignal)
	st.Close()
}

// Drain stops the relay gracefully and returns when it is done (see above).
func (r *Relay) Drain() {
	if r.draining.Swap(true) {
		return
	}
	// defaults 0 and 5 s: a lone relay has nobody to hand its traffic to, and its pools'
	// connections (the proxy's, postgres_fdw's) never end by themselves, so a long drain was a
	// minute of outage for every organization (block 14's agents scenario: 59-77 s)
	delay, timeout := durationOr(r.cfg.DrainDelay, 0), durationOr(r.cfg.DrainTimeout, 5*time.Second)
	r.log.Warn("draining: health check now fails", "delay", delay, "timeout", timeout)
	time.Sleep(delay)
	r.mu.Lock() // under mu: syncEndpoints and agent check draining under it too (review 3, R3-16)
	for org, ep := range r.endpoints {
		ep.ln.Close()
		delete(r.endpoints, org)
	}
	if r.agentLn != nil {
		r.agentLn.Close()
	}
	var all []*agentSession
	for _, agents := range r.sessions {
		for _, as := range agents {
			all = append(all, as)
		}
	}
	r.mu.Unlock()
	for _, as := range all {
		if !r.cfg.noDrainSignal {
			signalDrain(as.sess) // the agent dials again now (another relay behind the same name takes it)
		}
		_ = as.sess.GoAway()
	}
	deadline := time.Now().Add(timeout)
	for {
		open := 0
		for _, as := range all {
			if !as.sess.IsClosed() {
				open += as.sess.NumStreams()
			}
		}
		if open == 0 {
			break
		}
		if time.Now().After(deadline) {
			r.log.Warn("draining: timeout, closing the streams left", "streams", open)
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, as := range all {
		as.sess.Close()
	}
	r.log.Warn("drained")
}
