package tunnel

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The agent's state for the cloud service's statistics (tasks/monitoring-plan.md; the
// service's DESIGN v3 §9): GET /status on StatusListen (local), and, when StatusURL is set,
// the same JSON posted to the service every 60 s with the agent's secret (las_). Counters
// and states only: nothing of what the database holds or the clients send.
// LOKADB_AGENT_STATUS=off stops the posting; the tunnel is not affected.

// AgentStatus is the JSON of GET /status and of the post to the service.
type AgentStatus struct {
	Version   string        `json:"version"`
	Org       string        `json:"org,omitempty"`
	At        time.Time     `json:"at"`
	UptimeS   int64         `json:"uptime_s"`
	State     string        `json:"state"` // connected | reconnecting | revoked | cert_expired
	Serial    string        `json:"serial,omitempty"`
	CertEnds  time.Time     `json:"cert_ends,omitempty"`
	RenewErr  string        `json:"renew_error,omitempty"`
	Relays    []RelayStatus `json:"relays"`
	DB        DBStatus      `json:"db"`
	Reconnect int64         `json:"reconnects"`
	// refusals since the start
	RefusedCeiling int64 `json:"refused_max_conns"`
	RefusedAllow   int64 `json:"refused_cloud_allow"`
	Streams        int   `json:"streams"`
}

type RelayStatus struct {
	Addr      string    `json:"addr"`
	Sessions  int       `json:"sessions"`
	LastOK    time.Time `json:"last_ok,omitempty"`
	LastError string    `json:"last_error,omitempty"`
}

// DBStatus: the TCP time to the organization's database (no login), every 10 s.
type DBStatus struct {
	OK    bool      `json:"ok"`
	RTTms float64   `json:"rtt_ms"`
	At    time.Time `json:"at,omitempty"`
	Error string    `json:"error,omitempty"`
}

// agentStats: what the agent counts for /status.
type agentStats struct {
	started        time.Time
	reconnects     atomic.Int64
	refusedCeiling atomic.Int64
	refusedAllow   atomic.Int64

	mu       sync.Mutex
	lastOK   map[string]time.Time // relay -> its last session up
	lastErr  map[string]string    // relay -> why its last session ended (cleared when one is up)
	renewErr string
	revoked  bool
	db       DBStatus
}

func newAgentStats() *agentStats {
	return &agentStats{started: time.Now(), lastOK: map[string]time.Time{}, lastErr: map[string]string{}}
}

// up: a session to relay is up.
func (s *agentStats) up(relay string) {
	s.mu.Lock()
	s.lastOK[relay] = time.Now()
	delete(s.lastErr, relay)
	s.revoked = false
	s.mu.Unlock()
}

// down: a session to relay ended with err.
func (s *agentStats) down(relay string, err error) {
	s.reconnects.Add(1)
	s.mu.Lock()
	s.lastErr[relay] = trim(err.Error())
	if strings.Contains(err.Error(), "refused the session") || refusedByRelay(err) {
		s.revoked = true
	}
	s.mu.Unlock()
}

func (s *agentStats) renewed(err error) {
	s.mu.Lock()
	s.renewErr = ""
	if err != nil {
		s.renewErr = trim(err.Error())
	}
	s.mu.Unlock()
}

func trim(s string) string {
	if len(s) > 300 {
		return s[:300]
	}
	return s
}

// Status: the agent's state now.
func (a *Agent) Status() AgentStatus {
	st := AgentStatus{Version: a.cfg.Version, At: time.Now().UTC(), UptimeS: int64(time.Since(a.stats.started).Seconds()),
		Serial: a.creds.serial(), Reconnect: a.stats.reconnects.Load(), RefusedCeiling: a.stats.refusedCeiling.Load(),
		RefusedAllow: a.stats.refusedAllow.Load(), Relays: []RelayStatus{}}
	if leaf := a.creds.leaf(); leaf != nil {
		st.CertEnds = leaf.NotAfter.UTC()
		st.Org = strings.TrimPrefix(leaf.Subject.CommonName, "org-")
	}
	per, streams := map[string]int{}, 0
	a.mu.Lock()
	for key, s := range a.sess {
		if s.IsClosed() {
			continue
		}
		relay, _, _ := strings.Cut(key, "#")
		per[relay]++
		streams += s.NumStreams()
	}
	a.mu.Unlock()
	st.Streams = streams
	a.stats.mu.Lock()
	for relay := range a.stats.lastOK {
		if _, ok := per[relay]; !ok {
			per[relay] = 0
		}
	}
	for relay := range a.stats.lastErr {
		if _, ok := per[relay]; !ok {
			per[relay] = 0
		}
	}
	for relay, n := range per {
		st.Relays = append(st.Relays, RelayStatus{Addr: relay, Sessions: n, LastOK: a.stats.lastOK[relay].UTC(), LastError: a.stats.lastErr[relay]})
	}
	st.RenewErr, st.DB = a.stats.renewErr, a.stats.db
	revoked := a.stats.revoked
	a.stats.mu.Unlock()
	sort.Slice(st.Relays, func(i, j int) bool { return st.Relays[i].Addr < st.Relays[j].Addr })
	sessions := 0
	for _, n := range per {
		sessions += n
	}
	switch {
	case !st.CertEnds.IsZero() && time.Now().After(st.CertEnds):
		st.State = "cert_expired"
	case sessions > 0:
		st.State = "connected"
	case revoked:
		st.State = "revoked"
	default:
		st.State = "reconnecting"
	}
	return st
}

// probeDB measures the TCP time to the organization's database every 10 s.
func (a *Agent) probeDB() {
	for {
		start := time.Now()
		c, err := net.DialTimeout("tcp", a.cfg.DB, 3*time.Second)
		d := DBStatus{OK: err == nil, RTTms: float64(time.Since(start)) / 1e6, At: time.Now().UTC()}
		if err != nil {
			d.Error = "unreachable" // the address stays in the agent's log
		} else {
			c.Close()
		}
		a.stats.mu.Lock()
		a.stats.db = d
		a.stats.mu.Unlock()
		time.Sleep(10 * time.Second)
	}
}

// serveStatus serves GET /status on StatusListen.
func (a *Agent) serveStatus(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(a.Status())
	})
	l, err := net.Listen("tcp", addr)
	if err != nil {
		a.log.Error("status endpoint", "listen", addr, "err", err)
		return
	}
	a.log.Info("status endpoint", "listen", addr)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(l) }()
}

// reportStatus posts the status to the service every interval (60 s; the service may ask for
// another in its answer) until the service refuses the secret.
func (a *Agent) reportStatus() {
	if strings.EqualFold(os.Getenv("LOKADB_AGENT_STATUS"), "off") {
		a.log.Info("status reports to the service are off (LOKADB_AGENT_STATUS=off)")
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	interval := a.reportEvery
	for {
		time.Sleep(interval)
		secret, err := os.ReadFile(a.cfg.StatusTokenFile)
		if err != nil {
			a.log.Warn("status report: the secret cannot be read", "file", a.cfg.StatusTokenFile, "err", err)
			continue
		}
		st := a.Status()
		body, _ := json.Marshal(st)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "POST", a.cfg.StatusURL, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(secret)))
		resp, err := client.Do(req)
		cancel()
		if err != nil {
			a.log.Warn("status report failed", "err", err)
			continue
		}
		var out struct {
			Notices []struct {
				Kind    string `json:"kind"`
				Command string `json:"command"`
			} `json:"notices"`
			IntervalS int `json:"interval_s"`
		}
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusUnauthorized:
			a.log.Error("status report: the service refused the agent's secret: reports stop (the tunnel is not affected)")
			return
		case resp.StatusCode/100 != 2:
			a.log.Warn("status report refused", "status", resp.StatusCode)
			continue
		}
		if json.Unmarshal(data, &out) == nil {
			for _, n := range out.Notices {
				a.log.Warn("notice from the LokaDB service", "kind", n.Kind, "command", n.Command)
			}
			if out.IntervalS >= 30 && out.IntervalS <= 600 {
				interval = time.Duration(out.IntervalS) * time.Second
			}
		}
	}
}

func (s AgentStatus) String() string { return fmt.Sprintf("%s %s", s.State, s.Version) }
