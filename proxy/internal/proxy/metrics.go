package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/buildinfo"
)

// Metrics of the proxy for the cloud service's statistics (tasks/monitoring-plan.md; the
// service's DESIGN v3 §9): counters per organization, no statement text, no value, no table
// name. Config.MetricsListen serves them in the Prometheus text format on /metrics; the counters
// are kept either way (an atomic add on the path of a statement).
//
//	lokadb_proxy_statements_total{org,path}   statements on LokaDB tables; path "direct" (the
//	                                          direct path) or "fdw" (the cloud connection)
//	lokadb_proxy_errors_total{org}            responses that held an error
//	lokadb_proxy_response_seconds{org}        histogram: from the first message of a request to the
//	                                          client's ReadyForQuery
//
// A response is counted for the organization of the last statement on LokaDB tables in it.
// Past maxMetricOrgs organizations the rest is counted as org="other".

const maxMetricOrgs = 1000

var latencyBounds = [...]float64{0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5}

type orgStat struct {
	fdw, direct, errors, sumNs atomic.Int64
	buckets                    [len(latencyBounds) + 1]atomic.Int64 // the last: above the largest bound
}

type metrics struct {
	orgs  sync.Map // label -> *orgStat
	n     atomic.Int64
	other orgStat
	start time.Time
}

func newMetrics() *metrics { return &metrics{start: time.Now()} }

// label: the org label of a routing value (a number, or a name for what has none).
func orgLabel(org string) string {
	switch org {
	case "":
		return "all"
	case "-1", noOrg:
		return "none"
	}
	for _, c := range org {
		if c < '0' || c > '9' {
			return "other"
		}
	}
	return org
}

func (m *metrics) stat(org string) *orgStat {
	l := orgLabel(org)
	if v, ok := m.orgs.Load(l); ok {
		return v.(*orgStat)
	}
	if m.n.Load() >= maxMetricOrgs {
		return &m.other
	}
	v, loaded := m.orgs.LoadOrStore(l, &orgStat{})
	if !loaded {
		m.n.Add(1)
	}
	return v.(*orgStat)
}

// observe counts one response of st: its time and whether it held an error.
func (st *orgStat) observe(d time.Duration, failed bool) {
	st.sumNs.Add(int64(d))
	sec, i := d.Seconds(), 0
	for i < len(latencyBounds) && sec > latencyBounds[i] {
		i++
	}
	st.buckets[i].Add(1)
	if failed {
		st.errors.Add(1)
	}
}

func (m *metrics) write(w io.Writer, sessions int) {
	fmt.Fprintf(w, "# TYPE lokadb_proxy_info gauge\nlokadb_proxy_info{version=%q} 1\n", buildinfo.Version())
	fmt.Fprintf(w, "# TYPE lokadb_proxy_uptime_seconds gauge\nlokadb_proxy_uptime_seconds %d\n", int64(time.Since(m.start).Seconds()))
	fmt.Fprintf(w, "# TYPE lokadb_proxy_sessions gauge\nlokadb_proxy_sessions %d\n", sessions)
	labels := []string{}
	m.orgs.Range(func(k, _ any) bool { labels = append(labels, k.(string)); return true })
	sort.Slice(labels, func(i, j int) bool { // numbers in order, then names
		a, ea := strconv.Atoi(labels[i])
		b, eb := strconv.Atoi(labels[j])
		if ea == nil && eb == nil {
			return a < b
		}
		return ea == nil || (eb != nil && labels[i] < labels[j])
	})
	stats := make([]*orgStat, 0, len(labels)+1)
	for _, l := range labels {
		v, _ := m.orgs.Load(l)
		stats = append(stats, v.(*orgStat))
	}
	if m.other.sumNs.Load() != 0 || m.other.fdw.Load() != 0 || m.other.direct.Load() != 0 {
		labels, stats = append(labels, "other"), append(stats, &m.other)
	}
	fmt.Fprint(w, "# TYPE lokadb_proxy_statements_total counter\n")
	for i, l := range labels {
		fmt.Fprintf(w, "lokadb_proxy_statements_total{org=%q,path=\"fdw\"} %d\nlokadb_proxy_statements_total{org=%q,path=\"direct\"} %d\n", l, stats[i].fdw.Load(), l, stats[i].direct.Load())
	}
	fmt.Fprint(w, "# TYPE lokadb_proxy_errors_total counter\n")
	for i, l := range labels {
		fmt.Fprintf(w, "lokadb_proxy_errors_total{org=%q} %d\n", l, stats[i].errors.Load())
	}
	fmt.Fprint(w, "# TYPE lokadb_proxy_response_seconds histogram\n")
	for i, l := range labels {
		var cum int64
		for j, b := range latencyBounds {
			cum += stats[i].buckets[j].Load()
			fmt.Fprintf(w, "lokadb_proxy_response_seconds_bucket{org=%q,le=\"%g\"} %d\n", l, b, cum)
		}
		cum += stats[i].buckets[len(latencyBounds)].Load()
		fmt.Fprintf(w, "lokadb_proxy_response_seconds_bucket{org=%q,le=\"+Inf\"} %d\nlokadb_proxy_response_seconds_sum{org=%q} %g\nlokadb_proxy_response_seconds_count{org=%q} %d\n",
			l, cum, l, float64(stats[i].sumNs.Load())/1e9, l, cum)
	}
}

func (s *Server) countSessions() int {
	n := 0
	s.sessions.Range(func(_, _ any) bool { n++; return true })
	return n
}

// quantile: the upper bound of the bucket that holds the q-th part of the responses (seconds; the
// largest bound for the open one).
func (st *orgStat) quantile(q float64) float64 {
	var counts [len(latencyBounds) + 1]int64
	var total int64
	for i := range counts {
		counts[i] = st.buckets[i].Load()
		total += counts[i]
	}
	if total == 0 {
		return 0
	}
	want, cum := int64(float64(total)*q+0.999999), int64(0)
	for i, c := range counts {
		if cum += c; cum >= want {
			if i < len(latencyBounds) {
				return latencyBounds[i]
			}
			break
		}
	}
	return latencyBounds[len(latencyBounds)-1]
}

// ProxyStatus is the JSON of GET /status on metrics_listen.
type ProxyStatus struct {
	Version  string      `json:"version"`
	UptimeS  int64       `json:"uptime_s"`
	Sessions int         `json:"sessions"`
	Orgs     []OrgStatus `json:"orgs"`
}

type OrgStatus struct {
	Org       string  `json:"org"`
	Direct    int64   `json:"direct"`
	FDW       int64   `json:"fdw"`
	Errors    int64   `json:"errors"`
	Responses int64   `json:"responses"`
	P50ms     float64 `json:"p50_ms"` // upper bound of the histogram bucket
	P95ms     float64 `json:"p95_ms"`
}

func (m *metrics) status(sessions int) ProxyStatus {
	st := ProxyStatus{Version: buildinfo.Version(), UptimeS: int64(time.Since(m.start).Seconds()), Sessions: sessions, Orgs: []OrgStatus{}}
	add := func(l string, o *orgStat) {
		var n int64
		for i := range o.buckets {
			n += o.buckets[i].Load()
		}
		st.Orgs = append(st.Orgs, OrgStatus{Org: l, Direct: o.direct.Load(), FDW: o.fdw.Load(), Errors: o.errors.Load(), Responses: n,
			P50ms: o.quantile(0.5) * 1000, P95ms: o.quantile(0.95) * 1000})
	}
	m.orgs.Range(func(k, v any) bool { add(k.(string), v.(*orgStat)); return true })
	if m.other.fdw.Load()+m.other.direct.Load() > 0 {
		add("other", &m.other)
	}
	sort.Slice(st.Orgs, func(i, j int) bool { return st.Orgs[i].Org < st.Orgs[j].Org })
	return st
}

// serveMetrics serves /metrics and /healthz on Config.MetricsListen (when set).
func (s *Server) serveMetrics() {
	if s.cfg.MetricsListen == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.metrics.write(w, s.countSessions())
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.metrics.status(s.countSessions()))
	})
	// the statement shapes that name no customer, for a runner outside this process (its step 5 lists and decides
	// them): literals replaced by $n, like the log's. Not part of /status, which the runner forwards to the service.
	mux.HandleFunc("GET /unrouted", func(w http.ResponseWriter, _ *http.Request) {
		items, over := s.Unrouted()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "overflow": over})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	l, err := net.Listen("tcp", s.cfg.MetricsListen)
	if err != nil {
		s.log.Error("metrics endpoint", "listen", s.cfg.MetricsListen, "err", err)
		return
	}
	s.log.Info("metrics endpoint", "listen", s.cfg.MetricsListen)
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(l) }()
}

// countStatement: a statement on LokaDB tables of org goes by the direct path or the cloud
// connection; the response it ends in is timed for org (endRound).
func (s *session) countStatement(org string, direct bool) {
	if s.srv.metrics == nil { // a Server built without NewServer (tests)
		return
	}
	st := s.srv.metrics.stat(org)
	if direct {
		st.direct.Add(1)
	} else {
		st.fdw.Add(1)
	}
	s.mStat.Store(st)
}

// endRound: the client's ReadyForQuery goes out; the response that ends is counted.
func (s *session) endRound() {
	st, start, failed := s.mStat.Swap(nil), s.mStart.Swap(0), s.mErr.Swap(false)
	if st == nil || start == 0 {
		return
	}
	st.observe(time.Duration(time.Now().UnixNano()-start), failed)
}
