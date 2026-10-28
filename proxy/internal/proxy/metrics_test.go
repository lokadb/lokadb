package proxy

import (
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetricsText(t *testing.T) {
	m := newMetrics()
	s := &session{srv: &Server{metrics: m}}
	s.mStart.Store(time.Now().Add(-30 * time.Millisecond).UnixNano())
	s.countStatement("7", true)
	s.mErr.Store(true)
	s.endRound()
	s.mStart.Store(time.Now().Add(-2 * time.Millisecond).UnixNano())
	s.countStatement("7", false)
	s.endRound()
	s.countStatement("", false) // no round started: counted, not timed
	s.endRound()
	var b strings.Builder
	m.write(&b, 3)
	out := b.String()
	for _, want := range []string{
		`lokadb_proxy_sessions 3`,
		`lokadb_proxy_statements_total{org="7",path="direct"} 1`,
		`lokadb_proxy_statements_total{org="7",path="fdw"} 1`,
		`lokadb_proxy_statements_total{org="all",path="fdw"} 1`,
		`lokadb_proxy_errors_total{org="7"} 1`,
		`lokadb_proxy_response_seconds_bucket{org="7",le="0.005"} 1`, // the 2 ms response
		`lokadb_proxy_response_seconds_bucket{org="7",le="0.05"} 2`,  // and the 30 ms one
		`lokadb_proxy_response_seconds_count{org="7"} 2`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

// The counters name an organization and nothing the client sent: a label made of anything but
// digits is "other", and past the ceiling the rest is "other" too.
func TestMetricsLabelsAndCeiling(t *testing.T) {
	m := newMetrics()
	if l := orgLabel("1; DROP TABLE x"); l != "other" {
		t.Fatalf("label %q", l)
	}
	for i := 1; i <= maxMetricOrgs+50; i++ {
		m.stat(strconv.Itoa(i)).fdw.Add(1)
	}
	if n := m.n.Load(); n > maxMetricOrgs {
		t.Fatalf("%d organizations kept", n)
	}
	if m.other.fdw.Load() == 0 {
		t.Fatal("nothing counted as other")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := &Server{metrics: newMetrics()}
	rec := httptest.NewRecorder()
	srv.metrics.write(rec, srv.countSessions())
	if !strings.Contains(rec.Body.String(), "lokadb_proxy_info{version=") {
		t.Fatal(rec.Body.String())
	}
}

// The cost on the path of a statement: one request message, one statement, one response.
func BenchmarkMetricsPerStatement(b *testing.B) {
	s := &session{srv: &Server{metrics: newMetrics()}}
	b.RunParallel(func(pb *testing.PB) {
		ps := &session{srv: s.srv}
		for pb.Next() {
			if ps.mStart.Load() == 0 {
				ps.mStart.Store(time.Now().UnixNano())
			}
			ps.countStatement("7", true)
			ps.endRound()
		}
	})
}

func TestMetricsStatusQuantiles(t *testing.T) {
	m := newMetrics()
	st := m.stat("7")
	for i := 0; i < 90; i++ {
		st.observe(3*time.Millisecond, false)
	}
	for i := 0; i < 10; i++ {
		st.observe(300*time.Millisecond, i == 0)
	}
	got := m.status(2)
	if len(got.Orgs) != 1 || got.Orgs[0].P50ms != 5 || got.Orgs[0].P95ms != 500 || got.Orgs[0].Errors != 1 || got.Orgs[0].Responses != 100 {
		t.Fatalf("status: %+v", got)
	}
}
