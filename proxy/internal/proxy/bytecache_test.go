package proxy

import (
	"fmt"
	"strings"
	"testing"
)

// Block 9, review PF7: a session's caches are bounded in bytes, not entries (4.5 MB of point
// texts, 78 MB of 100-row INSERTs per session before), and a text is kept from its second
// sighting.
func TestByteCacheLimit(t *testing.T) {
	c := newByteCache[string, int](10_000)
	for i := 0; i < 100; i++ {
		c.put(fmt.Sprint(i), i, 900)
		if c.bytes > 10_000 {
			t.Fatalf("held %d bytes, limit 10000", c.bytes)
		}
	}
	if c.len() == 0 || c.len() > 11 {
		t.Fatalf("%d entries of 900 bytes in 10000", c.len())
	}
	if c.put("big", 1, 2_000) { // over an eighth of the limit
		t.Fatal("an entry of a fifth of the limit was kept")
	}
	if _, ok := c.get("big"); ok {
		t.Fatal("big is there")
	}
	var nilCache *byteCache[string, int] // a session made by a test
	nilCache.put("a", 1, 10)
	if _, ok := nilCache.get("a"); ok || nilCache.max() != 0 {
		t.Fatal("a nil cache kept something")
	}
}

func TestSeenSetAdmitsFromTheSecondSighting(t *testing.T) {
	var s seenSet
	if s.again("SELECT 1") {
		t.Fatal("the first sighting was taken for a second")
	}
	if !s.again("SELECT 1") {
		t.Fatal("the second sighting was not known")
	}
	if s.again("SELECT 1") { // forgotten once taken: the caller keeps it
		t.Fatal("the third sighting counted as a second again")
	}
	for i := 0; i < 3*maxSeen; i++ {
		s.again(fmt.Sprint("SELECT ", i))
	}
	if len(s.m) > maxSeen {
		t.Fatalf("remembers %d texts, limit %d", len(s.m), maxSeen)
	}
}

// The analysis cache of a session: texts of inlined literals are met once and held nowhere;
// a text that comes back is kept; 5000 INSERTs of 100 rows cost at most the budget.
func TestAnalysisCacheBytes(t *testing.T) {
	s := &session{meta: testMeta(), anCache: newByteCache[string, *Analysis](defaultCacheBytes / 2)}
	for i := 0; i < 2000; i++ {
		s.analysis(fmt.Sprintf("SELECT title FROM documents WHERE organization_id = 1 AND id = %d", i))
	}
	if n := s.anCache.len(); n != 0 {
		t.Fatalf("%d analyses kept of texts met once", n)
	}
	const repeated = "SELECT title FROM documents WHERE organization_id = 1 AND id = 99999"
	a1 := s.analysis(repeated)
	a2 := s.analysis(repeated) // the second sighting: kept now
	a3 := s.analysis(repeated)
	if a2 != a3 || a1 == a3 {
		t.Fatalf("the analysis of a text met twice: first %p second %p third %p (want the second kept)", a1, a2, a3)
	}
	rows := strings.Repeat("(1, 'some title of a document', 'and a body of some length to make a row'), ", 99) + "(1, 't', 'b')"
	for round := 0; round < 2; round++ { // each text twice: kept, up to the budget
		for i := 0; i < 400; i++ {
			s.analysis(fmt.Sprintf("INSERT INTO documents (organization_id, title, body) VALUES %s /* %d */", rows, i))
		}
	}
	if s.anCache.bytes > defaultCacheBytes/2 {
		t.Fatalf("the analysis cache holds %d bytes, budget %d", s.anCache.bytes, defaultCacheBytes/2)
	}
}

// Block 9, review PF5, and the user's decision of 26.10: the "route" line of every statement
// is the proxy's biggest log load (one mutex for every session; 20-25 % of the throughput in
// block 14's measurements). Off by default (at level Debug, which the default logger drops);
// route_log puts it at Info; no_route_log turns it off at every level.
func TestRouteLog(t *testing.T) {
	for _, c := range []struct {
		name          string
		on, off, want bool
	}{{"default", false, false, false}, {"route_log", true, false, true}, {"no_route_log", true, true, false}} {
		d := directSession(t, false)
		d.s.srv.cfg.RouteLog, d.s.srv.cfg.NoRouteLog = c.on, c.off
		d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
		if got := strings.Contains(d.logs.String(), "msg=route"); got != c.want {
			t.Errorf("%s: route line logged %v, want %v", c.name, got, c.want)
		}
	}
}
