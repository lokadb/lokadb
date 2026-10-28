package proxy

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// metaQ: a role's connection as loadMeta and the stamp see it: tenant is what lokadb.tables
// says (so that two roles' metadata can be told apart), stamp what the stamp reads ("" for a
// refused one); reads counts the full reads.
type metaQ struct {
	mu     sync.Mutex
	tenant string
	stamp  string
	role   string // current_user, read with the stamp (the key's role)
	reads  int
	gate   chan struct{} // when set, a full read waits for it (a reload in progress)
}

func (r *metaQ) q(sql string) ([][]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sql = strings.TrimSpace(sql)
	switch {
	case sql == metaStampSQL:
		if r.stamp == "" {
			return nil, errors.New("permission denied for table tables")
		}
		return [][]string{{r.stamp, r.role}}, nil
	case strings.HasPrefix(sql, "SELECT (to_regclass('lokadb.tables')"):
		r.reads++
		if r.gate != nil {
			g := r.gate
			r.mu.Unlock()
			<-g
			r.mu.Lock()
		}
		return [][]string{{"true"}}, nil
	case strings.HasPrefix(sql, "SELECT table_name, tenant_column"):
		return [][]string{{"documents", r.tenant, "documents__refs", "documents__cloud", "id"}}, nil
	}
	return nil, nil
}

func (r *metaQ) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.reads
}

// Block 9, item 5, step 3: the metadata cache keys by database and role. A role gets what it
// read itself, never another role's; the same stamp reads nothing again; a changed stamp or a
// DDL the proxy replayed reads again; a stamp the role may not read leaves the cache out.
func TestMetaCacheByRole(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	b := &metaQ{tenant: "org_id", stamp: "s1", role: "b"}
	tenant := func(m *Meta) string {
		if m == nil || m.Tables["documents"] == nil {
			return "<none>"
		}
		return m.Tables["documents"].TenantCol
	}
	m, _, err := c.load("db", "a", 0, a.q)
	if err != nil || tenant(m) != "organization_id" {
		t.Fatalf("role a: %v %s", err, tenant(m))
	}
	if m, _, _ = c.load("db", "a", 0, a.q); a.count() != 1 || tenant(m) != "organization_id" {
		t.Errorf("role a again, same stamp: %d reads (want 1), %s", a.count(), tenant(m))
	}
	if m, _, _ = c.load("db", "b", 0, b.q); b.count() != 1 || tenant(m) != "org_id" {
		t.Errorf("role b after role a: %d reads (want its own 1), got %s (want its own org_id)", b.count(), tenant(m))
	}
	a.stamp = "s2" // enable, a move, org-profile: what the stamp covers changed
	if _, _, _ = c.load("db", "a", 0, a.q); a.count() != 2 {
		t.Errorf("a changed stamp: %d reads, want 2", a.count())
	}
	if _, _, _ = c.load("db", "a", 1, a.q); a.count() != 3 {
		t.Errorf("a DDL replayed since: %d reads, want 3", a.count())
	}
	d := &metaQ{tenant: "organization_id", role: "d"} // the stamp refused: no rights on lokadb.*
	for i := 0; i < 2; i++ {
		_, _, _ = c.load("db", "d", 0, d.q)
	}
	if d.count() != 2 {
		t.Errorf("a role the stamp is refused to: %d reads, want one each time (2)", d.count())
	}
}

// One session reads an entry, the others wait for it: concurrent sessions of a role read once.
func TestMetaCacheOneRead(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if m, _, err := c.load("db", "a", 0, a.q); err != nil || m == nil {
				t.Errorf("load: %v %v", m, err)
			}
		}()
	}
	wg.Wait()
	if a.count() != 1 {
		t.Errorf("20 sessions at once: %d reads, want 1", a.count())
	}
}

// Block 9, review PR-15: the key is the role the metadata is read under (current_user), not the
// login's: with options=-c role=x a login role's sessions read x's rights, and the entry under the
// login's name would be handed to sessions that log in as it plainly.
func TestMetaCacheByCurrentRole(t *testing.T) {
	c := newMetaCache()
	plain := &metaQ{tenant: "organization_id", stamp: "s1", role: "app"}
	asX := &metaQ{tenant: "org_id", stamp: "s1", role: "x"} // the same login "app", options=-c role=x
	tenant := func(m *Meta) string {
		if m == nil || m.Tables["documents"] == nil {
			return "<none>"
		}
		return m.Tables["documents"].TenantCol
	}
	if m, _, _ := c.load("db", "app", 0, asX.q); tenant(m) != "org_id" {
		t.Fatalf("under role x: %s", tenant(m))
	}
	if m, _, _ := c.load("db", "app", 0, plain.q); plain.count() != 1 || tenant(m) != "organization_id" {
		t.Errorf("the plain login after the one behind -c role=x: %d reads (want its own 1), got %s", plain.count(), tenant(m))
	}
	if m, _, _ := c.load("db", "app", 0, asX.q); asX.count() != 1 || tenant(m) != "org_id" {
		t.Errorf("behind -c role=x again: %d reads (want the entry kept, 1), got %s", asX.count(), tenant(m))
	}
}

// Block 9, review PF1: a session that holds a role's metadata and is due a look asks the cloud
// nothing while a session verified the entry within stampEvery (recheck); a new connection, a
// DDL replayed, an old entry, or an entry nobody verified lately is not served that way.
func TestMetaCacheRecheck(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	m, role, _ := c.load("db", "login", 0, a.q)
	if role != "a" {
		t.Fatalf("the role the metadata were read under: %q, want a", role)
	}
	if got, ok := c.recheck("db", "a", 0); !ok || got != m {
		t.Fatalf("recheck of a verified entry: %v %v", got, ok)
	}
	if _, ok := c.recheck("db", "b", 0); ok {
		t.Fatal("recheck of a role never read")
	}
	if _, ok := c.recheck("db", "a", 1); ok {
		t.Fatal("recheck after a DDL the proxy replayed")
	}
	e := c.e["db\x00a"]
	e.verified = time.Now().Add(-2 * stampEvery) // nobody has looked for a while
	if _, ok := c.recheck("db", "a", 0); ok {
		t.Fatal("recheck of an entry nobody verified lately")
	}
	e.verified, e.at = time.Now(), time.Now().Add(-metaTTL-time.Second) // verified, but due its reload
	if _, ok := c.recheck("db", "a", 0); ok {
		t.Fatal("recheck of an entry past metaTTL with no reload running")
	}
	if a.count() != 1 {
		t.Fatalf("%d full reads, want the first alone", a.count())
	}
}

// An entry past metaTTL is read again by one session; the others, whose stamp is still the
// entry's, go on with it instead of waiting for the read; one whose database changed waits.
func TestMetaCacheStaleWhileReloading(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	old, _, _ := c.load("db", "a", 0, a.q)
	c.e["db\x00a"].at = time.Now().Add(-metaTTL - time.Second)
	slow := &metaQ{tenant: "organization_id", stamp: "s1", role: "a", gate: make(chan struct{})}
	done := make(chan *Meta, 1)
	go func() { m, _, _ := c.load("db", "a", 0, slow.q); done <- m }()
	for i := 0; slow.count() == 0 && i < 200; i++ { // the reload has started
		time.Sleep(5 * time.Millisecond)
	}
	same := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	got := make(chan *Meta, 1)
	go func() { m, _, _ := c.load("db", "a", 0, same.q); got <- m }()
	select {
	case m := <-got:
		if m != old {
			t.Errorf("a session of the same stamp got another metadata while the reload ran")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a session of the same stamp waited for the reload")
	}
	changed := &metaQ{tenant: "organization_id", stamp: "s2", role: "a"}
	waited := make(chan struct{})
	go func() { _, _, _ = c.load("db", "a", 0, changed.q); close(waited) }()
	select {
	case <-waited:
		t.Fatal("a session of a changed stamp did not wait for the new read")
	case <-time.After(150 * time.Millisecond):
	}
	close(slow.gate)
	<-done
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("the waiting session did not go on after the read")
	}
	if slow.count() != 1 || same.count() != 0 {
		t.Errorf("full reads: the reloader %d (want 1), the session that went on %d (want 0)", slow.count(), same.count())
	}
}

// Read again and the same, the entry keeps its pointer: the sessions keep their analyses (a
// new pointer made every session analyse and ask the cloud for result types again, every
// metaTTL).
func TestMetaCacheKeepsThePointer(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	first, _, _ := c.load("db", "a", 0, a.q)
	c.e["db\x00a"].at = time.Now().Add(-metaTTL - time.Second)
	again, _, _ := c.load("db", "a", 0, a.q)
	if a.count() != 2 {
		t.Fatalf("%d full reads, want the reload", a.count())
	}
	if again != first {
		t.Fatal("the same metadata read again came as a new pointer")
	}
	b := &metaQ{tenant: "org_id", stamp: "s1", role: "a"} // the same stamp, other content: the new one
	c.e["db\x00a"].at = time.Now().Add(-metaTTL - time.Second)
	if changed, _, _ := c.load("db", "a", 0, b.q); changed == first {
		t.Fatal("other metadata came as the old pointer")
	}
}

// meta_cache.verify: a new connection within the time of one that verified its role's entry takes
// it without the stamp; none at "0" (the default), none for a login never read, none once the
// verification is old or a DDL was replayed.
func TestMetaCacheRecentConnect(t *testing.T) {
	c := newMetaCache()
	a := &metaQ{tenant: "organization_id", stamp: "s1", role: "a"}
	m, _, _ := c.load("db", "login", 0, a.q)
	if _, _, ok := c.recent("db", "login", 0); ok {
		t.Fatal("recent with verify 0")
	}
	c.verifyFor = time.Second
	if got, role, ok := c.recent("db", "login", 0); !ok || got != m || role != "a" {
		t.Fatalf("recent after a load: %v %q %v", got, role, ok)
	}
	if _, _, ok := c.recent("db", "other", 0); ok {
		t.Fatal("recent for a login never read")
	}
	if _, _, ok := c.recent("db", "login", 1); ok {
		t.Fatal("recent after a DDL the proxy replayed")
	}
	c.e["db\x00a"].verified = time.Now().Add(-2 * time.Second)
	if _, _, ok := c.recent("db", "login", 0); ok {
		t.Fatal("recent with a verification older than verify")
	}
}

// Second review of performance, F8: sessions due a look at once (they connected together) ask
// the cloud for the stamp once between them, not once each; an entry metaTTL old with the same
// stamp is read again by one of them, and the others go on with it meanwhile.
func TestMetaCacheSharedStamp(t *testing.T) {
	for _, old := range []bool{false, true} {
		c := newMetaCache()
		full := &metaQ{tenant: "organization_id", stamp: "s1", role: "app"}
		var stamps atomic.Int64
		q := func(sql string) ([][]string, error) {
			if strings.TrimSpace(sql) == metaStampSQL {
				stamps.Add(1)
				time.Sleep(20 * time.Millisecond) // the stamp's own time
			}
			return full.q(sql)
		}
		if _, _, err := c.load("db", "app", 0, q); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		e := c.e["db\x00app"]
		e.verified = time.Now().Add(-6 * time.Second) // past stampEvery
		if old {
			e.at = time.Now().Add(-metaTTL - time.Second)
		}
		c.mu.Unlock()
		stamps.Store(0)
		const sessions = 100
		var wg sync.WaitGroup
		var got atomic.Int64
		start := make(chan struct{})
		for i := 0; i < sessions; i++ {
			wg.Add(1)
			go func() { // what refresh does for a session that holds the role's metadata (ddl.go)
				defer wg.Done()
				<-start
				if m, ok := c.recheck("db", "app", 0); ok && m != nil {
					got.Add(1)
					return
				}
				if m, ok, err := c.sharedStamp("db", "app", 0, q); ok && err == nil && m != nil {
					got.Add(1)
					return
				}
				_, _, _ = c.load("db", "app", 0, q)
			}()
		}
		close(start)
		wg.Wait()
		if stamps.Load() != 1 || got.Load() != sessions {
			t.Errorf("old %v: %d sessions due at once: %d stamps, %d took the shared answer; want 1 stamp, all of them", old, sessions, stamps.Load(), got.Load())
		}
		if want := 1 + map[bool]int{false: 0, true: 1}[old]; full.count() != want {
			t.Errorf("old %v: full reads %d, want %d", old, full.count(), want)
		}
	}
}

// Second review of the protocol, P5: a read of the role's metadata that panics or hangs does
// not hold the role's next connections for good: a panic releases the entry, a hung read is
// waited for metaWait at most, then the session reads for itself.
func TestMetaCacheStuckRead(t *testing.T) {
	defer func(w time.Duration) { metaWait = w }(metaWait)
	metaWait = 200 * time.Millisecond
	c := newMetaCache()
	hang := make(chan struct{})
	defer close(hang)
	first := &metaQ{tenant: "organization_id", stamp: "s1", role: "app", gate: hang}
	go func() { _, _, _ = c.load("db", "app", 0, first.q) }() // hangs in its full read
	for first.count() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	second := &metaQ{tenant: "organization_id", stamp: "s2", role: "app"}
	t0 := time.Now()
	m, _, err := c.load("db", "app", 0, second.q)
	if err != nil || m == nil || time.Since(t0) > 5*time.Second || second.count() != 1 {
		t.Fatalf("behind a hung read: %v, %v after %v, %d reads of its own; want its own read after metaWait", m, err, time.Since(t0), second.count())
	}
}
