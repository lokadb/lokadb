package proxy

import (
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// BACKLOG block 9, item 12: the organizations of ids, shared by the sessions: by database and
// role, gone when orgVersion moves or after orgCacheTTL, bounded in bytes.
func TestOrgCacheShared(t *testing.T) {
	c := newOrgCache(0)
	db := "orgcache-" + t.Name()
	v := orgVersion(db).Load()
	c.put(db, "app", "documents__refs", v, map[string]string{"7": "1"})
	if o, ok := c.get(db, "app", "documents__refs", "7"); !ok || o != "1" {
		t.Fatalf("get: %q %v", o, ok)
	}
	if _, ok := c.get(db, "other", "documents__refs", "7"); ok {
		t.Error("another role got the entry")
	}
	if _, ok := c.get(db+"x", "app", "documents__refs", "7"); ok {
		t.Error("another database got the entry")
	}
	orgVersion(db).Add(1)
	if _, ok := c.get(db, "app", "documents__refs", "7"); ok {
		t.Error("kept after orgVersion moved")
	}
	c.put(db, "app", "documents__refs", v, map[string]string{"8": "2"}) // read before the change
	if _, ok := c.get(db, "app", "documents__refs", "8"); ok {
		t.Error("a lookup that began before the change was kept")
	}
	v = orgVersion(db).Load()
	c.put(db, "app", "documents__refs", v, map[string]string{"9": "3"})
	c.mu.Lock()
	c.dbs[db].at = time.Now().Add(-orgCacheTTL - time.Second)
	c.mu.Unlock()
	if _, ok := c.get(db, "app", "documents__refs", "9"); ok {
		t.Error("kept past orgCacheTTL")
	}
	small := newOrgCache(400)
	for i := 0; i < 20; i++ {
		small.put(db, "app", "documents__refs", v, map[string]string{strings.Repeat("x", 10) + string(rune('a'+i)): "1"})
	}
	if small.bytes > 400 {
		t.Errorf("bounded at 400 bytes: holds %d", small.bytes)
	}
}

// Two sessions of one proxy: the second finds the organization the first looked up, without a
// lookup of its own; a session whose transaction moved a row looks up for itself, and what it
// reads does not reach the other.
func TestOrgCacheAcrossSessions(t *testing.T) {
	srv := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil)), orgs: newOrgCache(0)}
	db := "orgcache-" + t.Name()
	refsAnswer := func(org string) func(q string) ([][]string, string, bool) {
		return func(q string) ([][]string, string, bool) {
			if strings.Contains(q, `FROM "documents__refs" WHERE id IN`) {
				return [][]string{{"7", org}}, "", true
			}
			return nil, "", false
		}
	}
	start := func(f *fakeUpstream) (*pgproto3.Frontend, func()) {
		cli, cliSrv := tcpPair(t)
		up, upSrv := tcpPair(t)
		f.be = pgproto3.NewBackend(upSrv, upSrv)
		f.stmts, f.portals, f.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
		go f.run()
		hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
		s := newSession(srv, db, cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, testMeta(), nil)
		go s.run()
		fe := pgproto3.NewFrontend(cli, cli)
		return fe, func() { _ = cli.SetReadDeadline(time.Now().Add(3 * time.Second)) }
	}
	ask := func(fe *pgproto3.Frontend, dl func(), sql string) {
		t.Helper()
		sendAll(t, fe, &pgproto3.Query{String: sql})
		dl()
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				return
			}
		}
	}
	child := "SELECT content FROM document_versions WHERE document_id = 7"
	a := &fakeUpstream{answer: refsAnswer("1")}
	feA, dlA := start(a)
	ask(feA, dlA, child)
	b := &fakeUpstream{answer: refsAnswer("1")}
	feB, dlB := start(b)
	ask(feB, dlB, child)
	if n := countContaining(a.log(), "documents__refs"); n != 1 {
		t.Errorf("the first session: %d lookups, want 1 (%q)", n, a.log())
	}
	if n := countContaining(b.log(), "documents__refs"); n != 0 {
		t.Errorf("the second session looked up what the first had: %d lookups (%q)", n, b.log())
	}
	if !strings.Contains(strings.Join(b.log(), "\n"), "set_config('lokadb.org', '1'") {
		t.Errorf("the second session's statement not under organization 1: %q", b.log())
	}
	// a transaction that moves document 7 to organization 2: its lookups are its own
	c := &fakeUpstream{answer: refsAnswer("2")}
	feC, dlC := start(c)
	ask(feC, dlC, "BEGIN")
	ask(feC, dlC, "UPDATE documents SET organization_id = 2 WHERE id = 7")
	ask(feC, dlC, child)
	if n := countContaining(c.log(), "documents__refs"); n == 0 {
		t.Errorf("the moving transaction took the shared entry instead of looking up: %q", c.log())
	}
	if o, ok := srv.orgs.get(db, "", "documents__refs", "7"); ok && o == "2" {
		t.Error("what the moving transaction read reached the shared cache before its commit")
	}
	// the cooperative mode: a row of organization 2 that another session looked up is not found
	// under lokadb.tenant = 1, as before the shared cache, not refused (e2e coop)
	srv.orgs.put(db, "", "documents__refs", orgVersion(db).Load(), map[string]string{"9": "2"})
	d := &fakeUpstream{}
	feD, dlD := start(d)
	ask(feD, dlD, "BEGIN")
	ask(feD, dlD, "SET LOCAL lokadb.tenant = '1'")
	sendAll(t, feD, &pgproto3.Query{String: "SELECT title FROM documents WHERE id = 9"})
	dlD()
	if code := errorCode(t, feD); code != "" {
		t.Errorf("cooperative, organization 1, a row of organization 2 by its id: %s, want not found", code)
	}
}

// BACKLOG block 9, item 13: the caches of every session together are bounded; a session whose
// entry would go past the bound empties its own.
func TestCacheBytesTotal(t *testing.T) {
	var total atomic.Int64
	a := newByteCache[string, int](100000).shared(&total, 1500)
	b := newByteCache[string, int](100000).shared(&total, 1500)
	a.put("a1", 1, 100)
	a.put("a2", 1, 100)
	for i := 0; i < 20; i++ {
		b.put(string(rune('a'+i)), 1, 100)
	}
	if total.Load() > 1500 {
		t.Errorf("together %d bytes, bound 1500", total.Load())
	}
	a.reset()
	b.reset()
	if total.Load() != 0 {
		t.Errorf("after both reset: %d bytes counted", total.Load())
	}
}
