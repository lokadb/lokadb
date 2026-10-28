package proxy

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// directPoolTest: a Server with the pool of direct connections; each dial of organization 1
// opens a fake of its own (orgs), counted.
type directPoolTest struct {
	srv  *Server
	mu   sync.Mutex
	orgs []*fakeUpstream
	// answer: what each new organization fake answers (nil: the defaults)
	answer func(q string) ([][]string, string, bool)
	// marks: the lokadb comments each new organization fake's tables have (nil: organization 1's)
	marks []string
}

func newDirectPoolTest(t *testing.T, c *PoolConfig) *directPoolTest {
	pool, err := newDirectPool(c)
	if err != nil {
		t.Fatal(err)
	}
	dp := &directPoolTest{}
	dp.srv = &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil)), directPool: pool,
		cfg: Config{Customers: map[string]map[string]string{"db": {"1": "host=org1-owner"}},
			CustomersApp: map[string]map[string]string{"db": {"1": "host=org1"}}}}
	dp.srv.dialDirect = func(ctx context.Context, dsn string, params map[string]string) (*directLink, error) {
		dp.mu.Lock()
		f := &fakeUpstream{answer: dp.answer, marks: dp.marks}
		dp.mu.Unlock()
		c, cs := tcpPair(t)
		f.setBackend(pgproto3.NewBackend(cs, cs))
		go f.run()
		dp.mu.Lock()
		dp.orgs = append(dp.orgs, f)
		dp.mu.Unlock()
		return &directLink{conn: c, fe: pgproto3.NewFrontend(c, c), pid: 77, secret: []byte{1, 2, 3, 4}}, nil
	}
	return dp
}

func (dp *directPoolTest) dials() int {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return len(dp.orgs)
}

// session runs a session on dp's server; end closes the client and waits for the session to
// be over (its direct connections closed or given back).
func (dp *directPoolTest) session(t *testing.T, edit ...func(*Meta)) (d *directTest, end func()) {
	cloud := &fakeUpstream{profile: ProfileFDWDirect}
	cli, cliSrv := tcpPair(t)
	up, upSrv := tcpPair(t)
	cloud.setBackend(pgproto3.NewBackend(upSrv, upSrv))
	go cloud.run()
	hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
	meta := directMeta()
	for _, e := range edit {
		e(meta)
	}
	s := newSession(dp.srv, "db", cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, meta, nil)
	done := make(chan struct{})
	go func() { s.run(); close(done) }()
	d = &directTest{fe: pgproto3.NewFrontend(cli, cli), cloud: cloud, s: s}
	return d, func() {
		cli.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("the session did not end")
		}
	}
}

func (dp *directPoolTest) org(i int) *fakeUpstream {
	dp.mu.Lock()
	defer dp.mu.Unlock()
	return dp.orgs[i]
}

// Block 10, item 7: a session's direct connection outlives the session. It is reset (DISCARD
// ALL) and given to the next session, which does not get the settings the first one repeated
// on it and Parses its statements again under its own names.
func TestDirectPoolReuse(t *testing.T) {
	dp := newDirectPoolTest(t, &PoolConfig{})
	a, endA := dp.session(t)
	if code := a.query(t, "SET statement_timeout = 5000"); code != "" {
		t.Fatal(code)
	}
	if code := a.prepared(t, "s", "SELECT title FROM documents WHERE organization_id = $1", "1"); code != "" {
		t.Fatal(code)
	}
	endA()
	if dp.dials() != 1 {
		t.Fatalf("%d direct connections for the first session", dp.dials())
	}
	org := dp.org(0)
	log := org.log()
	if !ranAny(log, "SET statement_timeout = 5000") || !strings.Contains(strings.Join(log, "\n"), "DISCARD ALL") {
		t.Fatalf("the first session's setting, then the reset: %q", log)
	}
	b, endB := dp.session(t)
	defer endB()
	if code := b.prepared(t, "s", "SELECT title FROM documents WHERE organization_id = $1", "1"); code != "" {
		t.Fatal(code)
	}
	if dp.dials() != 1 {
		t.Fatalf("the second session opened a connection of its own: %d dials", dp.dials())
	}
	log = org.log()
	at := -1
	for i, q := range log {
		if q == "DISCARD ALL" {
			at = i
		}
	}
	after := log[at+1:]
	if ranAny(after, "statement_timeout") || !ranAny(after, "organization_id = 1") {
		t.Fatalf("after the reset the second session's own statements only: %q", after)
	}
}

// A connection on which the organization's server refused the role something (42501) is not
// kept; one older than max_age is not either.
func TestDirectPoolNotKept(t *testing.T) {
	for _, c := range []struct {
		name   string
		pool   *PoolConfig
		answer func(q string) ([][]string, string, bool)
		wait   time.Duration
	}{
		{"a privilege refused", &PoolConfig{}, func(q string) ([][]string, string, bool) {
			if strings.Contains(q, "documents") {
				return nil, "42501:permission denied for table documents__o1", true
			}
			return nil, "", false
		}, 0},
		{"past max_age", &PoolConfig{MaxAge: "100ms"}, nil, 200 * time.Millisecond},
	} {
		t.Run(c.name, func(t *testing.T) {
			dp := newDirectPoolTest(t, c.pool)
			dp.answer = c.answer
			a, endA := dp.session(t)
			_ = a.prepared(t, "s", "SELECT title FROM documents WHERE organization_id = $1", "1")
			time.Sleep(c.wait)
			endA()
			b, endB := dp.session(t)
			defer endB()
			_ = b.prepared(t, "s", "SELECT title FROM documents WHERE organization_id = $1", "1")
			if dp.dials() != 2 {
				t.Fatalf("%d dials: the first session's connection was kept", dp.dials())
			}
		})
	}
}

// A connection with an answer left unread (an aborted read: a ReadyForQuery of an earlier
// exchange still on the wire) is not taken for reset: the reset's own tag must come first.
func TestDirectPoolLeftover(t *testing.T) {
	pool, err := newDirectPool(&PoolConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s := &session{srv: &Server{directPool: pool}}
	c, cs := tcpPair(t)
	be := pgproto3.NewBackend(cs, cs)
	go func() {
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'}) // the earlier exchange's, unread
		_ = be.Flush()
		if _, err := be.Receive(); err != nil { // DISCARD ALL
			return
		}
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()
	time.Sleep(50 * time.Millisecond)
	dc := &directConn{conn: c, fe: pgproto3.NewFrontend(c, c), poolKey: "k", born: time.Now()}
	if s.parkDirect(dc) {
		t.Fatal("a connection with an unread answer was taken as reset")
	}
}

// The pool's key carries the DSN of the organization's role as customersApp finds it: an
// organization named by a uuid is configured by its number (block 18), and a key built from the
// config by the uuid held no DSN, so a changed DSN did not part the pooled connections.
func TestDirectPoolKeyUUID(t *testing.T) {
	const u = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	s := &session{db: "db", meta: &Meta{OrgNum: map[string]string{u: "7"}},
		srv: &Server{cfg: Config{CustomersApp: map[string]map[string]string{"db": {"7": "host=org7 user=lokadb_dml"}}}}}
	k1 := s.directPoolKeyFor(u, map[string]string{"search_path": "public"})
	if !strings.Contains(k1, "host=org7 user=lokadb_dml") {
		t.Fatalf("the key of organization %s holds no DSN: %q", u, k1)
	}
	s.srv.cfg.CustomersApp["db"]["7"] = "host=org7b user=lokadb_dml"
	if k2 := s.directPoolKeyFor(u, map[string]string{"search_path": "public"}); k2 == k1 {
		t.Errorf("a changed DSN keeps the key: %q", k2)
	}
}

// Review of admin, A1 (with block 8's check of the database): repoint moved organization 1 to
// another database at the same address (a failover behind one name). A pooled connection
// checked against the old database's id is closed, not handed out; the new one is checked
// against the new id, and while the address still reaches the old database it is refused.
func TestDirectPoolRepointSameAddress(t *testing.T) {
	dp := newDirectPoolTest(t, &PoolConfig{})
	id := func(v string) func(*Meta) { return func(m *Meta) { m.DBIDs = map[string]string{"1": v} } }
	sel := "SELECT title FROM documents WHERE organization_id = 1"
	dp.marks = []string{identityMark("1", "a")}
	a, endA := dp.session(t, id("a"))
	if code := a.query(t, sel); code != "" || dp.dials() != 1 {
		t.Fatalf("before: error %q, %d dials", code, dp.dials())
	}
	endA()
	dp.mu.Lock()
	dp.marks = []string{identityMark("1", "b")} // the address reaches the new database now
	dp.mu.Unlock()
	b, endB := dp.session(t, id("b"))
	if code := b.query(t, sel); code != "" || dp.dials() != 2 || !ranAny(dp.org(1).log(), "organization_id = 1") {
		t.Fatalf("after repoint: error %q, %d dials (want the pooled one dropped, a new one checked)", code, dp.dials())
	}
	if n := strings.Count(strings.Join(dp.org(0).log(), "\n"), "organization_id = 1"); n != 1 {
		t.Fatalf("the old database's connection ran %d reads, want the first session's only", n)
	}
	endB()
	dp.mu.Lock()
	dp.marks = []string{identityMark("1", "a")} // the address still reaches the old database
	dp.mu.Unlock()
	dp.srv.directPool.flush()
	c, endC := dp.session(t, id("b"))
	defer endC()
	if code := c.query(t, sel); code != "" || dp.dials() != 3 || ranAny(dp.org(2).log(), "organization_id = 1") || !ranAny(c.cloud.log(), "documents__org1") {
		t.Fatalf("an address on the old database: error %q, %d dials, it ran %q, the cloud %q", code, dp.dials(), dp.org(2).log(), c.cloud.log())
	}
}

// Review of performance, PF11: a direct connection unused for directIdleClose is closed before
// the session's next message, and the next direct statement opens another one.
func TestDirectIdleClose(t *testing.T) {
	directIdleClose = 50 * time.Millisecond
	defer func() { directIdleClose = 60 * time.Second }()
	d := directSession(t, false)
	q := "SELECT title FROM documents WHERE organization_id = 1"
	if code := d.query(t, q); code != "" || !d.direct(q) {
		t.Fatalf("direct read: %q", code)
	}
	time.Sleep(120 * time.Millisecond)
	if code := d.query(t, "SELECT count(*) FROM projects"); code != "" { // a message: the idle connection goes
		t.Fatal(code)
	}
	if code := d.query(t, q); code != "" || d.dials != 2 {
		t.Fatalf("after the idle close: %q, %d connections opened, want 2", code, d.dials)
	}
}

// Third review of performance, perf3-21: the reset of an idle direct connection (DISCARD ALL, here
// 600 ms in the organization's database) runs apart from the client's message, which is not
// held for it; the connection goes back to the pool after it, and the next session gets it.
func TestDirectIdleCloseInBackground(t *testing.T) {
	directIdleClose = 50 * time.Millisecond
	defer func() { directIdleClose = 60 * time.Second }()
	dp := newDirectPoolTest(t, &PoolConfig{})
	d, end := dp.session(t)
	q := "SELECT title FROM documents WHERE organization_id = 1"
	if code := d.query(t, q); code != "" {
		t.Fatal(code)
	}
	dp.org(0).delayQuery = 600 * time.Millisecond // its DISCARD ALL is slow
	time.Sleep(120 * time.Millisecond)
	start := time.Now()
	if code := d.query(t, "SELECT count(*) FROM projects"); code != "" { // the idle connection goes
		t.Fatal(code)
	}
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Errorf("the client's message waited %v for the reset of an idle direct connection (want it in the background)", took)
	}
	time.Sleep(900 * time.Millisecond) // the reset is over: the connection is in the pool
	end()
	e, endE := dp.session(t)
	defer endE()
	dp.org(0).delayQuery = 0
	if code := e.query(t, q); code != "" || dp.dials() != 1 {
		t.Fatalf("the next session: %q, %d connections opened, want the pooled one (1)", code, dp.dials())
	}
}

// Third review of performance, perf3-14: the types of a client's unnamed statement, which the
// cloud describes before the direct path takes it, are kept for the process: the next session
// that Parses the same text goes direct without asking the cloud (it had one exchange and a
// parse there for every statement of every session).
func TestDirectTypesShared(t *testing.T) {
	dp := newDirectPoolTest(t, &PoolConfig{})
	read := "SELECT title FROM documents WHERE organization_id = $1"
	run := func(d *directTest) {
		t.Helper()
		sendAll(t, d.fe, &pgproto3.Parse{Query: read}, &pgproto3.Bind{Parameters: [][]byte{[]byte("1")}}, &pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
		if types, code := d.answerTypes(t); code != "" || strings.Join(types, ",") != "ParseComplete,BindComplete,NoData,CommandComplete,ReadyForQuery" {
			t.Fatalf("the client got %v (%q)", types, code)
		}
	}
	unnamedParses := func(f *fakeUpstream) int {
		f.mu.Lock()
		defer f.mu.Unlock()
		n := 0
		for _, name := range f.parsed {
			if name == "" {
				n++
			}
		}
		return n
	}
	a, endA := dp.session(t)
	a.cloud.describeTypes = []uint32{20}
	run(a)
	if unnamedParses(a.cloud) != 1 {
		t.Fatalf("the first session: the cloud described the text %d times, want 1", unnamedParses(a.cloud))
	}
	endA()
	b, endB := dp.session(t)
	defer endB()
	run(b)
	if n := unnamedParses(b.cloud); n != 0 {
		t.Fatalf("the second session: the cloud described the same text again (%d), want the process's types", n)
	}
	if !ranAny(dp.org(0).log(), "SELECT title FROM documents WHERE organization_id = 1") {
		t.Fatalf("the second session's read did not go direct: %q", dp.org(0).log())
	}
}
