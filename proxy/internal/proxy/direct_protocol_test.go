package proxy

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The direct path (profile fdw+direct, stage 8a, direct_session.go) through a session: the
// cloud and organization 1's database are two fake upstreams.

// directMeta: testMeta with organization 1 moved and in fdw+direct, organization 2 moved in fdw.
func directMeta() *Meta {
	m := testMeta()
	m.OrgLock = true
	m.Foreign = map[string]map[string]string{
		"documents":         {"1": "documents__org1", "2": "documents__org2"},
		"document_versions": {"1": "document_versions__org1", "2": "document_versions__org2"},
	}
	// the role has on the partitions what it has on the parents: the cloud's way aims at them
	m.Direct = map[string]bool{"documents__org1": true, "documents__org2": true, "document_versions__org1": true, "document_versions__org2": true}
	m.Profiles = map[string]string{"1": ProfileFDWDirect, "2": "fdw"}
	return m
}

type directTest struct {
	// cloudW, orgW: the proxy's writes to the cloud and to the organization's database (item 9:
	// each one an exchange it sends, whatever it pipelines in it)
	cloudW, orgW atomic.Int64
	fe           *pgproto3.Frontend
	cloud, org1  *fakeUpstream
	probe        *fakeUpstream // organization 1's database on a connection of the proxy's own (orgTxStatus)
	dials        int
	s            *session
	cancelAddr   string      // where the direct connection's CancelRequest goes ("": nowhere)
	conns        []net.Conn  // organization 1's server side of each direct connection
	cancelTLS    *tls.Config // the DSN's TLS, which the cancel takes too
	logs         *syncBuf
	params       map[string]string // the startup parameters of the last direct connection
}

// syncBuf: the session's log, written by its goroutines and read by the test.
type syncBuf struct {
	mu sync.Mutex
	b  strings.Builder
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// directSession: a session whose organization 1 is reached at org1 (dialFail: it is not);
// edit changes its metadata before it starts.
func directSession(t *testing.T, dialFail bool, edit ...func(*Meta)) *directTest {
	d := &directTest{cloud: &fakeUpstream{profile: ProfileFDWDirect}, org1: &fakeUpstream{}, probe: &fakeUpstream{answerSimple: true}}
	meta := directMeta()
	for _, e := range edit {
		e(meta)
	}
	cli, cliSrv := tcpPair(t)
	up, upSrv := tcpPair(t)
	d.cloud.be = pgproto3.NewBackend(upSrv, upSrv)
	d.cloud.stmts, d.cloud.portals, d.cloud.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
	go d.cloud.run()
	d.logs = &syncBuf{}
	srv := &Server{log: slog.New(slog.NewTextHandler(d.logs, nil)),
		cfg: Config{Customers: map[string]map[string]string{"db": {"1": "host=org1-owner", "2": "host=org2-owner"}},
			CustomersApp: map[string]map[string]string{"db": {"1": "host=org1", "2": "host=org2"}}}}
	srv.dialDirect = func(ctx context.Context, dsn string, params map[string]string) (*directLink, error) {
		d.dials++
		d.params = params
		if dialFail || dsn != "host=org1" {
			return nil, errors.New("unreachable")
		}
		if len(params) == 0 { // orgTxStatus's connection of its own: the probe answers it
			c, cs := tcpPair(t)
			d.probe.setBackend(pgproto3.NewBackend(cs, cs))
			go d.probe.run()
			return &directLink{conn: c, fe: pgproto3.NewFrontend(c, c)}, nil
		}
		c, cs := tcpPair(t)
		d.conns = append(d.conns, cs)
		d.org1.setBackend(pgproto3.NewBackend(cs, cs))
		go d.org1.run()
		cw := &countConn{Conn: c, n: &d.orgW}
		return &directLink{conn: cw, fe: pgproto3.NewFrontend(cw, cw), addr: d.cancelAddr, pid: 77, secret: []byte{1, 2, 3, 4}, tls: d.cancelTLS}, nil
	}
	uw := &countConn{Conn: up, n: &d.cloudW}
	hj := &pgconn.HijackedConn{Conn: uw, Frontend: pgproto3.NewFrontend(uw, uw)}
	d.s = newSession(srv, "db", cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, meta, nil)
	go d.s.run()
	d.fe = pgproto3.NewFrontend(cli, cli)
	return d
}

func (d *directTest) query(t *testing.T, sql string) string {
	t.Helper()
	sendAll(t, d.fe, &pgproto3.Query{String: sql})
	return errorCode(t, d.fe)
}

// prepared: Parse of name (the cloud), then Bind, Describe, Execute, Sync with args.
func (d *directTest) prepared(t *testing.T, name, sql string, args ...string) string {
	t.Helper()
	sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Sync{})
	if code := errorCode(t, d.fe); code != "" {
		return code
	}
	return d.bound(t, name, args...)
}

func (d *directTest) bound(t *testing.T, name string, args ...string) string {
	t.Helper()
	var ps [][]byte
	for _, a := range args {
		ps = append(ps, []byte(a))
	}
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Describe{ObjectType: 'P'},
		&pgproto3.Execute{}, &pgproto3.Sync{})
	return errorCode(t, d.fe)
}

// direct: ran on organization 1's connection and not in the cloud.
func (d *directTest) direct(sub string) bool {
	return ranAny(d.org1.log(), sub) && !ranAny(d.cloud.log(), sub)
}

// clouded: ran in the cloud and not on organization 1's connection.
func (d *directTest) clouded(sub string) bool {
	return ranAny(d.cloud.log(), sub) && !ranAny(d.org1.log(), sub)
}

// DP: a statement of organization 1 confined by its conditions goes to its database, under
// the cloud's pause lock taken before and released after; organization 2 (fdw) stays in the cloud.
func TestDirectRead(t *testing.T) {
	d := directSession(t, false)
	if code := d.prepared(t, "s", "SELECT title FROM documents WHERE organization_id = $1", "1"); code != "" {
		t.Fatalf("error %s", code)
	}
	if !d.direct("SELECT title FROM documents WHERE organization_id = 1") {
		t.Fatalf("not on the direct path: cloud %q, org1 %q", d.cloud.log(), d.org1.log())
	}
	if code := d.bound(t, "s", "2"); code != "" || !d.clouded("organization_id = 2") {
		t.Fatalf("organization 2 (fdw): error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	cl := d.cloud.log() // the unlock is sent without waiting: seen once the next statement ran
	lock, unlock := -1, -1
	for i, q := range cl {
		if strings.Contains(q, "pg_advisory_lock_shared(lokadb.org_lock(1))") {
			lock = i
		}
		if strings.Contains(q, "pg_advisory_unlock_shared(lokadb.org_lock(1))") {
			unlock = i
		}
	}
	if lock < 0 || unlock < lock {
		t.Fatalf("pause lock around it: %q", cl)
	}
	// the statement is Parsed there once, under a name of the proxy's own
	if code := d.bound(t, "s", "1"); code != "" {
		t.Fatal(code)
	}
	if n := len(d.org1.stmts); n != 1 || d.org1.stmts["lokadb_d1"] == "" || d.dials != 1 {
		t.Fatalf("statements on the direct connection %v, dials %d", d.org1.stmts, d.dials)
	}
	// a simple query too
	if code := d.query(t, "SELECT slug FROM documents WHERE organization_id = 1"); code != "" || !d.direct("SELECT slug FROM documents") {
		t.Fatalf("simple query: %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
}

// What never goes the direct path: several organizations, writes the refs tables follow
// in forms 8b does not take (RETURNING of a DELETE, USING, an INSERT whose key the proxy has
// no sequence for), DDL, a transaction block, statements the organization's conditions do not
// confine.
func TestDirectNot(t *testing.T) {
	cases := []string{
		"SELECT title FROM documents WHERE organization_id IN (1, 2)",
		"SELECT a.title FROM documents a JOIN documents b ON a.project_id = b.project_id WHERE a.organization_id = 1 AND b.organization_id = 2",
		"SELECT title FROM documents WHERE organization_id = 1 AND id IN (SELECT document_id FROM document_versions)",
		"DELETE FROM documents WHERE organization_id = 1 RETURNING id",
		"DELETE FROM documents USING document_versions WHERE documents.organization_id = 1 AND document_versions.organization_id = 1",
		"INSERT INTO documents (organization_id, title) VALUES (1, 'x')", // directMeta gives the key no sequence
		"UPDATE documents SET organization_id = 2 WHERE organization_id = 1",
		"UPDATE documents SET id = 5 WHERE organization_id = 1",
		"ALTER TABLE documents ADD COLUMN x int",
		"SELECT title FROM documents WHERE organization_id = 1 AND nextval('s') > 0",
		"SELECT title FROM documents WHERE organization_id = 1 AND public.f(id)",
		"SELECT title FROM documents, projects WHERE documents.organization_id = 1",
		"SELECT title FROM documents WHERE slug = 'a'",
		"WITH x AS (DELETE FROM documents WHERE organization_id = 1 RETURNING id) SELECT * FROM x",
	}
	for _, sql := range cases {
		d := directSession(t, false)
		d.query(t, sql)
		if len(d.org1.log()) > 0 || d.dials > 0 {
			t.Errorf("%s: went the direct path: %q", sql, d.org1.log())
		}
	}
	// a transaction block that starts on the cloud's way stays there (8a.2: direct_tx_test.go)
	d := directSession(t, false)
	d.query(t, "BEGIN")
	d.query(t, "SELECT count(*) FROM projects")
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	if len(d.org1.log()) > 0 {
		t.Errorf("in a transaction block begun in the cloud: %q", d.org1.log())
	}
	// an UPDATE of other columns goes
	d = directSession(t, false)
	if code := d.query(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1"); code != "" || !d.direct("SET title = 'y'") {
		t.Errorf("UPDATE of a plain column: %q, org1 %q", code, d.org1.log())
	}
}

// The gate reads the profile after the lock: a switch committed while it waited (org-profile
// fdw, move-back) sends the statement the cloud's way, and the lock is released.
func TestDirectGateProfileChanged(t *testing.T) {
	d := directSession(t, false)
	d.cloud.profile = "fdw"
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" {
		t.Fatal(code)
	}
	if len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") || !ranAny(d.cloud.log(), "pg_advisory_unlock_shared(lokadb.org_lock(1))") {
		t.Fatalf("cloud %q, org1 %q", d.cloud.log(), d.org1.log())
	}
	d.cloud.profile = "" // moved back: no row in lokadb.orgs
	if code := d.query(t, "SELECT slug FROM documents WHERE organization_id = 1"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "SELECT slug") {
		t.Fatalf("after move-back: %q, org1 %q", code, d.org1.log())
	}
}

// The organization's database out of reach: the statement goes the cloud's way.
func TestDirectDialFails(t *testing.T) {
	d := directSession(t, true)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || !ranAny(d.cloud.log(), "documents__org1") {
		t.Fatalf("error %q, cloud %q", code, d.cloud.log())
	}
}

// Session settings that ran in the cloud are repeated on the direct connection before its
// next statement; ones it cannot repeat turn the direct path off.
func TestDirectSettings(t *testing.T) {
	d := directSession(t, false)
	d.query(t, "SET TimeZone = 'Europe/Moscow'")
	d.query(t, "SET LOCAL statement_timeout = 5")
	d.query(t, "SET lokadb.tenant = '1'")
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	org := d.org1.log()
	if len(org) != 2 || org[0] != "SET TimeZone = 'Europe/Moscow'" || !strings.HasPrefix(org[1], "SELECT title") {
		t.Fatalf("direct connection ran %q", org)
	}
	d.query(t, "RESET ALL")
	d.query(t, "SELECT slug FROM documents WHERE organization_id = 1")
	if org = d.org1.log(); len(org) != 4 || org[2] != "RESET ALL" {
		t.Fatalf("direct connection ran %q", org)
	}
	for _, off := range [][]string{
		{"SELECT set_config('row_security', 'off', false)"},
		{"SET session_replication_role = replica"},
		{"SET ROLE other"},
		{"BEGIN", "SET TimeZone = 'UTC'", "COMMIT"},
	} {
		d := directSession(t, false)
		for _, q := range off {
			d.query(t, q)
		}
		d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
		if len(d.org1.log()) > 0 || d.s.direct.off == "" {
			t.Errorf("%q: direct path still on: %q", off, d.org1.log())
		}
	}
	// set_config with a computed name: the proxy refuses it before the cloud (it may name
	// lokadb.org or lokadb.tenant; block 9's B9.12), so it never runs and has nothing to repeat
	d = directSession(t, false)
	if code := d.query(t, "SELECT set_config(lower('TimeZone'), 'UTC', false)"); code != "0A000" || len(d.cloud.log()) > 0 {
		t.Errorf("set_config with a computed name: %q, cloud %q", code, d.cloud.log())
	}
}

// Under lokadb.tenant the direct path takes only a statement whose own conditions name that
// organization; another organization's statement goes the cloud's way (42501 there).
func TestDirectTenant(t *testing.T) {
	d := directSession(t, false)
	d.s.srv.cfg.Tenant = sessionTenant()
	d.query(t, "SET lokadb.tenant = '7'")
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	if len(d.org1.log()) > 0 {
		t.Fatalf("another organization's statement went direct: %q", d.org1.log())
	}
	d.query(t, "SET lokadb.tenant = '1'")
	if code := d.query(t, "SELECT slug FROM documents WHERE organization_id = 1"); code != "" || !d.direct("SELECT slug") {
		t.Fatalf("named and confined: %q, org1 %q", code, d.org1.log())
	}
	// the second step of 8a.2: under a named organization a statement needs no conditions of
	// its own (its database holds its rows alone, as the cloud's policy lokadb_tenant shows them)
	for _, q := range []string{"SELECT title FROM documents", "UPDATE documents SET title = 'z'"} {
		if code := d.query(t, q); code != "" || !d.direct(q) {
			t.Fatalf("named, no conditions, %s: %q, org1 %q", q, code, d.org1.log())
		}
	}
	// its values name another organization the proxy knows of: the cloud's way, 42501 there
	n := len(d.org1.log())
	d.query(t, "SELECT title FROM documents WHERE organization_id = 2")
	d.query(t, "SET lokadb.tenant = '*'")
	d.query(t, "SELECT body FROM documents")
	d.query(t, "SET lokadb.tenant = '7'") // not in fdw+direct
	d.query(t, "SELECT body FROM documents")
	if len(d.org1.log()) != n {
		t.Fatalf("another organization's, '*' or an organization not in fdw+direct went direct: %q", d.org1.log()[n:])
	}
}

// The gate checks in the cloud what the FDW path would: the client's role may run the
// statement (review of block 9, A), the organization's partition is still a foreign table,
// no policy of the application's own is on the table. Any of them failing: the cloud's way,
// where Postgres answers as before (42501 for a missing privilege).
func TestDirectGateDenied(t *testing.T) {
	d := directSession(t, false)
	d.cloud.denied = true
	if code := d.query(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") {
		t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	var gate string
	for _, q := range d.cloud.log() {
		if strings.Contains(q, "FROM lokadb.orgs WHERE org = 1)") {
			gate = q
		}
	}
	for _, want := range []string{`has_table_privilege(to_regclass('"documents"'), 'SELECT')`,
		`has_table_privilege(to_regclass('"documents"'), 'UPDATE'), false) OR (coalesce(has_column_privilege(to_regclass('"documents"'), 'title', 'UPDATE'), false))`,
		`relname = documents__org1 AND relnamespace = (SELECT relnamespace FROM pg_class WHERE oid = to_regclass('"documents"'))`, "polname <> 'lokadb_tenant'"} {
		if !strings.Contains(gate, want) {
			t.Errorf("gate %q lacks %q", gate, want)
		}
	}
}

// The client's CancelRequest reaches the statement it runs on the direct path: the proxy
// sends one with the direct connection's key to its server (review of block 9).
func TestDirectCancel(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	d := directSession(t, false)
	d.cancelAddr = l.Addr().String()
	d.org1.delayQuery = 700 * time.Millisecond
	done := make(chan string, 1)
	go func() { done <- d.query(t, "SELECT title FROM documents WHERE organization_id = 1") }()
	time.Sleep(200 * time.Millisecond)
	go d.s.srv.forwardCancel(&pgproto3.CancelRequest{ProcessID: 0, SecretKey: nil}) // the key the client got (the fake cloud's)
	_ = l.(*net.TCPListener).SetDeadline(time.Now().Add(3 * time.Second))
	c, err := l.Accept()
	if err != nil {
		t.Fatalf("no CancelRequest reached the organization's server: %v", err)
	}
	buf := make([]byte, 64)
	n, _ := io.ReadAtLeast(c, buf, 16)
	c.Close()
	var got pgproto3.CancelRequest
	if err := got.Decode(buf[4:n]); err != nil || got.ProcessID != 77 || cancelKey(got.ProcessID, got.SecretKey) != "77/01020304" {
		t.Fatalf("CancelRequest %+v (%v), want the direct connection's key 77/01020304", got, err)
	}
	<-done
}

// A simple query string that is an UPDATE and a SET: the cloud runs both, the direct
// connection repeats the SET alone, never the UPDATE (review of block 9, 3).
func TestDirectSetsAlone(t *testing.T) {
	d := directSession(t, false)
	d.query(t, "UPDATE documents SET title = 'x' WHERE organization_id = 2 AND id = 5; SET statement_timeout = 5000")
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" {
		t.Fatal(code)
	}
	org := d.org1.log()
	if len(org) != 2 || org[0] != "SET statement_timeout = 5000" || !strings.HasPrefix(org[1], "SELECT title") {
		t.Fatalf("direct connection ran %q, want the SET alone, then the statement", org)
	}
}

// A cancel (or lock_timeout) while the gate waits for a move: the client gets the error, the
// statement runs neither on the direct path nor in the cloud (review of block 9, 4).
func TestDirectGateCancelled(t *testing.T) {
	for _, mode := range []string{"simple", "batch"} {
		d := directSession(t, false)
		if mode == "batch" {
			if code := d.prepared(t, "s", "SELECT slug FROM documents WHERE organization_id = $1", "2"); code != "" {
				t.Fatal(code)
			}
		}
		d.cloud.lockErr = "57014"
		var code string
		if mode == "simple" {
			code = d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
		} else {
			code = d.bound(t, "s", "1")
		}
		if code != "57014" || len(d.org1.log()) > 0 || ranAny(d.cloud.log(), "documents__org1") || ranAny(d.cloud.log(), "organization_id = 1") {
			t.Fatalf("%s: error %q, cloud %q, org1 %q", mode, code, d.cloud.log(), d.org1.log())
		}
		// the session goes on: the next statement runs
		d.cloud.lockErr = ""
		if code := d.query(t, "SELECT id, slug FROM documents WHERE organization_id = 1"); code != "" || !d.direct("SELECT id, slug") {
			t.Fatalf("%s: after the cancel: %q, org1 %q", mode, code, d.org1.log())
		}
	}
}

// A pipeline cut by Flush leaves the cloud's implicit transaction open: the simple query
// after it goes to the cloud, not the direct path (review of block 9, 5).
func TestDirectAfterFlush(t *testing.T) {
	d := directSession(t, false)
	sendAll(t, d.fe, &pgproto3.Parse{Query: "SELECT slug FROM documents WHERE organization_id = 2"}, &pgproto3.Bind{},
		&pgproto3.Execute{}, &pgproto3.Flush{})
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := m.(*pgproto3.CommandComplete); ok {
			break
		}
	}
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") {
		t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
}

// A direct connection dropped while idle (relay, agent, NAT): a read runs once more on a new
// connection, the client sees nothing of it; an UPDATE gets 08006 (it may have run).
func TestDirectIdleDrop(t *testing.T) {
	d := directSession(t, false)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" {
		t.Fatal(code)
	}
	d.conns[0].Close() // gone while idle
	if code := d.query(t, "SELECT slug FROM documents WHERE organization_id = 1"); code != "" || d.dials != 2 || !d.direct("SELECT slug") {
		t.Fatalf("read after the drop: error %q, dials %d, org1 %q", code, d.dials, d.org1.log())
	}
	d.conns[1].Close()
	if code := d.query(t, "UPDATE documents SET title = 'y' WHERE organization_id = 1"); code != "08006" || d.dials != 2 {
		t.Fatalf("UPDATE after the drop: error %q, dials %d", code, d.dials)
	}
}

// The direct path's role does DML alone: one that owns a table, may create or passes by row
// security is refused on connecting, the organization's statements go through the cloud, and
// the owner's DSN (customers, DDL replay) is never dialled by it (review of block 9, 6).
func TestDirectRoleChecked(t *testing.T) {
	d := directSession(t, false)
	d.org1.roleWhy = "owns documents"
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") {
		t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	d = directSession(t, false)
	d.s.srv.cfg.CustomersApp = nil
	if d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); d.dials != 0 {
		t.Fatalf("no customers_app: dialled %d", d.dials)
	}
}

// With TLS in the DSN (sslmode=require, a relay requiring it for the organization) the
// cancel goes through TLS too: SSLRequest, the handshake, then the CancelRequest (review of
// block 9, B). A server that refuses TLS gets no cancel in clear.
func TestDirectCancelTLS(t *testing.T) {
	certPath, keyPath := selfSigned(t, t.TempDir(), "localhost")
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	got := make(chan string, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			got <- err.Error()
			return
		}
		defer c.Close()
		req := make([]byte, 8)
		if _, err := io.ReadFull(c, req); err != nil {
			got <- err.Error()
			return
		}
		_, _ = c.Write([]byte{'S'})
		tc := tls.Server(c, &tls.Config{Certificates: []tls.Certificate{cert}})
		buf := make([]byte, 16)
		if _, err := io.ReadFull(tc, buf); err != nil {
			got <- "after TLS: " + err.Error()
			return
		}
		var m pgproto3.CancelRequest
		if err := m.Decode(buf[4:]); err != nil {
			got <- err.Error()
			return
		}
		got <- cancelKey(m.ProcessID, m.SecretKey)
	}()
	if err := sendCancel(l.Addr().String(), &tls.Config{InsecureSkipVerify: true}, false, 77, []byte{1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	if k := <-got; k != "77/01020304" {
		t.Fatalf("server got %q, want the CancelRequest 77/01020304 inside TLS", k)
	}
	// a server answering N to the SSLRequest: no cancel in clear when the DSN requires TLS
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l2.Close()
	go func() {
		c, err := l2.Accept()
		if err == nil {
			_, _ = io.ReadFull(c, make([]byte, 8))
			_, _ = c.Write([]byte{'N'})
			_, _ = io.ReadAll(c)
			c.Close()
		}
	}()
	if err := sendCancel(l2.Addr().String(), &tls.Config{InsecureSkipVerify: true}, false, 77, []byte{1, 2, 3, 4}); err == nil {
		t.Fatal("a cancel went in clear to a server refusing TLS")
	}
}

// A long statement on the direct path holds back none of the cloud's asynchronous messages:
// a notification the cloud sends meanwhile reaches the client before the statement's answer
// (review of block 9, A).
func TestDirectAsyncNotHeld(t *testing.T) {
	d := directSession(t, false)
	d.org1.delayQuery = 800 * time.Millisecond
	sendAll(t, d.fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1"})
	time.Sleep(300 * time.Millisecond)
	d.cloud.notify("ch")
	first := ""
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m.(type) {
		case *pgproto3.NotificationResponse:
			if first == "" {
				first = "notification"
			}
		case *pgproto3.CommandComplete:
			if first == "" {
				first = "answer"
			}
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if first != "notification" {
		t.Fatalf("first to the client: %s, want the cloud's notification while the direct statement ran", first)
	}
}

// The gate resolves the tables by the statement's own names under the session's search_path;
// tables it does not find (the application's in another schema) send the statement through
// the cloud, and the log says so once (review of block 9, C).
func TestDirectGateUnresolved(t *testing.T) {
	d := directSession(t, false)
	d.cloud.unresolved = true
	for i := 0; i < 2; i++ {
		if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") {
			t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
		}
	}
	if n := strings.Count(d.logs.String(), "not found by its name under the session's search_path"); n != 1 {
		t.Fatalf("logged %d times, want once: %s", n, d.logs.String())
	}
}

// Django sets the time zone with set_config('TimeZone', $1, false) at every connection: the
// direct path stays on, and the direct connection gets the cloud session's value of it.
func TestDirectSetConfig(t *testing.T) {
	d := directSession(t, false)
	d.query(t, "SELECT set_config('TimeZone', 'UTC', false)")
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	org := d.org1.log()
	if len(org) != 2 || org[0] != "SELECT set_config('timezone', 'Asia/Tokyo', false)" || !strings.HasPrefix(org[1], "SELECT title") {
		t.Fatalf("direct connection ran %q (direct path off: %q)", org, d.s.direct.off)
	}
}

// The direct connection keeps the organization's schema as its search_path: the session's
// SET search_path (Laravel's at every connection) is not repeated there and does not turn the
// direct path off; a startup parameter off the white list does (coordinator's decision of
// 25.10).
func TestDirectSearchPathPinned(t *testing.T) {
	d := directSession(t, false)
	d.s.params = map[string]string{"search_path": "app"}
	d.query(t, `set search_path to "public"`)
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	if org := d.org1.log(); len(org) != 1 || !strings.HasPrefix(org[0], "SELECT title") || d.params["search_path"] != `"public"` {
		t.Fatalf("direct connection ran %q with search_path %q (off: %q)", org, d.params["search_path"], d.s.direct.off)
	}
	d = directSession(t, false)
	d.s.params = map[string]string{"row_security": "off"}
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
	if len(d.org1.log()) > 0 || d.dials > 0 || !strings.Contains(d.s.direct.off, "row_security") {
		t.Fatalf("direct path with row_security in the startup packet: %q, %d dials, off %q", d.org1.log(), d.dials, d.s.direct.off)
	}
}

// An operator of an extension the cloud has (pg_trgm's % in its public) is not found in the
// organization's database under its schema: the gate checks the statement's operators are the
// server's own, and the statement goes through the cloud instead of failing there.
func TestDirectExtensionOperator(t *testing.T) {
	d := directSession(t, false)
	d.query(t, "SELECT title FROM documents WHERE organization_id = 1 AND title % 'ab'")
	if len(d.org1.log()) != 1 {
		t.Fatalf("with no extension operator in the cloud the statement should go direct: %q", d.org1.log())
	}
	var gate string
	for _, q := range d.cloud.log() {
		if strings.Contains(q, "FROM lokadb.orgs WHERE org = 1)") {
			gate = q
		}
	}
	if !strings.Contains(gate, "o.oprname IN ('=', '%')") || !strings.Contains(gate, "d.deptype = 'e'") {
		t.Fatalf("gate %q lacks the operator check", gate)
	}
	d = directSession(t, false)
	d.cloud.extOp = "%"
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1 AND title % 'ab'"); code != "" || len(d.org1.log()) > 0 || !ranAny(d.cloud.log(), "documents__org1") {
		t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	d = directSession(t, false)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1 AND title OPERATOR(public.%) 'ab'"); code != "" || len(d.org1.log()) > 0 {
		t.Fatalf("a schema-qualified operator went direct: %q", d.org1.log())
	}
}

// Block 4 and 10: the direct path in a database with schemas. public.documents is the LokaDB
// table, app.documents a cloud table of the same name; organization 1's tables are in schema
// lokadb of its database. A bare name goes direct when the session's search path resolves it to
// the LokaDB table, and the organization's connection resolves it the same way (the session's
// path, public replaced by lokadb); public.documents as written does not go direct (there it
// is lokadb.documents); the gate names the table by its key.
func TestDirectSchemas(t *testing.T) {
	d := directSession(t, false, func(m *Meta) {
		m.SchemasUsed = true
		m.Rels = map[string][]string{"documents": {"app", "public"}, "document_versions": {"public"}}
		m.Schemas = map[string]string{"1": "lokadb"}
	})
	q := "SELECT title FROM documents WHERE organization_id = 1"
	d.s.initPath("public, app", "alice")
	if code := d.query(t, q); code != "" || !d.direct(q) {
		t.Fatalf("public first: %q, org1 %q", code, d.org1.log())
	}
	if d.params["search_path"] != `"lokadb", "app", "public"` {
		t.Errorf("the connection's search_path: %q", d.params["search_path"])
	}
	var gate string
	for _, c := range d.cloud.log() {
		if strings.Contains(c, "FROM lokadb.orgs WHERE org = 1)") {
			gate = c
		}
	}
	if !strings.Contains(gate, `to_regclass('public."documents"')`) {
		t.Errorf("the gate does not name the table by its key: %q", gate)
	}
	// app first: documents is the cloud's table
	n := len(d.org1.log())
	d.s.initPath("app, public", "alice")
	d.query(t, q)
	if len(d.org1.log()) != n {
		t.Fatalf("a cloud table of the same name went direct: %q", d.org1.log()[n:])
	}
	// back to public first: the connection's path follows before the statement
	d.s.initPath("public", "alice")
	if code := d.query(t, q); code != "" || !inOrder(d.org1.log()[n:], `SET search_path = "lokadb", "public"`, q) {
		t.Fatalf("the path did not follow: %q, org1 %q", code, d.org1.log()[n:])
	}
	n = len(d.org1.log())
	d.query(t, "SELECT title FROM public.documents WHERE organization_id = 1")
	if len(d.org1.log()) != n {
		t.Fatalf("public.documents went direct: %q", d.org1.log()[n:])
	}
	// the form decision itself resolves the names by the path given (check -profile, learning)
	if v := DirectIn(q, d.s.meta, false, []string{"app", "public"}); v.OK {
		t.Errorf("DirectIn with app first: %+v", v)
	}
	if !DirectTakesIn(q, d.s.meta, false, []string{"public", "app"}) || DirectTakesIn(q, d.s.meta, false, []string{"app", "public"}) {
		t.Error("DirectTakesIn does not follow the path")
	}
}

// countConn counts the writes on a connection (item 9: the exchanges the proxy sends).
type countConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countConn) Write(b []byte) (int, error) {
	c.n.Add(1)
	return c.Conn.Write(b)
}
