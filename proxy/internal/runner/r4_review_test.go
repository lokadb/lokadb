package runner

// Review R4, area runner: temporary tests (not part of the tree).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/seal"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// r4pass: the password of the scratch Postgres (R4_PGPASS, else r4).
func r4pass() string {
	if p := os.Getenv("R4_PGPASS"); p != "" {
		return p
	}
	return "r4"
}

// R4-2a: an embedded runner with a move running gets a changed policy from the panel: the poll does not
// apply it (busy); a check job that starts during the move (the panel's Check, or the next customer's
// move) applies it, the policy needs a new process, and r.Stop() ends the move in the middle.
func TestR4CheckDuringMoveEndsTheMove(t *testing.T) {
	r, f := newTest(t)
	r.st.Secret, r.PollWait = "lrs_secret", 10*time.Millisecond
	dir := useDir(t)
	if err := os.WriteFile(filepath.Join(dir, "proxy.json"), []byte(`{"listen": ":6432", "fanout": {"*": {"mode": "reject"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	embeddedDone.Store(true) // the proxy of this process runs
	t.Cleanup(func() { embeddedDone.Store(false) })
	db := newSwitchDB(t, "")
	r.Cfg.Embedded, r.Cfg.found = true, &Found{DBName: "app", User: "app"}
	r.Cfg.DB = "host=127.0.0.1 port=" + strconv.Itoa(db.port()) + " user=u password=p dbname=app sslmode=disable"
	r.ops = &Ops{Cfg: r.Cfg, Log: r.Log}
	ctx, stop := context.WithCancel(context.Background())
	var stopped atomic.Bool
	r.Stop = func() { stopped.Store(true); stop() }

	var polls atomic.Int32
	srv := httptest.NewServer(func() http.Handler {
		m := f.handler().(*http.ServeMux)
		m.HandleFunc("POST /v1/runner/poll", func(w http.ResponseWriter, req *http.Request) {
			n := polls.Add(1)
			res := cloudproto.PollResponse{Settings: &cloudproto.Settings{Fanout: "allow"}, Jobs: []cloudproto.Job{}}
			if n == 5 { // a few polls later: a check (the panel's button, or the next customer's move)
				res.Jobs = append(res.Jobs, cloudproto.Job{ID: "jb_check", Kind: cloudproto.KindCheck, Args: json.RawMessage(`{"org":0}`)})
			}
			time.Sleep(20 * time.Millisecond)
			json.NewEncoder(w).Encode(res)
		})
		return m
	}())
	t.Cleanup(srv.Close)
	r.Cfg.Service, r.HTTP = srv.URL, srv.Client()

	r.start(ctx, cloudproto.Job{ID: "jb_move", Kind: cloudproto.KindMove, Args: json.RawMessage(`{"org":7,"roles":{"app":"a","dml":"d"}}`)})
	waitFor(t, func() bool { h, _ := db.counts(); return h > 0 })
	done := make(chan struct{})
	go func() { _ = r.pollLoop(ctx); close(done) }()
	waitFor(t, func() bool { return polls.Load() >= 3 })
	b, _ := os.ReadFile(filepath.Join(dir, "proxy.json"))
	t.Logf("proxy.json after 3 polls with the new policy, the move running: %s", b)
	time.Sleep(time.Second)
	_, closed := db.counts()
	if stopped.Load() || closed > 0 {
		t.Fatalf("R4-2a: a check job started during a move applied the panel's policy, ended the process and cut the move (stopped %v, closed %d)", stopped.Load(), closed)
	}
	// the jobs end (taken off the list here: the held database of this test would not let them go): then, and not
	// before, the process restarts for the policy
	r.mu.Lock()
	pending := r.restartPending
	r.running = map[string]*job{}
	r.mu.Unlock()
	if !pending {
		t.Fatal("the restart for the new policy was not kept for later")
	}
	r.restartIfPending()
	if !stopped.Load() {
		t.Fatal("no restart once the jobs ended")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the poll loop did not end")
	}
}

// R4-2b: the same with "Apply my choices" (fanout.decide), which the service and the panel allow while a
// move runs: a decision about a table is written to proxy.json, the embedded runner ends the process.
// Needs a Postgres (R4_PG=host:port, user postgres, password r4, database postgres).
func TestR4DecideDuringMoveEndsTheMove(t *testing.T) {
	pg := os.Getenv("R4_PG")
	if pg == "" {
		t.Skip("R4_PG not set")
	}
	r, f := newTest(t)
	r.st.Secret = "lrs_secret"
	dir := useDir(t)
	if err := os.WriteFile(filepath.Join(dir, "proxy.json"), []byte(`{"listen": ":6432", "fanout": {"*": {"mode": "reject"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	embeddedDone.Store(true)
	t.Cleanup(func() { embeddedDone.Store(false) })
	db := newSwitchDB(t, pg)
	db.hang.Store(true) // the move's statement takes long
	r.Cfg.Embedded, r.Cfg.found = true, &Found{DBName: "app", User: "app"}
	r.Cfg.DB = "host=127.0.0.1 port=" + strconv.Itoa(db.port()) + " user=postgres password=" + r4pass() + " dbname=postgres sslmode=disable"
	r.ops = &Ops{Cfg: r.Cfg, Log: r.Log}
	ctx, stop := context.WithCancel(context.Background())
	var stopped atomic.Bool
	r.Stop = func() { stopped.Store(true); stop() }

	r.start(ctx, cloudproto.Job{ID: "jb_move", Kind: cloudproto.KindMove, Args: json.RawMessage(`{"org":7,"roles":{"app":"a","dml":"d"}}`)})
	waitFor(t, func() bool { h, _ := db.counts(); return h > 0 })
	db.hang.Store(false) // the decide's own connection reaches Postgres
	r.start(ctx, cloudproto.Job{ID: "jb_decide", Kind: cloudproto.KindDecide, Args: json.RawMessage(`{"decisions":[{"table":"public.audit_log","choice":"refuse"}]}`)})
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); _, ok := f.results["jb_decide"]; return ok })
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return len(r.running) == 1 }) // the decide is done, the move runs
	time.Sleep(200 * time.Millisecond)
	_, closed := db.counts()
	f.mu.Lock()
	dres := f.results["jb_decide"]
	f.mu.Unlock()
	if dres.Status != cloudproto.StatusOK {
		t.Fatalf("decide: %s %s", dres.Status, dres.Result)
	}
	if stopped.Load() || closed > 0 {
		t.Fatalf("R4-2b: a decide during a move ended the embedded runner's process and the move with it (stopped %v, closed %d)", stopped.Load(), closed)
	}
	// the move ends: now the process restarts for the choices
	r.mu.Lock()
	r.running["jb_move"].cancel()
	r.mu.Unlock()
	waitFor(t, func() bool { return stopped.Load() })
}

// R4-3: a manual runner (its proxy outside it, the directory's proxy.json shared, as the panel's compose
// and docker run lines mount it) applies the panel's settings to that file: with no public proxy chosen
// it deletes "tls" and "allow", the person's own TLS between the application and the proxy and its
// allow list; the next start of the proxy runs without them. The panel's default policy (log before the
// first Cloud move) also replaces the mode the person wrote.
func TestR4ManualRunnerStripsProxyTLS(t *testing.T) {
	r, _ := newTest(t)
	dir := useDir(t)
	own := `{
  "listen": ":6432",
  "upstream": "env:LOKADB_UPSTREAM",
  "control_plane": "file:/etc/lokadb/control-plane.dsn",
  "credentials_key": "/etc/lokadb/credentials.key",
  "tls": {"cert": "/certs/proxy.crt", "key": "/certs/proxy.key", "require": true},
  "allow": ["10.20.0.0/16"],
  "fanout": {"app": {"mode": "reject"}}
}`
	path := filepath.Join(dir, "proxy.json")
	if err := os.WriteFile(path, []byte(own), 0o600); err != nil {
		t.Fatal(err)
	}
	r.Cfg.Embedded = false
	r.Cfg.DB = "host=db port=5432 dbname=app user=admin sslmode=require"
	// what the service sends to a runner that has its own proxy and whose person chose no policy: none
	r.settings = &cloudproto.Settings{}
	r.applySettings()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	fi, _ := os.Stat(path)
	t.Logf("proxy.json after the panel's settings (mode %v):\n%s", fi.Mode().Perm(), b)
	if m["tls"] == nil || m["allow"] == nil {
		t.Errorf("R4-3: the manual runner removed the proxy's own tls/allow from %s", path)
	}
	var fan map[string]struct{ Mode string }
	_ = json.Unmarshal(m["fanout"], &fan)
	if fan["app"].Mode != "reject" {
		t.Errorf("R4-3: the person's fanout mode reject became %q (the service's default, not a choice)", fan["app"].Mode)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("R4-3: the file's rights %v, were 0600", fi.Mode().Perm())
	}
	// a policy the person chose in the panel is written, and still nothing else of the file changes
	r.settings = &cloudproto.Settings{Fanout: "reject-writes"}
	r.applySettings()
	b, _ = os.ReadFile(path)
	m = nil
	_ = json.Unmarshal(b, &m)
	fan = nil
	_ = json.Unmarshal(m["fanout"], &fan)
	fi, _ = os.Stat(path)
	if m["tls"] == nil || m["allow"] == nil || fan["app"].Mode != "reject-writes" || fi.Mode().Perm() != 0o600 {
		t.Errorf("R4-3: after a chosen policy: tls %s allow %s mode %q rights %v", m["tls"], m["allow"], fan["app"].Mode, fi.Mode().Perm())
	}
}

// R4-4: agent.prepare with fresh=true for an organization that is moved and served by its agent: the
// runner revokes that agent and seals a new enrollment token to whatever installer key the job names.
// Whoever controls the service gets a token for a moved organization and enrolls an agent of its own:
// the SaaS's connections to that customer (postgres_fdw, the direct path) then reach it.
func TestR4PrepareHandsAMovedOrgToAnyKey(t *testing.T) {
	pg := os.Getenv("R4_PG")
	if pg == "" {
		t.Skip("R4_PG not set")
	}
	host, port, _ := net.SplitHostPort(pg)
	dsn := "host=" + host + " port=" + port + " user=postgres password=" + r4pass() + " dbname=postgres sslmode=disable"
	ctx := context.Background()
	tun, err := tunnel.OpenPGStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	if cn0, err := pgx.Connect(ctx, "postgres://postgres:"+r4pass()+"@"+pg+"/postgres?sslmode=disable"); err == nil {
		_, _ = cn0.Exec(ctx, "DELETE FROM lokadb_cp.agent_certs WHERE serial = 's-real-7'") // a run before this one
		cn0.Close(ctx)
	}
	// org 7: moved, its agent enrolled and connected (a valid certificate), its port served
	if err := tun.RecordCert(ctx, "s-real-7", "7", time.Now().Add(365*24*time.Hour), "enroll", "203.0.113.7", ""); err != nil {
		t.Fatal(err)
	}
	if err := tun.SetEndpoint(ctx, "7", tunnel.Endpoint{Listen: ":15007"}); err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(t.TempDir(), "ca.crt")
	_ = os.WriteFile(ca, []byte("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"), 0o644)
	cfg := &Config{DB: dsn, Relay: Relay{CA: ca, PortMin: 15000, PortMax: 15999, Agent: "relay.example:7443"}}
	o := &Ops{Cfg: cfg, Tun: tun, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	// lokadb.orgs of the SaaS database lists organization 7 (it is moved); 8 is not
	cn, err := pgx.Connect(ctx, "postgres://postgres:"+r4pass()+"@"+pg+"/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close(ctx)
	for _, q := range []string{"CREATE SCHEMA IF NOT EXISTS lokadb", "CREATE TABLE IF NOT EXISTS lokadb.orgs (org bigint PRIMARY KEY, server text NOT NULL)",
		"INSERT INTO lokadb.orgs VALUES (7, 'lokadb_org7') ON CONFLICT DO NOTHING"} {
		if _, err := cn.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	attacker, _ := seal.NewKey()
	_, err = o.Prepare(ctx, cloudproto.PrepareArgs{Org: 7, InstallerKey: attacker.Public(), Fresh: true})
	var je *cloudproto.JobError
	if !errors.As(err, &je) || je.Code != "already_moved" {
		t.Errorf("R4-29: fresh prepare of a moved organization: %v", err)
	}
	_, realValid, _ := tun.CertValid(ctx, "s-real-7")
	if !realValid {
		t.Errorf("R4-29: the moved organization's agent was revoked")
	}
	// an organization that is not moved yet gets its token as before
	res, err := o.Prepare(ctx, cloudproto.PrepareArgs{Org: 8, InstallerKey: attacker.Public(), Fresh: true})
	if err != nil || res.EnrollTokenSealed == "" {
		t.Errorf("fresh prepare of an organization that is not moved: %v", err)
	}
}

// R4-5 (runner side): a decision "key" leaves the spec in the runner's settings too, so that a check or enable that
// starts before the next poll does not put the older spec back (the service saves it as well: its own test).
func TestR4DecideKeepsItsSpecInTheSettings(t *testing.T) {
	r, _ := newTest(t)
	dir := useDir(t)
	old := `{"tables":[{"name":"documents","tenant_column":"org_id"}]}`
	decided := `{"tables":[{"name":"documents","tenant_column":"org_id"}],"routing":{"keys":[{"table":"documents","column":"slug"}]}}`
	r.settings = &cloudproto.Settings{Spec: json.RawMessage(old)}
	dr, _ := json.Marshal(cloudproto.DecideResult{Applied: []string{"table:documents"}, Spec: json.RawMessage(decided)})
	r.keepDecidedSpec(cloudproto.Result{Status: cloudproto.StatusOK, Result: dr})
	if err := os.WriteFile(filepath.Join(dir, "spec.json"), []byte(decided+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal([]byte(decided), &r.Cfg.spec)
	r.applySettings()
	if r.Cfg.spec.Routing == nil {
		t.Errorf("R4-5: the decided key documents.slug is gone from the runner's spec after the settings were applied")
	}
}
