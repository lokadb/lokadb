package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/seal"
)

// fakeService: the service's runner endpoints, recording what the runner sends.
type fakeService struct {
	mu      sync.Mutex
	reg     cloudproto.RegisterRequest
	events  []cloudproto.Event
	results map[string]cloudproto.Result
	auth    []string
	cancel  bool
	failEv  int // the events endpoint fails this many times first
}

func (f *fakeService) handler() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /v1/runner/register", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		_ = json.NewDecoder(r.Body).Decode(&f.reg)
		f.auth = append(f.auth, r.Header.Get("Authorization"))
		json.NewEncoder(w).Encode(cloudproto.RegisterResponse{RunnerID: "rn_1", Secret: "lrs_secret", Account: "acc1", Fingerprint: "SHA256:x"})
	})
	m.HandleFunc("POST /v1/runner/jobs/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.failEv > 0 {
			f.failEv--
			http.Error(w, "busy", 503)
			return
		}
		var b cloudproto.EventsRequest
		_ = json.NewDecoder(r.Body).Decode(&b)
		f.events = append(f.events, b.Events...)
		json.NewEncoder(w).Encode(cloudproto.EventsResponse{Cancel: f.cancel})
	})
	m.HandleFunc("POST /v1/runner/jobs/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		var b cloudproto.Result
		_ = json.NewDecoder(r.Body).Decode(&b)
		f.results[r.PathValue("id")] = b
		f.auth = append(f.auth, r.Header.Get("Authorization")+" "+r.Header.Get(cloudproto.RunnerHeader))
		w.WriteHeader(204)
	})
	return m
}

func newTest(t *testing.T) (*Runner, *fakeService) {
	f := &fakeService{results: map[string]cloudproto.Result{}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	r := &Runner{Cfg: &Config{Service: srv.URL, State: t.TempDir(), Name: "test", Token: "lrr_token"}, Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Version: "0.9.0", HTTP: srv.Client(), running: map[string]*job{}}
	return r, f
}

func TestRegister(t *testing.T) {
	r, f := newTest(t)
	if err := r.register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.reg.Protocol != cloudproto.RunnerProtocol || f.reg.Name != "test" || len(f.reg.Capabilities) == 0 || f.auth[0] != "Bearer lrr_token" {
		t.Fatalf("register: %+v %v", f.reg, f.auth)
	}
	b, err := os.ReadFile(filepath.Join(r.Cfg.State, "runner.json"))
	if err != nil {
		t.Fatal(err)
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil || st.Secret != "lrs_secret" || st.RunnerID != "rn_1" {
		t.Fatalf("state: %+v %v", st, err)
	}
	k, err := seal.ParseKey(st.Key)
	if err != nil || k.Public() != f.reg.PublicKey {
		t.Fatal("the stored key is not the registered one")
	}
	if fi, _ := os.Stat(filepath.Join(r.Cfg.State, "runner.json")); fi.Mode().Perm()&0o077 != 0 && os.PathSeparator == '/' {
		t.Fatalf("runner.json mode %v", fi.Mode())
	}
}

// TestJobFlow: an unknown kind is answered unsupported; events go out redacted and in order
// (retried when the service fails), a cancel in their answer cancels the job, the result is sent.
func TestJobFlow(t *testing.T) {
	r, f := newTest(t)
	r.st.Secret = "lrs_secret"
	ctx := context.Background()

	r.start(ctx, cloudproto.Job{ID: "jb_1", Kind: "something.new", V: 9})
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); _, ok := f.results["jb_1"]; return ok })
	if res := f.results["jb_1"]; res.Status != cloudproto.StatusUnsupported || !strings.Contains(res.Error.Message, "update it") {
		t.Fatalf("unknown kind: %+v", res)
	}
	if !strings.HasPrefix(f.auth[len(f.auth)-1], "Bearer lrs_secret 1") {
		t.Fatalf("auth: %v", f.auth)
	}

	// events: through the slog handler, redacted; the service fails twice first
	f.failEv, f.cancel = 2, true
	ev := newEvents()
	jctx, cancel := context.WithCancel(ctx)
	rj := &job{j: cloudproto.Job{ID: "jb_2"}, cancel: cancel, ev: ev}
	log := eventLogger(ev, r.Log)
	log.Info("copying", "dsn", "host=x password=secret user=y")
	log.Debug("not sent")
	log.Warn("slow", "table", "documents")
	done := make(chan struct{})
	close(done)
	r.flushEvents(ctx, rj, done)
	if len(f.events) != 2 || f.events[0].Msg != "copying" || f.events[0].Seq != 1 || f.events[1].Kind != "warning" ||
		strings.Contains(f.events[0].Attrs["dsn"].(string), "secret") {
		t.Fatalf("events: %+v", f.events)
	}
	select {
	case <-jctx.Done():
	default:
		t.Fatal("a cancel in the events' answer did not cancel the job")
	}
	if len(ev.pending(10)) != 0 {
		t.Fatal("events left unacknowledged")
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"host=relay port=15001 user=lokadb_app password=abc sslmode=prefer": "host=relay port=15001 user=lokadb_app password=*** sslmode=prefer",
		"postgres://u:secret@h:5432/db":                                     "postgres://u:***@h:5432/db",
		"no secret here":                                                    "no secret here",
	} {
		if got := Redact(in); got != want {
			t.Errorf("Redact(%q) = %q, want %q", in, got, want)
		}
	}
}

func waitFor(t *testing.T, ok func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out")
}

// TestUntilReady: a runner whose config has a problem polls with it and takes no job; once the
// problem is fixed it polls without problems and goes on.
func TestUntilReady(t *testing.T) {
	r, f := newTest(t)
	r.st.Secret, r.Recheck, r.PollWait = "lrs_secret", 50*time.Millisecond, 10*time.Millisecond
	var polls []cloudproto.PollRequest
	srv := httptest.NewServer(func() http.Handler {
		m := f.handler().(*http.ServeMux)
		m.HandleFunc("POST /v1/runner/poll", func(w http.ResponseWriter, req *http.Request) {
			var b cloudproto.PollRequest
			_ = json.NewDecoder(req.Body).Decode(&b)
			f.mu.Lock()
			polls = append(polls, b)
			f.mu.Unlock()
			// a job the service should not have given: the runner refuses it
			json.NewEncoder(w).Encode(cloudproto.PollResponse{Jobs: []cloudproto.Job{{ID: "jb_9", Kind: cloudproto.KindInventory}}})
		})
		return m
	}())
	t.Cleanup(srv.Close)
	r.Cfg.Service, r.HTTP = srv.URL, srv.Client()
	spec := filepath.Join(t.TempDir(), "spec.json")
	r.check = func(context.Context) []cloudproto.Problem {
		if _, err := os.Stat(spec); err != nil {
			return []cloudproto.Problem{{Key: "spec", Message: "spec: " + spec + ": no such file", Path: spec}}
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- r.untilReady(context.Background()) }()
	waitFor(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(polls) > 0 && f.results["jb_9"].Error != nil })
	f.mu.Lock()
	if p := polls[0].Problems; len(p) != 1 || p[0].Key != "spec" || f.results["jb_9"].Error.Code != "not_ready" {
		t.Fatalf("problems %+v, job %+v", p, f.results["jb_9"])
	}
	f.mu.Unlock()
	os.WriteFile(spec, []byte("{}"), 0o644)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not ready after the file appeared")
	}
	if len(r.problems) != 0 {
		t.Fatalf("problems left: %+v", r.problems)
	}
}

// A runner whose identity the service refuses (it was removed in the panel, the container was made
// again): an embedded one with a new token in its environment registers again, replacing runner.json
// only; without one it says what to do, waits and tries again, and does not end.
func TestRevokedIdentity(t *testing.T) {
	setup := func(t *testing.T, token string) (*Runner, *fakeService, *strings.Builder, func() int) {
		r, f := newTest(t)
		var logs strings.Builder
		var mu sync.Mutex
		r.Log = slog.New(slog.NewTextHandler(&lockedWriter{w: &logs, mu: &mu}, nil))
		r.Cfg.Embedded, r.Cfg.Token = true, token
		r.PollWait, r.RevokedWait, r.Recheck = 10*time.Millisecond, 20*time.Millisecond, 50*time.Millisecond
		r.recheckNow = make(chan struct{}, 1)
		r.st = State{RunnerID: "rn_old", Secret: "lrs_old", Key: "old", Service: r.Cfg.Service, TokenHash: tokenHash("lrr_spent")}
		if err := r.saveState(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(r.Cfg.State, "control-plane.dsn"), []byte("kept"), 0o600); err != nil {
			t.Fatal(err)
		}
		var fresh, refused int
		srv := httptest.NewServer(func() http.Handler {
			m := f.handler().(*http.ServeMux)
			m.HandleFunc("POST /v1/runner/poll", func(w http.ResponseWriter, req *http.Request) {
				f.mu.Lock()
				defer f.mu.Unlock()
				if req.Header.Get("Authorization") != "Bearer lrs_secret" {
					refused++
					http.Error(w, `{"error":{"code":"unauthorized"}}`, http.StatusUnauthorized)
					return
				}
				fresh++
				json.NewEncoder(w).Encode(cloudproto.PollResponse{})
			})
			return m
		}())
		t.Cleanup(srv.Close)
		r.Cfg.Service, r.HTTP = srv.URL, srv.Client()
		r.st.Service = srv.URL
		_ = r.saveState()
		return r, f, &logs, func() int { f.mu.Lock(); defer f.mu.Unlock(); return fresh*1000 + refused }
	}
	t.Run("a new token registers again and keeps the files", func(t *testing.T) {
		r, f, _, counts := setup(t, "lrr_new")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- r.pollLoop(ctx) }()
		waitFor(t, func() bool { return counts()/1000 > 0 })
		cancel()
		<-done
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.auth) == 0 || f.auth[0] != "Bearer lrr_new" {
			t.Fatalf("registered with %v", f.auth)
		}
		b, _ := os.ReadFile(filepath.Join(r.Cfg.State, "runner.json"))
		var st State
		if json.Unmarshal(b, &st) != nil || st.RunnerID != "rn_1" || st.Secret != "lrs_secret" || st.TokenHash != tokenHash("lrr_new") {
			t.Fatalf("runner.json: %s", b)
		}
		if k, _ := os.ReadFile(filepath.Join(r.Cfg.State, "control-plane.dsn")); string(k) != "kept" {
			t.Fatal("a provisioned file was touched")
		}
	})
	t.Run("the spent token is not tried again; the message says what to do; it waits and does not end", func(t *testing.T) {
		r, f, logs, counts := setup(t, "lrr_spent")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.pollLoop(ctx) }()
		waitFor(t, func() bool { return counts()%1000 >= 3 })
		select {
		case err := <-done:
			t.Fatalf("the loop ended: %v", err)
		default:
		}
		cancel()
		<-done
		f.mu.Lock()
		defer f.mu.Unlock()
		if len(f.auth) != 0 {
			t.Fatalf("a spent token was sent: %v", f.auth)
		}
		for _, want := range []string{"removed from the panel", "LOKADB_TOKEN", "your files are kept"} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("the message lacks %q: %s", want, logs.String())
			}
		}
	})
	t.Run("the manual runner names its own variable and file", func(t *testing.T) {
		r, _, _, _ := setup(t, "")
		r.Cfg.Embedded = false
		if m := r.revokedMessage(); !strings.Contains(m, "LOKADB_RUNNER_TOKEN") || !strings.Contains(m, "runner.json") || strings.Contains(m, "LOKADB_TOKEN ") {
			t.Fatalf("message: %s", m)
		}
	})
}

type lockedWriter struct {
	w  *strings.Builder
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
