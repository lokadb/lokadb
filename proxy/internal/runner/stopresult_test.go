package runner

// Review R4-15 (runner's side): a job the runner's stop ends sends its result (interrupted), or
// stays in runner.json for the next start to report; it no longer vanishes from both.

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

// switchDB: a TCP listener in front of Postgres. While hang is set, a connection is accepted and
// never answered (a statement that takes long: a move's copy); closed records the ones the client
// gave up. Otherwise it forwards to upstream (empty: none).
type switchDB struct {
	ln       net.Listener
	hang     atomic.Bool
	upstream string
	mu       sync.Mutex
	held     int
	closed   int
}

func newSwitchDB(t *testing.T, upstream string) *switchDB {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &switchDB{ln: ln, upstream: upstream}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if s.hang.Load() || s.upstream == "" {
				s.mu.Lock()
				s.held++
				s.mu.Unlock()
				go func() {
					_, _ = io.Copy(io.Discard, c) // until the client closes
					s.mu.Lock()
					s.closed++
					s.mu.Unlock()
					c.Close()
				}()
				continue
			}
			go func() {
				u, err := net.Dial("tcp", s.upstream)
				if err != nil {
					c.Close()
					return
				}
				go func() { _, _ = io.Copy(u, c); u.Close() }()
				_, _ = io.Copy(c, u)
				c.Close()
			}()
		}
	}()
	return s
}

func (s *switchDB) port() int { return s.ln.Addr().(*net.TCPAddr).Port }

func (s *switchDB) counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held, s.closed
}

func readRunning(t *testing.T, r *Runner) []string {
	t.Helper()
	b, err := os.ReadFile(r.statePath())
	if err != nil {
		t.Fatal(err)
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		t.Fatal(err)
	}
	return st.Running
}

// A graceful stop (SIGTERM, an update, a rollout) while a move runs: the move was cancelled, its
// result never sent (the cancelled ctx), and runner.json forgot it: the service waited for good.
func TestStopSendsTheResult(t *testing.T) {
	r, f := newTest(t)
	r.st.Secret = "lrs_secret"
	db := newSwitchDB(t, "")
	r.Cfg.DB = "host=127.0.0.1 port=" + strconv.Itoa(db.port()) + " user=u password=p dbname=app sslmode=disable"
	r.ops = &Ops{Cfg: r.Cfg, Log: r.Log}
	ctx, stop := context.WithCancel(context.Background())
	r.start(ctx, cloudproto.Job{ID: "jb_move", Kind: cloudproto.KindMove, Args: json.RawMessage(`{"org":7,"roles":{"app":"a","dml":"d"}}`)})
	waitFor(t, func() bool { h, _ := db.counts(); return h > 0 }) // the move works on the database
	stop()                                                        // SIGTERM
	r.jobs.Wait()
	f.mu.Lock()
	res, sent := f.results["jb_move"]
	f.mu.Unlock()
	run := readRunning(t, r)
	if !sent || res.Error == nil || res.Error.Code != "interrupted" {
		t.Errorf("the result of the move the stop ended: sent %v, %+v (want interrupted)", sent, res.Error)
	}
	if sent && len(run) != 0 {
		t.Errorf("runner.json still lists the job its result went for: %v", run)
	}
}

// An embedded runner holding a setting that needs a new process: Enable applies it first and ends
// its own process; the enable's result was never sent and runner.json forgot it (job_busy on the
// service for good).
func TestEnableEndingItsProcessSendsItsResult(t *testing.T) {
	r, f := newTest(t)
	r.st.Secret = "lrs_secret"
	dir := useDir(t)
	if err := os.WriteFile(filepath.Join(dir, "proxy.json"), []byte(`{"listen": ":6432", "fanout": {"*": {"mode": "reject"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	embeddedDone.Store(true)
	t.Cleanup(func() { embeddedDone.Store(false) })
	db := newSwitchDB(t, "")
	r.Cfg.Embedded, r.Cfg.found = true, &Found{DBName: "app", User: "app"}
	r.Cfg.DB = "host=127.0.0.1 port=" + strconv.Itoa(db.port()) + " user=u password=p dbname=app sslmode=disable"
	r.Cfg.spec.Tables = []admin.SpecTable{{Name: "documents", TenantColumn: "org_id"}}
	r.ops = &Ops{Cfg: r.Cfg, Log: r.Log}
	r.settings = &cloudproto.Settings{Fanout: "reject-writes"} // arrived while another job ran: kept, not applied
	ctx, stop := context.WithCancel(context.Background())
	var stopped atomic.Bool
	r.Stop = func() { stopped.Store(true); stop() }
	r.start(ctx, cloudproto.Job{ID: "jb_enable", Kind: cloudproto.KindEnable, Args: json.RawMessage(`{"confirm_db":"app"}`)})
	// the restart for the setting waits for the job (review 4, R4-16); the runner is stopped from outside instead
	time.Sleep(300 * time.Millisecond)
	if stopped.Load() {
		t.Errorf("enable ended its own process in the middle of the job")
	}
	stop()
	r.jobs.Wait()
	f.mu.Lock()
	_, sent := f.results["jb_enable"]
	f.mu.Unlock()
	run := readRunning(t, r)
	if !sent && len(run) == 0 {
		t.Errorf("enable ended its own process (Stop called: %v), its result never reached the service and runner.json forgot it", stopped.Load())
	}
}
