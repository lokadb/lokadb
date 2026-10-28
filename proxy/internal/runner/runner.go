package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/seal"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// lockKey: the advisory lock a working runner holds in the SaaS database, so that a second
// runner pointed at it by mistake waits instead of moving at the same time ("lokarun").
const lockKey = 0x6c6f6b6172756e

// State: what the runner keeps in its volume (runner.json, mode 600).
type State struct {
	RunnerID string `json:"runner_id"`
	Secret   string `json:"secret"`
	Key      string `json:"key"` // seal private key
	Service  string `json:"service"`
	// Running: the jobs that were running; after a restart they are reported interrupted.
	Running []string `json:"running"`
}

// Runner: lokadb runner.
type Runner struct {
	Cfg     *Config
	Log     *slog.Logger
	Version string
	HTTP    *http.Client
	// PollWait: how long the service may hold a poll (it answers within 25 s).
	PollWait time.Duration

	ops     *Ops
	started time.Time

	mu        sync.Mutex
	st        State
	running   map[string]*job
	exclusive sync.Mutex // move, move_back, enable: one at a time
	lastErr   string
	invHash   string
	interval  time.Duration
	inv       *cloudproto.Inventory
}

type job struct {
	j      cloudproto.Job
	cancel context.CancelFunc
	ev     *events
}

// ErrRevoked: the service refused the runner's secret (removed in the panel, or registered again).
var ErrRevoked = errors.New("the service refused this runner's secret: it was removed or registered again; register it with a new token (LOKADB_RUNNER_TOKEN) after deleting runner.json")

func (r *Runner) statePath() string { return filepath.Join(r.Cfg.State, "runner.json") }

func (r *Runner) loadState() (bool, error) {
	b, err := os.ReadFile(r.statePath())
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, json.Unmarshal(b, &r.st)
}

// saveState writes runner.json atomically; the caller holds r.mu.
func (r *Runner) saveState() error {
	b, err := json.MarshalIndent(r.st, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(r.Cfg.State, 0o700); err != nil {
		return err
	}
	tmp := r.statePath() + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, r.statePath())
}

// Run registers if it has to, takes the SaaS database's runner lock, recovers what a previous
// run left, then polls for jobs and sends status until ctx ends.
func (r *Runner) Run(ctx context.Context) error {
	r.started = time.Now()
	r.running = map[string]*job{}
	r.interval = 30 * time.Second
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 60 * time.Second}
	}
	if r.PollWait == 0 {
		r.PollWait = 25 * time.Second
	}
	have, err := r.loadState()
	if err != nil {
		return fmt.Errorf("%s: %w", r.statePath(), err)
	}
	if !have || r.st.Service != r.Cfg.Service {
		if err := r.register(ctx); err != nil {
			return err
		}
	}
	key, err := seal.ParseKey(r.st.Key)
	if err != nil {
		return err
	}
	fp, _ := seal.Fingerprint(key.Public())
	r.Log.Info("runner", "id", r.st.RunnerID, "service", r.Cfg.Service, "key", fp)
	tun, err := tunnel.OpenPGStore(ctx, r.Cfg.ControlPlane)
	if err != nil {
		return fmt.Errorf("control plane: %w", err)
	}
	defer tun.Close()
	r.ops = &Ops{Cfg: r.Cfg, Tun: tun, Key: key, Log: r.Log}

	unlock, err := r.lock(ctx)
	if err != nil {
		return err
	}
	if unlock == nil {
		return nil // ctx ended while waiting
	}
	defer unlock()
	r.recover(ctx)

	var wg sync.WaitGroup
	if r.Cfg.statusOn() {
		wg.Add(1)
		go func() { defer wg.Done(); r.statusLoop(ctx) }()
	}
	err = r.pollLoop(ctx)
	r.mu.Lock()
	for _, j := range r.running {
		j.cancel()
	}
	r.mu.Unlock()
	wg.Wait()
	return err
}

func (r *Runner) register(ctx context.Context) error {
	if r.Cfg.Token == "" {
		return fmt.Errorf("no %s: give the one-time registration token (lrr_…) from the panel in LOKADB_RUNNER_TOKEN", r.statePath())
	}
	key, err := seal.NewKey()
	if err != nil {
		return err
	}
	host, _ := os.Hostname()
	var res cloudproto.RegisterResponse
	if _, err := r.call(ctx, r.Cfg.Token, "/v1/runner/register", cloudproto.RegisterRequest{Name: r.Cfg.Name, Hostname: host, Version: r.Version,
		Protocol: cloudproto.RunnerProtocol, PublicKey: key.Public(), Capabilities: cloudproto.Kinds}, &res); err != nil {
		return fmt.Errorf("registration: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.st = State{RunnerID: res.RunnerID, Secret: res.Secret, Key: key.Private(), Service: r.Cfg.Service}
	if err := r.saveState(); err != nil {
		return err
	}
	r.Log.Info("registered", "runner", res.RunnerID, "account", res.Account, "key", res.Fingerprint)
	return nil
}

// lock: the SaaS database's runner lock, held on a connection of its own until unlock.
func (r *Runner) lock(ctx context.Context) (func(), error) {
	for {
		conn, err := pgx.Connect(ctx, r.Cfg.DB)
		if err != nil {
			return nil, fmt.Errorf("SaaS database: %w", err)
		}
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, int64(lockKey)).Scan(&got); err != nil {
			conn.Close(context.Background())
			return nil, err
		}
		if got {
			return func() { conn.Close(context.Background()) }, nil
		}
		conn.Close(context.Background())
		r.Log.Warn("another runner works on this database: waiting (standby)")
		select {
		case <-ctx.Done():
			return nil, nil
		case <-time.After(10 * time.Second):
		}
	}
}

// recover: move-cleanup, then the jobs the previous run was running are reported interrupted
// with the inventory, from which the service settles their moves (DESIGN §4.4).
func (r *Runner) recover(ctx context.Context) {
	if left, err := r.ops.Cleanup(ctx); err != nil {
		r.Log.Warn("move-cleanup", "err", err)
	} else if len(left) > 0 {
		r.Log.Info("move-cleanup removed what interrupted moves left", "what", left)
	}
	inv, err := r.ops.Inventory(ctx)
	if err != nil {
		r.Log.Error("inventory", "err", err)
	} else {
		r.sendInventory(ctx, &inv)
	}
	r.mu.Lock()
	ids := append([]string{}, r.st.Running...)
	r.mu.Unlock()
	for _, id := range ids {
		res := cloudproto.Result{Status: cloudproto.StatusFailed, Error: &cloudproto.JobError{Code: "interrupted",
			Message: "the runner restarted while the job ran"}}
		if err == nil {
			res.Result, _ = json.Marshal(inv)
		}
		r.postResult(ctx, id, res)
		r.Log.Warn("reported interrupted", "job", id)
	}
	r.mu.Lock()
	r.st.Running = nil
	_ = r.saveState()
	r.mu.Unlock()
}

func (r *Runner) sendInventory(ctx context.Context, inv *cloudproto.Inventory) {
	if _, err := r.call(ctx, "", "/v1/runner/inventory", inv, nil); err != nil {
		r.Log.Warn("sending the inventory", "err", err)
		return
	}
	r.mu.Lock()
	r.invHash, r.inv = inv.Hash, inv
	r.mu.Unlock()
}

func (r *Runner) pollLoop(ctx context.Context) error {
	backoff := time.Second
	for ctx.Err() == nil {
		r.mu.Lock()
		req := cloudproto.PollRequest{Version: r.Version, InventoryHash: r.invHash, Running: []string{}}
		for id := range r.running {
			req.Running = append(req.Running, id)
		}
		r.mu.Unlock()
		sort.Strings(req.Running)
		var res cloudproto.PollResponse
		pctx, cancel := context.WithTimeout(ctx, r.PollWait+30*time.Second)
		code, err := r.call(pctx, "", "/v1/runner/poll", req, &res)
		cancel()
		switch {
		case ctx.Err() != nil:
			return nil
		case code == http.StatusUnauthorized:
			return ErrRevoked
		case code == http.StatusUpgradeRequired:
			r.Log.Error("the service no longer speaks this runner's protocol: update the runner", "err", err)
			sleep(ctx, time.Minute)
			continue
		case err != nil:
			r.Log.Warn("poll", "err", err, "retry_in", backoff)
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
			continue
		}
		backoff = time.Second
		if res.StatusIntervalS > 0 {
			r.mu.Lock()
			r.interval = time.Duration(res.StatusIntervalS) * time.Second
			r.mu.Unlock()
		}
		if res.MinVersion != "" && res.LatestVersion != "" && res.LatestVersion != r.Version {
			r.Log.Info("a newer runner is out", "latest", res.LatestVersion)
		}
		for _, id := range res.Cancel {
			r.mu.Lock()
			if j := r.running[id]; j != nil {
				r.Log.Info("cancelling", "job", id)
				j.cancel()
			}
			r.mu.Unlock()
		}
		if res.WantInventory {
			if inv, err := r.ops.Inventory(ctx); err == nil {
				r.sendInventory(ctx, &inv)
			}
		}
		for _, j := range res.Jobs {
			r.start(ctx, j)
		}
	}
	return nil
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (r *Runner) start(ctx context.Context, j cloudproto.Job) {
	r.mu.Lock()
	if r.running[j.ID] != nil {
		r.mu.Unlock()
		return
	}
	jctx, cancel := context.WithCancel(ctx)
	rj := &job{j: j, cancel: cancel, ev: newEvents()}
	r.running[j.ID] = rj
	r.st.Running = append(r.st.Running, j.ID)
	_ = r.saveState()
	r.mu.Unlock()
	go r.runJob(ctx, jctx, rj)
}

func (r *Runner) runJob(ctx, jctx context.Context, rj *job) {
	defer rj.cancel()
	log := r.Log.With("job", rj.j.ID, "kind", rj.j.Kind)
	done := make(chan struct{})
	flushed := make(chan struct{})
	go func() { defer close(flushed); r.flushEvents(ctx, rj, done) }()
	log.Info("job started", "attempt", rj.j.Attempt)
	res := r.do(jctx, rj, eventLogger(rj.ev, log))
	close(done)
	<-flushed
	if res.Error != nil {
		log.Warn("job ended", "status", res.Status, "err", res.Error.Message)
		r.mu.Lock()
		r.lastErr = res.Error.Message
		r.mu.Unlock()
	} else {
		log.Info("job ended", "status", res.Status)
	}
	r.postResult(ctx, rj.j.ID, res)
	r.mu.Lock()
	delete(r.running, rj.j.ID)
	for i, id := range r.st.Running {
		if id == rj.j.ID {
			r.st.Running = append(r.st.Running[:i], r.st.Running[i+1:]...)
			break
		}
	}
	_ = r.saveState()
	r.mu.Unlock()
	if rj.j.Kind == cloudproto.KindMove || rj.j.Kind == cloudproto.KindMoveBack || rj.j.Kind == cloudproto.KindEnable {
		if inv, err := r.ops.Inventory(ctx); err == nil {
			r.sendInventory(ctx, &inv)
		}
	}
}

// do runs the job and makes its result.
func (r *Runner) do(ctx context.Context, rj *job, log *slog.Logger) cloudproto.Result {
	j := rj.j
	if cloudproto.Exclusive[j.Kind] {
		r.exclusive.Lock()
		defer r.exclusive.Unlock()
	}
	var out any
	var err error
	decode := func(v any) bool {
		if e := json.Unmarshal(j.Args, v); e != nil {
			err = &cloudproto.JobError{Code: "args", Message: "the job's arguments: " + e.Error()}
			return false
		}
		return true
	}
	switch j.Kind {
	case cloudproto.KindInventory:
		out, err = r.ops.Inventory(ctx)
	case cloudproto.KindLookup:
		var a cloudproto.LookupArgs
		if decode(&a) {
			out, err = r.ops.Lookup(ctx, a)
		}
	case cloudproto.KindCheck:
		var a cloudproto.CheckArgs
		if decode(&a) {
			out, err = r.ops.Check(ctx, a)
		}
	case cloudproto.KindEnable:
		var a cloudproto.EnableArgs
		if decode(&a) {
			err = r.ops.Enable(ctx, a, log)
		}
	case cloudproto.KindPrepare:
		var a cloudproto.PrepareArgs
		if decode(&a) {
			out, err = r.ops.Prepare(ctx, a)
		}
	case cloudproto.KindCredentials:
		var a cloudproto.CredentialsArgs
		if decode(&a) {
			err = r.ops.StoreCredentials(ctx, a)
		}
	case cloudproto.KindMove:
		var a cloudproto.MoveArgs
		if decode(&a) {
			out, err = r.ops.Move(ctx, a, log)
		}
	case cloudproto.KindMoveBack:
		var a cloudproto.OrgArgs
		if decode(&a) {
			err = r.ops.MoveBack(ctx, a, log)
		}
	case cloudproto.KindRelease:
		var a cloudproto.OrgArgs
		if decode(&a) {
			err = r.ops.Release(ctx, a)
		}
	case cloudproto.KindExtSync:
		var a cloudproto.OrgArgs
		if decode(&a) {
			out, err = r.ops.ExtSync(ctx, a)
		}
	default:
		return cloudproto.Result{Status: cloudproto.StatusUnsupported, Error: &cloudproto.JobError{Code: "unsupported",
			Message: fmt.Sprintf("this runner (%s) does not know jobs of kind %q: update it", r.Version, j.Kind)}}
	}
	if err != nil {
		var je *cloudproto.JobError
		switch {
		case errors.As(err, &je):
			je.Message = Redact(je.Message)
			return cloudproto.Result{Status: cloudproto.StatusFailed, Error: je}
		case ctx.Err() != nil:
			return cloudproto.Result{Status: cloudproto.StatusCancelled, Error: &cloudproto.JobError{Code: "cancelled", Message: Redact(err.Error())}}
		}
		return cloudproto.Result{Status: cloudproto.StatusFailed, Error: &cloudproto.JobError{Code: "error", Message: Redact(err.Error())}}
	}
	res := cloudproto.Result{Status: cloudproto.StatusOK}
	if out != nil {
		res.Result, _ = json.Marshal(out)
	}
	return res
}

// flushEvents sends the job's events as they come, until done and all are acknowledged;
// a cancel in the answer cancels the job.
func (r *Runner) flushEvents(ctx context.Context, rj *job, done <-chan struct{}) {
	backoff := time.Second
	for {
		evs := rj.ev.pending(200)
		if len(evs) > 0 {
			var res cloudproto.EventsResponse
			code, err := r.call(ctx, "", "/v1/runner/jobs/"+rj.j.ID+"/events", cloudproto.EventsRequest{Events: evs}, &res)
			switch {
			case err == nil:
				rj.ev.ack(evs[len(evs)-1].Seq)
				backoff = time.Second
				if res.Cancel {
					rj.cancel()
				}
				continue
			case code >= 400 && code < 500 && code != http.StatusTooManyRequests:
				r.Log.Warn("the service refused the job's events: dropped", "job", rj.j.ID, "err", err)
				rj.ev.ack(evs[len(evs)-1].Seq)
				continue
			case ctx.Err() != nil:
				return
			}
			sleep(ctx, backoff)
			backoff = min(backoff*2, time.Minute)
			continue
		}
		select {
		case <-done:
			if len(rj.ev.pending(1)) == 0 {
				return
			}
		case <-rj.ev.wake:
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
	}
}

// postResult sends a job's result until the service takes it (or refuses it for good).
func (r *Runner) postResult(ctx context.Context, id string, res cloudproto.Result) {
	backoff := time.Second
	for {
		code, err := r.call(ctx, "", "/v1/runner/jobs/"+id+"/result", res, nil)
		if err == nil || (code >= 400 && code < 500 && code != http.StatusTooManyRequests) || ctx.Err() != nil {
			if err != nil && ctx.Err() == nil {
				r.Log.Warn("the service refused the job's result", "job", id, "err", err)
			}
			return
		}
		r.Log.Warn("sending the result", "job", id, "err", err, "retry_in", backoff)
		sleep(ctx, backoff)
		backoff = min(backoff*2, time.Minute)
	}
}

// call POSTs in as JSON to the service and decodes the answer into out. bearer "" is the
// runner's secret. A non-2xx answer is an error carrying the service's message.
func (r *Runner) call(ctx context.Context, bearer, path string, in, out any) (int, error) {
	if bearer == "" {
		r.mu.Lock()
		bearer = r.st.Secret
		r.mu.Unlock()
	}
	b, err := json.Marshal(in)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Cfg.Service+path, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set(cloudproto.RunnerHeader, fmt.Sprint(cloudproto.RunnerProtocol))
	req.Header.Set("User-Agent", "lokadb-runner/"+r.Version)
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
			Hint  string `json:"hint"`
		}
		_ = json.Unmarshal(body, &e)
		msg := e.Error
		if msg == "" {
			msg = http.StatusText(resp.StatusCode)
		}
		if e.Hint != "" {
			msg += " (" + e.Hint + ")"
		}
		return resp.StatusCode, fmt.Errorf("%s: %d %s", path, resp.StatusCode, msg)
	}
	if out != nil && len(body) > 0 {
		if err := json.Unmarshal(body, out); err != nil {
			return resp.StatusCode, fmt.Errorf("%s: %w", path, err)
		}
	}
	return resp.StatusCode, nil
}
