package runner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
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
	// TokenHash: which registration token this identity was made with (never the token), so that a
	// new token in the environment is told from the spent one.
	TokenHash string `json:"token_hash,omitempty"`
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
	// Recheck: how often a runner with problems checks its config again (default 30 s).
	Recheck time.Duration

	ops     *Ops
	started time.Time

	// Stop ends the process (set by the command): a setting that needs a new process asks for it
	Stop context.CancelFunc

	settings   *cloudproto.Settings // the panel's, from the last poll answer
	recheckNow chan struct{}        // new settings: check again now, not in 30 s

	mu        sync.Mutex
	st        State
	running   map[string]*job
	exclusive sync.Mutex // move, move_back, enable: one at a time
	lastErr   string
	invHash   string
	interval  time.Duration
	inv       *cloudproto.Inventory
	problems  []cloudproto.Problem                       // the config's, while it is not ready (untilReady)
	check     func(context.Context) []cloudproto.Problem // Cfg.Check (a test sets another)
	// RevokedWait: the first wait after the service refuses the identity and there is no new token (doubles, up to ten minutes)
	RevokedWait time.Duration
	triedToken  string // the token (hashed) already tried after a refusal
}

type job struct {
	j      cloudproto.Job
	cancel context.CancelFunc
	ev     *events
}

func tokenHash(t string) string {
	h := sha256.Sum256([]byte(t))
	return hex.EncodeToString(h[:8])
}

// revokedMessage says what to do, in the words of the install the runner is: the embedded runner
// (one paste, LOKADB_TOKEN, files kept) or the manual one (LOKADB_RUNNER_TOKEN and its runner.json).
func (r *Runner) revokedMessage() string {
	if r.Cfg.Embedded {
		return "this LokaDB was removed from the panel or set up again elsewhere: take a new paste from the panel's setup page and put its LOKADB_TOKEN in your compose file or env, then restart; your files are kept (" + r.Cfg.State + " and the rest of " + Dir + ")"
	}
	return "the service refused this runner's secret (it was removed in the panel or registered again): take a new token from the panel's setup page, delete " + r.statePath() + ", and start the runner with the token in LOKADB_RUNNER_TOKEN"
}

// reregister: the service refused the stored identity. An embedded runner whose environment holds a
// token it has not registered with yet registers again with it, replacing runner.json only (the
// control plane, the keys, the CA and proxy.json stay) and carries on with the new key.
func (r *Runner) reregister(ctx context.Context) bool {
	r.mu.Lock()
	spent := r.st.TokenHash
	r.mu.Unlock()
	if !r.Cfg.Embedded || r.Cfg.Token == "" || tokenHash(r.Cfg.Token) == spent || r.triedToken == tokenHash(r.Cfg.Token) {
		return false
	}
	r.triedToken = tokenHash(r.Cfg.Token) // once per process: a spent token must not be tried every minute
	r.Log.Warn("the service refused this runner's identity; registering again with the token in LOKADB_TOKEN")
	if err := r.register(ctx); err != nil {
		r.Log.Error("registering again failed", "err", err)
		return false
	}
	r.mu.Lock()
	k := r.st.Key
	r.mu.Unlock()
	if key, err := seal.ParseKey(k); err == nil && r.ops != nil {
		r.ops.Key = key
	}
	return true
}

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
	r.recheckNow = make(chan struct{}, 1)
	r.running = map[string]*job{}
	r.interval = 30 * time.Second
	if r.HTTP == nil {
		r.HTTP = &http.Client{Timeout: 60 * time.Second}
	}
	if r.PollWait == 0 {
		r.PollWait = 25 * time.Second
	}
	if r.Recheck == 0 {
		r.Recheck = 30 * time.Second
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
	r.Log.Info("runner", "id", r.st.RunnerID, "version", r.Version, "service", r.Cfg.Service, "key", fp)
	if err := r.untilReady(ctx); err != nil || ctx.Err() != nil {
		return err
	}
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

// untilReady checks the config (Config.Check) until it passes. Meanwhile the runner polls with
// its problems, so the panel shows them; it logs each once and checks again every Recheck.
func (r *Runner) untilReady(ctx context.Context) error {
	if r.check == nil {
		r.check = func(ctx context.Context) []cloudproto.Problem {
			r.applySettings()
			return r.Cfg.Prepare(ctx)
		}
	}
	probs := r.check(ctx)
	if len(probs) == 0 {
		return nil
	}
	pctx, stop := context.WithCancel(ctx)
	defer stop()
	polled := make(chan error, 1)
	go func() { polled <- r.pollLoop(pctx) }()
	told := map[string]bool{}
	for {
		r.mu.Lock()
		r.problems = probs
		r.mu.Unlock()
		if len(probs) == 0 {
			r.Log.Info("ready: the config's problems are fixed")
			stop()
			return <-polled
		}
		now := map[string]bool{}
		for _, p := range probs {
			now[p.Message] = true
			if !told[p.Message] {
				r.Log.Warn("not ready: "+p.Message, "fix", p.Fix)
			}
		}
		if len(told) == 0 {
			r.Log.Warn("the runner is registered and shows these problems in the panel; it takes no jobs until they are fixed and checks again by itself", "every", r.Recheck)
		}
		told = now
		select {
		case <-ctx.Done():
			return nil
		case err := <-polled:
			return err
		case <-time.After(r.Recheck):
		case <-r.recheckNow:
		}
		probs = r.check(ctx)
	}
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
	r.st = State{RunnerID: res.RunnerID, Secret: res.Secret, Key: key.Private(), Service: r.Cfg.Service, TokenHash: tokenHash(r.Cfg.Token)}
	if err := r.saveState(); err != nil {
		return err
	}
	r.Log.Info("registered", "runner", res.RunnerID, "account", res.Account, "key", res.Fingerprint)
	return nil
}

// lock: the SaaS database's runner lock, held on a connection of its own until unlock.
func (r *Runner) lock(ctx context.Context) (func(), error) {
	for {
		conn, err := ownsql.Connect(ctx, r.Cfg.DB)
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
	told := "" // the newer release already logged
	first := r.RevokedWait
	if first == 0 {
		first = 30 * time.Second
	}
	revokedWait := first
	for ctx.Err() == nil {
		r.mu.Lock()
		req := cloudproto.PollRequest{Version: r.Version, InventoryHash: r.invHash, Running: []string{}, Problems: r.problems, Info: r.Cfg.Info()}
		ready := r.ops != nil
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
			if r.reregister(ctx) {
				backoff = time.Second
				revokedWait = first
				continue
			}
			// no new token: say what to do and wait, longer each time, instead of ending (a supervisor
			// would restart the process at once, in a loop)
			r.Log.Error(r.revokedMessage(), "retry_in", revokedWait)
			sleep(ctx, revokedWait)
			revokedWait = min(revokedWait*2, 10*time.Minute)
			continue
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
		if res.Settings != nil {
			r.mu.Lock()
			changed := r.settings == nil || !settingsEqual(r.settings, res.Settings)
			r.settings = res.Settings
			r.mu.Unlock()
			if changed && r.recheckNow != nil {
				select {
				case r.recheckNow <- struct{}{}:
				default:
				}
			}
			// a runner that is working takes the new settings now (the tables the person chose, a
			// new relay address), unless a job is running on the old ones: that job's start applies them
			if changed {
				r.mu.Lock()
				idle := r.ops != nil && len(r.running) == 0
				r.mu.Unlock()
				if idle {
					r.applySettings()
				}
			}
		}
		if res.StatusIntervalS > 0 {
			r.mu.Lock()
			r.interval = time.Duration(res.StatusIntervalS) * time.Second
			r.mu.Unlock()
		}
		// once per release: the version and, when the service names it, the image to pull
		if res.LatestVersion != "" && res.LatestVersion != r.Version && res.LatestVersion != told {
			told = res.LatestVersion
			if res.RunnerImage != "" {
				r.Log.Info("a newer runner is out", "latest", res.LatestVersion, "pull", res.RunnerImage)
			} else {
				r.Log.Info("a newer runner is out", "latest", res.LatestVersion)
			}
		}
		for _, id := range res.Cancel {
			r.mu.Lock()
			if j := r.running[id]; j != nil {
				r.Log.Info("cancelling", "job", id)
				j.cancel()
			}
			r.mu.Unlock()
		}
		if !ready {
			// the service gives a runner with problems no jobs; one that came anyway is refused
			for _, j := range res.Jobs {
				r.postResult(ctx, j.ID, cloudproto.Result{Status: cloudproto.StatusFailed, Error: &cloudproto.JobError{Code: "not_ready",
					Message: "the runner is not ready: its config has problems (the panel lists them)"}})
			}
			sleep(ctx, time.Second)
			continue
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
	if rj.j.Kind == cloudproto.KindCheck || rj.j.Kind == cloudproto.KindEnable || rj.j.Kind == cloudproto.KindDisable {
		r.applySettings() // the tables the person chose since the last job
	}
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
	// the new state (enabled, moved) goes to the service BEFORE the result: the panel moves on as soon as
	// it sees the job done, and must then read the new state, not the one of the last status poll
	if rj.j.Kind == cloudproto.KindMove || rj.j.Kind == cloudproto.KindMoveBack || rj.j.Kind == cloudproto.KindEnable || rj.j.Kind == cloudproto.KindDisable {
		if inv, err := r.ops.Inventory(ctx); err == nil {
			r.sendInventory(ctx, &inv)
		}
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
	case cloudproto.KindOrgList:
		var a cloudproto.OrgListArgs
		if decode(&a) {
			out, err = r.ops.OrgList(ctx, a)
		}
	case cloudproto.KindOrgTables:
		var a cloudproto.OrgTablesArgs
		if decode(&a) {
			out, err = r.ops.OrgTables(ctx, a)
		}
	case cloudproto.KindDisable:
		var a cloudproto.DisableArgs
		if decode(&a) {
			out, err = r.ops.Disable(ctx, a, log)
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
