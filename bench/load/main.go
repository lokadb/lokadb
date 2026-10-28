// benchload is the load generator of the cluster measurements (bench/, research/bench/METHOD.md).
// It is path-agnostic: the scenario scripts hand it a DSN (plain Postgres, the proxy, straight to
// the cloud) or a URL (the Laravel and Django HTTP apps) and it reports what the client saw.
//
//	benchload setup   -dsn D -orgs N -rows R           the benchmark schema: N organizations of R documents
//	benchload sql     -dsn D -op read,feed,... -c 64   closed loop of SQL operations
//	benchload http    -base URL -op read,feed,... -c 64  closed loop of HTTP requests
//	benchload connect -dsn D -n 500                    connection + first query, one connection per sample
//	benchload copy    -dsn D -org 1                    COPY out (query form), COPY in, one organization
//	benchload cursor  -dsn D -org 1 -fetch 1000        DECLARE CURSOR / FETCH in a transaction
//	benchload notify  -dsn D -n 200                    NOTIFY to LISTEN latency
//
// Every run prints one JSON line per operation to stdout; -timeline FILE adds a per-second
// record (ok, errors, mean latency per organization class) for the outage scenarios.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

func die(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: benchload setup|sql|http|connect|copy|cursor|notify ...")
		os.Exit(2)
	}
	args := os.Args[2:]
	switch os.Args[1] {
	case "setup":
		die(setup(args))
	case "sql":
		die(runLoop("sql", args))
	case "http":
		die(runLoop("http", args))
	case "connect":
		die(connectBench(args))
	case "copy":
		die(copyBench(args))
	case "cursor":
		die(cursorBench(args))
	case "notify":
		die(notifyBench(args))
	default:
		die(fmt.Errorf("unknown command %q", os.Args[1]))
	}
}

// Result is the JSON line of one operation of one run.
type Result struct {
	Op       string         `json:"op"`
	Clients  int            `json:"clients"`
	Seconds  float64        `json:"seconds"`
	Ops      int            `json:"ops"`
	Errors   int            `json:"errors"`
	QPS      float64        `json:"qps"` // operations per second that completed without an error
	P50      float64        `json:"p50_ms"`
	P95      float64        `json:"p95_ms"`
	P99      float64        `json:"p99_ms"`
	Max      float64        `json:"max_ms"`
	Mean     float64        `json:"mean_ms"`
	FirstErr string         `json:"first_error,omitempty"`
	ErrKinds map[string]int `json:"error_kinds,omitempty"`
	Extra    map[string]any `json:"extra,omitempty"`
}

// stats accumulates one operation's samples.
type stats struct {
	mu    sync.Mutex
	lat   []float64 // ms, successful ones
	errs  int
	first string
	kinds map[string]int
}

func (s *stats) add(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.errs++
		if s.kinds == nil {
			s.kinds = map[string]int{}
		}
		s.kinds[kind(err)]++
		if s.first == "" {
			s.first = trim(err.Error(), 500)
		}
		return
	}
	s.lat = append(s.lat, float64(d.Microseconds())/1000)
}

func trim(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > n {
		s = s[:n]
	}
	return s
}

// kind: the first words of an error, digits cut off, so the same failure counts together.
func kind(err error) string {
	s := trim(err.Error(), 90)
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if r < '0' || r > '9' {
			out = append(out, r)
		}
	}
	return string(out)
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(float64(len(sorted)-1)*p)]
}

func round(f float64) float64 { return float64(int64(f*1000+.5)) / 1000 }

func (s *stats) result(op string, clients int, secs float64) Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	sort.Float64s(s.lat)
	var sum float64
	for _, v := range s.lat {
		sum += v
	}
	r := Result{Op: op, Clients: clients, Seconds: round(secs), Ops: len(s.lat) + s.errs, Errors: s.errs, FirstErr: s.first, ErrKinds: s.kinds}
	r.QPS = round(float64(len(s.lat)) / secs) // completed without an error
	r.P50, r.P95, r.P99 = round(pct(s.lat, .50)), round(pct(s.lat, .95)), round(pct(s.lat, .99))
	if n := len(s.lat); n > 0 {
		r.Max, r.Mean = round(s.lat[n-1]), round(sum/float64(n))
	}
	return r
}

// emit prints the result as one JSON line; BENCH_TAGS (a JSON object: scenario, configuration, ...)
// is added as "tags", so a raw file says what each line measured.
func emit(r Result) {
	b, _ := json.Marshal(r)
	if t := os.Getenv("BENCH_TAGS"); t != "" {
		var m map[string]any
		var tags map[string]any
		if json.Unmarshal(b, &m) == nil && json.Unmarshal([]byte(t), &tags) == nil {
			m["tags"] = tags
			b, _ = json.Marshal(m)
		}
	}
	fmt.Println(string(b))
}

// timeline: per second and per organization class, how many operations succeeded and failed.
type timeline struct {
	mu    sync.Mutex
	start time.Time
	sec   map[int]map[string]*[4]float64 // second -> class -> ok, errors, sum of ok latencies, sum of failed latencies (ms)
}

func newTimeline() *timeline {
	return &timeline{start: time.Now(), sec: map[int]map[string]*[4]float64{}}
}

func (t *timeline) add(class string, d time.Duration, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := int(time.Since(t.start).Seconds())
	m := t.sec[s]
	if m == nil {
		m = map[string]*[4]float64{}
		t.sec[s] = m
	}
	c := m[class]
	if c == nil {
		c = &[4]float64{}
		m[class] = c
	}
	if err != nil {
		c[1]++
		c[3] += float64(d.Microseconds()) / 1000
	} else {
		c[0]++
		c[2] += float64(d.Microseconds()) / 1000
	}
}

func (t *timeline) write(path string) error {
	if path == "" {
		return nil
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	var secs []int
	for s := range t.sec {
		secs = append(secs, s)
	}
	sort.Ints(secs)
	fmt.Fprintln(f, "second,class,ok,errors,mean_ms,error_mean_ms")
	for _, s := range secs {
		var cl []string
		for c := range t.sec[s] {
			cl = append(cl, c)
		}
		sort.Strings(cl)
		for _, c := range cl {
			v := t.sec[s][c]
			mean, emean := 0.0, 0.0
			if v[0] > 0 {
				mean = v[2] / v[0]
			}
			if v[1] > 0 {
				emean = v[3] / v[1]
			}
			fmt.Fprintf(f, "%d,%s,%d,%d,%.2f,%.2f\n", s, c, int(v[0]), int(v[1]), mean, emean)
		}
	}
	return nil
}

// worker is one client: do() performs one operation and says its name and its organization class.
type worker struct {
	do    func(ctx context.Context) (op, class string, err error)
	close func()
}

// loop runs `clients` workers until warm+dur has passed; only what starts after warm is counted.
// The timeline counts from the start of the measured time.
func loop(ctx context.Context, clients int, warm, dur, stagger time.Duration, mk func(id int) worker) (map[string]*stats, *timeline) {
	all := map[string]*stats{}
	var mu sync.Mutex
	get := func(op string) *stats {
		mu.Lock()
		defer mu.Unlock()
		s := all[op]
		if s == nil {
			s = &stats{}
			all[op] = s
		}
		return s
	}
	tl := newTimeline()
	measureFrom := time.Now().Add(warm)
	end := measureFrom.Add(dur)
	go func() { time.Sleep(time.Until(measureFrom)); tl.mu.Lock(); tl.start = time.Now(); tl.mu.Unlock() }()
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := mk(id)
			defer w.close()
			// The clients start one after another over `stagger`, not all at once: a connection storm at the start and
			// sessions whose 30-second metadata cycles all coincide (performance review 3, P3-8 and P3-19) are not what an
			// application's sessions look like. The start is inside the warm-up, which is not counted.
			if stagger > 0 && clients > 1 {
				time.Sleep(stagger * time.Duration(id) / time.Duration(clients))
			}
			for time.Now().Before(end) && ctx.Err() == nil {
				st := time.Now()
				op, class, err := w.do(ctx)
				d := time.Since(st)
				if st.Before(measureFrom) {
					continue
				}
				if ctx.Err() != nil { // stopped (SIGINT): an operation cut off by the stop is not a result
					break
				}
				get(op).add(d, err)
				tl.add(class, d, err)
			}
		}(i)
	}
	wg.Wait()
	return all, tl
}

// openLoop is the open model: operations arrive at `rate` per second whatever the answers, and `clients` workers
// take them in order. The latency of an operation counts from when it was due, so the time it waited for a free
// worker is in it (no coordinated omission). One that could not start within `timeout` of being due is a timeout
// ("not started"); one that was started gets `timeout` of its own to finish, else it is a timeout ("not answered").
// (A deadline counted from the due time, as the first version had it, fails every operation of a full queue even
// when the system answers in a millisecond: the numbers past saturation said nothing.)
func openLoop(ctx context.Context, clients int, rate float64, timeout, warm, dur time.Duration, mk func(id int) worker) (map[string]*stats, *timeline) {
	all := map[string]*stats{}
	var mu sync.Mutex
	get := func(op string) *stats {
		mu.Lock()
		defer mu.Unlock()
		s := all[op]
		if s == nil {
			s = &stats{}
			all[op] = s
		}
		return s
	}
	tl := newTimeline()
	start := time.Now()
	measureFrom := start.Add(warm)
	end := measureFrom.Add(dur)
	go func() { time.Sleep(time.Until(measureFrom)); tl.mu.Lock(); tl.start = time.Now(); tl.mu.Unlock() }()
	due := make(chan time.Time, int(rate*timeout.Seconds())+clients)
	var dropped stats // arrivals the queue had no room for
	go func() {
		defer close(due)
		interval := time.Duration(float64(time.Second) / rate)
		for i := 0; ; i++ {
			at := start.Add(time.Duration(i) * interval)
			if !at.Before(end) || ctx.Err() != nil {
				return
			}
			time.Sleep(time.Until(at))
			select {
			case due <- at:
			default:
				if !at.Before(measureFrom) {
					dropped.add(0, errQueueFull)
				}
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			w := mk(id)
			defer w.close()
			for at := range due {
				counted := !at.Before(measureFrom)
				if time.Since(at) > timeout {
					if counted {
						get("timeout").add(0, errNotStarted)
						tl.add("timeout", 0, errNotStarted)
					}
					continue
				}
				octx, cancel := context.WithTimeout(ctx, timeout)
				op, class, err := w.do(octx)
				cancel()
				if ctx.Err() != nil {
					return
				}
				if err != nil && octx.Err() != nil {
					err = errTimedOut
				}
				if counted {
					get(op).add(time.Since(at), err)
					tl.add(class, time.Since(at), err)
				}
			}
		}(i)
	}
	wg.Wait()
	if dropped.errs > 0 {
		all["timeout"] = mergeErrs(all["timeout"], &dropped)
	}
	return all, tl
}

var (
	errNotStarted = errors.New("timeout: not started within the timeout (all workers busy)")
	errTimedOut   = errors.New("timeout: not answered within the timeout")
	errQueueFull  = errors.New("timeout: the queue of due operations was full")
)

func mergeErrs(a, b *stats) *stats {
	if a == nil {
		return b
	}
	a.errs += b.errs
	for k, v := range b.kinds {
		if a.kinds == nil {
			a.kinds = map[string]int{}
		}
		a.kinds[k] += v
	}
	return a
}

func opNames(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// loadFlags are the flags both closed loops share.
type loadFlags struct {
	clients  int
	warm     time.Duration
	dur      time.Duration
	ops      string
	timeline string
	orgs     int
	firstOrg int
	rows     int
	seed     int64
	hot      int
	classes  string
	stagger  time.Duration
	rate     float64
	timeout  time.Duration
}

func (l *loadFlags) bind(fs *flag.FlagSet) {
	fs.IntVar(&l.clients, "c", 16, "clients")
	fs.DurationVar(&l.warm, "warmup", 10*time.Second, "run first, not counted")
	fs.DurationVar(&l.dur, "d", 30*time.Second, "measured time")
	fs.StringVar(&l.ops, "op", "read", "operations, mixed evenly: read,feed,insert,update,report")
	fs.StringVar(&l.timeline, "timeline", "", "write a per-second timeline (CSV) here")
	fs.IntVar(&l.orgs, "orgs", 1, "the clients spread over this many organizations")
	fs.IntVar(&l.firstOrg, "first-org", 1, "the first organization of the range")
	fs.IntVar(&l.rows, "rows", 1000, "documents per organization (ids are spread over them)")
	fs.Int64Var(&l.seed, "seed", 1, "")
	fs.IntVar(&l.hot, "hot", 0, "only this many documents per organization are ever asked for (0: all): the keys the proxy has seen before, against the first look at a key")
	fs.StringVar(&l.classes, "classes", "", "ORG=CLASS,... names of organizations for the timeline (default: org<N>)")
	fs.Float64Var(&l.rate, "rate", 0, "the open model: this many operations a second, due whether the earlier ones were answered or not (0: the closed loop, the next after the answer); -c workers take them")
	fs.DurationVar(&l.timeout, "timeout", time.Second, "with -rate: an operation not started or not answered this long after it was due is a timeout")
	fs.DurationVar(&l.stagger, "stagger", -1, "the clients start one after another over this long (default: half of the warm-up, at most 10 s; 0: all at once)")
}

func (l *loadFlags) class(org int) string {
	for _, p := range strings.Split(l.classes, ",") {
		if k, v, ok := strings.Cut(p, "="); ok && k == fmt.Sprint(org) {
			return v
		}
	}
	return fmt.Sprintf("org%d", org)
}
