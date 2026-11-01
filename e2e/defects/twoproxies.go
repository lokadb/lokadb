package main

// Two proxy processes in front of one cloud database (the third question before the Show HN post:
// what do the caches that every process keeps for itself do to a move that runs under both?).
//
// TP1: the application writes both organizations through both proxies at once, organization 1
//      is moved out (move-org) and back (move-back) meanwhile. Nothing acknowledged is lost or
//      made twice, the cloud holds no row of organization 1 while it is moved, no answer carries a
//      row of another organization, and the metadata of each proxy follows the move at once: the
//      proxies' policy is `log`, which is `reject` once an organization has moved, so a statement
//      without an organization shows when a session reads the metadata again. NOTIFY lokadb_meta
//      makes that immediate; without it a session waits for metaTTL (30 s). Sessions are opened
//      one a second during the move, so some of them have just read the metadata when it switches.
// TP2: the process cache of "which organization is this key" (one minute): a key both proxies looked
//      for and did not find, then made through one proxy, is read through the other, which answers
//      nothing or the new row within the minute, never another organization's row.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "TP1", name: "two proxies, both organizations written through both, organization 1 moved out and back: nothing lost or doubled, no foreign row, the metadata follows at once", run: checkTwoProxies},
		check{id: "TP2", name: "two proxies: a key not found at both, then made through one, is found through the other within the key cache's minute, never as another organization's row", run: checkTwoProxyKeyCache},
	)
}

const (
	notifyLag = 2 * time.Second  // what NOTIFY lokadb_meta gives: a session reads the metadata again before its next statement
	metaLag   = 35 * time.Second // metaTTL (30 s) and a poll
	keyLag    = 70 * time.Second // orgCacheTTL (1 minute) and a poll
)

// proxyAt starts a proxy on its own port with the cloud and the given fanout mode and customers.
func (e *env) proxyAt(port int, tag, mode string, customers map[string]string) (func(), error) {
	c := map[string]any{
		"listen":    fmt.Sprintf(":%d", port),
		"upstream":  "host=" + e.cloudHost + " port=5432 sslmode=disable",
		"customers": map[string]any{e.db: customers},
		"fanout":    map[string]any{e.db: map[string]string{"mode": mode}},
	}
	cfg, _ := json.Marshal(c)
	p := filepath.Join(os.TempDir(), fmt.Sprintf("%s-proxy-%s.json", e.db, tag))
	if err := os.WriteFile(p, cfg, 0o644); err != nil {
		return nil, err
	}
	log, err := os.Create(filepath.Join(os.TempDir(), fmt.Sprintf("%s-proxy-%s.log", e.db, tag)))
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(e.bin, "proxy", "-config", p)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stop := func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); log.Close() }
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if c, err := pgx.Connect(e.ctx, tpDSN(port, e.db)); err == nil {
			c.Close(e.ctx)
			return stop, nil
		}
	}
	stop()
	return nil, fmt.Errorf("proxy %s did not start in 60 s", tag)
}

func tpDSN(port int, db string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", port, db)
}

// tpConn: a connection to a proxy; simple makes it the simple query protocol (the other proxy's
// clients use the extended one).
func tpConn(ctx context.Context, port int, db string, simple bool) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(tpDSN(port, db))
	if err != nil {
		return nil, err
	}
	if simple {
		cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}
	return pgx.ConnectConfig(ctx, cfg)
}

// tpWriter: the application of one organization through one proxy. It inserts a row, then reads it
// back by organization, by key and the organization's rows, and records every answer that is not
// its own organization's or that lacks the row it was told it had written.
type tpWriter struct {
	port    int
	simple  bool
	org     int
	tag     string
	mu      sync.Mutex
	acked   []string
	failed  int
	problem []string
}

func (w *tpWriter) note(format string, a ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.problem) < 5 {
		w.problem = append(w.problem, fmt.Sprintf(format, a...))
	}
}

func (w *tpWriter) run(e *env, stop <-chan struct{}, done *sync.WaitGroup) {
	defer done.Done()
	var c *pgx.Conn
	defer func() {
		if c != nil {
			c.Close(context.Background())
		}
	}()
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		case <-time.After(10 * time.Millisecond):
		}
		if c == nil {
			var err error
			if c, err = tpConn(e.ctx, w.port, e.db, w.simple); err != nil {
				c = nil
				w.mu.Lock()
				w.failed++
				w.mu.Unlock()
				continue
			}
		}
		title := fmt.Sprintf("tp-%s-%d", w.tag, n)
		var id int64
		if err := c.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES ($1, 2, $2) RETURNING id", w.org, title).Scan(&id); err != nil {
			w.mu.Lock()
			w.failed++
			w.mu.Unlock()
			c.Close(context.Background())
			c = nil
			continue
		}
		w.mu.Lock()
		w.acked = append(w.acked, title)
		w.mu.Unlock()
		// by key: the row is there, and it is this organization's
		rows, err := c.Query(e.ctx, "SELECT organization_id, title FROM documents WHERE id = $1", id)
		if err == nil {
			got := 0
			for rows.Next() {
				var o int
				var t string
				if rows.Scan(&o, &t) == nil {
					got++
					if o != w.org || t != title {
						w.note("by key %d: organization %d title %s, wanted organization %d title %s", id, o, t, w.org, title)
					}
				}
			}
			rows.Close()
			if rows.Err() == nil && got != 1 {
				w.note("by key %d: %d rows right after the insert was acknowledged", id, got)
			}
		}
		// the organization's rows: only its own
		if n%10 == 0 {
			rows, err := c.Query(e.ctx, "SELECT DISTINCT organization_id FROM documents WHERE organization_id = $1", w.org)
			if err == nil {
				for rows.Next() {
					var o int
					if rows.Scan(&o) == nil && o != w.org {
						w.note("organization %d read returned a row of organization %d", w.org, o)
					}
				}
				rows.Close()
			}
		}
	}
}

// metaProbe opens a session a second on one proxy (until stopped) and each one asks, every 250 ms,
// for a statement that names no organization. What it sees flips when the proxy's metadata says an
// organization has moved (the policy is log: refused then) or none has (allowed): want.
type metaProbe struct {
	e       *env
	port    int
	simple  bool
	want    string // "refused" or "ok"
	stop    chan struct{}
	mu      sync.Mutex
	flipped []time.Time // the first moment each session saw want
	open    int
	other   []string
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

func (e *env) startMetaProbe(port int, simple bool, want string) *metaProbe {
	p := &metaProbe{e: e, port: port, simple: simple, want: want, stop: make(chan struct{})}
	ctx, cancel := context.WithCancel(e.ctx)
	p.cancel = cancel
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			p.wg.Add(1)
			go p.session(ctx)
			select {
			case <-p.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return p
}

func (p *metaProbe) session(ctx context.Context) {
	defer p.wg.Done()
	c, err := tpConn(ctx, p.port, p.e.db, p.simple)
	if err != nil {
		return
	}
	defer c.Close(context.Background())
	p.mu.Lock()
	p.open++
	p.mu.Unlock()
	for {
		var n int64
		err := c.QueryRow(ctx, "SELECT count(*) FROM documents").Scan(&n)
		state := "ok"
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			state = "other"
			if strings.Contains(err.Error(), "does not name the organization") {
				state = "refused"
			} else {
				p.mu.Lock()
				if len(p.other) < 3 {
					p.other = append(p.other, oneLine(err.Error()))
				}
				p.mu.Unlock()
			}
		}
		if state == p.want {
			p.mu.Lock()
			p.flipped = append(p.flipped, time.Now())
			p.mu.Unlock()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// settle: the probes' sessions that did not see want yet get until metaLag after at (the end of the
// command that changed the metadata); the answer is how late the last one was, and how many never
// came (the sessions that had not flipped when the time was up).
func settle(ps []*metaProbe, at time.Time) (lag time.Duration, never int) {
	for _, p := range ps {
		close(p.stop)
	}
	deadline := at.Add(metaLag + 5*time.Second)
	for time.Now().Before(deadline) {
		busy := false
		for _, p := range ps {
			p.mu.Lock()
			if p.open > len(p.flipped) {
				busy = true
			}
			p.mu.Unlock()
		}
		if !busy {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	for _, p := range ps {
		p.mu.Lock()
		never += p.open - len(p.flipped)
		for _, t := range p.flipped {
			if d := t.Sub(at); d > lag {
				lag = d
			}
		}
		p.mu.Unlock()
		p.cancel()
	}
	for _, p := range ps {
		p.wg.Wait()
	}
	return lag, never
}

func checkTwoProxies(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	portA, portB := e.port, e.port+20000
	cust := map[string]string{"1": e.appCustomer()}
	stopA, err := e.proxyAt(portA, "a", "log", cust)
	if err != nil {
		return false, "", err
	}
	defer stopA()
	stopB, err := e.proxyAt(portB, "b", "log", cust)
	if err != nil {
		return false, "", err
	}
	defer stopB()

	var (
		writers []*tpWriter
		stop    = make(chan struct{})
		done    sync.WaitGroup
	)
	for _, org := range []int{1, 2} {
		writers = append(writers,
			&tpWriter{port: portA, org: org, tag: fmt.Sprintf("a%d", org)},
			&tpWriter{port: portB, org: org, tag: fmt.Sprintf("b%d", org), simple: true})
	}
	for _, w := range writers {
		done.Add(1)
		go w.run(e, stop, &done)
	}
	probes := func(want string) []*metaProbe {
		return []*metaProbe{e.startMetaProbe(portA, false, want), e.startMetaProbe(portB, true, want)}
	}
	var failures []string
	fail := func(f string, a ...any) { failures = append(failures, fmt.Sprintf(f, a...)) }
	finish := func() { // the writers end, whatever happens next
		select {
		case <-stop:
		default:
			close(stop)
		}
		done.Wait()
	}
	defer finish()

	time.Sleep(3 * time.Second)

	// out
	ps := probes("refused")
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		for _, p := range ps {
			close(p.stop)
			p.cancel()
		}
		return false, "move-org failed: " + oneLine(err.Error()), nil
	}
	lagOut, neverOut := settle(ps, time.Now())
	inCloud, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM documents d JOIN pg_class c ON c.oid = d.tableoid WHERE d.organization_id = 1 AND c.relkind <> 'f'`)
	if err != nil {
		return false, "", err
	}
	if inCloud != "0" {
		fail("%s rows of organization 1 are in the cloud's own partitions after move-org (want 0)", inCloud)
	}
	if neverOut > 0 {
		fail("%d sessions never saw organization 1 as moved", neverOut)
	}
	if lagOut > notifyLag {
		fail("the proxies' metadata followed move-org %s late (NOTIFY lokadb_meta tells at once, wanted within %s)", lagOut.Round(100*time.Millisecond), notifyLag)
	}
	time.Sleep(3 * time.Second)

	// back
	ps = probes("ok")
	if out, err := e.lokadb(e.moveBackArgs(1)...); err != nil {
		for _, p := range ps {
			close(p.stop)
			p.cancel()
		}
		return false, "move-back failed: " + lastErr(out), nil
	}
	lagBack, neverBack := settle(ps, time.Now())
	if neverBack > 0 {
		fail("%d sessions never saw organization 1 as back", neverBack)
	}
	if lagBack > notifyLag {
		fail("the proxies' metadata followed move-back %s late (%s)", lagBack.Round(100*time.Millisecond), notifyLag)
	}
	time.Sleep(3 * time.Second)
	finish()

	// what was written
	acked, failed := map[int][]string{}, 0
	for _, w := range writers {
		acked[w.org] = append(acked[w.org], w.acked...)
		failed += w.failed
		for _, p := range w.problem {
			fail("writer %s: %s", w.tag, p)
		}
	}
	for _, org := range []int{1, 2} {
		got := map[string]int{}
		rows, err := pgx.Connect(e.ctx, e.cloud())
		if err != nil {
			return false, "", err
		}
		r, err := rows.Query(e.ctx, "SELECT title FROM documents WHERE organization_id = $1 AND title LIKE 'tp-%'", org)
		if err != nil {
			rows.Close(e.ctx)
			return false, "", err
		}
		for r.Next() {
			var t string
			if err := r.Scan(&t); err != nil {
				rows.Close(e.ctx)
				return false, "", err
			}
			got[t]++
		}
		r.Close()
		rows.Close(e.ctx)
		var lost, doubled []string
		for _, t := range acked[org] {
			switch got[t] {
			case 0:
				lost = append(lost, t)
			case 1:
			default:
				doubled = append(doubled, t)
			}
		}
		// a row the application was not told about (its connection broke) may be there once
		unknown := 0
		want := map[string]bool{}
		for _, t := range acked[org] {
			want[t] = true
		}
		for t, n := range got {
			if !want[t] {
				unknown++
				if n > 1 {
					doubled = append(doubled, t)
				}
			}
		}
		sort.Strings(lost)
		sort.Strings(doubled)
		if len(lost) > 0 {
			fail("organization %d: %d acknowledged rows lost, first %s", org, len(lost), lost[0])
		}
		if len(doubled) > 0 {
			fail("organization %d: %d rows made twice, first %s", org, len(doubled), doubled[0])
		}
		if unknown > failed {
			fail("organization %d: %d rows nobody was told about, %d statements failed", org, unknown, failed)
		}
	}
	detail := fmt.Sprintf("acknowledged %d + %d rows (%d statements failed); metadata followed move-org in %s and move-back in %s",
		len(acked[1]), len(acked[2]), failed, lagOut.Round(100*time.Millisecond), lagBack.Round(100*time.Millisecond))
	if len(failures) > 0 {
		return false, strings.Join(failures, "; ") + " [" + detail + "]", nil
	}
	return true, detail, nil
}

func checkTwoProxyKeyCache(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	portA, portB := e.port, e.port+20000
	stopA, err := e.proxyAt(portA, "a", "reject", nil)
	if err != nil {
		return false, "", err
	}
	defer stopA()
	stopB, err := e.proxyAt(portB, "b", "reject", nil)
	if err != nil {
		return false, "", err
	}
	defer stopB()
	id := "900001" // a key no row has yet
	a, err := tpConn(e.ctx, portA, e.db, false)
	if err != nil {
		return false, "", err
	}
	defer a.Close(e.ctx)
	b, err := tpConn(e.ctx, portB, e.db, false)
	if err != nil {
		return false, "", err
	}
	defer b.Close(e.ctx)
	read := func(c *pgx.Conn) (string, error) { // the organizations of the rows with the key, "-" for none
		rows, err := c.Query(e.ctx, "SELECT organization_id FROM documents WHERE id = $1", id)
		if err != nil {
			return "", err
		}
		defer rows.Close()
		var got []string
		for rows.Next() {
			var o int
			if err := rows.Scan(&o); err != nil {
				return "", err
			}
			got = append(got, fmt.Sprint(o))
		}
		if err := rows.Err(); err != nil {
			return "", err
		}
		if len(got) == 0 {
			return "-", nil
		}
		return strings.Join(got, ","), nil
	}
	for _, c := range []*pgx.Conn{a, b} { // both proxies look for the key and find it nowhere
		if got, err := read(c); err != nil || got != "-" {
			return false, fmt.Sprintf("before the insert the key reads %q (%v), want none", got, err), nil
		}
	}
	if _, err := a.Exec(e.ctx, "INSERT INTO documents (id, organization_id, user_id, title) VALUES ($1, 2, 2, 'tp2')", id); err != nil {
		return false, "INSERT through proxy A: " + short(err), nil
	}
	start := time.Now()
	if got, err := read(a); err != nil || got != "2" {
		return false, fmt.Sprintf("proxy A reads %q (%v) after its own change, want 2", got, err), nil
	}
	for time.Since(start) < keyLag {
		got, err := read(b)
		if err != nil {
			return false, "proxy B: " + short(err), nil
		}
		if got != "-" && got != "2" {
			return false, fmt.Sprintf("proxy B read %q for a row of organization 2", got), nil
		}
		if got == "2" {
			lag := time.Since(start)
			return true, fmt.Sprintf("proxy B found the new row (organization 2) after %s; before that nothing, never another organization's row", lag.Round(time.Second)), nil
		}
		time.Sleep(time.Second)
	}
	return false, fmt.Sprintf("proxy B still did not read the row of organization 2 after %s", keyLag), nil
}
