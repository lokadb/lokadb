package main

// Block 5 (moving an organization): the catch-up of the logical mode on a slow delivery.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func init() {
	checks = append(checks,
		check{id: "MV12", name: "-mode logical, replication delivered 1 s late: the catch-up ends, the switch pauses writes about that long", run: checkSlowDelivery},
		check{id: "MV13", name: "move-org killed after the switch, before it dropped the slot: check reports the slot with its WAL, move-cleanup drops it", run: checkSlotAfterKill},
	)
}

// MV12. The organization's database subscribes through a proxy that holds what the cloud
// sends for 1 s (a loaded machine, a distant database): every marker takes over 500 ms, the
// old catch-up waited for less until -catchup-timeout. A writer inserts the organization's
// rows from the subscription on. The move must finish, the catch-up end on the steady lag, the switch pause
// writes about that lag, not more than half of -max-pause.
func checkSlowDelivery(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	addr, stop, err := delayProxy(e.ctx, e.cloudHost+":5432", time.Second)
	if err != nil {
		return false, "", err
	}
	defer stop()
	host, port, _ := net.SplitHostPort(addr)
	publisher := fmt.Sprintf("host=%s port=%s dbname=%s user=postgres password=pw sslmode=disable", host, port, e.db)

	var written atomic.Int64
	var stopWriter atomic.Bool
	wdone := make(chan error, 1)
	var started sync.Once
	// from the subscription on: while the organization is split out of DEFAULT a write past
	// the proxy fails on its CHECK (MV8)
	writer := func() {
		for !stopWriter.Load() {
			if _, err := one(e.ctx, e.cloud(), "WITH i AS (INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1, 'mv12') RETURNING 1) SELECT count(*) FROM i"); err != nil {
				wdone <- err
				return
			}
			written.Add(1)
			time.Sleep(50 * time.Millisecond)
		}
		wdone <- nil
	}
	mctx, mcancel := context.WithTimeout(e.ctx, 150*time.Second)
	defer mcancel()
	me := *e
	me.ctx = mctx
	out, took, merr := me.moveLines(1, func(l string) {
		if strings.Contains(l, "step 4/6") {
			started.Do(func() { go writer() })
		}
	}, "-mode", "logical", "-publisher", publisher, "-catchup-timeout", "2m")
	stopWriter.Store(true)
	var werr error
	started.Do(func() { wdone <- nil }) // never started
	werr = <-wdone
	if mctx.Err() != nil && e.ctx.Err() == nil {
		return false, "move-org still catching up after 150 s: " + lastLine(out), nil
	}
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	if werr != nil {
		return false, "the writer failed: " + firstErrLine(werr), nil
	}
	caught, paused := logField(out, `msg="replica caught up"`, "why"), logField(out, "msg=switched", "writes_paused")
	waited, dropped := logField(out, "msg=switched", "wait_last_changes"), logField(out, "msg=switched", "drop_subscription")
	behind := strings.Count(out, `msg="replica still behind"`)
	pause, _ := time.ParseDuration(paused)
	rows, err := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'mv12'")
	if err != nil {
		return false, "", err
	}
	ok := strings.Contains(caught, "steady") && pause > 0 && pause < 5*time.Second && rows == fmt.Sprint(written.Load())
	return ok, fmt.Sprintf("move took %s; caught up: %s after %d rounds behind; writes paused %s (the last changes %s, dropping the subscription %s); the writer's rows: %d in the cloud, %s in the organization's database",
		took.Round(100*time.Millisecond), caught, behind, paused, waited, dropped, written.Load(), rows), nil
}

// logField: the value of key on the first line of out containing marker ("" if none).
func logField(out, marker, key string) string {
	for _, l := range strings.Split(out, "\n") {
		if !strings.Contains(l, marker) {
			continue
		}
		i := strings.Index(l, " "+key+"=")
		if i < 0 {
			return ""
		}
		v := l[i+len(key)+2:]
		if strings.HasPrefix(v, `"`) {
			v, _, _ = strings.Cut(v[1:], `"`)
			return v
		}
		v, _, _ = strings.Cut(v, " ")
		return v
	}
	return ""
}

// delayProxy listens on this container's address and forwards every connection to target,
// holding what target sends for delay (what is sent to it goes at once). It returns the
// address the organization's database dials.
func delayProxy(ctx context.Context, target string, delay time.Duration) (string, func(), error) {
	var ip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", nil, err
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && n.IP.To4() != nil {
			ip = n.IP.String()
			break
		}
	}
	if ip == "" {
		return "", nil, errors.New("no address of this container")
	}
	ln, err := net.Listen("tcp", ip+":0")
	if err != nil {
		return "", nil, err
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var d net.Dialer
				t, err := d.DialContext(ctx, "tcp", target)
				if err != nil {
					return
				}
				defer t.Close()
				go func() { _, _ = io.Copy(t, c); t.Close() }()
				delayed(c, t, delay)
			}()
		}
	}()
	return ln.Addr().String(), func() { ln.Close() }, nil
}

// delayed copies src to dst, each chunk delay after it was read, in order.
func delayed(dst io.Writer, src io.Reader, delay time.Duration) {
	type chunk struct {
		b  []byte
		at time.Time
	}
	q := make(chan chunk, 4096)
	go func() {
		defer close(q)
		for {
			b := make([]byte, 64<<10)
			n, err := src.Read(b)
			if n > 0 {
				q <- chunk{b[:n], time.Now().Add(delay)}
			}
			if err != nil {
				return
			}
		}
	}()
	for c := range q {
		time.Sleep(time.Until(c.at))
		if _, err := dst.Write(c.b); err != nil {
			for range q {
			}
			return
		}
	}
}

// MV13. The switch drops the subscription without its slot (no round trip to the publisher
// with writes paused); the slot goes once writes resume. move-org killed in between (on
// "switched", while it verifies 120 000 rows) leaves the slot, which keeps the cloud's WAL.
// lokadb check must report it with the WAL it holds, move-cleanup drop it.
func checkSlotAfterKill(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := bigOrgRows(e); err != nil {
		return false, "", err
	}
	cmd := exec.CommandContext(e.ctx, e.bin, e.moveArgs(bigOrg, "-mode", "logical")...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return false, "", err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return false, "", err
	}
	var all strings.Builder
	killed := false
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if !killed && strings.Contains(sc.Text(), "msg=switched") {
			_ = cmd.Process.Signal(syscall.SIGKILL)
			killed = true
		}
	}
	_ = cmd.Wait()
	if !killed {
		return false, "move-org never switched: " + lastLine(all.String()), nil
	}
	slots := "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'lokadb%'"
	before, err := one(e.ctx, e.cloud(), slots)
	if err != nil {
		return false, "", err
	}
	// WAL the slot keeps: the cloud's writes since
	if err := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, user_id, title) SELECT 1, 2, 'mv13' || g FROM generate_series(1, 20000) g"); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	report, _ := e.lokadb("check", "-db", e.cloud(), "-spec", p)
	var found string
	for _, l := range strings.Split(report, "\n") {
		if strings.Contains(l, "replication slot lokadb_") {
			found = strings.TrimSpace(l)
		}
	}
	cout, cerr := e.lokadb("move-cleanup", "-db", e.cloud())
	if cerr != nil {
		return false, "move-cleanup failed: " + firstErrLine(cerr), nil
	}
	after, err := one(e.ctx, e.cloud(), slots)
	if err != nil {
		return false, "", err
	}
	ok := before == "1" && strings.Contains(found, "of WAL") && after == "0"
	return ok, fmt.Sprintf("slots after kill -9 on switched: %s; check: %q; after move-cleanup (%s): %s",
		before, found, lastLine(cout), after), nil
}
