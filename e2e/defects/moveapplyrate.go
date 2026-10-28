package main

// Block 5 (moving an organization): the trigger mode's catch-up keeps up with writes faster than
// its old ceiling 43 ms away (block 14 on 27.10: 8 writers made 1 845 changes/s, the catch-up
// applied 1 570/s for 65 minutes and never caught up).

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV26", name: "-mode trigger 43 ms from the organization's database over 2 200 changes/s: the catch-up keeps up and the move ends", run: checkApplyRate},
	)
}

// MV26. lokadb reaches the organization's database through a proxy that holds every answer 43 ms
// (block 14's distance), the cloud directly (lokadb next to it). A writer inserts 200 rows of
// organization 1 at a time into the cloud, one statement after another, the whole move: over
// 2 500 log entries a second. Before:
// rounds of 500 entries in 6 round trips, ~1 900/s at most whatever the database, the log left
// grew until -catchup-timeout. Now a round of up to 5 000 entries goes in one round trip.
func checkApplyRate(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// the organization in its partition first: a write past the proxy into DEFAULT fails on the
	// split's CHECK (MV8)
	if err := execSQL(e.ctx, e.cloud(), "SELECT lokadb.partition_org(table_name, 1) FROM lokadb.tables"); err != nil {
		return false, "", err
	}
	addr, stop, err := delayProxy(e.ctx, "customer:5432", 43*time.Millisecond)
	if err != nil {
		return false, "", err
	}
	defer stop()
	host, port, _ := net.SplitHostPort(addr)
	far := fmt.Sprintf("host=%s port=%s dbname=%s user=postgres password=pw sslmode=disable", host, port, e.db)
	rt, err := roundTrip(e, far)
	if err != nil {
		return false, "", err
	}

	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	var written atomic.Int64
	var stopWriter atomic.Bool
	wdone := make(chan error, 1)
	go func() {
		for !stopWriter.Load() {
			if _, err := w.Exec(e.ctx, "INSERT INTO documents (organization_id, user_id, title) SELECT 1, 1, 'mv26 ' || g FROM generate_series(1, 200) g"); err != nil {
				wdone <- err
				return
			}
			written.Add(200)
		}
		wdone <- nil
	}()
	t0 := time.Now()
	out, err := e.move(1, "-mode", "trigger", "-customer", far, "-catchup-timeout", "3m")
	took := time.Since(t0)
	stopWriter.Store(true)
	werr := <-wdone
	rate := float64(written.Load()) / took.Seconds()
	progress := lastMatch(out, `msg=progress step=catch-up .*`)
	if err != nil {
		return false, fmt.Sprintf("round trip to the organization's database %s; %d rows written (%.0f/s) in %s; move-org failed: %s; last progress: %s",
			rt.Round(time.Millisecond), written.Load(), rate, took.Round(time.Second), lastErr(out), progress), nil
	}
	if werr != nil {
		return false, "the writer failed: " + oneLine(werr.Error()), nil
	}
	caught := lastMatch(out, `msg="caught up".*`)
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title LIKE 'mv26 %'")
	ok := caught != "" && rate > 2200 && n == fmt.Sprint(written.Load())
	return ok, fmt.Sprintf("round trip to the organization's database %s; %d rows written during the move (%.0f/s); move-org done in %s; %s; last progress: %s; at the organization: %s of them",
		rt.Round(time.Millisecond), written.Load(), rate, took.Round(time.Second), caught, progress, n), nil
}

// roundTrip: the shortest of five SELECT 1 on one connection to dsn.
func roundTrip(e *env, dsn string) (time.Duration, error) {
	c, err := pgx.Connect(e.ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer c.Close(e.ctx)
	best := time.Hour
	for i := 0; i < 5; i++ {
		t := time.Now()
		if _, err := c.Exec(e.ctx, "SELECT 1"); err != nil {
			return 0, err
		}
		if d := time.Since(t); d < best {
			best = d
		}
	}
	return best, nil
}

// lastMatch: the last line of out matching re ("" if none), cut to its match.
func lastMatch(out, re string) string {
	r := regexp.MustCompile(re)
	last := ""
	for _, l := range strings.Split(out, "\n") {
		if m := r.FindString(l); m != "" {
			last = m
		}
	}
	return last
}
