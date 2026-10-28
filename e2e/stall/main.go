// stall writes for random organizations on several connections until SIGINT (or -duration)
// and reports how long the application could not write: the longest single write, the
// longest gap with no commit at all and the errors. A failed write is counted and the
// connection is opened again; nothing aborts the run (pgbench stops a client at its first
// error).
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	dsn := flag.String("dsn", "", "connection string")
	clients := flag.Int("c", 4, "connections")
	orgs := flag.Int("orgs", 20, "organizations 1..orgs")
	dur := flag.Duration("duration", 0, "stop after this long (0: on SIGINT/SIGTERM)")
	setOrg := flag.Bool("set-org", true, "set lokadb.org before each write, as the proxy does")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *dur > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}
	var (
		mu      sync.Mutex
		commits []time.Time
		maxLat  time.Duration
		errs    = map[string]int{}
		wg      sync.WaitGroup
	)
	start := time.Now()
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(i)))
			var c *pgx.Conn
			for ctx.Err() == nil {
				if c == nil {
					var err error
					if c, err = pgx.Connect(ctx, *dsn); err != nil {
						c = nil
						time.Sleep(100 * time.Millisecond)
						continue
					}
				}
				org := 1 + r.Intn(*orgs)
				t0 := time.Now()
				var err error
				if *setOrg {
					_, err = c.Exec(ctx, "SELECT set_config('lokadb.org', $1, false)", fmt.Sprint(org))
				}
				if err == nil {
					_, err = c.Exec(ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES ($1, 1, 'w')", org)
				}
				lat := time.Since(t0)
				mu.Lock()
				if err == nil {
					commits = append(commits, time.Now())
				} else if ctx.Err() == nil {
					msg := err.Error()
					if len(msg) > 120 {
						msg = msg[:120]
					}
					errs[msg]++
				}
				if lat > maxLat && ctx.Err() == nil {
					maxLat = lat
				}
				mu.Unlock()
				if err != nil {
					c.Close(context.Background())
					c = nil
				}
			}
			if c != nil {
				c.Close(context.Background())
			}
		}(i)
	}
	wg.Wait()
	sort.Slice(commits, func(a, b int) bool { return commits[a].Before(commits[b]) })
	var gap time.Duration
	var gapAt time.Duration
	prev := start
	for _, t := range commits {
		if d := t.Sub(prev); d > gap {
			gap, gapAt = d, prev.Sub(start)
		}
		prev = t
	}
	nerr := 0
	var es []string
	for m, n := range errs {
		nerr += n
		es = append(es, fmt.Sprintf("%dx %s", n, m))
	}
	sort.Strings(es)
	el := time.Since(start)
	fmt.Printf("writes %d in %.0f s (%.0f/s), longest write %d ms, longest gap between commits %.2f s (from %.0f s), errors %d",
		len(commits), el.Seconds(), float64(len(commits))/el.Seconds(), maxLat.Milliseconds(), gap.Seconds(), gapAt.Seconds(), nerr)
	if len(es) > 0 {
		fmt.Printf(": %s", strings.Join(es, "; "))
	}
	fmt.Println()
}
