package main

// Block 5 (moving an organization): the split out of DEFAULT pauses the organization for its
// end only, not for the copy of its rows (block 14 on 27.10: 66 s of pause at 3 million rows).

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "MV27", name: "organization 1 with 1.5 million rows in DEFAULT under writes of its rows: move-org pauses it at most 15 s for the split, the copy runs while it writes, no change is lost", run: checkSplitOnline},
	)
}

// MV27. Organization 1 gets 1.5 million documents before enable (DEFAULT holds them after it). A
// writer changes its rows the whole move (inserts, updates, deletes of what it inserted), each
// transaction holding the organization's lock shared as the proxy does, and keeps what it made
// in a table of its own in the same transaction. Before: the organization paused for the
// whole split (the copy of 1.5 million rows; the move is in trigger mode, whatever DEFECTS_MODE: the split is what is checked, and a logical copy of that many rows did not fit the check's 3 minutes on a loaded machine). Now: for the end only, at most 15 s, while the
// split takes over twice as long; the organization's rows after the move are the writer's.
func checkSplitOnline(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1, 1, 't' || g FROM generate_series(1, 1500000) g"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE mv27_expected AS SELECT id, title FROM documents WHERE organization_id = 1",
		"ALTER TABLE mv27_expected ADD PRIMARY KEY (id)"); err != nil {
		return false, "", err
	}
	lo, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM mv27_expected")
	if err != nil {
		return false, "", err
	}
	var first int64
	fmt.Sscan(lo, &first)
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)

	var mu sync.Mutex
	type op struct {
		at time.Time
		d  time.Duration
	}
	var done []op
	var werr error
	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		r := rand.New(rand.NewSource(27))
		var mine []int64
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			st := time.Now()
			err := pgx.BeginFunc(e.ctx, w, func(tx pgx.Tx) error {
				if _, err := tx.Exec(e.ctx, "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(1))"); err != nil {
					return err
				}
				switch {
				case n%3 == 0:
					var id int64
					if err := tx.QueryRow(e.ctx, "INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1, 'mv27') RETURNING id").Scan(&id); err != nil {
						return err
					}
					mine = append(mine, id)
					_, err := tx.Exec(e.ctx, "INSERT INTO mv27_expected VALUES ($1, 'mv27')", id)
					return err
				case n%3 == 1 || len(mine) == 0:
					id, title := first+r.Int63n(1500000), fmt.Sprintf("u%d", n)
					if _, err := tx.Exec(e.ctx, "UPDATE documents SET title = $2 WHERE organization_id = 1 AND id = $1", id, title); err != nil {
						return err
					}
					_, err := tx.Exec(e.ctx, "UPDATE mv27_expected SET title = $2 WHERE id = $1", id, title)
					return err
				default:
					i := r.Intn(len(mine))
					id := mine[i]
					mine = append(mine[:i], mine[i+1:]...)
					if _, err := tx.Exec(e.ctx, "DELETE FROM documents WHERE organization_id = 1 AND id = $1", id); err != nil {
						return err
					}
					_, err := tx.Exec(e.ctx, "DELETE FROM mv27_expected WHERE id = $1", id)
					return err
				}
			})
			if err != nil {
				mu.Lock()
				werr = err
				mu.Unlock()
				return
			}
			mu.Lock()
			done = append(done, op{st, time.Since(st)})
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(time.Second)
	out, moveErr := e.move(1, "-mode", "trigger")
	time.Sleep(500 * time.Millisecond)
	close(stop)
	<-finished

	paused := durationIn(out, `msg="organization resumed".* paused=([0-9.]+[a-zµ]+)`)
	splitFrom := lineTime(out, `msg="step 1/6`)
	splitTo := lineTime(out, `msg="step 2/6`)
	split := splitTo.Sub(splitFrom)
	var during int
	var longest time.Duration
	mu.Lock()
	for _, o := range done {
		if !o.at.Before(splitFrom) && o.at.Before(splitTo) {
			during++
		}
		if o.d > longest {
			longest = o.d
		}
	}
	all := len(done)
	mu.Unlock()
	if moveErr != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	if werr != nil {
		return false, "the writer failed: " + oneLine(werr.Error()), nil
	}
	diff, err := one(e.ctx, e.cloud(), `SELECT count(*) FROM (
		(SELECT id, title FROM documents WHERE organization_id = 1 EXCEPT SELECT id, title FROM mv27_expected)
		UNION ALL (SELECT id, title FROM mv27_expected EXCEPT SELECT id, title FROM documents WHERE organization_id = 1)) d`)
	if err != nil {
		return false, "", err
	}
	n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	ok := paused > 0 && paused <= 15*time.Second && split >= 2*paused && during > 0 && diff == "0"
	return ok, fmt.Sprintf("split %s, organization paused %s; the writer: %d changes (%d during the split), its longest transaction %s; organization 1: %s rows, %s differ from the writer's",
		split.Round(time.Millisecond), paused.Round(time.Millisecond), all, during, longest.Round(time.Millisecond), n, diff), nil
}

// durationIn: the first group of re's last match in out, as a duration (0: none).
func durationIn(out, re string) time.Duration {
	var d time.Duration
	r := regexp.MustCompile(re)
	for _, l := range strings.Split(out, "\n") {
		if m := r.FindStringSubmatch(l); m != nil {
			if v, err := time.ParseDuration(m[1]); err == nil {
				d = v
			}
		}
	}
	return d
}

// lineTime: the time= of the first line of out matching re (zero: none).
func lineTime(out, re string) time.Time {
	r := regexp.MustCompile(re)
	for _, l := range strings.Split(out, "\n") {
		if r.MatchString(l) {
			if i := strings.Index(l, "time="); i >= 0 {
				f := strings.Fields(l[i+5:])
				if len(f) > 0 {
					if t, err := time.Parse(time.RFC3339Nano, f[0]); err == nil {
						return t
					}
				}
			}
		}
	}
	return time.Time{}
}
