package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The split of an organization out of DEFAULT under its writes (block 14, 27.10: the organization
// paused for the whole copy, 66 s at 3 million rows). A writer of organization 7 takes the
// organization's lock shared in every transaction, as the proxy does, and inserts, updates and
// deletes its rows all along. The organization may pause for the end only: the writer's longest
// transaction is short against the split, it writes during the copy, and not one of its changes
// is lost: the organization's rows after the split are exactly what the writer made of them.
func TestSplitOnlineUnderWrites(t *testing.T) {
	base := review3Base(t)
	dsn := review3DB(t, base, "split_online")
	review3Exec(t, dsn,
		`DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'split_owner') THEN CREATE ROLE split_owner; END IF; END $$`,
		`CREATE TABLE docs4 (id bigint PRIMARY KEY, org_id bigint NOT NULL, body text)`,
		`INSERT INTO docs4 SELECT g, 7, 'b' || g FROM generate_series(1, 300000) g`,
		`INSERT INTO docs4 SELECT g, 8, 'o' || g FROM generate_series(300001, 301000) g`)
	spec := Spec{Tables: []SpecTable{{Name: "docs4", TenantColumn: "org_id"}}}
	ctx := context.Background()
	if err := Enable(ctx, dsn, spec, "split_owner", review3Log()); err != nil {
		t.Fatal(err)
	}
	cloud, err := connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer cloud.Close(ctx)
	w, err := connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close(ctx)

	want := map[int64]string{} // what the writer made of organization 7's rows
	for i := int64(1); i <= 300000; i++ {
		want[i] = fmt.Sprintf("b%d", i)
	}
	var mu sync.Mutex
	var longest time.Duration
	var ops, failed int
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		r := rand.New(rand.NewSource(1))
		next := int64(400000)
		for n := 0; ; n++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			id := r.Int63n(300000) + 1
			var sql string
			var args []any
			var apply func()
			switch n % 3 {
			case 0:
				next++
				nid := next
				sql, args = "INSERT INTO docs4 VALUES ($1, 7, $2)", []any{nid, fmt.Sprintf("n%d", nid)}
				apply = func() { want[nid] = fmt.Sprintf("n%d", nid) }
			case 1:
				body := fmt.Sprintf("u%d", n)
				sql, args = "UPDATE docs4 SET body = $2 WHERE org_id = 7 AND id = $1", []any{id, body}
				apply = func() {
					if _, ok := want[id]; ok {
						want[id] = body
					}
				}
			default:
				sql, args = "DELETE FROM docs4 WHERE org_id = 7 AND id = $1", []any{id}
				apply = func() { delete(want, id) }
			}
			st := time.Now()
			err := pgx.BeginFunc(ctx, w, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock_shared(lokadb.org_lock(7))"); err != nil {
					return err
				}
				_, err := tx.Exec(ctx, sql, args...)
				return err
			})
			d := time.Since(st)
			mu.Lock()
			if d > longest {
				longest = d
			}
			if err != nil {
				failed++
			} else {
				ops++
				apply()
			}
			mu.Unlock()
			if err != nil && failed > 20 {
				done <- err
				return
			}
		}
	}()
	time.Sleep(300 * time.Millisecond)

	var buf syncBuffer
	log := slog.New(slog.NewJSONHandler(&buf, nil))
	o := MoveOptions{CloudDSN: dsn, Spec: spec, Org: 7, LockTimeout: 5 * time.Second, Attempts: 3, Log: log}
	mu.Lock()
	before := ops
	mu.Unlock()
	start := time.Now()
	err = splitOrg(ctx, cloud, o, log)
	took := time.Since(start)
	mu.Lock()
	during := ops - before
	mu.Unlock()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	if werr := <-done; werr != nil {
		t.Fatalf("the writer: %v (%d failed)", werr, failed)
	}
	if err != nil {
		t.Fatalf("split: %v\n%s", err, buf.String())
	}
	var paused string
	for _, l := range strings.Split(buf.String(), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["msg"] == "organization resumed" {
			if ns, ok := m["paused"].(float64); ok {
				paused = time.Duration(ns).Round(time.Millisecond).String()
			}
		}
	}
	t.Logf("split in %s, organization paused %s; the writer: %d changes during the split, %d failed, its longest transaction %s",
		took.Round(time.Millisecond), paused, during, failed, longest.Round(time.Millisecond))

	rows, err := rowsOf(ctx, cloud, "SELECT id::text, body FROM docs4 WHERE org_id = 7")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, r := range rows {
		got[r[0]] = r[1]
	}
	lost := 0
	for id, body := range want {
		if got[fmt.Sprint(id)] != body {
			lost++
		}
	}
	extra := len(got) - len(want)
	inDefault := review3One(t, dsn, "SELECT count(*) FROM docs4__cloud WHERE org_id = 7")
	attached := review3One(t, dsn, "SELECT count(*) FROM pg_inherits WHERE inhrelid = 'docs4__o7'::regclass")
	others := review3One(t, dsn, "SELECT count(*) FROM docs4 WHERE org_id = 8")
	left := review3One(t, dsn, `SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'lokadb\_move\_org7\_split%'`)
	t.Logf("organization 7: %d rows, %d expected, %d differ, %d extra; in DEFAULT %s; partition attached %s; organization 8: %s rows; split triggers left %s",
		len(got), len(want), lost, extra, inDefault, attached, others, left)
	if lost != 0 || extra != 0 || inDefault != "0" || attached != "1" || others != "1000" || left != "0" {
		t.Fatal("the organization's rows after the split are not what the writer made of them")
	}
	if during == 0 || longest > took/2 {
		t.Fatalf("the organization paused for most of the split: its writer's longest transaction %s of %s, %d changes during it", longest, took, during)
	}
}

// syncBuffer: a bytes.Buffer for a logger used from several goroutines.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
