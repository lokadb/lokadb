package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The exchanges with the cloud of a read by a cold key (inlinelookup.go), on a cloud a few
// milliseconds away: the stand of TestProfileLive (LOKADB_PROFILE_PG, set up already), the proxy
// in this process reaching the cloud through delayLink (LOKADB_EXCHANGE_DELAY_MS each way, 2.5 by
// default), one client, the read of bench/load (a document by its organization and id, then its
// versions by the document's id: the second statement's key is cold, a new document each time).
// It logs the median and p90 of a read and the cloud's committed transactions per read
// (pg_stat_database: a lookup outside a transaction is one of its own).
func TestExchangeLive(t *testing.T) {
	pg := os.Getenv("LOKADB_PROFILE_PG")
	if pg == "" {
		t.Skip("LOKADB_PROFILE_PG not set")
	}
	delay := 2500 * time.Microsecond
	if v, err := strconv.ParseFloat(os.Getenv("LOKADB_EXCHANGE_DELAY_MS"), 64); err == nil {
		delay = time.Duration(v * float64(time.Millisecond))
	}
	ctx := context.Background()
	su, err := pgx.Connect(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	defer su.Close(ctx)
	cfg := su.Config()
	link := delayLink(t, fmt.Sprintf("%s:%d", cfg.Host, cfg.Port), delay)
	lh, lp, _ := net.SplitHostPort(link)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	srv := NewServer(Config{Listen: addr, Upstream: fmt.Sprintf("host=%s port=%s sslmode=disable", lh, lp), NoRouteLog: true,
		Fanout: map[string]FanoutPolicy{"bench": {Mode: FanoutReject}}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	go func() { _ = srv.ListenAndServe() }()
	for i := 0; i < 100; i++ {
		if c, err := net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	h, p, _ := net.SplitHostPort(addr)
	c, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%s dbname=bench user=app password=app sslmode=disable", h, p))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	xacts := func() int64 {
		time.Sleep(1500 * time.Millisecond) // the statistics of busy sessions are flushed once they idle
		var n int64
		if err := su.QueryRow(ctx, "SELECT xact_commit FROM pg_stat_database WHERE datname = 'bench'").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	const orgs, rows = 20, 20000
	r := rand.New(rand.NewSource(1))
	read := func() {
		org := 6 + r.Intn(orgs-5)
		doc := (org-1)*rows + 1 + r.Intn(rows)
		var title string
		var n int
		if err := c.QueryRow(ctx, "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", org, doc).Scan(&title); err != nil && err != pgx.ErrNoRows {
			t.Fatal(err)
		}
		if err := c.QueryRow(ctx, "SELECT count(*) FROM document_versions WHERE document_id = $1", doc).Scan(&n); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ { // prepared statements, generic plans
		read()
	}
	x0 := xacts()
	const n = 300
	ds := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		t0 := time.Now()
		read()
		ds = append(ds, time.Since(t0))
	}
	x1 := xacts()
	slices.Sort(ds)
	t.Logf("delay %v each way: a read (two statements, the second by a cold key) p50 %v, p90 %v; the cloud's transactions %.2f a read",
		delay, ds[n/2].Round(10*time.Microsecond), ds[n*9/10].Round(10*time.Microsecond), float64(x1-x0)/n)
}

// delayLink: an address that forwards to target, every chunk delayed by d in each direction
// (in order, without holding the next one back: a pipeline stays a pipeline).
func delayLink(t *testing.T, target string, d time.Duration) string {
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	pipe := func(dst, src net.Conn) {
		type chunk struct {
			at time.Time
			b  []byte
		}
		ch := make(chan chunk, 1024)
		go func() {
			defer close(ch)
			for {
				buf := make([]byte, 64<<10)
				n, err := src.Read(buf)
				if n > 0 {
					ch <- chunk{time.Now().Add(d), buf[:n]}
				}
				if err != nil {
					return
				}
			}
		}()
		for c := range ch {
			time.Sleep(time.Until(c.at))
			if _, err := dst.Write(c.b); err != nil {
				return
			}
		}
		dst.Close()
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			u, err := net.Dial("tcp", target)
			if err != nil {
				c.Close()
				continue
			}
			go pipe(u, c)
			go pipe(c, u)
		}
	}()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return net.JoinHostPort("127.0.0.1", port)
}
