package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The default cache of cloud connections for a client that connects for every request (PHP),
// on a stand where organization 1 lives behind a tunnel with a delay (LOKADB_POOL_LIVE: the
// cloud's host and port, a database whose organization 1 is moved, its role app/app): a
// connection, one read of organization 1, the disconnection, 50 times, with upstream_pool false
// and by default; the median of each.
func TestPoolPerRequestLive(t *testing.T) {
	cloud := os.Getenv("LOKADB_POOL_LIVE") // e.g. "host=cloud port=5432"
	db := os.Getenv("LOKADB_POOL_LIVE_DB")
	if cloud == "" || db == "" {
		t.Skip("LOKADB_POOL_LIVE and LOKADB_POOL_LIVE_DB not set")
	}
	run := func(name string, pool *PoolConfig) time.Duration {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		srv := NewServer(Config{Listen: addr, Upstream: cloud + " sslmode=disable", UpstreamPool: pool, NoRouteLog: true},
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		go func() { _ = srv.ListenAndServe() }()
		for i := 0; i < 100; i++ {
			if c, err := net.Dial("tcp", addr); err == nil {
				c.Close()
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		h, p, _ := net.SplitHostPort(addr)
		dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=app password=app sslmode=disable", h, p, db)
		var ds []time.Duration
		for i := 0; i < 52; i++ {
			t0 := time.Now()
			c, err := pgx.Connect(context.Background(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			var title string
			if err := c.QueryRow(context.Background(), "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", 1, 1+i%20).Scan(&title); err != nil && err != pgx.ErrNoRows {
				t.Fatal(err)
			}
			c.Close(context.Background())
			if i >= 2 { // the first two: the metadata read, the pool's first connection
				ds = append(ds, time.Since(t0))
			}
			time.Sleep(50 * time.Millisecond) // the reset and the return to the pool are the proxy's, after the client went
		}
		slices.Sort(ds)
		t.Logf("%s: connect, read organization 1, disconnect: median %v, p90 %v (%d requests)", name, ds[len(ds)/2].Round(time.Millisecond), ds[len(ds)*9/10].Round(time.Millisecond), len(ds))
		return ds[len(ds)/2]
	}
	off := run("upstream_pool false", &PoolConfig{Off: true})
	on := run("the default", nil)
	t.Logf("per request: %v without the cache, %v with it", off.Round(time.Millisecond), on.Round(time.Millisecond))
}
