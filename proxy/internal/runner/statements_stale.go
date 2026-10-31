package runner

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Shapes recorded while the metadata was incomplete (an enable still running, an older spec, a table
// that was declared later) would stay in unrouted.json for good: the list is added up across proxy
// restarts and only a decision removes a shape. Every shape is therefore looked at again with the
// metadata as it is now, and one the proxy would route (it names the customer by a column it looks up) leaves
// the list.

var (
	staleMu   sync.Mutex
	staleLast time.Time
)

// staleEvery: how often the list is checked again on its own (the panel's review checks it each time).
const staleEvery = time.Minute

// pruneRouted drops from the open list the shapes the current metadata routes. conn: a connection to the
// application's database as the runner's administrator.
func (c *Config) pruneRouted(ctx context.Context, conn *pgx.Conn) int {
	meta, err := proxy.LoadMeta(func(sql string) ([][]string, error) { return textRows(ctx, conn, sql) })
	if err != nil || meta == nil || meta.Denied != "" {
		return 0
	}
	c.unr.mu.Lock()
	defer c.unr.mu.Unlock()
	c.loadUnrouted()
	n := 0
	for fp, x := range c.unr.items {
		if x.Full == "" { // too long to read again
			continue
		}
		a := proxy.Analyze(x.Full, meta)
		if a.ParseError || a.Forbidden != "" || !a.Lokadb || a.Unhinted {
			continue
		}
		delete(c.unr.items, fp)
		n++
	}
	if n > 0 {
		c.saveUnrouted()
		c.openInfo()
	}
	return n
}

// pruneRoutedSometimes is pruneRouted at most once in staleEvery.
func (c *Config) pruneRoutedSometimes(ctx context.Context) {
	staleMu.Lock()
	if time.Since(staleLast) < staleEvery {
		staleMu.Unlock()
		return
	}
	staleLast = time.Now()
	staleMu.Unlock()
	conn, err := ownsql.Connect(ctx, c.DB)
	if err != nil {
		return
	}
	defer conn.Close(context.Background())
	c.pruneRouted(ctx, conn)
}

// textRows runs sql and gives every value as text (the simple protocol answers in text).
func textRows(ctx context.Context, conn *pgx.Conn, sql string) ([][]string, error) {
	rows, err := conn.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		raw := rows.RawValues()
		r := make([]string, len(raw))
		for i, b := range raw {
			r[i] = string(b)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
