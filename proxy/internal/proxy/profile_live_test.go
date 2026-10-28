package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// Where the proxy's CPU goes under the paths scenario of block 14 (organizations in the cloud,
// 32 clients, pgx with the extended protocol, the operations of bench/load sqlOp): the proxy in
// this process, a cloud Postgres at LOKADB_PROFILE_PG (a superuser's DSN of a throwaway
// database), the profiles written to LOKADB_PROFILE_OUT. Every operation runs 20 s on its own;
// the result is its rate and a CPU and an allocation profile.
func TestProfileLive(t *testing.T) {
	pg, out := os.Getenv("LOKADB_PROFILE_PG"), os.Getenv("LOKADB_PROFILE_OUT")
	if pg == "" || out == "" {
		t.Skip("LOKADB_PROFILE_PG and LOKADB_PROFILE_OUT not set")
	}
	ctx := context.Background()
	su, err := pgx.Connect(ctx, pg)
	if err != nil {
		t.Fatal(err)
	}
	cfg := su.Config()
	host, port := cfg.Host, cfg.Port
	exec := func(c *pgx.Conn, sqls ...string) {
		t.Helper()
		for _, q := range sqls {
			if _, err := c.Exec(ctx, q); err != nil {
				t.Fatalf("%.100s: %v", q, err)
			}
		}
	}
	const orgs, rows = 20, 20000
	if os.Getenv("LOKADB_PROFILE_SETUP") != "" {
		exec(su, `DROP DATABASE IF EXISTS bench WITH (FORCE)`,
			`DO $$ BEGIN CREATE ROLE app LOGIN PASSWORD 'app'; EXCEPTION WHEN duplicate_object THEN END $$`,
			`CREATE DATABASE bench OWNER app`)
		app, err := pgx.Connect(ctx, fmt.Sprintf("host=%s port=%d dbname=bench user=app password=app sslmode=disable", host, port))
		if err != nil {
			t.Fatal(err)
		}
		exec(app, "CREATE TABLE users (id bigserial PRIMARY KEY, name text)",
			"CREATE TABLE documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint, title text, body text)",
			"CREATE TABLE document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE SET NULL, content text)",
			"INSERT INTO users (name) VALUES ('u')")
		for o := 1; o <= orgs; o++ {
			exec(app, fmt.Sprintf(`INSERT INTO documents (organization_id, user_id, title, body) SELECT %d, 1, 'doc ' || g, repeat('x', 200) FROM generate_series(1, %d) g`, o, rows))
		}
		exec(app, "INSERT INTO document_versions (document_id, content) SELECT id, 'v' FROM documents ORDER BY id", "ANALYZE")
		app.Close(ctx)
		spec := admin.Spec{Tables: []admin.SpecTable{{Name: "documents", TenantColumn: "organization_id"},
			{Name: "document_versions", ParentTable: "documents", ParentColumn: "document_id"}}}
		if err := admin.Enable(ctx, fmt.Sprintf("host=%s port=%d dbname=bench user=%s password=%s sslmode=disable", host, port, cfg.User, cfg.Password),
			spec, "app", slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
			t.Fatal(err)
		}
	}
	su.Close(ctx)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	srv := NewServer(Config{Listen: addr, Upstream: fmt.Sprintf("host=%s port=%d sslmode=disable", host, port), NoRouteLog: true,
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
	dsn := fmt.Sprintf("host=%s port=%s dbname=bench user=app password=app sslmode=disable", h, p)
	if d := os.Getenv("LOKADB_PROFILE_DIRECT"); d != "" { // the same load straight to Postgres
		dsn = fmt.Sprintf("host=%s port=%d dbname=bench user=app password=app sslmode=disable", host, port)
	}
	clients := 32
	dur := 20 * time.Second
	for _, op := range []string{"read", "feed", "update", "insert"} {
		conns := make([]*pgx.Conn, clients)
		for i := range conns {
			if conns[i], err = pgx.Connect(ctx, dsn); err != nil {
				t.Fatal(err)
			}
		}
		var n, errs atomic.Int64
		run := func(d time.Duration) {
			var wg sync.WaitGroup
			stop := time.Now().Add(d)
			for i, c := range conns {
				wg.Add(1)
				go func(i int, c *pgx.Conn) {
					defer wg.Done()
					r := rand.New(rand.NewSource(int64(i)))
					for time.Now().Before(stop) {
						org := 6 + r.Intn(orgs-5) // organizations of the cloud, as class "cloud"
						doc := (org-1)*rows + 1 + r.Intn(rows)
						if err := benchOp(ctx, c, op, org, doc, r); err != nil {
							errs.Add(1)
						} else {
							n.Add(1)
						}
					}
				}(i, c)
			}
			wg.Wait()
		}
		run(3 * time.Second) // warm
		n.Store(0)
		errs.Store(0)
		f, err := os.Create(filepath.Join(out, op+".cpu"))
		if err != nil {
			t.Fatal(err)
		}
		_ = pprof.StartCPUProfile(f)
		run(dur)
		pprof.StopCPUProfile()
		f.Close()
		runtime.GC()
		if f, err := os.Create(filepath.Join(out, op+".alloc")); err == nil {
			_ = pprof.Lookup("allocs").WriteTo(f, 0)
			f.Close()
		}
		t.Logf("%s: %.0f ops/s (%d errors)", op, float64(n.Load())/dur.Seconds(), errs.Load())
		for _, c := range conns {
			c.Close(ctx)
		}
	}
}

// benchOp: bench/load sqlOp's operations.
func benchOp(ctx context.Context, c *pgx.Conn, op string, org, doc int, r *rand.Rand) error {
	switch op {
	case "read":
		var t string
		var n int
		if err := c.QueryRow(ctx, "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", org, doc).Scan(&t); err != nil {
			return err
		}
		return c.QueryRow(ctx, "SELECT count(*) FROM document_versions WHERE document_id = $1", doc).Scan(&n)
	case "feed":
		rows, err := c.Query(ctx, "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id DESC LIMIT 20", org)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		return rows.Err()
	case "insert":
		return pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
			var id int64
			if err := tx.QueryRow(ctx, "INSERT INTO documents (organization_id, user_id, title, body) VALUES ($1, 1, 'bench', 'b') RETURNING id", org).Scan(&id); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'v')", id)
			return err
		})
	case "update":
		_, err := c.Exec(ctx, "UPDATE documents SET title = $3 WHERE organization_id = $1 AND id = $2", org, doc, fmt.Sprint("u", r.Int()))
		return err
	}
	return nil
}
