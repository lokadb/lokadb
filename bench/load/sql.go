package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// The benchmark schema is the one of the defects stand: users, documents (tenant column
// organization_id), document_versions (tenant derived through document_id). One organization, B (-big-org,
// default 1), may be bigger than the others (-big); the organizations are inserted in order, so the ids of
// organization o are
//
//	o < B:  (o-1)*rows + 1 .. o*rows
//	o = B:  (B-1)*rows + 1 .. (B-1)*rows + big
//	o > B:  big + (o-2)*rows + 1 .. big + (o-1)*rows
func idRange(org, rows, big, bigOrg int) (lo, n int) {
	if big <= 0 {
		big = rows
	}
	if bigOrg < 1 {
		bigOrg = 1
	}
	switch {
	case org < bigOrg:
		return (org-1)*rows + 1, rows
	case org == bigOrg:
		return (bigOrg-1)*rows + 1, big
	}
	return big + (org-2)*rows + 1, rows
}

func setup(args []string) error {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	dsn := fs.String("dsn", "", "owner of the database")
	orgs := fs.Int("orgs", 2, "organizations")
	rows := fs.Int("rows", 1000, "documents per organization")
	big := fs.Int("big", 0, "documents of the big organization (-big-org; default: rows)")
	bigOrg := fs.Int("big-org", 1, "the organization that has -big documents")
	body := fs.Int("body", 200, "bytes of a document body")
	versions := fs.Bool("versions", true, "one version per document")
	chunk := fs.Int("chunk", 2000000, "rows per INSERT")
	feedIndex := fs.Bool("feed-index", true, "an index on (organization_id, id), as an application with a feed has: the feed's ORDER BY id DESC LIMIT reads the tail of one organization from it")
	_ = fs.Parse(args)
	ctx := context.Background()
	c, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	for _, s := range []string{
		"CREATE TABLE IF NOT EXISTS users (id bigserial PRIMARY KEY, name text)",
		"CREATE TABLE IF NOT EXISTS documents (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, user_id bigint, title text, body text)",
		"CREATE TABLE IF NOT EXISTS document_versions (id bigserial PRIMARY KEY, document_id bigint REFERENCES documents ON DELETE SET NULL, content text)",
		"INSERT INTO users (name) SELECT 'u' WHERE NOT EXISTS (SELECT 1 FROM users)",
	} {
		if _, err := c.Exec(ctx, s); err != nil {
			return err
		}
	}
	t0 := time.Now()
	total := 0
	for o := 1; o <= *orgs; o++ {
		n := *rows
		if o == *bigOrg && *big > 0 {
			n = *big
		}
		for done := 0; done < n; done += *chunk {
			k := min(*chunk, n-done)
			// ORDER BY keeps the ids of one organization consecutive
			if _, err := c.Exec(ctx, fmt.Sprintf(`INSERT INTO documents (organization_id, user_id, title, body)
				SELECT %d, 1, 'doc ' || g, repeat('x', %d) FROM generate_series(1, %d) g`, o, *body, k)); err != nil {
				return err
			}
		}
		total += n
	}
	if *feedIndex {
		if _, err := c.Exec(ctx, "CREATE INDEX IF NOT EXISTS documents_org_id ON documents (organization_id, id)"); err != nil {
			return err
		}
	}
	if *versions {
		if _, err := c.Exec(ctx, "INSERT INTO document_versions (document_id, content) SELECT id, 'v' FROM documents ORDER BY id"); err != nil {
			return err
		}
	}
	fmt.Printf("{\"setup\": true, \"orgs\": %d, \"documents\": %d, \"seconds\": %.1f}\n", *orgs, total, time.Since(t0).Seconds())
	return nil
}

// opsOf: the operation names of a mix, checked.
func opsOf(l *loadFlags) ([]string, error) {
	ops := opNames(l.ops)
	for _, o := range ops {
		switch o {
		case "read", "feed", "insert", "update", "report":
		default:
			return nil, fmt.Errorf("unknown operation %q (read, feed, insert, update, report)", o)
		}
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("no operations")
	}
	return ops, nil
}

func runLoop(mode string, args []string) error {
	fs := flag.NewFlagSet(mode, flag.ExitOnError)
	var l loadFlags
	l.bind(fs)
	dsn := fs.String("dsn", "", "sql: connection string")
	base := fs.String("base", "", "http: base URL")
	big := fs.Int("big", 0, "documents of the big organization (as in setup)")
	bigOrg := fs.Int("big-org", 1, "the big organization (as in setup)")
	simple := fs.Bool("simple", false, "sql: the simple query protocol")
	reconnect := fs.Bool("reconnect", false, "sql: a connection for every operation (connect, operation, close), as PHP without persistent connections does")
	setOrg := fs.Bool("set-org", false, "sql: set lokadb.org before each operation, as an application connected straight to the cloud has to (the baseline of the proxy's own cost)")
	coop := fs.Bool("coop", false, "sql: name the organization in every transaction (set_config lokadb.tenant), as a cooperating application does")
	_ = fs.Parse(args)
	ops, err := opsOf(&l)
	if err != nil {
		return err
	}
	// SIGINT/SIGTERM ends the run early and still prints what was measured (a writer that runs
	// "until the enable is over" is stopped this way)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mk := func(id int) worker {
		r := rand.New(rand.NewSource(l.seed*1000 + int64(id)))
		pick := func() (org, doc int) {
			org = l.firstOrg + r.Intn(l.orgs)
			lo, n := idRange(org, l.rows, *big, *bigOrg)
			if l.hot > 0 && l.hot < n {
				n = l.hot
			}
			return org, lo + r.Intn(n)
		}
		if mode == "http" {
			cl := &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4}}
			do := func(method, url string, body string) error {
				req, _ := http.NewRequestWithContext(ctx, method, url, strings.NewReader(body))
				if body != "" {
					req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				}
				resp, err := cl.Do(req)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				b, _ := io.ReadAll(resp.Body)
				if resp.StatusCode >= 300 {
					return fmt.Errorf("HTTP %d: %s", resp.StatusCode, trim(string(b), 120))
				}
				return nil
			}
			return worker{close: cl.CloseIdleConnections, do: func(ctx context.Context) (string, string, error) {
				op := ops[r.Intn(len(ops))]
				org, doc := pick()
				var err error
				switch op {
				case "read":
					err = do("GET", fmt.Sprintf("%s/doc/%d/%d", *base, org, doc), "")
				case "feed":
					err = do("GET", fmt.Sprintf("%s/feed/%d", *base, org), "")
				case "insert":
					err = do("POST", fmt.Sprintf("%s/doc/%d", *base, org), "title=bench")
				case "update":
					err = do("PUT", fmt.Sprintf("%s/doc/%d/%d", *base, org, doc), fmt.Sprintf("title=u%d", r.Int()))
				case "report":
					err = do("GET", *base+"/report", "")
				}
				return op, l.class(org), err
			}}
		}
		var c *pgx.Conn
		connect := func() error {
			cfg, err := pgx.ParseConfig(*dsn)
			if err != nil {
				return err
			}
			if *simple {
				cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
			}
			c, err = pgx.ConnectConfig(ctx, cfg)
			return err
		}
		return worker{close: func() {
			if c != nil {
				c.Close(context.Background())
			}
		}, do: func(ctx context.Context) (string, string, error) {
			op := ops[r.Intn(len(ops))]
			org, doc := pick()
			if c == nil || c.IsClosed() {
				if err := connect(); err != nil {
					time.Sleep(50 * time.Millisecond)
					return op, l.class(org), err
				}
			}
			var err error
			if *setOrg {
				if _, err = c.Exec(ctx, "SELECT set_config('lokadb.org', $1, false)", fmt.Sprint(org)); err != nil {
					return op, l.class(org), err
				}
			}
			if *coop {
				// the cooperative mode of the application: the organization is named in the transaction
				err = pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
					if _, err := tx.Exec(ctx, "SELECT set_config('lokadb.tenant', $1, true)", fmt.Sprint(org)); err != nil {
						return err
					}
					return sqlOp(ctx, tx, true, op, org, doc, r)
				})
			} else {
				err = sqlOp(ctx, c, false, op, org, doc, r)
			}
			if err != nil && c.IsClosed() || *reconnect {
				c.Close(context.Background())
				c = nil
			}
			return op, l.class(org), err
		}}
	}
	t0 := time.Now()
	stagger := l.stagger
	if stagger < 0 {
		stagger = min(l.warm/2, 10*time.Second)
	}
	var all map[string]*stats
	var tl *timeline
	if l.rate > 0 {
		all, tl = openLoop(ctx, l.clients, l.rate, l.timeout, l.warm, l.dur, mk)
	} else {
		all, tl = loop(ctx, l.clients, l.warm, l.dur, stagger, mk)
	}
	if el := time.Since(t0) - l.warm; el < l.dur && el > 0 {
		l.dur = el
	}
	var names []string
	for k := range all {
		names = append(names, k)
	}
	sortStrings(names)
	for _, k := range names {
		emit(all[k].result(k, l.clients, l.dur.Seconds()))
	}
	return tl.write(l.timeline)
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

// connectBench: what a new client pays: the connection (startup, authentication) and the first
// query, which for the proxy is also where it reads its metadata and, for an organization, sets it.
func connectBench(args []string) error {
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	n := fs.Int("n", 300, "samples")
	org := fs.Int("org", 1, "organization of the first query")
	coop := fs.Bool("coop", false, "the first query in a transaction that names the organization (set_config lokadb.tenant), as direct requires")
	_ = fs.Parse(args)
	ctx := context.Background()
	var conn, first stats
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		c, err := pgx.Connect(ctx, *dsn)
		conn.add(time.Since(t0), err)
		if err != nil {
			continue
		}
		t1 := time.Now()
		var x int
		const q = "SELECT count(*) FROM documents WHERE organization_id = $1 AND id = $2"
		if *coop {
			err = pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, "SELECT set_config('lokadb.tenant', $1, true)", fmt.Sprint(*org)); err != nil {
					return err
				}
				return tx.QueryRow(ctx, q, *org, 1).Scan(&x)
			})
		} else {
			err = c.QueryRow(ctx, q, *org, 1).Scan(&x)
		}
		first.add(time.Since(t1), err)
		c.Close(ctx)
	}
	emit(conn.result("connect", 1, 1))
	emit(first.result("first_query", 1, 1))
	return nil
}

// copyBench: COPY of one organization through the connection, out (query form and the table) and in.
func copyBench(args []string) error {
	fs := flag.NewFlagSet("copy", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	org := fs.Int("org", 1, "")
	inRows := fs.Int("in-rows", 10000, "rows of COPY FROM")
	_ = fs.Parse(args)
	ctx := context.Background()
	c, err := pgconn.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	one := func(name, sql string, in io.Reader, rows int) {
		var st stats
		t0 := time.Now()
		var buf bytes.Buffer
		var e error
		if in == nil {
			_, e = c.CopyTo(ctx, &buf, sql)
		} else {
			_, e = c.CopyFrom(ctx, in, sql)
		}
		st.add(time.Since(t0), e)
		r := st.result(name, 1, time.Since(t0).Seconds())
		r.Extra = map[string]any{"bytes": buf.Len(), "rows_out": strings.Count(buf.String(), "\n"), "rows_in": rows}
		emit(r)
	}
	one("copy_out_query", fmt.Sprintf("COPY (SELECT id, title FROM documents WHERE organization_id = %d) TO STDOUT", *org), nil, 0)
	one("copy_out_table", "COPY documents TO STDOUT", nil, 0)
	var sb strings.Builder
	for i := 0; i < *inRows; i++ {
		fmt.Fprintf(&sb, "%d\t1\tcopy %d\n", *org, i)
	}
	one("copy_in", "COPY documents (organization_id, user_id, title) FROM STDIN", strings.NewReader(sb.String()), *inRows)
	return nil
}

// cursorBench: a whole organization read through a cursor in a transaction.
func cursorBench(args []string) error {
	fs := flag.NewFlagSet("cursor", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	org := fs.Int("org", 1, "")
	fetch := fs.Int("fetch", 1000, "rows per FETCH")
	n := fs.Int("n", 5, "repeats")
	_ = fs.Parse(args)
	ctx := context.Background()
	c, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	var total, fetches stats
	rowsSeen := 0
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		err := pgx.BeginFunc(ctx, c, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, fmt.Sprintf("DECLARE bc CURSOR FOR SELECT id, title FROM documents WHERE organization_id = %d ORDER BY id", *org)); err != nil {
				return err
			}
			for {
				f0 := time.Now()
				rows, err := tx.Query(ctx, fmt.Sprintf("FETCH %d FROM bc", *fetch))
				if err != nil {
					return err
				}
				k := 0
				for rows.Next() {
					k++
				}
				if err := rows.Err(); err != nil {
					return err
				}
				fetches.add(time.Since(f0), nil)
				rowsSeen += k
				if k < *fetch {
					return nil
				}
			}
		})
		total.add(time.Since(t0), err)
	}
	r := total.result("cursor_total", 1, 1)
	r.Extra = map[string]any{"rows_per_run": rowsSeen / max(*n, 1), "fetch": *fetch}
	emit(r)
	emit(fetches.result("cursor_fetch", 1, 1))
	return nil
}

// notifyBench: how long a notification takes from NOTIFY to the listener, both through the same path.
func notifyBench(args []string) error {
	fs := flag.NewFlagSet("notify", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	n := fs.Int("n", 200, "")
	_ = fs.Parse(args)
	ctx := context.Background()
	got := make(chan string, 1024)
	cfg, err := pgconn.ParseConfig(*dsn)
	if err != nil {
		return err
	}
	cfg.OnNotification = func(_ *pgconn.PgConn, nt *pgconn.Notification) { got <- nt.Payload }
	l, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer l.Close(ctx)
	if _, err := l.Exec(ctx, "LISTEN bench").ReadAll(); err != nil {
		return err
	}
	s, err := pgconn.Connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer s.Close(ctx)
	var st stats
	for i := 0; i < *n; i++ {
		t0 := time.Now()
		if _, err := s.Exec(ctx, fmt.Sprintf("NOTIFY bench, '%d'", i)).ReadAll(); err != nil {
			st.add(0, err)
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := l.WaitForNotification(wctx)
		cancel()
		st.add(time.Since(t0), err)
		time.Sleep(20 * time.Millisecond)
	}
	emit(st.result("notify", 1, 1))
	return nil
}

// querier is what a connection and a transaction share.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// sqlOp: one operation of the mix; inTx says q is already a transaction.
func sqlOp(ctx context.Context, q querier, inTx bool, op string, org, doc int, r *rand.Rand) error {
	switch op {
	case "read":
		// as an ORM does it: the document, then its versions by the document's id
		var n int
		var t string
		if err := q.QueryRow(ctx, "SELECT title FROM documents WHERE organization_id = $1 AND id = $2", org, doc).Scan(&t); err != nil {
			return err
		}
		return q.QueryRow(ctx, "SELECT count(*) FROM document_versions WHERE document_id = $1", doc).Scan(&n)
	case "feed":
		rows, err := q.Query(ctx, "SELECT id, title FROM documents WHERE organization_id = $1 ORDER BY id DESC LIMIT 20", org)
		if err != nil {
			return err
		}
		for rows.Next() {
		}
		return rows.Err()
	case "insert":
		ins := func(q querier) error {
			var id int64
			if err := q.QueryRow(ctx, "INSERT INTO documents (organization_id, user_id, title, body) VALUES ($1, 1, 'bench', 'b') RETURNING id", org).Scan(&id); err != nil {
				return err
			}
			_, err := q.Exec(ctx, "INSERT INTO document_versions (document_id, content) VALUES ($1, 'v')", id)
			return err
		}
		if inTx {
			return ins(q)
		}
		return pgx.BeginFunc(ctx, q.(*pgx.Conn), func(tx pgx.Tx) error { return ins(tx) })
	case "update":
		_, err := q.Exec(ctx, "UPDATE documents SET title = $3 WHERE organization_id = $1 AND id = $2", org, doc, fmt.Sprint("u", r.Int()))
		return err
	case "report":
		var n int
		return q.QueryRow(ctx, "SELECT count(*) FROM documents WHERE title LIKE $1", "doc 1%").Scan(&n)
	}
	return nil
}
