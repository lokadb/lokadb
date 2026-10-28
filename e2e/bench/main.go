// bench measures latency and throughput of one connection path to the documents table.
//
//	bench setup  -dsn DSN -rows N           create the table and N documents per organization (1 and 2)
//	bench run    -path NAME -dsn DSN -org N  measure, print one JSON line
//	bench report FILE...                    JSON lines -> markdown tables
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var ctx = context.Background()

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: bench setup|run|report ...")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "setup":
		setup(os.Args[2:])
	case "run":
		run(os.Args[2:])
	case "report":
		report(os.Args[2:])
	case "read":
		readOnly(os.Args[2:])
	}
}

var body = strings.Repeat("lorem ipsum dolor sit amet ", 19)[:500]

func setup(args []string) {
	fs := flag.NewFlagSet("setup", flag.ExitOnError)
	dsn := fs.String("dsn", "", "")
	rows := fs.Int("rows", 20000, "documents per organization")
	_ = fs.Parse(args)
	c, err := pgx.Connect(ctx, *dsn)
	must(err)
	defer c.Close(ctx)
	_, err = c.Exec(ctx, `CREATE TABLE documents (
		id bigserial PRIMARY KEY, organization_id bigint NOT NULL, title text NOT NULL, body text NOT NULL)`)
	must(err)
	_, err = c.Exec(ctx, `CREATE INDEX documents_org_idx ON documents (organization_id)`)
	must(err)
	_, err = c.Exec(ctx, `INSERT INTO documents (organization_id, title, body)
		SELECT o, 'doc ' || g, $1 FROM generate_series(1, 2) o, generate_series(1, $2::int) g`, body, *rows)
	must(err)
	_, err = c.Exec(ctx, `ANALYZE documents`)
	must(err)
	fmt.Printf("setup: %d documents per organization\n", *rows)
}

type stat struct {
	N    int     `json:"n"`
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	Mean float64 `json:"mean_ms"`
	Errs int     `json:"errors"`
}

type result struct {
	Path       string          `json:"path"`
	Latency    map[string]stat `json:"latency"`
	QPS        float64         `json:"select_qps"`
	QPSErrs    int64           `json:"select_qps_errors"`
	ReadMBs    float64         `json:"bulk_read_mb_s"`
	ReadRows   int             `json:"bulk_read_rows"`
	ReadSec    float64         `json:"bulk_read_s"`
	InsertRows float64         `json:"bulk_insert_rows_s"`
}

func measure(n int, fn func(i int) error) stat {
	for i := 0; i < n/10; i++ { // warm-up: connections, prepared statements, caches
		_ = fn(-1 - i)
	}
	var d []float64
	errs := 0
	for i := 0; i < n; i++ {
		t := time.Now()
		if err := fn(i); err != nil {
			errs++
			if errs == 1 {
				fmt.Fprintln(os.Stderr, "first error:", err)
			}
			continue
		}
		d = append(d, float64(time.Since(t).Microseconds())/1000)
	}
	sort.Float64s(d)
	s := stat{N: len(d), Errs: errs}
	if len(d) == 0 {
		return s
	}
	q := func(p float64) float64 { return d[int(p*float64(len(d)-1))] }
	sum := 0.0
	for _, v := range d {
		sum += v
	}
	s.P50, s.P95, s.P99, s.Mean = q(0.5), q(0.95), q(0.99), sum/float64(len(d))
	return s
}

func run(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("path", "", "name of the path")
	dsn := fs.String("dsn", "", "")
	org := fs.Int64("org", 1, "organization to work on")
	explicitIDs := fs.Bool("explicit-ids", false, "the table has no id default (customer database reached directly)")
	n := fs.Int("n", 1000, "queries per latency test")
	conc := fs.Int("conc", 16, "connections for the throughput test")
	dur := fs.Duration("dur", 10*time.Second, "duration of the throughput test")
	batches := fs.Int("batches", 20, "bulk insert statements of 500 rows")
	_ = fs.Parse(args)

	c, err := pgx.Connect(ctx, *dsn)
	must(err)
	defer c.Close(ctx)
	ids, err := idsOf(c, *org)
	must(err)
	if len(ids) == 0 {
		must(fmt.Errorf("no documents for organization %d", *org))
	}
	r := result{Path: *path, Latency: map[string]stat{}}
	pick := func() int64 { return ids[rand.Intn(len(ids))] }
	nextID := time.Now().UnixNano() % 1e12 * 1000 // explicit ids: far above the sequence
	var created []int64
	insert := func(conn *pgx.Conn) (int64, error) {
		var id int64
		if *explicitIDs {
			nextID++
			_, err := conn.Exec(ctx, `INSERT INTO documents (id, organization_id, title, body) VALUES ($1, $2, $3, $4)`,
				nextID, *org, "bench", body)
			return nextID, err
		}
		err := conn.QueryRow(ctx, `INSERT INTO documents (organization_id, title, body) VALUES ($1, $2, $3) RETURNING id`,
			*org, "bench", body).Scan(&id)
		return id, err
	}

	var title, b string
	var id int64
	r.Latency["select_by_org_and_id"] = measure(*n, func(int) error {
		return c.QueryRow(ctx, `SELECT id, title, body FROM documents WHERE organization_id = $1 AND id = $2`, *org, pick()).Scan(&id, &title, &b)
	})
	r.Latency["select_by_id_only"] = measure(*n, func(int) error {
		return c.QueryRow(ctx, `SELECT id, title, body FROM documents WHERE id = $1`, pick()).Scan(&id, &title, &b)
	})
	r.Latency["insert_one"] = measure(*n, func(int) error {
		id, err := insert(c)
		if err == nil {
			created = append(created, id)
		}
		return err
	})
	r.Latency["update_one"] = measure(*n, func(i int) error {
		_, err := c.Exec(ctx, `UPDATE documents SET title = $1 WHERE organization_id = $2 AND id = $3`, fmt.Sprint("t", i), *org, pick())
		return err
	})

	// Throughput: conc connections doing point selects for dur.
	var ops, errs atomic.Int64
	var wg sync.WaitGroup
	stop := time.Now().Add(*dur)
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cc, err := pgx.Connect(ctx, *dsn)
			if err != nil {
				errs.Add(1)
				return
			}
			defer cc.Close(ctx)
			var id int64
			var t, b string
			for time.Now().Before(stop) {
				if err := cc.QueryRow(ctx, `SELECT id, title, body FROM documents WHERE organization_id = $1 AND id = $2`, *org, pick()).Scan(&id, &t, &b); err != nil {
					errs.Add(1)
					continue
				}
				ops.Add(1)
			}
		}()
	}
	wg.Wait()
	r.QPS, r.QPSErrs = float64(ops.Load())/dur.Seconds(), errs.Load()

	// Bulk read: every row of the organization, three times.
	start := time.Now()
	var bytes int64
	for k := 0; k < 3; k++ {
		rows, err := c.Query(ctx, `SELECT id, title, body FROM documents WHERE organization_id = $1`, *org)
		must(err)
		r.ReadRows = 0
		for rows.Next() {
			must(rows.Scan(&id, &title, &b))
			bytes += int64(8 + len(title) + len(b))
			r.ReadRows++
		}
		must(rows.Err())
	}
	r.ReadSec = time.Since(start).Seconds() / 3
	r.ReadMBs = float64(bytes) / 1e6 / time.Since(start).Seconds()

	// Bulk insert: 20 statements of 500 rows (multi-row VALUES, as ORMs batch).
	start = time.Now()
	const per = 500
	for k := 0; k < *batches; k++ {
		var sb strings.Builder
		var params []any
		cols := "organization_id, title, body"
		if *explicitIDs {
			cols = "id, organization_id, title, body"
		}
		fmt.Fprintf(&sb, "INSERT INTO documents (%s) VALUES ", cols)
		for j := 0; j < per; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			p := len(params)
			if *explicitIDs {
				nextID++
				params = append(params, nextID)
				created = append(created, nextID)
				fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d)", p+1, p+2, p+3, p+4)
			} else {
				fmt.Fprintf(&sb, "($%d, $%d, $%d)", p+1, p+2, p+3)
			}
			params = append(params, *org, "bulk", body)
		}
		if !*explicitIDs {
			sb.WriteString(" RETURNING id")
			rows, err := c.Query(ctx, sb.String(), params...)
			must(err)
			for rows.Next() {
				must(rows.Scan(&id))
				created = append(created, id)
			}
			must(rows.Err())
		} else {
			_, err := c.Exec(ctx, sb.String(), params...)
			must(err)
		}
	}
	r.InsertRows = float64(*batches*per) / time.Since(start).Seconds()

	// Leave the data as it was.
	_, err = c.Exec(ctx, `DELETE FROM documents WHERE organization_id = $1 AND id = ANY($2)`, *org, created)
	must(err)
	out, _ := json.Marshal(r)
	fmt.Println(string(out))
}

func idsOf(c *pgx.Conn, org int64) ([]int64, error) {
	rows, err := c.Query(ctx, `SELECT id FROM documents WHERE organization_id = $1`, org)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}

func report(files []string) {
	var rs []result
	for _, f := range files {
		fh, err := os.Open(f)
		must(err)
		sc := bufio.NewScanner(fh)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var r result
			if json.Unmarshal(sc.Bytes(), &r) == nil && r.Path != "" {
				rs = append(rs, r)
			}
		}
		fh.Close()
	}
	tests := []string{"select_by_org_and_id", "select_by_id_only", "insert_one", "update_one"}
	fmt.Println("| path | " + strings.Join(tests, " | ") + " |")
	fmt.Println("|---|" + strings.Repeat("---|", len(tests)))
	for _, r := range rs {
		cells := []string{r.Path}
		for _, t := range tests {
			s := r.Latency[t]
			cell := fmt.Sprintf("%.2f / %.2f / %.2f", s.P50, s.P95, s.P99)
			if s.Errs > 0 {
				cell += fmt.Sprintf(" (%d err)", s.Errs)
			}
			cells = append(cells, cell)
		}
		fmt.Println("| " + strings.Join(cells, " | ") + " |")
	}
	fmt.Println()
	fmt.Println("| path | select QPS (16 conns) | bulk read MB/s | bulk read of one org, s | bulk insert rows/s |")
	fmt.Println("|---|---|---|---|---|")
	for _, r := range rs {
		e := ""
		if r.QPSErrs > 0 {
			e = fmt.Sprintf(" (%d err)", r.QPSErrs)
		}
		fmt.Printf("| %s | %.0f%s | %.1f | %.2f (%d rows) | %.0f |\n", r.Path, r.QPS, e, r.ReadMBs, r.ReadSec, r.ReadRows, r.InsertRows)
	}
}

// readOnly times full reads of one organization's rows: median seconds and MB/s.
func readOnly(args []string) {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	path := fs.String("path", "", "label")
	dsn := fs.String("dsn", "", "")
	org := fs.Int64("org", 1, "")
	repeat := fs.Int("repeat", 5, "reads (after one warm-up)")
	_ = fs.Parse(args)
	c, err := pgx.Connect(ctx, *dsn)
	must(err)
	defer c.Close(ctx)
	read := func() (int, int64, time.Duration) {
		start := time.Now()
		rows, err := c.Query(ctx, `SELECT id, title, body FROM documents WHERE organization_id = $1`, *org)
		must(err)
		var n int
		var bytes int64
		var id int64
		var t, b string
		for rows.Next() {
			must(rows.Scan(&id, &t, &b))
			bytes += int64(8 + len(t) + len(b))
			n++
		}
		must(rows.Err())
		return n, bytes, time.Since(start)
	}
	read() // warm-up: connection, plan, FDW connection
	var secs []float64
	var n int
	var bytes int64
	for i := 0; i < *repeat; i++ {
		var d time.Duration
		n, bytes, d = read()
		secs = append(secs, d.Seconds())
	}
	sort.Float64s(secs)
	med := secs[len(secs)/2]
	fmt.Printf(`{"path":%q,"window_kb":%q,"rows":%d,"mb":%.1f,"median_s":%.3f,"min_s":%.3f,"max_s":%.3f,"mb_s":%.1f}`+"\n",
		*path, os.Getenv("WINDOW_KB"), n, float64(bytes)/1e6, med, secs[0], secs[len(secs)-1], float64(bytes)/1e6/med)
}
