// loadgen writes through the proxy for two organizations while one of them is being
// moved, then checks that every committed write is still there with its last value.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

type doc struct {
	id    int64
	org   int64
	title string
}

func main() {
	dsn := flag.String("dsn", "", "connection string (through the proxy)")
	docsT := flag.String("docs", "documents", "documents table")
	versT := flag.String("versions", "document_versions", "versions table")
	dur := flag.Duration("duration", 20*time.Second, "how long to write")
	bulk := flag.Int("bulk", 0, "documents to pre-insert per organization (makes the initial copy longer)")
	untilMoved := flag.Int64("until-moved", 0, "keep writing until this organization is in its own database, then 3s more")
	untilBack := flag.Int64("until-back", 0, "keep writing until this organization is back in the cloud (move-back), then 3s more")
	keep := flag.Bool("keep", false, "do not delete the rows written (faster for big runs)")
	flag.Parse()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		fmt.Println("LOAD FAIL connect:", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	orgs := []struct{ org, project int64 }{{1, 1}, {2, 2}}
	var docs []*doc
	var latencies []time.Duration
	errorsSeen, updates := 0, 0
	run := time.Now().UnixNano()
	if *bulk > 0 {
		t0 := time.Now()
		for _, o := range orgs {
			_, err := conn.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (organization_id, project_id, slug, title, body)
				SELECT $1::bigint, $2::bigint, 'bulk-%d-' || $1::text || '-' || g, 'bulk', repeat('x', 200) FROM generate_series(1, $3::int) g`, *docsT, run),
				o.org, o.project, *bulk)
			if err != nil {
				fmt.Println("LOAD FAIL bulk insert:", err)
				os.Exit(1)
			}
		}
		fmt.Printf("bulk: %d documents per organization in %s\n", *bulk, time.Since(t0).Round(time.Millisecond))
	}
	end := time.Now().Add(*dur)
	var movedAt time.Time
	for i := 0; ; i++ {
		if *untilMoved != 0 {
			if movedAt.IsZero() && i%10 == 0 {
				var n int
				// moved = its partition is a foreign table now (the proxy keeps the lokadb schema to itself)
				if conn.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_foreign_table f ON f.ftrelid = c.oid WHERE c.relname = $1`,
					fmt.Sprintf("%s__org%d", *docsT, *untilMoved)).Scan(&n) == nil && n == 1 {
					movedAt = time.Now()
					fmt.Printf("organization %d is now in its own database (after %d writes)\n", *untilMoved, i)
				}
			}
			if !movedAt.IsZero() && time.Since(movedAt) > 3*time.Second {
				break
			}
		}
		if *untilBack != 0 {
			if movedAt.IsZero() && i%10 == 0 {
				var n int
				// back = its partition is a local table again
				if conn.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_inherits i ON i.inhrelid = c.oid WHERE c.relname = $1 AND c.relkind = 'r'`,
					fmt.Sprintf("%s__o%d", *docsT, *untilBack)).Scan(&n) == nil && n == 1 {
					movedAt = time.Now()
					fmt.Printf("organization %d is back in the cloud (after %d writes)\n", *untilBack, i)
				}
			}
			if !movedAt.IsZero() && time.Since(movedAt) > 3*time.Second {
				break
			}
		}
		if time.Now().After(end) {
			if *untilBack != 0 {
				fmt.Println("LOAD FAIL the organization was not back in time")
				os.Exit(1)
			}
			if *untilMoved != 0 {
				fmt.Println("LOAD FAIL the organization was not moved in time")
				os.Exit(1)
			}
			break
		}
		o := orgs[i%2]
		t0 := time.Now()
		d := &doc{org: o.org, title: fmt.Sprintf("load %d", i)}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			err := tx.QueryRow(ctx, fmt.Sprintf(
				`INSERT INTO %s (organization_id, project_id, slug, title, body) VALUES ($1, $2, $3, $4, $5) RETURNING id`, *docsT),
				o.org, o.project, fmt.Sprintf("load-%d-%d", run, i), d.title, "load body").Scan(&d.id)
			if err != nil {
				return err
			}
			_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (document_id, content) VALUES ($1, $2)`, *versT), d.id, "v1")
			return err
		})
		if err == nil {
			docs = append(docs, d)
		}
		if err == nil && i%3 == 0 && len(docs) > 1 {
			target := docs[rand.Intn(len(docs))]
			title := fmt.Sprintf("updated %d", i)
			_, err = conn.Exec(ctx, fmt.Sprintf(`UPDATE %s SET title = $1 WHERE id = $2`, *docsT), title, target.id)
			if err == nil {
				target.title = title
				updates++
			}
		}
		latencies = append(latencies, time.Since(t0))
		if err != nil {
			errorsSeen++
			if errorsSeen <= 5 {
				fmt.Println("write error:", err)
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	missing, wrong := 0, 0
	perOrg := map[int64]int{}
	for _, d := range docs {
		var title string
		var versions int
		err := conn.QueryRow(ctx, fmt.Sprintf(
			`SELECT d.title, (SELECT count(*) FROM %s v WHERE v.document_id = d.id) FROM %s d WHERE d.id = $1`, *versT, *docsT),
			d.id).Scan(&title, &versions)
		switch {
		case err == pgx.ErrNoRows:
			missing++
		case err != nil:
			fmt.Println("read error:", err)
			missing++
		case title != d.title || versions != 1:
			wrong++
		}
		perOrg[d.org]++
	}
	for _, o := range orgs {
		var n int
		if err := conn.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE organization_id = $1 AND slug LIKE $2`, *docsT),
			o.org, fmt.Sprintf("bulk-%d-%%", run)).Scan(&n); err != nil || n != *bulk {
			fmt.Printf("bulk rows of org %d: %d (want %d) %v\n", o.org, n, *bulk, err)
			missing++
		}
	}
	// Leave the application's own data as it was. Versions first: not every schema
	// cascades (Django's FKs do not, the ORM does it).
	for _, pattern := range []string{"load-%d-%%", "bulk-%d-%%"} {
		if *keep {
			break
		}
		like := fmt.Sprintf(pattern, run)
		for _, sql := range []string{
			fmt.Sprintf(`DELETE FROM %s WHERE document_id IN (SELECT id FROM %s WHERE slug LIKE $1)`, *versT, *docsT),
			fmt.Sprintf(`DELETE FROM %s WHERE slug LIKE $1`, *docsT),
		} {
			if _, err := conn.Exec(ctx, sql, like); err != nil {
				fmt.Println("cleanup error:", err)
				errorsSeen++
			}
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p := func(q float64) time.Duration {
		return latencies[int(q*float64(len(latencies)-1))].Round(time.Millisecond)
	}
	fmt.Printf("load: committed org1=%d org2=%d updates=%d write_errors=%d | latency p50=%s p99=%s max=%s | missing=%d wrong=%d\n",
		perOrg[1], perOrg[2], updates, errorsSeen, p(0.5), p(0.99), p(1), missing, wrong)
	if missing > 0 || wrong > 0 || errorsSeen > 0 || perOrg[1] == 0 {
		fmt.Println("LOAD FAIL")
		os.Exit(1)
	}
	fmt.Println("LOAD OK  every committed write survived the move")
}
