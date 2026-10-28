package admin

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// The catch-up's rates: 1 845 changes/s coming, 1 570 applied (block 14, 27.10), the log left
// grows: never, stalled from the start; then applied faster than they come: an ETA, and the log
// left shorter at every sample, though still longer than at the start: not stalled.
func TestConvergence(t *testing.T) {
	c := &convergence{}
	t0 := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	if r := c.add(t0, 0, 10000); r.known {
		t.Fatalf("one sample: %+v", r)
	}
	var r convRates
	applied, left := int64(0), int64(10000)
	for s := 1; s <= 24; s++ { // 2 minutes: 1 570 applied a second, 275 more left
		applied += 5 * 1570
		left += 5 * 275
		r = c.add(t0.Add(time.Duration(s)*5*time.Second), applied, left)
	}
	if !r.known || r.eta != -1 || int(r.out) != 1570 || int(r.in) != 1845 {
		t.Fatalf("falling behind: %+v", r)
	}
	if r.stalled != 2*time.Minute {
		t.Fatalf("stalled %s, want 2m (the log only grew)", r.stalled)
	}
	at := t0.Add(2 * time.Minute)
	for s := 1; s <= 12; s++ { // 2 000 applied a second, 1 845 coming
		applied += 5 * 2000
		left -= 5 * (2000 - 1845)
		r = c.add(at.Add(time.Duration(s)*5*time.Second), applied, left)
	}
	if r.stalled != 5*time.Second || r.eta <= 0 || int(r.out) != 2000 || int(r.in) != 1845 {
		t.Fatalf("catching up: %+v", r)
	}
	want := time.Duration(float64(left) / float64(2000-1845) * float64(time.Second))
	if d := r.eta - want; d > time.Second || d < -time.Second {
		t.Fatalf("eta %s, want %s", r.eta, want)
	}
}

// applyLog's rounds: up to applyRoundEntries entries, up to applyRoundBytes of images (a round
// stops before the entry past it, and the next goes on from there), the role's right to turn
// the FK checks off asked once, and a round that fails in the organization's database rolled
// back there whole, its connection usable after it.
func TestApplyRounds(t *testing.T) {
	base := review3Base(t)
	cloudDSN := review3DB(t, base, "apply_rounds_c")
	custDSN := review3DB(t, base, "apply_rounds_k")
	review3Exec(t, cloudDSN, "CREATE SCHEMA lokadb", captureDDL(),
		`INSERT INTO lokadb.move_heartbeat VALUES (7, clock_timestamp(), false)`)
	review3Exec(t, custDSN, `CREATE TABLE ar__o7 (id bigint PRIMARY KEY, org_id bigint, v text CHECK (v <> 'bad'))`)
	ctx := context.Background()
	conn := func(dsn string) *pgx.Conn {
		c, err := connect(ctx, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close(context.Background()) })
		return c
	}
	cloud, cust := conn(cloudDSN), conn(custDSN)
	tbl := moveTable{name: "ar", tenant: "org_id", key: "id", org: 7, val: "7",
		cols: [][]string{{"id", "bigint", "true", "", "", ""}, {"org_id", "bigint", "false", "", "", ""}, {"v", "text", "false", "", "", ""}}}
	o := MoveOptions{Org: 7, Log: review3Log(), replica: &replicaRight{}}
	tables := []moveTable{tbl}

	// 12 000 small entries: rounds of 5 000, 5 000, 2 000
	review3Exec(t, cloudDSN, `INSERT INTO lokadb.move_log (org, tbl, op, row)
		SELECT 7, 'ar', 'I', jsonb_build_object('id', g, 'org_id', 7, 'v', 'v' || g) FROM generate_series(1, 12000) g`)
	cur := &logCursor{}
	var ns []int
	for {
		n, err := applyLog(ctx, nil, cloud, cust, o, tables, applyRoundEntries, cur)
		if err != nil {
			t.Fatal(err)
		}
		ns = append(ns, n)
		if !cur.more {
			break
		}
	}
	t.Logf("12 000 entries: rounds %v; the right to turn FK checks off asked: %v, granted: %v", ns, o.replica.asked, o.replica.ok)
	if len(ns) != 3 || ns[0] != 5000 || ns[1] != 5000 || ns[2] != 2000 || !o.replica.asked || !o.replica.ok {
		t.Fatalf("rounds %v, replica %+v", ns, *o.replica)
	}
	if n := review3One(t, custDSN, "SELECT count(*) FROM ar__o7"); n != "12000" {
		t.Fatalf("rows at the organization: %s", n)
	}

	// 24 images of ~1 MB stored (hex of md5, which compresses little): fewer than 24 a round
	review3Exec(t, cloudDSN, `INSERT INTO lokadb.move_log (org, tbl, op, row)
		SELECT 7, 'ar', 'U', jsonb_build_object('id', g, 'org_id', 7, 'v', (SELECT string_agg(md5(g::text || ':' || i), '') FROM generate_series(1, 32768) i))
		FROM generate_series(1, 24) g`)
	stored := review3One(t, cloudDSN, "SELECT sum(pg_column_size(row)) / 24 FROM lokadb.move_log")
	ns = nil
	for {
		n, err := applyLog(ctx, nil, cloud, cust, o, tables, applyRoundEntries, cur)
		if err != nil {
			t.Fatal(err)
		}
		ns = append(ns, n)
		if !cur.more {
			break
		}
	}
	t.Logf("24 images of %s bytes stored each: rounds %v (%d bytes a round at most)", stored, ns, applyRoundBytes)
	if len(ns) < 3 || ns[0] < 1 || ns[0] >= 24 {
		t.Fatalf("rounds %v: not cut by bytes", ns)
	}
	if n := review3One(t, custDSN, "SELECT count(*) FROM ar__o7 WHERE length(v) = 32768 * 32"); n != "24" {
		t.Fatalf("large rows at the organization: %s of 24", n)
	}

	// a round with an entry the organization's database refuses: nothing of it stays there,
	// and the connection is not left in a failed transaction
	review3Exec(t, cloudDSN, `INSERT INTO lokadb.move_log (org, tbl, op, row) VALUES
		(7, 'ar', 'I', '{"id": 20001, "org_id": 7, "v": "ok"}'),
		(7, 'ar', 'I', '{"id": 20002, "org_id": 7, "v": "bad"}'),
		(7, 'ar', 'I', '{"id": 20003, "org_id": 7, "v": "ok"}')`)
	_, err := applyLog(ctx, nil, cloud, cust, o, tables, applyRoundEntries, cur)
	t.Logf("a round with a refused entry: %v", err)
	if err == nil {
		t.Fatal("the refused entry went through")
	}
	var status string
	if err := cust.QueryRow(ctx, "SELECT count(*)::text FROM ar__o7 WHERE id > 20000").Scan(&status); err != nil {
		t.Fatalf("the connection after the failed round: %v", err)
	}
	if status != "0" || cust.PgConn().TxStatus() != 'I' {
		t.Fatalf("after the failed round: %s rows of it, transaction status %c", status, cust.PgConn().TxStatus())
	}
	if n := review3One(t, cloudDSN, "SELECT count(*) FROM lokadb.move_log"); n != "3" {
		t.Fatalf("the failed round's entries left in the log: %s of 3", n)
	}
}
