package admin

// move-org -dry-run: whether the trigger mode's catch-up can keep up with the organization's
// writes. A round of the catch-up costs a round trip to the organization's database and five to
// the cloud whatever its size, and some work per entry there; the organization's writes are one
// log entry per row changed. Block 14 on 27.10: 43 ms away, 8 writers made 1 845 changes/s and
// the catch-up (then 6 round trips a round of 500) applied 1 570/s: it never caught up.

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// estPerEntry: the work of one log entry in the organization's database (an upsert from its
// jsonb image), as measured: block 14, 27.10, 200-byte rows (60 ms a round of 500, its 6 round
// trips of 43 ms aside).
const estPerEntry = 120 * time.Microsecond

// MoveEstimate is what -dry-run reports.
type MoveEstimate struct {
	CustomerRTT, CloudRTT time.Duration
	Round                 time.Duration // a full round of applyRoundEntries
	Ceiling               float64       // log entries/s the catch-up applies at most
	Writes                float64       // rows changed/s over the sample (-1: not measured)
	WritesOf              string        // what Writes counts: the organization's partitions, or all of DEFAULT
	Sample                time.Duration
}

// EstimateMove measures the round trips from here to both databases and the organization's
// writes over sample, and estimates the catch-up's ceiling. It changes nothing.
func EstimateMove(ctx context.Context, o MoveOptions, sample time.Duration) (MoveEstimate, error) {
	e := MoveEstimate{Writes: -1, Sample: sample}
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return e, err
	}
	defer cloud.Close(ctx)
	cust, err := connect(ctx, o.CustomerDSN)
	if err != nil {
		return e, fmt.Errorf("customer database: %w", err)
	}
	defer cust.Close(ctx)
	if e.CloudRTT, err = rtt(ctx, cloud); err != nil {
		return e, err
	}
	if e.CustomerRTT, err = rtt(ctx, cust); err != nil {
		return e, fmt.Errorf("customer database: %w", err)
	}
	e.Round = e.CustomerRTT + 5*e.CloudRTT + applyRoundEntries*estPerEntry
	e.Ceiling = applyRoundEntries / e.Round.Seconds()

	// the organization's partitions if it has them, else DEFAULT (every organization still there)
	var own, def []string
	for _, t := range o.Spec.Tables {
		own = append(own, qr(hn(t.Name, fmt.Sprintf("__o%d", o.Org))))
		def = append(def, qr(hn(t.Name, "__cloud")))
	}
	tables, of := own, "the organization's partitions"
	var n int
	if err := cloud.QueryRow(ctx, "SELECT count(*) FROM unnest($1::text[]) t WHERE to_regclass(t) IS NOT NULL", own).Scan(&n); err != nil {
		return e, err
	}
	if n == 0 {
		tables, of = def, "every organization still in DEFAULT (an upper bound of this one's)"
	}
	changes := func() (int64, error) {
		var c int64
		err := cloud.QueryRow(ctx, `SELECT coalesce(sum(n_tup_ins + n_tup_upd + n_tup_del), 0)::bigint FROM pg_stat_all_tables
			WHERE relid IN (SELECT to_regclass(t) FROM unnest($1::text[]) t)`, tables).Scan(&c)
		return c, err
	}
	if sample > 0 {
		a, err := changes()
		if err != nil {
			return e, err
		}
		select {
		case <-ctx.Done():
			return e, ctx.Err()
		case <-time.After(sample):
		}
		if _, err := cloud.Exec(ctx, "SELECT pg_stat_clear_snapshot()"); err != nil {
			return e, err
		}
		b, err := changes()
		if err != nil {
			return e, err
		}
		e.Writes, e.WritesOf = float64(b-a)/sample.Seconds(), of
	}
	return e, nil
}

// rtt: the shortest of five round trips on c.
func rtt(ctx context.Context, c *pgx.Conn) (time.Duration, error) {
	best := time.Duration(-1)
	for i := 0; i < 5; i++ {
		t := time.Now()
		if _, err := c.Exec(ctx, "SELECT 1"); err != nil {
			return 0, err
		}
		if d := time.Since(t); best < 0 || d < best {
			best = d
		}
	}
	return best, nil
}

// String: the report of -dry-run.
func (e MoveEstimate) String() string {
	s := fmt.Sprintf("catch-up of the trigger mode, estimated: round trip to the organization's database %s, to the cloud %s; "+
		"a round of %d log entries about %s: up to %.0f changes/s",
		e.CustomerRTT.Round(100*time.Microsecond), e.CloudRTT.Round(100*time.Microsecond), applyRoundEntries, e.Round.Round(time.Millisecond), e.Ceiling)
	if e.Writes < 0 {
		return s
	}
	s += fmt.Sprintf("\nwrites now: %.0f rows changed/s over %s (%s)", e.Writes, e.Sample, e.WritesOf)
	switch {
	case e.Writes >= e.Ceiling:
		s += "\nverdict: the catch-up would not keep up at this rate: move when the organization writes less, or with -mode logical -allow-customer-replication"
	case e.Writes >= 0.7*e.Ceiling:
		s += "\nverdict: close to the ceiling: the catch-up may take long; -catchup-stall stops it if the log stops getting shorter"
	default:
		s += "\nverdict: the catch-up keeps up"
	}
	return s
}
