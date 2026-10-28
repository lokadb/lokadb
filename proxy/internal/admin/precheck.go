package admin

// The check before the point of no return (the admin review of 25.10, A7). compare, after the
// switch, found a copy that differs only when the organization already worked on it, and the
// way out was a move-back from that copy. The row counts of the two sides are equal here
// whatever the log applied, and cost little under the pause: counted with a small budget, a
// table too large to count in it is left to compare.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// countBudget: how long reading one table on one side may take under the pause.
const countBudget = 300 * time.Millisecond

// countWithin counts the rows of table in tx and sums their digests (the comparison after the
// switch's rowDigest: every column), within budget: ok false when it took longer (the statement
// is cancelled; the transaction goes on).
func countWithin(ctx context.Context, tx pgx.Tx, t moveTable, table string, budget time.Duration) (n int64, sum string, ok bool, err error) {
	if _, err := tx.Exec(ctx, "SAVEPOINT lokadb_count"); err != nil {
		return 0, "", false, err
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%dms'", budget.Milliseconds())); err != nil {
		return 0, "", false, err
	}
	err = tx.QueryRow(ctx, fmt.Sprintf("SELECT count(*), coalesce(sum(hashtextextended(%s, 0)::numeric), 0)::text FROM %s t", rowDigest(t), table)).Scan(&n, &sum)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "57014" {
			if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_count"); err != nil {
				return 0, "", false, err
			}
			return 0, "", false, nil
		}
		return 0, "", false, err
	}
	if _, err = tx.Exec(ctx, "RELEASE SAVEPOINT lokadb_count"); err != nil {
		return 0, "", false, err
	}
	// the budget was for the count only: snap goes on to the comparison after the switch
	_, err = tx.Exec(ctx, "SET LOCAL statement_timeout = DEFAULT")
	return n, sum, true, err
}

// verifyCounts compares, for each table, the rows of the organization's partition in the
// cloud (tx: the switch's locked transaction, nothing writes) with the customer's copy (snap:
// its snapshot taken under the same pause): their number and the sum of their digests, which
// catches a row with an old version too (review 3, R3-6: the count alone let one through). A
// difference stops the switch before anything commits in the cloud: the organization stays
// where it is, as outcome says (move-org, or move-back: block 6).
func verifyCounts(ctx context.Context, tx, snap pgx.Tx, tables []moveTable, log *slog.Logger, outcome string) error {
	for _, t := range tables {
		a, sa, okA, err := countWithin(ctx, tx, t, qr(t.local()), countBudget)
		if err != nil {
			return err
		}
		b, sb, okB, err := countWithin(ctx, snap, t, qr(t.name), countBudget)
		if err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		if !okA || !okB {
			log.Info("rows before the switch: not compared, too many to read under the pause (the comparison after the switch covers them)", "table", t.name)
			continue
		}
		if a != b {
			return fmt.Errorf("%s: the cloud's partition holds %d rows of the organization, the customer database's copy %d: %s", t.name, a, b, outcome)
		}
		if sa != sb {
			return fmt.Errorf("%s: the cloud's partition and the customer database's copy hold %d rows each, but not the same ones (their digests differ): %s", t.name, a, outcome)
		}
	}
	return nil
}
