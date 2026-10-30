package admin

// An autovacuum worker on a table the move needs to lock. Postgres cancels such a worker by
// itself when a lock request has waited deadlock_timeout for it, but every lock the move takes
// waits less than that (a quarter of it, see switchLockTry: a writer that holds the other half
// of a lock cycle must not be the one cancelled), so a worker that holds the table for minutes
// (the DEFAULT partition the split just deleted 480 000 rows from is vacuumed at once) is never
// asked to stop, every try times out, and the move gives up: "switch not possible right now"
// (55P03), or the split's last transaction is rolled back and run again for half a minute.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
)

// orgRels: the tables of the spec the move locks, as the cloud names them: the table, its
// DEFAULT partition and the organization's partition.
func orgRels(o MoveOptions) []string {
	var rels []string
	for _, st := range o.Spec.Tables {
		rels = append(rels, qr(st.Name), qr(hn(st.Name, "__cloud")), qr(hn(st.Name, fmt.Sprintf("__o%d", o.Org))))
	}
	return rels
}

// yieldAutovacuum cancels the autovacuum workers that hold a lock on one of rels, called after a
// try for a lock timed out; the worker comes back to its table at its next naptime, after the
// move has its lock. stuck: a worker holds a table and this role may not cancel it
// (pg_cancel_backend on an autovacuum worker wants a superuser or pg_signal_backend... on a
// managed service often neither): the caller then waits for its locks longer than
// deadlock_timeout, and Postgres cancels the worker itself. conn is not the connection that
// waits for the lock (it is blocked, or in the transaction that holds the pause).
func yieldAutovacuum(ctx context.Context, conn *pgx.Conn, rels []string, log *slog.Logger) (cancelled int, stuck bool) {
	if len(rels) == 0 {
		return 0, false
	}
	// to_regclass: a table that is not there (yet) is no one's lock. Autovacuum holds SHARE UPDATE
	// EXCLUSIVE. A role that is no superuser (and no pg_read_all_stats) sees nothing of another
	// role's session in pg_stat_activity, an autovacuum worker's included: its row is there with
	// an empty backend_type, and only pg_locks tells that somebody holds the table (R36).
	rows, err := rowsOf(ctx, conn, `SELECT DISTINCT l.pid::text, coalesce(a.backend_type, ''), coalesce(left(a.query, 60), ''), l.relation::regclass::text
		FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid
		WHERE l.locktype = 'relation' AND l.granted AND l.mode = 'ShareUpdateExclusiveLock' AND l.pid <> pg_backend_pid()
		  AND l.relation = ANY (SELECT to_regclass(r) FROM unnest($1::text[]) r WHERE to_regclass(r) IS NOT NULL)`, rels)
	if err != nil {
		return 0, false
	}
	for _, r := range rows {
		switch r[1] {
		case "autovacuum worker":
		case "":
			// Not shown to this role, so not known to be a worker, and not cancelled (it may be
			// somebody's CREATE INDEX CONCURRENTLY): waited for past deadlock_timeout, where
			// Postgres cancels it if it is autovacuum.
			log.Warn("a session of another role holds a table the move needs and this role cannot see it: the move waits for it longer than deadlock_timeout, which makes Postgres cancel it if it is autovacuum",
				"pid", r[0], "table", r[3])
			stuck = true
			continue
		default:
			continue
		}
		if _, err := rowsOf(ctx, conn, `SELECT pg_cancel_backend($1::text::int)::text`, r[0]); err != nil {
			log.Warn("an autovacuum worker holds a table the move needs and this role may not cancel it: the move waits for it longer than deadlock_timeout, which makes Postgres cancel it",
				"pid", r[0], "table", r[3], "err", strings.TrimSpace(firstLine(err.Error())))
			stuck = true
			continue
		}
		log.Info("autovacuum cancelled: it held a table the move needs (it runs again at its next naptime)", "pid", r[0], "table", r[3], "query", r[2])
		cancelled++
	}
	return cancelled, stuck
}

// patientTry: how long a try waits for its locks when autovacuum holds them and cannot be
// cancelled: past deadlock_timeout, where Postgres cancels the worker.
func patientTry(deadlockMs int64) int64 { return deadlockMs*3/2 + 200 }
