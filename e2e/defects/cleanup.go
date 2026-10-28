package main

// What a check that did not pass leaves behind must not fail the checks after it: the
// subscriptions (a shared catalog), replication slots and roles a killed move-org leaves are
// seen by every later check of the stand (leftovers counts them cluster-wide). A move-org
// that runs out of the check's time gets Ctrl-C (it cleans up itself, R4) and a while to
// do so; then cleanupAfter removes what is still there.

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// command runs lokadb under ctx; when ctx ends, lokadb gets Ctrl-C rather than a kill, and a
// minute to finish its cleanup.
func command(ctx context.Context, bin string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
	cmd.WaitDelay = time.Minute
	return cmd
}

// cleanupAfter removes the moves' leftovers of check database db, and no other check's, on a context of its own
// (the check's has ended). It returns what it removed, for the report.
func cleanupAfter(bin, cloudHost, db string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	var done []string
	// Only this check's: its organization's database has its name (fresh), and move-org names
	// slots and roles after the cloud database's OID (lokadb_<oid>_org<N>, lokadb_repl_<oid>_org<N>).
	oid, err := one(ctx, dsn(cloudHost, "postgres", "postgres"), "SELECT oid::text FROM pg_database WHERE datname = '"+db+"'")
	if err != nil || oid == "" {
		oid = "-" // the database is gone: no slot or role of it can be told apart
	}
	// subscriptions in the organization's database: without their slot first, the slot may
	// be gone or busy
	if subs, err := rowsAt(ctx, dsn("customer", "postgres", "postgres"),
		`SELECT s.subname::text, d.datname::text FROM pg_subscription s JOIN pg_database d ON d.oid = s.subdbid
		WHERE s.subname LIKE 'lokadb\_%' AND d.datname = '`+db+`'`); err == nil {
		for _, s := range subs {
			name := pgx.Identifier{s[0]}.Sanitize()
			if err := execSQL(ctx, dsn("customer", s[1], "postgres"), "ALTER SUBSCRIPTION "+name+" DISABLE",
				"ALTER SUBSCRIPTION "+name+" SET (slot_name = NONE)", "DROP SUBSCRIPTION "+name); err == nil {
				done = append(done, "subscription "+s[0])
			}
		}
	}
	// capture triggers, the move log, slots and roles: what move-cleanup does
	cloud := dsn(cloudHost, db, "postgres")
	if out, err := command(ctx, bin, "move-cleanup", "-db", cloud).CombinedOutput(); err == nil {
		if n := strings.Count(string(out), "msg=removed"); n > 0 {
			done = append(done, fmt.Sprintf("move-cleanup removed %d", n))
		}
	} else {
		done = append(done, "move-cleanup: "+oneLine(tail(string(out))))
	}
	// what move-cleanup could not (a role still in use when it ran, a slot of a killed walsender)
	_ = execSQL(ctx, dsn(cloudHost, "postgres", "postgres"),
		`SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name LIKE 'lokadb\_`+oid+`\_org%' AND active_pid IS NOT NULL`)
	if slots, err := rowsAt(ctx, dsn(cloudHost, "postgres", "postgres"), `SELECT slot_name::text FROM pg_replication_slots WHERE slot_name LIKE 'lokadb\_`+oid+`\_org%'`); err == nil {
		for _, s := range slots {
			if execSQL(ctx, dsn(cloudHost, "postgres", "postgres"), "SELECT pg_drop_replication_slot('"+s[0]+"')") == nil {
				done = append(done, "slot "+s[0])
			}
		}
	}
	if roles, err := rowsAt(ctx, dsn(cloudHost, "postgres", "postgres"), `SELECT rolname::text FROM pg_roles WHERE rolname LIKE 'lokadb\_repl\_`+oid+`\_org%'`); err == nil {
		for _, r := range roles {
			name := pgx.Identifier{r[0]}.Sanitize()
			_ = execSQL(ctx, cloud, "DROP OWNED BY "+name)
			if execSQL(ctx, dsn(cloudHost, "postgres", "postgres"), "DROP ROLE "+name) == nil {
				done = append(done, "role "+r[0])
			}
		}
	}
	return strings.Join(done, ", ")
}

func rowsAt(ctx context.Context, d, sql string) ([][]string, error) {
	c, err := pgx.Connect(ctx, d)
	if err != nil {
		return nil, err
	}
	defer c.Close(ctx)
	rows, err := c.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			row[i] = fmt.Sprint(v)
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
