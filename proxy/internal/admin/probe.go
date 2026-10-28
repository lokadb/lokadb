package admin

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Probe runs steps in a transaction that is always rolled back: whether this role, on this
// server, can do what move-org will do, whatever grants, managed-service roles
// (rds_superuser, cloudsqlsuperuser, azure_pg_admin) or settings decide it. nil: every
// step ran. The error names the step that failed.
func Probe(ctx context.Context, conn *pgx.Conn, steps ...string) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	// a probe must not queue behind anything, nor hold anything for long
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'; SET LOCAL statement_timeout = '15s'"); err != nil {
		return err
	}
	for _, s := range steps {
		if _, err := tx.Exec(ctx, s); err != nil {
			return fmt.Errorf("%s: %w", firstWords(s), err)
		}
	}
	return nil
}

// ProbeName is a name for an object a probe creates (and rolls back).
func ProbeName(what string) string {
	return fmt.Sprintf("lokadb_probe_%s_%d", what, os.Getpid())
}

// firstWords: the statement as far as it says what it does ("CREATE TABLE x").
func firstWords(s string) string {
	if strings.HasPrefix(s, "/*") {
		if _, rest, ok := strings.Cut(s, "*/"); ok {
			s = rest
		}
	}
	f := strings.Fields(s)
	if len(f) > 3 {
		f = f[:3]
	}
	return strings.Join(f, " ")
}

// SubscribeProbe: the subscription a logical move creates, without connecting anywhere.
func SubscribeProbe() string {
	// PostgreSQL 16+ refuses a conninfo without a password from a non-superuser at CREATE
	return fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION 'dbname=lokadb_probe password=lokadb_probe' PUBLICATION lokadb_probe WITH (connect = false)",
		ProbeName("sub"))
}
