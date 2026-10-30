package admin

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Defuse takes the moved organizations out of a COPY of the SaaS database (a dump restored for
// staging, a laptop, analytics: R01): its user mappings go, the customers' passwords with them,
// and each organization's server leads nowhere. The organizations' partitions stay (their rows
// are at the customers) and give an error when read. confirm must be the copy's database name, or
// nothing changes and the servers are only listed: on the SaaS database itself it cuts every
// moved customer off.
type DefuseResult struct {
	Database string
	Servers  []DefusedServer
	Done     bool
}

type DefusedServer struct {
	Name  string
	Roles []string // the roles with a user mapping on it ("PUBLIC" for one of PUBLIC)
}

func Defuse(ctx context.Context, conn *pgx.Conn, confirm string) (DefuseResult, error) {
	var res DefuseResult
	if err := conn.QueryRow(ctx, "SELECT current_database()").Scan(&res.Database); err != nil {
		return res, err
	}
	rows, err := conn.Query(ctx, `SELECT s.srvname, coalesce(array_agg(coalesce(r.rolname, 'PUBLIC')) FILTER (WHERE m.umid IS NOT NULL), '{}')
		FROM pg_foreign_server s LEFT JOIN pg_user_mappings m ON m.srvid = s.oid LEFT JOIN pg_roles r ON r.oid = m.umuser
		WHERE s.srvname ~ '^lokadb_org[0-9]+$' GROUP BY s.srvname ORDER BY s.srvname`)
	if err != nil {
		return res, err
	}
	var x DefusedServer
	if _, err := pgx.ForEachRow(rows, []any{&x.Name, &x.Roles}, func() error { res.Servers = append(res.Servers, x); return nil }); err != nil {
		return res, err
	}
	if len(res.Servers) == 0 || confirm != res.Database {
		return res, nil
	}
	var stmts []string
	for _, s := range res.Servers {
		for _, r := range s.Roles {
			who := r
			if r != "PUBLIC" {
				who = q(r)
			}
			stmts = append(stmts, fmt.Sprintf("DROP USER MAPPING IF EXISTS FOR %s SERVER %s", who, q(s.Name)))
		}
		stmts = append(stmts, fmt.Sprintf("ALTER SERVER %s OPTIONS (SET host 'defused.lokadb.invalid')", q(s.Name)))
	}
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		for _, st := range stmts {
			if _, err := tx.Exec(ctx, st); err != nil {
				return fmt.Errorf("%s: %w", st, err)
			}
		}
		return nil
	})
	res.Done = err == nil
	return res, err
}
