// Package pgext: the extensions LokaDB tables need in an organization's database, created
// where the role may (DESIGN §3.3 of private/cloud). Light on purpose: the agent installer,
// built without cgo, uses it too.
package pgext

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
)

// Sync creates what need lists and conn's role may create (admin: any available
// one; else only trusted ones) and says where each stands. The installer runs it as the
// customer's administrator, the runner as the cloud's role.
func Sync(ctx context.Context, conn *pgx.Conn, need []cloudproto.Extension, admin bool) []cloudproto.ExtensionStatus {
	out := []cloudproto.ExtensionStatus{}
	for _, e := range need {
		st := cloudproto.ExtensionStatus{Name: e.Name, CloudVersion: e.Version}
		var installed, available *string
		var trusted bool
		err := conn.QueryRow(ctx, `SELECT (SELECT extversion FROM pg_extension WHERE extname = $1),
			(SELECT default_version FROM pg_available_extensions WHERE name = $1),
			coalesce((SELECT bool_or(trusted) FROM pg_available_extension_versions WHERE name = $1), false)`, e.Name).Scan(&installed, &available, &trusted)
		switch {
		case err != nil:
			st.Status, st.Error = cloudproto.ExtFailed, err.Error()
		case installed != nil:
			st.Status, st.Version = cloudproto.ExtPresent, *installed
		case available == nil:
			st.Status = cloudproto.ExtUnavailable
			st.Error = "the extension's package is not installed on this Postgres server"
		case !admin && !trusted:
			st.Status = cloudproto.ExtNeedsAdmin
			st.Error = "only a superuser (the database's administrator) may create it"
		default:
			schema := e.Schema
			if schema == "" {
				schema = "public"
			}
			q := pgx.Identifier{schema}.Sanitize()
			_, err = conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+q)
			if err == nil {
				_, err = conn.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS `+pgx.Identifier{e.Name}.Sanitize()+` SCHEMA `+q)
			}
			if err != nil {
				st.Status, st.Error = cloudproto.ExtFailed, err.Error()
				break
			}
			st.Status = cloudproto.ExtCreated
			_ = conn.QueryRow(ctx, `SELECT extversion FROM pg_extension WHERE extname = $1`, e.Name).Scan(&st.Version)
		}
		out = append(out, st)
	}
	return out
}
