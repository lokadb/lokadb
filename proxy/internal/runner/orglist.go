package runner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// labelColumns: what a customers table calls the column a person recognizes it by, best first.
var labelColumns = []string{"name", "title", "display_name", "company_name", "company", "organization_name", "slug", "email", "login", "username"}

const orgListPageSize = 50

// identOf quotes a table name, with an optional schema.
func identOf(name string) pgx.Identifier {
	if i := strings.IndexByte(name, '.'); i > 0 {
		return pgx.Identifier{name[:i], name[i+1:]}
	}
	return pgx.Identifier{name}
}

// OrgList reads the customers table live, for the move form and the Organizations page: a page of
// up to 50 (search with Q), each with the label column found by name (or the person's, or none
// with only_keys) and the rows it has in the root tables of the spec. The service passes the page
// on and keeps nothing. Mode "smallest": the customers with the fewest rows, for a first move.
func (o *Ops) OrgList(ctx context.Context, a cloudproto.OrgListArgs) (cloudproto.OrgListResult, error) {
	res := cloudproto.OrgListResult{PageSize: orgListPageSize, Page: max(a.Page, 0)}
	set := o.settings()
	if set.OrgTable == "" || set.OrgKey == "" {
		return res, &cloudproto.JobError{Code: "no_org_table", Message: "the table of customers is not chosen yet: choose it in the setup (step 2)"}
	}
	res.OnlyKeys = set.OnlyKeys
	conn, err := ownsql.Connect(ctx, o.Cfg.DB)
	if err != nil {
		return res, err
	}
	defer conn.Close(context.Background())
	tbl, key := identOf(set.OrgTable).Sanitize(), pgx.Identifier{set.OrgKey}.Sanitize()

	label := ""
	if !set.OnlyKeys {
		label = set.OrgLabel
		if label == "" {
			label = o.labelColumn(ctx, conn, set.OrgTable)
		}
	}
	res.LabelColumn = label
	where, args := "", []any{}
	if q := strings.TrimSpace(a.Q); q != "" {
		where = fmt.Sprintf("WHERE %s::text ILIKE $1", key)
		if label != "" {
			where += fmt.Sprintf(" OR %s::text ILIKE $1", pgx.Identifier{label}.Sanitize())
		}
		args = append(args, "%"+strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(q)+"%")
	}
	sel := key + "::text"
	if label != "" {
		sel += ", " + pgx.Identifier{label}.Sanitize() + "::text"
	} else {
		sel += ", ''"
	}
	roots := o.rootTables()
	var keys []string
	if a.Mode == "smallest" && len(roots) > 0 {
		if keys, err = o.smallest(ctx, conn, roots[0], 5); err != nil || len(keys) == 0 {
			keys = nil
		}
	}
	var rows pgx.Rows
	if keys != nil {
		rows, err = conn.Query(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE %s::text = ANY($1) ORDER BY %s", sel, tbl, key, key), keys)
		res.Total = int64(len(keys))
	} else {
		if err = conn.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s %s", tbl, where), args...).Scan(&res.Total); err != nil {
			return res, err
		}
		rows, err = conn.Query(ctx, fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT %d OFFSET %d", sel, tbl, where, key, orgListPageSize, res.Page*orgListPageSize), args...)
	}
	if err != nil {
		return res, err
	}
	for rows.Next() {
		var it cloudproto.OrgListItem
		if err := rows.Scan(&it.Key, &it.Label); err != nil {
			rows.Close()
			return res, err
		}
		it.Rows = -1
		res.Orgs = append(res.Orgs, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, err
	}
	o.countRows(ctx, conn, roots, res.Orgs)
	return res, nil
}

func (o *Ops) settings() cloudproto.Settings {
	if o.Cfg.settings == nil {
		return cloudproto.Settings{}
	}
	return *o.Cfg.settings
}

// rootTables: the spec's tables that have a customer column of their own, with that column.
func (o *Ops) rootTables() [][2]string {
	var out [][2]string
	for _, t := range o.Cfg.spec.Tables {
		if t.TenantColumn != "" {
			out = append(out, [2]string{t.Name, t.TenantColumn})
		}
	}
	return out
}

func (o *Ops) labelColumn(ctx context.Context, conn *pgx.Conn, table string) string {
	schema, name := "public", table
	if i := strings.IndexByte(table, '.'); i > 0 {
		schema, name = table[:i], table[i+1:]
	}
	rows, err := conn.Query(ctx, `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2
		AND data_type IN ('text', 'character varying', 'citext', 'character', 'USER-DEFINED')`, schema, name)
	if err != nil {
		return ""
	}
	defer rows.Close()
	have := map[string]bool{}
	for rows.Next() {
		var c string
		if rows.Scan(&c) == nil {
			have[c] = true
		}
	}
	for _, c := range labelColumns {
		if have[c] {
			return c
		}
	}
	return ""
}

// smallest: the keys with the fewest rows in the root table (at least one), within 3 seconds.
func (o *Ops) smallest(ctx context.Context, conn *pgx.Conn, root [2]string, n int) ([]string, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '3s'"); err != nil {
		return nil, err
	}
	tc := pgx.Identifier{root[1]}.Sanitize()
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT %s::text FROM %s WHERE %s IS NOT NULL GROUP BY %s ORDER BY count(*), %s LIMIT %d", tc, identOf(root[0]).Sanitize(), tc, tc, tc, n))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

// countRows adds up the rows of each listed customer in the spec's root tables (3 seconds a
// table; a table that is slower leaves its rows out of the count).
func (o *Ops) countRows(ctx context.Context, conn *pgx.Conn, roots [][2]string, items []cloudproto.OrgListItem) {
	if len(items) == 0 || len(roots) == 0 {
		return
	}
	keys := make([]string, len(items))
	pos := map[string]int{}
	for i, it := range items {
		keys[i], pos[it.Key] = it.Key, i
	}
	for i := range items {
		items[i].Rows = 0
	}
	for _, r := range roots {
		cctx, cancel := context.WithTimeout(ctx, 4*time.Second)
		tx, err := conn.Begin(cctx)
		if err != nil {
			cancel()
			return
		}
		_, _ = tx.Exec(cctx, "SET LOCAL statement_timeout = '3s'")
		tc := pgx.Identifier{r[1]}.Sanitize()
		rows, err := tx.Query(cctx, fmt.Sprintf("SELECT %s::text, count(*) FROM %s WHERE %s::text = ANY($1) GROUP BY 1", tc, identOf(r[0]).Sanitize(), tc), keys)
		if err == nil {
			for rows.Next() {
				var k string
				var n int64
				if rows.Scan(&k, &n) == nil {
					if i, ok := pos[k]; ok {
						items[i].Rows += n
					}
				}
			}
			rows.Close()
		}
		_ = tx.Rollback(context.Background())
		cancel()
	}
}
