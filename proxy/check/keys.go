package check

import (
	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// relKeySQL: the key (package rel) of relation alias of pg_class, in SQL.
func relKeySQL(alias string) string {
	return rel.KeyOfSQL("(SELECT nspname::text FROM pg_catalog.pg_namespace WHERE oid = "+alias+".relnamespace)", alias+".relname::text")
}

// sysSchemas: the namespaces that are not the application's (SQL, for NOT IN).
const sysSchemas = `SELECT oid FROM pg_catalog.pg_namespace WHERE nspname IN ('information_schema', 'lokadb') OR nspname ~ '^pg_'`

// qk: how SQL names the table of key k.
func qk(k string) string { return admin.QR(k) }

// analyze: the proxy's analysis of sql against m, with the search path of check's connection
// (c.path: a bare name of a table outside public resolves by it; nil: public).
func (c *checker) analyze(sql string, m *proxy.Meta) *proxy.Analysis {
	if c.path == nil {
		return proxy.Analyze(sql, m)
	}
	return proxy.AnalyzeIn(sql, m, c.path)
}

// directPath: the search path the direct path's decision resolves bare names by (c.path; nil: public).
func (c *checker) directPath() []string {
	if c.path == nil {
		return []string{"public"}
	}
	return c.path
}

// readPath sets c.path, when a bare name depends on it: the search path of the application's
// role (-app-role, the first one), as its sessions start with it (its setting in this
// database, its own, the database's, the server's), else that of check's connection. A
// session that changes it later is the proxy's to follow, not check's.
func (c *checker) readPath() {
	if c.meta == nil || !c.meta.SchemasUsed || c.tx == nil {
		return
	}
	sql := `SELECT current_setting('search_path'), current_user::text`
	var args []any
	if len(c.o.AppRoles) > 0 {
		sql = `WITH r AS (SELECT oid FROM pg_roles WHERE rolname = $1), d AS (SELECT oid FROM pg_database WHERE datname = current_database()),
			v AS (SELECT substr(x, 13) AS path, CASE WHEN s.setrole <> 0 AND s.setdatabase <> 0 THEN 1 WHEN s.setrole <> 0 THEN 2 ELSE 3 END AS rank
				FROM pg_db_role_setting s, unnest(s.setconfig) x
				WHERE x LIKE 'search_path=%' AND (s.setrole = (SELECT oid FROM r) AND s.setdatabase IN (0, (SELECT oid FROM d))
					OR s.setrole = 0 AND s.setdatabase = (SELECT oid FROM d)))
			SELECT coalesce((SELECT path FROM v ORDER BY rank LIMIT 1), (SELECT boot_val FROM pg_settings WHERE name = 'search_path')), $1::text`
		args = []any{c.o.AppRoles[0]}
	}
	if r, err := rowsOf(c.ctx, c.tx, sql, args...); err == nil && len(r) == 1 {
		c.path = proxy.ParsePath(r[0][0], r[0][1])
	}
}
