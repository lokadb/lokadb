package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "F13", name: "a SELECT of a moved organization's table joined with cloud tables (Django's select_related) runs on its foreign partition, and no form of join returns another organization's rows (Z4 of block 14)", run: checkJoinTarget},
	)
}

// F13 (Z4 of block 14's second measurement pass). Django's feed is
// Document.objects.select_related("project")…order_by("-id")[:20], a JOIN with a cloud table:
// the proxy addressed only a SELECT of one table to the organization's partition, so this went
// through the parent, and with the data of 20 organizations the planner took all 20 000 rows of
// the organization from its database and sorted them in the cloud (5 requests a second at 16
// clients, p50 3.3 s). The proxy now puts the partition in the table's place in such a join and
// keeps the statement to the organization by a condition in WHERE; on the nullable side of an
// outer join, inside a join with an alias and in a subquery the statement stays on the parent.
// The organization's database (it controls it) holds a row of organization 2: no form may
// return it, and each returns what the parent returns under lokadb.org = 1 (the policy's rows).
// Then the same on fdw+direct: a join with a cloud table goes the cloud's way, aimed at the
// partition there too.
func checkJoinTarget(e *env) (bool, string, error) {
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	// the organization's database is its own: a row of another organization in it
	cons, err := e.customerRows("SELECT conname FROM pg_constraint WHERE conrelid = 'documents'::regclass AND contype = 'c'")
	if err != nil {
		stop()
		return false, "", err
	}
	planted := []string{}
	for _, c := range cons {
		planted = append(planted, fmt.Sprintf("ALTER TABLE documents DROP CONSTRAINT %q", c))
	}
	planted = append(planted, "INSERT INTO documents (id, organization_id, user_id, title) VALUES (900001, 2, 1, 'planted')")
	if err := execSQL(e.ctx, e.customer(), planted...); err != nil {
		stop()
		return false, "", err
	}
	type form struct {
		name, sql string
		params    []string
		target    bool
		named     bool // the application names the organization (SET LOCAL lokadb.tenant): the proxy knows it whatever the form
	}
	forms := []form{
		{"INNER JOIN, literal", "SELECT d.id, u.name FROM documents d JOIN users u ON u.id = d.user_id WHERE d.organization_id = 1 ORDER BY d.id DESC LIMIT 20", nil, true, false},
		{"INNER JOIN, $1", "SELECT d.id, u.name FROM documents d JOIN users u ON u.id = d.user_id WHERE d.organization_id = $1 ORDER BY d.id DESC LIMIT 20", []string{"1"}, true, false},
		{"Django's form (quoted, no alias)", `SELECT "documents"."id", "documents"."title", "users"."name" FROM "documents" INNER JOIN "users" ON ("documents"."user_id" = "users"."id") WHERE "documents"."organization_id" = $1 ORDER BY "documents"."id" DESC LIMIT 20`, []string{"1"}, true, false},
		{"comma join", "SELECT d.id, u.name FROM users u, documents d WHERE u.id = d.user_id AND d.organization_id = 1 ORDER BY d.id DESC LIMIT 20", nil, true, false},
		{"LEFT JOIN, the table on the kept side", "SELECT d.id, u.name FROM documents d LEFT JOIN users u ON u.id = d.user_id AND u.id < 5 WHERE d.organization_id = 1 ORDER BY d.id DESC LIMIT 20", nil, true, false},
		{"a subquery of cloud rows in the join", "SELECT d.id, x.n FROM documents d JOIN (SELECT id, name AS n FROM users WHERE id < 5) x ON x.id = d.user_id WHERE d.organization_id = 1 ORDER BY d.id DESC LIMIT 20", nil, true, false},
		{"the organization in ON, not in WHERE", "SELECT d.id, u.name FROM documents d JOIN users u ON u.id = d.user_id AND d.organization_id = 1 ORDER BY d.id DESC LIMIT 20", nil, true, false},
		{"LEFT JOIN, the table on the nullable side", "SELECT u.id, d.id FROM users u LEFT JOIN documents d ON d.user_id = u.id AND d.organization_id = 1 WHERE u.id <= 3 ORDER BY u.id, d.id DESC", nil, false, false},
		{"RIGHT JOIN, the table on the nullable side", "SELECT u.id, d.id FROM documents d RIGHT JOIN users u ON d.user_id = u.id AND d.organization_id = 1 WHERE u.id <= 3 ORDER BY u.id, d.id DESC", nil, false, false},
		{"the table in a subquery of the join", "SELECT u.name, x.id FROM users u JOIN (SELECT id, user_id FROM documents WHERE organization_id = 1) x ON x.user_id = u.id ORDER BY x.id DESC LIMIT 20", nil, false, false},
		// the organization named by the application: the proxy knows it for every form, and only the
		// form decides (a condition in WHERE on the nullable side would drop the users without a match)
		{"named, INNER JOIN", "SELECT d.id, u.name FROM documents d JOIN users u ON u.id = d.user_id ORDER BY d.id DESC LIMIT 20", nil, true, true},
		{"named, LEFT JOIN, the table on the nullable side", "SELECT u.id, d.id FROM users u LEFT JOIN documents d ON d.user_id = u.id AND d.title LIKE 'd1%' ORDER BY u.id, d.id DESC", nil, false, true},
		{"named, RIGHT JOIN, the table on the nullable side", "SELECT u.id, d.id FROM documents d RIGHT JOIN users u ON d.user_id = u.id AND d.title LIKE 'd1%' ORDER BY u.id, d.id DESC", nil, false, true},
		{"named, FULL JOIN", "SELECT u.id, d.id FROM documents d FULL JOIN users u ON d.user_id = u.id AND d.title LIKE 'd1%' ORDER BY u.id, d.id DESC", nil, false, true},
	}
	ok := true
	var notes []string
	run := func(profile string, forms []form) error {
		c, err := e.viaProxy()
		if err != nil {
			return err
		}
		defer c.Close(e.ctx)
		for _, f := range forms {
			want, err := policyRows(e, f.sql, f.params)
			if err != nil {
				return fmt.Errorf("%s: the parent under lokadb.org = 1: %w", f.name, err)
			}
			before := rewrittenRoutes(e)
			var got []string
			var qErr error
			if f.named { // each statement a query string of its own: the proxy aims one statement
				for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = '1'"} {
					if _, err := c.Exec(e.ctx, q).ReadAll(); err != nil {
						return fmt.Errorf("%s: %s: %w", f.name, q, err)
					}
				}
			}
			got, qErr = proxyRows(e, c, f.sql, f.params)
			if f.named {
				if _, err := c.Exec(e.ctx, "ROLLBACK").ReadAll(); err != nil {
					return fmt.Errorf("%s: ROLLBACK: %w", f.name, err)
				}
			}
			time.Sleep(200 * time.Millisecond) // the proxy's route line
			aimed := rewrittenRoutes(e) > before
			s := fmt.Sprintf("%s, %s: %d rows, on the partition %v", profile, f.name, len(got), aimed)
			bad := ""
			switch {
			case qErr != nil:
				bad = "error " + firstErrLine(qErr)
			case strings.Contains(strings.Join(got, ";"), "900001"):
				bad = "WRONG it returned organization 2's row of the organization's database"
			case strings.Join(got, ";") != strings.Join(want, ";"):
				bad = fmt.Sprintf("WRONG rows %v, the parent under the policy %v", got, want)
			case aimed != f.target:
				bad = fmt.Sprintf("WRONG on the partition %v, want %v", aimed, f.target)
			}
			if bad != "" {
				ok = false
				s += ": " + bad
			}
			notes = append(notes, s)
		}
		return nil
	}
	err = run("fdw", forms)
	stop()
	if err != nil {
		return false, "", err
	}
	// fdw+direct: the join with a cloud table is the cloud's, aimed at the partition there too
	stop, err = e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{
		"customers_app": map[string]any{e.db: map[string]string{"1": dsn("customer", e.db, "lokadb_dml")}},
	})
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	if err := run("fdw+direct", forms[:3]); err != nil {
		return false, "", err
	}
	return ok, strings.Join(notes, "; "), nil
}

// policyRows: the rows of sql on the parent under the policy of organization 1, as the
// application's role (the meaning the statement has, whatever way the proxy sends it).
func policyRows(e *env, sql string, params []string) ([]string, error) {
	c, err := pgconn.Connect(e.ctx, dsn(e.cloudHost, e.db, "app"))
	if err != nil {
		return nil, err
	}
	defer c.Close(e.ctx)
	if _, err := c.Exec(e.ctx, "SELECT set_config('lokadb.org', '1', false)").ReadAll(); err != nil {
		return nil, err
	}
	return proxyRows(e, c, sql, params)
}

// proxyRows: the rows of sql on c (the query string, or with params as a prepared statement's),
// each row its values joined by ",", in the order the statement gives them.
func proxyRows(e *env, c *pgconn.PgConn, sql string, params []string) ([]string, error) {
	var res *pgconn.Result
	if params == nil {
		rs, err := c.Exec(e.ctx, sql).ReadAll()
		if err != nil {
			return nil, err
		}
		for _, r := range rs { // the result with rows: a statement of its own, or the one of BEGIN; …; COMMIT
			if res == nil || len(r.FieldDescriptions) > 0 {
				res = r
			}
		}
		if res == nil {
			return nil, nil
		}
	} else {
		var vals [][]byte
		for _, p := range params {
			vals = append(vals, []byte(p))
		}
		res = c.ExecParams(e.ctx, sql, vals, nil, nil, nil).Read()
		if res.Err != nil {
			return nil, res.Err
		}
	}
	var out []string
	for _, r := range res.Rows {
		var cells []string
		for _, v := range r {
			if v == nil {
				cells = append(cells, "NULL")
			} else {
				cells = append(cells, string(v))
			}
		}
		out = append(out, strings.Join(cells, ","))
	}
	return out, nil
}

// rewrittenRoutes: how many route lines of the check's proxy log say the statement went as
// lokadb rewrote it (Analysis.Target: aimed at a partition).
func rewrittenRoutes(e *env) int {
	b, err := os.ReadFile(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, "msg=route") && strings.Contains(l, "rewritten=true") {
			n++
		}
	}
	return n
}
