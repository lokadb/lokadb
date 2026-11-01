package main

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func init() {
	checks = append(checks,
		check{id: "F14", name: "a policy of the application on a table (row security for its own roles) holds for a moved organization: through the proxy and straight at the foreign partition (R4-10)", run: checkMovedPolicy},
	)
}

// F14 (R4-10 of the third review). The application's own row security is its authorization
// (Supabase, PostgREST): a policy "reporter never sees hidden rows" on documents. enable keeps
// it on the parent. For a moved organization the proxy sent a statement of one organization
// straight to its foreign partition, which has no policy of the parent, and sync_grants gave
// the role the parent's privileges on that partition: reporter read and changed the hidden
// rows of organization 1 through the proxy and straight in the cloud, and not those of an
// organization that stays in the cloud. Now a table with a policy of the application is read
// and written through its parent (the policies apply there), and its partitions get no
// privileges of the roles the policies are for.
func checkMovedPolicy(e *env) (bool, string, error) {
	admin := dsn(e.cloudHost, "postgres", "postgres")
	if err := execSQL(e.ctx, admin,
		`DO $$ BEGIN CREATE ROLE reporter LOGIN PASSWORD 'reporter'; EXCEPTION WHEN duplicate_object THEN END $$`); err != nil {
		return false, "", err
	}
	// the application's policy before enable, as the README says: it stays on the parent
	if err := execSQL(e.ctx, e.cloud(),
		"GRANT SELECT, UPDATE ON documents TO reporter", "GRANT SELECT ON users TO reporter",
		"ALTER TABLE documents ENABLE ROW LEVEL SECURITY",
		"CREATE POLICY reporter_visible ON documents TO reporter USING (title NOT LIKE 'hidden%')",
		"UPDATE documents SET title = 'hidden ' || id WHERE id % 5 = 0"); err != nil {
		return false, "", err
	}
	hidden := func(org int) (string, error) {
		return one(e.ctx, e.cloud(), fmt.Sprintf("SELECT count(*)::text FROM documents WHERE organization_id = %d AND title LIKE 'hidden%%'", org))
	}
	h1, err := hidden(1)
	if err != nil {
		return false, "", err
	}
	stop, err := e.moved()
	if err != nil {
		return false, "", err
	}
	defer stop()
	// straight in the cloud: the partition is not a way round the parent's policy
	d, err := pgx.Connect(e.ctx, dsn(e.cloudHost, e.db, "reporter"))
	if err != nil {
		return false, "", err
	}
	defer d.Close(e.ctx)
	var part string
	if err := d.QueryRow(e.ctx, "SELECT count(*)::text FROM documents__org1 WHERE title LIKE 'hidden%'").Scan(&part); err != nil {
		part = "error: " + firstErrLine(err)
	}
	// a database moved before sync_grants kept its privileges away from such a partition still
	// has the parent's privileges on it: the proxy must not aim a statement there either
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT, UPDATE ON documents__org1 TO reporter"); err != nil {
		return false, "", err
	}
	defer execSQL(e.ctx, e.cloud(), "REVOKE ALL ON documents__org1 FROM reporter")
	c, err := pgx.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=reporter password=reporter sslmode=disable", e.port, e.db))
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	ask := func(sql string) string {
		var v string
		if err := c.QueryRow(e.ctx, sql).Scan(&v); err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	seen1 := ask("SELECT count(*)::text FROM documents WHERE organization_id = 1 AND title LIKE 'hidden%'")
	seen2 := ask("SELECT count(*)::text FROM documents WHERE organization_id = 2 AND title LIKE 'hidden%'")
	all1 := ask("SELECT count(*)::text FROM documents WHERE organization_id = 1")
	// the same join form the proxy aims at the partition without a policy (F13)
	join1 := ask("SELECT count(*)::text FROM documents d JOIN users u ON u.id = d.user_id WHERE d.organization_id = 1 AND d.title LIKE 'hidden%'")
	tag, upErr := c.Exec(e.ctx, "UPDATE documents SET title = 'changed by reporter' WHERE organization_id = 1 AND title LIKE 'hidden%'")
	changed, err := one(e.ctx, e.cloud(), "SELECT count(*)::text FROM documents WHERE organization_id = 1 AND title = 'changed by reporter'")
	if err != nil {
		return false, "", err
	}
	via := part
	if strings.HasPrefix(part, "error:") {
		via = "refused"
	}
	ok := h1 != "0" && seen1 == "0" && seen2 == "0" && all1 == "80" && join1 == "0" &&
		upErr == nil && tag.RowsAffected() == 0 && changed == "0" && (via == "refused" || via == "0")
	return ok, fmt.Sprintf("organization 1 holds %s hidden rows; straight at documents__org1: %s; with the parent's privileges on it all the same, reporter through the proxy sees %s of them (organization 2, in the cloud: %s), %s rows of the organization, %s by a join; UPDATE of the hidden rows: %s rows (%s), changed in the cloud %s",
		h1, part, seen1, seen2, all1, join1, rowsOf(tag.RowsAffected(), upErr), errText(upErr), changed), nil
}

func rowsOf(n int64, err error) string {
	if err != nil {
		return "-"
	}
	return fmt.Sprint(n)
}
