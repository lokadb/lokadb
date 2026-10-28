package main

// Block 4, item 8: lokadb checks FKs from LokaDB tables to cloud tables for the moved
// organizations, whose rows the cloud table's FK cannot reach (fkcloud.go in admin).

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "X14", name: "an FK onto a cloud table of routing.cloud_tables (ON DELETE RESTRICT) holds for a moved organization, in its own database only, under fanout reject", run: checkFKToCloudTable},
		check{id: "X14b", name: "through the proxy its fanout policy decides a delete of a cloud row referenced by a moved organization: reject refuses, log cascades", run: checkFKToCloudProxy},
		check{id: "X14c", name: "under fanout log a delete of a referenced cloud row is refused whole while a moved organization's database is unreachable", run: checkFKToCloudDown},
		check{id: "X14e", name: "the organizations table is a cloud table whose organization is its id (no declaration): under fanout reject-writes deleting a moved organization's row cascades in its own database only; deleting a shared user is refused as the policy says", run: checkOrganizationsTable},
		check{id: "X14f", name: "the refusal of a delete of a referenced cloud row names what is possible: a table with an organization column is told to declare it, a table without one (users) is not", run: checkFKToCloudRefusalWording},
		check{id: "X14d", name: "a client of the proxy cannot set lokadb.fanout (SET, set_config, a startup parameter, options): under reject the delete stays refused", run: checkFanoutGuard},
	)
}

const x14spec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}
], "routing": {"cloud_tables": [{"table": "projects", "tenant_column": "organization_id"}]}}`

// X14. documents.project_id -> projects ON DELETE RESTRICT; projects knows the organization
// of its rows (routing.cloud_tables). Organization 1 is moved. Project 11 (organization 1) is
// referenced, 12 (organization 1) is not, 21 (organization 2) is.
func checkFKToCloudTable(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE projects (id bigint PRIMARY KEY, organization_id bigint NOT NULL, name text)",
		"ALTER TABLE projects OWNER TO app",
		"INSERT INTO projects VALUES (11, 1, 'used'), (12, 1, 'unused'), (21, 2, 'used')",
		"ALTER TABLE documents ADD COLUMN project_id bigint REFERENCES projects ON DELETE RESTRICT",
		"UPDATE documents SET project_id = 10 * organization_id + 1"); err != nil {
		return false, "", err
	}
	e.spec = x14spec
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	try := func(sql string) string {
		if err := execSQL(e.ctx, e.cloud(), sql); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "done"
	}
	used1 := try("DELETE FROM projects WHERE id = 11")
	unused1 := try("DELETE FROM projects WHERE id = 12")
	used2 := try("DELETE FROM projects WHERE id = 21")
	missing := try("INSERT INTO documents (organization_id, project_id, title) VALUES (1, 99, 'x14')")
	present := try("INSERT INTO documents (organization_id, project_id, title) VALUES (1, 11, 'x14')")
	fk := func(s string) bool { return strings.Contains(s, "foreign key constraint") } // RESTRICT: "violates RESTRICT setting of foreign key constraint"
	ok := fk(used1) && unused1 == "done" && fk(used2) && fk(missing) && present == "done"
	return ok, fmt.Sprintf("delete project 11 (referenced by moved organization 1): %s; 12 (not referenced): %s; 21 (organization 2, in the cloud): %s; a document of organization 1 for a missing project: %s, for project 11: %s",
		used1, unused1, used2, missing, present), nil
}

// X14b. users is not in routing.cloud_tables: the proxy gives its fanout policy to the
// database (lokadb.fanout), and lokadb's trigger follows it. Users 1001 and 1002 each have a
// document of the moved organization 1.
func checkFKToCloudProxy(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO users (id, name) VALUES (1001, 'x14'), (1002, 'x14')",
		"INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1001, 'x14p-1001'), (1, 1002, 'x14p-1002')"); err != nil {
		return false, "", err
	}
	via := func(mode string, user int) string {
		stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": mode}}})
		if err != nil {
			return "proxy: " + err.Error()
		}
		defer stop()
		c, err := e.viaProxy()
		if err != nil {
			return "connect: " + err.Error()
		}
		defer c.Close(e.ctx)
		if _, err := simple(e.ctx, c, fmt.Sprintf("DELETE FROM users WHERE id = %d", user)); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "deleted"
	}
	rejected, logged := via("reject", 1001), via("log", 1002)
	left, _ := one(e.ctx, e.customer(), "SELECT coalesce(string_agg(title, ', ' ORDER BY title), '') FROM documents WHERE title LIKE 'x14p-%'")
	ok := strings.Contains(rejected, "fanout policy (reject)") && strings.Contains(rejected, "users has no column that holds the organization of its rows") && strings.Contains(rejected, "allow-list entry") && !strings.Contains(rejected, "declare users in routing.cloud_tables with") &&
		logged == "deleted" && left == "x14p-1001"
	return ok, fmt.Sprintf("via the proxy, delete user 1001 under reject: %s; user 1002 under log: %s; documents left in organization 1's database: %s",
		rejected, logged, left), nil
}

// X14c. Organization 1's database cannot be reached (its FDW server points at a closed port):
// under log the delete of user 1001, which organization 2 references as well, is refused
// whole, and the cloud keeps the user and organization 2's document.
func checkFKToCloudDown(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	port, err := one(e.ctx, e.cloud(), "SELECT option_value FROM pg_foreign_server s, pg_options_to_table(s.srvoptions) WHERE s.srvname = 'lokadb_org1' AND option_name = 'port'")
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO users (id, name) VALUES (1001, 'x14')",
		"INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1001, 'x14d-1'), (2, 1001, 'x14d-2')",
		"ALTER SERVER lokadb_org1 OPTIONS (SET port '1')"); err != nil {
		return false, "", err
	}
	deleted := "deleted"
	if err := execSQL(e.ctx, e.cloud(), "SET lokadb.fanout = 'log'", "DELETE FROM users WHERE id = 1001"); err != nil {
		deleted = "refused: " + firstErrLine(err)
	}
	if err := execSQL(e.ctx, e.cloud(), "ALTER SERVER lokadb_org1 OPTIONS (SET port '"+port+"')"); err != nil {
		return false, "", err
	}
	kept, _ := one(e.ctx, e.cloud(), "SELECT (SELECT count(*) FROM users WHERE id = 1001) || ' user, ' || (SELECT count(*) FROM documents WHERE title = 'x14d-2') || ' document of organization 2'")
	org1, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'x14d-1'")
	ok := strings.HasPrefix(deleted, "refused") && kept == "1 user, 1 document of organization 2" && org1 == "1"
	return ok, fmt.Sprintf("organization 1 unreachable, delete user 1001 under log: %s; the cloud keeps %s; organization 1's database keeps %s document(s)", deleted, kept, org1), nil
}

// X14d. The proxy's policy is reject. The application tries to switch it to log for its own
// delete of user 1001 (referenced by moved organization 1): by SET, by set_config, by a
// startup parameter and by -c in options. Each is refused or overridden: no cascade.
func checkFanoutGuard(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO users (id, name) VALUES (1001, 'x14')",
		"INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1001, 'x14g')"); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	try := func(params map[string]string, sqls ...string) string {
		cfg, err := pgconn.ParseConfig(fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=app password=app sslmode=disable", e.port, e.db))
		if err != nil {
			return "config: " + err.Error()
		}
		for k, v := range params {
			cfg.RuntimeParams[k] = v
		}
		c, err := pgconn.ConnectConfig(e.ctx, cfg)
		if err != nil {
			return "connection refused: " + firstErrLine(err)
		}
		defer c.Close(e.ctx)
		for _, q := range sqls {
			if _, err := simple(e.ctx, c, q); err != nil {
				return "refused: " + firstErrLine(err)
			}
		}
		return "deleted"
	}
	del := "DELETE FROM users WHERE id = 1001"
	got := []string{
		try(nil, "SET lokadb.fanout = 'log'", del),
		try(nil, "SELECT set_config('lokadb.fanout', 'log', false)", del),
		try(nil, "BEGIN; SET LOCAL lokadb.fanout = 'log'; "+del+"; COMMIT"),
		try(map[string]string{"lokadb.fanout": "log"}, del),
		try(map[string]string{"options": "-c lokadb.fanout=log"}, del),
	}
	left, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'x14g'")
	user, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM users WHERE id = 1001")
	ok := left == "1" && user == "1"
	for _, g := range got {
		ok = ok && strings.Contains(g, "refused")
	}
	return ok, fmt.Sprintf("SET: %s | set_config: %s | SET LOCAL in the string: %s | startup parameter: %s | options -c: %s; user 1001 left: %s, its document in organization 1: %s",
		got[0], got[1], got[2], got[3], got[4], user, left), nil
}

// X15. documents.organization_id -> organizations ON DELETE CASCADE, and the spec does not
// declare organizations: enable takes it as a cloud table whose organization column is its id.
// Organization 1 is moved. Under reject-writes (a CASCADE writes) deleting organization 1's
// row runs in organization 1's database only; deleting user 3, whom documents of the moved
// organization 1 and of organization 2 reference, is refused: users has no organization column.
func checkOrganizationsTable(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE organizations (id bigint PRIMARY KEY, name text)",
		"ALTER TABLE organizations OWNER TO app",
		"INSERT INTO organizations VALUES (1, 'one'), (2, 'two')",
		"ALTER TABLE documents ADD CONSTRAINT documents_org_fk FOREIGN KEY (organization_id) REFERENCES organizations ON DELETE CASCADE"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	lookup, _ := one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(column_name || ':' || coalesce(refs_table, 'NULL'), ','), '') FROM lokadb.lookups WHERE table_name = 'organizations'")
	before2, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	user := "done"
	if err := execSQL(e.ctx, e.cloud(), "SET lokadb.fanout = 'reject-writes'", "DELETE FROM users WHERE id = 3"); err != nil {
		user = "refused: " + firstErrLine(err)
	}
	org := "done"
	if err := execSQL(e.ctx, e.cloud(), "SET lokadb.fanout = 'reject-writes'", "DELETE FROM organizations WHERE id = 1"); err != nil {
		org = "refused: " + firstErrLine(err)
	}
	left1, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE organization_id = 1")
	after2, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	ok := lookup == "id:NULL" && org == "done" && left1 == "0" && after2 == before2 && before2 != "0" &&
		strings.HasPrefix(user, "refused") && strings.Contains(user, "fanout policy (reject-writes)")
	return ok, fmt.Sprintf("lookups of organizations: %s; delete organization 1 under reject-writes: %s; organization 1's database has %s document(s) left, organization 2 in the cloud %s of %s; delete user 3: %s",
		lookup, org, left1, after2, before2, user), nil
}

// X14f. projects has the organization column but is not declared: the refusal under reject
// tells to declare it. (users, which has none, is X14b.)
func checkFKToCloudRefusalWording(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE projects (id bigint PRIMARY KEY, organization_id bigint NOT NULL)",
		"ALTER TABLE projects OWNER TO app",
		"INSERT INTO projects VALUES (11, 1), (21, 2)",
		"ALTER TABLE documents ADD COLUMN project_id bigint REFERENCES projects ON DELETE CASCADE",
		"UPDATE documents SET project_id = 10 * organization_id + 1"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if mv, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(mv), nil
	}
	msg := "done"
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM projects WHERE id = 21"); err != nil {
		msg = firstErrLine(err)
	}
	ok := strings.Contains(msg, "declare projects in routing.cloud_tables with the column that holds the organization of its rows") && !strings.Contains(msg, "no column that holds")
	return ok, "delete project 21 undeclared, under the default fanout policy: " + msg, nil
}
