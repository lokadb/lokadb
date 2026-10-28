package main

// Security checks (research/ANALYSIS.md, section 5.2). The relay and agent ones (S4 window,
// S5, S6) need the tunnel: they are in e2e/run.sh.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "S1", name: "the proxy's log holds no literal of the application's queries", run: checkLogRedacted},
		check{id: "S2", name: "DDL replayed in the organization's database: safe forms only, owner only, unprivileged role", run: checkDDLReplay},
		check{id: "S3", name: "the cloud's roles reach nothing of the organization's database but the LokaDB tables", run: checkFDWReach},
		check{id: "S4", name: "the subscription connects as a role made for the move, dropped after it", run: checkReplicationRole},
		check{id: "S4", name: "a failed move leaves no subscription, slot, publication or role behind", run: checkFailedMoveCleanup},
	)
}

func (e *env) appCustomer() string { return dsn("customer", e.db, "lokadb_app") }

func (e *env) viaProxyAs(user string) (*pgconn.PgConn, error) {
	return pgconn.Connect(e.ctx, fmt.Sprintf("host=127.0.0.1 port=%d dbname=%s user=%s password=%s sslmode=disable", e.port, e.db, user, user))
}

// S1. The route log carried each statement with its literals: documents' titles and bodies.
func checkLogRedacted(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		stop()
		return false, "", err
	}
	for _, q := range []string{
		"INSERT INTO documents (organization_id, user_id, title) VALUES (1, 1, 'S1-secret-title')",
		"SELECT id FROM documents WHERE organization_id = 1 AND title = 'S1-secret-title'",
	} {
		if _, err := simple(e.ctx, c, q); err != nil {
			c.Close(e.ctx)
			stop()
			return false, "", err
		}
	}
	c.Close(e.ctx)
	stop()
	b, err := os.ReadFile(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	if err != nil {
		return false, "", err
	}
	log := string(b)
	routes := strings.Count(log, "msg=route")
	if strings.Contains(log, "S1-secret") {
		return false, fmt.Sprintf("the literal is in the proxy's log (%d route lines)", routes), nil
	}
	return routes >= 2, fmt.Sprintf("%d route lines, none with the literal", routes), nil
}

// S2. The proxy replayed a LokaDB table's DDL in the organization's database before the
// cloud ran it, with the credentials of its config (a superuser in the demo): any default
// expression ran there with those rights, and a role the cloud refuses still changed it.
func checkDDLReplay(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents TO intruder", "GRANT USAGE ON SCHEMA lokadb TO intruder"); err != nil {
		return false, "", err
	}
	// the proxy replays with what its config holds: the organization's admin role, as before the fix
	stop, err := e.proxyWith(map[string]string{"1": e.customer()})
	if err != nil {
		return false, "", err
	}
	defer stop()
	var notes []string
	run := func(user, sql string) error {
		c, err := e.viaProxyAs(user)
		if err != nil {
			return err
		}
		defer c.Close(e.ctx)
		_, err = simple(e.ctx, c, sql)
		return err
	}
	column := func(name string) string {
		n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM information_schema.columns WHERE table_name = 'documents' AND column_name = $1", name)
		return n
	}
	ok := true
	if err := run("app", "ALTER TABLE documents ADD COLUMN leak text DEFAULT pg_read_file('/etc/hostname')"); err == nil {
		notes = append(notes, "DEFAULT pg_read_file() accepted")
		ok = false
	}
	if n := column("leak"); n != "0" {
		notes = append(notes, "the organization's database got the column with pg_read_file()'s result")
		ok = false
	}
	if err := run("intruder", "ALTER TABLE documents ADD COLUMN intr text"); err == nil {
		notes = append(notes, "a role that does not own documents changed it")
		ok = false
	}
	if n := column("intr"); n != "0" {
		notes = append(notes, "the organization's database got a column from a role the cloud refused")
		ok = false
	}
	if err := run("app", "ALTER TABLE documents ADD COLUMN note text DEFAULT 'n'"); err != nil {
		notes = append(notes, "a plain ADD COLUMN failed: "+oneLine(err.Error()))
		ok = false
	}
	owner, err := one(e.ctx, e.customer(), "SELECT tableowner FROM pg_tables WHERE tablename = 'documents'")
	if err != nil {
		return false, "", err
	}
	if owner != "lokadb_app" {
		ok = false
	}
	notes = append(notes, "plain ADD COLUMN replayed: "+map[bool]string{true: "yes", false: "no"}[column("note") == "1"],
		"owner of the moved table there: "+owner)
	return ok, strings.Join(notes, "; "), nil
}

// S3. The user mapping was FOR PUBLIC with the organization's admin credentials and the
// application role had USAGE on the server: it could create its own foreign table and
// read any table of the organization's database.
func checkFDWReach(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	ok := true
	var notes []string
	public, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM pg_user_mappings WHERE srvname = 'lokadb_org1' AND usename = 'public'")
	if err != nil {
		return false, "", err
	}
	usage, err := one(e.ctx, e.cloud(), "SELECT has_server_privilege('app', 'lokadb_org1', 'USAGE')")
	if err != nil {
		return false, "", err
	}
	mapped, err := one(e.ctx, e.cloud(), "SELECT string_agg(DISTINCT u.option, ',') FROM pg_user_mappings m, unnest(m.umoptions) u(option) WHERE m.srvname = 'lokadb_org1' AND u.option LIKE 'user=%'")
	if err != nil {
		return false, "", err
	}
	notes = append(notes, "mappings FOR PUBLIC: "+public, "app has USAGE on the server: "+usage, "mapped to "+mapped)
	if public != "0" || usage != "false" || mapped != "user=lokadb_app" {
		ok = false
	}
	// directly in the cloud database, as the application role
	if err := execSQL(e.ctx, dsn("cloud", e.db, "app"),
		"CREATE FOREIGN TABLE leak (rolname text) SERVER lokadb_org1 OPTIONS (schema_name 'pg_catalog', table_name 'pg_authid')"); err == nil {
		n, rerr := one(e.ctx, dsn("cloud", e.db, "app"), "SELECT count(*) FROM leak")
		notes = append(notes, fmt.Sprintf("app created a foreign table on pg_authid of the organization's database (rows %s, %v)", n, rerr))
		ok = false
	} else {
		notes = append(notes, "app cannot create a foreign table on the server")
	}
	// the application still reads its moved organization
	n, err := one(e.ctx, dsn("cloud", e.db, "app"), "SELECT count(*) FROM documents WHERE organization_id = 1")
	if err != nil || n != "100" {
		notes = append(notes, fmt.Sprintf("app reads the moved organization: %s %v", n, err))
		ok = false
	}
	// through the proxy: LokaDB's own objects and FDW commands are refused
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, q := range []string{
		"SELECT count(*) FROM documents__org1",
		"CREATE FOREIGN TABLE leak2 (rolname text) SERVER lokadb_org1 OPTIONS (table_name 'x')",
	} {
		if _, err := simple(e.ctx, c, q); err == nil {
			notes = append(notes, "the proxy ran: "+q)
			ok = false
		}
	}
	return ok, strings.Join(notes, "; "), nil
}

// S4. The subscription kept the cloud's own admin DSN in the organization's database.
func checkReplicationRole(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	var users string
	// -mode logical: it is the subscription's role that is looked at (auto is triggers: K-1)
	_, _, moveErr := e.moveLines(1, func(line string) {
		if strings.Contains(line, "replica caught up") && users == "" {
			users, _ = one(e.ctx, e.cloud(), "SELECT coalesce(string_agg(DISTINCT usename::text, ','), '') FROM pg_stat_replication")
		}
	}, "-mode", "logical")
	if moveErr != nil {
		return false, "", moveErr
	}
	left, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	if moveMode == "trigger" { // nothing streams: the cloud is never connected to
		return users == "" && left == "", fmt.Sprintf("trigger mode: streamed as %q; left after the move: %q", users, left), nil
	}
	return strings.HasPrefix(users, "lokadb_repl_") && strings.HasSuffix(users, "_org1") && !strings.Contains(users, ",") && left == "", fmt.Sprintf("the subscription streamed as %q; left after the move: %q", users, left), nil
}

// leftovers: what of a move still exists after it (subscription, slot, publication, role).
func leftovers(e *env) (string, error) {
	var out []string
	for _, q := range []struct{ what, dsn, sql string }{
		{"subscription", e.customer(), "SELECT count(*) FROM pg_subscription"},
		{"slot", e.cloud(), "SELECT count(*) FROM pg_replication_slots"},
		{"publication", e.cloud(), "SELECT count(*) FROM pg_publication"},
		{"role", e.cloud(), "SELECT count(*) FROM pg_roles WHERE rolname LIKE 'lokadb\\_repl%'"},
		{"capture trigger", e.cloud(), "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'lokadb\\_move\\_org%'"},
	} {
		n, err := one(e.ctx, q.dsn, q.sql)
		if err != nil {
			return "", err
		}
		if n != "0" {
			out = append(out, q.what+"="+n)
		}
	}
	// the move log of the trigger mode, if it was ever created
	if exists, err := one(e.ctx, e.cloud(), "SELECT (to_regclass('lokadb.move_log') IS NOT NULL)::text"); err != nil {
		return "", err
	} else if exists == "true" {
		n, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.move_log")
		if err != nil {
			return "", err
		}
		if n != "0" {
			out = append(out, "move log="+n)
		}
	}
	return strings.Join(out, " "), nil
}

// S4 / R4. A move that fails after the subscription exists left the subscription, its
// slot (WAL kept forever) and the publication; now also the replication role.
func checkFailedMoveCleanup(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	// The organization's partition exists before the move (enable leaves it in DEFAULT):
	// otherwise the writer below would stop move-org's first step, before any subscription.
	if err := execSQL(e.ctx, e.cloud(), "SELECT lokadb.partition_org(table_name, 1) FROM lokadb.tables"); err != nil {
		return false, "", err
	}
	// a writer that holds the parent all along: every switch attempt times out
	holder, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer holder.Close(e.ctx)
	tx, err := holder.Begin(e.ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(e.ctx)
	if _, err := tx.Exec(e.ctx, "LOCK TABLE ONLY documents IN ROW EXCLUSIVE MODE"); err != nil {
		return false, "", err
	}
	start := time.Now()
	_, moveErr := e.move(1, "-lock-timeout", "200ms")
	_ = tx.Rollback(e.ctx)
	if moveErr == nil {
		return false, "move-org succeeded although the switch could not lock", nil
	}
	left, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	return left == "" && n == "100", fmt.Sprintf("move-org failed after %s as expected; left behind: %q; organization 1 still in the cloud: %s rows; move-org said: %s",
		time.Since(start).Round(time.Second), left, n, moveErr), nil
}
