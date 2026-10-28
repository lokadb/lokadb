package main

// lokadb check: what it says about privileges, extensions and the spec matches what enable
// and move-org then do; and the proxy refuses LokaDB's own objects, not lookalikes.

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "C1", name: "check reports what a read-only role cannot do (privileges, extensions), as move-org then fails", run: checkReadOnlyRole},
		check{id: "C2", name: "check -customer-app: the admin role must be able to give it the tables", run: checkCustomerApp},
		check{id: "C3", name: "a role that may subscribe but not create schemas: check and move-org pick triggers", run: checkSubscribeNoSchema},
		check{id: "C4", name: "check tells whether its role can set up postgres_fdw, and move-org installs it", run: checkCloudFDW},
		check{id: "C5", name: "the proxy refuses LokaDB's own objects (DROP of a partition too), not the application's lookalikes", run: checkLookalikes},
		check{id: "C6", name: "a spec listing a child before its parent: check and enable take it", run: checkSpecOrder},
		check{id: "C7", name: "a misspelt table in the spec: INVALID SPEC with a hint, not BLOCKED", run: checkSpecTypo},
		check{id: "C8", name: "an extension only a plain index needs: check warns, move-org makes the table without that index", run: checkIndexOnlyExtension},
	)
}

// runCheck runs lokadb check and returns its output and exit code.
func (e *env) runCheck(args ...string) (string, int) {
	out, err := exec.CommandContext(e.ctx, e.bin, append([]string{"check"}, args...)...).CombinedOutput()
	var x *exec.ExitError
	if errors.As(err, &x) {
		return string(out), x.ExitCode()
	}
	if err != nil {
		return string(out) + err.Error(), -1
	}
	return string(out), 0
}

func (e *env) checkWith(specBody string, args ...string) (string, int) {
	p, _ := e.specFile(specBody)
	return e.runCheck(append([]string{"-db", e.cloud(), "-spec", p}, args...)...)
}

func reported(out, code string) bool { return strings.Contains(out, "["+code+"]") }

// finding: the message of a finding, shortened.
func finding(out, code string) string {
	for _, l := range strings.Split(out, "\n") {
		if strings.Contains(l, "["+code+"]") {
			l = strings.TrimSpace(l)
			if len(l) > 220 {
				l = l[:220] + "…"
			}
			return l
		}
	}
	return "-"
}

func verdict(out string) string {
	if m := regexp.MustCompile(`Verdict: ([A-Z ]+):`).FindStringSubmatch(out); m != nil {
		return m[1]
	}
	return "?"
}

// role creates a login role (password = name) in the customer's or cloud cluster.
func role(e *env, host, name string, grants ...string) error {
	return execSQL(e.ctx, dsn(host, "postgres", "postgres"),
		append([]string{fmt.Sprintf(`DO $$ BEGIN CREATE ROLE %s LOGIN PASSWORD '%s'; EXCEPTION WHEN duplicate_object THEN END $$`, name, name)}, grants...)...)
}

// C1. CREATE on the schema and the database are granted, but every transaction of the role
// is read-only (a standby, default_transaction_read_only): the catalog says yes, move-org fails.
func checkReadOnlyRole(e *env) (bool, string, error) {
	ro := "ro_" + e.db
	if err := role(e, "customer", ro, "ALTER ROLE "+ro+" IN DATABASE "+e.db+" SET default_transaction_read_only = on"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON DATABASE "+e.db+" TO "+ro, "GRANT CREATE ON SCHEMA public TO "+ro); err != nil {
		return false, "", err
	}
	// a trusted extension the tables need (a column type): installable with CREATE on the database
	if err := execSQL(e.ctx, e.cloud(), "CREATE EXTENSION citext", "ALTER TABLE documents ADD COLUMN tag citext"); err != nil {
		return false, "", err
	}
	out, _ := e.checkWith(spec, "-customer", dsn("customer", e.db, ro))
	priv, ext := reported(out, "customer_privileges"), reported(out, "customer_extension_privilege")
	p, _ := e.specFile(spec)
	mv, moveErr := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app")
	if moveErr == nil {
		mv, moveErr = e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-mode", "trigger", "-customer", dsn("customer", e.db, ro))
	}
	why := "move-org passed"
	if moveErr != nil {
		why = "move-org failed: " + lastErr(mv)
	}
	return priv && ext && moveErr != nil, fmt.Sprintf("customer_privileges: %v (%s); customer_extension_privilege: %v (%s); %s",
		priv, finding(out, "customer_privileges"), ext, finding(out, "customer_extension_privilege"), why), nil
}

// lastErr: the error line move-org ended with.
func lastErr(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if strings.Contains(lines[i], "ERROR") || strings.HasPrefix(lines[i], "error:") {
			l := lines[i]
			if i+1 < len(lines) && strings.HasPrefix(strings.TrimSpace(lines[i+1]), "in:") {
				l += " " + strings.TrimSpace(lines[i+1]) // the statement
			}
			if len(l) > 300 {
				l = l[:300] + "…"
			}
			return l
		}
	}
	return lines[len(lines)-1]
}

// C2. The admin role may create tables but not give them to the role the cloud keeps.
func checkCustomerApp(e *env) (bool, string, error) {
	adm := "adm_" + e.db
	if err := role(e, "customer", adm); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON SCHEMA public TO "+adm); err != nil {
		return false, "", err
	}
	admDSN := dsn("customer", e.db, adm)
	before, _ := e.checkWith(spec, "-customer", admDSN, "-customer-app", e.appCustomer())
	if strings.Contains(before, "flag provided but not defined") {
		return false, "check has no -customer-app", nil
	}
	wrong, _ := e.checkWith(spec, "-customer", admDSN, "-customer-app", strings.Replace(e.appCustomer(), "password=lokadb_app", "password=wrong", 1))
	if err := execSQL(e.ctx, e.customer(), "GRANT lokadb_app TO "+adm, "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return false, "", err
	}
	after, _ := e.checkWith(spec, "-customer", admDSN, "-customer-app", e.appCustomer())
	ok := reported(before, "customer_app_owner") && reported(wrong, "customer_app_unreachable") && !reported(after, "customer_app_owner") && !reported(after, "customer_app_unreachable")
	return ok, fmt.Sprintf("before the grants: %s; a wrong password: %v; after GRANT lokadb_app TO %s and CREATE on public to lokadb_app: customer_app_owner %v",
		finding(before, "customer_app_owner"), reported(wrong, "customer_app_unreachable"), adm, reported(after, "customer_app_owner")), nil
}

// C3. pg_create_subscription but no CREATE on the database (PostgreSQL 16+ wants both): a
// logical move cannot run, a move by triggers can.
func checkSubscribeNoSchema(e *env) (bool, string, error) {
	sub := "sub_" + e.db
	if err := role(e, "customer", sub, "GRANT pg_create_subscription TO "+sub); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON SCHEMA public TO "+sub); err != nil {
		return false, "", err
	}
	subDSN := dsn("customer", e.db, sub)
	out, _ := e.checkWith(spec, "-customer", subDSN)
	priv := reported(out, "customer_privileges")
	mode := finding(out, "move_mode")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	mv, moveErr := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-customer", subDSN, "-publisher", e.cloud())
	moved := "move-org (auto) passed"
	if moveErr != nil {
		moved = "move-org (auto) failed: " + lastErr(mv)
	}
	if m := regexp.MustCompile(`msg="move mode" org=1 mode=(\w+)`).FindStringSubmatch(mv); m != nil {
		moved += ", mode " + m[1]
	}
	trig := strings.Contains(mode, "triggers")
	return !priv && trig && moveErr == nil, fmt.Sprintf("customer_privileges: %v (%s); %s; %s", priv, finding(out, "customer_privileges"), mode, moved), nil
}

// C4. (a) The check's cloud role may read everything but not use postgres_fdw: move-org
// would fail at CREATE SERVER. (b) postgres_fdw not installed: move-org installs it.
func checkCloudFDW(e *env) (bool, string, error) {
	ck := "ck_" + e.db
	if err := role(e, "cloud", ck, "GRANT pg_read_all_data TO "+ck); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	ckDSN := dsn("cloud", e.db, ck)
	noUsage, _ := e.runCheck("-db", ckDSN, "-spec", p)
	if err := execSQL(e.ctx, e.cloud(), "GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO "+ck); err != nil {
		return false, "", err
	}
	usage, _ := e.runCheck("-db", ckDSN, "-spec", p)
	if err := execSQL(e.ctx, e.cloud(), "DROP EXTENSION postgres_fdw"); err != nil {
		return false, "", err
	}
	missing, _ := e.runCheck("-db", e.cloud(), "-spec", p)
	if err := e.enable(); err != nil {
		return false, "", err
	}
	mv, moveErr := e.move(1)
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	ok := reported(noUsage, "cloud_postgres_fdw") && !reported(usage, "cloud_postgres_fdw") &&
		reported(missing, "cloud_postgres_fdw_install") && moveErr == nil
	return ok, fmt.Sprintf("without USAGE: %s; with USAGE: reported %v; not installed: %s; %s",
		finding(noUsage, "cloud_postgres_fdw"), reported(usage, "cloud_postgres_fdw"), finding(missing, "cloud_postgres_fdw_install"), moved), nil
}

// C5. After a move: the application's own documents__archive and its own foreign server
// run through the proxy; DROP TABLE documents__cloud (the cloud organizations' rows) does not.
func checkLookalikes(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO app"); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, dsn("cloud", e.db, "app"), "CREATE TABLE documents__archive (id bigint, title text)", "INSERT INTO documents__archive VALUES (1, 'old')"); err != nil {
		return false, "", err
	}
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
	run := func(q string) string {
		if _, err := simple(e.ctx, c, q); err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
				return "refused by the proxy"
			}
			return "failed: " + firstErrLine(err)
		}
		return "ran"
	}
	archive := run("SELECT count(*) FROM documents__archive")
	server := run("CREATE SERVER warehouse FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'dwh')")
	drop := run("DROP TABLE documents__cloud CASCADE")
	left, err := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil {
		left = "documents unreadable: " + firstErrLine(err)
	}
	return archive == "ran" && server == "ran" && drop == "refused by the proxy" && left == "100",
		fmt.Sprintf("SELECT FROM documents__archive: %s; CREATE SERVER warehouse: %s; DROP TABLE documents__cloud CASCADE: %s; documents of organization 2 in the cloud after it: %s",
			archive, server, drop, left), nil
}

func firstErrLine(err error) string {
	l, _, _ := strings.Cut(err.Error(), "\n")
	return l
}

// C6. document_versions (child) before documents (parent) in the spec file.
func checkSpecOrder(e *env) (bool, string, error) {
	const reversed = `{"tables": [
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"},
  {"name": "documents", "tenant_column": "organization_id"}
]}`
	out, code := e.checkWith(reversed)
	enableErr := e.enableSpec(reversed)
	enabled := "enable passed"
	if enableErr != nil {
		enabled = "enable failed: " + firstErrLine(enableErr)
	}
	return code != 3 && code != 5 && !reported(out, "spec_order") && enableErr == nil,
		fmt.Sprintf("check: %s (exit %d), spec_order reported %v; %s", verdict(out), code, reported(out, "spec_order"), enabled), nil
}

// C7. "documnets" in the spec.
func checkSpecTypo(e *env) (bool, string, error) {
	const typo = `{"tables": [
  {"name": "documnets", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documnets", "parent_column": "document_id"}
]}`
	out, code := e.checkWith(typo)
	hint := finding(out, "table_missing")
	return code == 5 && verdict(out) == "INVALID SPEC" && strings.Contains(hint, "did you mean documents"),
		fmt.Sprintf("check: %s (exit %d); %s; the child: %s", verdict(out), code, hint, finding(out, "parent_without_organization")), nil
}

// C8. pg_trgm only for a gin index; the organization's role may create tables in public but
// not install a trusted extension (that needs CREATE on the database).
func checkIndexOnlyExtension(e *env) (bool, string, error) {
	adm := "ix_" + e.db
	if err := role(e, "customer", adm); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(), "GRANT CREATE ON SCHEMA public TO "+adm); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE EXTENSION pg_trgm", "CREATE INDEX documents_title_trgm ON documents USING gin (title gin_trgm_ops)"); err != nil {
		return false, "", err
	}
	admDSN := dsn("customer", e.db, adm)
	out, _ := e.checkWith(spec, "-customer", admDSN)
	blocked, warned := reported(out, "customer_extension_privilege"), reported(out, "customer_index_skipped")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	p, _ := e.specFile(spec)
	mv, moveErr := e.lokadb("move-org", "-db", e.cloud(), "-spec", p, "-owner", "app", "-org", "1", "-customer", admDSN, "-publisher", e.cloud())
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents")
	idx, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE tablename = 'documents' AND indexdef LIKE '%gin_trgm_ops%'")
	viaCloud, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM documents WHERE organization_id = 1 AND title LIKE 'd1%'")
	return !blocked && warned && moveErr == nil && n == "100" && idx == "0" && viaCloud != "0",
		fmt.Sprintf("check: customer_extension_privilege %v, customer_index_skipped %v; %s; at the organization %s documents, trigram indexes %s; the cloud reads them: %s rows like 'd1%%'",
			blocked, warned, moved, n, idx, viaCloud), nil
}
