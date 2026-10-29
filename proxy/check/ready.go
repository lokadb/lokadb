package check

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// cloudReady: what move-org needs from the cloud database.
func (c *checker) cloudReady() error {
	r, err := c.rows(`SELECT current_setting('wal_level'), current_setting('max_replication_slots'),
		current_setting('max_wal_senders'), (SELECT count(*) FROM pg_available_extensions WHERE name = 'postgres_fdw')::text,
		current_setting('server_version_num')`)
	if err != nil {
		return err
	}
	v := r[0]
	// Without logical replication move-org moves with triggers (-mode trigger): not a blocker.
	c.cloudLogical = v[0] == "logical" && v[1] != "0" && v[2] != "0"
	if v[0] != "logical" {
		c.add(Info, "cloud_wal_level", "wal_level", fmt.Sprintf("the cloud database has wal_level = %s: move-org moves organizations with triggers (no logical replication; wal_level = logical, a restart, would let it subscribe instead)", v[0]), nil)
	} else if v[1] == "0" || v[2] == "0" {
		c.add(Info, "cloud_replication_slots", "", fmt.Sprintf("max_replication_slots = %s, max_wal_senders = %s: move-org moves organizations with triggers", v[1], v[2]), nil)
	}
	// What an interrupted move (move-org killed) left: capture triggers, the log, slots.
	if running, err := admin.MoveRunning(c.ctx, c.tx); err == nil && !running {
		if left, err := admin.MoveLeftovers(c.ctx, c.tx); err == nil && len(left) > 0 {
			c.add(Warning, "move_leftovers", "", "an interrupted move-org left these behind (no move runs now): run lokadb move-cleanup", left)
		}
	}
	c.cloudFDW(v[3] == "0")
	c.cloudVersion, _ = strconv.Atoi(v[4])
	enc, err := c.rows(`SELECT pg_encoding_to_char(encoding)::text, datcollate::text FROM pg_database WHERE datname = current_database()`)
	if err != nil {
		return err
	}
	c.cloudEncoding, c.cloudCollate = enc[0][0], enc[0][1]
	return c.appRolesRLS()
}

// appRolesRLS: the proxy keeps an organization's rows apart with the RLS policy lokadb_tenant
// (lokadb.org). A superuser or a BYPASSRLS role is not subject to it: through the proxy its
// query without the organization reads and writes every organization's rows. The roles are
// those of -app-role, else those pg_stat_statements counts as the application; with neither
// the login roles that would bypass it are listed.
func (c *checker) appRolesRLS() error {
	var roles []string
	src := "-app-role"
	if len(c.o.AppRoles) > 0 {
		roles = c.o.AppRoles
	} else {
		src = "pg_stat_statements"
		for r := range c.seenRoles {
			roles = append(roles, r)
		}
	}
	rls := roles
	if len(c.o.AppRoles) == 0 && len(c.o.RLSRoles) > 0 {
		src = "the application's settings"
		seen := map[string]bool{}
		rls = nil
		for _, r := range append(append([]string{}, c.o.RLSRoles...), roles...) {
			if !seen[r] {
				seen[r] = true
				rls = append(rls, r)
			}
		}
	}
	if len(rls) == 0 {
		r, err := c.rows(`SELECT rolname::text FROM pg_roles WHERE rolcanlogin AND (rolsuper OR rolbypassrls) AND rolname <> current_user ORDER BY 1`)
		if err != nil {
			return err
		}
		if len(r) > 0 {
			var l []string
			for _, x := range r {
				l = append(l, x[0])
			}
			c.add(Info, "app_role_unknown", "", "the application's roles are not known (no -app-role, no pg_stat_statements): it must not connect through the proxy as a superuser or a BYPASSRLS role, which the organization policy does not apply to. These login roles are such", l)
		}
		return nil
	}
	// the role itself, and (block 9, PR-11) one it can SET ROLE to: the proxy refuses a session
	// that can become a superuser, BYPASSRLS or REPLICATION role
	r, err := c.rows(`SELECT a.rolname::text,
		CASE WHEN a.rolname = b.rolname THEN '' ELSE ' (it can SET ROLE to ' || b.rolname || ')' END,
		CASE WHEN b.rolsuper THEN 'superuser' WHEN b.rolbypassrls THEN 'BYPASSRLS role' ELSE 'REPLICATION role' END
		FROM pg_roles a JOIN pg_roles b ON b.rolname = a.rolname OR pg_has_role(a.oid, b.oid, CASE WHEN current_setting('server_version_num')::int >= 160000 THEN 'SET' ELSE 'MEMBER' END)
		WHERE a.rolname = ANY($1) AND (b.rolsuper OR b.rolbypassrls OR b.rolreplication) AND (b.rolname = a.rolname OR NOT a.rolsuper) ORDER BY 1, 2`, rls)
	if err != nil {
		return err
	}
	if len(roles) > 0 {
		if err := c.cloudDirectAccess(roles); err != nil {
			return err
		}
	}
	for _, x := range r {
		c.add(Blocker, "app_role_bypasses_rls", x[0], fmt.Sprintf("the application's role %s (from %s)%s is a %s: row-level security does not apply to it (REPLICATION reads every organization's changes by logical decoding), so through the proxy its queries without the organization would read and change every organization's rows instead of being refused or kept to one, and the proxy does not let it in. Give the application a role without SUPERUSER, BYPASSRLS and REPLICATION that is not a member of one: make a separate administrator for LokaDB (LOKADB_DB), then ALTER ROLE %[1]s NOSUPERUSER NOBYPASSRLS NOREPLICATION (it stays the owner of the tables, so migrations keep working)",
			x[0], src, x[1], x[2]), nil)
	}
	return nil
}

// cloudFDW: an organization's partitions become postgres_fdw foreign tables. move-org
// installs the extension if it is missing and creates a server: tried here, rolled back.
func (c *checker) cloudFDW(unavailable bool) {
	const msg = "an organization's partitions become postgres_fdw foreign tables"
	if unavailable {
		c.add(Blocker, "cloud_postgres_fdw", "postgres_fdw", "postgres_fdw is not available in the cloud database (its package is not installed on that server): "+msg, nil)
		return
	}
	installed, err := c.rows(`SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'postgres_fdw')::text`)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()
	conn, err := ownsql.Connect(ctx, c.o.DSN) // the check's own transaction is read-only
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	steps := []string{ownMark + "CREATE EXTENSION IF NOT EXISTS postgres_fdw",
		ownMark + fmt.Sprintf("CREATE SERVER %s FOREIGN DATA WRAPPER postgres_fdw", admin.ProbeName("server"))}
	if err := admin.Probe(ctx, conn, steps...); err != nil {
		c.add(Blocker, "cloud_postgres_fdw", "postgres_fdw", fmt.Sprintf("the role of the check cannot set up postgres_fdw in the cloud database (%s): %s. Install the extension or grant USAGE on it",
			firstLine(strings.TrimPrefix(err.Error(), ownMark)), msg), nil)
		return
	}
	if installed[0][0] != "true" {
		c.add(Info, "cloud_postgres_fdw_install", "postgres_fdw", "postgres_fdw is not installed in the cloud database: move-org installs it (tried and rolled back)", nil)
	}
}

// customerDeps checks that the organization's database has, or move-org can create there,
// what the LokaDB tables use: move-org installs extensions and creates the application's
// collations, enums, domains, composite types and functions (admin.customerDeps), but not
// other types, operator classes or the collations of pg_catalog.
func (c *checker) customerDeps(ctx context.Context, conn *pgx.Conn, db string) {
	r, err := rowsOf(ctx, conn, `SELECT pg_encoding_to_char(encoding)::text, datcollate::text FROM pg_database WHERE datname = current_database()`)
	if err == nil {
		if r[0][0] != c.cloudEncoding {
			c.add(Warning, "customer_encoding", db, fmt.Sprintf("the organization's database uses encoding %s, the cloud %s: rows with characters %s cannot hold fail to copy", r[0][0], c.cloudEncoding, r[0][0]), nil)
		}
		if r[0][1] != c.cloudCollate {
			c.add(Change, "customer_collation_differs", db, fmt.Sprintf("the organization's database sorts text by %s, the cloud by %s: ORDER BY, comparisons and unique text keys of moved organizations behave differently", r[0][1], c.cloudCollate), nil)
		}
	}
	var unavailable, install, missing, collations, makeColl, create []*dependency
	var denied []*dependency
	why := "" // the first refusal of an extension
	exts := map[string]bool{}
	plain := map[string]bool{} // extension -> only plain indexes need it
	for _, d := range c.sortedDeps() {
		if d.ext != "" {
			if _, ok := plain[d.ext]; !ok {
				plain[d.ext] = true
			}
			plain[d.ext] = plain[d.ext] && d.plainIndex
		}
	}
	// The extensions as move-org plans them (admin.PlanExtensions: the cloud's version, what the
	// server has, the installation tried and rolled back).
	needs := map[string]*admin.ExtensionNeed{}
	var asDeps []admin.Dependency
	for _, d := range c.sortedDeps() {
		asDeps = append(asDeps, admin.Dependency{Kind: d.kind, Schema: d.schema, Name: d.name, Ext: d.ext, PlainIndex: d.plainIndex})
	}
	if list, err := admin.ExtensionNeeds(ctx, c.tx, asDeps, c.custSchema); err == nil {
		if err := admin.PlanExtensions(ctx, conn, list); err == nil {
			for _, n := range list {
				needs[n.Name] = n
			}
		}
	}
	var versionDiffers []string
	for _, d := range c.sortedDeps() {
		if d.ext != "" {
			if exts[d.ext] {
				continue
			}
			exts[d.ext] = true
			n := needs[d.ext]
			switch {
			case n == nil:
			case n.Present():
				if n.CustomerVersion != n.CloudVersion {
					versionDiffers = append(versionDiffers, fmt.Sprintf("%s: cloud %s, organization %s", n.Name, n.CloudVersion, n.CustomerVersion))
				}
			case n.Problem == admin.ExtPackage || n.Problem == admin.ExtVersion:
				unavailable = append(unavailable, d)
			case n.Problem != admin.ExtOK:
				denied = append(denied, d)
				if why == "" {
					why = n.Name + ": " + n.Detail
				}
			default:
				install = append(install, d)
			}
			continue
		}
		// $1 schema, $2 quoted name (a function's carries its argument types)
		probe := map[string]string{
			"type":           `SELECT (to_regtype(quote_ident($1) || '.' || $2) IS NOT NULL)::text`,
			"function":       `SELECT (to_regprocedure(quote_ident($1) || '.' || $2) IS NOT NULL)::text`,
			"operator class": `SELECT EXISTS (SELECT 1 FROM pg_opclass o JOIN pg_namespace n ON n.oid = o.opcnamespace WHERE n.nspname = $1 AND o.opcname = $2)::text`,
			"collation":      `SELECT EXISTS (SELECT 1 FROM pg_collation o JOIN pg_namespace n ON n.oid = o.collnamespace WHERE n.nspname = $1 AND o.collname = $2)::text`,
		}[d.kind]
		e, err := rowsOf(ctx, conn, probe, d.schema, d.name)
		if err != nil || e[0][0] == "true" {
			continue
		}
		switch {
		case d.kind == "collation" && d.created():
			makeColl = append(makeColl, d)
		case d.kind == "collation":
			collations = append(collations, d)
		case d.created():
			create = append(create, d)
		default:
			missing = append(missing, d)
		}
	}
	create = append(install, create...)
	made, refused, collWhy := c.probeCollations(ctx, conn, makeColl)
	create = append(create, made...)
	// What only plain indexes need is not a blocker: move-org makes the tables without those
	// indexes, and they answer the same, slower.
	split := func(ds []*dependency) (needed, indexOnly []*dependency) {
		for _, d := range ds {
			if plain[d.ext] {
				indexOnly = append(indexOnly, d)
			} else {
				needed = append(needed, d)
			}
		}
		return needed, indexOnly
	}
	unavailable, skipU := split(unavailable)
	denied, skipD := split(denied)
	if len(unavailable) > 0 {
		c.add(Blocker, "customer_extension_unavailable", db, "the organization's Postgres does not have extensions the LokaDB tables use, or not the cloud's version of them: install them on that server (the packages), or keep these tables in the cloud. What to run: "+extensionAdvice(needs, unavailable, c.orgLabel()), describeDeps(unavailable))
	}
	if len(denied) > 0 {
		c.add(Blocker, "customer_extension_privilege", db, fmt.Sprintf("extensions the LokaDB tables use are not installed in the organization's database, and its role cannot install them (tried and rolled back: %s). Install them there with a role that can. What to run: %s", why, extensionAdvice(needs, denied, c.orgLabel())), describeDeps(denied))
	}
	if len(versionDiffers) > 0 {
		c.add(Warning, "customer_extension_version", db, "the organization's database has extensions the LokaDB tables use in another version than the cloud's: move-org leaves them as they are (it installs only what is missing). A type, operator or index option of the cloud's version that the organization's lacks fails at the move or later", versionDiffers)
	}
	if skip := append(skipU, skipD...); len(skip) > 0 {
		c.add(Warning, "customer_index_skipped", db, "only plain indexes use these extensions, and the organization's database does not have them or its role cannot install them: move-org makes the tables there without those indexes. The organization's queries that would use them get slower, the results stay the same. A later migration that creates an index of this kind fails until the extension is there (the proxy prepares DDL in every organization's database before COMMIT). Install the extension there to keep them", describeDeps(skip))
	}
	if len(missing) > 0 {
		c.add(Unsupported, "customer_object_missing", db, "the organization's database lacks types or operator classes the LokaDB tables use, and move-org creates only enums, domains, composite types and functions so far. Until it does: create them there (same schema and name)", describeDeps(missing))
	}
	// functions of the application that move-org will not or will otherwise create (A2)
	var untrusted, definers []*dependency
	for _, d := range create {
		switch {
		case d.kind != "function" || d.ext != "":
		case d.untrusted != "":
			untrusted = append(untrusted, d)
		case d.secdef:
			definers = append(definers, d)
		}
	}
	if len(untrusted) > 0 {
		c.add(Blocker, "customer_function_untrusted", db, "functions the LokaDB tables use are written in a language that is not trusted (C, internal, plpython3u…) and are missing in the organization's database: move-org does not create them there, as they would run code on that server. Create them there yourself, or keep the tables in the cloud", describeDeps(untrusted))
	}
	if len(definers) > 0 {
		c.add(Change, "customer_function_security_definer", db, "SECURITY DEFINER functions the LokaDB tables use: move-org creates them in the organization's database owned by the application's role (-customer-app), not by the administrator, so they run with that role's rights there, not with the rights of the cloud's owner", describeDeps(definers))
	}
	if len(create) > 0 {
		c.add(Info, "customer_creates", db, "move-org will create these in the organization's database: they are missing there", describeDeps(create))
	}
	if len(collations) > 0 {
		c.add(Blocker, "customer_collation_missing", db, "the organization's database has no collations the LokaDB tables use, which its Postgres makes itself (initdb, pg_catalog): that server needs the same collation provider (ICU, or the operating system's locales)", describeDeps(collations))
	}
	if len(refused) > 0 {
		c.add(Blocker, "customer_collation_unavailable", db, fmt.Sprintf("move-org creates the application's collations in the organization's database, and its Postgres refuses these (tried and rolled back: %s): that server needs the same collation provider and locale (ICU, or the operating system's locales)", collWhy), describeDeps(refused))
	}
}

// probeCollations creates the application's collations in one transaction that is rolled
// back, as move-org would (admin.CollationDDL, in the schema move-org puts them in), each
// under a savepoint. why: the first refusal.
func (c *checker) probeCollations(ctx context.Context, conn *pgx.Conn, ds []*dependency) (ok, refused []*dependency, why string) {
	if len(ds) == 0 {
		return nil, nil, ""
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, ds, err.Error()
	}
	defer tx.Rollback(context.Background())
	for _, d := range ds {
		sch := d.schema
		if sch == "public" && c.custSchema != "" {
			sch = c.custSchema // as move-org: what the cloud has in public goes to the tables' schema
		}
		ddl, err := admin.CollationDDL(ctx, c.tx, admin.Dependency{Kind: d.kind, Schema: sch, Name: d.name, OID: d.oid})
		if err != nil {
			return ok, append(refused, ds[len(ok)+len(refused):]...), err.Error()
		}
		steps := []string{"SAVEPOINT lokadb_probe"}
		if sch != "public" {
			steps = append(steps, "CREATE SCHEMA IF NOT EXISTS "+q(sch))
		}
		var failed error
		for _, s := range append(steps, ddl) {
			if _, failed = tx.Exec(ctx, s); failed != nil {
				break
			}
		}
		if failed == nil {
			ok = append(ok, d)
			continue
		}
		if why == "" {
			why = d.name + ": " + firstLine(failed.Error())
		}
		refused = append(refused, d)
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_probe"); err != nil {
			return ok, append(refused, ds[len(ok)+len(refused):]...), why
		}
	}
	return ok, refused, why
}

// extensionAdvice: for the extensions of ds, the commands for the organization's administrator.
func extensionAdvice(needs map[string]*admin.ExtensionNeed, ds []*dependency, org string) string {
	var out []string
	seen := map[string]bool{}
	for _, d := range ds {
		if n := needs[d.ext]; n != nil && !seen[n.Name] {
			seen[n.Name] = true
			out = append(out, n.Message(org))
		}
	}
	return strings.Join(out, "; ")
}

// customerApp checks the role the cloud keeps for the organization's database
// (move-org -customer-app): it answers, and the admin role can give it the tables it creates
// (table: the name the probe uses; canCreate: the admin role can create it at all).
func (c *checker) customerApp(ctx context.Context, conn *pgx.Conn, admRole, table string, canCreate bool) {
	if c.o.CustomerAppDSN == "" {
		c.add(Info, "customer_app_missing", "", "no -customer-app: move-org without it leaves the cloud the credentials of the organization's admin role (postgres_fdw, DDL replay). Give both move-org and the check -customer-app with an unprivileged role", nil)
		return
	}
	app, err := pgx.Connect(ctx, c.o.CustomerAppDSN)
	if err != nil {
		c.add(Blocker, "customer_app_unreachable", "", "the role of -customer-app cannot connect to the organization's database (the cloud reads and writes the moved tables with it): "+firstLine(err.Error()), nil)
		return
	}
	defer app.Close(ctx)
	r, err := rowsOf(ctx, app, `SELECT current_user::text, current_database()::text, (SELECT rolsuper FROM pg_roles WHERE rolname = current_user)::text`)
	if err != nil {
		c.add(Blocker, "customer_app_unreachable", "", "the role of -customer-app connects but cannot run a query: "+firstLine(err.Error()), nil)
		return
	}
	role, appDB, super := r[0][0], r[0][1], r[0][2] == "true"
	var adb [][]string
	if adb, err = rowsOf(ctx, conn, `SELECT current_database()::text`); err == nil && adb[0][0] != appDB {
		c.add(Blocker, "customer_app_database", role, fmt.Sprintf("-customer-app connects to database %s, -customer to %s: they must be the same database", appDB, adb[0][0]), nil)
	}
	switch {
	case super:
		c.add(Warning, "customer_app_superuser", role, fmt.Sprintf("the role of -customer-app (%s) is a superuser: the cloud would keep a superuser's password of the organization's server. It needs no privileges beyond owning the moved tables", role), nil)
	case role == admRole:
		c.add(Info, "customer_app_is_admin", role, fmt.Sprintf("-customer-app is the admin role %s itself: the cloud keeps its credentials", role), nil)
	}
	if !canCreate || role == admRole {
		return
	}
	if err := admin.Probe(ctx, conn, "CREATE TABLE "+table+" (id bigint PRIMARY KEY)", fmt.Sprintf("ALTER TABLE %s OWNER TO %s", table, q(role))); err != nil {
		c.add(Blocker, "customer_app_owner", role, fmt.Sprintf("role %s cannot make %s the owner of the tables it creates (tried and rolled back: %s). move-org gives the moved tables to it: %s needs to be able to SET ROLE %s (GRANT %s TO %s), and %s needs CREATE on schema public (GRANT CREATE ON SCHEMA public TO %s)",
			admRole, role, firstLine(err.Error()), admRole, role, q(role), q(admRole), role, q(role)), nil)
	}
}

// customerReady checks the database the organization prepared for its data (the database
// is the customer's: lokadb connects to it and creates only its own objects there).
func (c *checker) customerReady() {
	ctx, cancel := context.WithTimeout(c.ctx, 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, c.o.CustomerDSN)
	if err != nil {
		c.add(Blocker, "customer_unreachable", "", "the organization's database does not answer: "+firstLine(err.Error()), nil)
		return
	}
	defer conn.Close(ctx)
	r, err := rowsOf(ctx, conn, `SELECT current_setting('server_version_num'), current_setting('server_version'),
		current_database()::text, current_user::text`)
	if err != nil {
		c.add(Blocker, "customer_unreachable", "", "the organization's database answered but cannot be inspected: "+firstLine(err.Error()), nil)
		return
	}
	v := r[0]
	num, _ := strconv.Atoi(v[0])
	db, role := v[2], v[3]
	c.add(Info, "customer_connected", db, fmt.Sprintf("the organization's database answers: PostgreSQL %s, database %s, role %s", v[1], db, role), nil)
	if num/10000 < c.cloudVersion/10000 {
		// move-org rebuilds the tables there from the cloud's definitions (pg_get_constraintdef,
		// pg_get_indexdef): syntax a later version has (UNIQUE NULLS NOT DISTINCT, 15) fails on
		// an earlier one in the middle of the move (review 2, R13)
		c.add(Warning, "customer_version_older", db, fmt.Sprintf("the organization's database runs PostgreSQL %d, older than the cloud's %d: move-org rebuilds the LokaDB tables there from the cloud's definitions, and what %d has and %d has not (UNIQUE NULLS NOT DISTINCT before 15, some index and constraint options) fails in the middle of the move. Give the organization PostgreSQL %d or later, or check the tables' DDL on %d first",
			num/10000, c.cloudVersion/10000, c.cloudVersion/10000, num/10000, c.cloudVersion/10000, num/10000), nil)
	} else if num/10000 != c.cloudVersion/10000 {
		c.add(Info, "customer_version", db, fmt.Sprintf("the organization's database runs PostgreSQL %d, the cloud %d: logical replication works across major versions, but this setup is tested with equal versions only", num/10000, c.cloudVersion/10000), nil)
	}
	// Where move-org puts the tables, decided as move-org decides it (probes, rolled back):
	// public if it can take them, else schema lokadb.
	var names, keys []string
	for _, t := range c.rep.Tables {
		keys = append(keys, t.Name)
		if s, _ := admin.SplitKey(t.Name); s != "public" {
			continue // to the same schema there: below
		}
		names = append(names, t.Name)
		if c.o.Org > 0 {
			names = append(names, admin.Helper(t.Name, fmt.Sprintf("__o%d", c.o.Org)))
		}
	}
	// A table of another schema goes to that schema at the organization, whatever is chosen
	// for public: a table of the organization's own there with its name is a conflict.
	if clash, err := admin.OtherSchemaClashes(ctx, conn, keys, c.o.Org); err != nil {
		c.add(Blocker, "customer_unreachable", "", "the organization's database answered but cannot be inspected: "+firstLine(err.Error()), nil)
		return
	} else {
		for _, r := range clash {
			c.add(Blocker, "customer_table_exists", role, "the organization's database cannot take the tables: "+r.String()+
				": move-org puts a table of schema "+r.Schema+" in the same schema there and leaves that one alone. Rename it, or use another database", nil)
		}
		seen := map[string]bool{}
		for _, k := range keys {
			if s, _ := admin.SplitKey(k); s != "public" && !seen[s] {
				seen[s] = true
				c.add(Info, "customer_schema", s, "move-org puts the organization's tables of schema "+s+" in schema "+s+" (it creates the schema if missing)", nil)
			}
		}
	}
	appRole := ""
	if c.o.CustomerAppDSN != "" {
		if cfg, err := pgconn.ParseConfig(c.o.CustomerAppDSN); err == nil {
			appRole = cfg.User
		}
	}
	choice, err := admin.ChooseCustomerSchema(ctx, conn, c.o.CustomerSchema, appRole, names, c.o.Org)
	if err != nil {
		c.add(Blocker, "customer_unreachable", "", "the organization's database answered but cannot be inspected: "+firstLine(err.Error()), nil)
		return
	}
	c.custSchema = choice.Schema
	if choice.Schema == "" {
		code := "customer_privileges"
		clashes, owners := 0, 0
		for _, r := range choice.Refused {
			switch r.Step {
			case "clash":
				clashes++
			case "owner":
				owners++
			}
		}
		hint := ""
		switch {
		case clashes == len(choice.Refused):
			code = "customer_table_exists"
			hint = ". Rename those tables, or give move-org another schema (-customer-schema)"
		case owners > 0:
			code = "customer_app_owner"
			hint = fmt.Sprintf(". move-org gives the moved tables to %s: %s needs to be able to SET ROLE %s (GRANT %s TO %s), and %s needs CREATE on the schema (or move-org creates schema %s for it: CREATE on the database)",
				appRole, role, appRole, q(appRole), q(role), appRole, admin.DefaultCustomerSchema)
		default:
			hint = fmt.Sprintf(". Grant CREATE on schema public, or CREATE on the database (move-org then creates schema %s)", admin.DefaultCustomerSchema)
			if clashes > 0 {
				hint += "; a schema with a table of the customer's of one of those names (above) takes none: rename that table, or give move-org another schema (-customer-schema)"
			}
		}
		c.add(Blocker, code, role, fmt.Sprintf("role %s cannot create the organization's tables in any schema: %s%s", role, choice.Refusals(), hint), nil)
	} else {
		msg := "move-org puts the organization's tables in schema " + choice.Schema
		if choice.Create {
			msg += " (it creates the schema"
			if appRole != "" && appRole != role {
				msg += ", owned by " + appRole
			}
			msg += ")"
		}
		if choice.Schema != "public" {
			msg += ", not public: " + choice.Refusals() + ". The move then goes by triggers (logical replication writes to the cloud's schema name)"
			if g := choice.GrantFor(); g != "" { // block 7
				msg += ". A logical move is possible after the organization runs " + g + "; otherwise -mode auto moves by triggers"
			}
		}
		c.add(Info, "customer_schema", choice.Schema, msg, nil)
	}

	// The same rule as move-org: -mode auto takes triggers; logical replication only by -mode logical
	// -allow-customer-replication (its role reads the change stream of the whole cloud database).
	canSub, why, err := admin.CanSubscribe(ctx, conn)
	if err != nil {
		c.add(Blocker, "customer_unreachable", "", "the organization's database answered but cannot be inspected: "+firstLine(err.Error()), nil)
		return
	}
	if choice.Schema != "" && choice.Schema != "public" {
		canSub, why = false, "the organization's tables go to schema "+choice.Schema
	}
	mode, _ := admin.MoveMode(admin.ModeAuto, c.cloudLogical, canSub)
	how := map[string]string{
		admin.ModeLogical: "logical replication (a subscription in the organization's database)",
		admin.ModeTrigger: "triggers (change capture in the cloud; no subscription, no replication there)"}[mode]
	if c.cloudLogical && !canSub {
		how += "; no subscription: " + firstLine(why)
	}
	if c.cloudLogical && canSub {
		how += "; a subscription is possible too (-mode logical), but its role reads the change stream of the whole cloud database for the time of the move, so move-org asks for -allow-customer-replication"
	}
	c.add(Info, "move_mode", db, "move-org -mode auto will move with "+how, nil)
	if mode == admin.ModeLogical {
		if err := admin.Probe(ctx, conn, "CREATE SCHEMA IF NOT EXISTS lokadb", "CREATE TABLE lokadb."+admin.ProbeName("table")+" (id bigint)"); err != nil {
			c.add(Blocker, "customer_privileges", role, fmt.Sprintf("role %s cannot create schema lokadb and its tables (%s): a logical move keeps its switch marker there", role, firstLine(err.Error())), nil)
		}
	}
	c.customerApp(ctx, conn, role, "", false) // the transfer of the tables was tried with the schema

	c.customerDeps(ctx, conn, db)

	// Tables an earlier move-org left in that schema: recreated.
	var ex [][]string
	if choice.Schema != "" {
		if ex, err = rowsOf(ctx, conn, `SELECT c.relname::text FROM pg_class c WHERE c.relnamespace = to_regnamespace($2)
			AND c.relname::text = ANY($1) AND coalesce(obj_description(c.oid, 'pg_class'), '') LIKE $3 ORDER BY 1`, names, q(choice.Schema), admin.TableMark+"%"); err != nil {
			c.add(Warning, "customer_tables_unknown", "", "could not list the tables of the organization's database: "+firstLine(err.Error()), nil)
			return
		}
	}
	for _, t := range ex {
		c.add(Info, "customer_previous_attempt", t[0], fmt.Sprintf("%s was left there by an earlier move-org: it will be recreated", t[0]), nil)
	}
	if choice.Schema != "public" {
		ex = nil // the count below is of public
	}
	other, err := rowsOf(ctx, conn, `SELECT count(*)::text FROM pg_class
		WHERE relnamespace = 'public'::regnamespace AND relkind IN ('r', 'p', 'f') AND NOT relispartition`)
	if err == nil {
		if n, _ := strconv.Atoi(other[0][0]); n-len(ex) > 0 {
			c.add(Info, "customer_contents", db, fmt.Sprintf("the database already holds %s of its own in schema public: move-org adds only the LokaDB tables (and schema lokadb), and leaves them alone",
				plural(n-len(ex), "table", "tables")), nil)
		}
	}
}

// orgLabel: the organization's number for the administrator's hints, "" when check is run
// without one.
func (c *checker) orgLabel() string {
	if c.o.Org > 0 {
		return fmt.Sprint(c.o.Org)
	}
	return ""
}
