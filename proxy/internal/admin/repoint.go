package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// RepointOptions configures Repoint.
type RepointOptions struct {
	CloudDSN string // admin DSN of the cloud database
	// CustomerAppDSN: the organization's database at its new address, as the cloud's role
	// there (the -customer-app of move-org): postgres_fdw connects with it from now on.
	CustomerAppDSN string
	// CustomerSchema: the schema of the organization's tables there ("": unchanged).
	CustomerSchema string
	Org            int64
	LockTimeout    time.Duration
	Attempts       int
	Log            *slog.Logger
	// The organization's other addresses (review A1, repoint_addrs.go), from the proxy's
	// config: its customers_app and customers entries ("" none), its control_plane and keys.
	// DirectDSN, OwnerDSN: the credentials to store in the control plane instead of the old
	// ones moved to the new address.
	ConfigDirect, ConfigOwner         string
	ControlPlane, KeyFile, KeyFileOld string
	DirectDSN, OwnerDSN               string
}

// foreignOf is one foreign table of an organization's server: the cloud's name, where it
// reads in the organization's database, and its columns (name, type).
type foreignOf struct {
	name, schema, table string
	cols                [][]string
}

// Repoint points an organization's server at another database holding the same rows: the
// customer restored its database elsewhere, failed over to a replica, changed the role's
// password. No row is copied. The new database is checked first: every foreign table must
// find its table there, with its columns and types, made for this organization. The
// organization pauses (org_lock) while the server changes, so no statement of it is half
// on the old database; postgres_fdw drops its cached connections when a server or user
// mapping changes.
func Repoint(ctx context.Context, o RepointOptions) error {
	if err := ValidOrg(o.Org); err != nil {
		return fmt.Errorf("repoint: %w", err)
	}
	log := o.Log.With("org", o.Org)
	app, err := pgconn.ParseConfig(o.CustomerAppDSN)
	if err != nil {
		return fmt.Errorf("customer app DSN: %w", err)
	}
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer cloud.Close(context.Background())
	var mine bool
	if err := cloud.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", moveLockKey).Scan(&mine); err != nil {
		return err
	}
	if !mine {
		return errors.New("a move-org or move-back is running against this cloud database")
	}
	server, schema, err := orgServer(ctx, cloud, o.Org)
	if err != nil {
		return err
	}
	if o.CustomerSchema == "" {
		o.CustomerSchema = schema
	}
	fts, err := foreignTables(ctx, cloud, server)
	if err != nil {
		return err
	}

	cust, err := connect(ctx, o.CustomerAppDSN)
	if err != nil {
		return fmt.Errorf("new customer database: %w", err)
	}
	defer cust.Close(context.Background())
	// a table of a schema other than public stays in its own schema (custSchemaOf)
	target := func(ft foreignOf) string {
		if ft.schema != schema {
			return ft.schema
		}
		return o.CustomerSchema
	}
	for _, ft := range fts {
		n, err := checkTarget(ctx, cust, ft, target(ft), o.Org)
		if err != nil {
			return fmt.Errorf("new customer database: %w", err)
		}
		log.Info("table found in the new database", "table", target(ft)+"."+ft.table, "rows", n)
	}

	// The proxy's other ways to the organization's database must reach the new one too.
	cloudCfg, err := pgconn.ParseConfig(o.CloudDSN)
	if err != nil {
		return err
	}
	same, err := databaseMark(ctx, cust)
	if err != nil {
		return err
	}
	inst := ""
	if o.ControlPlane != "" { // the credentials there are sealed for the cloud's installation (A14)
		if inst, err = CloudInstallation(ctx, o.CloudDSN); err != nil {
			return err
		}
	}
	addrs, cp, key, err := otherAddresses(ctx, o, inst, cloudCfg.Database, app)
	if cp != nil {
		defer cp.Close(context.Background())
	}
	if err != nil {
		return err
	}
	if err := checkOthers(ctx, addrs, same, o.Org); err != nil {
		return err
	}
	for _, a := range addrs {
		log.Info("the proxy's other address reaches the new database", "kind", a.kind, "source", a.source, "to", describeDSN(a.dsn))
	}
	// A new database id in the mark on the new database's tables (dbid.go): the old database
	// keeps the old one, and the proxy's check of the database refuses it from now on. Marked
	// before the pause; lokadb.orgs takes the id in the switch.
	if err := execAll(ctx, cloud, []string{dbIDSQL}); err != nil {
		return err
	}
	dbID := newDBID()
	for _, ft := range fts {
		if _, err := cust.Exec(ctx, fmt.Sprintf("COMMENT ON TABLE %s.%s IS %s", q(target(ft)), q(ft.table), lit(orgMark(o.Org, dbID)))); err != nil {
			log.Warn("the new database's tables cannot be marked with its id (the role does not own them): the proxy's check of the database cannot tell the old one from it",
				"table", target(ft)+"."+ft.table, "err", err)
			dbID = ""
			break
		}
	}

	// The TLS settings of the new DSN on the server (review 3, R3-5, fdwssl.go): set, added, and
	// those it does not set dropped, or the old database's sslmode or certificate would stay.
	ssl, err := FDWSSLOptions(o.CustomerAppDSN)
	if err != nil {
		return fmt.Errorf("-customer-app: %w", err)
	}
	had, err := rowsOf(ctx, cloud, `SELECT split_part(o, '=', 1) FROM pg_foreign_server, unnest(srvoptions) o WHERE srvname = $1`, server)
	if err != nil {
		return err
	}
	opts := []string{"SET host " + lit(app.Host), "SET port " + lit(fmt.Sprint(app.Port)), "SET dbname " + lit(app.Database)}
	set := map[string]bool{}
	for _, s := range ssl {
		k := strings.SplitN(s, " ", 2)[0]
		set[k] = true
		verb := "ADD "
		for _, h := range had {
			if h[0] == k {
				verb = "SET "
			}
		}
		opts = append(opts, verb+s)
	}
	for _, h := range had {
		if slices.Contains(fdwSSLKeys, h[0]) && !set[h[0]] {
			opts = append(opts, "DROP "+h[0])
		}
	}
	var steps []string
	steps = append(steps, fmt.Sprintf("ALTER SERVER %s OPTIONS (%s)", q(server), strings.Join(opts, ", ")))
	users, err := rowsOf(ctx, cloud, `SELECT CASE WHEN umuser = 0 THEN 'PUBLIC' ELSE quote_ident(usename::text) END
		FROM pg_user_mappings WHERE srvname = $1 ORDER BY 1`, server)
	if err != nil {
		return err
	}
	for _, u := range users {
		steps = append(steps, fmt.Sprintf("ALTER USER MAPPING FOR %s SERVER %s OPTIONS (SET user %s, SET password %s)",
			u[0], q(server), lit(app.User), lit(app.Password)))
	}
	if o.CustomerSchema != schema {
		for _, ft := range fts {
			if ft.schema == schema {
				steps = append(steps, fmt.Sprintf("ALTER FOREIGN TABLE %s OPTIONS (SET schema_name %s)", qr(ft.name), lit(o.CustomerSchema)))
			}
		}
		steps = append(steps, fmt.Sprintf("UPDATE lokadb.orgs SET schema = %s WHERE org = %d", lit(o.CustomerSchema), o.Org))
	}

	mo := MoveOptions{Org: o.Org, LockTimeout: o.LockTimeout, Attempts: o.Attempts, Log: o.Log}
	unpause, err := pauseOrg(ctx, cloud, mo, log)
	if err != nil {
		return err
	}
	start := time.Now()
	// the control plane first: the proxies read it again when they see the notification of
	// the cloud's transaction (credentials.go), and none of the organization's statements runs
	// meanwhile (the pause)
	undo := func() {}
	if cp != nil {
		if undo, err = storeOthers(ctx, cp, key, inst, cloudCfg.Database, o.Org, addrs); err != nil {
			unpause()
			return err
		}
	}
	steps = append(steps, fmt.Sprintf("UPDATE lokadb.orgs SET db_id = %s WHERE org = %d", nullLit(dbID), o.Org), metaChanged)
	err = pgx.BeginFunc(ctx, cloud, func(tx pgx.Tx) error {
		for _, s := range steps {
			if _, err := tx.Exec(ctx, s); err != nil {
				return fmt.Errorf("%w\n  in: %s", err, mask(s))
			}
		}
		return nil
	})
	if err != nil && cp != nil && repointCommitted(o.CloudDSN, o.Org, dbID, server, app, log) {
		// the error came after the COMMIT reached the cloud (review3 R3-10: the connection
		// broke on it): the server names the new database, the control plane keeps the new
		// credentials with it
		log.Warn("repoint's commit reported an error, but the cloud has the new address: the control plane keeps the new credentials", "err", err)
		err = nil
	}
	if err != nil && cp != nil {
		// the old credentials back, and the proxies told before the pause ends: one whose
		// ticker read the new ones meanwhile would send the direct path to the new database
		// while the cloud's server stays on the old one (review of block 10)
		undo()
		if _, nerr := cloud.Exec(ctx, metaChanged); nerr != nil {
			log.Warn("the proxies were not notified of the credentials put back: they read them within 30 s", "err", nerr)
		}
	}
	unpause()
	if err != nil {
		return err
	}
	log.Info("server repointed", "server", server, "host", app.Host, "db", app.Database, "paused", time.Since(start).Round(time.Millisecond))

	// Through postgres_fdw now: the cloud reads the new database.
	for _, ft := range fts {
		r, err := rowsOf(ctx, cloud, fmt.Sprintf("SELECT count(*)::text FROM %s", qr(ft.name)))
		if err != nil {
			return fmt.Errorf("read %s through the repointed server: %w", ft.name, err)
		}
		log.Info("read through the cloud", "table", ft.name, "rows", r[0][0])
	}
	return nil
}

// repointCommitted: the switch's transaction is in the cloud, read on a new connection: the
// database id it wrote (dbID, new and random), or without one the server's address.
func repointCommitted(cloudDSN string, org int64, dbID, server string, app *pgconn.Config, log *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := ownsql.Connect(ctx, cloudDSN)
	if err != nil {
		log.Warn("whether repoint's transaction committed is not known (the cloud does not answer): the control plane's credentials are put back; check the server's address and run repoint again", "err", err)
		return false
	}
	defer c.Close(context.Background())
	var done bool
	if dbID != "" {
		err = c.QueryRow(ctx, `SELECT coalesce((SELECT to_jsonb(o) ->> 'db_id' FROM lokadb.orgs o WHERE org = $1) = $2, false)`, org, dbID).Scan(&done)
	} else {
		err = c.QueryRow(ctx, `SELECT coalesce(srvoptions @> ARRAY['host=' || $2, 'port=' || $3, 'dbname=' || $4], false) FROM pg_foreign_server WHERE srvname = $1`,
			server, app.Host, fmt.Sprint(app.Port), app.Database).Scan(&done)
	}
	if err != nil {
		log.Warn("whether repoint's transaction committed is not known: the control plane's credentials are put back; check the server's address and run repoint again", "err", err)
		return false
	}
	return done
}

// orgServer: the server and schema lokadb.orgs records for a moved organization.
func orgServer(ctx context.Context, cloud querier, org int64) (server, schema string, err error) {
	r, err := rowsOf(ctx, cloud, `SELECT (to_regclass('lokadb.orgs') IS NOT NULL)::text`)
	if err != nil {
		return "", "", err
	}
	if r[0][0] != "true" {
		return "", "", errors.New("the cloud database is not enabled (no lokadb.orgs)")
	}
	r, err = rowsOf(ctx, cloud, `SELECT server, schema FROM lokadb.orgs WHERE org = $1`, org)
	if err != nil {
		return "", "", err
	}
	if len(r) == 0 {
		return "", "", fmt.Errorf("organization %d is not in its own database (not in lokadb.orgs)", org)
	}
	return r[0][0], r[0][1], nil
}

// foreignTables lists the foreign tables of a server, attached or not.
func foreignTables(ctx context.Context, cloud querier, server string) ([]foreignOf, error) {
	rows, err := rowsOf(ctx, cloud, `SELECT lokadb.key_of(c.oid),
			coalesce((SELECT split_part(o, '=', 2) FROM unnest(f.ftoptions) o WHERE o LIKE 'schema\_name=%'), 'public'),
			coalesce((SELECT substr(o, 12) FROM unnest(f.ftoptions) o WHERE o LIKE 'table\_name=%'), c.relname::text)
		FROM pg_foreign_table f JOIN pg_class c ON c.oid = f.ftrelid JOIN pg_foreign_server s ON s.oid = f.ftserver
		WHERE s.srvname = $1 ORDER BY c.relname`, server)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("server %s has no foreign tables", server)
	}
	var out []foreignOf
	for _, r := range rows {
		cols, err := rowsOf(ctx, cloud, `SELECT attname::text, format_type(atttypid, atttypmod) FROM pg_attribute
			WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum`, qr(r[0]))
		if err != nil {
			return nil, err
		}
		out = append(out, foreignOf{name: r[0], schema: r[1], table: r[2], cols: cols})
	}
	return out, nil
}

// checkTarget makes sure the new database has what the foreign table reads: the table, every
// column with the same type, made for this organization (move-org's comment, if kept), and
// readable by the role. Returns its row count.
func checkTarget(ctx context.Context, cust querier, ft foreignOf, schema string, org int64) (string, error) {
	have, err := rowsOf(ctx, cust, `SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a WHERE a.attrelid = to_regclass(quote_ident($1) || '.' || quote_ident($2))
		AND a.attnum > 0 AND NOT a.attisdropped`, schema, ft.table)
	if err != nil {
		return "", err
	}
	if len(have) == 0 {
		return "", fmt.Errorf("no table %s.%s", schema, ft.table)
	}
	types := map[string]string{}
	for _, c := range have {
		types[c[0]] = c[1]
	}
	var bad []string
	for _, c := range ft.cols {
		switch t, ok := types[c[0]]; {
		case !ok:
			bad = append(bad, c[0]+" missing")
		case t != c[1]:
			bad = append(bad, fmt.Sprintf("%s is %s, the cloud has %s", c[0], t, c[1]))
		}
	}
	if len(bad) > 0 {
		return "", fmt.Errorf("%s.%s: %s", schema, ft.table, strings.Join(bad, "; "))
	}
	mark, err := rowsOf(ctx, cust, `SELECT coalesce(obj_description(to_regclass(quote_ident($1) || '.' || quote_ident($2)), 'pg_class'), '')`, schema, ft.table)
	if err != nil {
		return "", err
	}
	if m := mark[0][0]; strings.HasPrefix(m, TableMark) && !isOrgMark(m, org) {
		return "", fmt.Errorf("%s.%s was made for another organization (%q)", schema, ft.table, m)
	}
	n, err := rowsOf(ctx, cust, fmt.Sprintf("SELECT count(*)::text FROM %s.%s", q(schema), q(ft.table)))
	if err != nil {
		return "", err
	}
	return n[0][0], nil
}
