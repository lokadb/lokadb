package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/creds"
)

// Routing profiles of a moved organization (tasks/routing-declarations.md, section 7). The
// storage is the same in all of them; the profile only says which way the proxy sends the
// organization's statements, so org-profile changes it without a new move.
const (
	ProfileFDW       = "fdw"        // every statement goes to the cloud, which reaches the organization by postgres_fdw
	ProfileFDWDirect = "fdw+direct" // statements of this organization's LokaDB tables only go straight to its database
	ProfileDirect    = "direct"     // no FDW: LokaDB statements go to the organization's database, cloud ones to the cloud
)

// Profiles: every profile, in the order of the docs.
var Profiles = []string{ProfileFDW, ProfileFDWDirect, ProfileDirect}

// ProfileServed: the profiles this proxy routes. The others can be estimated (check
// -profile) but not set: the proxy would route them as fdw, and the organization would
// believe it is on a path it is not.
var ProfileServed = map[string]bool{ProfileFDW: true, ProfileFDWDirect: true, ProfileDirect: true}

// ValidProfile reports whether p names a profile.
func ValidProfile(p string) bool {
	for _, x := range Profiles {
		if p == x {
			return true
		}
	}
	return false
}

// profileSQL: lokadb.orgs.profile, NULL = the proxy's default (fdw). A database moved by an
// older version has no column; org-profile adds it, readers check that it exists.
// The direct path's gate reads the column on the client's connection under the client's
// role: enable grants SELECT on lokadb.orgs to PUBLIC (block 9, B9.12), so no SECURITY DEFINER
// function is needed; lokadb.org_profile of an earlier 8a build is dropped.
// lokadb.direct_cloud_first: first statements whose transactions go the cloud's way (8a.2:
// the proxy writes one down when a direct transaction meets a statement it cannot take);
// PUBLIC reads and inserts (the proxy runs under the client's role), never updates or deletes.
// lokadb.direct_learnt: transaction forms learnt for the direct path ahead (8a.2, the second
// step: lokadb check -profile fdw+direct -direct-forms writes them for psql); PUBLIC reads, the
// owner alone writes (a form here goes direct without the proxy's two runs through the cloud).
const profileSQL = `ALTER TABLE lokadb.orgs ADD COLUMN IF NOT EXISTS profile text
  CHECK (profile IN ('fdw', 'fdw+direct', 'direct'));
DROP FUNCTION IF EXISTS lokadb.org_profile(bigint);
CREATE TABLE IF NOT EXISTS lokadb.direct_cloud_first (
  fingerprint text PRIMARY KEY CHECK (length(fingerprint) BETWEEN 1 AND 64),
  reason text NOT NULL CHECK (length(reason) <= 200),
  at timestamptz NOT NULL DEFAULT now());
REVOKE ALL ON lokadb.direct_cloud_first FROM PUBLIC;
GRANT SELECT, INSERT ON lokadb.direct_cloud_first TO PUBLIC;
CREATE TABLE IF NOT EXISTS lokadb.direct_learnt (
  fingerprint text NOT NULL CHECK (length(fingerprint) BETWEEN 1 AND 64),
  named boolean NOT NULL,
  at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (fingerprint, named));
REVOKE ALL ON lokadb.direct_learnt FROM PUBLIC;
GRANT SELECT ON lokadb.direct_learnt TO PUBLIC`

// OrgProfileOptions: lokadb org-profile.
type OrgProfileOptions struct {
	CloudDSN    string
	Org         int64
	Profile     string
	LockTimeout time.Duration // each wait for the pause, as in move-org
	Log         *slog.Logger
	// CustomerAppDSN, DirectRole (fdw+direct): the owner of the moved tables in the
	// organization's database (move-org's -customer-app) grants DirectRole DML on them, and on
	// the tables it creates there later (DDL replay). DirectRole is the role the proxy's direct
	// path runs the application's statements under (customers_app in its config): a role of its
	// own, made by the organization's administrator (CREATE ROLE … LOGIN), owning nothing.
	CustomerAppDSN string
	DirectRole     string
	// ControlPlane, KeyFile (direct, and leaving it): the organization's credentials the proxy
	// keeps (lokadb org-credentials, stage 9a): direct needs both kinds there and checks they
	// connect; leaving direct when its user mapping was dropped makes it again from the owner's.
	// DropFDWCredentials (direct): the cloud's user mappings of the organization's server go:
	// neither the cloud database nor its roles reach the organization's database any more.
	ControlPlane       string
	KeyFile            string
	KeyFileOld         string // the old key during a rotation (credentials_key_old, review of admin A8)
	DropFDWCredentials bool
}

// SetOrgProfile switches a moved organization to another routing profile under its pause
// (org_lock, the one move-org takes): the organization's open transactions finish on the
// old path, new ones wait and then go by the new one. Returns the previous profile.
func SetOrgProfile(ctx context.Context, o OrgProfileOptions) (string, error) {
	if err := ValidOrg(o.Org); err != nil {
		return "", fmt.Errorf("org-profile: %w", err)
	}
	if !ValidProfile(o.Profile) {
		return "", fmt.Errorf("unknown profile %q: one of %v", o.Profile, Profiles)
	}
	if !ProfileServed[o.Profile] {
		return "", fmt.Errorf("profile %q is not routed by this version of lokadb yet: estimate it with lokadb check -profile %s -org %d", o.Profile, o.Profile, o.Org)
	}
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	conn, err := connectPlain(ctx, o.CloudDSN)
	if err != nil {
		return "", err
	}
	defer conn.Close(context.Background())
	r, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.orgs') IS NOT NULL AND to_regprocedure('lokadb.org_lock(bigint)') IS NOT NULL)::text`)
	if err != nil {
		return "", err
	}
	if r[0][0] != "true" {
		return "", fmt.Errorf("this database has no lokadb metadata: run lokadb enable and move-org first")
	}
	if _, err := conn.Exec(ctx, profileSQL); err != nil {
		return "", fmt.Errorf("lokadb.orgs.profile: %w", err)
	}
	if r, err = rowsOf(ctx, conn, `SELECT coalesce(profile, '') FROM lokadb.orgs WHERE org = $1`, o.Org); err != nil {
		return "", err
	}
	if len(r) == 0 {
		return "", fmt.Errorf("organization %d is not in its own database (lokadb.orgs): a profile only applies to a moved organization", o.Org)
	}
	old := r[0][0]
	if old == "" {
		old = ProfileFDW
	}
	server := ""
	if o.Profile == ProfileDirect || old == ProfileDirect {
		if server, _, err = orgServer(ctx, conn, o.Org); err != nil {
			return "", err
		}
	}
	if o.Profile == ProfileDirect {
		if err := directCredentials(ctx, o); err != nil {
			return "", err
		}
	} else if o.DropFDWCredentials {
		return "", fmt.Errorf("-drop-fdw-credentials goes with profile direct alone: the other profiles reach the organization through the FDW")
	}
	if old == ProfileDirect && o.Profile != ProfileDirect { // the FDW way again: its user mapping first
		if err := restoreMapping(ctx, conn, o, server, log); err != nil {
			return "", err
		}
	}
	if o.Profile == ProfileFDWDirect || o.Profile == ProfileDirect {
		if o.DirectRole == "" {
			log.Warn("no -direct-role: the role in the proxy's customers_app needs DML on the organization's tables already (the proxy checks it, and sends the statements through the cloud without it)")
		} else if err := grantDirectRole(ctx, conn, o, log); err != nil {
			return "", err
		}
	}
	unpause, err := pauseOrg(ctx, conn, MoveOptions{Org: o.Org, LockTimeout: o.LockTimeout, Attempts: 5}, log)
	if err != nil {
		return "", err
	}
	defer unpause()
	// proxies listening on lokadb_meta read the metadata again; the profile there is a hint:
	// the direct path reads it again under the pause lock before each statement
	if _, err := conn.Exec(ctx, `UPDATE lokadb.orgs SET profile = $2 WHERE org = $1`, o.Org, o.Profile); err != nil {
		return "", err
	}
	if _, err := conn.Exec(ctx, `NOTIFY lokadb_meta`); err != nil {
		return "", err
	}
	if o.Profile == ProfileDirect && o.DropFDWCredentials {
		if err := dropMappings(ctx, conn, server, log); err != nil {
			return "", fmt.Errorf("profile direct set; its user mappings: %w", err)
		}
	}
	log.Info("organization profile set", "org", o.Org, "from", old, "to", o.Profile)
	return old, nil
}

// directCredentials: the proxy has both kinds of the organization's credentials (direct: its
// statements, owner: DDL replay), and they connect: on profile direct there is no other way.
func directCredentials(ctx context.Context, o OrgProfileOptions) error {
	if o.ControlPlane == "" || o.KeyFile == "" {
		return fmt.Errorf("profile direct needs -control-plane and -key: the proxy reaches the organization with the credentials stored there alone (lokadb org-credentials)")
	}
	set, err := loadCredentials(ctx, o.CloudDSN, o.ControlPlane, o.KeyFile, o.KeyFileOld)
	if err != nil {
		return err
	}
	db, err := dbName(o.CloudDSN)
	if err != nil {
		return err
	}
	for _, kind := range []string{creds.Direct, creds.Owner} {
		dsn := set.DSN(db, strconv.FormatInt(o.Org, 10), kind)
		if dsn == "" {
			return fmt.Errorf("profile direct: no %s credentials of organization %d in the control plane: lokadb org-credentials -kind %s first", kind, o.Org, kind)
		}
		c, err := connectPlain(ctx, dsn)
		if err != nil {
			return fmt.Errorf("profile direct: the %s credentials of organization %d do not connect", kind, o.Org) // the DSN's password not in the error
		}
		c.Close(context.Background())
	}
	return nil
}

func loadCredentials(ctx context.Context, cloudDSN, controlPlane, keyFile, keyFileOld string) (creds.Set, error) {
	key, err := creds.LoadKey(keyFile)
	if err != nil {
		return nil, err
	}
	keys := [][]byte{key}
	if keyFileOld != "" { // a rotation not resealed yet (review of admin, A8)
		k, err := creds.LoadKey(keyFileOld)
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	cp, err := pgx.Connect(ctx, controlPlane)
	if err != nil {
		return nil, fmt.Errorf("control plane: %w", err)
	}
	defer cp.Close(context.Background())
	inst, err := CloudInstallation(ctx, cloudDSN)
	if err != nil {
		return nil, err
	}
	set, errs, err := creds.Load(ctx, cp, inst, keys...)
	if err != nil {
		return nil, err
	}
	if len(errs) > 0 { // a row the keys do not open: said, not taken for "none there"
		return nil, fmt.Errorf("control plane: %w (-key-old: the key it is sealed with, during a rotation; then lokadb org-credentials -reseal)", errs[0])
	}
	return set, nil
}

func dbName(dsn string) (string, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("cloud DSN: %w", err)
	}
	return cfg.Database, nil
}

// dropMappings removes every user mapping of the organization's FDW server: the cloud keeps no
// password of its database (org-profile direct -drop-fdw-credentials, RD section 7.3).
func dropMappings(ctx context.Context, conn *pgx.Conn, server string, log *slog.Logger) error {
	rows, err := rowsOf(ctx, conn, `SELECT format('DROP USER MAPPING FOR %s SERVER %I', CASE WHEN umuser = 0 THEN 'PUBLIC' ELSE quote_ident(usename) END, srvname)
		FROM pg_user_mappings WHERE srvname = $1`, server)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := conn.Exec(ctx, r[0]); err != nil {
			return fmt.Errorf("%s: %w", r[0], err)
		}
	}
	log.Info("the organization's user mappings dropped: the cloud does not reach its database", "server", server, "mappings", len(rows))
	return nil
}

// restoreMapping: leaving profile direct, the organization's server gets its user mappings
// back when they were dropped: the foreign partitions' owner from the owner's credentials the
// proxy keeps, then lokadb.sync_mappings() copies it to the roles that read the tables.
func restoreMapping(ctx context.Context, conn *pgx.Conn, o OrgProfileOptions, server string, log *slog.Logger) error {
	r, err := rowsOf(ctx, conn, `SELECT DISTINCT quote_ident(pg_get_userbyid(c.relowner)), EXISTS (SELECT FROM pg_user_mappings m WHERE m.srvname = s.srvname AND m.umuser = c.relowner)::text
		FROM pg_foreign_table ft JOIN pg_class c ON c.oid = ft.ftrelid JOIN pg_foreign_server s ON s.oid = ft.ftserver WHERE s.srvname = $1`, server)
	if err != nil {
		return err
	}
	if len(r) == 0 || r[0][1] == "true" {
		return nil // no partition (nothing to read), or its mapping is there
	}
	if o.ControlPlane == "" || o.KeyFile == "" {
		return fmt.Errorf("organization %d has no user mapping (profile direct with -drop-fdw-credentials): leaving direct needs -control-plane and -key, its owner's credentials make it again", o.Org)
	}
	set, err := loadCredentials(ctx, o.CloudDSN, o.ControlPlane, o.KeyFile, o.KeyFileOld)
	if err != nil {
		return err
	}
	db, err := dbName(o.CloudDSN)
	if err != nil {
		return err
	}
	dsn := set.DSN(db, strconv.FormatInt(o.Org, 10), creds.Owner)
	if dsn == "" {
		return fmt.Errorf("organization %d: no owner credentials in the control plane to make its user mapping again", o.Org)
	}
	cc, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("organization %d: its owner credentials do not parse", o.Org)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER MAPPING FOR %s SERVER %s OPTIONS (user %s, password %s)", r[0][0], q(server), lit(cc.User), lit(cc.Password))); err != nil {
		return fmt.Errorf("organization %d: its user mapping: %s", o.Org, pgCodeOf(err))
	}
	if _, err := conn.Exec(ctx, `SELECT lokadb.sync_mappings()`); err != nil {
		return fmt.Errorf("lokadb.sync_mappings: %w", err)
	}
	log.Info("the organization's user mapping made again from its owner's credentials", "org", o.Org, "server", server)
	return nil
}

// pgCodeOf: an error's SQLSTATE and message without the statement (it carries a password).
func pgCodeOf(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code + " " + pe.Message
	}
	return "failed"
}

// grantDirectRole gives o.DirectRole DML on the organization's moved tables, as their owner.
func grantDirectRole(ctx context.Context, cloud *pgx.Conn, o OrgProfileOptions, log *slog.Logger) error {
	if o.CustomerAppDSN == "" {
		return fmt.Errorf("-direct-role needs -customer-app: the DSN of the moved tables' owner in the organization's database")
	}
	r, err := rowsOf(ctx, cloud, `SELECT coalesce(nullif(to_jsonb(o)->>'schema', ''), 'public') FROM lokadb.orgs o WHERE org = $1`, o.Org)
	if err != nil || len(r) == 0 {
		return fmt.Errorf("the organization's schema: %v", err)
	}
	schema := r[0][0]
	cust, err := connectPlain(ctx, o.CustomerAppDSN)
	if err != nil {
		return fmt.Errorf("organization's database: %w", err)
	}
	defer cust.Close(context.Background())
	if r, err = rowsOf(ctx, cust, `SELECT (rolsuper OR rolbypassrls OR rolcreaterole OR rolcreatedb OR rolreplication)::text,
		pg_has_role(rolname, current_user, 'USAGE')::text FROM pg_roles WHERE rolname = $1`, o.DirectRole); err != nil {
		return err
	}
	switch {
	case len(r) == 0:
		return fmt.Errorf("role %s does not exist in the organization's database: its administrator creates it (CREATE ROLE %s LOGIN PASSWORD …), owning nothing", o.DirectRole, q(o.DirectRole))
	case r[0][0] == "true":
		return fmt.Errorf("role %s is a superuser or has BYPASSRLS, CREATEROLE, CREATEDB or REPLICATION: the direct path needs a role of DML alone", o.DirectRole)
	case r[0][1] == "true":
		return fmt.Errorf("role %s is the tables' owner or a member of it: the direct path needs a role of DML alone", o.DirectRole)
	}
	// the LokaDB tables alone (lokadb.tables of the cloud), not whatever else the owner has in
	// the schema (block 5: a public schema may hold tables of the organization's own)
	names, err := rowsOf(ctx, cloud, `SELECT table_name FROM lokadb.tables ORDER BY 1`)
	if err != nil {
		return err
	}
	// each table where move-org put it: public's in the organization's schema, another
	// schema's in that schema (block 4); the role needs USAGE on each of those schemas
	var wantS, wantN []string
	for _, n := range names {
		wantS, wantN = append(wantS, custSchemaOf(n[0], schema)), append(wantN, nameOf(n[0]))
	}
	tables, err := rowsOf(ctx, cust, `SELECT format('%I.%I', p.schemaname, p.tablename), (p.tableowner = current_user)::text, p.schemaname
		FROM pg_tables p JOIN unnest($1::text[], $2::text[]) w(s, n) ON p.schemaname = w.s AND p.tablename = w.n ORDER BY 1`, wantS, wantN)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("no table of lokadb.tables in schema %s of the organization's database: is the organization moved there?", schema)
	}
	for _, t := range tables {
		if t[1] != "true" {
			return fmt.Errorf("the -customer-app role does not own %s in the organization's database: it is move-org's -customer-app that grants", t[0])
		}
	}
	var schemas []string
	seen := map[string]bool{}
	for _, t := range tables {
		if !seen[t[2]] {
			seen[t[2]] = true
			schemas = append(schemas, t[2])
		}
	}
	stmts := []string{}
	for _, sc := range schemas {
		if r, err = rowsOf(ctx, cust, `SELECT has_schema_privilege($1, $2, 'USAGE')::text`, o.DirectRole, sc); err != nil {
			return err
		}
		if r[0][0] != "true" {
			stmts = append(stmts, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", q(sc), q(o.DirectRole)))
		}
	}
	for _, t := range tables {
		stmts = append(stmts, fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO %s", t[0], q(o.DirectRole)))
	}
	// the tables the proxy's DDL replay creates there later, under the same owner
	for _, sc := range schemas {
		stmts = append(stmts, fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", q(sc), q(o.DirectRole)))
	}
	for _, st := range stmts {
		if _, err := cust.Exec(ctx, st); err != nil {
			return fmt.Errorf("%s: %w", st, err)
		}
	}
	log.Info("direct path role granted DML", "org", o.Org, "role", o.DirectRole, "tables", len(tables), "schemas", strings.Join(schemas, ", "))
	return nil
}
