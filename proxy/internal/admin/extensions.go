package admin

// The extensions the moved tables need in the organization's database. One list for move-org
// (before it changes anything, and when it installs them) and for lokadb check: which
// extensions the tables use (types of columns, operator classes and functions of indexes and
// CHECK constraints: DependencySQL), which version the cloud runs, what the organization's
// server and role can do about it, and the command its administrator runs when the role
// cannot. Installing is by the same version as the cloud's; a server that cannot give that
// version is a refusal that says which versions it has.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/ext"
)

// Why an extension cannot be had in the organization's database.
const (
	ExtOK        = ""        // installed there, or installable by this role
	ExtPackage   = "package" // the server does not have it (not installed on that machine)
	ExtVersion   = "version" // the server has it, but not the cloud's version
	ExtPrivilege = "denied"  // the role may not install it (not superuser, extension not trusted)
	ExtOther     = "refused" // another refusal of CREATE EXTENSION
)

// ExtensionNeed is one extension the tables use.
type ExtensionNeed struct {
	Name         string
	CloudVersion string
	CloudSchema  string // the schema it is in in the cloud
	Schema       string // the schema it is created in at the organization (what is in public goes to the tables' schema)
	PlainOnly    bool   // only plain indexes use it: the tables answer the same without them

	// filled by PlanExtensions
	CustomerVersion string   // installed there, "" if not
	Available       []string // versions the organization's server can install
	Problem         string   // ExtOK or one of the above
	Detail          string   // the server's words for a refusal
	err             error    // the refusal of the trial installation
}

// Present: it is installed in the organization's database already.
func (n ExtensionNeed) Present() bool { return n.CustomerVersion != "" }

// CreateSQL: the statements that install it as the cloud has it (the statement itself is
// ext.InstallAt, which the administrator's hint shows too).
func (n ExtensionNeed) CreateSQL() []string {
	var out []string
	if n.Schema != "public" {
		out = append(out, "CREATE SCHEMA IF NOT EXISTS "+q(n.Schema))
	}
	return append(out, ext.InstallAt(n.Name, n.Schema, n.CloudVersion)+" CASCADE")
}

// Message: why it cannot be had and what the organization's administrator runs, in the words of
// the DDL replay's hints (ext.Hint). org: the organization's number, "" if not known.
func (n ExtensionNeed) Message(org string) string {
	err := n.err
	switch n.Problem {
	case ExtPackage:
		err = &pgconn.PgError{Code: "58P01", Message: "the server has no control file for it"}
	case ExtVersion:
		err = &pgconn.PgError{Code: "22023", Message: fmt.Sprintf("the server has versions %s, the cloud runs %s", strings.Join(n.Available, ", "), n.CloudVersion)}
	}
	if h := ext.HintAt(err, "move", n.Name, n.Schema, org, n.CloudVersion); h != "" {
		return h
	}
	return fmt.Sprintf("extension %s cannot be installed in the organization's database (%s). Its administrator installs it there: %s; then run move-org again",
		n.Name, n.Detail, ext.InstallAt(n.Name, n.Schema, n.CloudVersion))
}

// ExtensionNeeds: the extensions the dependencies deps (DependencySQL of the moved tables) belong to,
// with the cloud's version and the schema they are created in at the organization: the cloud's,
// and what the cloud has in public goes to schema (the tables' there, "" for public).
func ExtensionNeeds(ctx context.Context, cloud querier, deps []Dependency, schema string) ([]*ExtensionNeed, error) {
	plain := map[string]bool{}
	var names []string
	for _, d := range deps {
		if d.Ext == "" {
			continue
		}
		if _, ok := plain[d.Ext]; !ok {
			plain[d.Ext] = true
			names = append(names, d.Ext)
		}
		plain[d.Ext] = plain[d.Ext] && d.PlainIndex
	}
	sort.Strings(names)
	var out []*ExtensionNeed
	for _, name := range names {
		r, err := rowsOf(ctx, cloud, `SELECT e.extversion::text, n.nspname::text FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = $1`, name)
		if err != nil {
			return nil, err
		}
		if len(r) == 0 {
			continue // the cloud's own dependency is gone: nothing to carry
		}
		n := &ExtensionNeed{Name: name, CloudVersion: r[0][0], CloudSchema: r[0][1], Schema: r[0][1], PlainOnly: plain[name]}
		if n.Schema == "public" && schema != "" {
			n.Schema = schema
		}
		out = append(out, n)
	}
	return out, nil
}

// TablesExtensions: ExtensionNeeds of the tables (their DependencySQL).
func TablesExtensions(ctx context.Context, cloud querier, tables []string, schema string) ([]*ExtensionNeed, error) {
	var deps []Dependency
	for _, t := range tables {
		ds, err := Dependencies(ctx, cloud, t)
		if err != nil {
			return nil, err
		}
		deps = append(deps, ds...)
	}
	return ExtensionNeeds(ctx, cloud, deps, schema)
}

// PlanExtensions finds out, for each need, what the organization's database has and what its
// role can install: what is installed (and its version), the versions the server has, and, for
// one not installed, the installation itself, tried in a transaction that is rolled back (each
// under a savepoint, so that one refused hides no other). Nothing is changed.
func PlanExtensions(ctx context.Context, cust *pgx.Conn, needs []*ExtensionNeed) error {
	var try []*ExtensionNeed
	for _, n := range needs {
		r, err := rowsOf(ctx, cust, `SELECT extversion::text FROM pg_extension WHERE extname = $1`, n.Name)
		if err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		if len(r) > 0 {
			n.CustomerVersion = r[0][0]
			continue
		}
		av, err := rowsOf(ctx, cust, `SELECT version::text FROM pg_available_extension_versions WHERE name = $1 ORDER BY version`, n.Name)
		if err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		for _, a := range av {
			n.Available = append(n.Available, a[0])
		}
		switch {
		case len(n.Available) == 0:
			n.Problem = ExtPackage
		case n.CloudVersion != "" && !slices.Contains(n.Available, n.CloudVersion):
			n.Problem = ExtVersion
		default:
			try = append(try, n)
		}
	}
	if len(try) == 0 {
		return nil
	}
	tx, err := cust.Begin(ctx)
	if err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '2s'; SET LOCAL statement_timeout = '30s'"); err != nil {
		return fmt.Errorf("customer database: %w", err)
	}
	for _, n := range try {
		if _, err := tx.Exec(ctx, "SAVEPOINT lokadb_ext_probe"); err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
		var failed error
		for _, s := range n.CreateSQL() {
			if _, failed = tx.Exec(ctx, s); failed != nil {
				break
			}
		}
		if failed != nil {
			n.Problem, n.Detail, n.err = ExtOther, firstLine(failed.Error()), failed
			var pe *pgconn.PgError
			if errors.As(failed, &pe) {
				n.Detail = firstLine(pe.Message)
				if pe.Code == "42501" { // insufficient_privilege: not superuser, not a trusted extension
					n.Problem = ExtPrivilege
				}
			}
		}
		if _, err := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT lokadb_ext_probe"); err != nil {
			return fmt.Errorf("customer database: %w", err)
		}
	}
	return nil
}

// ExtensionRefusal: why move-org cannot go on, for the extensions the tables cannot do without
// (those only plain indexes use are left out: the tables are made without such indexes). nil
// when none is refused.
func ExtensionRefusal(needs []*ExtensionNeed, org string) error {
	var msgs []string
	for _, n := range needs {
		if n.Problem != ExtOK && !n.PlainOnly {
			msgs = append(msgs, n.Message(org))
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf("the organization's database cannot get the extensions the tables use (nothing was changed): %s", strings.Join(msgs, "; "))
}

// preflightExtensions: move-org's check, before its first change, that the organization's
// database has or can get the extensions the tables use.
func preflightExtensions(ctx context.Context, cloud, cust *pgx.Conn, o MoveOptions, log *slog.Logger) error {
	var tables []string
	for _, st := range o.Spec.Tables {
		tables = append(tables, st.Name)
	}
	needs, err := TablesExtensions(ctx, cloud, tables, o.CustomerSchema)
	if err != nil {
		return err
	}
	if err := PlanExtensions(ctx, cust, needs); err != nil {
		return err
	}
	org := fmt.Sprint(o.Org)
	for _, n := range needs {
		switch {
		case n.Present() && n.CustomerVersion != n.CloudVersion:
			log.Warn("the organization's database has this extension in another version than the cloud: left as it is",
				"extension", n.Name, "cloud", n.CloudVersion, "organization", n.CustomerVersion)
		case n.Present():
			log.Info("extension present in the organization's database", "extension", n.Name, "version", n.CustomerVersion)
		case n.Problem == ExtOK:
			log.Info("extension will be installed in the organization's database", "extension", n.Name, "version", n.CloudVersion, "schema", n.Schema)
		case n.PlainOnly:
			log.Warn("an extension only plain indexes use cannot be installed in the organization's database: its tables get no such indexes", "extension", n.Name, "why", n.Message(org))
		}
	}
	return ExtensionRefusal(needs, org)
}
