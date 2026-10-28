// Package ext: extensions of the organizations' databases (what the application's migrations
// install in the cloud), shared by move-org's check before a move and the DDL replay after it:
// the statement an administrator runs, and what to tell them when the organization's database
// cannot take an extension.
package ext

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// cloudOnly are extensions that serve the cloud database itself: they are not installed in an
// organization's database by a migration (lokadb does not install them either when it moves a
// table: the tables do not depend on them).
var cloudOnly = map[string]bool{
	"postgres_fdw": true, "pg_stat_statements": true, "pg_buffercache": true, "pg_prewarm": true,
	"pgstattuple": true, "pg_cron": true, "pg_wait_sampling": true, "pg_stat_kcache": true,
	"pgaudit": true, "pg_repack": true, "pg_squeeze": true, "pglogical": true,
}

// CloudOnly: the extension is the cloud database's own, not replayed in organizations.
func CloudOnly(name string) bool { return cloudOnly[strings.ToLower(name)] }

// Install is the statement that installs extension name in schema there (schema "" = the
// database's default), as an administrator of an organization's database runs it.
func Install(name, schema string) string {
	if schema == "" {
		return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s", pgx.Identifier{name}.Sanitize())
	}
	return fmt.Sprintf("CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s", pgx.Identifier{name}.Sanitize(), pgx.Identifier{schema}.Sanitize())
}

// InstallAt is Install at version (the cloud's: move-org installs what the cloud runs); "" = the
// server's default.
func InstallAt(name, schema, version string) string {
	s := Install(name, schema)
	if version != "" {
		s += " VERSION '" + strings.ReplaceAll(version, "'", "''") + "'"
	}
	return s
}

// Update is the statement that brings an installed extension to its newest available version.
func Update(name string) string {
	return "ALTER EXTENSION " + pgx.Identifier{name}.Sanitize() + " UPDATE"
}

// Hint turns the error err of installing (op "create"), updating ("alter") or dropping
// ("drop") extension name in organization org's database into what its administrator can act
// on, with the exact statement; "" if err is none an administrator fixes. The cloud did not
// run the migration either, so repeating it after the fix is all that is left.
func Hint(err error, op, name, schema, org string) string {
	return HintAt(err, op, name, schema, org, "")
}

// HintAt is Hint for move-org's installation (op "move"): the statement carries the cloud's
// version, and the text ends with running move-org again, not the migration. org "" = the
// organization of the move (check, which may not know its number).
func HintAt(err error, op, name, schema, org, version string) string {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return ""
	}
	where := "organization " + org + "'s database"
	if org == "" {
		where = "the organization's database"
	}
	retry := "repeat the migration"
	if op == "move" {
		retry = "run move-org again"
	}
	run := InstallAt(name, schema, version)
	if op == "alter" {
		run = Update(name)
	}
	switch pe.Code {
	case "42501": // insufficient_privilege
		return fmt.Sprintf("extension %s cannot be %s in %s by the application's role (%s). Its administrator runs it there as a superuser (or as a role that may do so): %s; then %s",
			name, past(op), where, pe.Message, run, retry)
	case "58P01", "0A000": // undefined_file: no control file; feature_not_supported
		if op == "drop" {
			return ""
		}
		return fmt.Sprintf("extension %s is not available in %s (%s). Install its package on that Postgres server (for pgvector, PostGIS and the like the operating system's package for its major version), then its administrator runs: %s; then %s",
			name, where, pe.Message, run, retry)
	case "22023": // invalid_parameter_value: no installation script or update path for the version
		return fmt.Sprintf("extension %s has no such version to %s in %s (%s). The cloud's version of the package is not installed on that server: install the same version there, then run: %s; then %s",
			name, opVerb(op), where, pe.Message, run, retry)
	case "2BP01": // dependent_objects_still_exist
		return fmt.Sprintf("extension %s is still used in %s (%s): drop what uses it there first (its tables' columns of the extension's types), or keep the extension; the cloud did not drop it either",
			name, where, strings.TrimSpace(pe.Detail))
	}
	return ""
}

func past(op string) string {
	switch op {
	case "alter":
		return "updated"
	case "drop":
		return "dropped"
	}
	return "installed"
}

func opVerb(op string) string {
	switch op {
	case "alter":
		return "update to"
	case "drop":
		return "drop"
	}
	return "install"
}
