package main

// Block 5 (moving an organization): the organization's Postgres is older than the cloud's
// (risk hunt: PostgreSQL 14 for an organization of a cloud on 18). The DDL move-org sends is
// written for the organization's version, or refused before anything changes.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{customerHost: "customer14", id: "MV35", name: "the organization's Postgres is 14: a foreign key ON DELETE SET NULL between the moved tables (Laravel nullOnDelete) is created without the column list that is PostgreSQL 15, and still resets only the reference", run: checkSetNullOnPG14},
		check{customerHost: "customer14", id: "MV36", name: "the organization's Postgres is 14: UNIQUE NULLS NOT DISTINCT (PostgreSQL 15) is refused by check and by move-org before anything changes", run: checkNullsNotDistinctOnPG14},
	)
}

// MV35. notes.document_id references documents ON DELETE SET NULL. The FK among the organization's
// tables is (organization_id, document_id) in the cloud's definition with SET NULL (document_id): the
// column list is PostgreSQL 15, and PostgreSQL 14 stopped the move with a syntax error at "(".
func checkSetNullOnPG14(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE notes (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, document_id bigint REFERENCES documents (id) ON DELETE SET NULL, body text)`,
		"ALTER TABLE notes OWNER TO app",
		`INSERT INTO notes (organization_id, document_id, body) SELECT organization_id, id, 'n' FROM documents WHERE id IN (SELECT min(id) FROM documents GROUP BY organization_id)`); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "documents", "tenant_column": "organization_id"}, {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}, {"name": "notes", "tenant_column": "organization_id"}]}`
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	ver, _ := one(e.ctx, e.customer(), "SELECT current_setting('server_version_num')::int / 10000")
	if out, err := e.move(1, "-mode", "trigger"); err != nil {
		return false, fmt.Sprintf("move-org to Postgres %s failed: %s", ver, lastErr(out)), nil
	}
	doc, _ := one(e.ctx, e.customer(), "SELECT document_id FROM notes WHERE organization_id = 1")
	fk, _ := one(e.ctx, e.customer(), "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'notes'::regclass AND contype = 'f'")
	// the delete reaches the organization's database through the cloud; its FK resets the reference
	if err := execSQL(e.ctx, e.cloud(), "DELETE FROM documents WHERE id = "+doc); err != nil {
		return false, fmt.Sprintf("Postgres %s, FK %s: deleting the document through the cloud failed: %s", ver, fk, firstErrLine(err)), nil
	}
	after, _ := one(e.ctx, e.customer(), "SELECT coalesce(document_id::text, 'NULL') || '/' || organization_id FROM notes WHERE organization_id = 1")
	ok := ver == "14" && after == "NULL/1" && strings.Contains(fk, "SET NULL")
	return ok, fmt.Sprintf("Postgres %s at the organization; its FK: %s; after deleting document %s through the cloud, the note's document_id/organization_id: %s (want NULL/1)", ver, fk, doc, after), nil
}

// MV36. documents has a UNIQUE NULLS NOT DISTINCT (title) key on the cloud's Postgres 18.
func checkNullsNotDistinctOnPG14(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), "CREATE UNIQUE INDEX documents_title_nnd ON documents (title) NULLS NOT DISTINCT"); err != nil {
		return false, "", err
	}
	out, _ := e.checkWith(spec, "-customer", e.customer())
	found := reported(out, "customer_syntax")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	mv, moveErr := e.move(1, "-mode", "trigger")
	inCloud, _ := one(e.ctx, e.cloud(), orgInCloud)
	moved, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM lokadb.orgs WHERE org = 1")
	tables, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)")
	split, _ := one(e.ctx, e.cloud(), "SELECT coalesce(to_regclass('documents__o1')::text, 'none')")
	ok := found && moveErr != nil && strings.Contains(mv, "NULLS NOT DISTINCT") && strings.Contains(mv, "nothing was changed") &&
		moved == "0" && tables == "0" && split == "none" && inCloud == "100"
	return ok, fmt.Sprintf("check: customer_syntax %v (%s); move-org: %s; organization 1 in the cloud: %s documents, moved %s, split out of DEFAULT: %s; tables made at the organization: %s",
		found, finding(out, "customer_syntax"), lastErr(mv), inCloud, moved, split, tables), nil
}
