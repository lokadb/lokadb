// Package admin converts selected tables of an existing database into LokaDB tables
// and moves one organization's rows into that organization's own Postgres.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

type Spec struct {
	Tables  []SpecTable `json:"tables"`
	Routing *Routing    `json:"routing,omitempty"` // declarations the schema lacks (routing.go)
}

// SpecTable is either a root table that has a tenant column, or a child table
// whose tenant is derived through a single-column FK to an earlier table.
type SpecTable struct {
	Name         string `json:"name"`
	TenantColumn string `json:"tenant_column,omitempty"`
	ParentTable  string `json:"parent_table,omitempty"`
	ParentColumn string `json:"parent_column,omitempty"`
}

type runner struct {
	ctx context.Context
	tx  pgx.Tx
	log *slog.Logger
}

func (r *runner) exec(sql string, args ...any) error {
	r.log.Debug("exec", "sql", mask(sql))
	if _, err := r.tx.Exec(r.ctx, sql, args...); err != nil {
		return fmt.Errorf("%w\n  in: %s", err, mask(sql))
	}
	return nil
}

func (r *runner) rows(sql string, args ...any) ([][]string, error) {
	rows, err := r.tx.Query(r.ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%w\n  in: %s", err, mask(sql))
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v != nil {
				row[i] = fmt.Sprint(v)
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func q(s string) string { return pgx.Identifier{s}.Sanitize() }

// partKey is the partition key of a LokaDB table: its tenant column, NULL as -2 (rows of no
// organization, <table>__none).
func partKey(tc string) string { return fmt.Sprintf("(coalesce(%s, -2))", q(tc)) }

func lit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// MetaChangesSQL: lokadb.meta_changes and what fills it (part of baseSQL; the proxy's stamp
// reads it, and its live test sets it up).
const MetaChangesSQL = `
-- The metadata's changes (BACKLOG block 9, item 10): what the proxy's stamp reads instead of
-- hashing the catalog (20-200 ms of the cloud's work at thousands of organizations, at every
-- connection). A row per change, added in the transaction that makes it: count and max(id)
-- move exactly when it commits (a counter row would hold a lock until then, a sequence would
-- move before), and nothing locks. A change of the rows of lokadb's tables adds one (a trigger);
-- DDL, GRANT and REVOKE add one through an event trigger, which only a superuser may create:
-- without it (ddl_tracked false) the proxy goes on hashing the catalog.
CREATE TABLE IF NOT EXISTS lokadb.meta_changes (id bigserial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS lokadb.meta_tracking (one boolean PRIMARY KEY DEFAULT true CHECK (one), ddl_tracked boolean NOT NULL DEFAULT false);
INSERT INTO lokadb.meta_tracking DEFAULT VALUES ON CONFLICT DO NOTHING;
GRANT SELECT ON lokadb.meta_changes, lokadb.meta_tracking TO PUBLIC;
CREATE OR REPLACE FUNCTION lokadb.meta_changed() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  INSERT INTO lokadb.meta_changes DEFAULT VALUES;
  RETURN NULL;
END $$;
DO $m$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['tables', 'lookups', 'orgs', 'tenants', 'unique_refs'] LOOP
    IF to_regclass('lokadb.' || t) IS NOT NULL AND NOT EXISTS (SELECT 1 FROM pg_trigger
        WHERE tgname = 'lokadb_meta_changed' AND tgrelid = to_regclass('lokadb.' || t)) THEN
      EXECUTE format('CREATE TRIGGER lokadb_meta_changed AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON lokadb.%I FOR EACH STATEMENT EXECUTE FUNCTION lokadb.meta_changed()', t);
    END IF;
  END LOOP;
END $m$;
-- DDL of a temporary object alone (an application's CREATE TEMP TABLE, its index) is no change
CREATE OR REPLACE FUNCTION lokadb.meta_ddl() RETURNS event_trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF NOT (EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands())
      AND NOT EXISTS (SELECT 1 FROM pg_event_trigger_ddl_commands() c WHERE c.schema_name IS NULL OR c.schema_name NOT LIKE 'pg\_temp%')) THEN
    INSERT INTO lokadb.meta_changes DEFAULT VALUES;
  END IF;
EXCEPTION WHEN OTHERS THEN
  NULL; -- never the DDL's failure: the proxy reads the metadata again within metaTTL anyway
END $$;
-- triggers do not check EXECUTE when they fire: nobody calls these by hand
REVOKE ALL ON FUNCTION lokadb.meta_changed() FROM PUBLIC;
REVOKE ALL ON FUNCTION lokadb.meta_ddl() FROM PUBLIC;
DO $e$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'lokadb_meta_ddl') THEN
    CREATE EVENT TRIGGER lokadb_meta_ddl ON ddl_command_end EXECUTE FUNCTION lokadb.meta_ddl();
  END IF;
  UPDATE lokadb.meta_tracking SET ddl_tracked = true WHERE NOT ddl_tracked;
EXCEPTION WHEN insufficient_privilege THEN
  NULL; -- not a superuser: the proxy hashes the catalog (ddl_tracked stays false)
END $e$;
`

var baseSQL = `
CREATE SCHEMA IF NOT EXISTS lokadb;
` + keySQL + `
-- The policy's functions (org, org_lo_uuid, org_hi_uuid) run in the application's session, with
-- its search_path: every name in them is pg_catalog's, written out (the review of routing, 2nd
-- pass: a public.current_setting(text, boolean) of the application's, under SET search_path =
-- public, pg_catalog, gave the policy any organization it liked). No SET search_path on them:
-- it would keep Postgres from inlining them into the policy, which is evaluated for each row
-- (the other lokadb functions an application's session reaches have it, pg_temp last).
CREATE OR REPLACE FUNCTION lokadb.org() RETURNS bigint LANGUAGE sql STABLE
  AS $$ SELECT CASE WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '' THEN NULL
                    ELSE pg_catalog.current_setting('lokadb.org', true)::pg_catalog.int8 END $$;
-- org_default is the default of a tenant column the application never writes (child tables):
-- the proxy sets lokadb.org from the parent's id, -1 when the parent was not found (such a
-- row must not be stored under a made-up organization), -2 when the row has no
-- organization: no parent, or a parent that has none. That row stays in the cloud
-- (<table>__none).
CREATE OR REPLACE FUNCTION lokadb.org_default() RETURNS bigint LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE o bigint := lokadb.org();
BEGIN
  IF o = -2 THEN
    RETURN NULL;
  END IF;
  IF o IS NULL THEN
    RAISE EXCEPTION 'lokadb: the row reached the table without its organization (the write did not go through the proxy, or the proxy had not yet learnt that enable made the table lokadb''s); retry the statement'
      USING ERRCODE = '23503';
  END IF;
  IF o < 0 THEN
    RAISE EXCEPTION 'lokadb: the organization of the new row is unknown: its parent row was not found'
      USING ERRCODE = '23503';
  END IF;
  RETURN o;
END $$;
-- Organizations numbered by uuid (block 18, admin/tenants.go): lokadb.org holds the
-- organization's value (the proxy's text), -2 (rows of none), -1 (nothing found) or nothing
-- (every organization). The policy is a range over the key (coalesce(column, nil uuid)), as
-- for integers: org_lo_uuid and org_hi_uuid are its bounds; -1 an empty one.
CREATE OR REPLACE FUNCTION lokadb.org_lo_uuid() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN coalesce(pg_catalog.current_setting('lokadb.org', true), '') OPERATOR(pg_catalog.=) '' THEN '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-2' THEN '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-1' THEN 'ffffffff-ffff-ffff-ffff-ffffffffffff'::pg_catalog.uuid
    ELSE pg_catalog.current_setting('lokadb.org', true)::pg_catalog.uuid END $$;
CREATE OR REPLACE FUNCTION lokadb.org_hi_uuid() RETURNS uuid LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN coalesce(pg_catalog.current_setting('lokadb.org', true), '') OPERATOR(pg_catalog.=) '' THEN 'ffffffff-ffff-ffff-ffff-ffffffffffff'::pg_catalog.uuid
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-2' THEN '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-1' THEN '00000000-0000-0000-0000-000000000000'::pg_catalog.uuid
    ELSE pg_catalog.current_setting('lokadb.org', true)::pg_catalog.uuid END $$;
-- org_default_uuid: org_default of a child's organization column numbered by uuid.
CREATE OR REPLACE FUNCTION lokadb.org_default_uuid() RETURNS uuid LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE o text := nullif(current_setting('lokadb.org', true), '');
BEGIN
  IF o = '-2' THEN
    RETURN NULL;
  END IF;
  IF o IS NULL THEN
    RAISE EXCEPTION 'lokadb: the row reached the table without its organization (the write did not go through the proxy, or the proxy had not yet learnt that enable made the table lokadb''s); retry the statement'
      USING ERRCODE = '23503';
  END IF;
  IF o = '-1' THEN
    RAISE EXCEPTION 'lokadb: the organization of the new row is unknown: its parent row was not found'
      USING ERRCODE = '23503';
  END IF;
  RETURN o::uuid;
END $$;
-- Organizations named by a string (text, varchar, character(n); a Laravel ulid() is character(26)):
-- lokadb.org holds the string, the key is the column as text in the byte order of collation "C"
-- (coalesce(column::text, '')), the range's bounds are strings. org_max_text is above every
-- value (a string of four of the highest character the database encoding has); -1 is an empty
-- range (the lower bound there, '' the upper).
CREATE OR REPLACE FUNCTION lokadb.org_max_text() RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN pg_catalog.getdatabaseencoding() OPERATOR(pg_catalog.=) 'UTF8'
    THEN pg_catalog.repeat(pg_catalog.chr(1114111), 4) ELSE pg_catalog.repeat(pg_catalog.chr(255), 4) END $$;
CREATE OR REPLACE FUNCTION lokadb.org_lo_text() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN coalesce(pg_catalog.current_setting('lokadb.org', true), '') OPERATOR(pg_catalog.=) '' THEN ''::pg_catalog.text
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-2' THEN ''::pg_catalog.text
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-1' THEN lokadb.org_max_text()
    ELSE pg_catalog.current_setting('lokadb.org', true) END $$;
CREATE OR REPLACE FUNCTION lokadb.org_hi_text() RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN coalesce(pg_catalog.current_setting('lokadb.org', true), '') OPERATOR(pg_catalog.=) '' THEN lokadb.org_max_text()
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-2' THEN ''::pg_catalog.text
    WHEN pg_catalog.current_setting('lokadb.org', true) OPERATOR(pg_catalog.=) '-1' THEN ''::pg_catalog.text
    ELSE pg_catalog.current_setting('lokadb.org', true) END $$;
-- org_default_text: org_default of a child's organization column named by a string.
CREATE OR REPLACE FUNCTION lokadb.org_default_text() RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
DECLARE o text := nullif(current_setting('lokadb.org', true), '');
BEGIN
  IF o = '-2' THEN
    RETURN NULL;
  END IF;
  IF o IS NULL THEN
    RAISE EXCEPTION 'lokadb: the row reached the table without its organization (the write did not go through the proxy, or the proxy had not yet learnt that enable made the table lokadb''s); retry the statement'
      USING ERRCODE = '23503';
  END IF;
  IF o = '-1' THEN
    RAISE EXCEPTION 'lokadb: the organization of the new row is unknown: its parent row was not found'
      USING ERRCODE = '23503';
  END IF;
  RETURN o;
END $$;
-- The numbers of organizations numbered by uuid (lokadb org-add): N names (<t>__o<N>,
-- lokadb_org<N>, the tunnel), the value is the organization for rows. Every role reads it (the
-- proxy), only lokadb's owner writes it.
CREATE TABLE IF NOT EXISTS lokadb.tenants (org bigint PRIMARY KEY, tenant text NOT NULL UNIQUE,
  added timestamptz NOT NULL DEFAULT now());
CREATE SEQUENCE IF NOT EXISTS lokadb.tenants_org_seq OWNED BY lokadb.tenants.org;
-- moving: a move (partition_org, the split) is in progress in this transaction (lokadb.moving = on)
-- and the role is one that may run it: the owner of schema lokadb, or a member of it. Any session
-- can set the setting, so by itself it would switch the refs and key triggers off for a writer.
CREATE OR REPLACE FUNCTION lokadb.moving() RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(current_setting('lokadb.moving', true), '') = 'on'
    AND pg_has_role(current_user, (SELECT nspowner FROM pg_namespace WHERE nspname = 'lokadb'), 'MEMBER') $$;
-- parent_guard: a child row's organization comes from its parent and is not computed again
-- when the parent changes (a BEFORE trigger cannot move a row to another partition). A new
-- parent of another organization, or a parent for a row of no organization, is refused;
-- clearing the reference (ON DELETE SET NULL) keeps the row where it is. On INSERT the row's
-- organization (lokadb.org: the proxy's, or the application's own in the cooperative mode)
-- must be its parent's; a parent that is not there is left to the FK.
CREATE OR REPLACE FUNCTION lokadb.parent_guard() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  ref text := to_jsonb(NEW) ->> TG_ARGV[1];
  -- organizations as text: an integer or a uuid (block 18), compared as Postgres prints them
  had text;
  org text;
  n int;
BEGIN
  IF TG_OP = 'INSERT' THEN
    IF ref IS NULL THEN RETURN NEW; END IF;
    had := to_jsonb(NEW) ->> TG_ARGV[2];
  ELSE
    IF ref IS NULL OR ref IS NOT DISTINCT FROM to_jsonb(OLD) ->> TG_ARGV[1] THEN RETURN NEW; END IF;
    had := to_jsonb(OLD) ->> TG_ARGV[2];
  END IF;
  -- the reference as it is, of the key's type (block 17: uuid, text, not only integers)
  EXECUTE format('SELECT tenant::text FROM %s WHERE id = ($1).%I', lokadb.qrel(TG_ARGV[0]), TG_ARGV[1]) INTO org USING NEW;
  GET DIAGNOSTICS n = ROW_COUNT;
  IF TG_OP = 'INSERT' AND n = 0 THEN RETURN NEW; END IF;
  -- the blanks a character(n) organization column adds are not part of the value
  org := rtrim(org); had := rtrim(had);
  IF org IS DISTINCT FROM had THEN
    IF TG_OP = 'INSERT' THEN
      RAISE EXCEPTION 'lokadb: %.% = %: the parent belongs to organization %, the new row to %',
        TG_ARGV[3], TG_ARGV[1], ref, coalesce(org, 'none'), coalesce(had, 'none')
        USING ERRCODE = '23514';
    END IF;
    RAISE EXCEPTION 'lokadb: %.% = %: the new parent belongs to organization %, the row to %; lokadb does not move a row to another organization by its parent',
      TG_ARGV[3], TG_ARGV[1], ref, coalesce(org, 'none'), coalesce(had, 'none')
      USING ERRCODE = '0A000';
  END IF;
  RETURN NEW;
END $$;
-- refs_sync keeps <table>__refs (key -> organization; its column id holds the table's key,
-- TG_ARGV[2], id for the triggers of an older enable; a composite key, a JSON array of its
-- columns, in id_1 ... id_n). An UPDATE that moves a row to another
-- partition fires DELETE and INSERT; one inside a partition (a new key, or a new organization
-- while both are in DEFAULT) fires UPDATE. A new key in refs is subject to the FKs onto it:
-- their ON UPDATE action, as the original FK onto the table had. The key is read from the row
-- with its own type (block 17: uuid, text), the refs table's id is of that type.
-- key_cols: the columns of a key as lokadb writes it (compkey.go): one name, or a JSON array
-- of names (a composite key).
CREATE OR REPLACE FUNCTION lokadb.key_cols(k text) RETURNS text[] LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN left(k, 1) = '[' THEN ARRAY(SELECT jsonb_array_elements_text(k::jsonb)) ELSE ARRAY[k] END $$;
CREATE OR REPLACE FUNCTION lokadb.refs_sync() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  k text := CASE WHEN TG_NARGS > 2 THEN TG_ARGV[2] ELSE 'id' END;
  ks text[] := lokadb.key_cols(k);
  ids text;   -- the refs table's key columns: id, or id_1 ... id_n
  vals text;  -- the key of the row ($1)
  cond text;  -- the refs row of the key of the row ($1)
  newk text;  -- the key of NEW, OLD as text (a JSON array); NULL: a column of it is NULL
  oldk text;
BEGIN
  IF lokadb.moving() THEN RETURN NULL; END IF;
  IF cardinality(ks) = 1 THEN
    ids := 'id'; vals := format('($1).%I', k); cond := format('id = ($1).%I', k);
  ELSE
    SELECT string_agg(format('id_%s', o), ', ' ORDER BY o), string_agg(format('($1).%I', c), ', ' ORDER BY o),
           string_agg(format('id_%s = ($1).%I', o, c), ' AND ' ORDER BY o)
      INTO ids, vals, cond FROM unnest(ks) WITH ORDINALITY u(c, o);
  END IF;
  IF TG_OP <> 'INSERT' THEN
    SELECT CASE WHEN count(v) = count(*) THEN jsonb_agg(v ORDER BY o)::text END INTO oldk
      FROM unnest(ks) WITH ORDINALITY u(c, o), LATERAL (SELECT to_jsonb(OLD) ->> u.c AS v) x;
  END IF;
  IF TG_OP <> 'DELETE' THEN
    SELECT CASE WHEN count(v) = count(*) THEN jsonb_agg(v ORDER BY o)::text END INTO newk
      FROM unnest(ks) WITH ORDINALITY u(c, o), LATERAL (SELECT to_jsonb(NEW) ->> u.c AS v) x;
  END IF;
  -- a unique column other than the key (lokadb.unique_refs) may be NULL: no row stands for it
  IF TG_OP = 'INSERT' THEN
    IF newk IS NOT NULL THEN
      EXECUTE format('INSERT INTO %s (%s, tenant) VALUES (%s, ($1).%I)', lokadb.qrel(TG_ARGV[0]), ids, vals, TG_ARGV[1]) USING NEW;
    END IF;
  ELSIF TG_OP = 'DELETE' THEN
    EXECUTE format('DELETE FROM %s WHERE %s', lokadb.qrel(TG_ARGV[0]), cond) USING OLD;
  ELSIF newk IS NULL AND oldk IS NOT NULL THEN
    EXECUTE format('DELETE FROM %s WHERE %s', lokadb.qrel(TG_ARGV[0]), cond) USING OLD;
  ELSIF newk IS NOT NULL AND oldk IS NULL THEN
    EXECUTE format('INSERT INTO %s (%s, tenant) VALUES (%s, ($1).%I)', lokadb.qrel(TG_ARGV[0]), ids, vals, TG_ARGV[1]) USING NEW;
  ELSIF newk IS DISTINCT FROM oldk
     OR to_jsonb(NEW) ->> TG_ARGV[1] IS DISTINCT FROM to_jsonb(OLD) ->> TG_ARGV[1] THEN
    EXECUTE format('UPDATE %s SET (%s, tenant) = ROW(%s, ($1).%I) WHERE %s', lokadb.qrel(TG_ARGV[0]), ids, vals, TG_ARGV[1],
        replace(cond, '($1).', '($2).'))
      USING NEW, OLD;
  END IF;
  RETURN NULL;
END $$;
CREATE TABLE IF NOT EXISTS lokadb.tables (table_name text PRIMARY KEY, tenant_column text NOT NULL,
  refs_table text NOT NULL, cloud_partition text NOT NULL);
CREATE TABLE IF NOT EXISTS lokadb.lookups (table_name text, column_name text, refs_table text,
  PRIMARY KEY (table_name, column_name));
-- block 4, item 2: an FK onto a unique column of a LokaDB table other than its key points at a
-- map of that column (value -> organization) kept like the refs table, by its own trigger
CREATE TABLE IF NOT EXISTS lokadb.unique_refs (table_name text, column_name text, refs_table text NOT NULL,
  PRIMARY KEY (table_name, column_name));
CREATE TABLE IF NOT EXISTS lokadb.orgs (org bigint PRIMARY KEY, server text NOT NULL);
-- the schema of the organization's tables in its database (move-org -customer-schema)
ALTER TABLE lokadb.orgs ADD COLUMN IF NOT EXISTS schema text NOT NULL DEFAULT 'public';
-- the id of the organization's database in the mark on its tables there (dbid.go); NULL: none
ALTER TABLE lokadb.orgs ADD COLUMN IF NOT EXISTS db_id text;
-- the copies move-back left in organizations' databases it could not mark as returned (returned.go)
CREATE TABLE IF NOT EXISTS lokadb.returned (db_id text PRIMARY KEY, org bigint NOT NULL, at timestamptz NOT NULL DEFAULT now());
-- the table's key: its one-column primary key, or id (refs and lookups map it to the organization)
ALTER TABLE lokadb.tables ADD COLUMN IF NOT EXISTS key_column text NOT NULL DEFAULT 'id';
-- What enable changes and disable gives back: the table's owner before enable, and its
-- identity columns ([{"col", "generated": "a"|"d", "start", "increment", "min", "max",
-- "cache", "cycle"}], [] if none). NULL: enabled by an older version.
ALTER TABLE lokadb.tables ADD COLUMN IF NOT EXISTS orig_owner text;
ALTER TABLE lokadb.tables ADD COLUMN IF NOT EXISTS identities jsonb;
-- and its row security: off, on or forced (review3 R3-2: disable gives it back). NULL: enabled
-- by an older version (disable then reads it from the policies the parent has)
ALTER TABLE lokadb.tables ADD COLUMN IF NOT EXISTS orig_rls text;
` + MetaChangesSQL + `
-- publications whose publish_via_partition_root enable changed, with the value before it
CREATE TABLE IF NOT EXISTS lokadb.publications (pubname text PRIMARY KEY, viaroot_before boolean NOT NULL);
-- same_org(new, old): the organization column of an UPDATE (block 2, item 7; the proxy's
-- rewritten UPDATE calls it). Writing the same value back (Django save(), Rails) passes;
-- a real change is refused: moving a row to another organization is not supported.
-- (the same for organizations numbered by uuid, block 18)
CREATE OR REPLACE FUNCTION lokadb.same_org(new uuid, old uuid) RETURNS uuid LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF new IS DISTINCT FROM old THEN
    RAISE EXCEPTION 'lokadb: moving a row to another organization is not supported'
      USING ERRCODE = '0A000', DETAIL = format('organization %s -> %s', coalesce(old::text, 'none'), coalesce(new::text, 'none'));
  END IF;
  RETURN new;
END $$;
CREATE OR REPLACE FUNCTION lokadb.same_org(new text, old text) RETURNS text LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF new IS DISTINCT FROM old THEN
    RAISE EXCEPTION 'lokadb: moving a row to another organization is not supported'
      USING ERRCODE = '0A000', DETAIL = format('organization %s -> %s', coalesce(old, 'none'), coalesce(new, 'none'));
  END IF;
  RETURN new;
END $$;
CREATE OR REPLACE FUNCTION lokadb.same_org(new bigint, old bigint) RETURNS bigint LANGUAGE plpgsql STABLE SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF new IS DISTINCT FROM old THEN
    RAISE EXCEPTION 'lokadb: moving a row to another organization is not supported'
      USING ERRCODE = '0A000', DETAIL = format('organization %s -> %s', coalesce(old::text, 'none'), coalesce(new::text, 'none'));
  END IF;
  RETURN new;
END $$;
-- enable -prepare: what it has done for each table (stage: refs, tenant, fks, index, ready;
-- done of total rows, last: the last key of the batch done, as text: a key of any type), until
-- enable switches it
CREATE TABLE IF NOT EXISTS lokadb.prepare (table_name text PRIMARY KEY, stage text NOT NULL,
  done bigint NOT NULL DEFAULT 0, total bigint NOT NULL DEFAULT 0, last text,
  started timestamptz NOT NULL DEFAULT now(), updated timestamptz NOT NULL DEFAULT now());
-- an older lokadb kept integer keys only (block 17)
DO $p$ BEGIN
  IF (SELECT atttypid FROM pg_attribute WHERE attrelid = 'lokadb.prepare'::regclass AND attname = 'last') <> 'text'::regtype THEN
    ALTER TABLE lokadb.prepare ALTER COLUMN last TYPE text USING last::text;
  END IF;
END $p$;
-- prep_tenant: while enable -prepare fills a child's organization column, a row written by
-- the application gets it from its parent's refs table (arguments: refs table, the column
-- referencing the parent, the organization column, the key). BEFORE sets it. AFTER fixes a
-- row whose parent the same statement wrote (a data-modifying WITH): the parent's refs row
-- comes from its AFTER trigger, at the end of the statement, after this row's BEFORE.
CREATE OR REPLACE FUNCTION lokadb.prep_tenant() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  ref text := to_jsonb(NEW) ->> TG_ARGV[1];
  org text; -- an integer or a uuid organization (block 18), as Postgres prints it
BEGIN
  IF ref IS NOT NULL THEN -- of the parent's key's type (block 17)
    EXECUTE format('SELECT tenant::text FROM %s WHERE id = ($1).%I', lokadb.qrel(TG_ARGV[0]), TG_ARGV[1]) INTO org USING NEW;
  END IF;
  IF TG_WHEN = 'BEFORE' THEN
    RETURN jsonb_populate_record(NEW, jsonb_build_object(TG_ARGV[2], org));
  END IF;
  IF org IS DISTINCT FROM to_jsonb(NEW) ->> TG_ARGV[2] THEN
    -- the row by its key: one column, or several (a composite key, lokadb.key_cols)
    EXECUTE format('UPDATE %s SET %I = CAST($1 AS %s) WHERE %s', TG_RELID::regclass, TG_ARGV[2],
        (SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = TG_RELID AND attname = TG_ARGV[2]),
        (SELECT string_agg(format('%I = ($2).%I', c, c), ' AND ') FROM unnest(lokadb.key_cols(TG_ARGV[3])) c))
      USING org, NEW;
  END IF;
  RETURN NULL;
END $$;
-- The organization policy of a LokaDB table. lokadb.org set: only that organization's rows
-- (a range rather than =, which keeps partition pruning working in both modes); unset: all of
-- them; -2: the rows of no organization. Rows of a foreign partition (an organization's own
-- database, which the customer controls) are trusted only for that organization: a local
-- partition's rows pass, a foreign one's must carry the organization of its partition bound.
-- One condition for every organization, in the same policy: a restrictive policy per
-- organization made planning grow with the organizations, and as a qual of its own security
-- level it kept the organization range from being an index condition on DEFAULT.
-- foreign_parts and bound_orgs read the catalog once per statement (initplans), from the
-- foreign tables (pg_foreign_table, each found in pg_inherits by its index on inhrelid): as
-- many as the organizations moved, not every partition (review of performance F7: local_parts,
-- the local partitions read through all the parent's children, cost 5 ms a statement at a
-- thousand organizations, 15 at three thousand). The catalog itself, no list of lokadb's own to
-- drift from it. local_parts stays for policies of an older lokadb until they are made again.
CREATE OR REPLACE FUNCTION lokadb.local_parts(parent regclass) RETURNS oid[] LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(array_agg(i.inhrelid), '{}') FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
  WHERE i.inhparent = parent AND c.relkind <> 'f' $$;
CREATE OR REPLACE FUNCTION lokadb.foreign_parts(parent regclass) RETURNS oid[] LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(array_agg(f.ftrelid), '{}') FROM pg_foreign_table f JOIN pg_inherits i ON i.inhrelid = f.ftrelid
  WHERE i.inhparent = parent $$;
-- (the bound as text: an integer, or the value of an organization numbered by uuid, block 18)
CREATE OR REPLACE FUNCTION lokadb.bound_orgs(parent regclass) RETURNS jsonb LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT coalesce(jsonb_object_agg(c.oid::text, coalesce(
      replace((regexp_match(pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES IN \(''((?:[^'']|'''')*)''(::[a-z ]+(\(\d+\))?)?\)$'))[1], '''''', ''''),
      (regexp_match(pg_get_expr(c.relpartbound, c.oid), '^FOR VALUES IN \((-?\d+)\)$'))[1])), '{}')
  FROM pg_foreign_table f JOIN pg_inherits i ON i.inhrelid = f.ftrelid JOIN pg_class c ON c.oid = f.ftrelid
  WHERE i.inhparent = parent $$;
-- tenant_kind: uuid when the organization column tc of table parent is a uuid, else int (from
-- the catalog: enable asks before it records the table); tenant_none: the key of rows of no
-- organization; tenant_key: the partition key (block 18).
CREATE OR REPLACE FUNCTION lokadb.tenant_kind(parent text, tc text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE (SELECT a.atttypid FROM pg_attribute a WHERE a.attrelid = to_regclass(lokadb.qrel(parent)) AND a.attname = tc)
    WHEN 'uuid'::regtype THEN 'uuid'
    WHEN 'text'::regtype THEN 'text' WHEN 'varchar'::regtype THEN 'text' WHEN 'bpchar'::regtype THEN 'text'
    ELSE 'int' END $$;
CREATE OR REPLACE FUNCTION lokadb.tenant_none(parent text, tc text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE lokadb.tenant_kind(parent, tc) WHEN 'uuid' THEN '''00000000-0000-0000-0000-000000000000''::uuid'
    WHEN 'text' THEN '''''' ELSE '-2' END $$;
CREATE OR REPLACE FUNCTION lokadb.tenant_key(parent text, tc text) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE lokadb.tenant_kind(parent, tc)
    WHEN 'text' THEN format('(coalesce(%I::text, %s) COLLATE "C")', tc, lokadb.tenant_none(parent, tc))
    ELSE format('coalesce(%I, %s)', tc, lokadb.tenant_none(parent, tc)) END $$;
CREATE OR REPLACE FUNCTION lokadb.tenant_policy(parent text, tc text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  key text := lokadb.tenant_key(parent, tc);
  -- the bounds of the range and how a foreign partition's bound is compared, by kind
  lo text := 'coalesce(lokadb.org(), -9223372036854775808)';
  hi text := 'coalesce(lokadb.org(), 9223372036854775807)';
  bound text := format('((SELECT lokadb.bound_orgs(%L::regclass)) ->> tableoid::text)::bigint = %s', lokadb.qrel(parent), key);
  r record;
BEGIN
  IF lokadb.tenant_kind(parent, tc) = 'uuid' THEN
    lo := 'lokadb.org_lo_uuid()';
    hi := 'lokadb.org_hi_uuid()';
    bound := format('((SELECT lokadb.bound_orgs(%L::regclass)) ->> tableoid::text) = %s::text', lokadb.qrel(parent), key);
  ELSIF lokadb.tenant_kind(parent, tc) = 'text' THEN
    lo := 'lokadb.org_lo_text()';
    hi := 'lokadb.org_hi_text()';
    bound := format('((SELECT lokadb.bound_orgs(%L::regclass)) ->> tableoid::text) = %s', lokadb.qrel(parent), key);
  END IF;
  -- the per-organization policies of an older move-org
  FOR r IN SELECT polname FROM pg_policy WHERE polrelid = lokadb.qrel(parent)::regclass AND polname LIKE 'lokadb\_bind\_org%' LOOP
    EXECUTE format('DROP POLICY %I ON %s', r.polname, lokadb.qrel(parent));
  END LOOP;
  EXECUTE format('DROP POLICY IF EXISTS lokadb_tenant ON %s', lokadb.qrel(parent));
  -- Next to the application's own policies (carried by enable) it narrows what they allow:
  -- restrictive. Alone it has to allow: permissive.
  -- tableoid is cast: the planner takes tableoid for a column of one value per table, so that
  -- "tableoid <> ALL (...)" matched no row to it, and the OR with the bound's equality rated the
  -- whole policy at 0.5 % of the rows. The statement of an organization of 20 000 rows was
  -- expected to return 5 and read all of them, sorted (review of performance, F7, the feed).
  -- An expression has no such statistics: the estimate is 95 %.
  EXECUTE format($f$CREATE POLICY lokadb_tenant ON %1$s AS %4$s USING (%2$s >= %5$s AND %2$s <= %6$s
    AND (tableoid::bigint <> ALL ((SELECT lokadb.foreign_parts(%3$L::regclass))::oid[]::bigint[]) OR %7$s))$f$,
    lokadb.qrel(parent), key, lokadb.qrel(parent),
    CASE WHEN EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = lokadb.qrel(parent)::regclass AND polname <> 'lokadb_tenant')
      THEN 'RESTRICTIVE' ELSE 'PERMISSIVE' END, lo, hi, bound);
END $$;
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT t.table_name, t.tenant_column FROM lokadb.tables t
      WHERE NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = to_regclass(lokadb.qrel(t.table_name)) AND p.polname = 'lokadb_tenant'
                          AND pg_get_expr(p.polqual, p.polrelid) LIKE '%foreign_parts%'
                          AND pg_get_expr(p.polqual, p.polrelid) LIKE '%bigint[]%') LOOP
    PERFORM lokadb.tenant_policy(r.table_name, r.tenant_column);
  END LOOP;
END $$;
-- key_stats(parent, tc, cloud): extended statistics on DEFAULT (T__cloud) of the organization
-- column and the partition key together. The planner takes "organization_id = 1" and the key's
-- range or equality (coalesce(organization_id, -2), the policy's, and the proxy's condition for
-- the partition) for independent: 0.05 x 0.05 for the one organization of 20, and the statement
-- of an organization of 20 000 rows was expected to return 50 (review of performance, F7). The
-- dependency and the combinations tell it they are one. Not on the organizations' own partitions
-- (one organization each: nothing to be correlated). Dropped by disable; ANALYZE once, when made.
CREATE OR REPLACE FUNCTION lokadb.key_stats(parent text, tc text, cloud text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  st text := lokadb.helper(parent, '__ks');
  c regclass := to_regclass(lokadb.qrel(cloud));
BEGIN
  IF c IS NULL OR EXISTS (SELECT FROM pg_statistic_ext WHERE stxrelid = c AND stxname = (parse_ident(lokadb.qrel(st)))[array_upper(parse_ident(lokadb.qrel(st)), 1)]) THEN
    RETURN;
  END IF;
  EXECUTE format('CREATE STATISTICS %s (mcv, dependencies) ON %I, (%s) FROM %s', lokadb.qrel(st), tc, lokadb.tenant_key(parent, tc), lokadb.qrel(cloud));
  EXECUTE format('ANALYZE %s', lokadb.qrel(cloud));
END $$;
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT table_name, tenant_column, cloud_partition FROM lokadb.tables LOOP
    PERFORM lokadb.key_stats(r.table_name, r.tenant_column, r.cloud_partition);
  END LOOP;
END $$;
-- sync_grants(parent): each foreign partition of parent gets the privileges the parent has
-- (SELECT, INSERT, UPDATE, DELETE, of the table and of its columns) and no others. The proxy
-- sends a statement of one organization in its own database straight to its partition (the
-- parent's RLS filter cannot be shipped there); without them a role that may read the
-- parent got permission denied for documents__org1. A foreign partition holds the rows of its
-- one organization, which such a role reads through the parent all the same. Not for the
-- cloud's local partitions: DEFAULT holds every cloud organization, the policy that keeps
-- them apart is the parent's (enable revokes them there).
CREATE OR REPLACE FUNCTION lokadb.sync_grants(parent text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  p oid := lokadb.qrel(parent)::regclass;
  part record;
  g record;
BEGIN
  FOR part IN SELECT c.oid, c.oid::regclass::text AS name, c.relowner FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = p AND c.relkind = 'f' LOOP
  BEGIN
    -- the table's privileges: (grantee, privilege) of the partition but not of the parent go
    FOR g IN (SELECT coalesce(quote_ident(r.rolname), 'PUBLIC') AS role, x.privilege_type AS priv
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE c.oid = part.oid AND x.grantee <> part.relowner AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
      EXCEPT SELECT coalesce(quote_ident(r.rolname), 'PUBLIC'), x.privilege_type
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE c.oid = p) LOOP
      EXECUTE format('REVOKE %s ON %s FROM %s', g.priv, part.name, g.role);
    END LOOP;
    FOR g IN (SELECT coalesce(quote_ident(r.rolname), 'PUBLIC') AS role, x.privilege_type AS priv
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE c.oid = p AND x.grantee <> part.relowner AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
      EXCEPT SELECT coalesce(quote_ident(r.rolname), 'PUBLIC'), x.privilege_type
        FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE c.oid = part.oid) LOOP
      EXECUTE format('GRANT %s ON %s TO %s', g.priv, part.name, g.role);
    END LOOP;
    -- the same for the columns, matched by name
    FOR g IN (SELECT a.attname AS col, coalesce(quote_ident(r.rolname), 'PUBLIC') AS role, x.privilege_type AS priv
        FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE a.attrelid = part.oid AND x.grantee <> part.relowner AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE')
      EXCEPT SELECT a.attname, coalesce(quote_ident(r.rolname), 'PUBLIC'), x.privilege_type
        FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE a.attrelid = p) LOOP
      EXECUTE format('REVOKE %s (%I) ON %s FROM %s', g.priv, g.col, part.name, g.role);
    END LOOP;
    FOR g IN (SELECT a.attname AS col, coalesce(quote_ident(r.rolname), 'PUBLIC') AS role, x.privilege_type AS priv
        FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE a.attrelid = p AND x.grantee <> part.relowner AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE')
          AND EXISTS (SELECT 1 FROM pg_attribute pa WHERE pa.attrelid = part.oid AND pa.attname = a.attname AND NOT pa.attisdropped)
      EXCEPT SELECT a.attname, coalesce(quote_ident(r.rolname), 'PUBLIC'), x.privilege_type
        FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles r ON r.oid = x.grantee
        WHERE a.attrelid = part.oid) LOOP
      EXECUTE format('GRANT %s (%I) ON %s TO %s', g.priv, g.col, part.name, g.role);
    END LOOP;
  EXCEPTION WHEN insufficient_privilege THEN
    -- a role that cannot grant on it: the proxy reads such a partition through the parent
    RAISE WARNING 'lokadb: the privileges of % are not copied to %: %', parent, part.name, SQLERRM;
  END;
  END LOOP;
END $$;
-- sync_mappings: postgres_fdw reads an organization's database with the user mapping of the
-- current role. move-org maps the roles that had privileges then; a role granted one later
-- read nothing (no user mapping found). Each server of LokaDB foreign partitions gets a
-- mapping for every role that has a privilege on a parent of them, with the options of the
-- mapping of the partitions' owner (the credentials move-org set), and loses the mappings
-- of roles that have none any more. Never FOR PUBLIC (every role with USAGE on the server
-- would see its password); the server's owner, the partitions' owner and PUBLIC are left
-- as they are. It takes no arguments: whoever calls it can only bring the mappings to what
-- the privileges say. Security definer: a mapping for another role is the server owner's
-- (move-org's role) to create, and only it may read the options.
CREATE OR REPLACE FUNCTION lokadb.sync_mappings() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
-- (the LokaDB tables are named public.<t>: the search path holds pg_catalog only)
DECLARE
  srv record;
  r record;
  opts text;
  readers oid[];
BEGIN
  FOR srv IN SELECT DISTINCT s.oid, s.srvname, s.srvowner, c.relowner
      FROM lokadb.tables t
      JOIN pg_inherits i ON i.inhparent = to_regclass(lokadb.qrel(t.table_name))
      JOIN pg_class c ON c.oid = i.inhrelid AND c.relkind = 'f'
      JOIN pg_foreign_table ft ON ft.ftrelid = c.oid
      JOIN pg_foreign_server s ON s.oid = ft.ftserver LOOP
  BEGIN
    SELECT string_agg(format('%I %L', split_part(o, '=', 1), substr(o, length(split_part(o, '=', 1)) + 2)), ', ') INTO opts
      FROM pg_user_mappings m CROSS JOIN LATERAL unnest(m.umoptions) o
      WHERE m.srvid = srv.oid AND m.umuser = srv.relowner;
    IF opts IS NULL THEN
      RAISE WARNING 'lokadb: server %: no user mapping of the owner of its foreign tables to copy', srv.srvname;
      CONTINUE;
    END IF;
    -- the roles that may read or write a parent with a foreign partition on this server
    readers := ARRAY(SELECT DISTINCT a.oid FROM pg_roles a
      CROSS JOIN (SELECT DISTINCT i.inhparent AS parent FROM pg_inherits i JOIN pg_foreign_table ft ON ft.ftrelid = i.inhrelid
                  WHERE ft.ftserver = srv.oid) p
      WHERE a.rolname NOT LIKE 'pg\_%'
        AND (has_table_privilege(a.oid, p.parent, 'SELECT, INSERT, UPDATE, DELETE')
             OR has_any_column_privilege(a.oid, p.parent, 'SELECT, INSERT, UPDATE')));
    FOR r IN SELECT x AS role FROM unnest(readers) x
        WHERE NOT EXISTS (SELECT 1 FROM pg_user_mappings m WHERE m.srvid = srv.oid AND m.umuser = x) LOOP
      EXECUTE format('CREATE USER MAPPING FOR %I SERVER %I OPTIONS (%s)', pg_get_userbyid(r.role), srv.srvname, opts);
    END LOOP;
    FOR r IN SELECT m.umuser FROM pg_user_mappings m
        WHERE m.srvid = srv.oid AND m.umuser <> 0 AND m.umuser NOT IN (srv.srvowner, srv.relowner)
          AND m.umuser <> ALL (readers) LOOP
      EXECUTE format('DROP USER MAPPING FOR %I SERVER %I', pg_get_userbyid(r.umuser), srv.srvname);
    END LOOP;
  EXCEPTION
    WHEN insufficient_privilege THEN
      RAISE WARNING 'lokadb: the user mappings of server % are not synced: %', srv.srvname, SQLERRM;
    WHEN OTHERS THEN
      -- re-raised here: the server's log shows an error that reached the top with the statement
      -- it came from, and that statement is CREATE USER MAPPING ... OPTIONS (password '...')
      RAISE EXCEPTION 'lokadb: the user mappings of server % are not synced: %', srv.srvname, SQLERRM USING ERRCODE = SQLSTATE;
  END;
  END LOOP;
END $$;
-- sync_refs: what enable gives the roles granted a LokaDB table, for a role granted one after
-- it: USAGE on schema lokadb (the RLS policy calls lokadb.org()) and the parent's privileges on
-- its refs table (the proxy reads it as the session's role, the refs trigger writes it as
-- the writer). Only adds: a child table's reader needs SELECT on its parent's refs too.
-- Security definer like sync_mappings (the grantor holds USAGE on lokadb without grant option).
CREATE OR REPLACE FUNCTION lokadb.sync_refs() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE g record;
BEGIN
  FOR g IN SELECT DISTINCT coalesce(quote_ident(r.rolname), 'PUBLIC') AS role, x.privilege_type AS priv, t.refs_table
      FROM (SELECT table_name, refs_table FROM lokadb.tables UNION ALL SELECT table_name, refs_table FROM lokadb.unique_refs) t
      JOIN pg_class c ON c.oid = to_regclass(lokadb.qrel(t.table_name))
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x
      LEFT JOIN pg_roles r ON r.oid = x.grantee
      WHERE x.grantee <> c.relowner AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
        AND to_regclass(lokadb.qrel(t.refs_table)) IS NOT NULL LOOP
  BEGIN
    EXECUTE format('GRANT USAGE ON SCHEMA lokadb TO %s', g.role);
    EXECUTE format('GRANT %s ON %s TO %s', g.priv, lokadb.qrel(g.refs_table), g.role);
  EXCEPTION WHEN insufficient_privilege THEN
    RAISE WARNING 'lokadb: % is not granted % on %: %', g.role, g.priv, g.refs_table, SQLERRM;
  END;
  END LOOP;
END $$;
-- grant_sync: a GRANT or REVOKE on LokaDB tables (the proxy sends it so), then their foreign
-- partitions get the parent's privileges again, and the roles that may read them a user
-- mapping on the organizations' servers.
CREATE OR REPLACE FUNCTION lokadb.grant_sync(stmt text, parents text[]) RETURNS void LANGUAGE plpgsql AS $$
DECLARE t text;
BEGIN
  EXECUTE stmt;
  FOREACH t IN ARRAY parents LOOP
    PERFORM lokadb.sync_grants(t);
  END LOOP;
  PERFORM lokadb.sync_refs();
  PERFORM lokadb.sync_mappings();
END $$;
-- Only the LokaDB tables' owner may call them (through the proxy's GRANT), and enable and
-- move-org (the functions' owner): enable grants it EXECUTE.
REVOKE ALL ON FUNCTION lokadb.sync_mappings() FROM PUBLIC;
REVOKE ALL ON FUNCTION lokadb.sync_refs() FROM PUBLIC;
REVOKE ALL ON FUNCTION lokadb.grant_sync(text, text[]) FROM PUBLIC;
-- An installation moved before sync_grants: its foreign partitions get the parent's privileges.
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT t.table_name FROM lokadb.tables t WHERE to_regclass(lokadb.qrel(t.table_name)) IS NOT NULL LOOP
    PERFORM lokadb.sync_grants(r.table_name);
  END LOOP;
  PERFORM lokadb.sync_mappings();
END $$;
-- prep_moved_log, prep_moved (A3 of the admin review): between the switch's check of the child
-- tables' organizations and its lock, the application may move a parent to another
-- organization, and the children (a copy of the parent's organization that enable -prepare
-- keeps) stay behind. enable -prepare puts lokadb_prep_moved on such a parent: a transaction
-- that changes its organization leaves its id here, visible once it commits. The switch reads
-- prep_moved_count() before its check and after its lock (a read each: it has no time to check
-- the children again with the tables locked) and refuses when it moved. One row per
-- transaction, no row lock shared between them (a counter would make two moves wait on each
-- other, and deadlock with the parent rows). A security definer function: the application's
-- role needs no right on the table.
-- The log must not grow for as long as enable is not run: prep_moved_prune takes the rows older
-- than an interval once the table holds more than a limit, and counts them in prep_moved_pruned,
-- so that the count (rows + pruned) only ever grows: a row pruned while the switch checks cannot
-- hide a row added. Only one transaction prunes (a try lock: the others go on, none waits).
CREATE TABLE IF NOT EXISTS lokadb.prep_moved_log (xid bigint PRIMARY KEY, at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS lokadb.prep_moved_pruned (n bigint NOT NULL);
INSERT INTO lokadb.prep_moved_pruned SELECT 0 WHERE NOT EXISTS (SELECT FROM lokadb.prep_moved_pruned);
CREATE OR REPLACE FUNCTION lokadb.prep_moved_count() RETURNS bigint LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp AS $$
  SELECT (SELECT count(*) FROM lokadb.prep_moved_log) + (SELECT coalesce(sum(n), 0) FROM lokadb.prep_moved_pruned) $$;
CREATE OR REPLACE FUNCTION lokadb.prep_moved_prune(keep bigint, older interval) RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE pruned bigint;
BEGIN
  IF (SELECT count(*) FROM lokadb.prep_moved_log) <= keep OR NOT pg_try_advisory_xact_lock(hashtext('lokadb.prep_moved_prune')) THEN
    RETURN 0;
  END IF;
  WITH d AS (DELETE FROM lokadb.prep_moved_log WHERE at < now() - older RETURNING 1),
       u AS (UPDATE lokadb.prep_moved_pruned SET n = n + (SELECT count(*) FROM d) RETURNING 1)
    SELECT count(*) INTO pruned FROM d;
  RETURN pruned;
END $$;
REVOKE ALL ON FUNCTION lokadb.prep_moved_prune(bigint, interval) FROM PUBLIC;
CREATE OR REPLACE FUNCTION lokadb.prep_moved() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  INSERT INTO lokadb.prep_moved_log (xid) VALUES (txid_current()) ON CONFLICT DO NOTHING;
  -- the estimate of the planner (updated by autovacuum): no count of a table that may be large
  IF (SELECT reltuples FROM pg_class WHERE oid = 'lokadb.prep_moved_log'::regclass) > 100000 THEN
    PERFORM lokadb.prep_moved_prune(100000, interval '7 days');
  END IF;
  RETURN NULL;
END $$;
-- The parent trigger of an older enable fires on UPDATE only (tgtype bit 4: INSERT).
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT g.tgrelid::regclass AS rel, pg_get_triggerdef(g.oid) AS def FROM lokadb.tables t
      JOIN pg_trigger g ON g.tgrelid = to_regclass(lokadb.qrel(t.table_name)) AND g.tgname = 'lokadb_parent'
      WHERE g.tgtype & 4 = 0 LOOP
    EXECUTE format('DROP TRIGGER lokadb_parent ON %s', r.rel);
    EXECUTE replace(r.def, 'BEFORE UPDATE OF', 'BEFORE INSERT OR UPDATE OF');
  END LOOP;
END $$;
-- The refs trigger of an older enable fires on INSERT and DELETE only (tgtype bit 16: UPDATE).
DO $$ DECLARE r record; BEGIN
  FOR r IN SELECT t.table_name, t.tenant_column, t.refs_table, t.key_column FROM lokadb.tables t
      JOIN pg_trigger g ON g.tgrelid = to_regclass(lokadb.qrel(t.table_name)) AND g.tgname = 'lokadb_refs'
      WHERE g.tgtype & 16 = 0 LOOP
    EXECUTE format('DROP TRIGGER lokadb_refs ON %s', lokadb.qrel(r.table_name));
    EXECUTE format('CREATE TRIGGER lokadb_refs AFTER INSERT OR DELETE OR UPDATE OF %I, %I ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.refs_sync(%L, %L, %L)',
      r.key_column, r.tenant_column, lokadb.qrel(r.table_name), r.refs_table, r.tenant_column, r.key_column);
  END LOOP;
  -- the trigger updates refs as the writer: an older enable gave writers no UPDATE on it
  FOR r IN SELECT DISTINCT t.refs_table, coalesce(quote_ident(g.rolname), 'PUBLIC') AS role
      FROM lokadb.tables t JOIN pg_class c ON c.oid = to_regclass(lokadb.qrel(t.table_name))
      CROSS JOIN LATERAL aclexplode(c.relacl) x LEFT JOIN pg_roles g ON g.oid = x.grantee
      WHERE x.privilege_type = 'UPDATE' AND x.grantee <> c.relowner
        AND NOT has_table_privilege(coalesce(g.rolname::text, 'public'), to_regclass(lokadb.qrel(t.refs_table)), 'UPDATE') LOOP
    EXECUTE format('GRANT UPDATE ON %s TO %s', lokadb.qrel(r.refs_table), r.role);
  END LOOP;
END $$;
-- drop_invalid_copies drops dst's copies of src's invalid indexes: CREATE TABLE ... (LIKE src
-- INCLUDING ALL) copies an index left invalid by a failed CREATE INDEX CONCURRENTLY as a valid
-- one, which then enforces what the application never had (a unique one refuses its rows).
CREATE OR REPLACE FUNCTION lokadb.drop_invalid_copies(src text, dst text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r record;
  victim regclass;
BEGIN
  FOR r IN SELECT i.indisunique AS u, regexp_replace(pg_get_indexdef(i.indexrelid), '^.* USING ', '') AS d
      FROM pg_index i WHERE i.indrelid = lokadb.qrel(src)::regclass AND NOT i.indisvalid LOOP
    SELECT i.indexrelid::regclass INTO victim FROM pg_index i
      WHERE i.indrelid = lokadb.qrel(dst)::regclass AND i.indisunique = r.u
        AND regexp_replace(pg_get_indexdef(i.indexrelid), '^.* USING ', '') = r.d
        AND NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conindid = i.indexrelid)
      ORDER BY i.indexrelid DESC LIMIT 1;
    IF victim IS NOT NULL THEN
      EXECUTE format('DROP INDEX %s', victim);
    END IF;
  END LOOP;
END $$;

-- partition_org gives an organization its own partition <parent>__o<org>, moving its rows
-- out of the DEFAULT partition (move-org's first step: enable leaves every row in DEFAULT).
-- org NULL: the partition of rows of no organization, <parent>__none, for the key -2 (the
-- key is coalesce(<tenant column>, -2); the nil uuid for an organization numbered by uuid);
-- enable makes it. val: the organization's value (block 18, lokadb.tenants), org's own for an
-- organization numbered by integers.
-- Indexes come from the DEFAULT partition (LIKE ... INCLUDING ALL), FKs are copied from it,
-- triggers are cloned from the parent at ATTACH. DEFAULT is locked for the whole split, so
-- reads and writes that touch it wait until COMMIT, and ATTACH scans DEFAULT: the cost grows
-- with the organization's rows and the DEFAULT partition. Callers should set lock_timeout.
DROP FUNCTION IF EXISTS lokadb.partition_org(text, bigint); -- now with val (block 18)
CREATE OR REPLACE FUNCTION lokadb.partition_org(parent text, org bigint, val text DEFAULT NULL) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
  tc text := (SELECT tenant_column FROM lokadb.tables WHERE table_name = parent);
  def text := lokadb.helper(parent, '__cloud');
  part text := lokadb.helper(parent, coalesce('__o' || org, '__none'));
  -- the partition's bound: the organization's value, or the key of rows of none
  bnd text := CASE WHEN org IS NULL THEN lokadb.tenant_none(parent, tc) ELSE quote_literal(coalesce(val, org::text)) END;
  -- the key expression: the index on DEFAULT's key (<parent>__key) finds the rows
  which text := format('%s = %s', lokadb.tenant_key(parent, tc), bnd);
  -- generated columns are computed again by the new partition: Postgres refuses values for them
  cols text := (SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) FROM pg_attribute
                WHERE attrelid = lokadb.qrel(def)::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = '');
  r record;
  trg text[];
  mode text[];
BEGIN
  -- The FKs copied below lock the tables they reference (SHARE ROW EXCLUSIVE): a writer that
  -- inserted a parent row (its refs row) and now inserts here would wait for us while we wait
  -- for it. Taking them first makes us wait for such a writer instead.
  FOR r IN SELECT DISTINCT confrelid::regclass::text AS t FROM pg_constraint WHERE conrelid = lokadb.qrel(def)::regclass AND contype = 'f' ORDER BY 1 LOOP
    EXECUTE format('LOCK TABLE %s IN SHARE ROW EXCLUSIVE MODE', r.t);
  END LOOP;
  -- Writes to the parent wait for the split: ATTACH takes only SHARE UPDATE EXCLUSIVE on the
  -- parent, and an INSERT routed before it would put a row of this organization into DEFAULT
  -- and fail its new partition constraint. EXCLUSIVE lets reads through. The parent first,
  -- then DEFAULT: the order writers lock them in (the other order deadlocks with them).
  EXECUTE format('LOCK TABLE %s IN EXCLUSIVE MODE', lokadb.qrel(parent));
  -- Without the lock a row written between the copy and the delete would be lost. ATTACH
  -- needs this lock anyway: taking it first avoids a lock upgrade (and deadlocks with writers).
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', lokadb.qrel(def));
  IF to_regclass(lokadb.qrel(part)) IS NOT NULL THEN RETURN part; END IF;
  EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING ALL)', lokadb.qrel(part), lokadb.qrel(def));
  PERFORM lokadb.drop_invalid_copies(def, part);
  FOR r IN SELECT conname FROM pg_constraint WHERE conrelid = lokadb.qrel(part)::regclass AND conname LIKE 'lokadb_not_org%' LOOP
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', lokadb.qrel(part), r.conname);
  END LOOP;
  FOR r IN SELECT conname, pg_get_constraintdef(oid) AS d FROM pg_constraint WHERE conrelid = lokadb.qrel(def)::regclass AND contype = 'f' LOOP
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s', lokadb.qrel(part), r.conname, r.d);
  END LOOP;
  -- Deferred FK checks (Django declares FKs DEFERRABLE INITIALLY DEFERRED) would leave
  -- pending trigger events on the new table, and ATTACH refuses such a table.
  -- enable points every FK onto a LokaDB table at its refs table: one onto DEFAULT itself
  -- (missed by an older enable) would cascade into, or leave dangling, other organizations' rows
  IF EXISTS (SELECT 1 FROM pg_constraint WHERE contype = 'f' AND confrelid = lokadb.qrel(def)::regclass) THEN
    RAISE EXCEPTION 'lokadb: % is referenced by a foreign key that enable did not point at the refs table: moving rows would fire it', def
      USING ERRCODE = '0A000';
  END IF;
  SET CONSTRAINTS ALL IMMEDIATE;
  PERFORM set_config('lokadb.moving', 'on', true); -- the refs keep pointing at the moved rows
  -- The application's triggers (cloned from the parent) must not see the move as a DELETE;
  -- the new partition gets its own clones only at ATTACH.
  -- Each is switched back to the mode it had (origin, replica or always).
  SELECT array_agg(tgname::text), array_agg(tgenabled::text) INTO trg, mode FROM pg_trigger
    WHERE tgrelid = lokadb.qrel(def)::regclass AND NOT tgisinternal AND tgenabled <> 'D';
  FOR i IN 1 .. coalesce(array_length(trg, 1), 0) LOOP
    EXECUTE format('ALTER TABLE %s DISABLE TRIGGER %I', lokadb.qrel(def), trg[i]);
  END LOOP;
  -- One statement: exactly the rows deleted are the rows copied.
  EXECUTE format('WITH moved AS (DELETE FROM %s WHERE %s RETURNING %s) INSERT INTO %s (%s) SELECT %s FROM moved',
    lokadb.qrel(def), which, cols, lokadb.qrel(part), cols, cols);
  FOR i IN 1 .. coalesce(array_length(trg, 1), 0) LOOP
    EXECUTE format('ALTER TABLE %s ENABLE %s TRIGGER %I', lokadb.qrel(def),
      CASE mode[i] WHEN 'R' THEN 'REPLICA' WHEN 'A' THEN 'ALWAYS' ELSE '' END, trg[i]);
  END LOOP;
  PERFORM set_config('lokadb.moving', 'off', true);
  EXECUTE format('ALTER TABLE %s OWNER TO %I', lokadb.qrel(part), (SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = lokadb.qrel(parent)::regclass));
  EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES IN (%s)', lokadb.qrel(parent), lokadb.qrel(part), bnd);
  RETURN part;
END $$;

-- org_lock is the advisory lock key of an organization: the proxy holds it shared for every
-- statement on LokaDB tables (org NULL: a statement not confined to one real organization),
-- move-org holds it exclusive while it splits the organization out of DEFAULT. The
-- organization pauses, the others do not.
CREATE OR REPLACE FUNCTION lokadb.org_lock(org bigint) RETURNS bigint LANGUAGE sql IMMUTABLE
  SET search_path = pg_catalog, pg_temp AS $$ SELECT CASE WHEN org IS NULL THEN hashtextextended('lokadb all organizations', 0)
                    ELSE hashtextextended('lokadb org ' || org, 0) END $$;
-- org_lock_v: the same by the organization's value as text (block 18: a uuid; an integer as
-- text gets the same key). A name of its own, not an overload of org_lock: org_lock($1) with
-- an untyped parameter (an older lokadb's) would then take text.
DROP FUNCTION IF EXISTS lokadb.org_lock(text);
CREATE OR REPLACE FUNCTION lokadb.org_lock_v(org text) RETURNS bigint LANGUAGE sql IMMUTABLE
  SET search_path = pg_catalog, pg_temp AS $$ SELECT CASE WHEN org IS NULL THEN hashtextextended('lokadb all organizations', 0)
                    ELSE hashtextextended('lokadb org ' || org, 0) END $$;

-- split_org (an earlier build's way to split an organization out of DEFAULT, never called; the split is
-- the move-org's own: split.go) is gone from the databases that have it
DROP FUNCTION IF EXISTS lokadb.split_org(text, bigint, int, int);
-- create_index runs a CREATE INDEX template on every local partition: a unique index
-- cannot be created on a parent that has foreign partitions.
CREATE OR REPLACE FUNCTION lokadb.create_index(parent text, ddl text, idxname text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT c.relname, c.oid::regclass::text AS rel FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
           WHERE i.inhparent = lokadb.qrel(parent)::regclass AND c.relkind = 'r' LOOP
    EXECUTE replace(replace(ddl, '__lokadb_part__', r.rel),
                    '__lokadb_idx__', quote_ident(lokadb.helper_name(left(coalesce(nullif(idxname, ''), 'idx'), 30) || '__' || r.relname, '')));
  END LOOP;
END $$;

-- The proxy reads the metadata under the client's role (block 9, B9.12): every role may read
-- what routing needs. Nothing there is beyond the catalog: table and column names, the
-- organizations' ids, their FDW servers and schemas. The refs tables keep the grants of their
-- tables (enable), the move's log and state stay the owner's.
GRANT USAGE ON SCHEMA lokadb TO PUBLIC;
GRANT SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs, lokadb.tenants TO PUBLIC;
-- a database a move touched before: its log function is the capture's alone (captureSQL)
DO $$ BEGIN
  IF to_regprocedure('lokadb.move_log_entry(bigint,text,"char",text,text,jsonb,text)') IS NOT NULL THEN
    REVOKE EXECUTE ON FUNCTION lokadb.move_log_entry(bigint, text, "char", text, text, jsonb, text) FROM PUBLIC;
  END IF;
  IF to_regprocedure('lokadb.move_log_entry(bigint,text,"char",bigint,bigint,jsonb,text)') IS NOT NULL THEN
    REVOKE EXECUTE ON FUNCTION lokadb.move_log_entry(bigint, text, "char", bigint, bigint, jsonb, text) FROM PUBLIC;
  END IF;
END $$;

-- block 7 (DDL replay): replay_targets is where the proxy replays the DDL of LokaDB tables:
-- every moved organization (lokadb.orgs), its schema there, the options of its FDW server
-- (the address) and of a user mapping of it (the -customer-app role, which owns the
-- organization's tables). The proxy runs as the application's role, which sees no user
-- mapping's options (no USAGE on the server): the definer reads them. Only the owner of the
-- LokaDB tables may call it (enable grants it): who may change their schema in the cloud gets
-- the role that changes it in the organizations' databases. The proxy refuses it to clients.
CREATE OR REPLACE FUNCTION lokadb.replay_targets() RETURNS TABLE (org bigint, org_schema text, server_options text[], mapping_options text[])
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
  SELECT o.org, o.schema, s.srvoptions,
    (SELECT um.umoptions FROM pg_user_mappings um WHERE um.srvid = s.oid AND um.umoptions IS NOT NULL
     ORDER BY um.usename = current_user DESC, um.usename LIMIT 1)
  FROM lokadb.orgs o LEFT JOIN pg_foreign_server s ON s.srvname = o.server
  ORDER BY o.org
$$;
REVOKE ALL ON FUNCTION lokadb.replay_targets() FROM PUBLIC;
` + fkCloudSQL + fkCascadeSQL

// unsupported refuses a table whose behaviour enable would silently change. The table
// becomes the DEFAULT partition of a new parent: its triggers move to the parent, views and
// SQL functions are pointed at the parent, identity columns become sequences, its row
// security, rules and materialized views are made again (carryOver). Left are triggers
// Postgres does not allow there: a constraint trigger cannot be on a foreign table (the
// partition of a moved organization), a row trigger with transition tables not on a
// partitioned table, and a statement trigger with them cannot collect the rows of a foreign
// partition.
func (r *runner) unsupported(t string) error {
	rows, err := r.rows(`SELECT CASE WHEN tgconstraint <> 0 THEN 'constraint trigger ' ELSE 'trigger with transition tables ' END || tgname
		FROM pg_trigger WHERE tgrelid = $1::regclass AND NOT tgisinternal AND (tgconstraint <> 0 OR tgoldtable IS NOT NULL OR tgnewtable IS NOT NULL)`, qr(t))
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		var what []string
		for _, row := range rows {
			what = append(what, row[0])
		}
		return fmt.Errorf("%s: has %s: Postgres allows neither on a table with foreign partitions (where moved organizations live), so lokadb cannot carry them over", t, strings.Join(what, ", "))
	}
	return nil
}

// fk: a foreign key of a spec table or onto one. src, dst: the raw names; srcSQL: how SQL
// names the source table (regclass text: quoted, schema-qualified outside the search path);
// cols, refs: its columns and the ones they reference, in order; srcLokadb, dstLokadb: the
// table is a spec table (in public).
type fk struct {
	name, src, dst, def, srcSQL string
	cols, refs                  []string
	srcLokadb, dstLokadb        bool
	srcEnabled                  bool // src is the DEFAULT partition of a table enabled before
	// uref: the unique column of dst, other than its key, the FK references alone (block 4,
	// item 2): it points at the column's map (urefTable) instead of the refs table
	uref string
}

// urefTable: the map of unique column col of table t (value -> organization), kept like the
// refs table by a trigger of its own (urefTrigger) and recorded in lokadb.unique_refs.
func urefTable(t, col string) string { return hn(t, "__refs__"+col) }

func urefTrigger(col string) string { return rel.HelperName("lokadb_refs__"+col, "") }

// RefsColumns maps the columns an FK onto a LokaDB table references to the columns of its
// refs table: the key to id, the organization column to tenant. ok is false for any other
// column (a unique column other than the key): the refs table has nothing for it.
// A composite key (compkey.go) maps column by column to id_1 ... id_n, and the FK must
// reference all of them.
func RefsColumns(refs []string, key, tenant string) (out []string, ok bool) {
	kc, rc := KeyCols(key), RefsKeyCols(key)
	used := 0
	for _, c := range refs {
		switch i := slices.Index(kc, c); {
		case i >= 0:
			out = append(out, rc[i])
			used++
		case c == tenant && len(refs) > 1:
			out = append(out, "tenant")
		default:
			return nil, false
		}
	}
	return out, used == len(kc)
}

// refRe matches the target of an FK onto dst; group 2: the referenced columns.
var refRe = func(dst string) *regexp.Regexp {
	s, t := SplitKey(dst)
	sp := `(public\.)?`
	if s != "public" {
		sp = `("?` + regexp.QuoteMeta(s) + `"?\.)`
	}
	return regexp.MustCompile(`REFERENCES\s+` + sp + `"?` + regexp.QuoteMeta(t) + `"?\(([^)]*)\)`)
}

// Enable converts the spec's tables into LIST-partitioned tables (tenant column as key): the
// existing table becomes the DEFAULT partition of a new parent of the same name, so no row is
// copied and the table keeps its indexes, constraints and statistics. Every organization
// stays in DEFAULT until move-org gives it a partition of its own (lokadb.partition_org).
// Around it: a refs table per table for incoming FKs and id->tenant lookups, and an RLS
// policy driven by lokadb.org.
func Enable(ctx context.Context, dsn string, spec Spec, owner string, log *slog.Logger) error {
	return EnableWith(ctx, EnableOptions{DSN: dsn, Spec: spec, Owner: owner, Log: log})
}

// EnableOptions: see Enable and Prepare.
type EnableOptions struct {
	DSN, Owner string
	Spec       Spec
	// LockTimeout: the switch after Prepare waits at most this long for its locks, and is
	// tried again (Attempts in all). Zero: 5 s.
	LockTimeout time.Duration
	Attempts    int
	Log         *slog.Logger
	// AllowDowntime: enable without Prepare of tables that take longer than maxUnpreparedDowntime
	// to convert under the lock is refused unless this is set (the CLI's -allow-downtime).
	AllowDowntime bool
}

// EnableWith is Enable. After Prepare it is the switch: it checks what Prepare made, and the
// tables are locked only for catalog changes, however many rows they hold. Without Prepare
// it does all the work under the lock, and says how long that is expected to take.
func EnableWith(ctx context.Context, o EnableOptions) error {
	if o.LockTimeout == 0 {
		o.LockTimeout = 5 * time.Second
	}
	for attempt := 1; ; attempt++ {
		err := enableOnce(ctx, o)
		var pe *pgconn.PgError
		if err == nil || !errors.As(err, &pe) || pe.Code != "55P03" || attempt >= max(o.Attempts, 1) {
			return err
		}
		o.Log.Info("the switch did not get its locks in time: trying again", "attempt", attempt, "lock_timeout", o.LockTimeout)
		time.Sleep(time.Second)
	}
}

func enableOnce(ctx context.Context, o EnableOptions) error {
	spec, owner, log := o.Spec, o.Owner, o.Log
	conn, err := connectPlain(ctx, o.DSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	tx, err := conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r := &runner{ctx: ctx, tx: tx, log: log}
	// no statement of enable, prepared or not, waits for a lock without a limit: past it the
	// transaction ends, gives back every lock it holds, and EnableWith tries again
	if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", o.LockTimeout.Milliseconds())); err != nil {
		return err
	}

	if err := r.exec(baseSQL); err != nil {
		return err
	}
	p, err := r.plan(spec)
	if err != nil {
		return err
	}
	tenant, key, todo := p.tenant, p.key, p.todo
	fks, lookups, err := r.fks(p)
	if err != nil {
		return err
	}
	prepared, err := r.preparedFor(p)
	if err != nil {
		return err
	}
	if prepared {
		if err := r.verifyPrepared(p, fks, o.LockTimeout); err != nil {
			return err
		}
	} else if err := r.warnDowntime(p, o.AllowDowntime); err != nil {
		return err
	}
	for _, l := range lookups {
		if err := r.exec(fmt.Sprintf("INSERT INTO lokadb.lookups VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", lit(l[0]), lit(l[1]), lit(l[2]))); err != nil {
			return err
		}
	}
	for _, f := range fks {
		// prepared: an FK from a LokaDB table to a cloud table stays where it is (the table
		// becomes DEFAULT with it), one onto a LokaDB table has its replacement onto the refs
		// table, validated by enable -prepare
		if prepared && !f.dstLokadb {
			continue
		}
		if err := r.exec(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", f.srcSQL, q(f.name))); err != nil {
			return err
		}
	}

	var tablePubs [][4]string // publication, table, its DEFAULT, column list and row filter
	for _, st := range todo {
		t, tc := st.Name, tenant[st.Name]
		cloud, refs := hn(t, "__cloud"), hn(t, "__refs")
		log.Info("converting", "table", t, "tenant_column", tc)
		if err := r.unsupported(t); err != nil {
			return err
		}
		// Privileges of other roles are re-granted on the new table: the old one is dropped.
		grants, err := r.rows(`SELECT coalesce(quote_ident(g.rolname), 'PUBLIC'), a.privilege_type, a.is_grantable::text
			FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
			WHERE c.oid = $1::regclass AND a.grantee <> c.relowner`, qr(t))
		if err != nil {
			return err
		}
		// and privileges on columns: the column, the privilege, the role, grantable
		colGrants, err := r.rows(`SELECT quote_ident(a.attname), x.privilege_type, coalesce(quote_ident(g.rolname), 'PUBLIC'), x.is_grantable::text
			FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid CROSS JOIN LATERAL aclexplode(a.attacl) x
			LEFT JOIN pg_roles g ON g.oid = x.grantee
			WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND x.grantee <> c.relowner
			ORDER BY a.attnum, 2, 3`, qr(t))
		if err != nil {
			return err
		}

		if st.TenantColumn == "" && !prepared {
			if err := r.exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", qr(t), q(tc), tenantType(p.tenantKind[t]))); err != nil {
				return err
			}
			// From the parent's refs table (key -> organization): filled above in this run, or
			// by the enable that converted the parent.
			if err := r.exec(fmt.Sprintf("UPDATE %s c SET %s = p.tenant FROM %s p WHERE p.id = c.%s",
				qr(t), q(tc), qr(hn(st.ParentTable, "__refs")), q(st.ParentColumn))); err != nil {
				return err
			}
			// Rows without a parent, or under a parent of no organization, keep NULL: they
			// stay in the cloud (<table>__none, made below).
		}

		// Defaults must be evaluated by the parent (rows are routed to a partition with the
		// parent's values), and an identity column of the partition would refuse them: every
		// identity becomes a plain sequence default, set before the rename so that LIKE
		// carries it to the parent.
		idents, err := r.rows(`SELECT attname::text, pg_get_serial_sequence($1, attname) FROM pg_attribute
			WHERE attrelid = $1::regclass AND attidentity <> '' AND NOT attisdropped ORDER BY attnum`, qr(t))
		if err != nil {
			return err
		}
		orig, err := r.rows(`SELECT pg_get_userbyid(c.relowner)::text,
			(SELECT coalesce(jsonb_agg(jsonb_build_object('col', a.attname, 'generated', a.attidentity::text,
				'start', s.seqstart, 'increment', s.seqincrement, 'min', s.seqmin, 'max', s.seqmax,
				'cache', s.seqcache, 'cycle', s.seqcycle) ORDER BY a.attnum), '[]')
			  FROM pg_attribute a JOIN pg_sequence s ON s.seqrelid = pg_get_serial_sequence($1, a.attname)::regclass
			  WHERE a.attrelid = c.oid AND a.attidentity <> '' AND NOT a.attisdropped)::text,
			CASE WHEN c.relforcerowsecurity THEN 'forced' WHEN c.relrowsecurity THEN 'on' ELSE 'off' END
			FROM pg_class c WHERE c.oid = $1::regclass`, qr(t))
		if err != nil {
			return err
		}
		var seqs []string // the sequences behind the defaults, given to the owner
		keyIdentity := false
		for _, col := range idents {
			keyIdentity = keyIdentity || slices.Contains(KeyCols(key[t]), col[0])
			seq := hn(t, "__"+col[0]+"_seq")
			if col[0] == "id" {
				seq = hn(t, "__id_seq")
			}
			// The new sequence continues the identity's, not max(col): values of deleted rows
			// are never handed out again.
			next, err := r.rows(fmt.Sprintf(`SELECT greatest((SELECT coalesce(max(%s), 0) + 1 FROM %s),
				CASE WHEN is_called THEN last_value + 1 ELSE last_value END)::text FROM %s`, q(col[0]), qr(t), col[1]))
			if err != nil {
				return err
			}
			for _, s := range []string{
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP IDENTITY", qr(t), q(col[0])),
				fmt.Sprintf("CREATE SEQUENCE %s", qr(seq)),
				fmt.Sprintf("SELECT setval(%s, %s, false)", lit(qr(seq)), next[0][0]),
				fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval(%s)", qr(t), q(col[0]), lit(qr(seq))),
			} {
				if err := r.exec(s); err != nil {
					return err
				}
			}
			seqs = append(seqs, qr(seq))
		}
		identitySeqs := len(seqs) // they need USAGE for writers; identity needed none
		for _, kc := range KeyCols(key[t]) {
			if keyIdentity {
				break
			}
			serial, err := r.rows(`SELECT coalesce(pg_get_serial_sequence($1, $2), '')`, qr(t), kc)
			if err != nil {
				return err
			}
			if serial[0][0] != "" {
				seqs = append(seqs, serial[0][0]) // a serial key keeps its sequence
			}
		}

		// What depends on the table by its OID and must be pointed at the parent: the
		// application's triggers (they move to the parent, which clones them to every
		// partition, the foreign ones included), views, rules and SQL-standard functions
		// (replaced with their own definitions, now resolving the name to the parent).
		trgs, err := r.rows(`SELECT tgname::text, pg_get_triggerdef(oid), tgenabled::text FROM pg_trigger
			WHERE tgrelid = $1::regclass AND NOT tgisinternal AND tgname NOT LIKE 'lokadb\_prep\_%' ORDER BY tgname`, qr(t))
		if err != nil {
			return err
		}
		deps, err := Dependents(r.rows, t)
		if err != nil {
			return err
		}
		if err := unmovable(t, deps); err != nil {
			return err
		}

		// A publication holds the table by OID: it would go on publishing only DEFAULT. The
		// parent takes its place, with the same columns and row filter.
		pubs, err := r.rows(`SELECT quote_ident(p.pubname),
				coalesce(' (' || (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) FROM pg_attribute a
					WHERE a.attrelid = pr.prrelid AND a.attnum = ANY (pr.prattrs)) || ')', '')
				|| coalesce(' WHERE (' || pg_get_expr(pr.prqual, pr.prrelid) || ')', '')
			FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
			WHERE pr.prrelid = $1::regclass ORDER BY 1`, qr(t))
		if err != nil {
			return err
		}

		steps := []string{
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qr(t), q(nameOf(cloud))),
			// The key is coalesce(tc, -2) (the nil uuid for organizations numbered by uuid): rows
			// of no organization (NULL) get a key of their own, the policy below keeps its range
			// (pruning) and still lets them through.
			fmt.Sprintf("CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING GENERATED) PARTITION BY LIST (%s)",
				qr(t), qr(cloud), tenantKey(tc, p.tenantKind[t])),
		}
		for _, g := range trgs {
			steps = append(steps, fmt.Sprintf("DROP TRIGGER %s ON %s", q(g[0]), qr(cloud)))
		}
		steps = append(steps,
			// No row is copied: the table itself becomes the DEFAULT partition. With no other
			// partition, ATTACH has nothing to check.
			fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s DEFAULT", qr(t), qr(cloud)),
		)
		for _, p := range pubs {
			tablePubs = append(tablePubs, [4]string{p[0], t, cloud, p[1]}) // moved after the loop
		}
		if prepared {
			// the refs table is filled and the column of a child too; their triggers on the
			// table (now DEFAULT) hand over to the parent's
			steps = append(steps, fmt.Sprintf("DROP TRIGGER %s ON %s", prepRefsTrigger, qr(cloud)),
				fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", prepTenantTrigger, qr(cloud)),
				fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", prepTenantAfter, qr(cloud)),
				fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", prepMovedTrigger, qr(cloud)),
				"DELETE FROM lokadb.prep_moved_log",
				fmt.Sprintf("DELETE FROM lokadb.prepare WHERE table_name = %s", lit(t)))
		} else {
			steps = append(steps,
				// tenant NULL: a row of no organization
				// id of the key's type: bigint for an integer, uuid, text (block 17)
				// a composite key in id_1 ... id_n (compkey.go)
				fmt.Sprintf("CREATE TABLE %s %s", qr(refs), refsDDL(key[t], p.refsType[t], tenantType(p.tenantKind[t]))),
				fmt.Sprintf("INSERT INTO %s SELECT %s, %s FROM %s", qr(refs), colList(key[t], ""), q(tc), qr(cloud)))
		}
		steps = append(steps,
			fmt.Sprintf("CREATE TRIGGER lokadb_refs AFTER INSERT OR DELETE OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.refs_sync(%s, %s, %s)",
				trigCols(key[t], tc), qr(t), lit(refs), lit(tc), lit(key[t])),
		)
		for _, g := range trgs {
			steps = append(steps, g[1]) // its definition names the table: now the parent
			switch g[2] {
			case "D":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qr(t), q(g[0])))
			case "R":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE REPLICA TRIGGER %s", qr(t), q(g[0])))
			case "A":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE ALWAYS TRIGGER %s", qr(t), q(g[0])))
			}
		}
		steps = append(steps, depSteps(deps)...)
		carried, err := r.carryOver(t, cloud, owner)
		if err != nil {
			return err
		}
		steps = append(steps, carried...)
		if st.TenantColumn == "" {
			// The ORM never mentions the new column: the proxy sets lokadb.org from the parent id.
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT %s", qr(t), q(tc), tenantDefault(p.tenantKind[t])),
				fmt.Sprintf("CREATE TRIGGER lokadb_parent BEFORE INSERT OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.parent_guard(%s, %s, %s, %s)",
					q(st.ParentColumn), qr(t), lit(hn(st.ParentTable, "__refs")), lit(st.ParentColumn), lit(tc), lit(t)))
		}
		steps = append(steps,
			fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", qr(t)),
			fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", qr(t)),
			fmt.Sprintf("SELECT lokadb.tenant_policy(%s, %s)", lit(t), lit(tc)),
			fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(t), q(owner)),
			fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(cloud), q(owner)),
			fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(refs), q(owner)),
			fmt.Sprintf("INSERT INTO lokadb.tables (table_name, tenant_column, refs_table, cloud_partition, key_column, orig_owner, identities, orig_rls) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
				lit(t), lit(tc), lit(refs), lit(cloud), lit(key[t]), lit(orig[0][0]), lit(orig[0][1]), lit(orig[0][2])),
			fmt.Sprintf("INSERT INTO lokadb.lookups VALUES (%s, %s, NULL), (%s, %s, %s)", lit(t), lit(tc), lit(t), lit(key[t]), lit(refs)),
			fmt.Sprintf("SELECT lokadb.key_stats(%s, %s, %s)", lit(t), lit(tc), lit(cloud)),
		)
		for _, s := range seqs {
			steps = append(steps, fmt.Sprintf("ALTER SEQUENCE %s OWNER TO %s", s, q(owner)))
		}
		for _, g := range grants {
			opt := ""
			if g[2] == "true" {
				opt = " WITH GRANT OPTION"
			}
			steps = append(steps, fmt.Sprintf("GRANT %s ON %s TO %s%s", g[1], qr(t), g[0], opt),
				// The partition is read through the parent only: a grant on it would pass by the
				// organization policy, which lives on the parent.
				fmt.Sprintf("REVOKE %s ON %s FROM %s", g[1], qr(cloud), g[0]),
				// the RLS policy calls lokadb.org()
				fmt.Sprintf("GRANT USAGE ON SCHEMA lokadb TO %s", g[0]))
			switch g[1] {
			case "SELECT", "INSERT", "UPDATE", "DELETE": // the refs trigger runs as the writer; the proxy reads refs as the reader
				steps = append(steps, fmt.Sprintf("GRANT %s ON %s TO %s", g[1], qr(refs), g[0]))
			}
			if g[1] == "INSERT" {
				// identity needed no sequence privilege, nextval() does
				for _, s := range seqs[:identitySeqs] {
					steps = append(steps, fmt.Sprintf("GRANT USAGE ON SEQUENCE %s TO %s", s, g[0]))
				}
			}
		}
		// The same for privileges on columns. The refs table needs what the refs trigger does
		// for the role: an INSERT of any column, an UPDATE of the id or the tenant column.
		for _, g := range colGrants {
			col, priv, role := g[0], g[1], g[2]
			opt := ""
			if g[3] == "true" {
				opt = " WITH GRANT OPTION"
			}
			steps = append(steps, fmt.Sprintf("GRANT %s (%s) ON %s TO %s%s", priv, col, qr(t), role, opt),
				fmt.Sprintf("REVOKE %s (%s) ON %s FROM %s", priv, col, qr(cloud), role),
				fmt.Sprintf("GRANT USAGE ON SCHEMA lokadb TO %s", role))
			if priv == "SELECT" || priv == "INSERT" || priv == "UPDATE" && (strings.Contains(", "+colList(key[t], "")+", ", ", "+col+", ") || col == q(tc)) {
				steps = append(steps, fmt.Sprintf("GRANT %s ON %s TO %s", priv, qr(refs), role))
			}
			if priv == "INSERT" {
				for _, s := range seqs[:identitySeqs] {
					steps = append(steps, fmt.Sprintf("GRANT USAGE ON SEQUENCE %s TO %s", s, role))
				}
			}
		}
		for _, s := range steps {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}

	// FKs cannot live on a parent that will get foreign partitions: they go on the DEFAULT
	// partition, and partition_org copies them to every organization's partition.
	// FKs onto a unique column other than the key: that column's map, filled from the table
	// (its rows of every organization still in the cloud) and kept by its trigger
	for _, dst := range slices.Sorted(maps.Keys(p.urefs)) {
		for _, u := range slices.Sorted(maps.Keys(p.urefs[dst])) {
			m, tc := urefTable(dst, u), tenant[dst]
			for _, st := range []string{
				fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id %s PRIMARY KEY, tenant %s, UNIQUE (tenant, id))", qr(m), p.urefs[dst][u], tenantType(p.tenantKind[dst])),
				fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(m), q(owner)),
				fmt.Sprintf("INSERT INTO %s SELECT %s, %s FROM %s WHERE %s IS NOT NULL ON CONFLICT DO NOTHING", qr(m), q(u), q(tc), qr(dst), q(u)),
				fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(urefTrigger(u)), qr(dst)),
				fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR DELETE OR UPDATE OF %s, %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.refs_sync(%s, %s, %s)",
					q(urefTrigger(u)), q(u), q(tc), qr(dst), lit(m), lit(tc), lit(u)),
				fmt.Sprintf("INSERT INTO lokadb.unique_refs VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", lit(dst), lit(u), lit(m)),
			} {
				if err := r.exec(st); err != nil {
					return err
				}
			}
		}
	}
	for _, f := range fks {
		src, def := f.srcSQL, f.def
		if f.srcLokadb && !f.srcEnabled {
			src = qr(hn(f.src, "__cloud"))
		}
		if f.dstLokadb {
			var cols []string
			if def, cols, err = refsDef(f, p); err != nil {
				return err
			}
			if f.srcLokadb {
				refs := hn(f.dst, "__refs")
				if f.uref != "" {
					refs = urefTable(f.dst, f.uref)
				}
				if col, ok := fkLookup(f.cols, cols); ok {
					if err := r.exec(fmt.Sprintf("INSERT INTO lokadb.lookups VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
						lit(f.src), lit(col), lit(refs))); err != nil {
						return err
					}
				}
			}
		}
		add := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", src, q(f.name), def)
		if prepared && !f.dstLokadb {
			continue // left in place
		} else if prepared {
			// enable -prepare added and validated it next to the old one: it takes its name
			add = fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s", src, q(prepFK(f.name)), q(f.name))
		}
		if err := r.exec(add); err != nil {
			return err
		}
	}
	// the FKs onto cloud tables, for the organizations moved (fkcloud.go)
	if err := r.exec("SELECT lokadb.fk_cloud_sync()"); err != nil {
		return err
	}
	// ON DELETE CASCADE between LokaDB tables for the organizations moved (fkcascade.go)
	if err := r.exec("SELECT lokadb.fk_cascade_sync()"); err != nil {
		return err
	}
	if err := applyRouting(r, spec); err != nil {
		return err
	}
	// Rows of no organization get a partition of their own: statements of an organization
	// then prune it like any other (in DEFAULT they would scan DEFAULT). Organizations get
	// partitions of their own only when they move (move-org's first step).
	for _, st := range todo {
		if err := r.exec(fmt.Sprintf("SELECT lokadb.partition_org(%s, NULL)", lit(st.Name))); err != nil {
			return err
		}
	}
	// A publication of a LokaDB table publishes it through the parent (publish_via_partition_root):
	// under the table's own name, as before enable, rather than as <table>__cloud, __none and
	// __o<N>, whose rows a move takes out of DEFAULT would look deleted. The option is the
	// publication's: it is set only when no other partitioned table is published by it.
	// Before the parent replaces DEFAULT in it (below): Postgres takes a column list or a row
	// filter of a partitioned table only with publish_via_partition_root.
	pubs, err := r.rows(`WITH bt AS (SELECT to_regclass(lokadb.qrel(table_name)) AS oid FROM lokadb.tables
			UNION SELECT to_regclass(lokadb.qrel(cloud_partition)) FROM lokadb.tables)
		SELECT quote_ident(p.pubname), coalesce((SELECT string_agg(c.oid::regclass::text, ', ' ORDER BY 1) FROM pg_class c
			WHERE c.relkind = 'p' AND c.oid NOT IN (SELECT oid FROM bt)
			  AND (p.puballtables OR c.oid IN (SELECT prrelid FROM pg_publication_rel WHERE prpubid = p.oid)
			    OR c.relnamespace IN (SELECT pnnspid FROM pg_publication_namespace WHERE pnpubid = p.oid))), ''), p.pubname::text
		FROM pg_publication p WHERE NOT p.pubviaroot AND p.pubname NOT LIKE 'lokadb\_%'
		  AND (p.puballtables OR EXISTS (SELECT 1 FROM pg_publication_rel WHERE prpubid = p.oid AND prrelid IN (SELECT oid FROM bt))
		    OR EXISTS (SELECT 1 FROM pg_publication_namespace WHERE pnpubid = p.oid AND pnnspid IN (SELECT relnamespace FROM pg_class WHERE oid IN (SELECT oid FROM bt))))
		ORDER BY 1`)
	if err != nil {
		return err
	}
	for _, p := range pubs {
		if p[1] != "" {
			log.Warn("the publication also publishes other partitioned tables: it publishes the LokaDB tables as their partitions (<table>__cloud, __none, __o<N>)",
				"publication", p[0], "partitioned", p[1])
			continue
		}
		for _, s := range []string{
			fmt.Sprintf("INSERT INTO lokadb.publications VALUES (%s, false) ON CONFLICT DO NOTHING", lit(p[2])),
			fmt.Sprintf("ALTER PUBLICATION %s SET (publish_via_partition_root = true)", p[0]),
		} {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}
	viaRoot, err := r.rows(`SELECT quote_ident(pubname) FROM pg_publication WHERE pubviaroot`)
	if err != nil {
		return err
	}
	root := map[string]bool{}
	for _, v := range viaRoot {
		root[v[0]] = true
	}
	for _, tp := range tablePubs {
		pub, t, cloud, extra := tp[0], tp[1], tp[2], tp[3]
		if extra != "" && !root[pub] {
			// The parent without them would publish columns or rows the publication left out.
			log.Warn("the publication has a column list or a row filter for the table, and publishes other partitioned tables: it keeps publishing DEFAULT (<table>__cloud) only; set publish_via_partition_root = true on it and run enable again, or publish the parent yourself",
				"publication", pub, "table", t)
			continue
		}
		for _, s := range []string{
			fmt.Sprintf("ALTER PUBLICATION %s DROP TABLE %s", pub, qr(cloud)),
			fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s%s", pub, qr(t), extra),
		} {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}
	for _, s := range []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA lokadb TO %s", q(owner)),
		// block 7: the proxy replays the owner's DDL in the organizations' databases (the
		// owner may read their -customer-app credentials: a decision, see README, security)
		fmt.Sprintf("GRANT EXECUTE ON FUNCTION lokadb.replay_targets() TO %s", q(owner)),
		// block 2: a GRANT or REVOKE of the owner through the proxy (lokadb.grant_sync)
		fmt.Sprintf("GRANT EXECUTE ON FUNCTION lokadb.grant_sync(text, text[]), lokadb.sync_refs(), lokadb.sync_mappings() TO %s", q(owner)),
		fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA lokadb TO %s", q(owner)),
		// proxies listening on lokadb_meta read the metadata again: sent at COMMIT only
		"NOTIFY lokadb_meta",
	} {
		if err := r.exec(s); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	// After the commit: CREATE INDEX CONCURRENTLY runs outside a transaction, and writes go on.
	for _, st := range spec.Tables {
		if err := buildKeyIndex(ctx, conn, hn(st.Name, "__cloud"), keyIndex(st.Name), tenantKey(tenant[st.Name], p.tenantKind[st.Name]), log); err != nil {
			return err
		}
	}
	return nil
}
