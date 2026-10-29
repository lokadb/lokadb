package admin

// FKs from LokaDB tables to cloud tables (block 4, item 8). enable leaves such an FK on the
// DEFAULT partition (<table>__cloud) and partition_org copies it to the local partitions, but
// a foreign partition (a moved organization's rows, in its own database) cannot have one, so
// it held for the organizations still in the cloud only. Two triggers stand in for it there:
//   - lokadb_fk_<FK> on each foreign partition of the LokaDB table: a row written for a moved
//     organization must reference an existing cloud row, which it locks FOR KEY SHARE, as the
//     FK would (a concurrent DELETE waits);
//   - lokadb_fk_<table>_<FK> on the cloud table applies the FK's ON DELETE / ON UPDATE action
//     to the moved organizations' rows. The cloud table in routing.cloud_tables: in the
//     organization of the row. Otherwise in every moved organization, if the fanout policy
//     lets statements run in every organization's database (lokadb.fanout, set by the proxy:
//     log or allow; reject-writes for RESTRICT and NO ACTION, which only read); and refused
//     while any organization is moved if it does not (reject, the default, also outside the
//     proxy). An organization's database that is down refuses the change, as a fanout would.
//
// Each runs as the owner of the table it reads or changes (one SECURITY DEFINER function per
// table), as Postgres runs FK checks and actions. Unlike the FK they are not deferrable: a
// DEFERRABLE FK is checked at the end of each statement for the moved organizations.
const fkCloudSQL = `
-- fk_cloud: FK con of <parent>__cloud onto a cloud table (no row: no such FK any more).
CREATE OR REPLACE FUNCTION lokadb.fk_cloud(parent text, con text, OUT ref regclass, OUT refcols text[],
    OUT cols text[], OUT deltype text, OUT updtype text) LANGUAGE sql STABLE AS $$
  SELECT c.confrelid::regclass,
    ARRAY(SELECT a.attname::text FROM unnest(c.confkey) WITH ORDINALITY k(n, o)
          JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n ORDER BY k.o),
    ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(n, o)
          JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.o),
    c.confdeltype::text, c.confupdtype::text
  FROM pg_constraint c WHERE c.conrelid = to_regclass(lokadb.qrel(lokadb.helper(parent, '__cloud'))) AND c.conname = con AND c.contype = 'f' $$;
-- shown: the table as a message names it: the bare name in public, schema.name elsewhere (the
-- definers' search_path is pg_catalog alone, so a regclass prints with its schema always)
CREATE OR REPLACE FUNCTION lokadb.shown(r regclass) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT CASE WHEN n.nspname = 'public' THEN pg_catalog.quote_ident(c.relname) ELSE pg_catalog.quote_ident(n.nspname) || '.' || pg_catalog.quote_ident(c.relname) END
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = r $$;
-- fk_match: "(x.a, x.b) = (k.a, k.b)" for the columns a, b
CREATE OR REPLACE FUNCTION lokadb.fk_match(x text, xcols text[], k text, kcols text[]) RETURNS text LANGUAGE sql IMMUTABLE AS $$
  SELECT '(' || string_agg(x || '.' || quote_ident(c), ', ' ORDER BY o) || ') = ('
    || (SELECT string_agg(k || '.' || quote_ident(d), ', ' ORDER BY p) FROM unnest(kcols) WITH ORDINALITY u(d, p)) || ')'
  FROM unnest(xcols) WITH ORDINALITY t(c, o) $$;
-- eq_op: the equality operator of type typ with its schema, OPERATOR(schema.=): the btree
-- default operator class's (a domain by its base type). Where there is none (enums, arrays,
-- records: operators of pg_catalog), plain =. The SECURITY DEFINER triggers of the FKs onto
-- cloud tables have only pg_catalog and pg_temp (last) on their search path (A16 of the admin review: a path with
-- public lets anyone who can write there, on Postgres 14 and older, put an operator or a
-- cast in front of lokadb's), and compare keys of a type from an extension (citext) by this.
CREATE OR REPLACE FUNCTION lokadb.eq_op(typ oid) RETURNS text LANGUAGE sql STABLE AS $$
  WITH RECURSIVE b(t, d) AS (SELECT typ, 0 UNION ALL SELECT y.typbasetype, b.d + 1 FROM b JOIN pg_type y ON y.oid = b.t WHERE y.typtype = 'd')
  SELECT coalesce((SELECT format('OPERATOR(%I.%s)', n.nspname, o.oprname)
    FROM pg_opclass c JOIN pg_am a ON a.oid = c.opcmethod AND a.amname = 'btree'
    JOIN pg_amop ap ON ap.amopfamily = c.opcfamily AND ap.amoplefttype = c.opcintype AND ap.amoprighttype = c.opcintype AND ap.amopstrategy = 3
    JOIN pg_operator o ON o.oid = ap.amopopr JOIN pg_namespace n ON n.oid = o.oprnamespace
    WHERE c.opcdefault AND c.opcintype = (SELECT t FROM b ORDER BY d DESC LIMIT 1) LIMIT 1), '=') $$;
-- fk_match_in: fk_match with the operator of each column's type in table tbl (xcols and kcols
-- name columns of tbl, the one the rows x and k are of): "x.a OPERATOR(s.=) k.a AND ..."
CREATE OR REPLACE FUNCTION lokadb.fk_match_in(x text, xcols text[], k text, kcols text[], tbl regclass) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT string_agg(x || '.' || quote_ident(c) || ' ' || lokadb.eq_op((SELECT a.atttypid FROM pg_attribute a WHERE a.attrelid = tbl AND a.attname = c))
    || ' ' || k || '.' || quote_ident(kcols[o]), ' AND ' ORDER BY o)
  FROM unnest(xcols) WITH ORDINALITY t(c, o) $$;
-- fk_values: the values of cols in row j (jsonb) under the names of names; NULL if one is NULL
-- (MATCH SIMPLE: such a row references nothing)
CREATE OR REPLACE FUNCTION lokadb.fk_values(j jsonb, cols text[], names text[]) RETURNS jsonb LANGUAGE sql IMMUTABLE AS $$
  SELECT CASE WHEN bool_or(j -> c IS NULL OR j -> c = 'null') THEN NULL
    ELSE jsonb_object_agg(names[o], j -> c) END
  FROM unnest(cols) WITH ORDINALITY t(c, o) $$;
-- fk_cloud_check: the check of a row written to partition part of parent (old: an UPDATE's).
CREATE OR REPLACE FUNCTION lokadb.fk_cloud_check(parent text, con text, part oid, new jsonb, old jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  f record;
  keys jsonb;
  hit boolean;
BEGIN
  -- local partitions (__cloud, __none, a move's __o<N>) have the FK itself
  IF (SELECT relkind FROM pg_class WHERE oid = part) <> 'f' THEN RETURN; END IF;
  SELECT * INTO f FROM lokadb.fk_cloud(parent, con);
  IF f.ref IS NULL THEN RETURN; END IF;
  keys := lokadb.fk_values(new, f.cols, f.refcols);
  IF keys IS NULL OR keys = lokadb.fk_values(old, f.cols, f.refcols) THEN RETURN; END IF;
  EXECUTE format('SELECT true FROM %s x, jsonb_populate_record(NULL::%s, $1) k WHERE %s FOR KEY SHARE OF x',
    f.ref, f.ref, lokadb.fk_match_in('x', f.refcols, 'k', f.refcols, f.ref)) INTO hit USING keys;
  IF hit IS NULL THEN
    RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"', parent, con
      USING ERRCODE = '23503', DETAIL = format('Key (%s)=(%s) is not present in table "%s" (checked by lokadb in the cloud for organization %s, moved to its own database).',
        array_to_string(f.cols, ', '), (SELECT string_agg(new ->> c, ', ' ORDER BY o) FROM unnest(f.cols) WITH ORDINALITY t(c, o)),
        lokadb.shown(f.ref), (lokadb.bound_orgs(to_regclass(lokadb.qrel(parent))) ->> part::text));
  END IF;
END $$;
-- fk_cloud_act: the FK's action on the moved organizations' rows when a row of the cloud
-- table is deleted (new NULL) or updated.
CREATE OR REPLACE FUNCTION lokadb.fk_cloud_act(parent text, con text, new jsonb, old jsonb) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  f record;
  mode text := coalesce(nullif(current_setting('lokadb.fanout', true), ''), 'reject');
  act text;
  keys jsonb;
  vals jsonb;
  tc text;
  parts regclass[];
  p regclass;
  hit boolean;
  assign text;
  hint text;
BEGIN
  SELECT * INTO f FROM lokadb.fk_cloud(parent, con);
  IF f.ref IS NULL THEN RETURN; END IF;
  keys := lokadb.fk_values(old, f.refcols, f.cols);
  IF keys IS NULL THEN RETURN; END IF;
  IF new IS NOT NULL THEN
    vals := lokadb.fk_values(new, f.refcols, f.cols);
    IF vals = keys THEN RETURN; END IF; -- the key did not change
  END IF;
  act := CASE WHEN new IS NULL THEN f.deltype ELSE f.updtype END;
  IF to_regclass('lokadb.routing') IS NOT NULL THEN
    SELECT r.column_name INTO tc FROM lokadb.routing r WHERE r.kind = 'cloud_table' AND to_regclass(lokadb.qrel(r.table_name)) = f.ref;
  END IF;
  -- the foreign partitions to look in: the row's organization's, or every one
  SELECT coalesce(array_agg(c.oid::regclass ORDER BY c.relname), '{}') INTO parts
    FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    WHERE i.inhparent = to_regclass(lokadb.qrel(parent)) AND c.relkind = 'f'
      AND (tc IS NULL OR CASE WHEN lokadb.tenant_kind(parent, (SELECT t.tenant_column FROM lokadb.tables t WHERE t.table_name = parent)) = 'int'
        THEN (lokadb.bound_orgs(to_regclass(lokadb.qrel(parent))) ->> c.oid::text)::bigint = (old ->> tc)::bigint
        -- an organization named by its value, a uuid or a string (block 18): both are its canonical text
        ELSE (lokadb.bound_orgs(to_regclass(lokadb.qrel(parent))) ->> c.oid::text) = rtrim(old ->> tc) END);
  IF parts = '{}' THEN RETURN; END IF;
  IF tc IS NULL AND NOT (mode IN ('log', 'allow') OR mode = 'reject-writes' AND act IN ('a', 'r')) THEN
    -- a table with a column that holds the organization of its rows can be declared; a shared one
    -- (users, linked to the organizations through a membership table) and a dictionary cannot
    IF EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = f.ref AND a.attnum > 0 AND NOT a.attisdropped
               AND a.attname IN (SELECT t.tenant_column FROM lokadb.tables t)) THEN
      hint := format('declare %s in routing.cloud_tables with the column that holds the organization of its rows, or set the fanout policy to log or allow', lokadb.shown(f.ref));
    ELSE
      hint := format('%s has no column that holds the organization of its rows, so it cannot be declared in routing.cloud_tables: set the fanout policy to log or allow, allow only the reviewed statement (an allow-list entry for it in the proxy config), or delete softly in the application', lokadb.shown(f.ref));
    END IF;
    RAISE EXCEPTION 'lokadb: % on table "%" is refused: its rows are referenced by "%" (foreign key "%") in the databases of the moved organizations, and the fanout policy (%) does not let lokadb check them in every one; %',
      CASE WHEN new IS NULL THEN 'delete' ELSE 'update of the key' END, lokadb.shown(f.ref), parent, con, mode, hint
      USING ERRCODE = '0A000';
  END IF;
  FOREACH p IN ARRAY parts LOOP
    IF act IN ('a', 'r') THEN
      EXECUTE format('SELECT true FROM %s x, jsonb_populate_record(NULL::%s, $1) k WHERE %s LIMIT 1',
        p, p, lokadb.fk_match_in('x', f.cols, 'k', f.cols, p)) INTO hit USING keys;
      IF hit THEN
        RAISE EXCEPTION 'update or delete on table "%" violates foreign key constraint "%" on table "%"', lokadb.shown(f.ref), con, parent
          USING ERRCODE = '23503', DETAIL = format('Key (%s)=(%s) is still referenced from table "%s" by organization %s, moved to its own database.',
            array_to_string(f.refcols, ', '), (SELECT string_agg(old ->> c, ', ' ORDER BY o) FROM unnest(f.refcols) WITH ORDINALITY t(c, o)),
            parent, (lokadb.bound_orgs(to_regclass(lokadb.qrel(parent))) ->> p::oid::text));
      END IF;
    ELSIF act = 'c' AND new IS NULL THEN
      EXECUTE format('DELETE FROM %s x USING jsonb_populate_record(NULL::%s, $1) k WHERE %s',
        p, p, lokadb.fk_match_in('x', f.cols, 'k', f.cols, p)) USING keys;
    ELSE
      -- CASCADE of an UPDATE: the new key; SET NULL; SET DEFAULT
      SELECT string_agg(format('%I = %s', c, CASE act WHEN 'c' THEN 'n.' || quote_ident(c) WHEN 'n' THEN 'NULL' ELSE 'DEFAULT' END), ', ')
        INTO assign FROM unnest(f.cols) c;
      EXECUTE format('UPDATE %s x SET %s FROM jsonb_populate_record(NULL::%s, $1) k, jsonb_populate_record(NULL::%s, $2) n WHERE %s',
        p, assign, p, p, lokadb.fk_match_in('x', f.cols, 'k', f.cols, p)) USING keys, coalesce(vals, '{}');
    END IF;
  END LOOP;
END $$;
-- fk_cloud_fn: the name of the SECURITY DEFINER trigger function of kind (check, act) for
-- table t, running as its owner
CREATE OR REPLACE FUNCTION lokadb.fk_cloud_fn(kind text, t regclass) RETURNS text LANGUAGE sql STABLE AS $$
  SELECT 'fk_' || kind || '__' || left(c.relname, 40) || '_' || left(md5(t::text), 8) FROM pg_class c WHERE c.oid = t $$;
-- fk_cloud_sync gives every FK from a LokaDB table to a cloud table its two triggers and drops
-- those of FKs gone. enable and move-org call it (move-org also right after it attaches the
-- organization's foreign partition); it changes nothing that is in place.
CREATE OR REPLACE FUNCTION lokadb.fk_cloud_sync() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r record;
  fn text;
  trg text;
  p regclass;
BEGIN
  FOR r IN SELECT g.oid, g.tgname, g.tgrelid::regclass AS rel, string_to_array(encode(g.tgargs, 'escape'), '\000') AS args
      FROM pg_trigger g JOIN pg_proc p ON p.oid = g.tgfoid
      WHERE p.pronamespace = 'lokadb'::regnamespace AND p.proname ~ '^fk_(check|act)__' AND g.tgparentid = 0 LOOP
    IF (SELECT f.ref FROM lokadb.fk_cloud(r.args[1], r.args[2]) f) IS NULL THEN
      EXECUTE format('DROP TRIGGER %I ON %s', r.tgname, r.rel);
    END IF;
  END LOOP;
  FOR r IN SELECT t.table_name AS parent, c.conname::text AS con, c.confrelid::regclass AS ref,
      to_regclass(lokadb.qrel(t.table_name)) AS rel,
      pg_get_userbyid(d.relowner) AS ref_owner, pg_get_userbyid(s.relowner) AS owner,
      f.cols, f.refcols
    FROM lokadb.tables t
    JOIN pg_class s ON s.oid = to_regclass(lokadb.qrel(t.table_name))
    JOIN pg_constraint c ON c.conrelid = to_regclass(lokadb.qrel(t.cloud_partition)) AND c.contype = 'f'
    JOIN pg_class d ON d.oid = c.confrelid
    CROSS JOIN LATERAL lokadb.fk_cloud(t.table_name, c.conname::text) f
    -- onto a LokaDB table (its refs table): the organization's database has that FK itself
    WHERE NOT EXISTS (SELECT 1 FROM lokadb.tables b WHERE c.confrelid IN
            (to_regclass(lokadb.qrel(b.refs_table)), to_regclass(lokadb.qrel(b.table_name)), to_regclass(lokadb.qrel(b.cloud_partition))))
      AND NOT EXISTS (SELECT 1 FROM lokadb.unique_refs u WHERE c.confrelid = to_regclass(lokadb.qrel(u.refs_table)))
    ORDER BY 1, 2 LOOP
    -- the check, as the owner of the cloud table: it locks the referenced row
    fn := lokadb.fk_cloud_fn('check', r.ref);
    IF to_regprocedure('lokadb.' || quote_ident(fn) || '()') IS NULL THEN
      EXECUTE format($f$CREATE FUNCTION lokadb.%I() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $b$
        BEGIN
          PERFORM lokadb.fk_cloud_check(TG_ARGV[0], TG_ARGV[1], TG_RELID, to_jsonb(NEW), CASE TG_OP WHEN 'UPDATE' THEN to_jsonb(OLD) END);
          RETURN NULL;
        END $b$ $f$, fn);
    END IF;
    EXECUTE format('ALTER FUNCTION lokadb.%I() OWNER TO %I', fn, r.ref_owner);
    EXECUTE format('ALTER FUNCTION lokadb.%I() SET search_path = pg_catalog, pg_temp', fn);
    -- on each foreign partition, not on the parent: the cloud's partitions have the FK and
    -- their writes pay nothing
    trg := left('lokadb_fk_' || r.con, 63);
    FOR p IN SELECT i.inhrelid::regclass FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = r.rel AND c.relkind = 'f'
          AND NOT EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = i.inhrelid AND g.tgname = trg) LOOP
      EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.%I(%L, %L)',
        trg, (SELECT string_agg(quote_ident(c), ', ') FROM unnest(r.cols) c), p, fn, r.parent, r.con);
    END LOOP;
    -- the action, as the owner of the LokaDB table: it changes the organizations' rows
    fn := lokadb.fk_cloud_fn('act', r.rel);
    IF to_regprocedure('lokadb.' || quote_ident(fn) || '()') IS NULL THEN
      EXECUTE format($f$CREATE FUNCTION lokadb.%I() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $b$
        BEGIN
          PERFORM lokadb.fk_cloud_act(TG_ARGV[0], TG_ARGV[1], CASE TG_OP WHEN 'UPDATE' THEN to_jsonb(NEW) END, to_jsonb(OLD));
          RETURN NULL;
        END $b$ $f$, fn);
    END IF;
    EXECUTE format('ALTER FUNCTION lokadb.%I() OWNER TO %I', fn, r.owner);
    EXECUTE format('ALTER FUNCTION lokadb.%I() SET search_path = pg_catalog, pg_temp', fn);
    trg := left('lokadb_fk_' || r.parent || '_' || r.con, 63);
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = r.ref AND tgname = trg) THEN
      EXECUTE format('CREATE TRIGGER %I AFTER DELETE OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.%I(%L, %L)',
        trg, (SELECT string_agg(quote_ident(c), ', ') FROM unnest(r.refcols) c), r.ref, fn, r.parent, r.con);
    END IF;
  END LOOP;
END $$;
`
