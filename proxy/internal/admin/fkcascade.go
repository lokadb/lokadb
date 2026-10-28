package admin

// ON DELETE CASCADE between LokaDB tables for the moved organizations (block 5, after block 17's
// U2). In an organization's database the FK between its tables is a local FK, and it keeps its
// CASCADE: a parent deleted through the cloud (its foreign partition) takes its children with
// it there, where the cloud does not see them go, and their rows stayed in <child>__refs
// (refs-diff showed them stale, a new row of the same key in any organization failed on
// __refs). So before the parent goes, lokadb_fkcascade_<child>_… BEFORE DELETE on each foreign
// partition of the parent reads the keys of the children it is about to take (in the child's
// foreign partition of the same organization), takes them out of <child>__refs, and goes on
// the same way to their children. The organization's own CASCADE then deletes the rows.
//
// It only reads the organization's database. Deleting the children from the cloud as well
// deadlocked: the trigger runs as the child table's owner, whose user mapping is another
// connection of postgres_fdw than the deleting role's; the parent's DELETE there cascades to
// the children that the other connection deleted and has not committed, and both wait for the
// cloud's commit. One read of the organization's database per parent row (and per child row
// with children of its own). A child added at the same moment by another transaction may keep
// its __refs row (refs-sync).
//
// It runs as the owner of the child table (one SECURITY DEFINER function per child), as
// Postgres runs an FK's action: a role that may delete the parent and read nothing of the child
// deletes them both.
const fkCascadeSQL = `
-- fk_cascades: the FKs ON DELETE CASCADE onto the parent of foreign partition parent_part from
-- its LokaDB children (on <child>__cloud, onto <parent>__refs), each with the child's foreign
-- partition of the same organization (schema-qualified: the cascade's functions run with a
-- fixed search_path), the child's FK columns and the parent's columns behind them.
CREATE OR REPLACE FUNCTION lokadb.fk_cascades(parent_part regclass)
  RETURNS TABLE (child text, con text, child_part text, cols text[], pcols text[]) LANGUAGE sql STABLE AS $$
  SELECT ct.table_name, c.conname::text, quote_ident(cn.nspname) || '.' || quote_ident(cf.relname),
    ARRAY(SELECT a.attname::text FROM unnest(c.conkey) WITH ORDINALITY k(n, o)
          JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.o),
    -- <parent>__refs (id, tenant): the parent's key and tenant columns; a composite key's
    -- id_1 ... id_n are its columns (lokadb.key_cols)
    ARRAY(SELECT CASE WHEN a.attname = 'id' THEN pt.key_column WHEN a.attname = 'tenant' THEN pt.tenant_column
            WHEN a.attname ~ '^id_[0-9]+$' THEN (lokadb.key_cols(pt.key_column))[substr(a.attname, 4)::int] ELSE a.attname::text END
          FROM unnest(c.confkey) WITH ORDINALITY k(n, o)
          JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n ORDER BY k.o)
  FROM pg_inherits pi
  JOIN lokadb.tables pt ON pi.inhparent = to_regclass(lokadb.qrel(pt.table_name))
  JOIN pg_constraint c ON c.contype = 'f' AND c.confdeltype = 'c' AND c.confrelid = to_regclass(lokadb.qrel(pt.refs_table))
  JOIN lokadb.tables ct ON c.conrelid = to_regclass(lokadb.qrel(ct.table_name || '__cloud'))
  JOIN pg_inherits ci ON ci.inhparent = to_regclass(lokadb.qrel(ct.table_name))
  JOIN pg_class cf ON cf.oid = ci.inhrelid AND cf.relkind = 'f'
  JOIN pg_namespace cn ON cn.oid = cf.relnamespace
  WHERE pi.inhrelid = parent_part
    AND lokadb.bound_orgs(pi.inhparent) ->> parent_part::oid::text = lokadb.bound_orgs(ci.inhparent) ->> cf.oid::text $$;
-- fk_cascade_refs: the rows of child_part (child's foreign partition) whose cols hold the
-- parent's pvals leave <child>__refs, and so on for their own children. It reads the
-- organization's database and changes only the cloud's __refs.
CREATE OR REPLACE FUNCTION lokadb.fk_cascade_refs(child text, child_part text, cols text[], pvals jsonb) RETURNS void
  LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  t record;
  r jsonb;
  g record;
BEGIN
  SELECT b.refs_table, b.key_column INTO t FROM lokadb.tables b WHERE b.table_name = child;
  IF t IS NULL OR pvals IS NULL OR EXISTS (SELECT 1 FROM jsonb_each(pvals) e WHERE e.value = 'null'::jsonb) THEN
    RETURN;
  END IF;
  FOR r IN EXECUTE format('SELECT to_jsonb(x) FROM %s x, jsonb_populate_record(NULL::%s, $1) k WHERE %s',
      child_part, child_part, lokadb.fk_match_in('x', cols, 'k', cols, to_regclass(child_part))) USING pvals LOOP
    -- the row's key in its refs table: id, or id_1 ... id_n of a composite key
    EXECUTE format('DELETE FROM %s x USING jsonb_populate_record(NULL::%s, $1) k WHERE %s',
      lokadb.qrel(t.refs_table), lokadb.qrel(t.refs_table),
      (SELECT lokadb.fk_match_in('x', array_agg(n ORDER BY o), 'k', array_agg(n ORDER BY o), to_regclass(lokadb.qrel(t.refs_table)))
         FROM (SELECT CASE WHEN cardinality(lokadb.key_cols(t.key_column)) = 1 THEN 'id' ELSE 'id_' || o END AS n, o
               FROM unnest(lokadb.key_cols(t.key_column)) WITH ORDINALITY u(c, o)) z))
      USING (SELECT jsonb_object_agg(CASE WHEN cardinality(lokadb.key_cols(t.key_column)) = 1 THEN 'id' ELSE 'id_' || o END, r -> c)
             FROM unnest(lokadb.key_cols(t.key_column)) WITH ORDINALITY u(c, o));
    FOR g IN SELECT * FROM lokadb.fk_cascades(to_regclass(child_part)) LOOP
      PERFORM lokadb.fk_cascade_refs(g.child, g.child_part, g.cols,
        (SELECT jsonb_object_agg(u.c, r -> u.p) FROM unnest(g.cols, g.pcols) u(c, p)));
    END LOOP;
  END LOOP;
END $$;
-- fk_cascade_sync gives every foreign partition of a LokaDB parent the triggers of the FKs ON
-- DELETE CASCADE of its LokaDB children and drops those of FKs or partitions gone; it touches
-- only what changed. enable and move-org's switch call it.
CREATE OR REPLACE FUNCTION lokadb.fk_cascade_sync() RETURNS void LANGUAGE plpgsql AS $$
DECLARE
  r record;
  fn text;
  trg text;
  want text[] := '{}';
BEGIN
  FOR r IN SELECT p.oid::regclass AS parent_part, f.*, (SELECT relowner FROM pg_class WHERE oid = to_regclass(lokadb.qrel(f.child))) AS owner
      FROM pg_class p CROSS JOIN LATERAL lokadb.fk_cascades(p.oid::regclass) f WHERE p.relkind = 'f' LOOP
    fn := 'fkcascade__' || left(r.child, 40) || '_' || left(md5(r.child), 8);
    trg := 'lokadb_fkcascade_' || left(r.child, 30) || '_' || left(md5(r.con), 6);
    want := want || (r.parent_part::text || ' ' || trg);
    IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = fn AND pronamespace = 'lokadb'::regnamespace) THEN
      EXECUTE format($f$CREATE FUNCTION lokadb.%I() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $b$
BEGIN
  -- TG_ARGV: the child table, its foreign partition, its FK columns, the parent's columns they hold
  PERFORM lokadb.fk_cascade_refs(TG_ARGV[0], TG_ARGV[1], ARRAY(SELECT jsonb_array_elements_text(TG_ARGV[2]::jsonb)),
    (SELECT jsonb_object_agg(a.c, to_jsonb(OLD) -> b.p)
       FROM jsonb_array_elements_text(TG_ARGV[2]::jsonb) WITH ORDINALITY a(c, o)
       JOIN jsonb_array_elements_text(TG_ARGV[3]::jsonb) WITH ORDINALITY b(p, q) ON a.o = b.q));
  RETURN OLD;
END $b$ $f$, fn);
      EXECUTE format('REVOKE ALL ON FUNCTION lokadb.%I() FROM PUBLIC', fn);
    END IF;
    EXECUTE format('ALTER FUNCTION lokadb.%I() SET search_path = pg_catalog, pg_temp', fn); -- also where an older version made it
    -- as the owner of the child table, as an FK's action runs
    EXECUTE format('ALTER FUNCTION lokadb.%I() OWNER TO %I', fn, pg_get_userbyid(r.owner));
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = r.parent_part AND tgname = trg) THEN
      EXECUTE format('CREATE TRIGGER %I BEFORE DELETE ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.%I(%L, %L, %L, %L)',
        trg, r.parent_part, fn, r.child, r.child_part, to_jsonb(r.cols)::text, to_jsonb(r.pcols)::text);
    END IF;
  END LOOP;
  FOR r IN SELECT g.tgname::text AS tgname, g.tgrelid::regclass AS rel FROM pg_trigger g
      WHERE g.tgname LIKE 'lokadb\_fkcascade\_%' AND NOT g.tgisinternal LOOP
    IF NOT (r.rel::text || ' ' || r.tgname) = ANY (want) THEN
      EXECUTE format('DROP TRIGGER %I ON %s', r.tgname, r.rel);
    END IF;
  END LOOP;
END $$;
`
