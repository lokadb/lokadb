package admin

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// Routing declares what the database schema does not say about where a row's organization
// can be found (tasks/routing-declarations.md, section 2). The proxy reads the result as
// lookups (lokadb.lookups); lokadb.routing remembers which lookups came from declarations.
type Routing struct {
	Relations   []Relation   `json:"relations,omitempty"`
	CloudTables []CloudTable `json:"cloud_tables,omitempty"`
	Keys        []Key        `json:"keys,omitempty"`
}

// Key: a column of a LokaDB table unique across the whole table (a slug, a public token, an
// external id). enable keeps a map value -> organization (KeyTable) by a trigger, which also
// keeps the value unique across organizations; `WHERE column = $1` then names the organization.
type Key struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

// KeyTable: the map of a key, in schema lokadb (the proxy refuses the application access to it).
func KeyTable(table, column string) string { return rel.HelperName(table, "__key__"+column) }

// CloudTable: a table that stays in the cloud but knows the organization of its rows
// (projects.account_id). A statement joining it to LokaDB tables names its organization
// by its organization column or its id.
type CloudTable struct {
	Table        string `json:"table"`
	TenantColumn string `json:"tenant_column"`
}

// Relation: column From ("table.column") of a LokaDB table holds the id of a row of the
// LokaDB table To ("table.key": the table's key, its primary key column, usually id), although no FK says so (Rails, Laravel without foreign()).
// The proxy then finds the organization of `WHERE column = $1` and of an INSERT naming the
// column through To's refs table.
type Relation struct {
	From string `json:"from"`
	To   string `json:"to"`
	// Confirmed: the owner accepts rows whose parent is missing or belongs to another
	// organization (the data check found some). Statements by such a value find no row or
	// the parent's organization, not the row's.
	Confirmed bool `json:"confirmed,omitempty"`
}

// Parse splits the relation into its tables (keys: "schema.table" outside public) and columns,
// the last part of each side.
func (r Relation) Parse() (src, col, dst, key string, err error) {
	cut := func(s string) (string, string, bool) {
		i := strings.LastIndex(s, ".")
		if i < 0 {
			return "", "", false
		}
		return s[:i], s[i+1:], true
	}
	var ok1, ok2 bool
	src, col, ok1 = cut(r.From)
	dst, key, ok2 = cut(r.To)
	if !ok1 || !ok2 || src == "" || col == "" || dst == "" || key == "" {
		return "", "", "", "", fmt.Errorf("relation %s -> %s: want \"table.column\" -> \"table.key\" (\"schema.table.column\" outside public)", r.From, r.To)
	}
	return src, col, dst, key, nil
}

// PolymorphicType: the column that would say which table col (<x>_id) points at, <x>_type
// (Rails' polymorphic belongs_to); "" for a column not named <x>_id.
func PolymorphicType(col string) string {
	if x, ok := strings.CutSuffix(col, "_id"); ok && x != "" {
		return x + "_type"
	}
	return ""
}

// Merge adds the declarations of patch that the spec does not have yet (a patch from
// lokadb check -suggest).
func (s *Spec) Merge(patch *Routing) {
	if patch == nil {
		return
	}
	if s.Routing == nil {
		s.Routing = &Routing{}
	}
	for _, p := range patch.Relations {
		dup := false
		for _, r := range s.Routing.Relations {
			dup = dup || r.From == p.From
		}
		if !dup {
			s.Routing.Relations = append(s.Routing.Relations, p)
		}
	}
	for _, p := range patch.Keys {
		dup := false
		for _, k := range s.Routing.Keys {
			dup = dup || k == p
		}
		if !dup {
			s.Routing.Keys = append(s.Routing.Keys, p)
		}
	}
	for _, p := range patch.CloudTables {
		dup := false
		for _, c := range s.Routing.CloudTables {
			dup = dup || c.Table == p.Table
		}
		if !dup {
			s.Routing.CloudTables = append(s.Routing.CloudTables, p)
		}
	}
}

// RelationCheck is what the data says about a relation: of Rows rows with the column set,
// Found have a parent and OtherOrg have a parent of another organization (-1: not compared).
type RelationCheck struct {
	Rows     int64 `json:"rows"`
	Found    int64 `json:"parents_found"`
	OtherOrg int64 `json:"parents_of_another_organization"`
}

func (c RelationCheck) Clean() bool { return c.Found == c.Rows && c.OtherOrg <= 0 }

func (c RelationCheck) String() string {
	s := fmt.Sprintf("%d of %d rows have the parent", c.Found, c.Rows)
	if c.OtherOrg >= 0 {
		s += fmt.Sprintf(", %d of them in another organization", c.OtherOrg)
	}
	return s
}

// CheckRelation counts the rows of src whose col finds a row in parent (a table with the
// key column parentKey, or a refs table with id and, when tenant columns are given, the
// organization). srcTenant and
// parentTenant "" leave organizations uncompared.
func CheckRelation(ctx context.Context, tx pgx.Tx, src, col, srcTenant, parent, parentKey, parentTenant string) (RelationCheck, error) {
	other := "-1"
	if srcTenant != "" && parentTenant != "" {
		other = fmt.Sprintf("count(*) FILTER (WHERE p.%s IS DISTINCT FROM c.%s)", q(parentTenant), q(srcTenant))
	}
	var out RelationCheck
	// marked as the check's own: pg_stat_statements then does not count it as the application's
	err := tx.QueryRow(ctx, fmt.Sprintf(`/* lokadb check */ SELECT count(*), count(p.%s), %s
		FROM %s c LEFT JOIN %s p ON p.%s = c.%s WHERE c.%s IS NOT NULL`, q(parentKey), other, qr(src), qr(parent), q(parentKey), q(col), q(col))).
		Scan(&out.Rows, &out.Found, &out.OtherOrg)
	if other == "-1" {
		out.OtherOrg = -1
	}
	return out, err
}

const routingSQL = `CREATE TABLE IF NOT EXISTS lokadb.routing (kind text NOT NULL, table_name text NOT NULL,
  column_name text NOT NULL, target text NOT NULL, PRIMARY KEY (kind, table_name, column_name));
-- key_sync keeps a key's map (routing.keys): value -> organization. Its primary key keeps the
-- value unique across organizations, which no index of the partitioned table can.
CREATE OR REPLACE FUNCTION lokadb.key_sync() RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp AS $$
DECLARE
  was text;
  now text;
BEGIN
  IF lokadb.moving() THEN RETURN NULL; END IF;
  IF TG_OP <> 'INSERT' THEN
    was := to_jsonb(OLD) ->> TG_ARGV[1];
  END IF;
  IF TG_OP <> 'DELETE' THEN
    now := to_jsonb(NEW) ->> TG_ARGV[1];
  END IF;
  IF was IS NOT NULL THEN
    EXECUTE format('DELETE FROM lokadb.%I WHERE id = $1', TG_ARGV[0]) USING was;
  END IF;
  IF now IS NOT NULL THEN -- the organization of its own type: an integer or a uuid (block 18)
    EXECUTE format('INSERT INTO lokadb.%I (id, tenant) VALUES ($1, ($2).%I)', TG_ARGV[0], TG_ARGV[2])
      USING now, NEW;
  END IF;
  RETURN NULL;
END $$`

// applyRouting registers the spec's declarations as lookups (enable, routing apply) and
// drops the lookups of declarations the spec no longer has. Every LokaDB table is recorded.
func applyRouting(r *runner, spec Spec) error {
	if err := r.exec(routingSQL); err != nil {
		return err
	}
	tables := map[string]string{} // LokaDB table -> tenant column
	keys := map[string]string{}   // LokaDB table -> its key column
	rows, err := r.rows(`SELECT table_name, tenant_column, key_column FROM lokadb.tables`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		tables[row[0]], keys[row[0]] = row[1], row[2]
	}
	// the kind of the organizations (block 18): a cloud table's organization column is of it
	dbKind, err := r.dbTenantKind()
	if err != nil {
		return err
	}
	if dbKind == "" {
		dbKind = TenantInt
	}
	want := map[string]bool{}
	if spec.Routing != nil {
		for _, rel := range spec.Routing.Relations {
			src, col, dst, key, err := rel.Parse()
			if err != nil {
				return err
			}
			if tables[src] == "" || tables[dst] == "" {
				return fmt.Errorf("relation %s -> %s: both tables must be LokaDB tables", rel.From, rel.To)
			}
			if key != keys[dst] {
				return fmt.Errorf("relation %s -> %s: the organization of %s rows is found by its key %s.%s, not by %s", rel.From, rel.To, dst, dst, keys[dst], rel.To)
			}
			ok, err := r.rows(`SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped`, qr(src), col)
			if err != nil {
				return err
			}
			if len(ok) == 0 {
				return fmt.Errorf("relation %s -> %s: %s has no column %s", rel.From, rel.To, src, col)
			}
			if typ := PolymorphicType(col); typ != "" {
				poly, err := r.rows(`SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped`, qr(src), typ)
				if err != nil {
					return err
				}
				if len(poly) > 0 {
					return fmt.Errorf("relation %s -> %s: %s.%s says which table %s points at (a polymorphic reference): its ids of other tables would find their organization in %s. lokadb does not take a relation with a condition on the type yet",
						rel.From, rel.To, src, typ, col, dst)
				}
			}
			// The parent's refs table has every row of the parent, the moved organizations'
			// too; the rows of src are read through its parent table, the foreign partitions too.
			// enable -prepare checked it on the data outside the lock (prepare_routing.go)
			checked, err := r.rows(`DELETE FROM lokadb.routing WHERE kind = 'relation_checked' AND table_name = $1 AND column_name = $2 RETURNING 1`, src, col)
			if err != nil {
				return err
			}
			if len(checked) == 0 {
				chk, err := CheckRelation(r.ctx, r.tx, src, col, tables[src], hn(dst, "__refs"), "id", "tenant")
				if err != nil {
					return err
				}
				r.log.Info("relation", "from", rel.From, "to", rel.To, "data", chk.String())
				if !chk.Clean() && !rel.Confirmed {
					return fmt.Errorf("relation %s -> %s: %s; statements by %s would miss those rows. Fix the data, or confirm the relation (\"confirmed\": true) if that is acceptable",
						rel.From, rel.To, chk, rel.From)
				}
			}
			want[src+"."+col] = true
			added, err := r.rows(fmt.Sprintf(`INSERT INTO lokadb.lookups VALUES (%s, %s, %s) ON CONFLICT DO NOTHING RETURNING 1`,
				lit(src), lit(col), lit(hn(dst, "__refs"))))
			if err != nil {
				return err
			}
			if len(added) > 0 { // an FK may have registered the same lookup: that one is not ours
				if err := r.exec(fmt.Sprintf(`INSERT INTO lokadb.routing VALUES ('relation', %s, %s, %s) ON CONFLICT DO NOTHING`,
					lit(src), lit(col), lit(dst))); err != nil {
					return err
				}
			}
		}
	}
	implied, err := ImpliedCloudTables(r.rows, spec)
	if err != nil {
		return err
	}
	for i := 0; i < len(implied); {
		if tables[implied[i].Table] != "" { // a LokaDB table of an earlier enable, not in this spec
			implied = append(implied[:i], implied[i+1:]...)
			continue
		}
		i++
	}
	if spec.Routing != nil || len(implied) > 0 {
		var cts []CloudTable
		if spec.Routing != nil {
			cts = spec.Routing.CloudTables
		}
		for _, ct := range append(append([]CloudTable(nil), cts...), implied...) {
			if tables[ct.Table] != "" {
				return fmt.Errorf("cloud table %s: it is a LokaDB table", ct.Table)
			}
			// id: a key of any kind lokadb takes (block 17), the organization: an integer
			cols, err := r.rows(`SELECT a.attname::text, `+keyKindSQL+` FROM pg_attribute a
				WHERE a.attrelid = to_regclass($1) AND a.attname IN ('id', $2) AND a.attnum > 0 AND NOT a.attisdropped`, qr(ct.Table), ct.TenantColumn)
			if err != nil {
				return err
			}
			kind := map[string]string{}
			for _, c := range cols {
				kind[c[0]] = c[1]
			}
			if kind["id"] == "" || TenantOfKey(kind[ct.TenantColumn]) != dbKind {
				return fmt.Errorf("cloud table %s: it needs a column id of type %s and a column %s of the organizations' kind (%s)", ct.Table, KeyTypes, ct.TenantColumn, dbKind)
			}
			want["cloud:"+ct.Table] = true
			for _, s := range []string{
				fmt.Sprintf(`DELETE FROM lokadb.lookups WHERE table_name = %s`, lit(ct.Table)),
				cloudLookupsSQL(ct),
				fmt.Sprintf(`INSERT INTO lokadb.routing VALUES ('cloud_table', %s, %s, '') ON CONFLICT (kind, table_name, column_name) DO NOTHING`, lit(ct.Table), lit(ct.TenantColumn)),
				fmt.Sprintf(`DELETE FROM lokadb.routing WHERE kind = 'cloud_table' AND table_name = %s AND column_name <> %s`, lit(ct.Table), lit(ct.TenantColumn)),
			} {
				if err := r.exec(s); err != nil {
					return err
				}
			}
		}
	}
	gone, err := r.rows(`SELECT table_name FROM lokadb.routing WHERE kind = 'cloud_table'`)
	if err != nil {
		return err
	}
	for _, g := range gone {
		if want["cloud:"+g[0]] {
			continue
		}
		r.log.Info("cloud table removed", "table", g[0])
		for _, s := range []string{
			fmt.Sprintf(`DELETE FROM lokadb.lookups WHERE table_name = %s`, lit(g[0])),
			fmt.Sprintf(`DELETE FROM lokadb.routing WHERE kind = 'cloud_table' AND table_name = %s`, lit(g[0])),
		} {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}
	if err := applyKeys(r, spec, tables, want); err != nil {
		return err
	}
	old, err := r.rows(`SELECT table_name, column_name FROM lokadb.routing WHERE kind = 'relation'`)
	if err != nil {
		return err
	}
	for _, o := range old {
		if want[o[0]+"."+o[1]] {
			continue
		}
		r.log.Info("relation removed", "from", o[0]+"."+o[1])
		for _, s := range []string{
			fmt.Sprintf(`DELETE FROM lokadb.lookups WHERE table_name = %s AND column_name = %s`, lit(o[0]), lit(o[1])),
			fmt.Sprintf(`DELETE FROM lokadb.routing WHERE kind = 'relation' AND table_name = %s AND column_name = %s`, lit(o[0]), lit(o[1])),
		} {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}
	return nil
}

// ApplyRouting applies the spec's routing declarations to a database enable has run on
// (lokadb routing apply): the proxies see them when they next reload the metadata.
func ApplyRouting(ctx context.Context, dsn string, spec Spec, log *slog.Logger) error {
	conn, err := connectPlain(ctx, dsn)
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
	ok, err := r.rows(`SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil {
		return err
	}
	if ok[0][0] != "true" {
		return fmt.Errorf("lokadb enable has not run in this database: the declarations are applied by enable")
	}
	if err := applyRouting(r, spec); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// KeyMapName: a declared key whose map's name Postgres would cut (longer than 63 bytes) is
// refused; check refuses it the same way (review 3 of check, L2).
func KeyMapName(k Key) error {
	if kt := KeyTable(k.Table, k.Column); len(kt) > 63 {
		return fmt.Errorf("key %s.%s: the name of its map, %s, is longer than 63 bytes", k.Table, k.Column, kt)
	}
	return nil
}

// applyKeys makes the map of every declared key (filled from the table, kept by a trigger)
// and registers its lookup; the maps of keys the spec no longer has are dropped.
func applyKeys(r *runner, spec Spec, tables map[string]string, want map[string]bool) error {
	dbKind, err := r.dbTenantKind() // the maps' organization column is of the organizations' kind
	if err != nil {
		return err
	}
	if spec.Routing != nil {
		for _, k := range spec.Routing.Keys {
			if tables[k.Table] == "" {
				return fmt.Errorf("key %s.%s: %s is not a LokaDB table", k.Table, k.Column, k.Table)
			}
			if err := KeyMapName(k); err != nil {
				return err
			}
			kt := KeyTable(k.Table, k.Column)
			want["key:"+k.Table+"."+k.Column] = true
			had, err := r.rows(`SELECT 1 FROM lokadb.routing WHERE kind = 'key' AND table_name = $1 AND column_name = $2`, k.Table, k.Column)
			if err != nil {
				return err
			}
			if len(had) > 0 {
				continue
			}
			prepared, err := r.rows(`DELETE FROM lokadb.routing WHERE kind = 'key_prepared' AND table_name = $1 AND column_name = $2 RETURNING 1`, k.Table, k.Column)
			if err != nil {
				return err
			}
			if len(prepared) > 0 {
				// enable -prepare made and checked the map: only the trigger moves to the parent
				// (the temporary one stays on the DEFAULT partition otherwise, firing twice)
				for _, s := range []string{
					fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(prepKeyTrigger(k.Column)), qr(hn(k.Table, "__cloud"))),
					fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR DELETE OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.key_sync(%s, %s, %s)",
						q("lokadb_key_"+k.Column), trigCols(k.Column, tables[k.Table]), qr(k.Table), lit(kt), lit(k.Column), lit(tables[k.Table])),
					fmt.Sprintf(`INSERT INTO lokadb.lookups VALUES (%s, %s, %s) ON CONFLICT (table_name, column_name) DO UPDATE SET refs_table = EXCLUDED.refs_table`,
						lit(k.Table), lit(k.Column), lit("lokadb."+kt)),
					fmt.Sprintf(`INSERT INTO lokadb.routing VALUES ('key', %s, %s, %s)`, lit(k.Table), lit(k.Column), lit(kt)),
				} {
					if err := r.exec(s); err != nil {
						return err
					}
				}
				r.log.Info("key", "table", k.Table, "column", k.Column, "map", "lokadb."+kt, "prepared", true)
				continue
			}
			ok, err := r.rows(`SELECT 1 FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped`, qr(k.Table), k.Column)
			if err != nil {
				return err
			}
			if len(ok) == 0 {
				return fmt.Errorf("key %s.%s: %s has no column %s", k.Table, k.Column, k.Table, k.Column)
			}
			// The map's primary key refuses a value used twice, in any organizations: the
			// declaration is refused rather than half applied.
			dup, err := r.rows(fmt.Sprintf(`SELECT %s::text FROM %s WHERE %s IS NOT NULL GROUP BY 1 HAVING count(*) > 1 LIMIT 3`,
				q(k.Column), qr(k.Table), q(k.Column)))
			if err != nil {
				return err
			}
			if len(dup) > 0 {
				return fmt.Errorf("key %s.%s: values occur more than once (for example %q): a key must be unique across the whole table", k.Table, k.Column, dup[0][0])
			}
			grantees, err := r.rows(`SELECT DISTINCT coalesce(quote_ident(g.rolname), 'PUBLIC')
				FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
				WHERE c.oid = $1::regclass AND a.grantee <> c.relowner`, qr(k.Table))
			if err != nil {
				return err
			}
			owner, err := r.rows(`SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = $1::regclass`, qr(k.Table))
			if err != nil {
				return err
			}
			steps := []string{
				fmt.Sprintf("CREATE TABLE lokadb.%s (id text PRIMARY KEY, tenant %s)", q(kt), tenantType(dbKind)),
				fmt.Sprintf("INSERT INTO lokadb.%s SELECT %s::text, %s FROM %s WHERE %s IS NOT NULL", q(kt), q(k.Column), q(tables[k.Table]), qr(k.Table), q(k.Column)),
				fmt.Sprintf("SELECT lokadb.owner_to('TABLE', %s, %s)", lit("lokadb."+q(kt)), lit(owner[0][0])),
				fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR DELETE OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.key_sync(%s, %s, %s)",
					q("lokadb_key_"+k.Column), trigCols(k.Column, tables[k.Table]), qr(k.Table), lit(kt), lit(k.Column), lit(tables[k.Table])),
				fmt.Sprintf(`INSERT INTO lokadb.lookups VALUES (%s, %s, %s) ON CONFLICT (table_name, column_name) DO UPDATE SET refs_table = EXCLUDED.refs_table`,
					lit(k.Table), lit(k.Column), lit("lokadb."+kt)),
				fmt.Sprintf(`INSERT INTO lokadb.routing VALUES ('key', %s, %s, %s)`, lit(k.Table), lit(k.Column), lit(kt)),
			}
			for _, g := range grantees {
				// writers fire the trigger, readers are looked up by the proxy as themselves
				steps = append(steps, fmt.Sprintf("GRANT SELECT, INSERT, DELETE ON lokadb.%s TO %s", q(kt), g[0]))
			}
			for _, s := range steps {
				if err := r.exec(s); err != nil {
					return err
				}
			}
			r.log.Info("key", "table", k.Table, "column", k.Column, "map", "lokadb."+kt)
		}
	}
	old, err := r.rows(`SELECT table_name, column_name, target FROM lokadb.routing WHERE kind = 'key'`)
	if err != nil {
		return err
	}
	for _, o := range old {
		if want["key:"+o[0]+"."+o[1]] {
			continue
		}
		r.log.Info("key removed", "table", o[0], "column", o[1])
		for _, s := range []string{
			fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q("lokadb_key_"+o[1]), qr(o[0])),
			fmt.Sprintf("DROP TABLE IF EXISTS lokadb.%s", q(o[2])),
			fmt.Sprintf(`DELETE FROM lokadb.lookups WHERE table_name = %s AND column_name = %s`, lit(o[0]), lit(o[1])),
			fmt.Sprintf(`DELETE FROM lokadb.routing WHERE kind = 'key' AND table_name = %s AND column_name = %s`, lit(o[0]), lit(o[1])),
		} {
			if err := r.exec(s); err != nil {
				return err
			}
		}
	}
	return nil
}

// cloudLookupsSQL: the lookups of a cloud table: its organization column (refs NULL) and id
// (refs: the table itself). Where the organization column is id (the organizations table) it
// is one lookup: the id is the organization.
func cloudLookupsSQL(ct CloudTable) string {
	if ct.TenantColumn == "id" {
		return fmt.Sprintf(`INSERT INTO lokadb.lookups VALUES (%s, 'id', NULL)`, lit(ct.Table))
	}
	return fmt.Sprintf(`INSERT INTO lokadb.lookups VALUES (%s, %s, NULL), (%s, 'id', %s)`, lit(ct.Table), lit(ct.TenantColumn), lit(ct.Table), lit(ct.Table))
}
