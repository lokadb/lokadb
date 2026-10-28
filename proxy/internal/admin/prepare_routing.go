package admin

// The routing declarations (routing.go) in enable's two steps (prepare.go): what grows with
// the rows, the map of a key and the check of a relation on the data, is done by enable
// -prepare under the application's writes; the switch only registers the lookups.

import (
	"fmt"
)

// prepKeyTrigger keeps the map of key column col in step while the table is not yet LokaDB's
// (lokadb_prep_%: the switch does not carry it over to the parent; applyKeys replaces it by
// the real trigger on the parent).
func prepKeyTrigger(col string) string {
	n := "lokadb_prep_key_" + col
	if len(n) > 63 {
		n = n[:63]
	}
	return n
}

// routingKeys: the declared keys of tables this run converts.
func (p *plan) routingKeys() []Key {
	todo := map[string]bool{}
	for _, st := range p.todo {
		todo[st.Name] = true
	}
	var out []Key
	for _, k := range p.keys {
		if todo[k.Table] {
			out = append(out, k)
		}
	}
	return out
}

// routing prepares the declared keys and relations of the tables of this run.
func (c *prep) routing(spec Spec) error {
	if spec.Routing == nil {
		return nil
	}
	if err := c.ddl(routingSQL); err != nil {
		return err
	}
	for _, k := range c.p.routingKeys() {
		if err := c.key(k); err != nil {
			return fmt.Errorf("key %s.%s: %w", k.Table, k.Column, err)
		}
	}
	return c.relations(spec)
}

// key fills the map of one key in batches and hangs the trigger that keeps it, then records
// it as prepared: the switch registers it without scanning the table.
func (c *prep) key(k Key) error {
	t, col := k.Table, k.Column
	tc, kc := c.p.tenant[t], c.p.key[t]
	kt := KeyTable(t, col)
	if len(kt) > 63 {
		return fmt.Errorf("the name of its map, %s, is longer than 63 bytes", kt)
	}
	if ok, err := c.has(`SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = $1::regclass AND attname = $2 AND attnum > 0 AND NOT attisdropped)::text`, qr(t), col); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s has no column %s", t, col)
	}
	// the map's primary key refuses a value used twice: the declaration is refused up front
	dup, err := rowsOf(c.ctx, c.conn, fmt.Sprintf(`SELECT %s::text FROM %s WHERE %s IS NOT NULL GROUP BY 1 HAVING count(*) > 1 LIMIT 3`, q(col), qr(t), q(col)))
	if err != nil {
		return err
	}
	if len(dup) > 0 {
		return fmt.Errorf("values occur more than once (for example %q): a key must be unique across the whole table", dup[0][0])
	}
	writers, err := c.privileges(t, KeyCols(kc)[0], tc)
	if err != nil {
		return err
	}
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS lokadb.%s (id text PRIMARY KEY, tenant %s)", q(kt), tenantType(c.p.tenantKind[t])),
		fmt.Sprintf("ALTER TABLE lokadb.%s OWNER TO %s", q(kt), q(c.o.Owner)),
		"GRANT USAGE ON SCHEMA lokadb TO " + q(c.o.Owner),
	}
	// the trigger writes the map as the writer
	for _, w := range writers {
		if w[1] == "INSERT" || w[1] == "UPDATE" || w[1] == "DELETE" {
			stmts = append(stmts, fmt.Sprintf("GRANT SELECT, INSERT, DELETE ON lokadb.%s TO %s", q(kt), w[0]), "GRANT USAGE ON SCHEMA lokadb TO "+w[0])
		}
	}
	if ok, err := c.has(`SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)::text`, qr(t), prepKeyTrigger(col)); err != nil {
		return err
	} else if !ok {
		stmts = append(stmts, fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR DELETE OR UPDATE OF %s, %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.key_sync(%s, %s, %s)",
			q(prepKeyTrigger(col)), q(col), q(tc), qr(t), lit(kt), lit(col), lit(tc)))
	}
	if err := c.ddl(stmts...); err != nil {
		return err
	}
	est, err := rowsOf(c.ctx, c.conn, `SELECT greatest(reltuples, 0)::bigint::text FROM pg_class WHERE oid = $1::regclass`, qr(t))
	if err != nil {
		return err
	}
	var total int64
	fmt.Sscan(est[0][0], &total)
	if err := c.stage(t, "key_"+col, total); err != nil {
		return err
	}
	bk := newBKey(kc, c.p.keyType[t])
	if err := c.batches(t, bk, fmt.Sprintf(`WITH b AS (SELECT %[1]s, x.%[2]s::text AS v, x.%[3]s AS tc FROM %[4]s x WHERE %[7]s AND x.%[2]s IS NOT NULL ORDER BY %[8]s LIMIT %[5]d FOR SHARE),
		u AS (INSERT INTO lokadb.%[6]s AS m (id, tenant) SELECT v, tc FROM b
			ON CONFLICT (id) DO UPDATE SET tenant = EXCLUDED.tenant WHERE m.tenant IS DISTINCT FROM EXCLUDED.tenant RETURNING 1)
		SELECT %[9]s, (SELECT count(*) FROM b)::text, (SELECT count(*) FROM u)::text`,
		bk.sel("x."), q(col), q(tc), qr(t), prepBatch, q(kt), bk.within("x."), bk.order("x.", false), bk.lastOf("b"))); err != nil {
		return err
	}
	// map rows of values gone since an earlier run (it was stopped, or the trigger was)
	if _, err := c.conn.Exec(c.ctx, fmt.Sprintf("DELETE FROM lokadb.%s m WHERE NOT EXISTS (SELECT 1 FROM %s x WHERE x.%s::text = m.id)", q(kt), qr(t), q(col))); err != nil {
		return err
	}
	_, err = c.conn.Exec(c.ctx, `INSERT INTO lokadb.routing VALUES ('key_prepared', $1, $2, $3) ON CONFLICT DO NOTHING`, t, col, kt)
	return err
}

// relations checks the declared relations on the data, here and not under the switch's lock:
// parents found, parents of another organization. A relation that is not clean is refused
// unless the owner confirmed it. The switch registers the lookup and skips the scan.
func (c *prep) relations(spec Spec) error {
	for _, rel := range spec.Routing.Relations {
		src, col, dst, key, err := rel.Parse()
		if err != nil {
			return err
		}
		if c.p.tenant[src] == "" || c.p.tenant[dst] == "" {
			continue // a table of another run, or not LokaDB's: the switch reports it
		}
		if key != c.p.key[dst] {
			return fmt.Errorf("relation %s -> %s: the organization of %s rows is found by its key %s.%s, not by %s", rel.From, rel.To, dst, dst, c.p.key[dst], rel.To)
		}
		tx, err := c.conn.Begin(c.ctx)
		if err != nil {
			return err
		}
		chk, err := CheckRelation(c.ctx, tx, src, col, c.p.tenant[src], hn(dst, "__refs"), "id", "tenant")
		_ = tx.Rollback(c.ctx)
		if err != nil {
			return fmt.Errorf("relation %s -> %s: %w", rel.From, rel.To, err)
		}
		c.o.Log.Info("enable -prepare: relation", "from", rel.From, "to", rel.To, "data", chk.String())
		if !chk.Clean() && !rel.Confirmed {
			return fmt.Errorf("relation %s -> %s: %s; statements by %s would miss those rows. Fix the data, or confirm the relation (\"confirmed\": true) if that is acceptable",
				rel.From, rel.To, chk, rel.From)
		}
		if _, err := c.conn.Exec(c.ctx, `INSERT INTO lokadb.routing VALUES ('relation_checked', $1, $2, $3) ON CONFLICT DO NOTHING`, src, col, dst); err != nil {
			return err
		}
	}
	return nil
}

// verifyKeys: before the switch locks anything, the map of every prepared key agrees with its
// table and the trigger that keeps it is there.
func (r *runner) verifyKeys(p *plan, count func(what, sql string) error) error {
	for _, k := range p.routingKeys() {
		t, col, kc, tc := k.Table, k.Column, p.key[k.Table], p.tenant[k.Table]
		kt := KeyTable(t, col)
		if err := count("key "+t+"."+col+" not prepared", fmt.Sprintf(`SELECT (1 - count(*))::text FROM lokadb.routing
			WHERE kind = 'key_prepared' AND table_name = %s AND column_name = %s`, lit(t), lit(col))); err != nil {
			return err
		}
		if err := count("map of key "+t+"."+col+" rows that differ from "+t, fmt.Sprintf(`SELECT count(*)::text FROM %s x FULL JOIN lokadb.%s m ON m.id = x.%s::text
			WHERE x.%s IS NULL OR (x.%s IS NOT NULL AND (m.id IS NULL OR m.tenant IS DISTINCT FROM x.%s))`,
			qr(t), q(kt), q(col), q(KeyCols(kc)[0]), q(col), q(tc))); err != nil {
			return err
		}
		if err := count("trigger "+prepKeyTrigger(col)+" on "+t+" missing or disabled", fmt.Sprintf(`SELECT (1 - count(*))::text FROM pg_trigger
			WHERE tgrelid = %s::regclass AND tgname = %s AND tgenabled <> 'D'`, lit(qr(t)), lit(prepKeyTrigger(col)))); err != nil {
			return err
		}
	}
	return nil
}
