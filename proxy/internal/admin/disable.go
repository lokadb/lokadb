package admin

// disable undoes enable (tasks/rollback.md, section 3): every spec table becomes a plain
// table again. The table that stays is the DEFAULT partition T__cloud, the table enable
// started from (its OID, indexes, constraints, statistics); the rows of the other partitions
// (organizations moved back, rows of no organization) are poured into it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/trig"
)

// DisableOptions configures Disable.
type DisableOptions struct {
	CloudDSN string
	Spec     Spec
	DryRun   bool
	// Report: filled by a dry run (and by a real one, before it changes anything).
	Report *DisableReport
	// Grace: how long the refs tables and schema lokadb stay after the commit (proxies with
	// the old metadata still read the refs and call lokadb.org_lock until they reload it).
	Grace time.Duration
	// LockTimeout: how long disable waits for any lock (review3 R3-9); Attempts: how many times
	// it tries for the tables' lock. 0: 5s and 5.
	LockTimeout time.Duration
	Attempts    int
	Log         *slog.Logger
}

// disableLock begins disable's transaction and locks the tables. A long reader of one of them
// (a report) made disable wait without end, and every query of every LokaDB table queued behind
// its ACCESS EXCLUSIVE (review3 R3-9): now it waits LockTimeout and tries again, as move-org and
// split do. The timeout stays for the transaction: a later lock that waits too long rolls the
// whole disable back instead of queueing the application behind it.
func disableLock(ctx context.Context, conn *pgx.Conn, tables []string, o DisableOptions, log *slog.Logger) (pgx.Tx, error) {
	wait, attempts := o.LockTimeout, o.Attempts
	if wait <= 0 {
		wait = 5 * time.Second
	}
	if attempts <= 0 {
		attempts = 5
	}
	for i := 1; ; i++ {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%dms'", wait.Milliseconds())); err != nil {
			_ = tx.Rollback(ctx)
			return nil, err
		}
		_, err = tx.Exec(ctx, "LOCK TABLE "+strings.Join(tables, ", ")+" IN ACCESS EXCLUSIVE MODE")
		if err == nil {
			return tx, nil
		}
		_ = tx.Rollback(ctx)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || (pgErr.Code != "55P03" && pgErr.Code != "40P01") {
			return nil, err
		}
		if i >= attempts {
			return nil, fmt.Errorf("disable changed nothing: the tables stayed busy (%d attempts of %s: %v); run it when the long transactions on them are done, or with a larger -lock-timeout", attempts, wait, err)
		}
		log.Warn("the tables are busy, disable tries again", "attempt", i, "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(i) * time.Second):
		}
	}
}

// identity is how enable found an identity column (lokadb.tables.identities, block 3).
type identity struct {
	Col       string `json:"col"`
	Generated string `json:"generated"` // a: ALWAYS, d: BY DEFAULT
	Start     int64  `json:"start"`
	Increment int64  `json:"increment"`
	Min       int64  `json:"min"`
	Max       int64  `json:"max"`
	Cache     int64  `json:"cache"`
	Cycle     bool   `json:"cycle"`
}

// enable's sequence for an identity column (admin.go)
func identitySeq(t, col string) string {
	if col == "id" {
		return hn(t, "__id_seq")
	}
	return hn(t, "__"+col+"_seq")
}

// Disable turns the spec's LokaDB tables back into plain tables, in one transaction.
func Disable(ctx context.Context, o DisableOptions) error {
	log := o.Log
	conn, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	var mine bool
	if err := conn.QueryRow(ctx, "SELECT pg_try_advisory_lock($1)", moveLockKey).Scan(&mine); err != nil {
		return err
	}
	if !mine {
		return errors.New("a move-org or move-back is running against this cloud database")
	}
	r, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil {
		return err
	}
	if r[0][0] != "true" {
		return errors.New("the database is not enabled (no lokadb.tables)")
	}
	orgs, err := rowsOf(ctx, conn, `SELECT coalesce(string_agg(org::text, ', ' ORDER BY org), '') FROM lokadb.orgs`)
	if err != nil {
		return err
	}
	if orgs[0][0] != "" {
		return fmt.Errorf("organizations %s are in their own databases: bring each back first (lokadb move-back -org N), then disable", orgs[0][0])
	}
	enabled := map[string]string{}
	rows, err := rowsOf(ctx, conn, `SELECT table_name, tenant_column FROM lokadb.tables`)
	if err != nil {
		return err
	}
	for _, row := range rows {
		enabled[row[0]] = row[1]
	}
	if len(enabled) == 0 && !o.DryRun {
		// an earlier disable committed, or enable -prepare ran and enable did not (block 3)
		log.Info("no table is enabled: undoing enable -prepare if it ran, dropping what an earlier disable left")
		if err := undoPrepare(ctx, conn, o.Spec, log); err != nil {
			return err
		}
		return dropLeftovers(ctx, conn, o.Spec, 0, log)
	}
	added := map[string]bool{} // tenant column added by enable (a child table)
	for _, st := range o.Spec.Tables {
		if _, ok := enabled[st.Name]; !ok {
			return fmt.Errorf("%s is in the spec but not enabled", st.Name)
		}
		added[st.Name] = st.TenantColumn == ""
	}
	if len(o.Spec.Tables) != len(enabled) {
		return fmt.Errorf("the spec lists %d tables, lokadb.tables %d: disable takes the whole spec enable was given", len(o.Spec.Tables), len(enabled))
	}

	// What is outside DEFAULT moves into it: the pause grows with it. Of those, the rows of no
	// organization (the partition <table>__none) are counted apart: a lot of them means the
	// organizations table was chosen wrong.
	var total int64
	rep := &DisableReport{}
	for _, st := range o.Spec.Tables {
		n, err := rowsOf(ctx, conn, fmt.Sprintf(`SELECT count(*) FILTER (WHERE tableoid <> %s::regclass)::text,
			count(*) FILTER (WHERE tableoid = coalesce(to_regclass(%s), 0::oid::regclass))::text FROM %s`,
			lit(qr(hn(st.Name, "__cloud"))), lit(qr(hn(st.Name, "__none"))), qr(st.Name)))
		if err != nil {
			return err
		}
		var c, none int64
		fmt.Sscan(n[0][0], &c)
		fmt.Sscan(n[0][1], &none)
		total += c
		rep.Tables = append(rep.Tables, DisableTableReport{Name: st.Name, Rows: c, NoOrgRows: none})
		rep.NoOrgRows += none
		log.Info("rows outside DEFAULT", "table", st.Name, "rows", c, "rows_without_organization", none)
	}
	rep.Rows = total
	rep.Pause = (time.Duration(total/rowsPerSecond+1) * time.Second).String()
	if o.Report != nil {
		*o.Report = *rep
	}
	if o.DryRun {
		log.Info("dry run: nothing changed. A real disable runs in one transaction; reads and writes of these tables wait while the rows outside DEFAULT are copied into it",
			"rows_to_copy", total, "rows_without_organization", rep.NoOrgRows, "expected_pause", rep.Pause)
		return nil
	}

	cols, err := rowsOf(ctx, conn, `SELECT string_agg(attname::text, ',') FROM pg_attribute
		WHERE attrelid = 'lokadb.tables'::regclass AND attname IN ('orig_owner', 'identities') AND NOT attisdropped`)
	if err != nil {
		return err
	}
	recorded := strings.Contains(cols[0][0], "identities")

	var lock []string
	for _, st := range o.Spec.Tables {
		lock = append(lock, qr(st.Name))
	}
	tx, err := disableLock(ctx, conn, lock, o, log)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	rn := &runner{ctx: ctx, tx: tx, log: log}
	start := time.Now()

	// FKs onto refs tables, from anywhere: they are put back onto the tables themselves.
	type fkBack struct{ table, name, def string }
	var fks []fkBack
	for _, st := range o.Spec.Tables {
		refs := hn(st.Name, "__refs")
		// refs.id stands for the table's key (block 4: not always id; a composite key's columns
		// in id_1 ... id_n), refs.tenant for its organization column
		key, err := rn.rows(`SELECT coalesce(to_jsonb(t) ->> 'key_column', 'id'), t.tenant_column FROM lokadb.tables t WHERE t.table_name = $1`, st.Name)
		if err != nil {
			return err
		}
		if len(key) == 0 {
			return fmt.Errorf("%s__cloud has no one-column primary key: the FKs onto %s cannot be pointed back at it", st.Name, refs)
		}
		rows, err := rn.rows(`SELECT c.conrelid::regclass::text, c.conname::text, pg_get_constraintdef(c.oid),
			lokadb.key_of(s.oid), s.relispartition::text FROM pg_constraint c JOIN pg_class s ON s.oid = c.conrelid
			WHERE c.contype = 'f' AND c.confrelid = $1::regclass AND c.conparentid = 0`, qr(refs))
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := rn.exec(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", row[0], q(row[1]))); err != nil {
				return err
			}
			table := row[0]
			for et := range enabled {
				if hn(et, "__cloud") == row[3] {
					table = qr(et) // renamed below
				}
			}
			if row[4] == "true" && table == row[0] {
				continue // the copy on an organization's partition or __none: the partition goes
			}
			def := fkOntoTable(row[2], refs, st.Name, key[0][0], key[0][1])
			fks = append(fks, fkBack{table: table, name: row[1], def: def})
		}
		// FKs onto a unique column's map (block 4, item 2): back onto that column
		maps, err := rn.rows(`SELECT column_name, refs_table FROM lokadb.unique_refs WHERE table_name = $1 ORDER BY 1`, st.Name)
		if err != nil {
			return err
		}
		for _, mp := range maps {
			rows, err := rn.rows(`SELECT c.conrelid::regclass::text, c.conname::text, pg_get_constraintdef(c.oid), s.relispartition::text,
				lokadb.key_of(s.oid) FROM pg_constraint c JOIN pg_class s ON s.oid = c.conrelid
				WHERE c.contype = 'f' AND c.confrelid = $1::regclass AND c.conparentid = 0`, qr(mp[1]))
			if err != nil {
				return err
			}
			for _, row := range rows {
				if err := rn.exec(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", row[0], q(row[1]))); err != nil {
					return err
				}
				table := row[0]
				for et := range enabled {
					if hn(et, "__cloud") == row[4] {
						table = qr(et) // renamed below
					}
				}
				if row[3] == "true" && table == row[0] {
					continue // a copy on an organization's partition or __none: the partition goes
				}
				fks = append(fks, fkBack{table: table, name: row[1], def: fkOntoTable(row[2], mp[1], st.Name, mp[0], key[0][1])})
			}
			if err := rn.exec(fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(urefTrigger(mp[0])), qr(st.Name))); err != nil {
				return err
			}
		}
	}

	for i := len(o.Spec.Tables) - 1; i >= 0; i-- {
		st := o.Spec.Tables[i]
		t, tc, def := st.Name, enabled[st.Name], hn(st.Name, "__cloud")
		log.Info("converting back", "table", t)
		// What names the parent: re-created once the name means DEFAULT again.
		deps, err := Dependents(rn.rows, t)
		if err != nil {
			return err
		}
		if err := unmovable(t, deps); err != nil {
			return err
		}
		// a materialized view over the parent has no CREATE OR REPLACE: made again over the
		// plain table, with what reads it (carry.go)
		var mvSteps []string
		mvs, err := rn.matviews(t)
		if err != nil {
			return err
		}
		seen := map[string]bool{}
		for _, mv := range mvs {
			s, err := rn.matviewSteps(mv[0], seen)
			if err != nil {
				return fmt.Errorf("%s: %w", t, err)
			}
			mvSteps = append(mvSteps, s...)
		}
		// enable's sequences of identity columns, by name (identitySeq), owned by none again here
		cols, err := rn.rows(`SELECT attname::text FROM pg_attribute WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped`, qr(t))
		if err != nil {
			return err
		}
		var idSeqs []string
		for _, c := range cols {
			idSeqs = append(idSeqs, qr(identitySeq(t, c[0])))
		}
		// The trigger functions enable rewrote (R04, trig.Rewrite) get their own text back,
		// unless a trigger of a LokaDB table still enabled calls them too: schema lokadb goes.
		tfuncs, err := rn.rows(`SELECT DISTINCT pg_get_functiondef(p.oid), p.prosrc FROM pg_trigger g
			JOIN pg_proc p ON p.oid = g.tgfoid JOIN pg_language l ON l.oid = p.prolang
			WHERE g.tgrelid = $1::regclass AND NOT g.tgisinternal AND l.lanname = 'plpgsql'
			  AND NOT EXISTS (SELECT 1 FROM pg_trigger g2 JOIN lokadb.tables lt ON to_regclass(lokadb.qrel(lt.table_name)) = g2.tgrelid
			                  WHERE g2.tgfoid = p.oid AND g2.tgrelid <> $1::regclass)`, qr(t))
		if err != nil {
			return err
		}
		trgs, err := rn.rows(`SELECT tgname::text, pg_get_triggerdef(oid), tgenabled::text FROM pg_trigger
			WHERE tgrelid = $1::regclass AND NOT tgisinternal AND tgname NOT IN ('lokadb_refs', 'lokadb_parent') ORDER BY tgname`, qr(t))
		if err != nil {
			return err
		}
		grants, err := rn.rows(`SELECT coalesce(quote_ident(g.rolname), 'PUBLIC'), a.privilege_type, a.is_grantable::text
			FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a LEFT JOIN pg_roles g ON g.oid = a.grantee
			WHERE c.oid = $1::regclass AND a.grantee <> c.relowner`, qr(t))
		if err != nil {
			return err
		}
		// What enable carried to the parent (carry.go, grants): the application's row security
		// and its policies, its rules on the table, its privileges on columns (review3 R3-2:
		// dropped with the old parent, a reader saw every organization's rows)
		appRLS, err := rn.appRowSecurity(t)
		if err != nil {
			return err
		}
		rules, err := rn.rows(`SELECT pg_get_ruledef(oid) FROM pg_rewrite WHERE ev_class = $1::regclass AND rulename <> '_RETURN' ORDER BY rulename`, qr(t))
		if err != nil {
			return err
		}
		colGrants, err := rn.rows(`SELECT quote_ident(a.attname), x.privilege_type, coalesce(quote_ident(g.rolname), 'PUBLIC'), x.is_grantable::text
			FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid CROSS JOIN LATERAL aclexplode(a.attacl) x
			LEFT JOIN pg_roles g ON g.oid = x.grantee
			WHERE c.oid = $1::regclass AND a.attnum > 0 AND NOT a.attisdropped AND x.grantee <> c.relowner
			  AND NOT (a.attname = $2 AND $3)`, qr(t), tc, added[t])
		if err != nil {
			return err
		}
		parts, err := rn.rows(`SELECT lokadb.key_of(c.oid), c.relkind::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE i.inhparent = $1::regclass AND lokadb.key_of(c.oid) <> $2`, qr(t), def)
		if err != nil {
			return err
		}
		// Publications that hold the parent by name (enable moved them there from DEFAULT, block 3
		// A10), with their row filter and column list: they go back to the plain table.
		pubs, err := rn.publications(qr(t))
		if err != nil {
			return err
		}
		insCols, err := rn.rows(`SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) FROM pg_attribute
			WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = ''`, qr(def))
		if err != nil {
			return err
		}
		chks, err := rn.rows(`SELECT conname::text FROM pg_constraint WHERE conrelid = $1::regclass AND contype = 'c' AND conname LIKE 'lokadb\_%'`, qr(def))
		if err != nil {
			return err
		}
		var steps []string
		for _, c := range chks {
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", qr(def), q(c[0])))
		}
		// The rows poured into DEFAULT are not new: no trigger of DEFAULT may see them as inserts,
		// neither the application's (an audit, a counter; review 3, R3-3) nor LokaDB's (lokadb_refs
		// would write their refs again). Every trigger of DEFAULT that is not Postgres' own is off
		// for the INSERTs and on again with its mode right after, as its owner may do; the table is
		// locked ACCESS EXCLUSIVE, nobody else writes it meanwhile. This took
		// session_replication_role = replica before, which a managed Postgres often denies the
		// tables' owner (RDS, Cloud SQL, Azure, Yandex): disable failed where enable, move-org and
		// move-back had passed. Postgres' own triggers stay: the FKs of DEFAULT onto other tables
		// check the rows (the FKs onto refs tables are dropped above), and a row that breaks one
		// stops disable with nothing changed.
		// A trigger cloned from the parent onto DEFAULT is the parent's (lokadb_refs among them):
		// before Postgres 15 its clone was marked tgisinternal, and lokadb_refs wrote the poured
		// rows' refs again (duplicate key in <table>__refs on a 13 or 14 cloud). Postgres' own
		// triggers (an FK's, tgconstraint) stay.
		always, err := rn.rows(`SELECT tgname::text, tgenabled::text FROM pg_trigger
			WHERE tgrelid = $1::regclass AND (NOT tgisinternal OR tgparentid <> 0 AND tgconstraint = 0) AND tgenabled <> 'D' ORDER BY tgname`, qr(def))
		if err != nil {
			return err
		}
		if len(parts) == 0 {
			always = nil
		}
		for _, g := range always {
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qr(def), q(g[0])))
		}
		for _, p := range parts {
			if p[1] == "f" {
				return fmt.Errorf("%s has the foreign partition %s with no organization in lokadb.orgs: look at it (move-back) before disable", t, p[0])
			}
			steps = append(steps,
				fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", qr(t), qr(p[0])),
				fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", qr(def), insCols[0][0], insCols[0][0], qr(p[0])),
				fmt.Sprintf("DROP TABLE %s", qr(p[0])))
		}
		if len(always) > 0 {
			// A deferred FK (Django makes them all DEFERRABLE INITIALLY DEFERRED) of DEFAULT has
			// queued its checks for the poured rows, and Postgres does not ALTER a table with
			// pending trigger events: they run now, not at COMMIT.
			steps = append(steps, "SET CONSTRAINTS ALL IMMEDIATE")
		}
		for _, g := range always {
			mode := map[string]string{"O": "", "A": "ALWAYS ", "R": "REPLICA "}[g[1]]
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE %sTRIGGER %s", qr(def), mode, q(g[0])))
		}
		steps = append(steps,
			fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s", qr(t), qr(def)),
			fmt.Sprintf("DROP STATISTICS IF EXISTS %s", qr(hn(t, "__ks"))), // key_stats: not the table's before enable
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qr(t), q(nameOf(hn(t, "__lokadb_old")))),
			fmt.Sprintf("ALTER TABLE %s RENAME TO %s", qr(def), q(nameOf(t))))
		steps = append(steps, depSteps(deps)...) // their text names t: now the plain table
		for _, ru := range rules {
			steps = append(steps, ru[0]) // its text names t: now the plain table
		}
		steps = append(steps, appRLS...)
		steps = append(steps, mvSteps...) // before the old parent goes: they read it until then
		for _, f := range tfuncs {
			if trig.Rewritten(f[1]) {
				steps = append(steps, strings.Replace(f[0], f[1], trig.Restore(f[1]), 1))
			}
		}
		for _, g := range trgs {
			steps = append(steps, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", q(g[0]), qr(t)), g[1]) // a clone may be left on it
			switch g[2] {
			case "D":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qr(t), q(g[0])))
			case "R":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE REPLICA TRIGGER %s", qr(t), q(g[0])))
			case "A":
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE ALWAYS TRIGGER %s", qr(t), q(g[0])))
			}
		}
		steps = append(steps,
			fmt.Sprintf("DROP TRIGGER IF EXISTS lokadb_refs ON %s", qr(t)),
			fmt.Sprintf("DROP TRIGGER IF EXISTS lokadb_parent ON %s", qr(t)),
			fmt.Sprintf("DROP INDEX IF EXISTS %s", qr(inSchemaOf(t, keyIndex(t)))),
			// the sequences enable gave the parent's columns (R03) go back to the plain table's
			// before the old parent goes, or they would go with it; the identities' are owned by
			// none, restoreOwnerAndIdentity makes the identities again and drops them
			ownSequencesSQL(t, idSeqs),
			fmt.Sprintf("DROP TABLE %s", qr(hn(t, "__lokadb_old"))))
		if added[t] {
			steps = append(steps, fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", qr(t), q(tc)))
		}
		for _, g := range grants {
			opt := ""
			if g[2] == "true" {
				opt = " WITH GRANT OPTION"
			}
			steps = append(steps, fmt.Sprintf("GRANT %s ON %s TO %s%s", g[1], qr(t), g[0], opt))
		}
		for _, g := range colGrants {
			opt := ""
			if g[3] == "true" {
				opt = " WITH GRANT OPTION"
			}
			steps = append(steps, fmt.Sprintf("GRANT %s (%s) ON %s TO %s%s", g[1], g[0], qr(t), g[2], opt))
		}
		for _, p := range pubs {
			steps = append(steps, fmt.Sprintf("ALTER PUBLICATION %s ADD TABLE %s%s", p[0], qr(t), p[1]))
		}
		for _, s := range steps {
			if err := rn.exec(s); err != nil {
				return err
			}
		}
		// Indexes the proxy made per partition (lokadb.create_index) get their own names back.
		idx, err := rn.rows(`SELECT c.relname::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE i.indrelid = $1::regclass AND c.relname LIKE '%' || $2`, qr(t), "__"+nameOf(def))
		if err != nil {
			return err
		}
		for _, x := range idx {
			name := strings.TrimSuffix(x[0], "__"+nameOf(def))
			if free, err := rn.rows(`SELECT (to_regclass($1) IS NULL)::text`, qr(inSchemaOf(t, name))); err == nil && free[0][0] == "true" && name != "idx" {
				if err := rn.exec(fmt.Sprintf("ALTER INDEX %s RENAME TO %s", qr(inSchemaOf(t, x[0])), q(name))); err != nil {
					return err
				}
			}
		}
		if err := restoreOwnerAndIdentity(rn, t, recorded, log); err != nil {
			return err
		}
	}
	for _, f := range fks {
		if err := rn.exec(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID", f.table, q(f.name), f.def)); err != nil {
			return err
		}
	}
	for _, st := range o.Spec.Tables {
		if err := rn.exec(fmt.Sprintf("DELETE FROM lokadb.lookups WHERE table_name = %s", lit(st.Name))); err != nil {
			return err
		}
		if err := rn.exec(fmt.Sprintf("DELETE FROM lokadb.tables WHERE table_name = %s", lit(st.Name))); err != nil {
			return err
		}
	}
	// publish_via_partition_root as it was before enable turned it on (block 3 records it)
	vr, err := rn.rows(`SELECT (to_regclass('lokadb.publications') IS NOT NULL)::text`)
	if err != nil {
		return err
	}
	if vr[0][0] == "true" {
		rows, err := rn.rows(`SELECT quote_ident(b.pubname), b.viaroot_before::text FROM lokadb.publications b
			JOIN pg_publication p ON p.pubname = b.pubname`)
		if err != nil {
			return err
		}
		for _, p := range rows {
			if err := rn.exec(fmt.Sprintf("ALTER PUBLICATION %s SET (publish_via_partition_root = %s)", p[0], p[1])); err != nil {
				return err
			}
		}
	} else if left, err := rn.rows(`SELECT coalesce(string_agg(pubname::text, ', '), '') FROM pg_publication WHERE pubviaroot AND pubname NOT LIKE 'lokadb\_%'`); err == nil && left[0][0] != "" {
		log.Warn("publish_via_partition_root stays on: enable did not record whether it was on before (an older enable)", "publications", left[0][0])
	}
	if err := rn.exec(metaChanged); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	log.Info("tables are plain again", "rows_copied", total, "paused", time.Since(start).Round(time.Millisecond))

	for _, f := range fks {
		if err := execAll(ctx, conn, []string{fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", f.table, q(f.name))}); err != nil {
			log.Warn("foreign key left NOT VALID: rows reference rows that are gone", "table", f.table, "constraint", f.name, "err", err)
		}
	}
	return dropLeftovers(ctx, conn, o.Spec, o.Grace, log)
}

// restoreOwnerAndIdentity gives the table back the owner and the identity columns enable
// recorded (block 3: lokadb.tables.orig_owner, identities). A database enabled before they
// were recorded gets BY DEFAULT identities with the sequence's own parameters, and keeps
// its current owner.
func restoreOwnerAndIdentity(rn *runner, t string, recorded bool, log *slog.Logger) error {
	var owner string
	var ids []identity
	if recorded {
		r, err := rn.rows(`SELECT coalesce(orig_owner, ''), coalesce(identities::text, '') FROM lokadb.tables WHERE table_name = $1`, t)
		if err != nil {
			return err
		}
		owner = r[0][0]
		if r[0][1] == "" {
			recorded = false
		} else if err := json.Unmarshal([]byte(r[0][1]), &ids); err != nil {
			return fmt.Errorf("lokadb.tables.identities of %s: %w", t, err)
		}
	}
	if !recorded {
		// enable's sequences, found by name and default
		r, err := rn.rows(`SELECT a.attname::text FROM pg_attribute a JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
			WHERE a.attrelid = $1::regclass AND NOT a.attisdropped AND a.attidentity = ''
			  AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval(%'`, qr(t))
		if err != nil {
			return err
		}
		for _, row := range r {
			seq := identitySeq(t, row[0])
			p, err := rn.rows(`SELECT seqstart::text, seqincrement::text, seqmin::text, seqmax::text, seqcache::text, seqcycle::text
				FROM pg_sequence WHERE seqrelid = to_regclass($1)`, qr(seq))
			if err != nil {
				return err
			}
			if len(p) == 0 {
				continue // a serial column's own sequence: kept as it is
			}
			id := identity{Col: row[0], Generated: "d", Cycle: p[0][5] == "true"}
			fmt.Sscan(p[0][0], &id.Start)
			fmt.Sscan(p[0][1], &id.Increment)
			fmt.Sscan(p[0][2], &id.Min)
			fmt.Sscan(p[0][3], &id.Max)
			fmt.Sscan(p[0][4], &id.Cache)
			ids = append(ids, id)
			log.Warn("enable recorded no identity kind for this column (an older enable): restored as GENERATED BY DEFAULT", "table", t, "column", row[0])
		}
	}
	for _, id := range ids {
		seq := identitySeq(t, id.Col)
		next, err := rn.rows(fmt.Sprintf(`SELECT (CASE WHEN is_called THEN last_value + %d ELSE last_value END)::text FROM %s`, id.Increment, qr(seq)))
		if err != nil {
			return err
		}
		kind := "BY DEFAULT"
		if id.Generated == "a" {
			kind = "ALWAYS"
		}
		cycle := "NO CYCLE"
		if id.Cycle {
			cycle = "CYCLE"
		}
		for _, s := range []string{
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT", qr(t), q(id.Col)),
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s ADD GENERATED %s AS IDENTITY (START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d CACHE %d %s)",
				qr(t), q(id.Col), kind, id.Start, id.Increment, id.Min, id.Max, id.Cache, cycle),
			fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s RESTART WITH %s", qr(t), q(id.Col), next[0][0]),
			fmt.Sprintf("DROP SEQUENCE %s", qr(seq)),
		} {
			if err := rn.exec(s); err != nil {
				return err
			}
		}
	}
	if owner != "" {
		if err := rn.exec(fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(t), q(owner))); err != nil {
			return err
		}
	}
	return nil
}

// dropLeftovers drops the refs tables and, once no table is enabled, schema lokadb, after
// grace: until then proxies with the old metadata still read them.
func dropLeftovers(ctx context.Context, conn *pgx.Conn, spec Spec, grace time.Duration, log *slog.Logger) error {
	if grace > 0 {
		log.Info("waiting for proxies to reload their metadata before the refs tables and schema lokadb go", "grace", grace)
		select {
		case <-time.After(grace):
		case <-ctx.Done():
			return fmt.Errorf("interrupted: the refs tables and schema lokadb stay; run disable again to drop them: %w", ctx.Err())
		}
	}
	var steps []string
	var names []string
	for _, st := range spec.Tables {
		names = append(names, lit(st.Name))
	}
	// the unique columns' maps (block 4, item 2)
	steps = append(steps, doTag(fmt.Sprintf(`DO $$ DECLARE r record; BEGIN
		IF to_regclass('lokadb.unique_refs') IS NULL THEN RETURN; END IF;
		FOR r IN SELECT refs_table FROM lokadb.unique_refs WHERE table_name IN (%s) LOOP
			EXECUTE format('DROP TABLE IF EXISTS %%s', lokadb.rel(r.refs_table));
		END LOOP;
		DELETE FROM lokadb.unique_refs WHERE table_name IN (%s);
	END $$`, strings.Join(names, ", "), strings.Join(names, ", "))))
	for i := len(spec.Tables) - 1; i >= 0; i-- {
		steps = append(steps, "DROP TABLE IF EXISTS "+qr(hn(spec.Tables[i].Name, "__refs")))
	}
	if err := execAll(ctx, conn, steps); err != nil {
		return err
	}
	r, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.tables') IS NULL OR NOT EXISTS (SELECT 1 FROM lokadb.tables))::text`)
	if err != nil {
		return err
	}
	if r[0][0] == "true" {
		// CASCADE drops lokadb's own objects, which depend on one another; what the application
		// made over them after enable (a view calling a lokadb function, a default, a policy)
		// would go silently with them. The check and the drop are one transaction.
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			// lokadb's own triggers on tables outside it (the FKs onto cloud tables, fkcloud.go):
			// no table is enabled any more, nothing needs them
			left, err := rowsOf(ctx, tx, `SELECT format('DROP TRIGGER %I ON %s', t.tgname, t.tgrelid::regclass)
				FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_class c ON c.oid = t.tgrelid
				WHERE NOT t.tgisinternal AND t.tgname LIKE 'lokadb\_%' AND p.pronamespace = to_regnamespace('lokadb')
				  AND c.relnamespace <> to_regnamespace('lokadb') ORDER BY 1`)
			if err != nil {
				return err
			}
			for _, d := range left {
				if _, err := tx.Exec(ctx, d[0]); err != nil {
					return err
				}
				log.Info("lokadb's trigger on a table outside schema lokadb dropped", "sql", d[0])
			}
			// lokadb's event trigger (the metadata's change log, MetaChangesSQL) depends on its
			// function in schema lokadb: lokadb's own, not the application's
			if _, err := tx.Exec(ctx, "DROP EVENT TRIGGER IF EXISTS lokadb_meta_ddl"); err != nil {
				return err
			}
			deps, err := lokadbDependents(ctx, tx)
			if err != nil {
				return err
			}
			if len(deps) > 0 {
				return fmt.Errorf("schema lokadb stays: objects of the application depend on it, and dropping it would drop them too:\n  %s\n"+
					"  drop them or make them not use lokadb's objects, then run disable again (the tables are plain already)", strings.Join(deps, "\n  "))
			}
			_, err = tx.Exec(ctx, "DROP SCHEMA IF EXISTS lokadb CASCADE")
			return err
		})
		if err != nil {
			return err
		}
		log.Info("schema lokadb dropped: the database is as before enable")
	}
	return nil
}

// lokadbDependents: the objects outside schema lokadb that depend on one inside it (what DROP
// SCHEMA lokadb CASCADE would drop besides lokadb's own), described, sorted.
func lokadbDependents(ctx context.Context, q querier) ([]string, error) {
	// the namespace an object belongs to: its own, or its table's (a column default, a
	// constraint, a trigger, a rule, a policy); an object of another kind counts as outside
	rows, err := rowsOf(ctx, q, `WITH d AS (
		  SELECT d.classid, d.objid, d.objsubid FROM pg_depend d
		  WHERE to_regnamespace('lokadb') IS NOT NULL AND d.deptype IN ('n', 'a')
		    AND ((d.refclassid = 'pg_namespace'::regclass AND d.refobjid = to_regnamespace('lokadb'))
		      OR (pg_identify_object(d.refclassid, d.refobjid, 0)).schema = 'lokadb'))
		SELECT DISTINCT pg_describe_object(classid, objid, objsubid) FROM d
		WHERE coalesce(CASE classid
		    WHEN 'pg_class'::regclass THEN (SELECT relnamespace FROM pg_class WHERE oid = objid)
		    WHEN 'pg_attrdef'::regclass THEN (SELECT c.relnamespace FROM pg_attrdef a JOIN pg_class c ON c.oid = a.adrelid WHERE a.oid = objid)
		    WHEN 'pg_constraint'::regclass THEN (SELECT coalesce(c.relnamespace, x.connamespace) FROM pg_constraint x LEFT JOIN pg_class c ON c.oid = x.conrelid WHERE x.oid = objid)
		    WHEN 'pg_trigger'::regclass THEN (SELECT c.relnamespace FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid WHERE t.oid = objid)
		    WHEN 'pg_rewrite'::regclass THEN (SELECT c.relnamespace FROM pg_rewrite r JOIN pg_class c ON c.oid = r.ev_class WHERE r.oid = objid)
		    WHEN 'pg_policy'::regclass THEN (SELECT c.relnamespace FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid WHERE p.oid = objid)
		    WHEN 'pg_proc'::regclass THEN (SELECT pronamespace FROM pg_proc WHERE oid = objid)
		    WHEN 'pg_type'::regclass THEN (SELECT typnamespace FROM pg_type WHERE oid = objid)
		    WHEN 'pg_namespace'::regclass THEN objid
		  END, 0) <> to_regnamespace('lokadb')::oid
		ORDER BY 1`)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, r := range rows {
		out = append(out, r[0])
	}
	return out, nil
}

// fkOntoTable turns the definition of an FK onto a refs table back into one onto the table
// itself, on its key (refs.id stands for it), valid or not as the caller adds it.
func fkOntoTable(def, refs, table, key, tenant string) string {
	onRefs := refRe(refs) // schema-qualified outside public
	m := onRefs.FindStringSubmatch(def)
	if m == nil {
		return strings.TrimSuffix(def, " NOT VALID")
	}
	// the refs columns the FK references, back to the table's: id (or id_1 ... id_n of a
	// composite key) to the key's columns, tenant to the organization column
	onto := map[string]string{"tenant": q(tenant)}
	for i, c := range RefsKeyCols(key) {
		onto[c] = q(KeyCols(key)[i])
	}
	var cols []string
	for _, c := range strings.Split(m[len(m)-1], ",") {
		c = strings.Trim(strings.TrimSpace(c), `"`)
		if o, ok := onto[c]; ok && o != `""` {
			cols = append(cols, o)
		} else {
			cols = append(cols, q(c))
		}
	}
	// literal: a $ in a name is not a group reference
	return strings.TrimSuffix(strings.Replace(def, m[0], "REFERENCES "+qr(table)+"("+strings.Join(cols, ", ")+")", 1), " NOT VALID")
}

// undoPrepare takes back what enable -prepare (block 3) did to the plain tables when enable
// itself never ran: its triggers, the FKs onto the refs tables it added next to the old ones,
// the organization column it added to child tables, the index on the key. The refs tables and
// schema lokadb go with dropLeftovers.
func undoPrepare(ctx context.Context, conn *pgx.Conn, spec Spec, log *slog.Logger) error {
	for i := len(spec.Tables) - 1; i >= 0; i-- {
		st := spec.Tables[i]
		plain, err := rowsOf(ctx, conn, `SELECT (relkind = 'r')::text FROM pg_class WHERE oid = to_regclass($1)`, qr(st.Name))
		if err != nil {
			return err
		}
		if len(plain) == 0 || plain[0][0] != "true" {
			continue
		}
		var steps []string
		for _, tg := range []string{prepRefsTrigger, prepTenantTrigger, prepTenantAfter, prepMovedTrigger} {
			steps = append(steps, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", tg, qr(st.Name)))
		}
		if r, err := rowsOf(ctx, conn, `SELECT (to_regclass($1) IS NOT NULL)::text`, qr(hn(st.Name, "__refs"))); err != nil {
			return err
		} else if r[0][0] == "true" {
			fks, err := rowsOf(ctx, conn, `SELECT conrelid::regclass::text, conname::text FROM pg_constraint
				WHERE contype = 'f' AND confrelid = $1::regclass`, qr(hn(st.Name, "__refs")))
			if err != nil {
				return err
			}
			for _, f := range fks { // the FK onto the table itself is still there next to it
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", f[0], q(f[1])))
			}
		}
		// lokadb.tables is empty before enable: the column comes from the spec, and is dropped only
		// where lokadb.prepare shows -prepare reached the table (it added the column)
		touched, err := rowsOf(ctx, conn, `SELECT (to_regclass('lokadb.prepare') IS NOT NULL
			AND EXISTS (SELECT 1 FROM lokadb.prepare WHERE table_name = $1))::text`, st.Name)
		if err != nil {
			// lokadb.prepare missing: the planner checks the EXISTS anyway; treat as untouched
			touched = [][]string{{"false"}}
		}
		if st.TenantColumn == "" && touched[0][0] == "true" {
			if col := parentTenant(spec, st); col != "" {
				steps = append(steps, fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", qr(st.Name), q(col)))
			}
		}
		steps = append(steps, fmt.Sprintf("DROP INDEX IF EXISTS %s", qr(inSchemaOf(st.Name, keyIndex(st.Name)))))
		if err := execAll(ctx, conn, steps); err != nil {
			return err
		}
		log.Info("enable -prepare undone", "table", st.Name)
	}
	return nil
}

// parentTenant: the organization column a child table of the spec gets (its root's).
func parentTenant(spec Spec, st SpecTable) string {
	for st.TenantColumn == "" && st.ParentTable != "" {
		found := false
		for _, p := range spec.Tables {
			if p.Name == st.ParentTable {
				st, found = p, true
				break
			}
		}
		if !found {
			return ""
		}
	}
	return st.TenantColumn
}

// appRowSecurity: the steps that give the plain table t (by its name, once DEFAULT has it again)
// the application's row security that enable carried to the parent (carry.go): its policies,
// row security enabled, forced as it was. The state before enable is lokadb.tables.orig_rls; a
// database enabled before it was recorded tells it by the parent: policies of the application
// there were carried because row security was on, and lokadb_owner was added because it was
// not forced (row security on without a policy of its own cannot be told from off then).
func (r *runner) appRowSecurity(t string) ([]string, error) {
	pols, err := r.rows(`SELECT quote_ident(polname), CASE WHEN polpermissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
			CASE polcmd WHEN 'r' THEN 'SELECT' WHEN 'a' THEN 'INSERT' WHEN 'w' THEN 'UPDATE' WHEN 'd' THEN 'DELETE' ELSE 'ALL' END,
			coalesce((SELECT string_agg(CASE WHEN o = 0 THEN 'PUBLIC' ELSE quote_ident(pg_get_userbyid(o)) END, ', ') FROM unnest(polroles) o), 'PUBLIC'),
			coalesce(' USING (' || pg_get_expr(polqual, polrelid) || ')', ''),
			coalesce(' WITH CHECK (' || pg_get_expr(polwithcheck, polrelid) || ')', '')
		FROM pg_policy WHERE polrelid = $1::regclass
		  AND polname NOT IN ('lokadb_tenant', 'lokadb_owner') AND polname NOT LIKE 'lokadb\_bind\_org%'
		ORDER BY polname`, qr(t))
	if err != nil {
		return nil, err
	}
	state := ""
	if col, err := r.rows(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = 'lokadb.tables'::regclass AND attname = 'orig_rls' AND NOT attisdropped)::text`); err != nil {
		return nil, err
	} else if col[0][0] == "true" {
		rec, err := r.rows(`SELECT coalesce(orig_rls, '') FROM lokadb.tables WHERE table_name = $1`, t)
		if err != nil {
			return nil, err
		}
		if len(rec) > 0 {
			state = rec[0][0]
		}
	}
	if state == "" { // enabled before orig_rls: what the parent shows
		owner, err := r.rows(`SELECT EXISTS (SELECT FROM pg_policy WHERE polrelid = $1::regclass AND polname = 'lokadb_owner')::text`, qr(t))
		if err != nil {
			return nil, err
		}
		switch {
		case len(pols) == 0:
			state = "off"
		case owner[0][0] == "true":
			state = "on"
		default:
			state = "forced"
		}
	}
	if state == "off" {
		return nil, nil
	}
	var steps []string
	for _, p := range pols {
		steps = append(steps, fmt.Sprintf("CREATE POLICY %s ON %s AS %s FOR %s TO %s%s%s", p[0], qr(t), p[1], p[2], p[3], p[4], p[5]))
	}
	steps = append(steps, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", qr(t)))
	if state == "forced" {
		steps = append(steps, fmt.Sprintf("ALTER TABLE %s FORCE ROW LEVEL SECURITY", qr(t)))
	}
	return steps, nil
}

// DisableReport: what disable would do, counted without changing anything.
type DisableReport struct {
	Tables    []DisableTableReport
	Rows      int64  // rows in partitions of organizations, copied back into the default one
	NoOrgRows int64  // rows of no organization
	Pause     string // the expected wait of the tables, an estimate
}

type DisableTableReport struct {
	Name      string
	Rows      int64
	NoOrgRows int64
}
