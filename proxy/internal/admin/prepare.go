package admin

// enable in two steps (1.4, tasks/enable-online.md). Prepare does what took enable's lock
// as long as the rows took: it fills the refs tables and the organization column of child
// tables in batches, adds the FKs onto the refs tables (NOT VALID, then VALIDATE) and builds
// the key index CONCURRENTLY, while triggers keep all of it in step with the application's
// writes. enable then only switches: it checks what Prepare made and changes the catalog.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

const (
	prepRefsTrigger   = "lokadb_prep_refs"
	prepTenantTrigger = "lokadb_prep_tenant"
	prepTenantAfter   = "lokadb_prep_tenant_after"
	prepMovedTrigger  = "lokadb_prep_moved" // on a parent of child tables without an organization column of their own (A3)
	prepBatch         = 10000
	// rowsPerSecond: what enable without Prepare filled under its lock (e2e/enabletime:
	// 2.15 million rows, 39 s without a transaction), for the downtime it announces
	rowsPerSecond = 55000
)

// prepFK names the FK Prepare adds next to name, onto the refs table; enable renames it.
func prepFK(name string) string {
	n := hn(name, "__lokadb")
	if len(n) > 63 {
		h := fnv.New32a()
		h.Write([]byte(name))
		n = fmt.Sprintf("%s_%08x__lokadb", name[:46], h.Sum32())
	}
	return n
}

// plan is what an enable run works on: the organization column and key of every LokaDB
// table, enabled before or now, and the tables this run converts (todo, names).
type plan struct {
	tenant, key map[string]string
	// keyType: the key column's type (what a key is cast to), refsType: the type of the refs
	// table's id (block 17: keys of any kind, keys.go); refsType only for the tables of this run
	keyType, refsType map[string]string
	// tenantKind: the kind of every table's organization column (block 18, tenants.go), one
	// for the whole database
	tenantKind map[string]string
	enabled    map[string]bool
	todo       []SpecTable
	names      []string
	// parentOfDerived: tables of this run that have child tables with no organization column of
	// their own: those copy the parent's, so a change of it in the window between the switch's
	// check and its lock matters (A3, prepMovedTrigger). One enabled before is guarded by
	// same_org.
	parentOfDerived map[string]bool
	// the enabled table of a refs table, of a DEFAULT partition, of a unique column's map
	refsOf, cloudOf, urefOf map[string]string
	// urefs[table][column]: the type of a unique column other than the key that an FK of this
	// run references (block 4, item 2: it gets a map)
	urefs map[string]map[string]string
	// keys: the keys the spec declares (routing.keys), prepared with the tables of this run
	keys []Key
}

// plan reads lokadb.tables: enable can run again. Tables enabled before are left as they are
// (a run that failed building its key index after COMMIT is finished), tables new to the
// spec are added.
func parentKey(t string) string {
	if t == "" {
		return ""
	}
	return RelKey(SplitKey(t))
}

// ChildTenantClash: a child table (parent_table) that has a column of the name of the
// organization column enable would add to it; enable refuses it, check reports it the same way.
func ChildTenantClash(table, tenantCol, parent string) error {
	return fmt.Errorf("%s already has a column %s: lokadb would add it as the organization column it takes from %s. If the column holds the row's organization, give %s \"tenant_column\": %q in the spec instead of parent_table; otherwise rename it",
		table, tenantCol, parent, table, tenantCol)
}

// AddingTablesRefused: why enable would not add tables to LokaDB tables enabled before, "" when
// it would. A table added now would keep the rows of an organization already moved in the
// cloud, and its new rows too: move-org does not carry a single table yet. check calls it too
// (review 3 of check, M5).
func AddingTablesRefused(ctx context.Context, q querier, names []string) (string, error) {
	moved, err := rowsOf(ctx, q, `SELECT coalesce(string_agg(org::text, ', ' ORDER BY org), '') FROM lokadb.orgs`)
	if err != nil {
		return "", err
	}
	if moved[0][0] != "" {
		return fmt.Sprintf("%s: cannot be added while organizations are moved (%s): their rows of it would stay in the cloud",
			strings.Join(names, ", "), moved[0][0]), nil
	}
	if running, err := MoveRunning(ctx, q); err != nil {
		return "", err
	} else if running {
		return "a move-org is running: add tables once it is done", nil
	}
	return "", nil
}

func (r *runner) plan(spec Spec) (*plan, error) {
	p := &plan{tenant: map[string]string{}, key: map[string]string{}, enabled: map[string]bool{},
		keyType: map[string]string{}, refsType: map[string]string{}, tenantKind: map[string]string{}, refsOf: map[string]string{}, cloudOf: map[string]string{}, urefOf: map[string]string{}, urefs: map[string]map[string]string{}}
	done, err := r.rows(`SELECT table_name, tenant_column, key_column FROM lokadb.tables`)
	if err != nil {
		return nil, err
	}
	if spec.Routing != nil {
		p.keys = spec.Routing.Keys
	}
	urefs, err := r.rows(`SELECT table_name, refs_table FROM lokadb.unique_refs`)
	if err != nil {
		return nil, err
	}
	for _, row := range urefs {
		p.urefOf[row[1]] = row[0]
	}
	for _, row := range done {
		p.enabled[row[0]], p.tenant[row[0]], p.key[row[0]] = true, row[1], row[2]
		p.refsOf[hn(row[0], "__refs")], p.cloudOf[hn(row[0], "__cloud")] = row[0], row[0]
	}
	for _, t := range spec.Tables {
		t.Name, t.ParentTable = RelKey(SplitKey(t.Name)), parentKey(t.ParentTable)
		want := t.TenantColumn
		if want == "" {
			want = p.tenant[t.ParentTable]
		}
		if want == "" {
			return nil, fmt.Errorf("%s: parent %q must be listed before it", t.Name, t.ParentTable)
		}
		if p.enabled[t.Name] {
			if p.tenant[t.Name] != want {
				return nil, fmt.Errorf("%s is enabled with organization column %s, the spec says %s", t.Name, p.tenant[t.Name], want)
			}
			continue
		}
		p.tenant[t.Name] = want
		p.todo = append(p.todo, t)
		p.names = append(p.names, t.Name)
		if t.TenantColumn == "" && !p.enabled[t.ParentTable] {
			if p.parentOfDerived == nil {
				p.parentOfDerived = map[string]bool{}
			}
			p.parentOfDerived[t.ParentTable] = true
		}
	}
	if len(p.todo) > 0 && len(p.enabled) > 0 {
		if why, err := AddingTablesRefused(r.ctx, r.tx, p.names); err != nil {
			return nil, err
		} else if why != "" {
			return nil, errors.New(why)
		}
	}
	for _, t := range p.names {
		k, err := r.tableKey(t)
		if err != nil {
			return nil, err
		}
		p.key[t], p.keyType[t], p.refsType[t] = k.col, k.typ, k.refsType
	}
	for _, st := range p.todo {
		if st.TenantColumn != "" {
			continue
		}
		// enable adds the organization column to a child (enable -prepare has added it
		// already): one of that name the table has of its own would fail ADD COLUMN in the
		// middle of enable (review 3 of check, M1)
		has, err := r.rows(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass($1) AND attname = $2 AND attnum > 0 AND NOT attisdropped)
			AND NOT EXISTS (SELECT FROM lokadb.prepare WHERE table_name = $3)`, qr(st.Name), p.tenant[st.Name], st.Name)
		if err != nil {
			return nil, err
		}
		if has[0][0] == "true" {
			return nil, ChildTenantClash(st.Name, p.tenant[st.Name], st.ParentTable)
		}
	}
	for _, st := range p.todo {
		if st.TenantColumn == "" && Composite(p.key[st.ParentTable]) {
			return nil, fmt.Errorf("%s finds its organization through %s, whose key has several columns (%s): lokadb does not follow a composite reference to a parent yet; give %s an organization column of its own (tenant_column)",
				st.Name, st.ParentTable, strings.Join(KeyCols(p.key[st.ParentTable]), ", "), st.Name)
		}
	}
	for t := range p.enabled { // a routing key of a table enabled before is prepared by its key
		if p.keyType[t] != "" || !r.relExists(t) {
			continue
		}
		k, err := r.keyOfColumn(t, p.key[t])
		if err != nil {
			return nil, err
		}
		p.keyType[t] = k.typ
	}
	if err := r.tenantKinds(p); err != nil {
		return nil, err
	}
	return p, nil
}

// tenantKinds fills p.tenantKind (block 18): a root table of the spec by its organization
// column, a child by its parent's, a table enabled before by its own; every one of the same
// kind, since they hold the numbers of the same organizations.
func (r *runner) tenantKinds(p *plan) error {
	for t := range p.enabled {
		if !r.relExists(t) {
			continue
		}
		kind, _, err := r.tenantKindOf(t, p.tenant[t])
		if err != nil {
			return err
		}
		p.tenantKind[t] = kind
	}
	for _, st := range p.todo {
		if st.TenantColumn == "" {
			p.tenantKind[st.Name] = p.tenantKind[st.ParentTable]
			continue
		}
		kind, base, err := r.tenantKindOf(st.Name, st.TenantColumn)
		if err != nil {
			return err
		}
		if kind == "" {
			if base == "" {
				return fmt.Errorf("%s has no column %s", st.Name, st.TenantColumn)
			}
			return fmt.Errorf("%s.%s is %s: lokadb takes organization columns of type %s", st.Name, st.TenantColumn, base, TenantTypes)
		}
		p.tenantKind[st.Name] = kind
	}
	first, kind := "", ""
	for t, k := range p.tenantKind {
		if k == "" {
			continue
		}
		if kind != "" && k != kind {
			return fmt.Errorf("the organization columns of %s (%s) and %s (%s) are of different kinds: the LokaDB tables of a database hold the numbers of the same organizations", first, kind, t, k)
		}
		first, kind = t, k
	}
	return nil
}

// fks collects the FKs that touch the tables this run converts: every one must move, Postgres
// cannot keep a real FK across a foreign partition. lookups: (table, column, refs table) of
// a new table's FKs onto an enabled table, which already point at its refs table and stay.
// Matched by the tables' keys (lokadb.key_of), not by regclass text: that is quoted for names
// that need it ("order", "Doc"), and such FKs were missed. conparentid = 0: a partition's copy of an FK
// goes with its parent's.
func (r *runner) fks(p *plan) (fks []fk, lookups [][3]string, err error) {
	rows, err := r.rows(`SELECT c.conname, lokadb.key_of(s.oid), lokadb.key_of(d.oid),
		pg_get_constraintdef(c.oid), c.conrelid::regclass::text,
		(lokadb.key_of(s.oid) = ANY($1))::text,
		(lokadb.key_of(d.oid) = ANY($1))::text,
		to_json(ARRAY(SELECT a.attname FROM unnest(c.conkey) WITH ORDINALITY k(n, o)
			JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.n ORDER BY k.o))::text,
		to_json(ARRAY(SELECT a.attname FROM unnest(c.confkey) WITH ORDINALITY k(n, o)
			JOIN pg_attribute a ON a.attrelid = c.confrelid AND a.attnum = k.n ORDER BY k.o))::text
		FROM pg_constraint c
		JOIN pg_class s ON s.oid = c.conrelid
		JOIN pg_class d ON d.oid = c.confrelid
		WHERE c.contype = 'f' AND c.conparentid = 0 AND c.conname NOT LIKE '%\_\_lokadb'
		  AND (lokadb.key_of(s.oid) = ANY($1) OR lokadb.key_of(d.oid) = ANY($1))`, p.names)
	if err != nil {
		return nil, nil, err
	}
	for _, row := range rows {
		f := fk{name: row[0], src: row[1], dst: row[2], def: row[3], srcSQL: row[4], srcLokadb: row[5] == "true", dstLokadb: row[6] == "true"}
		if err := json.Unmarshal([]byte(row[7]), &f.cols); err != nil {
			return nil, nil, err
		}
		if err := json.Unmarshal([]byte(row[8]), &f.refs); err != nil {
			return nil, nil, err
		}
		if p.refsOf[f.dst] != "" || p.urefOf[f.dst] != "" {
			if col, ok := fkLookup(f.cols, f.refs); ok && f.srcLokadb {
				lookups = append(lookups, [3]string{f.src, col, f.dst})
			}
			continue
		}
		if t := p.cloudOf[f.src]; t != "" {
			// an enabled table's partition onto a new table: re-added on that partition
			f.src, f.srcLokadb, f.srcEnabled = t, true, true
		}
		// Onto a LokaDB table an FK can point at its refs table only: the key, or the
		// organization column and the key (a tenant-scoped FK).
		if _, ok := RefsColumns(f.refs, p.key[f.dst], p.tenant[f.dst]); f.dstLokadb && !ok && len(f.refs) == 1 {
			// a unique column other than the key, alone (block 4, item 2): onto its map
			typ, err := r.uniqueColumn(f.dst, f.refs[0])
			if err != nil {
				return nil, nil, err
			}
			for _, st := range p.todo {
				if typ != "" && st.Name == f.src && st.ParentTable == f.dst && st.ParentColumn == f.cols[0] {
					return nil, nil, fmt.Errorf("%s finds its organization through %s.%s by FK %s, a unique column of %s other than its key %s: not supported yet (the parent's key is how a child's rows find their organization)",
						f.src, f.src, f.cols[0], f.name, f.dst, p.key[f.dst])
				}
			}
			if typ != "" {
				f.uref = f.refs[0]
				if p.urefs[f.dst] == nil {
					p.urefs[f.dst] = map[string]string{}
				}
				p.urefs[f.dst][f.uref] = typ
				fks = append(fks, f)
				continue
			}
		}
		if _, ok := RefsColumns(f.refs, p.key[f.dst], p.tenant[f.dst]); f.dstLokadb && !ok {
			return nil, nil, fmt.Errorf("FK %s on %s points at %s (%s), not at its key %s (with the organization column %s or without): not supported yet",
				f.name, f.srcSQL, f.dst, strings.Join(f.refs, ", "), p.key[f.dst], p.tenant[f.dst])
		}
		fks = append(fks, f)
	}
	return fks, lookups, nil
}

// refsDef is the definition of an FK onto a LokaDB table pointed at its refs table, which
// holds the key in its column id and the organization in tenant; cols: the refs columns.
func refsDef(f fk, p *plan) (string, []string, error) {
	m := refRe(f.dst).FindStringSubmatch(f.def)
	if m == nil {
		return "", nil, fmt.Errorf("FK %s on %s: its target is not readable in %q", f.name, f.srcSQL, f.def)
	}
	if f.uref != "" {
		return strings.Replace(f.def, m[0], "REFERENCES "+qr(urefTable(f.dst, f.uref))+"(id)", 1), []string{"id"}, nil
	}
	cols, _ := RefsColumns(f.refs, p.key[f.dst], p.tenant[f.dst])
	return strings.Replace(f.def, m[0], "REFERENCES "+qr(hn(f.dst, "__refs"))+"("+strings.Join(cols, ", ")+")", 1), cols, nil
}

// uniqueColumn: the type of column col of table t when a unique index of that column alone,
// without a predicate, is on t (or on its DEFAULT partition, when t is enabled); "" if none.
func (r *runner) uniqueColumn(t, col string) (string, error) {
	rows, err := r.rows(`SELECT format_type(a.atttypid, a.atttypmod) FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
		WHERE i.indrelid IN (to_regclass($1), to_regclass($2)) AND i.indisunique AND i.indnkeyatts = 1
		  AND i.indpred IS NULL AND i.indexprs IS NULL AND i.indimmediate AND a.attname = $3 LIMIT 1`, qr(t), qr(hn(t, "__cloud")), col)
	if err != nil || len(rows) == 0 {
		return "", err
	}
	return rows[0][0], nil
}

// preparedFor: whether Prepare made the tables of this run ready. Some of them prepared,
// not all, or not to the end: refused.
func (r *runner) preparedFor(p *plan) (bool, error) {
	rows, err := r.rows(`SELECT table_name, stage FROM lokadb.prepare WHERE table_name = ANY($1)`, p.names)
	if err != nil || len(rows) == 0 {
		return false, err
	}
	stage := map[string]string{}
	for _, row := range rows {
		stage[row[0]] = row[1]
	}
	var not []string
	for _, t := range p.names {
		if stage[t] != "ready" {
			not = append(not, t+" ("+coalesce(stage[t], "not started")+")")
		}
	}
	if len(not) > 0 {
		return false, fmt.Errorf("enable -prepare has not finished: %s; run it again", strings.Join(not, ", "))
	}
	return true, nil
}

func coalesce(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

// verifyPrepared checks, before the switch takes a lock, that what Prepare made agrees with
// the tables now: the refs tables and the organization column of child tables (the triggers
// keep them so from here on), the FKs onto the refs tables, the key indexes. Then it locks
// the tables, waiting at most timeout.
func (r *runner) verifyPrepared(p *plan, fks []fk, timeout time.Duration) error {
	// A3: what the application changes while the counts below run is not in them. A parent moved
	// to another organization in that time leaves its children's copy behind; the switch has
	// no time to count again under its lock (a pass over every row, the tables locked), so it
	// counts the moves the log holds (lokadb.prep_moved_count) now and once more after the lock.
	epoch := func() (string, error) {
		n, err := r.rows(`SELECT lokadb.prep_moved_count()::text`)
		if err != nil {
			return "", err
		}
		return n[0][0], nil
	}
	before, err := epoch()
	if err != nil {
		return err
	}
	var wrong []string
	count := func(what, sql string) error {
		n, err := r.rows(sql)
		if err != nil {
			return err
		}
		if n[0][0] != "0" {
			wrong = append(wrong, fmt.Sprintf("%s: %s", what, n[0][0]))
		}
		return nil
	}
	for _, st := range p.todo {
		t, tc, k := st.Name, p.tenant[st.Name], p.key[st.Name]
		if err := count(t+"__refs rows that differ from "+t, fmt.Sprintf(`SELECT count(*)::text FROM %s x FULL JOIN %s r ON %s
			WHERE r.%s IS NULL OR x.%s IS NULL OR r.tenant IS DISTINCT FROM x.%s`, qr(t), qr(hn(t, "__refs")),
			keyEq("r.", RefsKeyCols(k), "x.", KeyCols(k)), q(RefsKeyCols(k)[0]), q(KeyCols(k)[0]), q(tc))); err != nil {
			return err
		}
		triggers := []string{prepRefsTrigger}
		if p.parentOfDerived[t] {
			triggers = append(triggers, prepMovedTrigger) // (a prepare of an older version has none: run it again)
		}
		if st.TenantColumn == "" {
			triggers = append(triggers, prepTenantTrigger, prepTenantAfter)
			if err := count(t+" rows whose "+tc+" differs from their parent's", fmt.Sprintf(`SELECT count(*)::text FROM %s c
				LEFT JOIN %s p ON p.id = c.%s WHERE c.%s IS DISTINCT FROM p.tenant`,
				qr(t), qr(hn(st.ParentTable, "__refs")), q(st.ParentColumn), q(tc))); err != nil {
				return err
			}
		}
		if err := count(t+" triggers of enable -prepare missing or disabled", fmt.Sprintf(`SELECT (%d - count(*))::text FROM pg_trigger
			WHERE tgrelid = %s::regclass AND tgname = ANY ('{%s}') AND tgenabled <> 'D'`, len(triggers), lit(qr(t)), strings.Join(triggers, ","))); err != nil {
			return err
		}
		if err := count(t+" key index missing or invalid", fmt.Sprintf(`SELECT (1 - count(*))::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			WHERE i.indrelid = %s::regclass AND c.relname = %s AND i.indisvalid`, lit(qr(t)), lit(keyIndex(t)))); err != nil {
			return err
		}
	}
	if err := r.verifyKeys(p, count); err != nil {
		return err
	}
	for _, f := range fks {
		if !f.dstLokadb {
			continue
		}
		if err := count("FK "+prepFK(f.name)+" on "+f.srcSQL+" missing or not validated", fmt.Sprintf(`SELECT (1 - count(*))::text FROM pg_constraint
			WHERE conrelid = %s::regclass AND conname = %s AND convalidated`, lit(f.srcSQL), lit(prepFK(f.name)))); err != nil {
			return err
		}
	}
	if len(wrong) > 0 {
		return fmt.Errorf("what enable -prepare made does not agree with the tables (%s): run enable -prepare again", strings.Join(wrong, "; "))
	}
	// From here every lock waits at most timeout: a lock queued behind a long transaction
	// would stop every other session behind it. EnableWith tries again.
	var rels []string
	for _, t := range p.names {
		rels = append(rels, qr(t), qr(hn(t, "__refs")))
	}
	for _, f := range fks {
		if f.dstLokadb {
			rels = append(rels, f.srcSQL)
		}
	}
	if err := r.exec(fmt.Sprintf("SET LOCAL lock_timeout = %d; LOCK TABLE %s IN ACCESS EXCLUSIVE MODE",
		timeout.Milliseconds(), strings.Join(dedup(rels), ", "))); err != nil {
		return err
	}
	after, err := epoch()
	if err != nil {
		return err
	}
	if after != before {
		return fmt.Errorf("a table with child tables changed an organization between the check of enable -prepare and the lock: their copy of it may be behind: run enable -prepare again, then enable")
	}
	return nil
}

func dedup(s []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, x := range s {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

// maxUnpreparedDowntime: enable without Prepare is refused (unless forced) when its tables are
// expected to stay locked longer than this.
const maxUnpreparedDowntime = 10 * time.Second

// warnDowntime says how long enable without Prepare is expected to lock the tables, and refuses
// it when that is longer than maxUnpreparedDowntime and allow is not set. A table never
// analyzed has reltuples 0 or -1: its size stands in (about 100 bytes a row).
func (r *runner) warnDowntime(p *plan, allow bool) error {
	if len(p.names) == 0 {
		return nil
	}
	n, err := r.rows(`SELECT coalesce(sum(greatest(c.reltuples, pg_relation_size(c.oid) / 100, 0)), 0)::bigint::text FROM pg_class c
		WHERE c.oid = ANY (SELECT to_regclass(lokadb.rel(t)) FROM unnest($1::text[]) t)`, p.names)
	if err != nil {
		return err
	}
	var rows int64
	fmt.Sscan(n[0][0], &rows)
	if !allow && time.Duration(rows/rowsPerSecond)*time.Second > maxUnpreparedDowntime {
		return fmt.Errorf("enable without -prepare would lock the %d tables of the spec for about %s (%d rows), and every statement on them would wait: "+
			"run lokadb enable -prepare first (online, can be repeated), then enable switches in a short lock; or -allow-downtime if that downtime is acceptable",
			len(p.names), (time.Duration(rows/rowsPerSecond) * time.Second).String(), rows)
	}
	if rows > 5*rowsPerSecond {
		r.log.Warn("enable without -prepare locks its tables, and every statement on them waits, until it is done",
			"rows", rows, "expected", (time.Duration(rows/rowsPerSecond) * time.Second).String(),
			"instead", "lokadb enable -prepare (online, can be repeated), then lokadb enable (a short switch)")
	}
	return nil
}

// prep is a run of Prepare.
type prep struct {
	ctx  context.Context
	conn *pgx.Conn
	o    EnableOptions
	p    *plan
}

// Prepare is the first step of a two-step enable (see EnableWith): online, it can be
// stopped and run again. The tables keep working as they are until enable switches them.
func Prepare(ctx context.Context, o EnableOptions) error {
	if o.LockTimeout == 0 {
		o.LockTimeout = 5 * time.Second
	}
	conn, err := connectPlain(ctx, o.DSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	var p *plan
	var fks []fk
	if err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		r := &runner{ctx: ctx, tx: tx, log: o.Log}
		if err := r.exec(baseSQL); err != nil {
			return err
		}
		if p, err = r.plan(o.Spec); err != nil {
			return err
		}
		for _, t := range p.names {
			if err := r.unsupported(t); err != nil {
				return err
			}
			if err := r.carryCheck(t); err != nil {
				return err
			}
		}
		fks, _, err = r.fks(p)
		return err
	}); err != nil {
		return err
	}
	if len(p.todo) == 0 {
		o.Log.Info("every table of the spec is enabled: nothing to prepare")
		return nil
	}
	c := &prep{ctx: ctx, conn: conn, o: o, p: p}
	o.Log.Info("preparing online (writes go on)", "tables", len(p.todo))
	// parents before children: a child's organization comes from its parent's refs table
	for _, st := range p.todo {
		if err := c.table(st); err != nil {
			return fmt.Errorf("%s: %w", st.Name, err)
		}
	}
	for _, f := range fks {
		if err := c.fk(f); err != nil {
			return err
		}
	}
	if err := c.routing(o.Spec); err != nil {
		return err
	}
	for _, st := range p.todo {
		if err := c.stage(st.Name, "index", 0); err != nil {
			return err
		}
		if err := buildKeyIndex(ctx, conn, st.Name, keyIndex(st.Name), tenantKey(p.tenant[st.Name], p.tenantKind[st.Name]), o.Log); err != nil {
			return fmt.Errorf("%s: %w", st.Name, err)
		}
		if err := c.stage(st.Name, "ready", 0); err != nil {
			return err
		}
	}
	o.Log.Info("prepared: the tables are ready for the switch (lokadb enable)", "tables", strings.Join(p.names, ", "))
	return nil
}

// Prepared: every table of the spec that is not enabled yet has been prepared to the end
// (Prepare reached "ready" with it). The switch still checks the tables itself.
func Prepared(ctx context.Context, o EnableOptions) (bool, error) {
	conn, err := connectPlain(ctx, o.DSN)
	if err != nil {
		return false, err
	}
	defer conn.Close(ctx)
	var ok bool
	err = pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
		r := &runner{ctx: ctx, tx: tx, log: o.Log}
		if err := r.exec(baseSQL); err != nil {
			return err
		}
		p, err := r.plan(o.Spec)
		if err != nil {
			return err
		}
		if len(p.todo) == 0 {
			return nil
		}
		ok, err = r.preparedFor(p)
		return err
	})
	if err != nil {
		return false, nil // not started, or not finished: Prepare is what to do
	}
	return ok, nil
}

// stage records where Prepare is with a table (total: its estimated rows, 0: unchanged).
func (c *prep) stage(t, stage string, total int64) error {
	_, err := c.conn.Exec(c.ctx, `INSERT INTO lokadb.prepare (table_name, stage, total) VALUES ($1, $2, $3)
		ON CONFLICT (table_name) DO UPDATE SET stage = $2, total = greatest($3, lokadb.prepare.total), updated = now(),
		  done = CASE WHEN lokadb.prepare.stage = $2 THEN lokadb.prepare.done ELSE 0 END,
		  last = CASE WHEN lokadb.prepare.stage = $2 THEN lokadb.prepare.last END`, t, stage, total)
	return err
}

// ddl runs short catalog changes in one transaction, waiting for their locks at most
// LockTimeout, and tries again: a lock queued behind a long transaction stops everyone.
// The changes of one call lock one table of the application, no more: two, in an order the
// application's transactions may take the other way, can deadlock them.
func (c *prep) ddl(stmts ...string) error {
	for attempt := 1; ; attempt++ {
		err := pgx.BeginFunc(c.ctx, c.conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(c.ctx, fmt.Sprintf("SET LOCAL lock_timeout = %d", c.o.LockTimeout.Milliseconds())); err != nil {
				return err
			}
			for _, s := range stmts {
				if _, err := tx.Exec(c.ctx, s); err != nil {
					return fmt.Errorf("%w\n  in: %s", err, mask(s))
				}
			}
			return nil
		})
		var pe *pgconn.PgError
		if err == nil || !errors.As(err, &pe) || pe.Code != "55P03" || attempt >= max(c.o.Attempts, 10) {
			return err
		}
		c.o.Log.Info("waiting for a lock", "attempt", attempt, "lock_timeout", c.o.LockTimeout)
		time.Sleep(time.Second)
	}
}

func (c *prep) has(sql string, args ...any) (bool, error) {
	r, err := rowsOf(c.ctx, c.conn, sql, args...)
	if err != nil {
		return false, err
	}
	return r[0][0] == "true", nil
}

// table prepares one table: a child's organization column, then the refs table.
func (c *prep) table(st SpecTable) error {
	t, tc, k := st.Name, c.p.tenant[st.Name], c.p.key[st.Name]
	refs := hn(t, "__refs")
	est, err := rowsOf(c.ctx, c.conn, `SELECT greatest(reltuples, 0)::bigint::text FROM pg_class WHERE oid = $1::regclass`, qr(t))
	if err != nil {
		return err
	}
	var total int64
	fmt.Sscan(est[0][0], &total)
	writers, err := c.privileges(t, KeyCols(k)[0], tc)
	if err != nil {
		return err
	}
	if st.TenantColumn == "" {
		// The column without a default: no rewrite. The trigger gives it to the rows the
		// application writes from now on, the batches to the rows already there.
		stmts := []string{fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", qr(t), q(tc), tenantType(c.p.tenantKind[t]))}
		if ok, err := c.has(`SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)::text`, qr(t), prepTenantTrigger); err != nil {
			return err
		} else if !ok {
			for _, w := range [][2]string{{prepTenantTrigger, "BEFORE"}, {prepTenantAfter, "AFTER"}} {
				stmts = append(stmts, fmt.Sprintf("CREATE TRIGGER %s %s INSERT OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.prep_tenant(%s, %s, %s, %s)",
					w[0], w[1], q(st.ParentColumn), qr(t), lit(hn(st.ParentTable, "__refs")), lit(st.ParentColumn), lit(tc), lit(k)))
			}
		}
		// the trigger reads the parent's refs table as the writer
		for _, w := range writers {
			if w[1] == "INSERT" || w[1] == "UPDATE" {
				stmts = append(stmts, fmt.Sprintf("GRANT SELECT ON %s TO %s", qr(hn(st.ParentTable, "__refs")), w[0]))
			}
		}
		// the parent's organization is copied into this table's column: a change of it is
		// noted (prep_moved), so the switch can tell that the copy may be behind. Its own
		// transaction: one with the child would lock the child, then the parent, while the
		// application writes the parent, then the child (a deadlock, E14).
		if pt := st.ParentTable; c.p.parentOfDerived[pt] {
			if ok, err := c.has(`SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)::text`, qr(pt), prepMovedTrigger); err != nil {
				return err
			} else if !ok {
				if err := c.ddl(fmt.Sprintf("CREATE TRIGGER %s AFTER UPDATE OF %s ON %s FOR EACH ROW WHEN (OLD.%s IS DISTINCT FROM NEW.%s) EXECUTE FUNCTION lokadb.prep_moved()",
					prepMovedTrigger, q(c.p.tenant[pt]), qr(pt), q(c.p.tenant[pt]), q(c.p.tenant[pt]))); err != nil {
					return err
				}
			}
		}
		if err := c.ddl(stmts...); err != nil {
			return err
		}
		if err := c.stage(t, "tenant", total); err != nil {
			return err
		}
		// a row whose parent is missing keeps NULL: no organization (<table>__none)
		bk := newBKey(k, c.p.keyType[t])
		if err := c.batches(t, bk, fmt.Sprintf(`WITH b AS (SELECT %[1]s FROM %[2]s x WHERE %[7]s ORDER BY %[8]s LIMIT %[3]d),
			u AS (UPDATE %[2]s c SET %[4]s = (SELECT p.tenant FROM %[5]s p WHERE p.id = c.%[6]s)
				WHERE %[9]s IN (SELECT %[10]s FROM b) AND c.%[4]s IS DISTINCT FROM (SELECT p.tenant FROM %[5]s p WHERE p.id = c.%[6]s) RETURNING 1)
			SELECT %[11]s, (SELECT count(*) FROM b)::text, (SELECT count(*) FROM u)::text`,
			bk.sel("x."), qr(t), prepBatch, q(tc), qr(hn(st.ParentTable, "__refs")), q(st.ParentColumn), bk.within("x."), bk.order("x.", false),
			bk.row("c."), bk.names(), bk.lastOf("b"))); err != nil {
			return err
		}
	}
	stmts := []string{
		fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s %s", qr(refs), refsDDL(k, c.p.refsType[t], tenantType(c.p.tenantKind[t]))),
		fmt.Sprintf("ALTER TABLE %s OWNER TO %s", qr(refs), q(c.o.Owner)),
		"GRANT USAGE ON SCHEMA lokadb TO " + q(c.o.Owner),
	}
	// the trigger writes the refs table as the writer
	for _, w := range writers {
		stmts = append(stmts, fmt.Sprintf("GRANT %s ON %s TO %s", w[1], qr(refs), w[0]), "GRANT USAGE ON SCHEMA lokadb TO "+w[0])
	}
	if ok, err := c.has(`SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = $1::regclass AND tgname = $2)::text`, qr(t), prepRefsTrigger); err != nil {
		return err
	} else if !ok {
		stmts = append(stmts, fmt.Sprintf("CREATE TRIGGER %s AFTER INSERT OR DELETE OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION lokadb.refs_sync(%s, %s, %s)",
			prepRefsTrigger, trigCols(k, tc), qr(t), lit(refs), lit(tc), lit(k)))
	}
	if err := c.ddl(stmts...); err != nil {
		return err
	}
	if err := c.stage(t, "refs", total); err != nil {
		return err
	}
	// FOR SHARE: a row of the batch cannot be deleted or changed until the batch commits,
	// and after that the trigger keeps its refs row.
	bk := newBKey(k, c.p.keyType[t])
	ids := refsColList(k, "")
	if err := c.batches(t, bk, fmt.Sprintf(`WITH b AS (SELECT %[1]s, x.%[2]s AS tc FROM %[3]s x WHERE %[6]s ORDER BY %[7]s LIMIT %[4]d FOR SHARE),
		u AS (INSERT INTO %[5]s (%[8]s, tenant) SELECT %[9]s, tc FROM b
			ON CONFLICT (%[8]s) DO UPDATE SET tenant = EXCLUDED.tenant WHERE %[5]s.tenant IS DISTINCT FROM EXCLUDED.tenant RETURNING 1)
		SELECT %[10]s, (SELECT count(*) FROM b)::text, (SELECT count(*) FROM u)::text`,
		bk.sel("x."), q(tc), qr(t), prepBatch, qr(refs), bk.within("x."), bk.order("x.", false), ids, bk.names(), bk.lastOf("b"))); err != nil {
		return err
	}
	// refs rows of rows gone since an earlier run (it was stopped, or the triggers were)
	_, err = c.conn.Exec(c.ctx, fmt.Sprintf("DELETE FROM %s r WHERE NOT EXISTS (SELECT 1 FROM %s x WHERE %s)", qr(refs), qr(t), keyEq("x.", KeyCols(k), "r.", RefsKeyCols(k))))
	return err
}

// privileges: the roles that read or write the table (its owner too), with each of SELECT,
// INSERT, UPDATE, DELETE they need on its refs table while the triggers write it as them.
func (c *prep) privileges(t, k, tc string) ([][]string, error) {
	return rowsOf(c.ctx, c.conn, `SELECT DISTINCT coalesce(quote_ident(g.rolname), 'PUBLIC'), x.privilege_type
		FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('r', c.relowner))) x LEFT JOIN pg_roles g ON g.oid = x.grantee
		WHERE c.oid = $1::regclass AND x.privilege_type IN ('SELECT', 'INSERT', 'UPDATE', 'DELETE')
		UNION SELECT DISTINCT coalesce(quote_ident(g.rolname), 'PUBLIC'), x.privilege_type
		FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) x LEFT JOIN pg_roles g ON g.oid = x.grantee
		WHERE a.attrelid = $1::regclass AND (x.privilege_type IN ('SELECT', 'INSERT') OR x.privilege_type = 'UPDATE' AND a.attname IN ($2, $3))
		ORDER BY 1, 2`, qr(t), k, tc)
}

// batchFrom stands for the comparison with the lower bound $1 in the SQL of a batch: >= for
// the first batch, from the table's smallest key, > for the others, after the last key done.
// No bound made of the type's smallest value: uuid and text have none to write (block 17),
// and a condition "$1 IS NULL OR" keeps a generic plan off the key's index.
const batchFrom = "__lokadb_from__"

// batches runs sql, a batch from key $1 (batchFrom) up to key $2, until it finds no row, and
// logs the progress. Keys of any kind (block 17) travel as text, cast by sql.
// sql gives the last key, the rows of the batch, the rows it changed.
func (c *prep) batches(t string, k bkey, sql string) error {
	cur, err := rowsOf(c.ctx, c.conn, `SELECT stage, done::text, total::text, coalesce(last::text, '') FROM lokadb.prepare WHERE table_name = $1`, t)
	if err != nil {
		return err
	}
	stage := cur[0][0]
	var done, total int64
	fmt.Sscan(cur[0][1], &done)
	fmt.Sscan(cur[0][2], &total)
	// Up to the last key there now: rows written later have the trigger. Chasing them made
	// every batch find a few new rows of a table under writes, and never end. By the key's
	// index, not max(): uuid has no max() before Postgres 18. Cast to text outside: in
	// "SELECT k::text … ORDER BY k" the ORDER BY names the output column, the text, and 9
	// came after 20.
	bounds, err := rowsOf(c.ctx, c.conn, fmt.Sprintf("SELECT coalesce((SELECT %[1]s FROM %[2]s x ORDER BY %[3]s LIMIT 1)::text, ''), coalesce((SELECT %[1]s FROM %[2]s x ORDER BY %[4]s LIMIT 1)::text, '')",
		k.text("x."), qr(t), k.order("x.", false), k.order("x.", true)))
	if err != nil || bounds[0][1] == "" {
		return err
	}
	last, from := bounds[0][0], ">=" // from the smallest key
	if cur[0][3] != "" {             // resumed after the batch a stopped run did last
		last, from = cur[0][3], ">"
	}
	start, logged, changed := time.Now(), time.Now(), int64(0)
	for {
		r, err := rowsOf(c.ctx, c.conn, strings.Replace(sql, batchFrom, from, 1), last, bounds[0][1])
		if err != nil {
			return err
		}
		if r[0][1] == "0" {
			break
		}
		var n, ch int64
		fmt.Sscan(r[0][1], &n)
		fmt.Sscan(r[0][2], &ch)
		done, changed, last, from = done+n, changed+ch, r[0][0], ">"
		if _, err := c.conn.Exec(c.ctx, `UPDATE lokadb.prepare SET done = $2, last = $3, updated = now() WHERE table_name = $1`, t, done, last); err != nil {
			return err
		}
		if time.Since(logged) > 5*time.Second {
			logged = time.Now()
			args := []any{"table", t, "stage", stage, "rows", done, "of about", total}
			if rate := float64(done) / time.Since(start).Seconds(); rate > 0 && total > done {
				args = append(args, "left", (time.Duration(float64(total-done)/rate) * time.Second).Round(time.Second).String())
			}
			c.o.Log.Info("enable -prepare", args...)
		}
	}
	c.o.Log.Info("enable -prepare", "table", t, "stage", stage, "rows", done, "changed", changed, "took", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// fk adds the FK onto the refs table next to the one onto the table: NOT VALID (a short
// lock, no scan), then VALIDATE (writes go on). Both hold until enable drops the old one.
func (c *prep) fk(f fk) error {
	if !f.dstLokadb {
		return nil // onto a cloud table: it stays as it is
	}
	if f.uref != "" {
		return fmt.Errorf("FK %s on %s points at %s (%s), a unique column other than the key: enable -prepare does not build that column's map yet; run enable without -prepare", f.name, f.srcSQL, f.dst, f.uref)
	}
	def, _, err := refsDef(f, c.p)
	if err != nil {
		return err
	}
	name := prepFK(f.name)
	st, err := rowsOf(c.ctx, c.conn, `SELECT convalidated::text FROM pg_constraint WHERE conrelid = $1::regclass AND conname = $2`, f.srcSQL, name)
	if err != nil {
		return err
	}
	if len(st) == 0 {
		if err := c.ddl(fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s NOT VALID", f.srcSQL, q(name), def)); err != nil {
			return err
		}
	} else if st[0][0] == "true" {
		return nil
	}
	start := time.Now()
	if err := c.ddl(fmt.Sprintf("ALTER TABLE %s VALIDATE CONSTRAINT %s", f.srcSQL, q(name))); err != nil {
		return err
	}
	c.o.Log.Info("enable -prepare: FK onto the refs table", "table", f.srcSQL, "fk", name, "took", time.Since(start).Round(time.Millisecond).String())
	return nil
}

// buildKeyIndex builds index name on table (a key; expr) CONCURRENTLY, unless it is there and valid;
// an invalid one (an interrupted build) is dropped first. CREATE INDEX CONCURRENTLY waits
// for every transaction older than it, silently: who it waits for is logged.
func buildKeyIndex(ctx context.Context, conn *pgx.Conn, table, name, expr string, log *slog.Logger) error {
	rel := qr(table)
	r, err := rowsOf(ctx, conn, `SELECT i.indisvalid::text FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1 AND c.relnamespace = $2::regnamespace`, name, q(schemaOf(table)))
	if err != nil {
		return err
	}
	if len(r) == 1 && r[0][0] == "true" {
		return nil
	}
	if len(r) == 1 {
		if _, err := conn.Exec(ctx, "DROP INDEX CONCURRENTLY IF EXISTS "+qr(inSchemaOf(table, name))); err != nil {
			return err
		}
	}
	stop := watchWaits(ctx, conn, "CREATE INDEX CONCURRENTLY "+name, log)
	start := time.Now()
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE INDEX CONCURRENTLY IF NOT EXISTS %s ON %s (%s)", q(name), rel, expr))
	stop()
	if err != nil {
		return fmt.Errorf("index %s on the organization column: %w", name, err)
	}
	log.Info("index on the organization column", "table", rel, "index", name, "took", time.Since(start).Round(time.Millisecond))
	return nil
}

// watchWaits logs, every 5 s, the sessions conn's statement waits for: their pid, role,
// application, state and how long their transaction runs (not their queries: they may hold
// the application's data).
func watchWaits(ctx context.Context, conn *pgx.Conn, what string, log *slog.Logger) (stop func()) {
	pid := conn.PgConn().PID()
	cfg := conn.Config().Copy()
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var w *pgx.Conn
		for {
			select {
			case <-done:
				if w != nil {
					w.Close(context.Background())
				}
				return
			case <-time.After(5 * time.Second):
			}
			if w == nil {
				var err error
				ownsql.Configure(cfg)
				if w, err = pgx.ConnectConfig(ctx, cfg); err != nil {
					continue
				}
			}
			rows, err := rowsOf(ctx, w, `SELECT a.pid::text, coalesce(a.usename::text, ''), coalesce(a.application_name, ''), coalesce(a.state, ''),
				coalesce(date_trunc('second', now() - a.xact_start)::text, '')
				FROM pg_stat_activity a WHERE a.pid = ANY (pg_blocking_pids($1)) ORDER BY a.xact_start`, pid)
			if err != nil {
				continue
			}
			for _, b := range rows {
				log.Info(what+" waits for an older transaction to end", "pid", b[0], "role", b[1], "application", b[2], "state", b[3], "transaction_for", b[4])
			}
		}
	}()
	return func() { close(done); <-finished }
}
