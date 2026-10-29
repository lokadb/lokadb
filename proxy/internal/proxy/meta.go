package proxy

import (
	"errors"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/rel"
)

// Table describes one LokaDB-managed table as recorded by `lokadb enable`.
type Table struct {
	Name      string
	TenantCol string
	Refs      string // <table>__refs: key -> tenant map kept in the cloud DB (its column id)
	Key       string // the column refs maps: the one-column primary key, or id
	Cloud     string // default partition holding all cloud tenants
	Cols      []string
	ColType   map[string]string
	// OrgValue: the organizations are named by their value, a uuid or a string (Meta.TenantKind):
	// the organization goes into SQL quoted, a numeric-looking string too (Table.orgLit).
	OrgValue bool
	// UserTypes: a column has a type outside pg_catalog (enum, domain, composite). Its OID
	// differs in each organization's database, so the direct path leaves the table to the FDW
	// (a client that cached the cloud's description would read the answer by the wrong type).
	UserTypes bool
	// Defaults: the columns with a default of their own (not generated). The organization's
	// tables have none (move-org copies no defaults: postgres_fdw sends every column), so an
	// INSERT on the direct path names them all, but the key and the organization column, which
	// the proxy fills (stage 8b). KeyDefault: the key's default ("" without one), whatever it
	// is: nextval of a sequence, gen_random_uuid(), uuidv7(), a function of an extension of the
	// cloud's (block 17); the cloud computes it for a direct INSERT that names no key, as it does
	// for an INSERT through the parent. Identity, Generated: a column of either kind.
	Defaults            map[string]bool
	KeyDefault          string
	KeyDefaults         map[string]string // a composite key's: the default of each of its columns that has one
	Identity, Generated bool
	// Uniques: the column sets of the DEFAULT partition's valid unique indexes without a
	// predicate or an expression, each sorted and joined by uniqueSep. move-org puts them on the
	// organization's table, the proxy replays the later ones there (create_index): an upsert
	// whose conflict target is one of them, the key or (organization, key) can run natively in
	// the organization's database (direct_write.go, block 2 of 8b.2).
	Uniques map[string]bool
}

const uniqueSep = "\x1f"

// uniqueSet: cols sorted and joined, as Table.Uniques holds them.
func uniqueSet(cols []string) string {
	c := append([]string{}, cols...)
	sort.Strings(c)
	return strings.Join(c, uniqueSep)
}

// unique: cols are the columns of a unique index the organization's table has: the key, the
// organization column with the key (move-org's PRIMARY KEY and UNIQUE), or one of Uniques.
func (t *Table) unique(cols []string) bool {
	set := uniqueSet(cols)
	key := tableKey(t)
	if kc, ok := tupleCols(t.Key); ok { // a composite key: its columns (move-org's PRIMARY KEY)
		return set == uniqueSet(kc) || t.Uniques[set]
	}
	return set == key || set == uniqueSet([]string{t.TenantCol, key}) || t.Uniques[set]
}

// Meta is the routing metadata of one database.
type Meta struct {
	Tables map[string]*Table
	// Denied: the database is enabled (its LokaDB tables have the policy lokadb_tenant) but the
	// metadata could not be read under the client's role (no USAGE on schema lokadb, no SELECT on
	// its tables) or not at all (Retry): the proxy cannot route, so a statement on a LokaDB table
	// is refused with this message instead of going through unrouted, past the fanout policy.
	// Tables, Views and Funcs then name what is refused, nothing else is filled.
	Denied string
	Retry  bool // Denied after a read that failed, not a refusal: read again before the next statement
	// Lookups[table][column] = refs table that maps the column value to a tenant,
	// or "" when the column is the tenant column itself.
	Lookups map[string]map[string]string
	// KeyKinds[refs]: the kind of the values a refs table, a key's map or a cloud table maps
	// (int, uuid, text; keys.go, block 17); missing: int.
	KeyKinds map[string]string
	// TupleKinds[refs]: the kinds of the columns of the composite key refs maps (KeyKinds
	// says kindTuple; tuples.go)
	TupleKinds map[string][]string
	// TenantKind: the kind of the organization columns (int, uuid or text, block 18): an
	// organization is its value there, kept in canonical form ("": int).
	TenantKind string
	// OrgNum[value] = the number N of an organization numbered by uuid (lokadb.tenants, block
	// 18): lokadb.orgs, Profiles, Schemas and the config (customers, customers_app) go by it.
	OrgNum map[string]string
	// Foreign[table][org] = foreign partition of an organization in its own database.
	Foreign map[string]map[string]string
	// Local[table][org] = an organization's own partition in the cloud (<table>__o<N>, while
	// a move splits it out of DEFAULT); Writable[partition]: the session's role may insert
	// into and update a local partition (DEFAULT included) directly. The proxy aims a cloud
	// organization's upsert at it (Analysis.TargetLocal).
	Local    map[string]map[string]string
	Writable map[string]bool
	// Direct[partition]: a foreign partition on which the session's role has every privilege
	// it has on the parent (lokadb.sync_grants gives them): a statement aimed straight at it
	// would otherwise fail with permission denied where the parent allows it (foreignPart).
	Direct map[string]bool
	// GrantSync: the database has lokadb.grant_sync and the session's role may call it (a GRANT
	// on a LokaDB table goes through it).
	GrantSync bool
	// StmtTriggers[table]: the application has a statement-level trigger on the parent.
	// Postgres fires it for a statement on the parent only, not on a partition: writes
	// stay on the parent (foreignPart).
	StmtTriggers map[string]bool
	// RowTriggers[table]: the events (trigInsert, trigDelete, trigUpdate) of the row-level
	// triggers on the parent or one of its partitions other than lokadb_refs and lokadb_parent
	// (the application's, lokadb_fk_* of FKs onto cloud tables, lokadb_fkcascade_* of a cascade
	// at the organization, a move's capture). A write on the direct path would not fire them:
	// a write of an event some trigger fires on goes the cloud's way (stage 8b, rowTrigger).
	RowTriggers map[string]int
	// RefsFromLokadb[table]: a LokaDB table has an FK onto table's refs table (a child whose rows
	// a moved organization keeps in its own database). The cloud checks it through the FDW,
	// on its own connection: a DELETE on the direct path, its rows deleted in the organization's
	// open transaction, would wait there for the cloud's refs delete, which waits for it (a
	// deadlock neither server sees). Such a table deletes the cloud's way (8b, DP18).
	RefsFromLokadb map[string]bool
	// CloudFirst: fingerprints of first statements whose transactions go the cloud's way
	// (lokadb.direct_cloud_first, 8a.2: a direct transaction of that shape met a statement it
	// could not take); CloudFirstTable: the table exists; CloudFirstFull: more rows than
	// maxCloudFirstRows, the rest unread.
	CloudFirst                      map[string]bool
	CloudFirstTable, CloudFirstFull bool
	// Learnt: transaction forms learnt ahead (lokadb.direct_learnt, written from lokadb check
	// -profile fdw+direct -direct-forms): the fingerprint of the first statement, with "#named"
	// when the application named the organization (direct_tx.go's learnKey without the database)
	Learnt map[string]bool
	// SameOrg: the database has lokadb.same_org, which refuses moving a row to another
	// organization: an UPDATE that sets the organization column calls it.
	SameOrg bool
	// Indexes[index] = the LokaDB table whose index (on it or on one of its partitions) it is.
	Indexes map[string]string
	// Schemas[N]: the schema of the organization's tables in its own database, where the
	// proxy replays DDL ("" or missing: public). By the organization's number (orgNum).
	Schemas map[string]string
	// Profiles[N]: the routing profile of a moved organization (lokadb.orgs.profile, block
	// 10; missing: fdw). By the organization's number (orgNum).
	Profiles map[string]string
	// DBIDs[N]: the id of the organization's database in the mark on its tables there
	// (lokadb.orgs.db_id, admin/dbid.go; missing: not recorded). By the organization's number.
	DBIDs map[string]string
	// direct, directKnown: some profile of Profiles has the direct path, counted by LoadMeta
	// (anyDirect; a Meta built otherwise counts each time)
	direct, directKnown bool
	// OrgLock: the database has lokadb.org_lock: statements take their organization's lock,
	// which a move holds while it splits the organization out of DEFAULT (orgLock).
	OrgLock bool
	// Views[view]: the LokaDB tables a view (or materialized view) of public reads, through
	// other views too. Funcs: functions of public, other than internal and C ones, whose
	// source names a LokaDB table or that depend on one. A statement using them reads or
	// writes LokaDB rows the analyzer cannot attribute to an organization (loadDeps).
	Views map[string][]string
	Funcs map[string]bool
	// Cloud[table]: the organization column of a cloud table declared in
	// routing.cloud_tables. Its lookups are Lookups[table]: the column (refs "") and id
	// (refs: the table itself, read by the proxy with that column).
	Cloud map[string]string
	// Tables, Cloud, Views, Local, Foreign, Writable, Direct, StmtTriggers and Indexes are by
	// key (package rel): the bare name in public, schema.name in another schema.
	// Rels[name]: the schemas that have a relation of that name, for the names of the LokaDB
	// and cloud tables. SchemasUsed: a LokaDB or cloud table is outside public, or the name of
	// one is another relation's in another schema too. Only then does a bare name depend on
	// the session's search_path (qualify); else it is public's, as before.
	Rels        map[string][]string
	SchemasUsed bool
	// PathFuncs: the functions and procedures of the application whose source sets
	// search_path and that have no SET search_path of their own (their change outlives
	// them): a call makes the session's path unknown to the proxy (schemas_path.go). Read
	// only when SchemasUsed.
	PathFuncs map[string]bool
}

// cloud: the organization column of a declared cloud table ("" for anything else).
func (m *Meta) cloud(table string) string {
	if m == nil {
		return ""
	}
	return m.Cloud[table]
}

// metaChannel: lokadb commands that change the routing metadata (enable, move-org, move-back,
// disable) run NOTIFY lokadb_meta in their transaction. Every upstream connection of the proxy
// listens to it (server.go; again after the client's UNLISTEN * or DISCARD ALL), and a session
// that hears it reloads the metadata before its next statement outside a transaction. The
// client never sees these notifications. Postgres delivers them between transactions only:
// a transaction open across the change still runs with the old metadata, and metaTTL stays the
// fallback (a connection that could not listen, a command that does not notify).
const metaChannel = "lokadb_meta"

// dropsListen: sql ends the proxy's LISTEN (UNLISTEN * or of its channel, DISCARD ALL).
func dropsListen(sql string) bool {
	if !hasFold(sql, "unlisten") && !hasFold(sql, "discard") {
		return false
	}
	tree, err := pg.Parse(sql)
	if err != nil {
		return false
	}
	for _, raw := range tree.Stmts {
		if u := raw.Stmt.GetUnlistenStmt(); u != nil && (u.Conditionname == "" || u.Conditionname == metaChannel) {
			return true
		}
		if d := raw.Stmt.GetDiscardStmt(); d != nil && d.Target == pg.DiscardMode_DISCARD_ALL {
			return true
		}
	}
	return false
}

// Events of a row trigger (pg_trigger.tgtype bits).
const (
	trigInsert = 4
	trigDelete = 8
	trigUpdate = 16
)

// rowTrigger: table has a row trigger of the cloud on event (RowTriggers): such a write goes
// the cloud's way. A trigger on DELETE alone (lokadb_fkcascade_*) leaves direct INSERTs and
// UPDATEs alone.
func (m *Meta) rowTrigger(table string, event int) bool {
	return m != nil && m.RowTriggers[table]&event != 0
}

// boundRe: the organization of a partition by its bound: an integer, or (block 18) a quoted
// literal as Postgres prints it, with its cast if it has one: a uuid (canonical, as
// canonicalUUID) or a string ('01J…'::text), the session's organization.
var boundRe = regexp.MustCompile(`^FOR VALUES IN \((?:'((?:[^']|'')*)'(?:::[a-z ]+(?:\(\d+\))?)?|(-?[0-9]+))\)$`)

// boundOf: the organization a partition's bound names, false for DEFAULT, a bound of several
// values and the empty string (the key of rows of no organization in a table named by strings).
func boundOf(expr string) (string, bool) {
	b := boundRe.FindStringSubmatch(expr)
	if b == nil {
		return "", false
	}
	v := b[2]
	if v == "" {
		v = strings.ReplaceAll(b[1], "''", "'")
	}
	return v, v != ""
}

// maxCloudFirstRows: lokadb.direct_cloud_first rows a session reads (PUBLIC may insert: a table
// filled with garbage is read this far, and the proxy warns).
const maxCloudFirstRows = 10000

// field: r[i], "" for a row without it (a catalog answer of an older shape).
func field(r []string, i int) string {
	if i < len(r) {
		return r[i]
	}
	return ""
}

func (m *Meta) table(rv *pg.RangeVar) *Table {
	if m == nil || rv == nil {
		return nil
	}
	return m.Tables[relKey(rv)]
}

// relKey: the key of the relation rv names; unqualified: public's (qualify gives a name the
// session's search_path resolves elsewhere its schema).
func relKey(rv *pg.RangeVar) string { return rel.RelKey(rv.Schemaname, rv.Relname) }

// keyOf: the key of relation alias (of pg_class) in SQL.
func keyOf(alias string) string {
	return rel.KeyOfSQL("(SELECT nspname::text FROM pg_catalog.pg_namespace WHERE oid = "+alias+".relnamespace)", alias+".relname::text")
}

type queryFunc func(sql string) ([][]string, error)

// LoadMeta reads the routing metadata `lokadb enable` recorded; nil if the database has none.
func LoadMeta(q func(sql string) ([][]string, error)) (*Meta, error) { return loadMeta(q) }

func loadMeta(q queryFunc) (*Meta, error) {
	rows, err := q(`SELECT (to_regclass('lokadb.tables') IS NOT NULL)::text`)
	if err != nil {
		if denied(err) { // no USAGE on schema lokadb
			return deniedMeta(q, err)
		}
		return nil, err
	}
	if len(rows) == 0 || rows[0][0] != "true" {
		// not visible to the role, or not there: the catalog tells which
		return deniedMeta(q, nil)
	}
	m, err := readMeta(q)
	if err != nil && denied(err) { // no SELECT on a table of lokadb
		return deniedMeta(q, err)
	}
	return m, err
}

// denied: err is Postgres' insufficient_privilege (42501).
func denied(err error) bool {
	var pe *pgconn.PgError
	var ue *upstreamError
	return errors.As(err, &pe) && pe.Code == "42501" || errors.As(err, &ue) && ue.code == "42501"
}

// deniedMeta: the metadata of a database whose lokadb tables the role cannot read (why), from
// the catalog alone, which every role reads: the LokaDB tables are those with the policy
// lokadb_tenant (enable puts it on each) and their partitions. None: the database is not
// enabled (nil). why nil: lokadb.tables was not visible at all, and the policies decide.
func deniedMeta(q queryFunc, why error) (*Meta, error) {
	rows, err := q(`SELECT ` + keyOf("c") + ` FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_class c ON c.oid = p.polrelid
		WHERE p.polname = 'lokadb_tenant'
		UNION SELECT ` + keyOf("k") + ` FROM pg_catalog.pg_policy p JOIN pg_catalog.pg_inherits i ON i.inhparent = p.polrelid
		JOIN pg_catalog.pg_class k ON k.oid = i.inhrelid WHERE p.polname = 'lokadb_tenant'`)
	if err != nil {
		if why != nil {
			return nil, why
		}
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	m := &Meta{Tables: map[string]*Table{}, Lookups: map[string]map[string]string{}}
	for _, r := range rows {
		m.Tables[r[0]] = &Table{Name: r[0], ColType: map[string]string{}}
	}
	if err := loadDeps(q, m); err != nil {
		return nil, err
	}
	m.Denied = metaDeniedMsg
	return m, nil
}

// metaDeniedMsg: the error of a statement on a LokaDB table when the metadata is unreadable.
const metaDeniedMsg = "lokadb: the proxy cannot read the lokadb metadata of this database under this role, so it cannot route statements on LokaDB tables and refuses them; " +
	"GRANT USAGE ON SCHEMA lokadb and SELECT ON lokadb.tables, lokadb.lookups, lokadb.orgs to the role (enable and move-org grant them to PUBLIC)"

// metaUnreadMsg: the same while the metadata could not be read at all (Meta.Retry).
const metaUnreadMsg = "lokadb: the proxy could not read the lokadb metadata of this database yet, so it refuses statements on LokaDB tables rather than run them unrouted; retry"

// unreadMeta: the metadata of an enabled database whose read failed (err): what to refuse,
// from the catalog, read again before the next statement. nil: not enabled, or not even that.
func unreadMeta(q queryFunc) *Meta {
	m, err := deniedMeta(q, nil)
	if err != nil || m == nil {
		return nil
	}
	m.Denied, m.Retry = metaUnreadMsg, true
	return m
}

// readMeta reads the metadata of an enabled database (loadMeta).
func readMeta(q queryFunc) (*Meta, error) {
	var rows [][]string
	var err error
	m := &Meta{Tables: map[string]*Table{}, Lookups: map[string]map[string]string{}}
	if rows, err = q(`SELECT (to_regprocedure('lokadb.org_lock(bigint)') IS NOT NULL)::text`); err != nil {
		return nil, err
	}
	m.OrgLock = len(rows) > 0 && rows[0][0] == "true"
	// key_column: an enable before it knew only id
	rows, err = q(`SELECT table_name, tenant_column, refs_table, cloud_partition, coalesce(to_jsonb(t) ->> 'key_column', 'id') FROM lokadb.tables t`)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		m.Tables[r[0]] = &Table{Name: r[0], TenantCol: r[1], Refs: r[2], Cloud: r[3], Key: r[4], ColType: map[string]string{}}
	}
	if len(m.Tables) == 0 {
		return nil, nil
	}
	rows, err = q(`SELECT table_name, column_name, coalesce(refs_table, '') FROM lokadb.lookups`)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if m.Lookups[r[0]] == nil {
			m.Lookups[r[0]] = map[string]string{}
		}
		m.Lookups[r[0]][r[1]] = r[2]
	}
	m.Cloud = map[string]string{}
	for t, cols := range m.Lookups {
		for col, refs := range cols {
			if m.Tables[t] == nil && refs == "" { // routing.cloud_tables (admin/routing.go)
				m.Cloud[t] = col
			}
		}
	}
	rows, err = q(`SELECT t.table_name, a.attname, format_type(a.atttypid, a.atttypmod),
		(SELECT typnamespace <> 'pg_catalog'::regnamespace FROM pg_type WHERE oid = a.atttypid)::text,
		(a.atthasdef AND a.attgenerated = '')::text, (a.attidentity <> '')::text, (a.attgenerated <> '')::text,
		coalesce(pg_get_expr(d.adbin, d.adrelid), '')
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN lokadb.tables t ON t.table_name = ` + keyOf("c") + `
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE a.attnum > 0 AND NOT a.attisdropped
		ORDER BY t.table_name, a.attnum`)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		t := m.Tables[r[0]]
		if t == nil {
			continue
		}
		t.Cols = append(t.Cols, r[1])
		t.ColType[r[1]] = r[2]
		t.UserTypes = t.UserTypes || field(r, 3) == "true"
		t.Identity = t.Identity || field(r, 5) == "true"
		t.Generated = t.Generated || field(r, 6) == "true"
		if field(r, 4) == "true" {
			if t.Defaults == nil {
				t.Defaults = map[string]bool{}
			}
			t.Defaults[r[1]] = true
			if key := t.Key; r[1] == key || key == "" && r[1] == "id" {
				t.KeyDefault = field(r, 7)
			} else if cols, ok := tupleCols(key); ok && slices.Contains(cols, r[1]) {
				if t.KeyDefaults == nil {
					t.KeyDefaults = map[string]string{}
				}
				t.KeyDefaults[r[1]] = field(r, 7)
			}
		}
	}
	// the DEFAULT partition's unique indexes (Table.Uniques)
	rows, err = q(`SELECT t.table_name, (SELECT string_agg(a.attname::text, chr(31) ORDER BY a.attname::text)
			FROM unnest(i.indkey::int2[]) k JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k)
		FROM pg_partitioned_table pt
		JOIN pg_class p ON p.oid = pt.partrelid
		JOIN lokadb.tables t ON t.table_name = ` + keyOf("p") + `
		JOIN pg_index i ON i.indrelid = pt.partdefid
		WHERE i.indisunique AND i.indisvalid AND i.indpred IS NULL AND i.indexprs IS NULL`)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if t := m.Tables[r[0]]; t != nil && field(r, 1) != "" {
			if t.Uniques == nil {
				t.Uniques = map[string]bool{}
			}
			t.Uniques[r[1]] = true
		}
	}
	m.Schemas = map[string]string{}
	// a database enabled by an older version has no column schema until a move adds it
	if rows, err = q(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs')
		AND attname = 'schema' AND NOT attisdropped)::text`); err != nil {
		return nil, err
	}
	if len(rows) > 0 && rows[0][0] == "true" {
		if rows, err = q(`SELECT org::text, schema FROM lokadb.orgs`); err != nil {
			return nil, err
		}
		for _, r := range rows {
			m.Schemas[r[0]] = r[1]
		}
	}
	m.DBIDs = map[string]string{}
	if rows, err = q(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs')
		AND attname = 'db_id' AND NOT attisdropped)::text`); err != nil {
		return nil, err
	}
	if len(rows) > 0 && rows[0][0] == "true" {
		if rows, err = q(`SELECT org::text, db_id FROM lokadb.orgs WHERE db_id IS NOT NULL`); err != nil {
			return nil, err
		}
		for _, r := range rows {
			m.DBIDs[r[0]] = r[1]
		}
	}
	m.Profiles = map[string]string{}
	if rows, err = q(`SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = to_regclass('lokadb.orgs')
		AND attname = 'profile' AND NOT attisdropped)::text`); err != nil {
		return nil, err
	}
	if len(rows) > 0 && rows[0][0] == "true" {
		if rows, err = q(`SELECT org::text, profile FROM lokadb.orgs WHERE profile IS NOT NULL`); err != nil {
			return nil, err
		}
		for _, r := range rows {
			m.Profiles[r[0]] = r[1]
		}
	}
	m.direct, m.directKnown = m.anyDirect(), true // once, not on each BEGIN (review of performance, PF13)
	rows, err = q(`SELECT t.table_name, ` + keyOf("c") + `, pg_get_expr(c.relpartbound, c.oid), c.relkind::text,
		(has_table_privilege(c.oid, 'INSERT') AND has_table_privilege(c.oid, 'UPDATE'))::text,
		(NOT EXISTS (SELECT 1 FROM unnest(ARRAY['SELECT', 'INSERT', 'UPDATE']) pr
			WHERE has_any_column_privilege(p.oid, pr) AND NOT has_table_privilege(c.oid, pr))
			AND (NOT has_table_privilege(p.oid, 'DELETE') OR has_table_privilege(c.oid, 'DELETE')))::text
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN lokadb.tables t ON t.table_name = ` + keyOf("p") + `
		WHERE c.relkind IN ('f', 'r')`)
	if err != nil {
		return nil, err
	}
	m.Foreign, m.Local, m.Writable, m.Direct = map[string]map[string]string{}, map[string]map[string]string{}, map[string]bool{}, map[string]bool{}
	for _, r := range rows {
		if r[3] == "r" && r[4] == "true" {
			m.Writable[r[1]] = true
		}
		if r[3] == "f" && r[5] == "true" {
			m.Direct[r[1]] = true
		}
		if org, ok := boundOf(r[2]); ok {
			parts := m.Foreign
			if r[3] == "r" {
				parts = m.Local
			}
			if parts[r[0]] == nil {
				parts[r[0]] = map[string]string{}
			}
			parts[r[0]][org] = r[1]
		}
	}
	if err := triggers(q, m, "lokadb.tables t"); err != nil {
		return nil, err
	}
	if rows, err = q(`SELECT DISTINCT t.table_name FROM lokadb.tables t
		JOIN pg_constraint c ON c.contype = 'f' AND c.confrelid = to_regclass(` + rel.QuotedSQL("t.refs_table") + `)
		JOIN pg_class k ON k.oid = c.conrelid
		JOIN lokadb.tables b ON b.table_name = ` + keyOf("k")); err != nil {
		return nil, err
	}
	m.RefsFromLokadb = map[string]bool{}
	for _, r := range rows {
		m.RefsFromLokadb[r[0]] = true
	}
	if rows, err = q(`SELECT (to_regprocedure('lokadb.same_org(bigint,bigint)') IS NOT NULL)::text`); err != nil {
		return nil, err
	}
	m.SameOrg = len(rows) > 0 && rows[0][0] == "true"
	m.CloudFirst = map[string]bool{}
	if rows, err = q(`SELECT (to_regclass('lokadb.direct_cloud_first') IS NOT NULL)::text`); err != nil {
		return nil, err
	}
	if m.CloudFirstTable = len(rows) > 0 && rows[0][0] == "true"; m.CloudFirstTable {
		if rows, err = q(`SELECT fingerprint FROM lokadb.direct_cloud_first ORDER BY at DESC LIMIT ` + strconv.Itoa(maxCloudFirstRows+1)); err != nil {
			return nil, err
		}
		for i, r := range rows {
			if i == maxCloudFirstRows {
				m.CloudFirstFull = true
				break
			}
			m.CloudFirst[r[0]] = true
		}
	}
	m.Learnt = map[string]bool{}
	if rows, err = q(`SELECT (to_regclass('lokadb.direct_learnt') IS NOT NULL)::text`); err != nil {
		return nil, err
	}
	if len(rows) > 0 && rows[0][0] == "true" {
		if rows, err = q(`SELECT fingerprint || CASE WHEN named THEN '#named' ELSE '' END FROM lokadb.direct_learnt ORDER BY at DESC LIMIT ` + strconv.Itoa(maxCloudFirstRows)); err != nil {
			return nil, err
		}
		for _, r := range rows {
			m.Learnt[r[0]] = true
		}
	}
	// (and the session's role may call it: the tables' owner; another role's GRANT goes as written)
	if rows, err = q(`SELECT coalesce(has_function_privilege(to_regprocedure('lokadb.grant_sync(text,text[])'), 'EXECUTE'), false)::text`); err != nil {
		return nil, err
	}
	m.GrantSync = len(rows) > 0 && rows[0][0] == "true"
	rows, err = q(`SELECT ` + keyOf("ic") + `, t.table_name
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_class c ON c.oid = i.indrelid
		LEFT JOIN pg_inherits h ON h.inhrelid = c.oid
		LEFT JOIN pg_class p ON p.oid = h.inhparent
		JOIN lokadb.tables t ON t.table_name = ` + keyOf("c") + ` OR t.table_name = ` + keyOf("p") + ``)
	if err != nil {
		return nil, err
	}
	m.Indexes = map[string]string{}
	for _, r := range rows {
		m.Indexes[r[0]] = r[1]
	}
	if err := loadKeyKinds(q, m); err != nil {
		return nil, err
	}
	if err := loadOrgNums(q, m); err != nil {
		return nil, err
	}
	if err := loadDeps(q, m); err != nil {
		return nil, err
	}
	if err := loadRels(q, m); err != nil {
		return nil, err
	}
	return m, nil
}

// loadRels fills m.Rels and m.SchemasUsed.
func loadRels(q queryFunc, m *Meta) error {
	m.Rels = map[string][]string{}
	seen := map[string]bool{}
	var names []string
	// views over LokaDB tables too: a bare name of one outside public resolves by the search
	// path, as a table's (review of routing R1)
	for _, keys := range []map[string]bool{tableKeys(m), cloudKeys(m), viewKeys(m)} {
		for k := range keys {
			s, n := rel.SplitKey(k)
			m.SchemasUsed = m.SchemasUsed || s != "public"
			if !seen[n] {
				seen[n] = true
				names = append(names, n)
			}
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	rows, err := q(`SELECT c.relname::text, n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.relname::text = ANY (` + sqlString(textArray(names)) + `) AND c.relkind IN ('r', 'p', 'v', 'm', 'f') AND NOT c.relispartition
		  AND n.oid NOT IN (` + sysSchemas + `)
		ORDER BY 1, 2`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		m.Rels[r[0]] = append(m.Rels[r[0]], r[1])
		m.SchemasUsed = m.SchemasUsed || len(m.Rels[r[0]]) > 1
	}
	if !m.SchemasUsed {
		return nil
	}
	rows, err = q(`SELECT DISTINCT p.proname::text FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
		WHERE p.pronamespace NOT IN (` + sysSchemas + `) AND l.lanname NOT IN ('internal', 'c') AND p.prosrc ~* 'search_path'
		  AND NOT EXISTS (SELECT 1 FROM unnest(coalesce(p.proconfig, '{}')) c WHERE c ILIKE 'search_path=%')`)
	if err != nil {
		return err
	}
	m.PathFuncs = map[string]bool{}
	for _, r := range rows {
		m.PathFuncs[r[0]] = true
	}
	return nil
}

func tableKeys(m *Meta) map[string]bool {
	out := map[string]bool{}
	for k := range m.Tables {
		out[k] = true
	}
	return out
}

func cloudKeys(m *Meta) map[string]bool {
	out := map[string]bool{}
	for k := range m.Cloud {
		out[k] = true
	}
	return out
}

// textArray: names as a text array literal.
func textArray(names []string) string {
	var out []string
	for _, n := range names {
		out = append(out, `"`+strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(n)+`"`)
	}
	return "{" + strings.Join(out, ",") + "}"
}

// LoadDeps fills m.Views and m.Funcs from the catalog (lokadb check before enable: m holds
// the tables of the spec).
func LoadDeps(q func(sql string) ([][]string, error), m *Meta) error {
	if err := loadDeps(q, m); err != nil {
		return err
	}
	return loadRels(q, m)
}

func loadDeps(q queryFunc, m *Meta) error {
	m.Views, m.Funcs = map[string][]string{}, map[string]bool{}
	var names []string
	named := map[string]bool{}
	for t := range m.Tables {
		_, n := rel.SplitKey(t) // a function's source names it as it likes: by its name
		if !named[n] {
			named[n] = true
			names = append(names, regexp.QuoteMeta(n))
		}
	}
	if len(names) == 0 {
		return nil
	}
	sort.Strings(names)
	rows, err := q(`SELECT DISTINCT ` + keyOf("v") + `, ` + keyOf("t") + `
		FROM pg_rewrite r
		JOIN pg_class v ON v.oid = r.ev_class
		JOIN pg_depend d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid AND d.refclassid = 'pg_class'::regclass
		JOIN pg_class t ON t.oid = d.refobjid
		WHERE v.relkind IN ('v', 'm') AND v.relnamespace NOT IN (` + sysSchemas + `) AND t.oid <> v.oid`)
	if err != nil {
		return err
	}
	reads := map[string][]string{} // view -> relations it reads directly
	for _, r := range rows {
		reads[r[0]] = append(reads[r[0]], r[1])
	}
	for v := range reads {
		seen, tables := map[string]bool{v: true}, map[string]bool{}
		next := []string{v}
		for len(next) > 0 {
			x := next[0]
			next = next[1:]
			for _, rel := range reads[x] {
				if m.Tables[rel] != nil {
					tables[rel] = true
				} else if !seen[rel] {
					seen[rel] = true
					next = append(next, rel)
				}
			}
		}
		for t := range tables {
			m.Views[v] = append(m.Views[v], t)
		}
		sort.Strings(m.Views[v])
	}
	// a function's source names a table or a view over one as it likes: in any case, quoted or
	// not (~*; review of routing R1: FROM Documents, or a view, went unseen)
	for v := range m.Views {
		_, n := rel.SplitKey(v)
		if !named[n] {
			named[n] = true
			names = append(names, regexp.QuoteMeta(n))
		}
	}
	// or a partition or helper table of one by its exact name (ownSuffixRe; the 2nd review of
	// routing: a function reading documents__org2 went for a cloud one), not a table of the
	// application named like one (documents__archive)
	// and a function that calls one of those: the names found are looked for again, a few rounds
	for round := 0; round < 4; round++ {
		sort.Strings(names)
		rows, err = q(`SELECT DISTINCT p.proname::text FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang
		WHERE p.pronamespace NOT IN (` + sysSchemas + `) AND l.lanname NOT IN ('internal', 'c')
		  AND (p.prosrc ~* ` + sqlString(`\m(`+strings.Join(names, "|")+`)(__(`+ownSuffixesPG+`))?\M`) + `
		    OR EXISTS (SELECT 1 FROM pg_depend d JOIN pg_class t ON t.oid = d.refobjid
		      WHERE d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.refclassid = 'pg_class'::regclass
		        AND ` + keyOf("t") + ` = ANY (` + sqlString("{"+strings.Join(quotedNames(m), ",")+"}") + `)))`)
		if err != nil {
			return err
		}
		more := false
		for _, r := range rows {
			m.Funcs[r[0]] = true
			if !named[r[0]] {
				named[r[0]], more = true, true
				names = append(names, regexp.QuoteMeta(r[0]))
			}
		}
		if !more {
			break
		}
	}
	return nil
}

// quotedNames: the LokaDB tables as elements of a text array literal.
func quotedNames(m *Meta) []string {
	var out []string
	for t := range m.Tables {
		out = append(out, `"`+strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(t)+`"`)
	}
	sort.Strings(out)
	return out
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// sysSchemas: the namespaces that are not the application's (SQL, for NOT IN).
const sysSchemas = `SELECT oid FROM pg_catalog.pg_namespace WHERE nspname IN ('information_schema', 'lokadb') OR nspname ~ '^pg_'`

// triggers reads StmtTriggers and RowTriggers of the tables src names (a FROM item with a
// column table_name, by key: lokadb.tables, or the VALUES of CatalogFacts).
func triggers(q queryFunc, m *Meta, src string) error {
	rows, err := q(`SELECT DISTINCT t.table_name FROM pg_trigger g
		JOIN pg_class p ON p.oid = g.tgrelid
		JOIN ` + src + ` ON t.table_name = ` + keyOf("p") + `
		WHERE NOT g.tgisinternal AND g.tgtype & 1 = 0`)
	if err != nil {
		return err
	}
	m.StmtTriggers = map[string]bool{}
	for _, r := range rows {
		m.StmtTriggers[r[0]] = true
	}
	if rows, err = q(`SELECT t.table_name, bit_or(g.tgtype::int & 28)::text FROM ` + src + `
		JOIN pg_class p ON t.table_name = ` + keyOf("p") + `
		JOIN pg_trigger g ON g.tgrelid = p.oid OR g.tgrelid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = p.oid)
		WHERE NOT g.tgisinternal AND g.tgtype & 1 = 1 AND g.tgname NOT IN ('lokadb_refs', 'lokadb_parent')
		GROUP BY t.table_name`); err != nil {
		return err
	}
	m.RowTriggers = map[string]int{}
	for _, r := range rows {
		events := trigInsert | trigDelete | trigUpdate // an answer without the events: every one
		if e, err := strconv.Atoi(field(r, 1)); err == nil {
			events = e
		}
		m.RowTriggers[r[0]] = events
	}
	return nil
}

// CatalogFacts fills what the direct path's forms depend on (DirectRefusal, DirectTakes) for
// the tables of m.Tables from the catalog alone, before enable has recorded them: a column of
// a type of the application's (Table.UserTypes), the statement and row triggers of the
// application's (StmtTriggers, RowTriggers), as LoadMeta reads them after enable. check judges
// a database before enable with it (review of check, 2).
func CatalogFacts(q func(sql string) ([][]string, error), m *Meta) error {
	if m == nil || len(m.Tables) == 0 {
		return nil
	}
	names := make([]string, 0, len(m.Tables))
	for t := range m.Tables {
		names = append(names, "("+sqlString(t)+")")
	}
	sort.Strings(names)
	src := "(VALUES " + strings.Join(names, ", ") + ") t(table_name)"
	rows, err := q(`SELECT DISTINCT t.table_name FROM ` + src + `
		JOIN pg_class c ON t.table_name = ` + keyOf("c") + `
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		JOIN pg_type y ON y.oid = a.atttypid
		WHERE y.typnamespace <> 'pg_catalog'::regnamespace`)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if t := m.Tables[r[0]]; t != nil {
			t.UserTypes = true
		}
	}
	return triggers(q, m, src)
}

// anyDirect: some organization's profile has the direct path (fdw+direct, direct).
func (m *Meta) anyDirect() bool {
	if m.directKnown {
		return m.direct
	}
	for _, p := range m.Profiles {
		if directProfile(p) {
			return true
		}
	}
	return false
}
