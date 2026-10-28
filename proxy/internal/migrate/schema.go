// Package migrate compares the schema of the LokaDB tables in the cloud with their copies in
// the organizations' databases (schema-diff) and brings an organization's copy up to the
// cloud's (schema-sync): after a replay that failed half-way, a change made there by hand,
// a migration the proxy did not see.
//
// What the organization's table must have is what move-org gives it, from the cloud: the
// parent's columns (name, type, NOT NULL, generated expression, collation), its CHECK
// constraints, the indexes of the DEFAULT partition (<table>__cloud, the indexes of the
// parent and lokadb's own), and an FK widened with the organization column for every FK onto
// another LokaDB table. Defaults are not compared: the cloud computes them before a row reaches
// the organization. move-org's own PRIMARY KEY (id), UNIQUE (<tenant>, id) and lokadb_owner
// are left out on both sides.
package migrate

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// Diff is one difference of an organization's table from the cloud's.
type Diff struct {
	Org    string `json:"org"`
	Table  string `json:"table"`
	Kind   string `json:"kind"`   // see the kinds below
	Object string `json:"object"` // the column, index, constraint
	Cloud  string `json:"cloud,omitempty"`
	Here   string `json:"organization,omitempty"`
	// Fix: the statement schema-sync runs in the organization's database ("" = none: by hand
	// or by move-org). Drop: it removes something the organization has (-drop-extra only).
	Fix  string `json:"fix,omitempty"`
	Drop bool   `json:"drop,omitempty"`
}

// Kinds of a Diff.
const (
	MissingTable  = "missing_table"
	MissingColumn = "missing_column"
	ExtraColumn   = "extra_column"
	ColumnType    = "column_type"
	NotNull       = "not_null"
	Generated     = "generated"
	Collation     = "collation"
	MissingIndex  = "missing_index"
	ExtraIndex    = "extra_index"
	MissingCheck  = "missing_check"
	ExtraCheck    = "extra_check"
	MissingFK     = "missing_fk"
	ExtraFK       = "extra_fk"
)

// Org is one organization's database as the cloud reaches it.
type Org struct {
	Org    string `json:"org"`
	Schema string `json:"schema"`
	dsn    string
	Error  string `json:"error,omitempty"` // it could not be compared (unreachable, no credentials)
	Diffs  []Diff `json:"diffs"`
	Synced []Diff `json:"synced,omitempty"` // schema-sync: what it fixed
}

// Options of Diff and Sync.
type Options struct {
	CloudDSN string            // admin of the cloud database
	Orgs     []string          // only these organizations ("" = all moved)
	DSN      map[string]string // organization -> DSN of its database, instead of its FDW server's
	// DropExtra: schema-sync also drops what the organization has and the cloud has not
	// (columns, indexes, constraints).
	DropExtra bool
}

type column struct{ name, typ, notnull, gen, coll, kind, def string }

type index struct{ name, def, key string }

type constraint struct{ name, def, key string }

// table is what one side has of a LokaDB table.
type table struct {
	exists bool
	cols   map[string]column
	order  []string
	idx    []index
	checks []constraint
	fks    []constraint
}

// indexDefRe: an index definition without its name and table ("UNIQUE … USING btree (…)"),
// which is how the two sides are compared: the replay names a partition's index otherwise.
var indexDefRe = regexp.MustCompile(`^CREATE (UNIQUE )?INDEX (?:"(?:[^"]|"")*"|\S+) ON (?:ONLY )?(?:(?:"(?:[^"]|"")*"|[^\s."]+)\.)?(?:"(?:[^"]|"")*"|\S+) (.*)$`)

func indexKey(def string) string {
	m := indexDefRe.FindStringSubmatch(def)
	if m == nil {
		return def
	}
	return m[1] + m[2]
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func rowsOf(ctx context.Context, c querier, sql string, args ...any) ([][]string, error) {
	rows, err := c.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		r := make([]string, len(vals))
		for i, v := range vals {
			if v != nil {
				r[i] = fmt.Sprint(v)
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const colsSQL = `SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), a.attnotnull::text,
	CASE WHEN a.attgenerated <> '' THEN pg_get_expr(d.adbin, d.adrelid) ELSE '' END,
	CASE WHEN a.attcollation <> 0 AND a.attcollation <> ty.typcollation THEN a.attcollation::regcollation::text ELSE '' END,
	a.attgenerated::text, CASE WHEN a.attgenerated = '' THEN coalesce(pg_get_expr(d.adbin, d.adrelid), '') ELSE '' END
	FROM pg_attribute a JOIN pg_type ty ON ty.oid = a.atttypid
	LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
	WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`

// FK: its columns but the organization column, the table it points at, its ON DELETE action
const fkSQL = `SELECT c.conname::text,
	(SELECT string_agg(a.attname::text, ',' ORDER BY k.n) FROM unnest(c.conkey) WITH ORDINALITY k(attnum, n)
	 JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum WHERE a.attname <> $2),
	%s, c.confdeltype::text, c.condeferrable::text, c.condeferred::text
	FROM pg_constraint c WHERE c.contype = 'f' AND c.conrelid = $1`

// cloudTable reads the cloud's side of a LokaDB table: the parent, its DEFAULT partition.
func cloudTable(ctx context.Context, c querier, name, tenant string) (*table, error) {
	t := &table{cols: map[string]column{}}
	rows, err := rowsOf(ctx, c, `SELECT to_regclass(`+admin.RelSQL("$1::text")+`)::oid::text, to_regclass(`+admin.RelSQL("($1 || '__cloud')")+`)::oid::text`, name)
	if err != nil {
		return nil, err
	}
	parent, def := rows[0][0], rows[0][1]
	if parent == "" || def == "" {
		return nil, fmt.Errorf("%s: no such LokaDB table in the cloud (or no %s__cloud)", name, name)
	}
	t.exists = true
	if err := t.readCols(ctx, c, parent); err != nil {
		return nil, err
	}
	// The indexes of DEFAULT: the parent's (named as the parent's: that is the name the
	// replay gives them in the organizations' databases) and its own.
	rows, err = rowsOf(ctx, c, `SELECT coalesce(pc.relname, ic.relname)::text, pg_get_indexdef(i.indexrelid)
		FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid
		LEFT JOIN pg_inherits h ON h.inhrelid = i.indexrelid LEFT JOIN pg_class pc ON pc.oid = h.inhparent
		WHERE i.indrelid = $1::oid AND NOT i.indisprimary ORDER BY 1`, def)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		t.idx = append(t.idx, index{r[0], r[1], indexKey(r[1])})
	}
	if t.checks, err = checks(ctx, c, parent); err != nil {
		return nil, err
	}
	// FKs of DEFAULT onto refs tables: onto another LokaDB table, which the organization has
	rows, err = rowsOf(ctx, c, fmt.Sprintf(fkSQL, `coalesce((SELECT bt.table_name FROM lokadb.tables bt WHERE to_regclass(`+admin.RelSQL("bt.refs_table")+`) = c.confrelid), '')`), def, tenant)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if r[2] != "" {
			t.fks = append(t.fks, fkOf(r))
		}
	}
	return t, nil
}

// fkOf: an FK as compared; def holds the LokaDB table it points at.
func fkOf(r []string) constraint {
	return constraint{name: r[0], key: r[1] + " -> " + r[2] + " on delete " + r[3] + deferral(r[4], r[5]), def: r[2]}
}

func deferral(deferrable, deferred string) string {
	switch {
	case deferred == "true":
		return " deferred"
	case deferrable == "true":
		return " deferrable"
	}
	return ""
}

func (t *table) readCols(ctx context.Context, c querier, oid string) error {
	rows, err := rowsOf(ctx, c, colsSQL, oid)
	if err != nil {
		return err
	}
	for _, r := range rows {
		t.cols[r[0]] = column{r[0], r[1], r[2], r[3], r[4], r[5], r[6]}
		t.order = append(t.order, r[0])
	}
	return nil
}

func checks(ctx context.Context, c querier, oid string) ([]constraint, error) {
	rows, err := rowsOf(ctx, c, `SELECT conname::text, pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = $1::oid AND contype = 'c' AND conname <> 'lokadb_owner' ORDER BY conname`, oid)
	if err != nil {
		return nil, err
	}
	var out []constraint
	for _, r := range rows {
		out = append(out, constraint{r[0], r[1], r[0] + " " + r[1]})
	}
	return out, nil
}

// orgTable reads an organization's copy of a LokaDB table, nil if it has none.
func orgTable(ctx context.Context, c querier, schema, name, tenant string) (*table, error) {
	t := &table{cols: map[string]column{}}
	rows, err := rowsOf(ctx, c, `SELECT coalesce(to_regclass(quote_ident($1) || '.' || quote_ident($2))::oid::text, '')`, schema, name)
	if err != nil {
		return nil, err
	}
	oid := rows[0][0]
	if oid == "" {
		return t, nil
	}
	t.exists = true
	if err := t.readCols(ctx, c, oid); err != nil {
		return nil, err
	}
	// move-org's PRIMARY KEY (id) and UNIQUE (<tenant>, id) are not the cloud's indexes
	rows, err = rowsOf(ctx, c, `SELECT ic.relname::text, pg_get_indexdef(i.indexrelid) FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		WHERE i.indrelid = $1::oid AND NOT EXISTS (SELECT FROM pg_constraint k WHERE k.conindid = i.indexrelid AND k.contype IN ('p', 'u'))
		ORDER BY 1`, oid)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		t.idx = append(t.idx, index{r[0], r[1], indexKey(r[1])})
	}
	if t.checks, err = checks(ctx, c, oid); err != nil {
		return nil, err
	}
	rows, err = rowsOf(ctx, c, fmt.Sprintf(fkSQL, `c.confrelid::regclass::text`), oid, tenant)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		// the table it points at, without the schema: the organization's copies share one
		if i := strings.LastIndex(r[2], "."); i >= 0 {
			r[2] = r[2][i+1:]
		}
		r[2] = strings.Trim(r[2], `"`)
		t.fks = append(t.fks, fkOf(r))
	}
	return t, nil
}

func q(s string) string { return pgx.Identifier{s}.Sanitize() }

// compare lists what the organization's table lacks or has more than the cloud's, with the
// statement that fixes it there.
func compare(org, schema, name, tenant string, cl, or *table) []Diff {
	rel := q(schema) + "." + q(name)
	d := func(kind, obj, cloud, here, fix string, drop bool) Diff {
		return Diff{Org: org, Table: name, Kind: kind, Object: obj, Cloud: cloud, Here: here, Fix: fix, Drop: drop}
	}
	if !or.exists {
		return []Diff{d(MissingTable, name, "", "", "", false)}
	}
	var out []Diff
	for _, n := range cl.order {
		c := cl.cols[n]
		o, ok := or.cols[n]
		if !ok {
			out = append(out, d(MissingColumn, n, describeCol(c), "", "ALTER TABLE "+rel+" ADD COLUMN "+colDef(c), false))
			continue
		}
		if c.typ != o.typ {
			out = append(out, d(ColumnType, n, c.typ, o.typ, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s USING %s::%s", rel, q(n), c.typ, q(n), c.typ), false))
		}
		if c.gen != o.gen || c.kind != o.kind {
			out = append(out, d(Generated, n, c.gen, o.gen, "", false)) // a generated column is made again by hand
		}
		if c.coll != o.coll {
			coll := c.coll
			if coll == "" {
				coll = `"default"`
			}
			out = append(out, d(Collation, n, c.coll, o.coll, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s COLLATE %s", rel, q(n), c.typ, coll), false))
		}
		if c.notnull != o.notnull {
			act := "SET"
			if c.notnull != "true" {
				act = "DROP"
			}
			out = append(out, d(NotNull, n, c.notnull, o.notnull, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s %s NOT NULL", rel, q(n), act), false))
		}
	}
	var extra []string
	for n := range or.cols {
		if _, ok := cl.cols[n]; !ok {
			extra = append(extra, n)
		}
	}
	sort.Strings(extra)
	for _, n := range extra {
		out = append(out, d(ExtraColumn, n, "", describeCol(or.cols[n]), "ALTER TABLE "+rel+" DROP COLUMN "+q(n), true))
	}
	// indexes, by definition: a multiset on each side
	have := map[string]int{}
	for _, i := range or.idx {
		have[i.key]++
	}
	for _, i := range cl.idx {
		if have[i.key] > 0 {
			have[i.key]--
			continue
		}
		out = append(out, d(MissingIndex, i.name, i.key, "", indexOn(i, rel), false))
	}
	want := map[string]int{}
	for _, i := range cl.idx {
		want[i.key]++
	}
	for _, i := range or.idx {
		if want[i.key] > 0 {
			want[i.key]--
			continue
		}
		out = append(out, d(ExtraIndex, i.name, "", i.key, "DROP INDEX "+q(schema)+"."+q(i.name), true))
	}
	out = append(out, constraints(cl.checks, or.checks, func(c constraint) Diff {
		return d(MissingCheck, c.name, c.def, "", fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", rel, q(c.name), c.def), false)
	}, func(c constraint) Diff {
		return d(ExtraCheck, c.name, "", c.def, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", rel, q(c.name)), true)
	})...)
	out = append(out, constraints(cl.fks, or.fks, func(c constraint) Diff {
		return d(MissingFK, c.name, c.key, "", fkDDL(rel, schema, tenant, c), false)
	}, func(c constraint) Diff {
		return d(ExtraFK, c.name, "", c.key, fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", rel, q(c.name)), true)
	})...)
	return out
}

func constraints(cl, or []constraint, missing, extra func(constraint) Diff) []Diff {
	var out []Diff
	have := map[string]int{}
	for _, c := range or {
		have[c.key]++
	}
	for _, c := range cl {
		if have[c.key] > 0 {
			have[c.key]--
			continue
		}
		out = append(out, missing(c))
	}
	want := map[string]int{}
	for _, c := range cl {
		want[c.key]++
	}
	for _, c := range or {
		if want[c.key] > 0 {
			want[c.key]--
			continue
		}
		out = append(out, extra(c))
	}
	return out
}

func describeCol(c column) string {
	s := c.typ
	if c.coll != "" {
		s += " collate " + c.coll
	}
	if c.gen != "" {
		s += " generated (" + c.gen + ")"
	}
	if c.notnull == "true" {
		s += " not null"
	}
	return s
}

// colDef: a column as move-org declares it, plus the cloud's default: rows the organization
// already has get it, which a NOT NULL column needs.
func colDef(c column) string {
	s := q(c.name) + " " + c.typ
	if c.coll != "" {
		s += " COLLATE " + c.coll
	}
	switch {
	case c.gen != "" && c.kind == "v":
		s += " GENERATED ALWAYS AS (" + c.gen + ") VIRTUAL"
	case c.gen != "":
		s += " GENERATED ALWAYS AS (" + c.gen + ") STORED"
	case c.def != "" && !strings.Contains(c.def, "nextval("):
		s += " DEFAULT " + c.def
	}
	if c.notnull == "true" {
		s += " NOT NULL"
	}
	return s
}

// indexOn: the cloud's index definition, put on the organization's table under its name.
func indexOn(i index, rel string) string {
	m := indexDefRe.FindStringSubmatch(i.def)
	if m == nil {
		return ""
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s %s", m[1], q(i.name), rel, m[2])
}

// fkDDL: an FK between LokaDB tables as move-org makes it: widened with the organization
// column, so a row points only inside its organization; SET NULL resets the reference only.
func fkDDL(rel, schema, tenant string, c constraint) string {
	parts := strings.SplitN(c.key, " -> ", 2)
	cols := strings.Split(parts[0], ",")
	var qc []string
	for _, x := range cols {
		qc = append(qc, q(x))
	}
	s := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s, %s) REFERENCES %s.%s (%s, id)", rel, q(c.name), q(tenant), strings.Join(qc, ", "), q(schema), q(c.def), q(tenant))
	switch {
	case strings.Contains(c.key, " on delete c"):
		s += " ON DELETE CASCADE"
	case strings.Contains(c.key, " on delete n"):
		s += " ON DELETE SET NULL (" + strings.Join(qc, ", ") + ")"
	case strings.Contains(c.key, " on delete d"):
		s += " ON DELETE SET DEFAULT (" + strings.Join(qc, ", ") + ")"
	case strings.Contains(c.key, " on delete r"):
		s += " ON DELETE RESTRICT"
	}
	switch {
	case strings.HasSuffix(c.key, " deferred"):
		s += " DEFERRABLE INITIALLY DEFERRED"
	case strings.HasSuffix(c.key, " deferrable"):
		s += " DEFERRABLE"
	}
	return s
}

// targets: the moved organizations and how to reach their databases (lokadb.replay_targets,
// the function the proxy replays DDL by; DSN overrides it).
func targets(ctx context.Context, cloud querier, o Options) ([]*Org, error) {
	rows, err := rowsOf(ctx, cloud, `SELECT org::text, org_schema, coalesce(array_to_json(server_options), '[]')::text,
		coalesce(array_to_json(mapping_options), '[]')::text FROM lokadb.replay_targets()`)
	if err != nil {
		return nil, fmt.Errorf("the moved organizations (lokadb.replay_targets(); run enable of this version): %w", err)
	}
	var out []*Org
	for _, r := range rows {
		if len(o.Orgs) > 0 && !slices.Contains(o.Orgs, r[0]) {
			continue
		}
		org := &Org{Org: r[0], Schema: r[1], dsn: o.DSN[r[0]]}
		if org.dsn == "" {
			var srv, um []string
			if err := unmarshal(r[2], &srv); err != nil {
				return nil, err
			}
			if err := unmarshal(r[3], &um); err != nil {
				return nil, err
			}
			if len(srv) > 0 && len(um) > 0 {
				org.dsn = proxy.OptionsDSN(srv, um)
			}
		}
		out = append(out, org)
	}
	for _, want := range o.Orgs {
		if !slices.ContainsFunc(out, func(g *Org) bool { return g.Org == want }) {
			return nil, fmt.Errorf("organization %s is not moved (not in lokadb.orgs)", want)
		}
	}
	return out, nil
}
