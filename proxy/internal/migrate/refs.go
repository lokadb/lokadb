package migrate

// Data: the refs tables against the organizations' rows (block 7, item 5).
//
// <table>__refs maps every id of a LokaDB table to its organization: the proxy finds the
// organization of a statement by it, and the FKs of other tables point at it. The cloud
// keeps it up to date for every write that passes by the cloud (a trigger on the parent).
// A row written straight into the organization's database is not there: the proxy does not
// find it by its id, and an FK onto it fails. One deleted there leaves its id behind.
//
// The cloud reads the organization's rows through its foreign partition (<table>__org<N>),
// so the comparison is one query per table in the cloud database. An organization in the
// cloud (one moved back, review 3 R3-4) has its rows in the cloud's own partitions: named with
// -org, or all of them with -cloud, it is compared against the parent's rows of it. The maps
// of unique columns (lokadb.unique_refs, an FK onto a unique column other than the key) are
// compared the same way as the refs tables.

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// RefsDiff is what one table of one organization lacks or has too much in its refs table.
type RefsDiff struct {
	Org   string `json:"org"`
	Table string `json:"table"`
	// Column: a map of a unique column of the table (lokadb.unique_refs), not its refs table.
	Column string `json:"column,omitempty"`
	// Cloud: the organization is in the cloud (not in lokadb.orgs): its rows there were compared.
	Cloud bool `json:"cloud,omitempty"`
	// Missing: ids of the organization's rows the refs table does not have (rows written
	// there past the cloud); Conflict: ids it has, but for another organization (the same
	// id in two organizations: fixed by hand); Stale: ids it has for the organization, which
	// has no such row (deleted there past the cloud).
	Missing, Conflict, Stale int64
	// Examples: a few ids of each kind.
	MissingIDs  []string `json:"missing_ids,omitempty"`
	ConflictIDs []string `json:"conflict_ids,omitempty"`
	StaleIDs    []string `json:"stale_ids,omitempty"`
	// SequenceBehind: the largest id at the organization is past the cloud's sequence of the
	// id column: the cloud will hand out ids the organization has already.
	SequenceBehind string `json:"sequence_behind,omitempty"`
	// SequenceKept: why refs-sync left the sequence where it is (review A6): the cloud's
	// sequence is shared by every organization, and the organization's ids are its own data.
	SequenceKept string `json:"sequence_kept,omitempty"`
	Fixed        string `json:"fixed,omitempty"` // refs-sync: what it did
}

// RefsOptions of RefsCompare.
type RefsOptions struct {
	CloudDSN string
	Orgs     []string
	Sync     bool // add the missing ids, move the sequence on
	// DeleteStale: also delete the ids of rows the organization does not have (an FK of a
	// cloud table onto one of them refuses it, or cascades: see the FK's ON DELETE).
	DeleteStale bool
	// MaxSequenceJump: the most refs-sync moves the cloud's sequence on by itself (default
	// DefaultMaxSequenceJump). The organization writes its own ids: one huge id there would
	// otherwise move the sequence every organization shares to it (up to its maximum, after
	// which no organization can insert), or take a range of ids from the others.
	MaxSequenceJump int64
	// Cloud: also every organization in the cloud (the tenants of the refs tables that are not
	// in lokadb.orgs); one named in Orgs is compared whether or not this is set.
	Cloud bool
}

// DefaultMaxSequenceJump: rows written past the cloud move the sequence on by about their
// number; a jump larger than this is shown and left to the operator.
const DefaultMaxSequenceJump = 100000

// Clean: nothing to fix.
func (d RefsDiff) Clean() bool {
	return d.Missing == 0 && d.Conflict == 0 && d.Stale == 0 && d.SequenceBehind == ""
}

// RefsCompare compares, and with o.Sync fixes, the refs tables (and maps of unique columns)
// of every moved organization, and of the organizations in the cloud o asks for.
func RefsCompare(ctx context.Context, o RefsOptions) ([]RefsDiff, error) {
	cloud, err := connect(ctx, o.CloudDSN)
	if err != nil {
		return nil, err
	}
	defer cloud.Close(ctx)
	// The table's key (block 4: lokadb.tables.key_column; id before it) and its sequence: an
	// identity's, or the one the parent's default calls (enable leaves the sequence owned by
	// <table>__cloud, the original table); none for a key of another kind (uuid, text).
	tables, err := rowsOf(ctx, cloud, `SELECT t.table_name, t.refs_table,
		coalesce(pg_get_serial_sequence(`+admin.RelSQL("t.table_name")+`, k.col),
			(SELECT dep.refobjid::regclass::text FROM pg_attrdef ad
			 JOIN pg_attribute a ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum AND a.attname = k.col
			 JOIN pg_depend dep ON dep.classid = 'pg_attrdef'::regclass AND dep.objid = ad.oid
			  AND dep.refclassid = 'pg_class'::regclass
			 JOIN pg_class s ON s.oid = dep.refobjid AND s.relkind = 'S'
			 WHERE ad.adrelid = to_regclass(`+admin.RelSQL("t.table_name")+`) LIMIT 1), ''),
		k.key, t.tenant_column
		FROM lokadb.tables t CROSS JOIN LATERAL (SELECT coalesce(to_jsonb(t) ->> 'key_column', 'id') AS col) k0
		-- a composite key (a JSON array of its columns) has no sequence of its own to check
		CROSS JOIN LATERAL (SELECT CASE WHEN left(k0.col, 1) = '[' THEN NULL ELSE k0.col END AS col, k0.col AS key) k
		ORDER BY t.table_name`)
	if err != nil {
		return nil, fmt.Errorf("the LokaDB tables (lokadb.tables): %w", err)
	}
	maps := map[string][][]string{} // table -> (column, map table)
	if r, err := rowsOf(ctx, cloud, `SELECT (to_regclass('lokadb.unique_refs') IS NOT NULL)::text`); err != nil {
		return nil, err
	} else if r[0][0] == "true" {
		rows, err := rowsOf(ctx, cloud, `SELECT table_name, column_name, refs_table FROM lokadb.unique_refs ORDER BY 1, 2`)
		if err != nil {
			return nil, err
		}
		for _, m := range rows {
			maps[m[0]] = append(maps[m[0]], m[1:])
		}
	}
	movedRows, err := rowsOf(ctx, cloud, `SELECT org::text FROM lokadb.orgs ORDER BY org`)
	if err != nil {
		return nil, fmt.Errorf("the moved organizations (lokadb.orgs): %w", err)
	}
	lock, err := rowsOf(ctx, cloud, `SELECT (to_regprocedure('lokadb.org_lock(bigint)') IS NOT NULL)::text`)
	if err != nil {
		return nil, err
	}
	type target struct {
		ten   admin.Tenant
		cloud bool
	}
	var targets []target
	moved := map[string]bool{}
	for _, r := range movedRows {
		moved[r[0]] = true
		if len(o.Orgs) > 0 && !contains(o.Orgs, r[0]) {
			continue
		}
		var n int64
		fmt.Sscan(r[0], &n)
		// block 18: the refs name an organization numbered by uuid by its value (lokadb.tenants)
		ten, err := admin.ResolveTenant(ctx, cloud, n, "")
		if err != nil {
			return nil, fmt.Errorf("organization %s: %w", r[0], err)
		}
		targets = append(targets, target{ten, false})
	}
	cloudOrgs := map[string]bool{}
	for _, x := range o.Orgs {
		if !moved[x] {
			cloudOrgs[x] = true
		}
	}
	if o.Cloud {
		// the organizations the refs tables name, other than the moved ones
		for _, t := range tables {
			rows, err := rowsOf(ctx, cloud, fmt.Sprintf("SELECT DISTINCT tenant::text FROM %s WHERE tenant IS NOT NULL", admin.QR(t[1])))
			if err != nil {
				return nil, err
			}
			for _, r := range rows {
				cloudOrgs[r[0]] = true
			}
		}
	}
	cloudList := make([]string, 0, len(cloudOrgs))
	for x := range cloudOrgs {
		cloudList = append(cloudList, x)
	}
	slices.Sort(cloudList)
	seen := map[int64]bool{}
	for _, x := range cloudList {
		var n int64
		value := ""
		if _, err := fmt.Sscan(x, &n); err != nil || fmt.Sprint(n) != x {
			n, value = 0, x // a uuid organization's value, as the refs hold it
		}
		ten, err := admin.ResolveTenant(ctx, cloud, n, value)
		if err != nil {
			return nil, fmt.Errorf("organization %s: %w", x, err)
		}
		if moved[fmt.Sprint(ten.Org)] || seen[ten.Org] {
			continue // the value of a moved one, or one already listed
		}
		seen[ten.Org] = true
		targets = append(targets, target{ten, true})
	}
	var out []RefsDiff
	for _, tg := range targets {
		org := fmt.Sprint(tg.ten.Org)
		for _, t := range tables {
			table, refs, seq, key, tc := t[0], t[1], t[2], t[3], t[4]
			// the organization's rows: its foreign partition, or the parent's rows of it in the
			// cloud (whose ids the cloud's sequence gave: none to check)
			src := admin.QR(admin.Helper(table, fmt.Sprintf("__org%s", org)))
			if tg.cloud {
				src, seq = fmt.Sprintf("(SELECT * FROM %s WHERE %s = %s)", admin.QR(table), q(tc), tg.ten.Lit()), ""
			}
			d, err := refsTable(ctx, cloud, o, table, refs, src, tg.ten, seq, key, false, lock[0][0] == "true")
			if err != nil {
				return nil, fmt.Errorf("%s, organization %s: %w", table, org, err)
			}
			d.Cloud = tg.cloud
			out = append(out, d)
			for _, m := range maps[table] {
				d, err := refsTable(ctx, cloud, o, table, m[1], src, tg.ten, "", m[0], true, lock[0][0] == "true")
				if err != nil {
					return nil, fmt.Errorf("%s (%s), organization %s: %w", table, m[0], org, err)
				}
				d.Column, d.Cloud = m[0], tg.cloud
				out = append(out, d)
			}
		}
	}
	return out, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// refsTable compares one table of one organization: its rows (src, an SQL FROM item: its
// partition, or the parent's rows of it) by the table's key (key), against the refs table
// (refs, whose id holds the key); or, with unique, by a unique column (key) against its map,
// which has no row for a NULL value.
func refsTable(ctx context.Context, cloud *pgx.Conn, o RefsOptions, table, refs, src string, ten admin.Tenant, seq, key string, unique, orgLock bool) (RefsDiff, error) {
	d := RefsDiff{Org: fmt.Sprint(ten.Org), Table: table}
	org := ten.Lit() // the organization as the refs hold it
	tx, err := cloud.Begin(ctx)
	if err != nil {
		return d, err
	}
	defer tx.Rollback(ctx)
	if o.Sync && orgLock {
		// The organization pauses (as for a move's split): a write through the proxy has its
		// row at the organization and its refs row still uncommitted in the cloud; adding
		// that id here would make its commit fail.
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock("+ten.LockCall()+")"); err != nil {
			return d, err
		}
	}
	p, rf := src, admin.QR(refs)
	count := func(sql string) (int64, []string, error) {
		rows, err := rowsOf(ctx, tx, "SELECT count(*)::text, coalesce((array_agg(id::text ORDER BY id))[1:5]::text, '{}') FROM ("+sql+") x")
		if err != nil {
			return 0, nil, err
		}
		var n int64
		fmt.Sscan(rows[0][0], &n)
		ids := strings.Trim(rows[0][1], "{}")
		if ids == "" {
			return n, nil, nil
		}
		return n, strings.Split(ids, ","), nil
	}
	// the key: one column (refs.id), or several (a composite key: refs.id_1 ... id_n); shown as
	// its value, or a JSON array of its values
	kc, rc := admin.KeyCols(key), admin.RefsKeyCols(key)
	var eq, fk, rk []string
	for i := range kc {
		eq = append(eq, "r."+q(rc[i])+" = f."+q(kc[i]))
		fk, rk = append(fk, "f."+q(kc[i])), append(rk, "r."+q(rc[i]))
	}
	match := strings.Join(eq, " AND ")
	k, rid := fk[0], rk[0]
	if len(kc) > 1 {
		k, rid = "jsonb_build_array("+strings.Join(fk, ", ")+")", "jsonb_build_array("+strings.Join(rk, ", ")+")"
	}
	notThere := fmt.Sprintf("FROM %s f WHERE NOT EXISTS (SELECT FROM %s r WHERE %s)", p, rf, match)
	if unique {
		notThere += " AND " + fk[0] + " IS NOT NULL"
	}
	staleWhere := fmt.Sprintf("r.tenant = %s AND NOT EXISTS (SELECT FROM %s f WHERE %s)", org, p, match)
	stale := fmt.Sprintf("SELECT %s AS id FROM %s r WHERE %s", rid, rf, staleWhere)
	if d.Missing, d.MissingIDs, err = count("SELECT " + k + " AS id " + notThere); err != nil {
		return d, err
	}
	if d.Conflict, d.ConflictIDs, err = count(fmt.Sprintf("SELECT %s AS id FROM %s f JOIN %s r ON %s WHERE r.tenant IS DISTINCT FROM %s", k, p, rf, match, org)); err != nil {
		return d, err
	}
	if d.Stale, d.StaleIDs, err = count(stale); err != nil {
		return d, err
	}
	if seq != "" {
		rows, err := rowsOf(ctx, tx, fmt.Sprintf("SELECT coalesce((SELECT max(%[1]s) FROM %[2]s f) > (SELECT CASE WHEN is_called THEN last_value ELSE last_value - 1 END FROM %[3]s), false)::text, coalesce((SELECT max(%[1]s) FROM %[2]s f)::text, '')", k, p, seq))
		if err != nil {
			return d, err
		}
		if rows[0][0] == "true" {
			d.SequenceBehind = rows[0][1]
		}
	}
	var seqFrom string
	if d.SequenceBehind != "" {
		// how far the sequence would go, and whether its range holds the id at all
		rows, err := rowsOf(ctx, tx, fmt.Sprintf(`SELECT s.last_value::text, (%[2]s::numeric - s.last_value)::text,
				(%[2]s::numeric > q.seqmax)::text, (%[2]s::numeric - s.last_value > %[3]d)::text
			FROM %[1]s s, pg_sequence q WHERE q.seqrelid = %[4]s::regclass`, seq, d.SequenceBehind, maxJump(o), sqlLit(seq)))
		if err != nil {
			return d, err
		}
		seqFrom = rows[0][0]
		switch {
		case rows[0][2] == "true":
			d.SequenceKept = fmt.Sprintf("the organization's id %s is past the largest value the sequence may give: fix that row by hand", d.SequenceBehind)
		case rows[0][3] == "true":
			d.SequenceKept = fmt.Sprintf("moving it from %s to %s (by %s) is more than -max-sequence-jump %d: look at the organization's ids, then run refs-sync -max-sequence-jump %s",
				seqFrom, d.SequenceBehind, rows[0][1], maxJump(o), rows[0][1])
		}
	}
	if !o.Sync || d.Clean() {
		return d, nil
	}
	var did []string
	if d.Missing > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s, tenant) SELECT %s, %s %s", rf, strings.Join(quoted(rc), ", "), strings.Join(fk, ", "), org, notThere)); err != nil {
			return d, err
		}
		did = append(did, fmt.Sprintf("added %d id(s)", d.Missing))
	}
	if d.Stale > 0 && o.DeleteStale {
		if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s r WHERE %s", rf, staleWhere)); err != nil {
			return d, fmt.Errorf("deleting the stale ids: %w", err)
		}
		did = append(did, fmt.Sprintf("deleted %d stale id(s)", d.Stale))
	}
	if d.SequenceBehind != "" && d.SequenceKept == "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SELECT setval(%s, %s)", sqlLit(seq), d.SequenceBehind)); err != nil {
			return d, err
		}
		did = append(did, fmt.Sprintf("sequence moved from %s to %s", seqFrom, d.SequenceBehind))
	}
	if err := tx.Commit(ctx); err != nil {
		return d, err
	}
	d.Fixed = strings.Join(did, ", ")
	return d, nil
}

func maxJump(o RefsOptions) int64 {
	if o.MaxSequenceJump > 0 {
		return o.MaxSequenceJump
	}
	return DefaultMaxSequenceJump
}

func sqlLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// RefsText: the report as an operator reads it.
func RefsText(ds []RefsDiff, sync bool) string {
	var b strings.Builder
	if len(ds) == 0 {
		b.WriteString("no organization is moved\n")
	}
	for _, d := range ds {
		where := ""
		if d.Cloud {
			where = " (in the cloud)"
		}
		if d.Column != "" {
			fmt.Fprintf(&b, "organization %s%s, %s, the map of %s: ", d.Org, where, d.Table, d.Column)
		} else {
			fmt.Fprintf(&b, "organization %s%s, %s: ", d.Org, where, d.Table)
		}
		if d.Clean() {
			b.WriteString("refs match the organization's rows\n")
			continue
		}
		var parts []string
		if d.Missing > 0 {
			parts = append(parts, fmt.Sprintf("%d row(s) written past the cloud, not in the refs table (ids %s)", d.Missing, strings.Join(d.MissingIDs, ",")))
		}
		if d.Conflict > 0 {
			parts = append(parts, fmt.Sprintf("%d id(s) the refs table gives to another organization (ids %s): fix by hand", d.Conflict, strings.Join(d.ConflictIDs, ",")))
		}
		if d.Stale > 0 {
			s := fmt.Sprintf("%d id(s) of rows the organization no longer has (ids %s)", d.Stale, strings.Join(d.StaleIDs, ","))
			if sync && !strings.Contains(d.Fixed, "stale") {
				s += ": kept (refs-sync -delete-stale deletes them)"
			}
			parts = append(parts, s)
		}
		if d.SequenceBehind != "" {
			parts = append(parts, "its largest id "+d.SequenceBehind+" is past the cloud's sequence")
		}
		if sync && d.SequenceKept != "" {
			parts = append(parts, "the sequence is not moved: "+d.SequenceKept)
		}
		b.WriteString(strings.Join(parts, "; "))
		if d.Fixed != "" {
			b.WriteString("; fixed: " + d.Fixed)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// RefsLeft: what refs-sync left (or refs-diff found) still to fix.
func RefsLeft(ds []RefsDiff, sync, deleteStale bool) bool {
	for _, d := range ds {
		switch {
		case !sync && !d.Clean():
			return true
		case sync && (d.Conflict > 0 || d.Stale > 0 && !deleteStale || d.SequenceKept != ""): // refs-sync fixed the rest
			return true
		}
	}
	return false
}

func quoted(cols []string) []string {
	var out []string
	for _, c := range cols {
		out = append(out, q(c))
	}
	return out
}
