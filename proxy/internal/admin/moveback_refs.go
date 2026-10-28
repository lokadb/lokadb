package admin

import (
	"context"
	"fmt"
	"strings"
)

// The refs of the rows move-back returns (review3 R3-4). While the organization was in its own
// database, a row written there past the cloud (by its own application, a script, a restore)
// has no row in the refs table, and a row deleted there leaves its refs row behind. move-back
// copied the rows and left the refs as they were: an FK of the cloud onto the returned row
// failed, the deleted row's id stayed taken, and refs-diff, which compares only organizations
// in lokadb.orgs, no longer showed either. Now the switch brings the refs tables (and the maps
// of unique columns, lokadb.unique_refs) to the returned rows, in its transaction, with the log
// drained and no writer left; an id another organization holds there stops it before anything
// is switched.

// refsMap: a refs table and the columns of the table it holds (its own columns alongside).
type refsMap struct {
	refs       string
	cols, rcol []string
	nullable   bool // a unique column's map: a NULL value has no row
}

// refsMaps: the refs table of t and the maps of its unique columns.
func refsMaps(ctx context.Context, conn querier, t moveTable) ([]refsMap, error) {
	out := []refsMap{{refs: hn(t.name, "__refs"), cols: KeyCols(keyOf(t)), rcol: RefsKeyCols(keyOf(t))}}
	rows, err := rowsOf(ctx, conn, `SELECT column_name, refs_table FROM lokadb.unique_refs WHERE table_name = $1 ORDER BY 1`, t.name)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out = append(out, refsMap{refs: r[1], cols: []string{r[0]}, rcol: []string{"id"}, nullable: true})
	}
	return out, nil
}

// refsSteps: the statements that bring the refs of t to its local partition (the returned
// rows), or an error naming the ids another organization holds in them.
func refsSteps(ctx context.Context, conn querier, t moveTable) ([]string, error) {
	maps, err := refsMaps(ctx, conn, t)
	if err != nil {
		return nil, err
	}
	var steps []string
	for _, m := range maps {
		var eq, fc, rc, notNull []string
		for i := range m.cols {
			eq = append(eq, "r."+q(m.rcol[i])+" = f."+q(m.cols[i]))
			fc, rc = append(fc, "f."+q(m.cols[i])), append(rc, q(m.rcol[i]))
			notNull = append(notNull, "f."+q(m.cols[i])+" IS NOT NULL")
		}
		match, part, refs := strings.Join(eq, " AND "), qr(t.local()), qr(m.refs)
		rows := part + " f"
		if m.nullable {
			rows = fmt.Sprintf("(SELECT * FROM %s f WHERE %s) f", part, strings.Join(notNull, " AND "))
		}
		conflict, err := rowsOf(ctx, conn, fmt.Sprintf(`SELECT count(*)::text, coalesce((array_agg(x ORDER BY x))[1:5]::text, '{}')
			FROM (SELECT concat_ws(', ', %s) AS x FROM %s JOIN %s r ON %s WHERE r.tenant IS DISTINCT FROM %s) c`,
			strings.Join(fc, ", "), rows, refs, match, t.val))
		if err != nil {
			return nil, err
		}
		if conflict[0][0] != "0" {
			return nil, fmt.Errorf("%s: %s row(s) of organization %d have an id another organization holds in %s (%s): written in its database past the cloud; "+
				"nothing switched, the organization stays in its own database: change or delete those rows there, then run move-back again",
				t.name, conflict[0][0], t.org, m.refs, strings.Trim(conflict[0][1], "{}"))
		}
		steps = append(steps,
			// rows the organization deleted past the cloud: their ids are free again
			fmt.Sprintf("DELETE FROM %s r WHERE r.tenant = %s AND NOT EXISTS (SELECT FROM %s WHERE %s)", refs, t.val, rows, match),
			// rows it wrote past the cloud
			fmt.Sprintf("INSERT INTO %s (%s, tenant) SELECT %s, %s FROM %s WHERE NOT EXISTS (SELECT FROM %s r WHERE %s)",
				refs, strings.Join(rc, ", "), strings.Join(fc, ", "), t.val, rows, refs, match))
	}
	return steps, nil
}
