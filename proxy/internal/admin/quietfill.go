package admin

import (
	"fmt"
	"strings"
)

// The organization column of a child table is filled from its parent's refs table: an UPDATE
// of every row. The application's triggers must not see it (review 3, R4-5): a Supabase
// moddatetime or Hasura set_current_timestamp_updated_at rewrote updated_at of every row, a
// version counter went up, an audit trigger logged an UPDATE per row, a trigger of "rows are
// immutable" made enable impossible.

// userTriggers: the application's triggers of table t ($1, as SQL names it): name and
// tgenabled (O origin, R replica, A always, D disabled); not lokadb's own.
const userTriggersSQL = `SELECT tgname::text, tgenabled::text FROM pg_trigger
	WHERE tgrelid = $1::regclass AND NOT tgisinternal AND tgname NOT LIKE 'lokadb\_prep\_%' ORDER BY tgname`

// quietly runs fill with the application's triggers of t off: DISABLE TRIGGER for each that is
// on, ENABLE again in its mode (O, R, A) after. In the transaction of enable the table is
// locked meanwhile; a rollback puts them back with everything else.
func (r *runner) quietly(t string, fill func() error) error {
	trs, err := r.rows(userTriggersSQL, qr(t))
	if err != nil {
		return err
	}
	var on [][2]string
	for _, g := range trs {
		if g[1] != "D" {
			on = append(on, [2]string{g[0], g[1]})
		}
	}
	for _, g := range on {
		if err := r.exec(fmt.Sprintf("ALTER TABLE %s DISABLE TRIGGER %s", qr(t), q(g[0]))); err != nil {
			return err
		}
	}
	if err := fill(); err != nil {
		return err
	}
	for _, g := range on {
		mode := map[string]string{"O": "ENABLE", "R": "ENABLE REPLICA", "A": "ENABLE ALWAYS"}[g[1]]
		if err := r.exec(fmt.Sprintf("ALTER TABLE %s %s TRIGGER %s", qr(t), mode, q(g[0]))); err != nil {
			return err
		}
	}
	return nil
}

// quietNeeded: enable -prepare fills the column on a table the application keeps writing: its
// triggers cannot be switched off. The session leaves them out with session_replication_role =
// replica (only this session's batches; a trigger enabled ALWAYS or REPLICA still fires under
// it). Returns the names of the triggers that fire either way, and whether there are triggers
// at all.
func (c *prep) quietNeeded(t string) (always []string, has bool, err error) {
	trs, err := rowsOf(c.ctx, c.conn, userTriggersSQL, qr(t))
	if err != nil {
		return nil, false, err
	}
	for _, g := range trs {
		if g[1] == "D" {
			continue
		}
		has = true
		if g[1] == "A" || g[1] == "R" {
			always = append(always, g[0])
		}
	}
	return always, has, nil
}

// refuseQuiet: why enable -prepare cannot fill the organization column of t without its
// triggers firing, in words that say what to do.
func refuseQuiet(t string, always []string, why string) error {
	if len(always) > 0 {
		return fmt.Errorf("%s has triggers of the application that fire for any session (enabled ALWAYS or REPLICA: %s): filling its organization column would run them on every row (updated_at, versions, an audit, a webhook per row). Run enable without -prepare, which holds the table and switches them off for the fill, or disable them for the time of enable -prepare", t, strings.Join(always, ", "))
	}
	return fmt.Errorf("%s has triggers of the application, and this role cannot set session_replication_role to keep them out of the fill of its organization column (%s): it would run them on every row (updated_at, versions, an audit, a webhook per row). Use a superuser (or, from PostgreSQL 15, a role granted SET on session_replication_role) for enable -prepare, or run enable without -prepare, which holds the table and switches them off for the fill", t, why)
}
