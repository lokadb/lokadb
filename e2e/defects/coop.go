package main

// The cooperative mode (block 10, tasks/cooperative-mode.md): the application names the
// organization (SET LOCAL lokadb.tenant / set_config('lokadb.tenant', …, true)) and the proxy
// takes it instead of guessing. Organization 1 is in its own database, the fanout policy is
// reject.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "C1", name: "the cooperative mode: the named organization is taken, reconciled and never widened", run: checkCooperative},
	)
}

// pgCode: the SQLSTATE of err ("" none).
func pgCode(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	if err != nil {
		return "?"
	}
	return ""
}

func checkCooperative(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{"1": e.appCustomer()}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	doc2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)

	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// tx runs sqls in one transaction after set_config('lokadb.tenant', org, true), the last
	// one through the extended protocol with params; it returns the last one's rows and error.
	tx := func(org string, sql string, params ...string) ([][]byte, error) {
		if _, err := simple(e.ctx, c, "BEGIN"); err != nil {
			return nil, err
		}
		defer simple(e.ctx, c, "ROLLBACK") //nolint:errcheck // a no-op after COMMIT
		if err := c.ExecParams(e.ctx, "SELECT set_config('lokadb.tenant', $1, true)", [][]byte{[]byte(org)}, nil, nil, nil).Read().Err; err != nil {
			return nil, err
		}
		var ps [][]byte
		for _, p := range params {
			ps = append(ps, []byte(p))
		}
		r := c.ExecParams(e.ctx, sql, ps, nil, nil, nil).Read()
		if r.Err != nil {
			return nil, r.Err
		}
		if _, err := simple(e.ctx, c, "COMMIT"); err != nil {
			return nil, err
		}
		var col [][]byte
		for _, row := range r.Rows {
			col = append(col, row[0])
		}
		return col, nil
	}

	// P4: an INSERT with an explicit id looks its id up in the refs table, which cannot hold it
	// yet. Named, nothing is looked up: the application may not even read the refs table.
	if err := execSQL(e.ctx, e.cloud(), "REVOKE SELECT ON documents__refs FROM app"); err != nil {
		return false, "", err
	}
	_, err = tx("1", "INSERT INTO documents (id, organization_id, title) VALUES ($1, 1, 'c1-p4') RETURNING id", "900001")
	where, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'c1-p4'")
	note(err == nil && where == "1", fmt.Sprintf("P4: INSERT with an explicit id under lokadb.tenant = 1, refs table not readable: error %q, rows in organization 1's database %s", firstErrLineOr(err), where))
	if err := execSQL(e.ctx, e.cloud(), "GRANT SELECT ON documents__refs TO app"); err != nil {
		return false, "", err
	}

	// 1.3: a condition on a column without a lookup (no FK, no relation) is a fanout under
	// reject; named, it runs in organization 1 only.
	rows, err := tx("1", "SELECT count(*) FROM documents WHERE title LIKE $1", "d%")
	n := ""
	if len(rows) == 1 {
		n = string(rows[0])
	}
	note(err == nil && n == "100", fmt.Sprintf("1.3: SELECT by a column without a lookup under lokadb.tenant = 1: error %q, count %s (organization 1 has 100)", firstErrLineOr(err), n))

	// reconciliation: the statement names organization 2 under lokadb.tenant = 1
	rows, err = tx("1", "SELECT title FROM documents WHERE organization_id = $1", "2")
	note(pgCode(err) == "42501" && len(rows) == 0, fmt.Sprintf("organization_id = 2 under lokadb.tenant = 1: code %q, %d rows (want 42501)", pgCode(err), len(rows)))

	// the review's case: a row of organization 2 by its id, the application in organization 1
	rows, err = tx("1", "SELECT title FROM documents WHERE id = $1", doc2)
	note(err == nil && len(rows) == 0, fmt.Sprintf("a document of organization 2 by its id under lokadb.tenant = 1: error %q, %d rows (want 0: the proxy does not widen what the statement may read)", firstErrLineOr(err), len(rows)))
	rows, err = tx("1", "SELECT title FROM documents WHERE id = $1", doc1)
	note(err == nil && len(rows) == 1, fmt.Sprintf("its own document by id: error %q, %d rows", firstErrLineOr(err), len(rows)))

	// block 9's condition: a child row named into another organization than its parent's; the
	// proxy does not look the parent up, lokadb_parent refuses it (block 3)
	_, err = tx("2", "INSERT INTO document_versions (document_id, content) VALUES ($1, 'c1-child')", doc1)
	wrong, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content = 'c1-child' AND organization_id <> 1")
	note(pgCode(err) == "23514" && wrong == "0", fmt.Sprintf("a version of organization 1's document under lokadb.tenant = 2: code %q (want 23514), rows under another organization than the document's: %s", pgCode(err), wrong))

	// the value is checked: not an organization
	_, err = tx("acme", "SELECT 1")
	note(pgCode(err) == "22023", fmt.Sprintf("lokadb.tenant = 'acme': code %q (want 22023)", pgCode(err)))

	// tenant.local_only is on by default (the coordinator's decision of 24.10, block 9's
	// B9.11): a value for the whole session would reach the next client behind a pooler in
	// transaction mode
	_, err = simple(e.ctx, c, "SET lokadb.tenant = '1'")
	note(pgCode(err) == "0A000", fmt.Sprintf("SET lokadb.tenant = '1' for the session, no \"local_only\" in the config: code %q (want 0A000)", pgCode(err)))
	return ok, strings.Join(notes, "; "), nil
}

func firstErrLineOr(err error) string {
	if err == nil {
		return ""
	}
	return firstErrLine(err)
}
