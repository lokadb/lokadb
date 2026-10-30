package check

// Block 7: can the proxy replay DDL in the databases of the organizations moved already.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

// replayReady: for every moved organization, as the proxy reaches its database
// (lokadb.replay_targets: its FDW server and user mapping), may that role create in the
// schema of its tables. CREATE INDEX needs it, and the proxy replays CREATE INDEX as that
// role: without it every CREATE INDEX on a LokaDB table is refused, in every organization.
func (c *checker) replayReady() {
	has, err := c.rows(`SELECT (to_regprocedure('lokadb.replay_targets()') IS NOT NULL)::text`)
	if err != nil || has[0][0] != "true" {
		return // enabled by an older version: the proxy replays by its config's customers
	}
	rows, err := c.rows(`SELECT org::text, org_schema, coalesce(array_to_json(server_options), '[]')::text,
		coalesce(array_to_json(mapping_options), '[]')::text FROM lokadb.replay_targets()`)
	if err != nil {
		c.addFinding(Finding{Severity: Warning, Code: "replay_targets_unreadable", Message: "the moved organizations' databases cannot be checked: " + firstLine(err.Error())})
		return
	}
	for _, r := range rows {
		var srv, um []string
		_ = json.Unmarshal([]byte(r[2]), &srv)
		_ = json.Unmarshal([]byte(r[3]), &um)
		org, schema := r[0], r[1]
		if len(srv) == 0 || len(um) == 0 {
			c.addFinding(Finding{Severity: Blocker, Code: "replay_no_credentials", Object: "organization " + org, Subject: orgSubject(org),
				Message: fmt.Sprintf("organization %s: no user mapping of its FDW server: the proxy refuses schema changes of LokaDB tables (it cannot replay them there) unless its config gives the database in customers", org)})
			continue
		}
		ctx, cancel := context.WithTimeout(c.ctx, 10*time.Second)
		conn, err := tunnel.PGConnect(ctx, proxy.OptionsDSN(srv, um))
		if err != nil {
			cancel()
			c.addFinding(Finding{Severity: Warning, Code: "replay_unreachable", Object: "organization " + org, Subject: orgSubject(org),
				Message: fmt.Sprintf("organization %s: its database cannot be reached as the proxy reaches it: %s", org, firstLine(err.Error()))})
			continue
		}
		res, err := conn.Exec(ctx, fmt.Sprintf("SELECT current_user::text, has_schema_privilege(%s, 'CREATE')::text", sqlLit(schema))).ReadAll()
		conn.Close(ctx)
		cancel()
		if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
			c.addFinding(Finding{Severity: Warning, Code: "replay_unreachable", Object: "organization " + org, Subject: orgSubject(org),
				Message: fmt.Sprintf("organization %s: its database cannot be inspected: %v", org, err)})
			continue
		}
		role, may := string(res[0].Rows[0][0]), string(res[0].Rows[0][1])
		if may != "true" {
			c.addFinding(Finding{Severity: Blocker, Code: "customer_app_no_create_on_schema", Object: "organization " + org,
				Subject: &Subject{Kind: "schema", Side: "customer", Schema: schema, Role: role, Database: "organization " + org},
				Message: fmt.Sprintf("organization %s: role %s may not create in schema %s of its database: the proxy replays CREATE INDEX as that role, so every CREATE INDEX on a LokaDB table is refused. The organization grants it: GRANT CREATE ON SCHEMA %s TO %s", org, role, schema, q(schema), q(role))})
		}
	}
}

func sqlLit(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

// ddlWriteTransactions finds transaction shapes that change the columns of a LokaDB table and
// then write to the same table (Django: AddField and RunPython in one migration, one
// transaction). Through the proxy the organization's database gets the DDL just before
// COMMIT, so the write in between does not find the new column there: it fails, the
// transaction rolls back, nothing is lost.
func (c *checker) ddlWriteTransactions(txs []*proxy.RecordedTx) {
	type hitTx struct {
		tx     *proxy.RecordedTx
		tables []string
	}
	var hits []hitTx
	for _, tx := range txs {
		changed := map[string]bool{} // LokaDB tables whose columns this transaction changed so far
		hit := map[string]bool{}
		for _, s := range tx.Statements {
			for _, t := range c.columnDDL(s) {
				changed[t] = true
			}
			for t := range c.writes(s).lokadb {
				if changed[t] {
					hit[t] = true
				}
			}
		}
		if len(hit) > 0 {
			hits = append(hits, hitTx{tx, keys(hit)})
		}
	}
	if len(hits) == 0 {
		return
	}
	all := map[string]bool{}
	var total, top int64 = 0, -1
	var example []string
	var shapes []TxShape
	for _, h := range hits {
		for _, t := range h.tables {
			all[t] = true
		}
		total += h.tx.Count
		var stmts []TxStatement
		var short1 []string
		for _, s := range h.tx.Statements {
			fp, _ := pg.Fingerprint(analyzable(s))
			w := c.writes(s)
			stmts = append(stmts, TxStatement{Fingerprint: fp, SQL: s, Write: len(w.all()) > 0, Tables: append(c.columnDDL(s), w.all()...)})
			short1 = append(short1, short(s))
		}
		shapes = append(shapes, TxShape{LokadbTables: h.tables, Shapes: 1, Count: h.tx.Count, Statements: stmts})
		if h.tx.Count > top {
			top, example = h.tx.Count, short1
		}
	}
	tables := keys(all)
	c.addFinding(Finding{Severity: Warning, Code: "ddl_then_write_transaction", Object: strings.Join(tables, ", "),
		Subject:      &Subject{Kind: "tables", LokadbTables: tables},
		Transactions: shapes, Examples: example,
		Message: fmt.Sprintf("%s change the columns of LokaDB tables (%s) and then write to them, %s seen. The organizations' databases get the schema change just before COMMIT, so a write in between does not find the new column there and fails, and the transaction rolls back. Split the schema change and the data write into separate transactions or migrations (Django: one migration for AddField, another for RunPython)",
			plural(len(hits), "transaction shape", "transaction shapes"), strings.Join(tables, ", "), plural(int(total), "transaction", "transactions"))})
}

// columnDDL lists the LokaDB tables whose columns a statement changes (ALTER TABLE, RENAME COLUMN).
func (c *checker) columnDDL(sql string) []string {
	tree, err := pg.Parse(analyzable(sql))
	if err != nil {
		return nil
	}
	var out []string
	for _, raw := range tree.Stmts {
		switch {
		case raw.Stmt.GetAlterTableStmt() != nil:
			if rv := raw.Stmt.GetAlterTableStmt().Relation; c.lokadbRel(rv) {
				out = append(out, rv.Relname)
			}
		case raw.Stmt.GetRenameStmt() != nil:
			if r := raw.Stmt.GetRenameStmt(); r.RenameType == pg.ObjectType_OBJECT_COLUMN && c.lokadbRel(r.Relation) {
				out = append(out, r.Relation.Relname)
			}
		}
	}
	return out
}

// orgSubject: the subject of a finding about one moved organization's database.
func orgSubject(org string) *Subject {
	return &Subject{Kind: "database", Side: "customer", Database: "organization " + org}
}
