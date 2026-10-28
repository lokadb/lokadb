package proxy

import (
	"strings"
	"testing"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// DirectRefusal is the direct path's own decision on a form (directShape), with its reason:
// check -profile reports it instead of a copy (review of check, 2).
func TestDirectRefusal(t *testing.T) {
	m := testMeta()
	m.Tables["documents"].UserTypes = true
	for sql, want := range map[string]string{
		"select * from document_versions where organization_id = $1":                    "",
		"select * from documents where organization_id = $1":                            ShapeUserType,
		"update document_versions set organization_id = $1 where id = $2":               ShapeKeyUpdate,
		"select * from document_versions d join projects p on p.id = 1 where d.id = $1": "", // Direct's own reason (cloud_table), not a form's
	} {
		if got := DirectRefusal(sql, m, true, defaultPath); got != want {
			t.Errorf("%s: %q, want %q", sql, got, want)
		}
	}
}

// CatalogFacts reads the types and triggers the direct path's forms depend on for the tables
// of a Meta built before enable, from the catalog alone (no lokadb.tables).
func TestCatalogFacts(t *testing.T) {
	m := &Meta{Tables: map[string]*Table{"documents": {Name: "documents"}, "document_versions": {Name: "document_versions"}}}
	err := CatalogFacts(func(sql string) ([][]string, error) {
		if strings.Contains(sql, "lokadb.tables") || !strings.Contains(sql, "(VALUES ('document_versions'), ('documents')) t(table_name)") {
			t.Fatalf("not from the tables given: %s", sql)
		}
		switch {
		case strings.Contains(sql, "typnamespace"):
			return [][]string{{"documents"}}, nil
		case strings.Contains(sql, "tgtype & 1 = 0"):
			return [][]string{{"document_versions"}}, nil
		case strings.Contains(sql, "bit_or"):
			return [][]string{{"documents", "16"}}, nil
		}
		return nil, nil
	}, m)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Tables["documents"].UserTypes || m.Tables["document_versions"].UserTypes || !m.StmtTriggers["document_versions"] || m.RowTriggers["documents"] != trigUpdate {
		t.Fatalf("UserTypes %v/%v, StmtTriggers %v, RowTriggers %v", m.Tables["documents"].UserTypes, m.Tables["document_versions"].UserTypes, m.StmtTriggers, m.RowTriggers)
	}
}

// Review of performance, PF12: the cheap tests before parsing pass what may set something
// (a setting statement, comments before it, set_config; lokadb.tenant) and nothing else
// (UPDATE … SET, OFFSET, a column tenant_id).
func TestParsePrefilters(t *testing.T) {
	for sql, want := range map[string]bool{
		"SET statement_timeout = 0":                          true,
		"/* app */ set TimeZone = 'UTC'":                     true,
		"-- note\nRESET ALL":                                 true,
		"update t set a = 1; discard all":                    true,
		"select * from t offset 10":                          false,
		"update documents set title = 'x' where id = 1":      false,
		"select set_config('statement_timeout', '1', false)": true,
	} {
		low := strings.ToLower(sql)
		if got := strings.Contains(low, "set_config") || settingStmt.MatchString(low); got != want {
			t.Errorf("%q: parsed %v, want %v", sql, got, want)
		}
	}
	for sql, want := range map[string]bool{
		"SET LOCAL lokadb.tenant = 1":                   true,
		"select set_config('lokadb.tenant', '1', true)": true,
		"select * from documents where tenant_id = 1":   false,
		"RESET ALL": true,
	} {
		if got := mentionsTenant(sql, ""); got != want {
			t.Errorf("%q: mentionsTenant %v, want %v", sql, got, want)
		}
	}
}

// Third review of performance, perf3-27: txStmt reads the first word before it parses, and answers
// as the parse does for every statement (comments and parentheses before it go to the parse).
func TestTxStmtPrefilter(t *testing.T) {
	for _, sql := range []string{"BEGIN", "begin read only", "  \n START TRANSACTION", "commit", "COMMIT AND CHAIN", "end", "ROLLBACK", "rollback to savepoint a",
		"abort", "SAVEPOINT s1", "release s1", "PREPARE TRANSACTION 'x'", "/* a */ BEGIN", "-- a\nCOMMIT", "(select 1)", "select 1", "SELECT * FROM documents WHERE id = 1",
		"update documents set title = 'x'", "prepare p as select 1", "BEGIN; SELECT 1", "set local lokadb.tenant = 1", "commit_x", ""} {
		full := func() *pg.TransactionStmt {
			tree, err := pg.Parse(sql)
			if err != nil || len(tree.Stmts) != 1 {
				return nil
			}
			return tree.Stmts[0].Stmt.GetTransactionStmt()
		}()
		if got := txStmt(sql); (got == nil) != (full == nil) || got != nil && got.Kind != full.Kind {
			t.Errorf("%q: txStmt %v, parse %v", sql, got, full)
		}
	}
}
