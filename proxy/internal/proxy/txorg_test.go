package proxy

import "testing"

// A query string with transaction control still gets lokadb.org set before it, unless a
// ROLLBACK inside it would undo that before a statement on a LokaDB table.
func TestTxControlOrg(t *testing.T) {
	m := testMeta()
	cases := []struct {
		sql                  string
		lokadb, txControl    bool
		reverts, orgReverted bool
	}{
		{"BEGIN; INSERT INTO document_versions (document_id, content) VALUES (10, 'x'); COMMIT", true, true, false, false},
		{"INSERT INTO documents (organization_id, title) VALUES (1, 'a'); COMMIT", true, true, false, false},
		{"BEGIN; UPDATE documents SET title = 'b' WHERE organization_id = 1; ROLLBACK", true, true, true, false},
		{"ROLLBACK; INSERT INTO document_versions (document_id, content) VALUES (10, 'x')", true, true, true, true},
		{"SAVEPOINT s; SELECT 1; ROLLBACK TO s; DELETE FROM documents WHERE organization_id = 1", true, true, true, true},
		{"ROLLBACK; SELECT 1", false, true, true, false},
		{"COMMIT", false, true, false, false},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.Lokadb != c.lokadb || a.TxControl != c.txControl || a.Reverts != c.reverts || a.OrgReverted != c.orgReverted {
			t.Errorf("%s: lokadb %v txcontrol %v reverts %v orgReverted %v, want %v %v %v %v", c.sql,
				a.Lokadb, a.TxControl, a.Reverts, a.OrgReverted, c.lokadb, c.txControl, c.reverts, c.orgReverted)
		}
	}
}
