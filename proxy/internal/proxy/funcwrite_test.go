package proxy

import "testing"

// review3 R4-25: a call of a function over LokaDB tables (select, CALL, a table function, in a
// condition) and a MERGE into a view of a LokaDB table are writes: under reject-writes and under a
// "read" decision they must not run in every organization.
func TestFunctionCallIsWrite(t *testing.T) {
	m := testMeta()
	m.Funcs = map[string]bool{"purge_documents": true, "archive_doc": true}
	m.Views = map[string][]string{"docs_v": {"documents"}}
	f, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: FanoutRejectWrites}}}.FanoutFor("db")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := Config{Fanout: map[string]FanoutPolicy{"db": {Mode: FanoutReject, Tables: map[string]string{"documents": TableRead}}}}.FanoutFor("db")
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{
		`select purge_documents()`,
		`call purge_documents()`,
		`select * from purge_documents()`,
		`select count(*) from documents d where archive_doc(d.id)`,
		`merge into docs_v d using (values (1, 'x')) s(id, title) on d.id = s.id when matched then update set title = s.title`,
	} {
		an := analyze(sql, m)
		if !an.Write {
			t.Errorf("%s: not a write", sql)
		}
		if v := f.Decide(sql, an); v == FanoutRun || v == FanoutAllowed {
			t.Errorf("%s: runs in every organization under reject-writes (verdict %d)", sql, v)
		}
		if v := fr.Decide(sql, an); v == FanoutAllowed {
			t.Errorf("%s: allowed everywhere by the read decision", sql)
		}
	}
}
