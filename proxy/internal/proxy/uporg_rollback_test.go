package proxy

import (
	"strings"
	"testing"
)

// lastSetBefore: the last set_config of lokadb.org in log before position at ("" none).
func lastSetBefore(log []string, at int) string {
	for i := at - 1; i >= 0; i-- {
		if strings.Contains(log[i], "set_config('lokadb.org'") {
			return log[i]
		}
	}
	return ""
}

// R4-9 of the third review (the reviewers' test, kept): the cloud's ROLLBACK of a direct transaction is the proxy's own
// (rollbackTx: cloudRun), so upOrg keeps the lokadb.org a statement on cloud tables set inside
// that transaction, though Postgres rolled the set_config back. The next statement whose
// organization equals that stale value is sent without set_config.
func TestR4UpOrgAfterDirectRollback(t *testing.T) {
	d, l := txSession(t)
	steps := []string{
		"BEGIN",
		"SELECT title FROM documents WHERE organization_id = 1", // learnt: the direct transaction of 1
		"SELECT count(*) FROM projects",                         // cloud tables alone: the cloud's open transaction, lokadb.org '-1' (closed)
		"ROLLBACK",                                              // the proxy's own ROLLBACK in the cloud: the set_config is undone there
		"SELECT * FROM reports.org_docs",                        // a statement the proxy does not see LokaDB tables in (R1's class)
	}
	for _, q := range steps {
		_, code, _ := d.exchange(t, q)
		if code != "" {
			t.Fatalf("%s: error %s", q, code)
		}
	}
	all := l.get()
	t.Logf("ran:\n%s", strings.Join(all, "\n"))
	at := first(all, "cloud: SELECT * FROM reports.org_docs")
	rb := first(all, "cloud: ROLLBACK")
	set := lastSetBefore(all, at)
	setAt := -1
	for i := at - 1; i >= 0; i-- {
		if all[i] == set {
			setAt = i
			break
		}
	}
	if setAt >= 0 && setAt < rb {
		t.Errorf("the last set_config before the statement (%q) ran inside the transaction the cloud rolled back: the statement runs under what lokadb.org was before it (here: never set, every organization)", set)
	}
}

// The same in the cooperative mode, with organizations: a transaction under organization 2
// commits lokadb.org = '2' in the cloud; a direct transaction of organization 1 sets '1' in the
// cloud's open transaction and is rolled back; the next transaction of organization 1 that goes
// the cloud's way is sent without set_config: it runs under '2'.
func TestR4UpOrgAfterDirectRollbackTenant(t *testing.T) {
	d, l := txSession(t)
	d.s.srv.cfg.Tenant = &TenantConfig{} // local_only, the default
	run := func(qs ...string) {
		t.Helper()
		for _, q := range qs {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
	}
	// the form SELECT slug FROM documents under a named organization, learnt (as TestDirectTxTenant)
	run("BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT slug FROM documents", "COMMIT")
	run("BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT slug FROM documents", "COMMIT")
	// organization 2's transaction, the cloud's way: lokadb.org = '2' committed there
	run("BEGIN", "SET LOCAL lokadb.tenant = 2", "SELECT body FROM documents", "COMMIT")
	n := len(l.get())
	// organization 1, direct; a statement on cloud tables sets '1' in the cloud's transaction; ROLLBACK
	run("BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT slug FROM documents", "SELECT count(*) FROM projects", "ROLLBACK")
	mid := l.get()[n:]
	t.Logf("direct transaction:\n%s", strings.Join(mid, "\n"))
	// organization 1 again, a form the direct path has not learnt: the cloud's way
	n = len(l.get())
	run("BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT * FROM reports.org_docs")
	last := l.get()[n:]
	t.Logf("next transaction:\n%s", strings.Join(last, "\n"))
	if !ranAny(last, "cloud: SELECT * FROM reports.org_docs") {
		t.Fatalf("the statement did not go the cloud's way")
	}
	if !ranAny(last, "set_config('lokadb.org', '1'") {
		t.Errorf("organization 1's statement went to the cloud without set_config: lokadb.org there is still '2' (the direct transaction's '1' was rolled back)")
	}
	run("ROLLBACK")
}
