package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Stage 9b: organization 1 on profile direct. A named read and a write go to its database; its
// first transaction is direct without learning; a read that does not name it, a join with a
// cloud table without a copy and a form the direct path does not take are refused with their
// reason, never sent the cloud's way.
func TestDirectProfile(t *testing.T) {
	d := directSession(t, false, func(m *Meta) {
		m.Profiles["1"] = ProfileDirect
		m.RowTriggers = map[string]int{"document_versions": trigInsert | trigDelete | trigUpdate} // a table the direct path does not take
	})
	d.s.srv.cfg.Tenant = sessionTenant() // SET lokadb.tenant for the session too
	refused := func(q, want string) {
		t.Helper()
		n := len(d.cloud.log())
		sendAll(t, d.fe, &pgproto3.Query{String: q})
		code, msg := errorCodeMsg(t, d.fe)
		if code != "0A000" || !strings.Contains(msg, want) {
			t.Fatalf("%s: %q %q", q, code, msg)
		}
		for _, c := range d.cloud.log()[n:] {
			if strings.Contains(c, "documents") && !strings.Contains(c, "lokadb.orgs") {
				t.Fatalf("%s went the cloud's way: %q", q, c)
			}
		}
	}
	refused("SELECT title FROM documents WHERE organization_id = 1", "must name the organization")
	// a write may be guessed: its organization's CHECK and FKs check it
	if code := d.query(t, "UPDATE documents SET title = 'w' WHERE organization_id = 1"); code != "" || !d.direct("UPDATE documents SET title = 'w'") {
		t.Fatalf("an UPDATE: %q, org1 %q", code, d.org1.log())
	}
	// a transaction of a form never seen: direct at once (nothing to learn on)
	for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1", "SELECT slug FROM documents"} {
		if code := d.query(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	if !d.direct("SELECT slug FROM documents") {
		t.Fatalf("the first transaction did not go direct: org1 %q", d.org1.log())
	}
	if code := d.query(t, "COMMIT"); code != "" {
		t.Fatalf("COMMIT: %q", code)
	}
	for _, q := range []string{"BEGIN", "SET LOCAL lokadb.tenant = 1"} {
		d.query(t, q)
	}
	d.query(t, "ROLLBACK")
	d.query(t, "SET lokadb.tenant = '1'")
	refused("SELECT d.title FROM documents d JOIN projects p ON p.id = d.project_id", "no copy in its database")
	refused("UPDATE document_versions SET content = 'x' WHERE organization_id = 1", "does not take this statement")
	d.query(t, "SET lokadb.tenant = '*'")
	refused("SELECT title FROM documents WHERE organization_id = 1", "across organizations")
}

// errorCodeMsg reads up to ReadyForQuery: the first error's code and message.
func errorCodeMsg(t *testing.T, fe *pgproto3.Frontend) (string, string) {
	t.Helper()
	code, msg := "", ""
	for {
		m, err := fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			if code == "" {
				code, msg = m.Code, m.Message
			}
		case *pgproto3.ReadyForQuery:
			return code, msg
		}
	}
}

// Block 9's review of 9b: inside a cloud transaction (a form not learnt for another reason)
// the refusal is the server's: the transaction is aborted, 25P02 up to ROLLBACK.
func TestDirectProfileInCloudTx(t *testing.T) {
	d := directSession(t, false, func(m *Meta) { m.Profiles["1"] = ProfileDirect })
	for _, q := range []string{"BEGIN", "SELECT count(*) FROM projects"} { // the cloud's transaction
		if code := d.query(t, q); code != "" {
			t.Fatalf("%s: %q", q, code)
		}
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "SELECT title FROM documents WHERE organization_id = 1"})
	if _, code, st := d.answerAll(t); code != "0A000" || st != 'E' {
		t.Fatalf("a read of organization 1 without its name in the cloud's transaction: %q %q; cloud %q; org1 %q", code, st, d.cloud.log(), d.org1.log())
	}
	if _, code, _ := d.exchange(t, "SELECT 1"); code != "25P02" {
		t.Fatalf("after the refusal: %q", code)
	}
	if _, code, st := d.exchange(t, "ROLLBACK"); code != "" || st != 'I' {
		t.Fatalf("ROLLBACK: %q %q", code, st)
	}
}

// FOR UPDATE OF d names an alias of the FROM list, not a table: a statement on LokaDB tables
// alone goes to the organization's database on fdw+direct and on direct; with the alias of a
// cloud table it is what a JOIN with that table is (the cloud's way, refused on direct).
func TestDirectForUpdateOfAlias(t *testing.T) {
	own := "SELECT title FROM documents d WHERE d.organization_id = 1 FOR UPDATE OF d"
	cloud := "SELECT d.title FROM documents d JOIN projects p ON p.id = d.project_id WHERE d.organization_id = 1 FOR UPDATE OF p"

	d := directSession(t, false) // organization 1 on fdw+direct
	if code := d.query(t, own); code != "" || !d.direct(own) {
		t.Fatalf("fdw+direct, FOR UPDATE OF a LokaDB table's alias: %q, org1 %q, cloud %q", code, d.org1.log(), d.cloud.log())
	}
	// in the cloud, the join with the organization's foreign partition in the table's place
	// (Analysis.Target of a join with cloud tables)
	if code := d.query(t, cloud); code != "" || !d.clouded(`FROM "documents__org1" d JOIN projects p ON p.id = d.project_id WHERE d.organization_id = 1 AND d.organization_id = 1 FOR UPDATE OF p`) {
		t.Fatalf("fdw+direct, FOR UPDATE OF a cloud table's alias: %q, org1 %q", code, d.org1.log())
	}

	d = directSession(t, false, func(m *Meta) { m.Profiles["1"] = ProfileDirect })
	d.s.srv.cfg.Tenant = sessionTenant()
	if code := d.query(t, "SET lokadb.tenant = '1'"); code != "" {
		t.Fatalf("SET: %q", code)
	}
	if code := d.query(t, own); code != "" || !d.direct(own) {
		t.Fatalf("direct, FOR UPDATE OF a LokaDB table's alias: %q, org1 %q, cloud %q", code, d.org1.log(), d.cloud.log())
	}
	sendAll(t, d.fe, &pgproto3.Query{String: cloud})
	if code, msg := errorCodeMsg(t, d.fe); code != "0A000" || !strings.Contains(msg, "joins a cloud table (projects)") {
		t.Fatalf("direct, FOR UPDATE OF a cloud table's alias: %q %q", code, msg)
	}
}
