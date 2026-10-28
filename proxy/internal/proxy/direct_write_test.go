package proxy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// keyed: documents' key comes from a sequence of the cloud, as enable leaves it.
func keyed(m *Meta) {
	for _, t := range m.Tables {
		t.Defaults = map[string]bool{"id": true}
		t.KeyDefault = "nextval('" + t.Name + "__id_seq'::regclass)"
	}
}

// order: the positions of subs in log, in that order; -1 for one not found after the previous.
func order(log []string, subs ...string) []int {
	var at []int
	i := 0
	for _, sub := range subs {
		found := -1
		for ; i < len(log); i++ {
			if strings.Contains(log[i], sub) {
				found = i
				i++
				break
			}
		}
		at = append(at, found)
		if found < 0 {
			break
		}
	}
	return at
}

func inOrder(log []string, subs ...string) bool {
	at := order(log, subs...)
	return len(at) == len(subs) && at[len(at)-1] >= 0
}

// answerTypes reads the client's answer up to ReadyForQuery: the message types and the error code.
func (d *directTest) answerTypes(t *testing.T) ([]string, string) {
	t.Helper()
	var types []string
	code := ""
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, strings.TrimPrefix(fmt.Sprintf("%T", m), "*pgproto3."))
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			code = m.Code
		case *pgproto3.ReadyForQuery:
			return types, code
		}
	}
}

// Stage 8b: an INSERT without its key goes to the organization's database with the keys the
// cloud's sequence gives and their refs rows written in the cloud's transaction first, which
// commits after the organization's INSERT (coordinator's decision of 25.10).
func TestDirectInsert(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT nextval('documents__id_seq'::regclass) AS id FROM generate_series(1, 2))") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) VALUES (1, 'x'), (1, 'y')"); code != "" {
		t.Fatalf("error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	org := d.org1.log()
	if len(org) != 1 || !strings.Contains(org[0], "(organization_id, title, id) VALUES (1, 'x', '41'), (1, 'y', '42')") {
		t.Fatalf("organization's database ran %q", org)
	}
	cloud := d.cloud.log()
	if !inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "has_table_privilege(to_regclass('\"documents\"'), 'INSERT')",
		"INSERT INTO \"documents__refs\" (id, tenant) SELECT id, 1 FROM lokadb_k", "COMMIT") || ranAny(cloud, "ROLLBACK") || ranAny(cloud, "'x'") {
		t.Fatalf("the cloud ran %q", cloud)
	}
	// a child row: the organization column the proxy adds
	d = directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "nextval('document_versions__id_seq'") {
			return [][]string{{"7"}}, "", true
		}
		return nil, "", false
	}
	d.s.orgCache["documents__refs"] = map[string]string{"5": "1"}
	if code := d.query(t, "INSERT INTO document_versions (document_id, content) VALUES (5, 'c')"); code != "" {
		t.Fatalf("child: error %q, cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); len(org) != 1 || !strings.Contains(org[0], "(document_id, content, id, organization_id) VALUES (5, 'c', '7', '1')") {
		t.Fatalf("child: organization's database ran %q", org)
	}
}

// INSERT … RETURNING id (Rails, Django at every insert) by a simple query: the client gets the
// organization's rows, the keys the proxy put in, with their description.
func TestDirectInsertReturning(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, "RETURNING id") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "INSERT INTO documents (organization_id, title) VALUES (1, 'x') RETURNING id"})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "RowDescription,DataRow,CommandComplete,ReadyForQuery" {
		t.Fatalf("client got %v (%q)", types, code)
	}
}

// The cloud's COMMIT fails after the organization's: the client gets 40003 (its rows are
// written, their refs are not: refs-diff finds them).
func TestDirectCloudCommitFails(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		switch {
		case strings.Contains(q, "WITH lokadb_k AS"):
			return [][]string{{"41"}}, "", true
		case q == "COMMIT":
			return nil, "deferred constraint failed", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) VALUES (1, 'x')"); code != "40003" {
		t.Fatalf("client got %q, want 40003", code)
	}
	if org := d.org1.log(); len(org) != 1 || !strings.HasPrefix(org[0], "INSERT") {
		t.Fatalf("organization's database ran %q", org)
	}
}

// A failed INSERT in the organization's database (a unique violation) rolls the cloud back:
// no refs row of a key that has no row.
func TestDirectInsertFails(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return nil, "duplicate key value violates unique constraint", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) VALUES (1, 'x')"); code != "23505" {
		t.Fatalf("client got %q, want the organization's 23505", code)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "INSERT INTO \"documents__refs\"", "ROLLBACK") || ranAny(cloud, "COMMIT") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// A key the client gives: its refs row is written unless the key is another organization's
// (then the cloud's way answers, as through the FDW); one of this organization already (a crash
// between the two commits) is left to the organization's primary key.
func TestDirectInsertKeyGiven(t *testing.T) {
	for _, c := range []struct {
		others string
		direct bool
	}{{"0", true}, {"1", false}} {
		d := directSession(t, false, keyed)
		d.cloud.answer = func(q string) ([][]string, string, bool) {
			if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (7))") && strings.Contains(q, "ON CONFLICT (id) DO NOTHING") {
				return [][]string{{c.others}}, "", true
			}
			return nil, "", false
		}
		if code := d.query(t, "INSERT INTO documents (id, organization_id, title) VALUES (7, 1, 'x')"); code != "" {
			t.Fatalf("%s: error %q", c.others, code)
		}
		org, cloud := d.org1.log(), d.cloud.log()
		switch {
		case c.direct && (len(org) != 1 || !strings.Contains(org[0], "(id, organization_id, title) VALUES (7, 1, 'x')") || !inOrder(cloud, "WITH lokadb_v(id)", "COMMIT")):
			t.Errorf("key of this organization: org1 %q, cloud %q", org, cloud)
		case !c.direct && (len(org) != 0 || !inOrder(cloud, "WITH lokadb_v(id)", "ROLLBACK", "VALUES (7, 1, 'x')")):
			t.Errorf("key of another organization: org1 %q, cloud %q", org, cloud)
		}
	}
}

// A DELETE runs in a transaction of the organization's database, RETURNING the keys; their
// refs rows are deleted in the cloud's transaction, then the organization commits, then the
// cloud. The client, which sent no RETURNING, gets none of the keys.
func TestDirectDelete(t *testing.T) {
	d := directSession(t, false, keyed)
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") && strings.HasSuffix(q, "RETURNING id") {
			return [][]string{{"5"}, {"6"}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "DELETE FROM documents WHERE organization_id = 1 AND slug = 'a'"})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("client got %v (%q)", types, code)
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN", "DELETE FROM documents WHERE organization_id = 1 AND slug = 'a' RETURNING id", "COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared", "'DELETE'", "DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (5, 6)", "COMMIT") || ranAny(cloud, "ROLLBACK") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// A DELETE the cloud refuses (an FK of a cloud table onto __refs, RESTRICT) keeps the
// organization's rows: both sides roll back, the client gets the cloud's error.
func TestDirectDeleteRestricted(t *testing.T) {
	d := directSession(t, false, keyed)
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") {
			return [][]string{{"5"}}, "", true
		}
		return nil, "", false
	}
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "DELETE FROM \"documents__refs\"") {
			return nil, "update or delete on table \"documents__refs\" violates foreign key constraint", true
		}
		return nil, "", false
	}
	if code := d.query(t, "DELETE FROM documents WHERE organization_id = 1"); code != "XX000" {
		t.Fatalf("client got %q, want the cloud's error", code)
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN", "RETURNING id", "ROLLBACK") || ranAny(org, "COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "DELETE FROM \"documents__refs\"", "ROLLBACK") || ranAny(cloud, "COMMIT") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// The extended protocol (pgx, PDO, JDBC once the statement is prepared): the organization's
// database Parses the rewritten statement unnamed and the client's Bind goes to it; a
// DELETE's keys never reach the client, whose Describe of the portal gets NoData.
func TestDirectWriteBatch(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	if code := d.prepared(t, "ins", "INSERT INTO documents (organization_id, title) VALUES ($1, $2)", "1", "x"); code != "" {
		t.Fatalf("insert: %q, cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); len(org) != 1 || org[0] != "INSERT INTO documents (organization_id, title, id) VALUES (1, x, '41')" {
		t.Fatalf("insert: organization's database ran %q", org)
	}
	d = directSession(t, false, keyed)
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") && strings.HasSuffix(q, "RETURNING id") {
			return [][]string{{"5"}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Parse{Name: "del", Query: "DELETE FROM documents WHERE organization_id = $1 AND slug = $2"}, &pgproto3.Sync{})
	if code := errorCode(t, d.fe); code != "" {
		t.Fatal(code)
	}
	sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "del", Parameters: [][]byte{[]byte("1"), []byte("a")}, ResultFormatCodes: []int16{1}},
		&pgproto3.Describe{ObjectType: 'P'}, &pgproto3.Execute{}, &pgproto3.Sync{})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "BindComplete,NoData,CommandComplete,ReadyForQuery" {
		t.Fatalf("delete: client got %v (%q)", types, code)
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN", "DELETE FROM documents WHERE organization_id = 1 AND slug = a RETURNING id", "COMMIT") {
		t.Fatalf("delete: organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (5)", "COMMIT") {
		t.Fatalf("delete: the cloud ran %q", cloud)
	}
}

// What stays on the cloud's way: a table with a row trigger of the cloud (the application's,
// lokadb_fk_*: it would not fire in the organization's database) for INSERT, DELETE and UPDATE;
// an INSERT that leaves out a column with a default of the cloud's, or writes DEFAULT; a table
// with an identity or generated column.
func TestDirectWriteNot(t *testing.T) {
	triggers := func(m *Meta) { m.RowTriggers = map[string]int{"documents": trigInsert | trigDelete | trigUpdate} }
	slugDefault := func(m *Meta) { m.Tables["documents"].Defaults["slug"] = true }
	generated := func(m *Meta) { m.Tables["documents"].Generated = true }
	for _, c := range []struct {
		sql  string
		edit func(*Meta)
	}{
		{"INSERT INTO documents (organization_id, title) VALUES (1, 'x')", triggers},
		{"DELETE FROM documents WHERE organization_id = 1", triggers},
		{"DELETE FROM documents WHERE organization_id = 1", func(m *Meta) { m.RefsFromLokadb = map[string]bool{"documents": true} }}, // a LokaDB child's FK onto documents__refs (DP18)
		{"UPDATE documents SET title = 'y' WHERE organization_id = 1", triggers},
		{"INSERT INTO documents (organization_id, title) VALUES (1, 'x')", slugDefault},
		{"INSERT INTO documents (organization_id, title, slug) VALUES (1, 'x', DEFAULT)", slugDefault},
		{"INSERT INTO documents (organization_id, title) VALUES (1, 'x')", generated},
		{"INSERT INTO documents (organization_id, title) SELECT 1, 'x'", nil},
	} {
		edits := []func(*Meta){keyed}
		if c.edit != nil {
			edits = append(edits, c.edit)
		}
		d := directSession(t, false, edits...)
		d.query(t, c.sql)
		if len(d.org1.log()) > 0 || ranAny(d.cloud.log(), "pg_advisory_xact_lock_shared") {
			t.Errorf("%s: went the direct path: org1 %q", c.sql, d.org1.log())
		}
	}
}

// uuidKeyed: documents keyed by a uuid the cloud's DEFAULT makes (block 17).
func uuidKeyed(m *Meta) {
	d := m.Tables["documents"]
	d.ColType["id"] = "uuid"
	d.Defaults = map[string]bool{"id": true}
	d.KeyDefault = "gen_random_uuid()"
	if err := loadKeyKinds(nil, m); err != nil {
		panic(err)
	}
}

// Block 17: a direct INSERT of a table keyed by a uuid takes its keys from the key's DEFAULT
// computed in the cloud (not only from a sequence); a key the client gives goes to the refs
// table canonical and cast to uuid (VALUES of plain literals would be text); a DELETE's keys
// as well. Before, such a table took the cloud's way: its key's default was no nextval.
func TestDirectWriteUUID(t *testing.T) {
	const k1, k2 = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "6f1d1c3e-4a7b-4c2d-9e8f-0a1b2c3d4e5f"
	d := directSession(t, false, uuidKeyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT gen_random_uuid() AS id FROM generate_series(1, 2))") {
			return [][]string{{k1}, {k2}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) VALUES (1, 'x'), (1, 'y')"); code != "" {
		t.Fatalf("no key: error %q, cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); len(org) != 1 || !strings.Contains(org[0], "(organization_id, title, id) VALUES (1, 'x', '"+k1+"'), (1, 'y', '"+k2+"')") {
		t.Fatalf("no key: organization's database ran %q", org)
	}
	// a key given, in upper case
	d = directSession(t, false, uuidKeyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES ('"+k1+"'::uuid))") {
			return [][]string{{"0"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (id, organization_id, title) VALUES ('"+strings.ToUpper(k1)+"', 1, 'x')"); code != "" {
		t.Fatalf("key given: error %q", code)
	}
	if org, cloud := d.org1.log(), d.cloud.log(); len(org) != 1 || !inOrder(cloud, "WITH lokadb_v(id)", "COMMIT") {
		t.Fatalf("key given: org1 %q, cloud %q", org, cloud)
	}
	// a DELETE: the keys the organization returns, cast to uuid
	d = directSession(t, false, uuidKeyed)
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") && strings.HasSuffix(q, "RETURNING id") {
			return [][]string{{k1}, {k2}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "DELETE FROM documents WHERE organization_id = 1 AND slug = 'a'"})
	if _, code := d.answerTypes(t); code != "" {
		t.Fatalf("delete: error %q", code)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN ('"+k1+"'::uuid, '"+k2+"'::uuid)", "COMMIT") {
		t.Fatalf("delete: the cloud ran %q", cloud)
	}
}

// A row trigger on DELETE alone (lokadb_fkcascade_* of a cascade at the organization, block 5)
// keeps the DELETE on the cloud's way, not the INSERT and the UPDATE: a trigger counts only
// for the events it fires on.
func TestDirectWriteTriggerEvent(t *testing.T) {
	onDelete := func(m *Meta) { m.RowTriggers = map[string]int{"documents": trigDelete} }
	for _, c := range []struct {
		sql    string
		direct bool
	}{
		{"INSERT INTO documents (organization_id, title) VALUES (1, 'x')", true},
		{"UPDATE documents SET title = 'y' WHERE organization_id = 1", true},
		{"DELETE FROM documents WHERE organization_id = 1", false},
	} {
		d := directSession(t, false, keyed, onDelete)
		d.cloud.answer = func(q string) ([][]string, string, bool) {
			if strings.Contains(q, "WITH lokadb_k AS") {
				return [][]string{{"41"}}, "", true
			}
			return nil, "", false
		}
		d.query(t, c.sql)
		if went := len(d.org1.log()) > 0; went != c.direct {
			t.Errorf("%s: direct %v, want %v (org1 %q)", c.sql, went, c.direct, d.org1.log())
		}
	}
}

// composite: K8's orders (account_id, id), its id from a sequence of the cloud, and parcels
// (tracking_no, piece) next to documents (tasks/composite-direct.md).
func composite(m *Meta) {
	c := compositeMeta()
	for _, t := range []string{"orders", "parcels"} {
		m.Tables[t], m.Lookups[t] = c.Tables[t], c.Lookups[t]
		m.Foreign[t] = map[string]string{"1": t + "__org1", "2": t + "__org2"}
		m.Direct[t+"__org1"], m.Direct[t+"__org2"] = true, true
	}
	m.Tables["orders"].Defaults = map[string]bool{"id": true}
	m.Tables["orders"].KeyDefaults = map[string]string{"id": "nextval('orders_id_seq'::regclass)"}
	if err := loadKeyKinds(nil, m); err != nil {
		panic(err)
	}
}
