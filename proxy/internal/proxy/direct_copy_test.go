package proxy

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// copyClient sends the COPY, then, once the CopyInResponse came, its stream and end; it returns
// what the client got after the stream: the message types and the error code.
func (d *directTest) copyClient(t *testing.T, sql string, data []string, end pgproto3.FrontendMessage) ([]string, string, string) {
	t.Helper()
	sendAll(t, d.fe, &pgproto3.Query{String: sql})
	m, err := d.fe.Receive()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.(*pgproto3.CopyInResponse); !ok {
		types := []string{strings.TrimPrefix(strings.TrimPrefix(typeName(m), "*"), "pgproto3.")}
		for {
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				return types, "", ""
			}
			if m, err = d.fe.Receive(); err != nil {
				t.Fatal(err)
			}
			types = append(types, strings.TrimPrefix(typeName(m), "*pgproto3."))
		}
	}
	var msgs []pgproto3.FrontendMessage
	for _, line := range data {
		msgs = append(msgs, &pgproto3.CopyData{Data: []byte(line)})
	}
	sendAll(t, d.fe, append(msgs, end)...)
	var types []string
	code, tag := "", ""
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		types = append(types, strings.TrimPrefix(typeName(m), "*pgproto3."))
		switch m := m.(type) {
		case *pgproto3.ErrorResponse:
			code = m.Code
		case *pgproto3.CommandComplete:
			tag = string(m.CommandTag)
		case *pgproto3.ReadyForQuery:
			return types, code, tag
		}
	}
}

// Block 2 of 8b.2, form 2: a COPY FROM STDIN of the organization the application named goes to
// its database: the stream into a stage, then the cloud gives one key per row with its refs row,
// the rows go in with them, the organization commits, then the cloud; the client gets COPY n.
func TestDirectCopy(t *testing.T) {
	d := directSession(t, false, keyed)
	d.s.tenant.session = "1"
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT nextval('documents__id_seq'::regclass) AS id FROM generate_series(1, 2))") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return nil, "", true
		}
		return nil, "", false
	}
	types, code, tag := d.copyClient(t, "COPY documents (organization_id, title) FROM STDIN", []string{"1\tx\n", "1\ty\n"}, &pgproto3.CopyDone{})
	if code != "" || tag != "COPY 2" || strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("client got %v %q %q; cloud %q, org1 %q", types, code, tag, d.cloud.log(), d.org1.log())
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN", `DROP TABLE IF EXISTS pg_temp.__lokadb_dcopy; CREATE TEMP TABLE __lokadb_dcopy ON COMMIT DROP AS SELECT "organization_id", "title" FROM "documents" WITH NO DATA; ALTER TABLE __lokadb_dcopy ADD COLUMN __lokadb_n bigint GENERATED ALWAYS AS IDENTITY`,
		"COPY __lokadb_dcopy(organization_id, title) FROM STDIN",
		`INSERT INTO "documents" ("organization_id", "title", "id") SELECT s."organization_id", s."title", k.id FROM __lokadb_dcopy s JOIN unnest(ARRAY[41, 42]::bigint[]) WITH ORDINALITY AS k(id, n) ON k.n = s.__lokadb_n`,
		"COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "'INSERT'", "lokadb_k", "COMMIT") || ranAny(cloud, "ROLLBACK") {
		t.Fatalf("the cloud ran %q", cloud)
	}
	if ranAny(d.cloud.log(), "COPY") {
		t.Fatalf("the COPY reached the cloud: %q", d.cloud.log())
	}
}

// Keys in the stream: the rows go in RETURNING them, their refs rows follow; a key of another
// organization refuses the whole COPY (23505), nothing commits on either side.
func TestDirectCopyKeyOfOther(t *testing.T) {
	d := directSession(t, false, keyed)
	d.s.tenant.session = "1"
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (7), (8))") {
			return [][]string{{"1"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, `ORDER BY s.__lokadb_n RETURNING "id"`) {
			return [][]string{{"7"}, {"8"}}, "", true
		}
		return nil, "", false
	}
	_, code, _ := d.copyClient(t, "COPY documents (id, organization_id, title) FROM STDIN", []string{"7\t1\tx\n", "8\t1\ty\n"}, &pgproto3.CopyDone{})
	if code != "23505" {
		t.Fatalf("client got %q; cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); !inOrder(org, "COPY __lokadb_dcopy", "INSERT", "ROLLBACK") || ranExactly(org, "COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "WITH lokadb_v(id)", "ROLLBACK") || ranAny(cloud, "COMMIT") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// The client's CopyFail: the organization's COPY fails, both sides roll back, the client gets
// the organization's error.
func TestDirectCopyFail(t *testing.T) {
	d := directSession(t, false, keyed)
	d.s.tenant.session = "1"
	_, code, _ := d.copyClient(t, "COPY documents (organization_id, title) FROM STDIN", []string{"1\tx\n"}, &pgproto3.CopyFail{Message: "client gave up"})
	if code != "57014" {
		t.Fatalf("client got %q", code)
	}
	if org := d.org1.log(); !inOrder(org, "COPY __lokadb_dcopy", "ROLLBACK") || ranAny(org, "INSERT") || ranExactly(org, "COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "BEGIN", "ROLLBACK") || ranAny(cloud, "COMMIT") || ranAny(cloud, "lokadb_k") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// Without the organization named, or every one (lokadb.tenant = '*'), the COPY goes the cloud's
// way, as do a COPY from a file and one of a table with a row trigger of the cloud.
func TestDirectCopyNot(t *testing.T) {
	triggers := func(m *Meta) { m.RowTriggers = map[string]int{"documents": trigInsert} }
	for _, c := range []struct {
		tenant string
		edit   func(*Meta)
	}{{"", nil}, {TenantAll, nil}, {"1", triggers}} {
		edits := []func(*Meta){keyed}
		if c.edit != nil {
			edits = append(edits, c.edit)
		}
		d := directSession(t, false, edits...)
		d.s.tenant.session = c.tenant
		sendAll(t, d.fe, &pgproto3.Query{String: "COPY documents (organization_id, title) FROM STDIN"})
		if _, err := d.fe.Receive(); err != nil {
			t.Fatal(err)
		}
		if len(d.org1.log()) > 0 || ranAny(d.cloud.log(), "lokadb.org_lock(1)") {
			t.Errorf("tenant %q: went the direct path: org1 %q, cloud %q", c.tenant, d.org1.log(), d.cloud.log())
		}
	}
	m := directMeta()
	keyed(m)
	if t2, _, _, _, _ := copyShape("COPY documents FROM '/tmp/x'", m); t2 != nil {
		t.Error("a COPY from a file was taken")
	}
	m.Tables["documents"].Key = `["organization_id","id"]` // a composite key (block 1): its id neither named nor with a default
	if t2, _, _, _, _ := copyShape("COPY documents (organization_id, title) FROM STDIN", m); t2 != nil {
		t.Error("a COPY of a table with a composite key without its id's default was taken")
	}
	m.Tables["documents"].KeyDefaults = map[string]string{"id": "nextval('documents_id_seq'::regclass)"} // stage 2: taken
	if t2, _, _, _, _ := copyShape("COPY documents (organization_id, title) FROM STDIN", m); t2 == nil {
		t.Error("a COPY of a table with a composite key whose id has a default was not taken")
	}
}

func typeName(m pgproto3.BackendMessage) string { return fmt.Sprintf("%T", m) }

// ranExactly: one of log is q itself (the stage's CREATE says ON COMMIT DROP).
func ranExactly(log []string, q string) bool {
	for _, l := range log {
		if l == q {
			return true
		}
	}
	return false
}
