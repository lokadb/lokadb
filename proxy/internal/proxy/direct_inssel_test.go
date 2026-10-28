package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 2 of 8b.2: an INSERT … SELECT of one moved organization that names the key runs in a
// transaction of its database, RETURNING the keys; their refs rows follow in the cloud's open
// transaction, then the organization commits, then the cloud. The organization column the
// statement does not name is added around the client's SELECT.
func TestDirectInsertSelect(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (1001), (1002))") {
			return [][]string{{"0"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"1001"}, {"1002"}}, "", true
		}
		return nil, "", false
	}
	sql := "INSERT INTO document_versions (id, document_id, content) SELECT id + 1000, document_id, content FROM document_versions WHERE organization_id = 1"
	sendAll(t, d.fe, &pgproto3.Query{String: sql})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("client got %v (%q); cloud %q, org1 %q", types, code, d.cloud.log(), d.org1.log())
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN",
		"INSERT INTO document_versions (id, document_id, content, organization_id) SELECT lokadb_s.*, '1' FROM (SELECT id + 1000, document_id, content FROM document_versions WHERE organization_id = 1) lokadb_s RETURNING id",
		"COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "'INSERT'",
		"INSERT INTO \"document_versions__refs\" (id, tenant) SELECT id, 1 FROM lokadb_v ON CONFLICT (id) DO NOTHING", "COMMIT") || ranAny(cloud, "ROLLBACK") || ranAny(cloud, "lokadb_k") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// A key of the INSERT … SELECT that is another organization's: both sides roll back, the
// client gets 23505, as the parent's refs would refuse it through the FDW.
func TestDirectInsertSelectOtherOrg(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (1001))") {
			return [][]string{{"1"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"1001"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO document_versions (id, document_id, content, organization_id) SELECT 1001, document_id, content, 1 FROM document_versions WHERE organization_id = 1 LIMIT 1"); code != "23505" {
		t.Fatalf("client got %q; cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); !inOrder(org, "BEGIN", "INSERT", "ROLLBACK") || ranAny(org, "COMMIT") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "WITH lokadb_v(id)", "ROLLBACK") || ranAny(cloud, "COMMIT") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// The INSERT … SELECT forms that stay the cloud's: no key named (its rows are not counted
// before it runs), ON CONFLICT.
func TestDirectInsertSelectNot(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO document_versions (document_id, content) SELECT document_id, content FROM document_versions WHERE organization_id = 1",
		"INSERT INTO document_versions (id, document_id, content) SELECT id + 1000, document_id, content FROM document_versions WHERE organization_id = 1 ON CONFLICT (id) DO NOTHING",
	} {
		d := directSession(t, false, keyed)
		d.query(t, sql)
		if len(d.org1.log()) > 0 || ranAny(d.cloud.log(), "pg_advisory_xact_lock_shared") {
			t.Errorf("%s: went the direct path: org1 %q", sql, d.org1.log())
		}
	}
	m := directMeta()
	keyed(m)
	is := "INSERT INTO document_versions (id, document_id, content) SELECT id + 1000, document_id, content FROM document_versions WHERE organization_id = 1"
	if !DirectTakes(is, m, false) || !DirectTxTakes(is, m, false) {
		t.Errorf("DirectTakes %v, DirectTxTakes %v (in a transaction too: step 4)", DirectTakes(is, m, false), DirectTxTakes(is, m, false))
	}
}

// Django 5.2's bulk_create, INSERT … SELECT * FROM unnest(…) without its key: as many keys as
// the longest array come from the cloud first, and go in as one more array; a child table's
// organization column as a constant after the unnest's columns.
func TestDirectInsertUnnest(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT nextval('documents__id_seq'::regclass) AS id FROM generate_series(1, 2))") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (organization_id, title) SELECT * FROM unnest(ARRAY[1, 1], ARRAY['x', 'y'])"); code != "" {
		t.Fatalf("error %q; cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
	if org := d.org1.log(); len(org) != 1 || !strings.Contains(org[0], "INSERT INTO documents (organization_id, title, id) SELECT * FROM unnest(ARRAY[1, 1], ARRAY['x', 'y'], ARRAY[41, 42]::bigint[])") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "BEGIN", "lokadb_k", "COMMIT") || ranAny(cloud, "ROLLBACK") {
		t.Fatalf("the cloud ran %q", cloud)
	}
	// a child row: its organization from the parent's key, added as a constant
	d = directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "nextval('document_versions__id_seq'") && strings.Contains(q, "generate_series(1, 3)") {
			return [][]string{{"7"}, {"8"}, {"9"}}, "", true
		}
		return nil, "", false
	}
	d.s.orgCache["documents__refs"] = map[string]string{"5": "1"}
	if code := d.query(t, "INSERT INTO document_versions (document_id, content) SELECT * FROM unnest(ARRAY[5, 5, 5], ARRAY['a', 'b'])"); code != "" {
		t.Fatalf("child: error %q; cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); len(org) != 1 || !strings.Contains(org[0], "(document_id, content, id, organization_id) SELECT *, '1' FROM unnest(ARRAY[5, 5, 5], ARRAY['a', 'b'], ARRAY[7, 8, 9]::bigint[])") {
		t.Fatalf("child: organization's database ran %q", org)
	}
}

// unnest forms the cloud keeps: a column list for its result, WITH ORDINALITY, other targets
// than *, LIMIT.
func TestDirectInsertUnnestNot(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO documents (organization_id, title) SELECT * FROM unnest(ARRAY[1], ARRAY['x']) AS u(o, t)",
		"INSERT INTO documents (organization_id, title) SELECT o, t FROM unnest(ARRAY[1], ARRAY['x']) AS u(o, t)",
		"INSERT INTO documents (organization_id, title) SELECT * FROM unnest(ARRAY[1], ARRAY['x']) LIMIT 1",
	} {
		d := directSession(t, false, keyed)
		d.query(t, sql)
		if len(d.org1.log()) > 0 || ranAny(d.cloud.log(), "pg_advisory_xact_lock_shared") {
			t.Errorf("%s: went the direct path: org1 %q", sql, d.org1.log())
		}
	}
	m := directMeta()
	keyed(m)
	un := "INSERT INTO documents (organization_id, title) SELECT * FROM unnest(ARRAY[1], ARRAY['x'])"
	if !DirectTakes(un, m, false) || !DirectTxTakes(un, m, false) {
		t.Errorf("DirectTakes %v, DirectTxTakes %v (in a transaction too: step 4)", DirectTakes(un, m, false), DirectTxTakes(un, m, false))
	}
}
