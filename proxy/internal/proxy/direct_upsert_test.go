package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 2 of 8b.2: an upsert of one moved organization runs natively in its database.

// slugUnique: documents has a unique index on slug in the cloud's DEFAULT partition (and so
// at the organization, move-org).
func slugUnique(m *Meta) { m.Tables["documents"].Uniques = map[string]bool{"slug": true} }

// No key in the statement: the cloud gives one per row and writes their refs rows; the row the
// organization updated instead (its key is its own) leaves its given key's refs row before the
// cloud commits. The client, which sent no RETURNING, gets the INSERT's tag alone.
func TestDirectUpsert(t *testing.T) {
	d := directSession(t, false, keyed, slugUnique)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS (SELECT nextval('documents__id_seq'::regclass) AS id FROM generate_series(1, 2))") {
			return [][]string{{"41"}, {"42"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"41", "t"}, {"17", "f"}}, "", true // the second slug was there: row 17 updated
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x'), (1, 'b', 'y') ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title"})
	types, code := d.answerTypes(t)
	if code != "" || strings.Join(types, ",") != "CommandComplete,ReadyForQuery" {
		t.Fatalf("client got %v (%q); cloud %q, org1 %q", types, code, d.cloud.log(), d.org1.log())
	}
	org := d.org1.log()
	if len(org) != 1 || !strings.Contains(org[0], "(organization_id, slug, title, id) VALUES (1, 'a', 'x', '41'), (1, 'b', 'y', '42') ON CONFLICT (slug) DO UPDATE SET title = excluded.title RETURNING id, xmax::text = '0'") {
		t.Fatalf("organization's database ran %q", org)
	}
	cloud := d.cloud.log()
	if !inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared(lokadb.org_lock(1))", "INSERT INTO \"documents__refs\" (id, tenant) SELECT id, 1 FROM lokadb_k",
		"DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (42)", "COMMIT") || ranAny(cloud, "ROLLBACK") || ranAny(cloud, "lokadb_d WHERE id IN (41") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// The client's RETURNING: it gets its own columns, not the two the proxy added.
func TestDirectUpsertReturning(t *testing.T) {
	d := directSession(t, false, keyed, slugUnique)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, "RETURNING id, id, xmax::text = '0'") {
			return [][]string{{"41", "41", "t"}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "INSERT INTO documents (organization_id, slug) VALUES (1, 'a') ON CONFLICT (slug) DO NOTHING RETURNING id"})
	var fields, values int
	for {
		m, err := d.fe.Receive()
		if err != nil {
			t.Fatal(err)
		}
		switch m := m.(type) {
		case *pgproto3.RowDescription:
			fields = len(m.Fields)
		case *pgproto3.DataRow:
			values = len(m.Values)
			if string(m.Values[0]) != "41" {
				t.Errorf("row %q", m.Values)
			}
		case *pgproto3.ErrorResponse:
			t.Fatalf("error %s; org1 %q", m.Message, d.org1.log())
		}
		if _, ok := m.(*pgproto3.ReadyForQuery); ok {
			break
		}
	}
	if fields != 1 || values != 1 {
		t.Fatalf("client got %d fields, rows of %d values", fields, values)
	}
	if cloud := d.cloud.log(); ranAny(cloud, "lokadb_d") || !inOrder(cloud, "lokadb_k", "COMMIT") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// Keys named: the refs rows the cloud added for them (an upsert's are reported) of rows the
// organization skipped leave __refs; a key already this organization's stays.
func TestDirectUpsertKeyGiven(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (7), (8))") && strings.Contains(q, "RETURNING id) SELECT k FROM") {
			return [][]string{{"0"}, {"8"}}, "", true // 7 is this organization's row already; 8 added
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") {
			return [][]string{{"7", "f"}}, "", true // 7 updated; 8 skipped (its row conflicted with nothing: DO UPDATE WHERE)
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO documents (id, organization_id, title) VALUES (7, 1, 'x'), (8, 1, 'y') ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title WHERE documents.title <> 'keep'"); code != "" {
		t.Fatalf("error %q; cloud %q", code, d.cloud.log())
	}
	if org := d.org1.log(); len(org) != 1 || !strings.Contains(org[0], "ON CONFLICT (id) DO UPDATE SET title = excluded.title WHERE documents.title <> 'keep' RETURNING id, xmax::text = '0'") {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "WITH lokadb_v(id)", "DELETE FROM \"documents__refs\" AS lokadb_d WHERE id IN (8)", "COMMIT") || ranAny(cloud, "lokadb_d WHERE id IN (7") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// Upserts the organization's database might not run as the cloud does go the cloud's way: a
// conflict target that is no unique index of the organization's table, ON CONSTRAINT, a partial
// index's predicate, DO UPDATE of the key or the organization column, a row trigger of the
// cloud on UPDATE, an expression in the target.
func TestDirectUpsertNot(t *testing.T) {
	updTrigger := func(m *Meta) { m.RowTriggers = map[string]int{"documents": trigUpdate} }
	for _, c := range []struct {
		sql  string
		edit func(*Meta)
	}{
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (title) DO NOTHING", slugUnique},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) DO NOTHING", nil},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT ON CONSTRAINT documents_slug_key DO NOTHING", slugUnique},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) WHERE title IS NOT NULL DO NOTHING", slugUnique},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (lower(slug)) DO NOTHING", slugUnique},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) DO UPDATE SET organization_id = 2", slugUnique},
		{"INSERT INTO documents (id, organization_id, title) VALUES (7, 1, 'x') ON CONFLICT (id) DO UPDATE SET id = 8", nil},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) DO UPDATE SET title = 'y'", func(m *Meta) { slugUnique(m); updTrigger(m) }},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT DO UPDATE SET title = 'y'", slugUnique},
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

// The two added columns in binary (pgx asks one binary format for every column).
func TestUpsertSplitBinary(t *testing.T) {
	w := &directWrite{table: &Table{Key: "id", ColType: map[string]string{"id": "bigint"}}, upsert: true}
	answer := []pgproto3.BackendMessage{
		&pgproto3.DataRow{Values: [][]byte{{0, 0, 0, 0, 0, 0, 0, 41}, {1}}},
		&pgproto3.DataRow{Values: [][]byte{{0, 0, 0, 0, 0, 0, 0, 17}, {0}}},
		&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 2")},
	}
	out, inserted, err := w.split(answer, false, true, kindInt)
	if err != nil || strings.Join(inserted, ",") != "41" || len(out) != 1 {
		t.Fatalf("out %v, inserted %v, err %v", out, inserted, err)
	}
	w.table.ColType["id"] = "uuid"
	u := []byte{0x6f, 0x1c, 0x2e, 0x3d, 0x4a, 0x5b, 0x46, 0x7c, 0x8d, 0x9e, 0xaf, 0xb0, 0xc1, 0xd2, 0xe3, 0xf4}
	_, inserted, err = w.split([]pgproto3.BackendMessage{&pgproto3.DataRow{Values: [][]byte{u, {1}}}}, false, true, kindUUID)
	if err != nil || len(inserted) != 1 || inserted[0] != "6f1c2e3d-4a5b-467c-8d9e-afb0c1d2e3f4" {
		t.Fatalf("uuid: %v, %v", inserted, err)
	}
}

// An upsert inside a transaction is taken too (step 4): the transaction's form is learnt with
// it; a COPY FROM STDIN only with the organization named.
func TestDirectTxTakesUpsertCopy(t *testing.T) {
	m := directMeta()
	keyed(m)
	slugUnique(m)
	up := "INSERT INTO documents (organization_id, slug) VALUES (1, 'a') ON CONFLICT (slug) DO NOTHING"
	if !DirectTakes(up, m, false) || !DirectTxTakes(up, m, false) {
		t.Fatalf("DirectTakes %v, DirectTxTakes %v", DirectTakes(up, m, false), DirectTxTakes(up, m, false))
	}
	cp := "COPY documents (organization_id, title) FROM STDIN"
	if !DirectTxTakes(cp, m, true) || DirectTxTakes(cp, m, false) {
		t.Fatalf("COPY: named %v, not named %v", DirectTxTakes(cp, m, true), DirectTxTakes(cp, m, false))
	}
	m.SchemasUsed = true // directCopyStart does not take it there: nor does learning (review of block 10)
	if DirectTxTakes(cp, m, true) {
		t.Fatal("COPY in a database with schemas: taken")
	}
}

// A key named that __refs maps to another organization: the organization's database has no
// conflict for it, so the row would go in and the key be in two organizations. The cloud rolls
// its transaction back and the upsert goes the cloud's way, as an INSERT of 8b does.
func TestDirectUpsertOtherOrg(t *testing.T) {
	d := directSession(t, false, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id) AS (VALUES (9))") {
			return [][]string{{"1"}}, "", true // 9 is another organization's
		}
		return nil, "", false
	}
	d.query(t, "INSERT INTO documents (id, organization_id, title) VALUES (9, 1, 'x') ON CONFLICT (id) DO NOTHING")
	if org := d.org1.log(); len(org) != 0 {
		t.Fatalf("organization's database ran %q", org)
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "WITH lokadb_v(id)", "ROLLBACK") || ranAny(cloud, "lokadb_d") {
		t.Fatalf("the cloud ran %q", cloud)
	}
}

// The gate of a DO UPDATE upsert checks the client's UPDATE of its SET columns (the
// organization's database runs it as the direct role, which may write everything); DO NOTHING
// needs INSERT alone.
func TestDirectUpsertGate(t *testing.T) {
	m := directMeta()
	keyed(m)
	slugUnique(m)
	s := &session{meta: m}
	for _, c := range []struct {
		sql    string
		update bool
	}{
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) DO UPDATE SET title = EXCLUDED.title", true},
		{"INSERT INTO documents (organization_id, slug, title) VALUES (1, 'a', 'x') ON CONFLICT (slug) DO NOTHING", false},
	} {
		di := directShape(c.sql, analyze(c.sql, m), m, false, defaultPath)
		if !di.ok || di.write == nil {
			t.Fatalf("%s: not taken", c.sql)
		}
		q := s.gateCheck(di, "1").sql
		got := strings.Contains(q, "has_column_privilege(to_regclass('\"documents\"'), 'title', 'UPDATE')") && strings.Contains(q, "has_table_privilege(to_regclass('\"documents\"'), 'UPDATE')")
		if got != c.update || !strings.Contains(q, "'INSERT'") {
			t.Errorf("%s: gate %s", c.sql, q)
		}
	}
}
