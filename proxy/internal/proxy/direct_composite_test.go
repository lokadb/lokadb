package proxy

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// tuples: tupleRefs' answer (direct_composite.go): how many keys map to another organization,
// then each row's key, its refs row added.
func tuples(others string, keys ...[]string) [][]string {
	rows := [][]string{{others, "", "", ""}}
	for _, k := range keys {
		rows = append(rows, append([]string{"", "true"}, k...))
	}
	return rows
}

// A composite key on the direct path (tasks/composite-direct.md): an INSERT's rows go to the
// cloud with the defaults of the columns it does not name in place, one statement writes their
// refs rows by id_1, id_2 and gives the keys back (block 10's review of stage 1: one exchange);
// a DELETE returns the key's columns and their refs rows leave by rows of the key.
func TestDirectWriteComposite(t *testing.T) {
	d := directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id_1, id_2, lokadb_n) AS MATERIALIZED (VALUES (1777, 1, 1), (1777, 2, 2))") && strings.Contains(q, "ON CONFLICT (id_1, id_2) DO NOTHING") {
			return tuples("0", []string{"1777", "1"}, []string{"1777", "2"}), "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES (1777, 1, 1, 5), (1777, 2, 1, 6)"); code != "" {
		t.Fatalf("parcels: error %q, cloud %q", code, d.cloud.log())
	}
	if org, cloud := d.org1.log(), d.cloud.log(); len(org) != 1 || !strings.Contains(org[0], "(tracking_no, piece, account_id, weight) VALUES (1777, 1, 1, 5), (1777, 2, 1, 6)") ||
		!inOrder(cloud, "BEGIN", "pg_advisory_xact_lock_shared", "INSERT INTO \"parcels__refs\" (id_1, id_2, tenant) SELECT id_1, id_2, 1 FROM lokadb_v", "COMMIT") {
		t.Fatalf("parcels: org1 %q, cloud %q", org, cloud)
	}
	// kind A without its id: the sequence's nextval in the cloud's VALUES, one exchange
	d = directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id_1, id_2, lokadb_n) AS MATERIALIZED (VALUES (1, (nextval('orders_id_seq'::regclass))::bigint, 1))") {
			return tuples("0", []string{"1", "41"}), "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO orders (account_id, title) VALUES (1, 'x')"); code != "" {
		t.Fatalf("orders: error %q, cloud %q", code, d.cloud.log())
	}
	if org, cloud := d.org1.log(), d.cloud.log(); len(org) != 1 || !strings.Contains(org[0], "(account_id, title, id) VALUES (1, 'x', '41')") ||
		!inOrder(cloud, "lokadb_v(id_1, id_2, lokadb_n)", "COMMIT") || ranAny(cloud, "ROLLBACK") || ranAny(cloud, "lokadb_k AS") {
		t.Fatalf("orders: org1 %q, cloud %q", org, cloud)
	}
	// a key of another organization: the cloud's way answers
	d = directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_v(id_1, id_2, lokadb_n)") {
			return tuples("1", []string{"2001", "1"}), "", true
		}
		return nil, "", false
	}
	d.query(t, "INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES (2001, 1, 1, 7)")
	if org, cloud := d.org1.log(), d.cloud.log(); len(org) != 0 || !inOrder(cloud, "lokadb_v(id_1, id_2, lokadb_n)", "ROLLBACK", "VALUES (2001, 1, 1, 7)") {
		t.Fatalf("another organization's key: org1 %q, cloud %q", org, cloud)
	}
	// a DELETE by the whole key
	d = directSession(t, false, composite)
	d.s.orgCache["parcels__refs"] = map[string]string{tupleKey([]string{"1777", "1"}): "1"}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "DELETE") && strings.HasSuffix(q, "RETURNING tracking_no, piece") {
			return [][]string{{"1777", "1"}}, "", true
		}
		return nil, "", false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "DELETE FROM parcels WHERE tracking_no = 1777 AND piece = 1"})
	if _, code := d.answerTypes(t); code != "" {
		t.Fatalf("delete: error %q, cloud %q", code, d.cloud.log())
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "DELETE FROM \"parcels__refs\" AS lokadb_d WHERE (id_1, id_2) IN ((1777, 1))", "COMMIT") {
		t.Fatalf("delete: the cloud ran %q", cloud)
	}
	// an upsert: the refs row of a row that was updated, not inserted, leaves __refs
	d = directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "VALUES (1901, 1, 1), (1001, 1, 2)") {
			return tuples("0", []string{"1901", "1"}, []string{"1001", "1"}), "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, "RETURNING tracking_no, piece, xmax::text = '0'") {
			return [][]string{{"1901", "1", "t"}, {"1001", "1", "f"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES (1901, 1, 1, 1), (1001, 1, 1, 99) ON CONFLICT (tracking_no, piece) DO UPDATE SET weight = excluded.weight"); code != "" {
		t.Fatalf("upsert: error %q, org1 %q, cloud %q", code, d.org1.log(), d.cloud.log())
	}
	if cloud := d.cloud.log(); !inOrder(cloud, "lokadb_v(id_1, id_2, lokadb_n)", "DELETE FROM \"parcels__refs\" AS lokadb_d WHERE (id_1, id_2) IN ((1001, 1))", "COMMIT") || ranAny(cloud, "IN ((1901, 1))") {
		t.Fatalf("upsert: the cloud ran %q", cloud)
	}
	// INSERT … SELECT naming the key: RETURNING its columns, their refs rows after
	d = directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "VALUES (6001, 1, 1), (6001, 2, 2)") {
			return tuples("0", []string{"6001", "1"}, []string{"6001", "2"}), "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, "RETURNING tracking_no, piece") {
			return [][]string{{"6001", "1"}, {"6001", "2"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO parcels (tracking_no, piece, account_id, weight) SELECT tracking_no + 5000, piece, account_id, weight FROM parcels WHERE account_id = 1"); code != "" {
		t.Fatalf("insert select: error %q, org1 %q, cloud %q", code, d.org1.log(), d.cloud.log())
	}
	if org, cloud := d.org1.log(), d.cloud.log(); !inOrder(org, "BEGIN", "RETURNING tracking_no, piece", "COMMIT") || !inOrder(cloud, "lokadb_v(id_1, id_2, lokadb_n)", "COMMIT") {
		t.Fatalf("insert select: org1 %q, cloud %q", org, cloud)
	}
	// unnest without the id: the cloud's defaults as one more array
	d = directSession(t, false, composite)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		switch {
		case strings.Contains(q, "WITH lokadb_k AS (SELECT (nextval('orders_id_seq'::regclass))::text AS k1 FROM generate_series(1, 2))"):
			return [][]string{{"41"}, {"42"}}, "", true
		case strings.Contains(q, "VALUES (1, 41, 1), (1, 42, 2)"):
			return tuples("0", []string{"1", "41"}, []string{"1", "42"}), "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT") && strings.HasSuffix(q, "RETURNING account_id, id") {
			return [][]string{{"1", "41"}, {"1", "42"}}, "", true
		}
		return nil, "", false
	}
	if code := d.query(t, "INSERT INTO orders (account_id, title) SELECT * FROM unnest(ARRAY[1, 1]::bigint[], ARRAY['a', 'b'])"); code != "" {
		t.Fatalf("unnest: error %q, org1 %q, cloud %q", code, d.org1.log(), d.cloud.log())
	}
	if org, cloud := d.org1.log(), d.cloud.log(); !inOrder(org, "BEGIN", "ARRAY['41', '42']::bigint[]", "COMMIT") || !inOrder(cloud, "lokadb_k AS", "lokadb_v(id_1, id_2, lokadb_n)", "COMMIT") {
		t.Fatalf("unnest: org1 %q, cloud %q", org, cloud)
	}
	// the organization column not named: no organization, the cloud's way
	d = directSession(t, false, composite)
	d.query(t, "INSERT INTO orders (title) VALUES ('x')")
	if len(d.org1.log()) > 0 {
		t.Errorf("no organization: went the direct path: org1 %q", d.org1.log())
	}
}

// Block 9's pipeline with a composite key (block 9's review of stage 2): one tupleRefs per
// write in the cloud's round trip, the organization's rows with the keys it gave, an upsert's
// refs row of a row it updated deleted after, then both commits.
func TestDirectPipelineComposite(t *testing.T) {
	d, l := txSession(t, composite)
	var id atomic.Int32
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		switch {
		case strings.Contains(q, "lokadb_v(id_1, id_2, lokadb_n)") && strings.Contains(q, "nextval('orders_id_seq'"):
			return tuples("0", []string{"1", itoa(41 + int(id.Add(1)) - 1)}), "", true
		case strings.Contains(q, "VALUES (1901, 1, 1), (1001, 1, 2)"):
			return tuples("0", []string{"1901", "1"}, []string{"1001", "1"}), "", true
		}
		return nil, "", false
	}
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "INSERT INTO parcels") {
			return [][]string{{"1901", "1", "t"}, {"1001", "1", "f"}}, "", true
		}
		return nil, "", false
	}
	for name, sql := range map[string]string{
		"o": "INSERT INTO orders (account_id, title) VALUES ($1, $2)",
		"u": "INSERT INTO parcels (tracking_no, piece, account_id, weight) VALUES ($1, 1, 1, 1), (1001, 1, 1, 99) ON CONFLICT (tracking_no, piece) DO UPDATE SET weight = excluded.weight",
	} {
		sendAll(t, d.fe, &pgproto3.Parse{Name: name, Query: sql}, &pgproto3.Sync{})
		if code := errorCode(t, d.fe); code != "" {
			t.Fatalf("prepare %s: %s", name, code)
		}
	}
	unit := func(name string, args ...string) []pgproto3.FrontendMessage {
		var ps [][]byte
		for _, a := range args {
			ps = append(ps, []byte(a))
		}
		return []pgproto3.FrontendMessage{&pgproto3.Bind{PreparedStatement: name, Parameters: ps}, &pgproto3.Execute{}}
	}
	n := len(l.get())
	var ms []pgproto3.FrontendMessage
	for _, u := range [][]pgproto3.FrontendMessage{unit("o", "1", "x"), unit("o", "1", "y"), unit("u", "1901")} {
		ms = append(ms, u...)
	}
	sendAll(t, d.fe, append(ms, &pgproto3.Sync{})...)
	types, code, st := d.answerAll(t)
	all := l.get()[n:]
	if code != "" || st != 'I' || strings.Count(strings.Join(types, ","), "CommandComplete") != 3 {
		t.Fatalf("pipeline: %v %q %q; %q", types, code, st, all)
	}
	if !inOrder(all, "cloud: BEGIN", "cloud: WITH lokadb_v(id_1, id_2, lokadb_n)", "cloud: WITH lokadb_v(id_1, id_2, lokadb_n)", "cloud: WITH lokadb_v(id_1, id_2, lokadb_n)",
		"org1: INSERT INTO orders (account_id, title, id) VALUES (1, x, '41')", "org1: INSERT INTO orders (account_id, title, id) VALUES (1, y, '42')",
		"org1: INSERT INTO parcels", "cloud: DELETE FROM \"parcels__refs\" AS lokadb_d WHERE (id_1, id_2) IN ((1001, 1))", "org1: COMMIT", "cloud: COMMIT") ||
		ranAny(all, "lokadb_k AS") || ranAny(all, "IN ((1901, 1))") {
		t.Fatalf("the pipeline's transaction: %q", all)
	}
}
