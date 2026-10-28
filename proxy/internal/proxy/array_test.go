package proxy

import (
	"encoding/binary"
	"reflect"
	"testing"
)

func TestArrayHints(t *testing.T) {
	m := testMeta()
	cases := []struct {
		name, sql string
		hints     []Hint
	}{
		{"pgx = ANY($1)", `SELECT * FROM documents WHERE id = ANY($1)`,
			[]Hint{{"documents", "id", []ValueRef{{Param: 1, Array: true}}}}},
		{"cast array literal", `select * from documents where organization_id = any('{1,2}'::bigint[])`,
			[]Hint{{"documents", "organization_id", []ValueRef{{Const: "1"}, {Const: "2"}}}}},
		{"ARRAY[...]", `select * from document_versions where document_id = any(array[$1, 7])`,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Param: 1}, {Const: "7"}}}}},
		{"empty literal", `delete from documents where id = any('{}'::bigint[])`,
			[]Hint{{"documents", "id", nil}}},
		{"django bulk_create with psycopg 3", `INSERT INTO "documents" ("organization_id", "project_id", "slug") SELECT * FROM UNNEST(($1::int2[])::bigint[], ($2::int2[])::bigint[], ($3)::varchar[]) RETURNING "documents"."id"`,
			[]Hint{{"documents", "organization_id", []ValueRef{{Param: 1, Array: true}}}}},
		{"child bulk insert", `INSERT INTO "document_versions" ("document_id", "content") SELECT * FROM UNNEST(($1::int8[]), ($2)::text[])`,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Param: 1, Array: true}}}}},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.ParseError || a.Unhinted || !reflect.DeepEqual(a.Hints, c.hints) {
			t.Errorf("%s: unhinted=%v hints=%+v, want %+v", c.name, a.Unhinted, a.Hints, c.hints)
		}
	}
	for _, sql := range []string{
		`select * from documents where id <> any($1)`,
		`select * from documents where id > any($1)`,
		`select * from documents where id = any(select document_id from document_versions where content = $1)`,
		`select * from documents where organization_id = any('{{1,2}}'::bigint[])`,
		`select * from documents where organization_id = any('{1,NULL}'::bigint[])`,
	} {
		if a := analyze(sql, m); !a.Unhinted {
			t.Errorf("%s: hinted %+v, want no organization", sql, a.Hints)
		}
	}
}

// arrayBin encodes a one-dimensional array as Postgres array_send does; nil elements are NULLs.
func arrayBin(oid uint32, size int, elems ...*int64) []byte {
	b := binary.BigEndian.AppendUint32(nil, 1)
	hasNull := uint32(0)
	for _, e := range elems {
		if e == nil {
			hasNull = 1
		}
	}
	b = binary.BigEndian.AppendUint32(b, hasNull)
	b = binary.BigEndian.AppendUint32(b, oid)
	b = binary.BigEndian.AppendUint32(b, uint32(len(elems)))
	b = binary.BigEndian.AppendUint32(b, 1)
	for _, e := range elems {
		if e == nil {
			b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
			continue
		}
		b = binary.BigEndian.AppendUint32(b, uint32(size))
		switch size {
		case 2:
			b = binary.BigEndian.AppendUint16(b, uint16(*e))
		case 4:
			b = binary.BigEndian.AppendUint32(b, uint32(*e))
		case 8:
			b = binary.BigEndian.AppendUint64(b, uint64(*e))
		}
	}
	return b
}

func i64(v int64) *int64 { return &v }

func TestResolveOrgArrays(t *testing.T) {
	s := &session{meta: testMeta(), orgCache: map[string]map[string]string{
		"documents__refs": {"10": "1", "11": "1", "20": "2"},
	}}
	ids := `SELECT * FROM documents WHERE id = ANY($1)`
	orgs := `INSERT INTO "documents" ("organization_id", "slug") SELECT * FROM UNNEST(($1::int2[])::bigint[], ($2)::varchar[])`
	empty := []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 20} // ndim 0
	cases := []struct {
		name    string
		sql     string
		param   []byte
		format  int16
		org     string
		unknown bool
	}{
		{"text ids of one organization", ids, []byte("{10,11}"), 0, "1", false},
		{"text ids of two organizations", ids, []byte("{10,20}"), 0, "", true}, // a fanout: the policy's (review of routing R5)
		{"binary int8[] (pgx)", ids, arrayBin(20, 8, i64(11), i64(10)), 1, "1", false},
		{"binary int4[]", ids, arrayBin(23, 4, i64(20)), 1, "2", false},
		{"empty text array", ids, []byte("{}"), 0, "-1", false},
		{"empty array: nothing can match", ids, empty, 1, "-1", false},
		{"django text int2[] organizations", orgs, []byte("{2,2,2}"), 0, "2", false},
		{"django binary int2[] organizations", orgs, arrayBin(21, 2, i64(1), i64(1)), 1, "1", false},
		{"NULL element", orgs, arrayBin(21, 2, i64(1), nil), 1, "", true},
		{"not integers", ids, []byte("{a,b}"), 0, "", true},
		{"two dimensions", ids, []byte("{{10}}"), 0, "", true},
	}
	for _, c := range cases {
		p := &portalInfo{params: [][]byte{c.param, []byte("{x}")}, formats: []int16{c.format, 0}}
		org, incomplete, unknown, _ := s.resolveOrg(analyze(c.sql, s.meta), p.values, false)
		if org != c.org || incomplete || unknown != c.unknown {
			t.Errorf("%s: org %q incomplete %v unknown %v, want %q %v", c.name, org, incomplete, unknown, c.org, c.unknown)
		}
	}
	// ids not cached yet: a refs lookup is needed first
	p := &portalInfo{params: [][]byte{[]byte("{30}")}}
	if _, incomplete, _, _ := s.resolveOrg(analyze(ids, s.meta), p.values, false); !incomplete {
		t.Error("uncached id: lookup not requested")
	}
}

func TestFanoutUnknownValues(t *testing.T) {
	f, _ := Config{}.FanoutFor("db")
	an := analyze(`SELECT * FROM documents WHERE id = ANY($1)`, testMeta())
	if f.Decide("", an) != FanoutNone || f.decide("", an, true) != FanoutRefused || fanoutTable(an) != "documents" {
		t.Errorf("decide: static %d, unreadable values %d, table %q", f.Decide("", an), f.decide("", an, true), fanoutTable(an))
	}
}
