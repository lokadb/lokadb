package main

// Block 17 (tasks/uuid-keys.md): keys whose type comes from the catalog, not an integer
// assumed; first uuid, text and varchar the same way.

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "U1", name: "check: tables keyed by uuid and varchar, children through FKs on them, pass without id_type / parent_column_type; a numeric key still gets id_type", run: checkUUIDReport},
		check{id: "U2", name: "enable and move-org -mode logical of tables keyed by uuid and varchar under writes (a key from the cloud's DEFAULT, a key changed, a cascade from a cloud table)", run: func(e *env) (bool, string, error) { return checkUUIDMove(e, "logical") }},
		check{id: "U2", name: "the same, -mode trigger, in batches of 7", run: func(e *env) (bool, string, error) { return checkUUIDMove(e, "trigger", "-batch-rows", "7") }},
		check{id: "U3", name: "the proxy (fanout reject) routes by a uuid: literals in any form, text and binary parameters, uuid[] arrays, IN, an FK onto a uuid key, a key from the cloud's DEFAULT; by a varchar key too", run: checkUUIDRouting},
		check{id: "U4", name: "enable -prepare of tables keyed by uuid and varchar, in several batches, then the switch and a move", run: checkUUIDPrepare},
		check{id: "U4", name: "lokadb.prepare of an older lokadb (last bigint) is turned to text and a stopped -prepare resumes after its last key", run: checkPrepareOldLast},
		check{id: "U5", name: "refs-diff / refs-sync by the table's key (ticket_no, an integer key not named id): rows written past the cloud are found and added, the key's sequence moved on", run: checkRefsDiffKey},
		check{id: "U5", name: "the same for a uuid key not named id (file_uuid): no sequence to move", run: checkRefsDiffUUID},
		check{id: "U6", name: "move-back, then disable, of tables keyed by uuid and varchar: every row back, FKs on the tables again", run: checkUUIDBack},
		check{id: "U7", name: "fdw+direct: reads, an UPDATE, INSERTs (keys from the cloud's DEFAULT, a key given, a child by the uuid FK) and DELETEs by a uuid go straight to the organization's database, __refs following", run: checkUUIDDirect},
	)
}

// uuidSpec: udocs keyed by a uuid the cloud's DEFAULT makes, uversions under them through a
// uuid FK, utags keyed by a varchar, utag_uses under them through it. ucomments stays in the
// cloud, its FK onto udocs cascades.
const uuidSpec = `{"tables": [
  {"name": "udocs", "tenant_column": "organization_id"},
  {"name": "uversions", "parent_table": "udocs", "parent_column": "doc_id"},
  {"name": "utags", "tenant_column": "organization_id"},
  {"name": "utag_uses", "parent_table": "utags", "parent_column": "tag_code"}
]}`

// uuidApp makes the tables of uuidSpec: 40 documents (organization 1 has d2, d4, ... d40) with
// a version and a comment each, and "lonely" of organization 1 with neither; 20 tags (tag-2,
// tag-4, ... of organization 1) used once each.
func uuidApp(e *env) error {
	return execSQL(e.ctx, e.cloud(),
		"CREATE TABLE udocs (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), organization_id bigint NOT NULL, title text)",
		"CREATE TABLE uversions (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), doc_id uuid REFERENCES udocs ON DELETE CASCADE, content text)",
		"CREATE TABLE ucomments (id bigserial PRIMARY KEY, doc_id uuid REFERENCES udocs ON DELETE CASCADE, body text)",
		"CREATE TABLE utags (code varchar(20) PRIMARY KEY, organization_id bigint NOT NULL, label text)",
		"CREATE TABLE utag_uses (id bigserial PRIMARY KEY, tag_code varchar(20) REFERENCES utags, note text)",
		"ALTER TABLE udocs OWNER TO app", "ALTER TABLE uversions OWNER TO app", "ALTER TABLE ucomments OWNER TO app",
		"ALTER TABLE utags OWNER TO app", "ALTER TABLE utag_uses OWNER TO app",
		"INSERT INTO udocs (organization_id, title) SELECT 1 + g % 2, 'd' || g FROM generate_series(1, 40) g",
		"INSERT INTO udocs (organization_id, title) VALUES (1, 'lonely')",
		"INSERT INTO uversions (doc_id, content) SELECT id, 'v' || title FROM udocs WHERE title <> 'lonely'",
		"INSERT INTO ucomments (doc_id, body) SELECT id, 'c' || title FROM udocs WHERE title <> 'lonely'",
		"INSERT INTO utags SELECT 'tag-' || g, 1 + g % 2, 'label ' || g FROM generate_series(1, 20) g",
		"INSERT INTO utag_uses (tag_code, note) SELECT code, 'use of ' || code FROM utags")
}

// expects collects the comparisons of a check: what was found, and whether all matched.
type expects struct {
	ok    bool
	notes []string
}

func (x *expects) eq(what, got, want string) {
	s := what + ": " + got
	if got != want {
		x.ok = false
		s = "WRONG " + s + " (want " + want + ")"
	}
	x.notes = append(x.notes, s)
}

// query: one value of sql on d, or the error as the value.
func query(e *env, d, sql string) string {
	v, err := one(e.ctx, d, sql)
	if err != nil {
		return "error: " + firstErrLine(err)
	}
	return v
}

// U1. check reported every table keyed by a uuid id_type ("lokadb supports integer keys only
// so far") and every child of one parent_column_type.
func checkUUIDReport(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(), "CREATE TABLE unum (id numeric PRIMARY KEY, organization_id bigint NOT NULL)", "ALTER TABLE unum OWNER TO app"); err != nil {
		return false, "", err
	}
	out, _ := e.checkWith(uuidSpec)
	x := &expects{ok: true}
	for _, code := range []string{"id_type", "id_missing", "parent_column_type"} {
		x.eq("uuid spec, "+code, fmt.Sprint(reported(out, code)), "false")
	}
	if reported(out, "id_type") {
		x.notes = append(x.notes, finding(out, "id_type"))
	}
	out, _ = e.checkWith(`{"tables": [{"name": "unum", "tenant_column": "organization_id"}]}`)
	x.eq("numeric key, id_type", fmt.Sprint(reported(out, "id_type")), "true")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// U2. enable refused a table keyed by a uuid ("needs a primary key of one integer column").
// Now the refs tables hold uuid and varchar keys, and organization 1 moves under writes made
// at the catch-up: a document whose key the cloud's DEFAULT makes, with a version (its
// organization from lokadb.org, as the proxy sets it), a title changed, a document deleted
// with its version (its cloud comment cascades), a key changed, a tag added and one renamed.
func checkUUIDMove(e *env, mode string, extra ...string) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	lonely, err := one(e.ctx, e.cloud(), "SELECT id::text FROM udocs WHERE title = 'lonely'")
	if err != nil {
		return false, "", err
	}
	var once sync.Once
	var werr error
	out, _, merr := e.moveLines(1, func(l string) {
		if strings.Contains(l, "step 5/6") {
			once.Do(func() {
				werr = execSQL(e.ctx, e.cloud(),
					"SELECT set_config('lokadb.org', '1', false)",
					"WITH d AS (INSERT INTO udocs (organization_id, title) VALUES (1, 'inserted') RETURNING id) INSERT INTO uversions (doc_id, content) SELECT id, 'v-inserted' FROM d",
					"UPDATE udocs SET title = 'updated' WHERE title = 'd2'",
					// d4's version first: after the switch (in trigger mode it can come before the
					// writes end) the FK cascade would run in the organization's database, which
					// leaves the version's refs row in the cloud (found by this check, not block 17's:
					// it happens with integer keys as well)
					"DELETE FROM uversions WHERE doc_id = (SELECT id FROM udocs WHERE title = 'd4')",
					"DELETE FROM udocs WHERE title = 'd4'",
					"UPDATE udocs SET id = '00000000-0000-4000-8000-000000000001' WHERE title = 'lonely'",
					"INSERT INTO utags VALUES ('tag-new', 1, 'added')",
					"DELETE FROM utag_uses WHERE tag_code = 'tag-6'",
					"UPDATE utags SET code = 'tag-six' WHERE code = 'tag-6'")
			})
		}
	}, append([]string{"-mode", mode}, extra...)...)
	if werr != nil {
		return false, "the application's writes during the move failed: " + firstErrLine(werr), nil
	}
	if merr != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	x := &expects{ok: true}
	cust := e.customer()
	// 20 documents + lonely + inserted - d4
	x.eq("documents at the organization", query(e, cust, "SELECT count(*)::text || ', ' || count(*) FILTER (WHERE title = 'updated') || ' updated, ' || count(*) FILTER (WHERE title = 'inserted') || ' inserted, ' || count(*) FILTER (WHERE title = 'd4') || ' d4' FROM udocs"), "21, 1 updated, 1 inserted, 0 d4")
	x.eq("lonely's key", query(e, cust, "SELECT id::text FROM udocs WHERE title = 'lonely'"), "00000000-0000-4000-8000-000000000001")
	x.eq("lonely's old key gone from the refs", query(e, e.cloud(), "SELECT count(*) FROM udocs__refs WHERE id = '"+lonely+"'"), "0")
	// 20 versions - d4's + the inserted one's
	x.eq("versions at the organization", query(e, cust, "SELECT count(*)::text || ', ' || count(*) FILTER (WHERE content = 'v-inserted') FROM uversions"), "20, 1")
	x.eq("comments in the cloud (d4's cascaded)", query(e, e.cloud(), "SELECT count(*)::text || ', ' || count(*) FILTER (WHERE body = 'cd4') FROM ucomments"), "39, 0")
	x.eq("tags at the organization", query(e, cust, "SELECT count(*)::text || ', ' || string_agg(code, ',' ORDER BY code) FILTER (WHERE code IN ('tag-2', 'tag-6', 'tag-new', 'tag-six')) FROM utags"), "11, tag-2,tag-new,tag-six")
	x.eq("tag uses at the organization", query(e, cust, "SELECT count(*)::text FROM utag_uses"), "9")
	x.eq("refs of organization 1 (udocs, utags)", query(e, e.cloud(), "SELECT (SELECT count(*) FROM udocs__refs WHERE tenant = 1)::text || ', ' || (SELECT count(*) FROM utags__refs WHERE tenant = 1)"), "21, 11")
	x.eq("refs types", query(e, e.cloud(), "SELECT format_type(a.atttypid, a.atttypmod) || ', ' || format_type(b.atttypid, b.atttypmod) FROM pg_attribute a, pg_attribute b WHERE a.attrelid = 'udocs__refs'::regclass AND a.attname = 'id' AND b.attrelid = 'utags__refs'::regclass AND b.attname = 'id'"), "uuid, character varying(20)")
	// the cloud reads the organization's rows through its foreign partition
	x.eq("organization 1's documents read in the cloud", query(e, e.cloud(), "SELECT count(*) FROM udocs WHERE organization_id = 1"), "21")
	diff, code, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	x.eq("refs-diff exit", fmt.Sprint(code), "0")
	if code != 0 {
		x.notes = append(x.notes, oneLine(diff))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}

// uuidBytes: a uuid as uuid_send gives it.
func uuidBytes(s string) []byte {
	b, _ := hex.DecodeString(strings.ReplaceAll(s, "-", ""))
	return b
}

// uuidArray: a one-dimensional uuid[] as array_send gives it.
func uuidArray(keys ...string) []byte {
	b := binary.BigEndian.AppendUint32(nil, 1) // dimensions
	b = binary.BigEndian.AppendUint32(b, 0)    // no NULL
	b = binary.BigEndian.AppendUint32(b, 2950) // uuid
	b = binary.BigEndian.AppendUint32(b, uint32(len(keys)))
	b = binary.BigEndian.AppendUint32(b, 1) // lower bound
	for _, k := range keys {
		b = binary.BigEndian.AppendUint32(b, 16)
		b = append(b, uuidBytes(k)...)
	}
	return b
}

// U3. The proxy read a value of a refs lookup as an integer only: a statement by a uuid named
// no organization, and the fanout policy (reject) refused it. Organization 1 is moved; each
// statement must run in its database and find its rows there. Parameters go as pgx sends
// them: a uuid binary (16 bytes, OID 2950), arrays binary with uuid elements; and as text.
func checkUUIDRouting(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	key := func(title string) string {
		v, _ := one(e.ctx, e.cloud(), "SELECT id::text FROM udocs WHERE title = $1", title)
		return v
	}
	k2, k6, lonely := key("d2"), key("d6"), key("lonely")
	stop, err := e.proxyReject()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x := &expects{ok: true}
	// got: the first column of the rows, joined, or the error
	got := func(res *pgconn.Result) string {
		if res.Err != nil {
			return "error: " + firstErrLine(res.Err)
		}
		var vals []string
		for _, r := range res.Rows {
			vals = append(vals, string(r[0]))
		}
		return strings.Join(vals, ",")
	}
	lit := func(what, sql, want string) {
		res, err := simple(e.ctx, c, sql)
		v := ""
		switch {
		case err != nil:
			v = "error: " + firstErrLine(err)
		case len(res) > 0:
			v = got(res[0])
		}
		x.eq(what, v, want)
	}
	param := func(what, sql string, p []byte, oid uint32, format int16, want string) {
		x.eq(what, got(c.ExecParams(e.ctx, sql, [][]byte{p}, []uint32{oid}, []int16{format}, nil).Read()), want)
	}
	upper := strings.ToUpper(k2)
	lit("literal, upper case", "SELECT title FROM udocs WHERE id = '"+upper+"'", "d2")
	lit("literal in braces, cast", "SELECT title FROM udocs WHERE id = '{"+k2+"}'::uuid", "d2")
	lit("literal without hyphens", "SELECT title FROM udocs WHERE id = '"+strings.ReplaceAll(upper, "-", "")+"'", "d2")
	lit("IN", "SELECT title FROM udocs WHERE id IN ('"+k2+"', '"+k6+"') ORDER BY title", "d2,d6")
	lit("= ANY of an array literal", "SELECT title FROM udocs WHERE id = ANY('{"+k2+","+k6+"}'::uuid[]) ORDER BY title", "d2,d6")
	param("binary parameter", "SELECT title FROM udocs WHERE id = $1", uuidBytes(k2), 2950, 1, "d2")
	param("text parameter", "SELECT title FROM udocs WHERE id = $1", []byte(upper), 0, 0, "d2")
	param("binary uuid[] parameter", "SELECT title FROM udocs WHERE id = ANY($1) ORDER BY title", uuidArray(k2, k6), 2951, 1, "d2,d6")
	param("text uuid[] parameter", "SELECT title FROM udocs WHERE id = ANY($1::uuid[]) ORDER BY title", []byte("{"+k2+","+k6+"}"), 0, 0, "d2,d6")
	// only the FK onto the uuid key names the organization
	param("by the FK onto the uuid key", "SELECT content FROM uversions WHERE doc_id = $1", uuidBytes(k2), 2950, 1, "vd2")
	param("a child inserted under its parent's organization", "INSERT INTO uversions (doc_id, content) VALUES ($1, 'via proxy') RETURNING content", uuidBytes(k6), 2950, 1, "via proxy")
	param("UPDATE by the key", "UPDATE udocs SET title = 'updated via proxy' WHERE id = $1 RETURNING title", uuidBytes(k6), 2950, 1, "updated via proxy")
	lit("DELETE by the key", "DELETE FROM udocs WHERE id = '"+lonely+"' RETURNING title", "lonely")
	// a key the cloud's DEFAULT makes, found again by it
	res, err := simple(e.ctx, c, "INSERT INTO udocs (organization_id, title) VALUES (1, 'new via proxy') RETURNING id")
	newKey := ""
	if err == nil && len(res) > 0 && res[0].Err == nil && len(res[0].Rows) == 1 {
		newKey = string(res[0].Rows[0][0])
	}
	x.eq("a new key from the cloud's DEFAULT", fmt.Sprint(len(newKey) == 36), "true")
	param("the new row by its key", "SELECT title FROM udocs WHERE id = $1", []byte(newKey), 0, 0, "new via proxy")
	lit("by the varchar key", "SELECT label FROM utags WHERE code = 'tag-2'", "label 2")
	param("by the FK onto the varchar key", "SELECT note FROM utag_uses WHERE tag_code = $1", []byte("tag-2"), 0, 0, "use of tag-2")
	// where the writes went
	cust := e.customer()
	x.eq("at the organization: the child, the update, the delete, the new row", query(e, cust, `SELECT (SELECT count(*) FROM uversions WHERE content = 'via proxy')::text || ', ' ||
		(SELECT count(*) FROM udocs WHERE title = 'updated via proxy') || ', ' || (SELECT count(*) FROM udocs WHERE title = 'lonely') || ', ' ||
		(SELECT count(*) FROM udocs WHERE title = 'new via proxy')`), "1, 1, 0, 1")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// U4. enable -prepare filled the refs tables in batches by an integer key ($1::bigint, from
// math.MinInt64): a uuid or varchar key failed. 25000 more documents: three batches of udocs.
func checkUUIDPrepare(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"INSERT INTO udocs (organization_id, title) SELECT 1 + g % 2, 'bulk' FROM generate_series(1, 25000) g",
		"INSERT INTO uversions (doc_id, content) SELECT id, 'bulk' FROM udocs WHERE title = 'bulk'"); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	p, err := e.specFile(uuidSpec)
	if err != nil {
		return false, "", err
	}
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app", "-prepare"); err != nil {
		return false, "enable -prepare failed: " + lastErr(out), nil
	}
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app"); err != nil {
		return false, "the switch failed: " + lastErr(out), nil
	}
	x := &expects{ok: true}
	x.eq("udocs__refs differs from udocs", query(e, e.cloud(), "SELECT count(*) FROM udocs d FULL JOIN udocs__refs r ON r.id = d.id WHERE r.id IS NULL OR d.id IS NULL OR r.tenant IS DISTINCT FROM d.organization_id"), "0")
	x.eq("utags__refs differs from utags", query(e, e.cloud(), "SELECT count(*) FROM utags d FULL JOIN utags__refs r ON r.id = d.code WHERE r.id IS NULL OR d.code IS NULL OR r.tenant IS DISTINCT FROM d.organization_id"), "0")
	x.eq("versions whose organization differs from their document's", query(e, e.cloud(), "SELECT count(*) FROM uversions v LEFT JOIN udocs d ON d.id = v.doc_id WHERE v.organization_id IS DISTINCT FROM d.organization_id"), "0")
	x.eq("tag uses whose organization differs from their tag's", query(e, e.cloud(), "SELECT count(*) FROM utag_uses u LEFT JOIN utags t ON t.code = u.tag_code WHERE u.organization_id IS DISTINCT FROM t.organization_id"), "0")
	if out, err := e.move(1); err != nil {
		x.ok = false
		x.notes = append(x.notes, "WRONG move-org 1 failed: "+lastLine(out))
	} else {
		x.eq("organization 1's documents at the organization", query(e, e.customer(), "SELECT count(*) FROM udocs"), "12521")
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}

// U4, the older lokadb.prepare: its last was bigint, a key of any kind is text now. A run of an
// older lokadb stopped in documents' refs after key 100 (its refs rows up to there made); the
// new lokadb turns the column to text, once, and goes on after 100.
func checkPrepareOldLast(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE SCHEMA lokadb",
		`CREATE TABLE lokadb.prepare (table_name text PRIMARY KEY, stage text NOT NULL,
			done bigint NOT NULL DEFAULT 0, total bigint NOT NULL DEFAULT 0, last bigint,
			started timestamptz NOT NULL DEFAULT now(), updated timestamptz NOT NULL DEFAULT now())`,
		"INSERT INTO lokadb.prepare (table_name, stage, done, total, last) VALUES ('documents', 'refs', 100, 200, 100)",
		"CREATE TABLE documents__refs (id bigint PRIMARY KEY, tenant bigint, UNIQUE (tenant, id))",
		"INSERT INTO documents__refs SELECT id, organization_id FROM documents WHERE id <= 100",
		"ALTER TABLE documents__refs OWNER TO app"); err != nil {
		return false, "", err
	}
	x := &expects{ok: true}
	p, err := e.specFile(spec)
	if err != nil {
		return false, "", err
	}
	out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app", "-prepare")
	if err != nil {
		return false, "enable -prepare failed: " + lastErr(out), nil
	}
	// resumed: 100 done before, 100 more, 100 of them new; from the start it would be 300 rows
	x.eq("documents' refs resumed after 100", fmt.Sprint(strings.Contains(out, "table=documents stage=refs rows=200 changed=100")), "true")
	x.eq("type of lokadb.prepare.last", query(e, e.cloud(), "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'lokadb.prepare'::regclass AND attname = 'last'"), "text")
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app", "-prepare"); err != nil {
		x.ok = false
		x.notes = append(x.notes, "WRONG enable -prepare again failed: "+lastErr(out))
	}
	if out, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app"); err != nil {
		x.ok = false
		x.notes = append(x.notes, "WRONG the switch failed: "+lastErr(out))
	}
	x.eq("documents__refs differs from documents", query(e, e.cloud(), "SELECT count(*) FROM documents d FULL JOIN documents__refs r ON r.id = d.id WHERE r.id IS NULL OR d.id IS NULL OR r.tenant IS DISTINCT FROM d.organization_id"), "0")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// U5, the uuid half: no sequence behind a uuid key; the refs table's id is a uuid.
func checkRefsDiffUUID(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE ufiles (file_uuid uuid PRIMARY KEY DEFAULT gen_random_uuid(), organization_id bigint NOT NULL, body text)",
		"ALTER TABLE ufiles OWNER TO app",
		"INSERT INTO ufiles (organization_id, body) SELECT 1 + g % 2, 'f' || g FROM generate_series(1, 20) g"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "ufiles", "tenant_column": "organization_id"}]}`
	const k = "6f1d1c3e-4a7b-4c2d-9e8f-0a1b2c3d4e5f"
	return refsDiffSync(e, "ufiles", "file_uuid", k, "('"+k+"', 1, 'written past the cloud')", false)
}

// U6. move-back and disable of tables keyed by uuid and varchar: enable refused them before.
// Rows written while organization 1 is moved come back; after disable the tables are plain
// again and the FKs point at them, not at the refs tables.
func checkUUIDBack(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	if err := execSQL(e.ctx, e.cloud(),
		"SELECT set_config('lokadb.org', '1', false)",
		"WITH d AS (INSERT INTO udocs (organization_id, title) VALUES (1, 'while moved') RETURNING id) INSERT INTO uversions (doc_id, content) SELECT id, 'v-while moved' FROM d",
		"INSERT INTO utags VALUES ('tag-moved', 1, 'while moved')"); err != nil {
		return false, "writes while moved failed: " + firstErrLine(err), nil
	}
	if out, err := e.run6(e.moveBackArgs(1)...); err != nil {
		return false, "move-back failed: " + oneLine(err.Error()) + " " + lastLine(out), nil
	}
	x := &expects{ok: true}
	x.eq("organization 1 in the cloud's own partitions (udocs, uversions, utags)", query(e, e.cloud(), `SELECT (SELECT count(*) FROM udocs__o1)::text || ', ' || (SELECT count(*) FROM uversions__o1) || ', ' || (SELECT count(*) FROM utags__o1)`), "22, 21, 11")
	x.eq("lokadb.orgs", query(e, e.cloud(), "SELECT count(*) FROM lokadb.orgs"), "0")
	if out, err := e.run6(e.disableArgs()...); err != nil {
		return false, strings.Join(x.notes, "; ") + "; disable failed: " + oneLine(err.Error()) + " " + lastLine(out), nil
	}
	x.eq("after disable: kinds of udocs, uversions, utags", query(e, e.cloud(), "SELECT string_agg(relkind::text, ',' ORDER BY relname) FROM pg_class WHERE relname IN ('udocs', 'uversions', 'utags')"), "r,r,r")
	x.eq("rows (udocs, uversions, utags, utag_uses, ucomments)", query(e, e.cloud(), `SELECT (SELECT count(*) FROM udocs)::text || ', ' || (SELECT count(*) FROM uversions) || ', ' || (SELECT count(*) FROM utags) || ', ' || (SELECT count(*) FROM utag_uses) || ', ' || (SELECT count(*) FROM ucomments)`), "42, 41, 21, 20, 40")
	x.eq("FKs onto udocs and utags", query(e, e.cloud(), "SELECT string_agg(conrelid::regclass::text || '->' || confrelid::regclass::text, ',' ORDER BY conrelid::regclass::text) FROM pg_constraint WHERE contype = 'f' AND confrelid IN ('udocs'::regclass, 'utags'::regclass)"), "ucomments->udocs,utag_uses->utags,uversions->udocs")
	return x.ok, strings.Join(x.notes, "; "), nil
}

// U5, the integer half: refs-diff read the organization's partition by a column id, which a
// table keyed by another column (block 4) does not have: "column f.id does not exist".
func checkRefsDiffKey(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE tickets (ticket_no bigserial PRIMARY KEY, organization_id bigint NOT NULL, body text)",
		"ALTER TABLE tickets OWNER TO app",
		"INSERT INTO tickets (organization_id, body) SELECT 1 + g % 2, 't' || g FROM generate_series(1, 20) g"); err != nil {
		return false, "", err
	}
	e.spec = `{"tables": [{"name": "tickets", "tenant_column": "organization_id"}]}`
	return refsDiffSync(e, "tickets", "ticket_no", "900001", "(900001, 1, 'written past the cloud')", true)
}

// refsDiffSync: after a move of organization 1, a row (values) is written into its database
// past the cloud and another one deleted there; refs-diff must find both (and, seq, the key's
// sequence behind), refs-sync -delete-stale fix them, and the proxy then find the new row by
// its key (newKey, as SQL text).
func refsDiffSync(e *env, table, key, newKey, values string, seq bool) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	if out, err := e.move(1, "-customer-app", e.appCustomer()); err != nil {
		return false, "move-org failed: " + lastLine(out), nil
	}
	sch, err := one(e.ctx, e.cloud(), "SELECT schema FROM lokadb.orgs WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	rel := sch + "." + table
	gone, err := one(e.ctx, e.customer(), fmt.Sprintf("SELECT %s::text FROM %s ORDER BY %s LIMIT 1", key, rel, key))
	if err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.customer(),
		fmt.Sprintf("INSERT INTO %s (%s, organization_id, body) VALUES %s", rel, key, values),
		fmt.Sprintf("DELETE FROM %s WHERE %s = '%s'", rel, key, gone)); err != nil {
		return false, "", err
	}
	diff, dcode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	var notes []string
	ok := dcode == 2 && strings.Contains(diff, "1 row(s) written past the cloud") && strings.Contains(diff, "1 id(s) of rows the organization no longer has") &&
		strings.Contains(diff, newKey) && strings.Contains(diff, "past the cloud's sequence") == seq
	notes = append(notes, fmt.Sprintf("refs-diff exit %d: %s", dcode, oneLine(diff)))
	// the row's key 900001 is far past the sequence: a jump refs-sync makes only when asked (A6, M13)
	sync, scode, err := e.schemaCmd("refs-sync", "-delete-stale", "-max-sequence-jump", "1000000")
	if err != nil {
		return false, "", err
	}
	after, acode, err := e.schemaCmd("refs-diff")
	if err != nil {
		return false, "", err
	}
	if scode != 0 || acode != 0 {
		ok = false
		notes = append(notes, fmt.Sprintf("refs-sync -delete-stale exit %d, refs-diff after it exit %d: %s", scode, acode, oneLine(sync+after)))
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	res, selErr := simple(e.ctx, c, fmt.Sprintf("SELECT body FROM %s WHERE %s = '%s'", table, key, newKey))
	got := "-"
	if selErr == nil && len(res) > 0 && len(res[0].Rows) > 0 {
		got = string(res[0].Rows[0][0])
	}
	if got != "written past the cloud" {
		ok = false
	}
	notes = append(notes, fmt.Sprintf("by %s through the proxy: %q (error %s)", key, got, errText(selErr)))
	return ok, strings.Join(notes, "; "), nil
}

// U7. The direct path (fdw+direct) found the organization of a statement by an integer only:
// a read or an UPDATE by a uuid named none and stayed off it (and under fanout reject was
// refused); a direct INSERT took its keys from a sequence alone, so a table keyed by
// gen_random_uuid() wrote the cloud's way. pgx sends the uuid parameters binary.
func checkUUIDDirect(e *env) (bool, string, error) {
	if err := uuidApp(e); err != nil {
		return false, "", err
	}
	e.spec = uuidSpec
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "setup failed: " + lastErr(err.Error()), nil
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	k, err := one(e.ctx, e.cloud(), "SELECT id::text FROM udocs WHERE title = 'd2'")
	if err != nil {
		return false, "", err
	}
	var key [16]byte
	copy(key[:], uuidBytes(k))
	c, err := e.viaProxyPgx("u7")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	x := &expects{ok: true}
	before := directCount(e.proxyRoutes(), "1")
	x.eq("read by the uuid", strings.Join(rowsText(e, c, "SELECT title FROM udocs WHERE id = $1", key), ","), "d2,tag: SELECT 1")
	x.eq("read by the FK onto it", strings.Join(rowsText(e, c, "SELECT content FROM uversions WHERE doc_id = $1", key), ","), "vd2,tag: SELECT 1")
	_, uerr := c.Exec(e.ctx, "UPDATE udocs SET title = 'direct' WHERE id = $1", key)
	x.eq("UPDATE by the uuid", errText(uerr), errText(nil))
	x.eq("read by the varchar key", strings.Join(rowsText(e, c, "SELECT label FROM utags WHERE code = $1", "tag-2"), ","), "label 2,tag: SELECT 1")
	x.eq("statements of organization 1 on the direct path", fmt.Sprint(directCount(e.proxyRoutes(), "1")-before >= 4), "true")
	x.eq("the update at the organization", query(e, e.customer(), "SELECT title FROM udocs WHERE id = '"+k+"'"), "direct")
	// writes (stage 8b): keys from the key's DEFAULT computed in the cloud, a key given, a
	// child under its parent by the uuid FK, a DELETE by the key; each one statement on the
	// direct path, __refs following. A DELETE of udocs goes the cloud's way: uversions' FK
	// onto it cascades, and the cascade's trigger (lokadb_fkcascade_*, block 5) fires in the
	// cloud only; that trigger is on DELETE alone, so INSERTs and UPDATEs of udocs stay direct.
	stepOn := func(what, want, sql string, args ...any) []string {
		n := directCount(e.proxyRoutes(), "1")
		rows := rowsText(e, c, sql, args...)
		x.eq(what+" on the direct path", fmt.Sprint(directCount(e.proxyRoutes(), "1")-n), want)
		return rows[:max(0, len(rows)-1)]
	}
	step := func(what, sql string, args ...any) []string { return stepOn(what, "1", sql, args...) }
	ins := step("INSERT without a key", "INSERT INTO udocs (organization_id, title) VALUES (1, 'u7-a'), (1, 'u7-b') RETURNING id::text")
	if len(ins) == 2 {
		in := "'" + strings.Join(ins, "', '") + "'"
		x.eq("the new rows at the organization, their refs, in the cloud's partition", query(e, e.customer(), "SELECT count(*) FROM udocs WHERE id IN ("+in+")")+", "+
			query(e, e.cloud(), "SELECT count(*) FROM udocs__refs WHERE tenant = 1 AND id IN ("+in+")")+", "+
			query(e, e.cloud(), "SELECT count(*) FROM ONLY udocs__cloud WHERE title LIKE 'u7-%'"), "2, 2, 0")
	} else {
		x.eq("INSERT without a key returned", strings.Join(ins, ","), "two keys")
	}
	const given = "7a6b5c4d-3e2f-4a1b-8c9d-0e1f2a3b4c5d"
	var gk [16]byte
	copy(gk[:], uuidBytes(given))
	step("INSERT with a key given", "INSERT INTO udocs (id, organization_id, title) VALUES ($1, 1, 'u7-given')", gk)
	step("a child by the uuid FK", "INSERT INTO uversions (doc_id, content) VALUES ($1, 'u7-child')", gk)
	x.eq("the given key and the child at the organization and in the refs", query(e, e.customer(), "SELECT count(*) FROM udocs WHERE id = '"+given+"'")+" "+
		query(e, e.customer(), "SELECT count(*) FROM uversions WHERE doc_id = '"+given+"' AND organization_id = 1")+", "+
		query(e, e.cloud(), "SELECT count(*) FROM udocs__refs WHERE tenant = 1 AND id = '"+given+"'")+" "+
		query(e, e.cloud(), "SELECT count(*) FROM uversions__refs r WHERE tenant = 1 AND id = (SELECT id FROM uversions WHERE content = 'u7-child')"), "1 1, 1 1")
	step("DELETE of the child", "DELETE FROM uversions WHERE doc_id = $1", gk)
	stepOn("DELETE by the key (a cascade's trigger: the cloud's way)", "0", "DELETE FROM udocs WHERE id = $1", gk)
	x.eq("after the DELETEs: at the organization, in the refs", query(e, e.customer(), "SELECT count(*) FROM udocs WHERE id = '"+given+"'")+", "+
		query(e, e.cloud(), "SELECT count(*) FROM udocs__refs WHERE id = '"+given+"'"), "0, 0")
	if !x.ok {
		x.notes = append(x.notes, e.proxyLines("msg=route"))
	}
	return x.ok, strings.Join(x.notes, "; "), nil
}
