package main

// Block 4, schema support: what the application declared on a table keeps working the same
// at the organization after move-org.

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X1", name: "EXCLUDE and UNIQUE DEFERRABLE stay constraints at the organization after move-org", run: checkExcludeDeferrable},
		check{id: "X2", name: "an ICU collation of the application: check does not block, move-org creates it at the organization", run: checkICUCollation},
		check{id: "X3", name: "a 58-byte table name, its __org1 and __org2 longer than 63 bytes: lokadb shortens its names, two organizations move into partitions of their own, a migration's unique index gets distinct names", run: checkLongNames},
		check{id: "X4", name: "check reports an FK declared on every partition once, and the primary key of a table without id not again", run: checkReportRepeats},
		check{id: "X9", name: "an invalid unique index (a failed CREATE INDEX CONCURRENTLY) is not made valid at the organization", run: checkInvalidIndexMoved},
	)
}

// X1 (A2). Overlapping ranges must be refused and a deferred unique checked only at COMMIT,
// in the organization's database as in the cloud.
func checkExcludeDeferrable(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents ADD COLUMN during tstzrange",
		"ALTER TABLE documents ADD CONSTRAINT documents_no_overlap EXCLUDE USING gist (during WITH &&)",
		"ALTER TABLE documents ADD CONSTRAINT documents_title_key UNIQUE (title) DEFERRABLE INITIALLY DEFERRED"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if out, err := e.move(1); err != nil {
		return false, "move-org failed: " + lastErr(out), nil
	}
	cons, err := one(e.ctx, e.customer(), `SELECT coalesce(string_agg(conname::text || ':' || contype::text || CASE WHEN condeferred THEN ':deferred' ELSE '' END, ', ' ORDER BY conname), '')
		FROM pg_constraint WHERE conrelid = 'documents'::regclass AND conname IN ('documents_no_overlap', 'documents_title_key')`)
	if err != nil {
		return false, "", err
	}
	// through the cloud: rows of organization 1 go to its database
	overlap := execSQL(e.ctx, e.cloud(),
		"INSERT INTO documents (organization_id, title, during) VALUES (1, 'x1a', '[2026-01-01,2026-01-10)')",
		"INSERT INTO documents (organization_id, title, during) VALUES (1, 'x1b', '[2026-01-05,2026-01-15)')")
	overlapRes := "accepted"
	if overlap != nil {
		overlapRes = "refused: " + firstErrLine(overlap)
	}
	deferred := execSQL(e.ctx, e.cloud(), "BEGIN",
		"INSERT INTO documents (organization_id, title) VALUES (1, 'd2')",
		"UPDATE documents SET title = 'd2-renamed' WHERE organization_id = 1 AND title = 'd2' AND id = (SELECT max(id) FROM documents WHERE organization_id = 1)",
		"COMMIT")
	deferredRes := "committed"
	if deferred != nil {
		deferredRes = "refused: " + firstErrLine(deferred)
	}
	dup := execSQL(e.ctx, e.cloud(), "INSERT INTO documents (organization_id, title) VALUES (1, 'd4')")
	dupRes := "accepted"
	if dup != nil {
		dupRes = "refused: " + firstErrLine(dup)
	}
	ok := cons == "documents_no_overlap:x, documents_title_key:u:deferred" &&
		strings.Contains(overlapRes, "refused") && strings.Contains(overlapRes, "documents_no_overlap") &&
		deferredRes == "committed" && strings.Contains(dupRes, "documents_title_key")
	return ok, fmt.Sprintf("constraints at the organization: %q; overlapping ranges: %s; a duplicate title fixed before COMMIT: %s; a duplicate title: %s",
		cons, overlapRes, deferredRes, dupRes), nil
}

// X2 (step 5). A case-insensitive ICU collation on a column with a unique index: the
// organization's database gets the same collation, so 'ABC' and 'abc' still collide there.
func checkICUCollation(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE COLLATION public.ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
		"ALTER TABLE documents ADD COLUMN code text COLLATE public.ci",
		"CREATE UNIQUE INDEX documents_code ON documents (organization_id, code)"); err != nil {
		return false, "", err
	}
	out, code := e.checkWith(spec, "-customer", e.customer())
	blocked := reported(out, "customer_collation_missing")
	if err := e.enable(); err != nil {
		return false, "", err
	}
	mv, moveErr := e.move(1)
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	coll, _ := one(e.ctx, e.customer(), "SELECT coalesce((SELECT collprovider::text || ' deterministic ' || collisdeterministic::text FROM pg_collation WHERE collname = 'ci'), 'none')")
	dup := "not tried"
	if moveErr == nil {
		err := execSQL(e.ctx, e.cloud(),
			"INSERT INTO documents (organization_id, title, code) VALUES (1, 'x2a', 'ABC')",
			"INSERT INTO documents (organization_id, title, code) VALUES (1, 'x2b', 'abc')")
		dup = "accepted"
		if err != nil {
			dup = "refused: " + firstErrLine(err)
		}
	}
	ok := !blocked && code != 3 && moveErr == nil && coll == "i deterministic false" && strings.Contains(dup, "documents_code")
	return ok, fmt.Sprintf("check: %s (exit %d), customer_collation_missing %v, %s; %s; collation ci at the organization: %s; 'ABC' then 'abc': %s",
		verdict(out), code, blocked, finding(out, "customer_creates"), moved, coll, dup), nil
}

// X3 (A8). 58 bytes: <t>__org1 and <t>__org2 (64) do not fit. Postgres cut both to the same
// 63 bytes, so the second organization's partition had the first one's name; a unique index
// of a migration got the same name on two partitions. lokadb shortens such names with a hash.
func checkLongNames(e *env) (bool, string, error) {
	t := "documents_" + strings.Repeat("l", 48)
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE "+t+" (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, title text)",
		"ALTER TABLE "+t+" OWNER TO app",
		"INSERT INTO "+t+" (organization_id, title) SELECT 1 + g % 2, 'r' || g FROM generate_series(1, 100) g"); err != nil {
		return false, "", err
	}
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	val := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	e.spec = `{"tables": [{"name": "` + t + `", "tenant_column": "organization_id"}]}`
	out, code := e.checkWith(e.spec)
	note(code != 3 && code != 5 && !strings.Contains(out, "exceed Postgres"), fmt.Sprintf("check: %s (exit %d)", verdict(out), code))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if mv, err := e.move(1); err != nil {
		note(false, "move-org 1 failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	db2 := e.db + "_c2"
	if err := execSQL(e.ctx, dsn("customer", "postgres", "postgres"),
		"DROP DATABASE IF EXISTS "+db2+" WITH (FORCE)", "CREATE DATABASE "+db2+" OWNER app"); err != nil {
		return false, "", err
	}
	cust2, app2 := dsn("customer", db2, "postgres"), dsn("customer", db2, "lokadb_app")
	if err := execSQL(e.ctx, cust2, "GRANT CREATE ON SCHEMA public TO lokadb_app"); err != nil {
		return false, "", err
	}
	if mv, err := e.move(2, "-customer", cust2, "-customer-app", app2); err != nil {
		note(false, "move-org 2 failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	parts := val(e.cloud(), `SELECT count(*) || ' ' || max(octet_length(c.relname)) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = '`+t+`'::regclass AND c.relkind = 'f'`)
	n1, n2 := val(e.customer(), "SELECT count(*) FROM "+t), val(cust2, "SELECT count(*) FROM "+t)
	c1, c2 := val(e.cloud(), "SELECT count(*) FROM "+t+" WHERE organization_id = 1"), val(e.cloud(), "SELECT count(*) FROM "+t+" WHERE organization_id = 2")
	note(parts == "2 63" && n1 == "50" && n2 == "50" && c1 == "50" && c2 == "50",
		fmt.Sprintf("two organizations moved: foreign partitions and their longest name %s; at organization 1 %s rows, at 2 %s; through the cloud %s and %s", parts, n1, n2, c1, c2))
	// a migration's unique index, named past the 30 bytes the partitions' names keep of it
	stop, err := e.proxyWith(map[string]string{"1": e.customer(), "2": cust2})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	_, err = simple(e.ctx, c, "CREATE UNIQUE INDEX "+t[:40]+"_title_key ON "+t+" (organization_id, title)")
	idx := val(e.cloud(), `SELECT count(DISTINCT ic.relname) || ' ' || max(octet_length(ic.relname)) FROM pg_index x JOIN pg_class ic ON ic.oid = x.indexrelid
		JOIN pg_inherits i ON i.inhrelid = x.indrelid WHERE i.inhparent = '`+t+`'::regclass AND x.indisunique AND NOT x.indisprimary`)
	note(err == nil && idx == "2 63", // DEFAULT and __none, the organizations' partitions are foreign
		fmt.Sprintf("CREATE UNIQUE INDEX through the proxy: %s; on the cloud's partitions: %s distinct names, longest", firstErrLineOr(err), idx))
	return ok, strings.Join(notes, "; "), nil
}

// X4 (U7). As Pagila: payment is partitioned and every partition declares its own FK to
// customer (onto code and region, a unique pair other than its key customer_id: an FK of one
// unique column is supported since block 4, item 2); payment_p2 alone also declares one on ref.
// The report (format §4): one finding on payment naming the three partitions in
// subject.partitions and in examples; the FK of payment_p2 alone is on payment_p2, with
// subject.partition_of payment.
func checkReportRepeats(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE customer (customer_id bigserial PRIMARY KEY, organization_id bigint NOT NULL, code text, region text, UNIQUE (code, region))",
		"CREATE TABLE payment (id bigserial, code text, ref text, region text, paid date NOT NULL) PARTITION BY RANGE (paid)",
		"CREATE TABLE payment_p1 PARTITION OF payment FOR VALUES FROM ('2022-01-01') TO ('2022-02-01')",
		"CREATE TABLE payment_p2 PARTITION OF payment FOR VALUES FROM ('2022-02-01') TO ('2022-03-01')",
		"CREATE TABLE payment_p3 PARTITION OF payment FOR VALUES FROM ('2022-03-01') TO ('2022-04-01')",
		"ALTER TABLE payment_p1 ADD CONSTRAINT payment_p1_code_fkey FOREIGN KEY (code, region) REFERENCES customer (code, region)",
		"ALTER TABLE payment_p2 ADD CONSTRAINT payment_p2_code_fkey FOREIGN KEY (code, region) REFERENCES customer (code, region)",
		"ALTER TABLE payment_p3 ADD CONSTRAINT payment_p3_code_fkey FOREIGN KEY (code, region) REFERENCES customer (code, region)",
		"ALTER TABLE payment_p2 ADD CONSTRAINT payment_p2_ref_fkey FOREIGN KEY (ref, region) REFERENCES customer (code, region)"); err != nil {
		return false, "", err
	}
	spec := `{"tables": [{"name": "customer", "tenant_column": "organization_id"}]}`
	out, code := e.checkWith(spec)
	fks := strings.Count(out, "[fk_multicolumn]") // an FK of one unique column is supported (block 4, item 2)
	pk := strings.Contains(out, "[unique_not_tenant_scoped] customer_pkey ")
	js, _ := e.checkWith(spec, "-json")
	var rep struct {
		Findings []struct {
			Code     string   `json:"code"`
			Examples []string `json:"examples"`
			Subject  *struct {
				Table       string   `json:"table"`
				Columns     []string `json:"columns"`
				PartitionOf string   `json:"partition_of"`
				Partitions  []string `json:"partitions"`
			} `json:"subject"`
		} `json:"findings"`
	}
	i := max(strings.Index(js, "{"), 0)
	if err := json.NewDecoder(strings.NewReader(js[i:])).Decode(&rep); err != nil {
		return false, "check -json: " + err.Error(), nil
	}
	var got []string
	for _, f := range rep.Findings {
		if f.Code != "fk_multicolumn" || f.Subject == nil {
			continue
		}
		s := f.Subject
		got = append(got, fmt.Sprintf("%s(%s) partition_of=%q partitions=%v examples=%v",
			s.Table, strings.Join(s.Columns, ","), s.PartitionOf, s.Partitions, f.Examples))
	}
	sort.Strings(got)
	want := `payment(code,region) partition_of="" partitions=[payment_p1 payment_p2 payment_p3] examples=[payment_p1 payment_p2 payment_p3]; ` +
		`payment_p2(ref,region) partition_of="payment" partitions=[] examples=[]`
	// customer_id is customer's key since block 4 item 1: its primary key is pinned
	ok := fks == 2 && !pk && strings.Join(got, "; ") == want
	return ok, fmt.Sprintf("check: %s (exit %d); fk_multicolumn findings: %d; customer_pkey as unique_not_tenant_scoped: %v; subjects: %s",
		verdict(out), code, fks, pk, strings.Join(got, "; ")), nil
}

// X9 (A11, describe). Two documents of organization 1 share a title; CREATE UNIQUE INDEX
// CONCURRENTLY fails on them and leaves the index invalid: it enforces nothing in the cloud.
func checkInvalidIndexMoved(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(), "UPDATE documents SET title = 'dup' WHERE id IN (2, 4)"); err != nil {
		return false, "", err
	}
	failed := execSQL(e.ctx, e.cloud(), "CREATE UNIQUE INDEX CONCURRENTLY documents_title_u ON documents (title)")
	valid, err := one(e.ctx, e.cloud(), "SELECT indisvalid::text FROM pg_index WHERE indexrelid = 'documents_title_u'::regclass")
	if err != nil || failed == nil || valid != "false" {
		return false, fmt.Sprintf("setup: CREATE INDEX CONCURRENTLY error %v, valid %s, %v", failed, valid, err), nil
	}
	if err := e.enable(); err != nil {
		return false, "enable failed: " + lastErr(err.Error()), nil
	}
	mv, moveErr := e.move(1)
	moved := "move-org passed"
	if moveErr != nil {
		moved = "move-org failed: " + lastErr(mv)
	}
	idx, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM pg_indexes WHERE tablename = 'documents' AND indexdef LIKE '%(title)%'")
	dups, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM documents WHERE title = 'dup'")
	return moveErr == nil && idx == "0" && dups == "2", fmt.Sprintf("%s; unique title indexes at the organization: %s; its rows titled dup: %s", moved, idx, dups), nil
}
