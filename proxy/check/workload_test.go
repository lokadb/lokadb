package check

import (
	"reflect"
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// testChecker knows documents (organization_id) and document_versions (via document_id) as LokaDB tables.
func testChecker() *checker {
	types := map[string]string{"id": "bigint", "organization_id": "bigint", "project_id": "bigint", "document_id": "bigint",
		"slug": "text", "title": "text", "content": "text"}
	tbl := func(name string, cols ...string) *proxy.Table {
		t := &proxy.Table{Name: name, TenantCol: "organization_id", Refs: name + "__refs", Cloud: name + "__cloud", Cols: cols, ColType: map[string]string{}}
		for _, c := range cols {
			t.ColType[c] = types[c]
		}
		return t
	}
	return &checker{
		o:   Options{Workload: []string{"recording"}},
		rep: &Report{},
		meta: &proxy.Meta{
			Tables: map[string]*proxy.Table{
				"documents":         tbl("documents", "id", "organization_id", "project_id", "slug", "title"),
				"document_versions": tbl("document_versions", "id", "document_id", "content", "organization_id"),
			},
			Lookups: map[string]map[string]string{
				"documents":         {"organization_id": "", "id": "documents__refs"},
				"document_versions": {"organization_id": "", "id": "document_versions__refs", "document_id": "documents__refs"},
			},
		},
	}
}

func TestClassify(t *testing.T) {
	c := testChecker()
	cases := []struct {
		sql  string
		want kind
	}{
		{`select * from "documents" where "documents"."id" = $1 limit $2`, kRouted},
		{`select * from document_versions where document_id in ($1, $2)`, kRouted},
		{`select * from documents where id = any($1)`, kRouted}, // pgx with a slice
		{`select * from documents where id <> any($1)`, kFanout},
		{`select * from documents where slug = $1`, kFanout},
		{`update documents set title = $1 where project_id = $2`, kFanout},
		{`select count(*) from projects`, kCloud},
		{`insert into documents (organization_id, slug) values ($1, $2) on conflict (organization_id, slug) do update set slug = excluded.slug`, kUpsert},
		{`alter table documents add column status text`, kDDL},
		{`alter table documents add constraint documents_slug_check check (slug <> '')`, kDDLRejected},
		{`copy documents from stdin`, kRouted}, // the proxy loads it through a staging table (block 2); the organization comes with the rows
		{`copy documents from '/tmp/x'`, kForbidden},
		{`declare c cursor for select * from documents where organization_id = $1`, kRouted}, // cursors are tested through the proxy (block 9, CU1-CU3)
		{`declare c cursor for select * from documents`, kFanout},
		{`selec 1`, kParse},
		// pg_stat_statements of PostgreSQL 18 normalizes utility statements into text the parser refuses
		{`savepoint $1`, kCloud},
		{`set search_path = $1`, kCloud},
		// LokaDB's own objects and FDW commands: the proxy refuses them whatever the fanout policy
		{`select * from documents__refs where id = $1`, kForbidden},
		{`select count(*) from lokadb.orgs where org = $1`, kForbidden},
		{`create foreign table x (id int) server lokadb_org1`, kForbidden},
		{`import foreign schema public from server lokadb_org1 into public`, kForbidden},
		{`drop server lokadb_org1 cascade`, kForbidden},
		{`select set_config($2, $1, $3)`, kCloud}, // set_config('lokadb.tenant', ?, true) in pg_stat_statements
		{`select set_config('lokadb.fanout', $1, false)`, kForbidden},
		// the application's own foreign data and tables named like LokaDB's are its business
		{`create foreign table x (id int) server s`, kCloud},
		{`import foreign schema public from server s into public`, kCloud},
		{`select * from documents__archive`, kCloud},
		{`select * from documents_archive`, kCloud},
	}
	for _, tc := range cases {
		if got, _ := c.classify(tc.sql); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.sql, got, tc.want)
		}
	}
}

func TestProxyMade(t *testing.T) {
	c := testChecker()
	for sql, want := range map[string]bool{
		// what the proxy itself runs on the application's connection after enable
		`SELECT id::text, tenant::text FROM "documents__refs" WHERE id IN ($1 /*, ... */)`: true,
		`SELECT org::text, schema FROM lokadb.orgs`:                                        true,
		`SELECT org::text, profile FROM lokadb.orgs WHERE profile IS NOT NULL`:             true,
		`SELECT org::text, db_id FROM lokadb.orgs WHERE db_id IS NOT NULL`:                 true,
		`SELECT coalesce((SELECT profile FROM lokadb.orgs WHERE org = $1), $2), (coalesce(has_table_privilege(to_regclass($3), $4), $5))::text, (to_regclass($6) IS NOT NULL)::text`: true,
		`SELECT current_setting($1)`: true,
		`SELECT fingerprint || CASE WHEN named THEN $1 ELSE $2 END FROM lokadb.direct_learnt ORDER BY at DESC LIMIT $3`:                                                                                          true,
		`SELECT fingerprint FROM lokadb.direct_cloud_first ORDER BY at DESC LIMIT $1`:                                                                                                                            true,
		`INSERT INTO lokadb.direct_cloud_first (fingerprint, reason) SELECT $1, $2 WHERE (SELECT count(*) FROM lokadb.direct_cloud_first) < $3 ON CONFLICT DO NOTHING`:                                           true,
		`WITH lokadb_k AS (SELECT nextval($1::regclass) AS id FROM generate_series($2, $3)), lokadb_r AS (INSERT INTO "documents__refs" (id, tenant) SELECT id, $4 FROM lokadb_k) SELECT id::text FROM lokadb_k`: true,
		`DELETE FROM "documents__refs" AS lokadb_d WHERE id IN ($1, $2)`:                                                                                                                                         true,
		`SELECT id::text, coalesce(tenant::text, $1) FROM "documents__refs" WHERE id IN ($2)`:                                                                                                                    true,
		`UPDATE "documents__org1" documents SET title = $1 WHERE documents.id = $2`:                                                                                                                              true,
		`DELETE FROM "document_versions__org12" WHERE id = $1`:                                                                                                                                                   true,
		`SELECT pg_advisory_lock(hashtext($1), hashtext($2))`:                                                                                                                                                    true,
		`SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2::text))`:                                                                                                                                         true,
		`SELECT lokadb.grant_sync($1, ARRAY[$2]::text[])`:                                                                                                                                                        true,
		`SELECT set_config('lokadb.org', $1, false)`:                                                                                                                                                             true,
		`DO $lokadb$ BEGIN RAISE EXCEPTION USING MESSAGE = 'x'; END $lokadb$`:                                                                                                                                    true,
		"SELECT string_agg(c.relname, $1) FROM pg_class c\n\t\tWHERE c.oid = ANY(ARRAY[$2]::regclass[]) AND NOT pg_has_role(current_user, c.relowner, $3)":                                                       true,
		`SELECT table_name, tenant_column, refs_table, cloud_partition FROM lokadb.tables`:                                                                                                                       true,
		`SELECT table_name, tenant_column, refs_table, cloud_partition, coalesce(to_jsonb(t) ->> 'key_column', 'id') FROM lokadb.tables t`:                                                                       true,
		`SELECT table_name, column_name, coalesce(refs_table, $1) FROM lokadb.lookups`:                                                                                                                           true,
		`SELECT c.relname, a.attname FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN lokadb.tables t ON t.table_name = c.relname`:                                                                 true,
		`SELECT ic.relname, t.table_name FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class c ON c.oid = i.indrelid LEFT JOIN pg_inherits h ON h.inhrelid = c.oid LEFT JOIN pg_class p ON p.oid = h.inhparent JOIN lokadb.tables t ON t.table_name = c.relname OR t.table_name = p.relname`: true,
		// what Postgres runs on its own: deferred FK checks at COMMIT, move-org's initial copy
		`SELECT $2 FROM ONLY "public"."document_versions__cloud" x WHERE $1 OPERATOR(pg_catalog.=) "document_id" FOR KEY SHARE OF x`: true,
		`COPY public.documents__o1 (id, organization_id, slug) TO STDOUT`:                                                            true,
		`COPY (SELECT org, token FROM ONLY lokadb.move_marker WHERE (org = 1)) TO STDOUT`:                                            true,
		// a SELECT of one organization aimed at its foreign partition (block 2)
		`SELECT status, count(*) FROM "documents__org1" documents WHERE documents.organization_id = $1 GROUP BY status`: true,
		// … joined with a cloud table (Django's select_related)
		`SELECT documents.id, p.name FROM "documents__org1" documents JOIN projects p ON p.id = documents.project_id WHERE documents.organization_id = $1`: true,
		`SELECT p.name, d.id FROM projects p, "documents__org1" d WHERE d.organization_id = $1`:                                                            true,
		// a cloud organization's upsert in its local partition (block 2)
		`INSERT INTO "documents__cloud" AS documents (organization_id, slug) VALUES ($1, $2) ON CONFLICT (organization_id, slug) DO UPDATE SET slug = excluded.slug WHERE documents.organization_id = $3`: true,
		`INSERT INTO "documents__o3" AS documents (organization_id, slug) VALUES ($1, $2) ON CONFLICT (organization_id, slug) DO NOTHING`:                                                                 true,
		// the application's own statements stay
		`insert into documents__cloud (organization_id, slug) values ($1, $2)`: false,
		`select * from projects__org1`:                                         false,
		`delete from documents__archive where id = $1`:                         false,
		`update projects__org1 set name = $1`:                                  false,
		`select * from documents where id = $1`:                                false,
		`SELECT id, tenant FROM documents__refs`:                               false,
		`select * from lokadb.orgs`:                                            false,
		`select t.* from lokadb.tables t join documents d on true`:             false,
		`copy documents__o1 from stdin`:                                        false,
		`copy (select * from documents__o1 join projects on true) to stdout`:   false,
	} {
		if got := c.notApplication(sql); got != want {
			t.Errorf("%s: notApplication %v, want %v", sql, got, want)
		}
	}
}

func TestFanoutKind(t *testing.T) {
	c := testChecker()
	cfg := proxy.Config{Fanout: map[string]proxy.FanoutPolicy{"app": {Mode: proxy.FanoutRejectWrites,
		Allow: []string{"delete from documents where slug like 'tmp%'"}}}}
	var err error
	if c.fan, err = cfg.FanoutFor("app"); err != nil {
		t.Fatal(err)
	}
	for sql, want := range map[string]kind{
		`select * from documents where slug = $1`:         kFanout,  // a read: runs everywhere
		`update documents set title = $1 where slug = $2`: kRefused, // a write: refused
		`delete from documents where slug like $1`:        kAllowed, // reviewed
	} {
		if got := c.fanoutKind(sql); got != want {
			t.Errorf("%s: got %d, want %d", sql, got, want)
		}
	}
}

func TestWrites(t *testing.T) {
	c := testChecker()
	cases := []struct {
		sql           string
		lokadb, cloud []string
	}{
		{`insert into documents (slug) values ($1)`, []string{"documents"}, nil},
		{`with d as (delete from documents where id = $1 returning project_id) update projects set n = n - 1 where id in (select project_id from d)`,
			[]string{"documents"}, []string{"projects"}},
		{`select * from documents join projects on projects.id = documents.project_id`, nil, nil},
		{`insert into audit.log (msg) values ($1)`, nil, []string{"audit.log"}},
		{`update pg_catalog.pg_class set relname = relname where false`, nil, nil},
	}
	for _, tc := range cases {
		w := c.writes(tc.sql)
		if b, cl := keys(w.lokadb), keys(w.cloud); !reflect.DeepEqual(b, tc.lokadb) || !reflect.DeepEqual(cl, tc.cloud) {
			t.Errorf("%s: got lokadb %v cloud %v, want %v %v", tc.sql, b, cl, tc.lokadb, tc.cloud)
		}
	}
}

func TestMixedTransactions(t *testing.T) {
	c := testChecker()
	c.transactions([]*proxy.RecordedTx{
		{Statements: []string{"insert into documents (organization_id, slug) values ($1, $2)", "insert into document_versions (document_id, content) values ($1, $2)"}, Count: 50},
		{Statements: []string{"insert into documents (organization_id, slug) values ($1, $2)", "update projects set updated_at = now() where id = $1"}, Count: 7},
		{Statements: []string{"select * from documents where id = $1", "update projects set updated_at = now() where id = $1"}, Count: 3},
	})
	s := c.rep.Summary
	if !s.TxPatternsChecked || s.TxPatterns != 2 || s.MixedTxPatterns != 1 {
		t.Fatalf("summary %+v, want 2 shapes writing LokaDB tables, 1 of them mixed", s)
	}
	if len(c.rep.Findings) != 1 || c.rep.Findings[0].Code != "mixed_write_transaction" || c.rep.Findings[0].Object != "projects + documents" ||
		!strings.Contains(c.rep.Findings[0].Message, "7 transactions seen") {
		t.Fatalf("findings %+v", c.rep.Findings)
	}
}

// railsChecker: conversations and messages, both with account_id; messages.conversation_id has no FK.
func railsChecker() *checker {
	tbl := func(name string, cols ...string) *proxy.Table {
		t := &proxy.Table{Name: name, TenantCol: "account_id", Refs: name + "__refs", Cloud: name + "__cloud", Cols: cols, ColType: map[string]string{}}
		for _, c := range cols {
			t.ColType[c] = "bigint"
		}
		return t
	}
	return &checker{
		o:    Options{Workload: []string{"recording"}},
		rep:  &Report{},
		spec: admin.Spec{Tables: []admin.SpecTable{{Name: "conversations", TenantColumn: "account_id"}, {Name: "messages", TenantColumn: "account_id"}}},
		fan:  &proxy.Fanout{Mode: proxy.FanoutReject},
		meta: &proxy.Meta{
			Tables: map[string]*proxy.Table{
				"conversations": tbl("conversations", "id", "account_id", "inbox_id"),
				"messages":      tbl("messages", "id", "account_id", "conversation_id", "sender_id"),
			},
			Lookups: map[string]map[string]string{
				"conversations": {"account_id": "", "id": "conversations__refs"},
				"messages":      {"account_id": "", "id": "messages__refs"},
			},
		},
	}
}

// A declared relation (the patch, as -suggest writes it) routes the feed; the suggestions
// themselves are check/ext's (its tests).
func TestPatchRelation(t *testing.T) {
	feed := `SELECT "messages".* FROM "messages" WHERE "messages"."conversation_id" = $1`
	c := railsChecker()
	c.o.Patch = &admin.Routing{Relations: []admin.Relation{{From: "messages.conversation_id", To: "conversations.id"}}}
	c.relations()
	if k, _ := c.classify(feed); k != kRouted {
		t.Errorf("feed after the patch: kind %d", k)
	}
	// a relation to a table that is not LokaDB is a spec error
	c = railsChecker()
	c.o.Patch = &admin.Routing{Relations: []admin.Relation{{From: "messages.sender_id", To: "users.id"}}}
	c.relations()
	if !hasCode(c.rep.Findings, "routing_invalid") {
		t.Errorf("findings: %+v", c.rep.Findings)
	}
}

func hasCode(fs []Finding, code string) bool {
	for _, f := range fs {
		if f.Code == code {
			return true
		}
	}
	return false
}

func TestPatchCloudTable(t *testing.T) {
	feed := `select m.* from messages m join conversations c on c.id = m.conversation_id join inboxes i on i.id = c.inbox_id where i.account_id = $1 and m.sender_id = $2`
	c := railsChecker()
	if k, _ := c.classify(feed); k != kFanout {
		t.Errorf("before the patch: kind %d", k)
	}
	c.o.Patch = &admin.Routing{CloudTables: []admin.CloudTable{{Table: "inboxes", TenantColumn: "account_id"}}}
	c.relations()
	if k, _ := c.classify(feed); k != kRouted {
		t.Errorf("after the patch: kind %d", k)
	}
	if k, _ := c.classify(`select * from inboxes where account_id = 1`); k != kCloud {
		t.Errorf("a cloud table alone: kind %d", k)
	}
}

func TestKeyPatch(t *testing.T) {
	c := railsChecker()
	c.meta.Tables["conversations"].ColType["uuid"] = "text"
	byToken := `select * from conversations where uuid = $1`
	if k, _ := c.classify(byToken); k != kFanout {
		t.Fatalf("before: kind %d", k)
	}
	c.o.Patch = &admin.Routing{Keys: []admin.Key{{Table: "conversations", Column: "uuid"}}}
	c.relations()
	if k, _ := c.classify(byToken); k != kRouted {
		t.Errorf("after the patch: kind %d", k)
	}
	// a new row's own key names no organization: its other columns do
	if a := proxy.Analyze(`insert into conversations (account_id, uuid) values ($1, $2)`, c.meta); a.Unhinted || len(a.Hints) != 1 || a.Hints[0].Column != "account_id" {
		t.Errorf("insert: %+v", a.Hints)
	}
}

// A polymorphic reference (taggable_type next to taggable_id) is not taken (nor suggested:
// check/ext's test).
func TestPolymorphic(t *testing.T) {
	c := railsChecker()
	c.meta.Tables["messages"].ColType["conversation_type"] = "text"
	c.o.Patch = &admin.Routing{Relations: []admin.Relation{{From: "messages.conversation_id", To: "conversations.id"}}}
	c.relations()
	if !hasCode(c.rep.Findings, "routing_invalid") {
		t.Errorf("taken: %+v", c.rep.Findings)
	}
}

func TestDDLThenWrite(t *testing.T) {
	c := railsChecker()
	c.ddlWriteTransactions([]*proxy.RecordedTx{
		// Django AddField + RunPython in one transaction
		{Statements: []string{"alter table messages add column kind text", "update messages set kind = $1 where account_id = $2"}, Count: 4},
		// the write before the DDL works against the old schema; DDL alone and a write alone are fine
		{Statements: []string{"update messages set sender_id = $1 where id = $2", "alter table messages add column other text"}, Count: 1},
		{Statements: []string{"alter table conversations add column x text"}, Count: 1},
		{Statements: []string{"update conversations set inbox_id = $1 where id = $2"}, Count: 9},
		// another table's write is not the changed table's
		{Statements: []string{"alter table conversations rename column x to y", "update messages set kind = $1 where account_id = $2"}, Count: 2},
	})
	if len(c.rep.Findings) != 1 || c.rep.Findings[0].Code != "ddl_then_write_transaction" || c.rep.Findings[0].Object != "messages" ||
		c.rep.Findings[0].Severity != Warning || !strings.Contains(c.rep.Findings[0].Message, "4 transactions seen") {
		t.Fatalf("findings %+v", c.rep.Findings)
	}
}

// Block 10: a pattern the application runs after naming the organization (SET LOCAL
// lokadb.tenant) is routed; the same pattern seen once without the name stays a fanout.
func TestClassifyNamed(t *testing.T) {
	c := testChecker()
	const q = `select * from documents where slug = $1`
	tx := func(stmts ...string) *proxy.RecordedTx { return &proxy.RecordedTx{Statements: stmts, Count: 1} }
	named := namedOnly([]*proxy.RecordedTx{tx(`SET LOCAL lokadb.tenant = '<organization>'`, q)})
	if k, _ := c.classifyNamed(q, named); k != kRouted {
		t.Errorf("after SET LOCAL: %v, want routed", k)
	}
	named = namedOnly([]*proxy.RecordedTx{tx(`SET LOCAL lokadb.tenant = '<organization>'`, q), tx(q)})
	if k, _ := c.classifyNamed(q, named); k != kFanout {
		t.Errorf("also without it: %v, want fanout", k)
	}
}

// The target of a relation is the table's key, not always id (Pagila: customer_id).
func TestRelationToKey(t *testing.T) {
	c := railsChecker()
	c.meta.Tables["conversations"].Key = "conversation_no"
	c.o.Patch = &admin.Routing{Relations: []admin.Relation{{From: "messages.conversation_id", To: "conversations.id"}}}
	c.relations()
	if !hasCode(c.rep.Findings, "routing_invalid") {
		t.Errorf("the wrong key: %+v", c.rep.Findings)
	}
}

// Z3 of block 14's trial: a COPY FROM into a LokaDB table is routed, and its cost is told.
func TestCopyFromCost(t *testing.T) {
	c := testChecker()
	c.copyFromCost([]*group{
		{kind: kRouted, sql: `copy documents (organization_id, title) from stdin`, count: 3},
		{kind: kRouted, sql: `select * from documents where id = $1`, count: 9},
		{kind: kRouted, sql: `copy (select * from documents where organization_id = $1) to stdout`, count: 2},
	})
	if len(c.rep.Findings) != 1 || c.rep.Findings[0].Code != "copy_from_cost" || c.rep.Findings[0].Severity != Info ||
		len(c.rep.Findings[0].Patterns) != 1 || c.rep.Findings[0].Patterns[0].Calls != 3 || !strings.Contains(c.rep.Findings[0].Message, "fdw+direct") {
		t.Fatalf("findings %+v", c.rep.Findings)
	}
	c = testChecker()
	c.copyFromCost([]*group{{kind: kRouted, sql: `select * from documents where id = $1`, count: 9}})
	if len(c.rep.Findings) != 0 {
		t.Fatalf("no COPY FROM: findings %+v", c.rep.Findings)
	}
}
