package proxy

import (
	"reflect"
	"strings"
	"testing"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

func testMeta() *Meta {
	cols := func(names ...string) (map[string]string, []string) {
		m := map[string]string{}
		for _, n := range names {
			m[n] = "bigint"
		}
		m["title"], m["slug"], m["content"] = "character varying(255)", "character varying(255)", "text"
		return m, names
	}
	dt, dc := cols("id", "organization_id", "project_id", "slug", "title")
	vt, vc := cols("id", "document_id", "content", "organization_id")
	return &Meta{
		Tables: map[string]*Table{
			"documents":         {Name: "documents", TenantCol: "organization_id", Refs: "documents__refs", Cloud: "documents__cloud", Cols: dc, ColType: dt},
			"document_versions": {Name: "document_versions", TenantCol: "organization_id", Refs: "document_versions__refs", Cloud: "document_versions__cloud", Cols: vc, ColType: vt},
		},
		Lookups: map[string]map[string]string{
			"documents":         {"organization_id": "", "id": "documents__refs"},
			"document_versions": {"organization_id": "", "id": "document_versions__refs", "document_id": "documents__refs"},
		},
	}
}

func TestHints(t *testing.T) {
	m := testMeta()
	cases := []struct {
		name, sql string
		lokadb    bool
		unhinted  bool
		hints     []Hint
	}{
		{"laravel find", `select * from "documents" where "documents"."id" = $1 limit 1`, true, false,
			[]Hint{{"documents", "id", []ValueRef{{Param: 1}}}}},
		{"laravel eager in", `select * from "document_versions" where "document_versions"."document_id" in (1, 2, 3)`, true, false,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Const: "1"}, {Const: "2"}, {Const: "3"}}}}},
		{"gorm unqualified", `SELECT * FROM "documents" WHERE organization_id = $1 ORDER BY id`, true, false,
			[]Hint{{"documents", "organization_id", []ValueRef{{Param: 1}}}}},
		{"django join", `SELECT "d"."id" FROM "documents" "d" INNER JOIN "projects" ON ("d"."project_id" = "projects"."id") WHERE "d"."organization_id" = 2`, true, false,
			[]Hint{{"documents", "organization_id", []ValueRef{{Const: "2"}}}}},
		{"no tenant", `select * from documents where slug = 'a-1'`, true, true, nil},
		{"or is not a hint", `select * from documents where id = 1 or id = 2`, true, true, nil},
		{"insert child", `insert into "document_versions" ("content", "document_id") values ($1, $2) returning "id"`, true, false,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Param: 2}}}}},
		{"django unnest insert", `INSERT INTO "documents" ("organization_id", "slug") SELECT * FROM UNNEST(('{2,2}'::int2[])::bigint[], (ARRAY['a', 'b'])::varchar(100)[])`, true, false,
			[]Hint{{"documents", "organization_id", []ValueRef{{Const: "2"}, {Const: "2"}}}}},
		{"cloud only", `select * from projects where id = 1`, false, false, nil},
		{"subquery unhinted", `select * from projects where id in (select project_id from documents)`, true, true, nil},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.ParseError || a.Lokadb != c.lokadb || a.Unhinted != c.unhinted || !reflect.DeepEqual(a.Hints, c.hints) {
			t.Errorf("%s: got lokadb=%v unhinted=%v hints=%+v parseErr=%v", c.name, a.Lokadb, a.Unhinted, a.Hints, a.ParseError)
		}
	}
}

func TestDDL(t *testing.T) {
	m := testMeta()
	a := analyze(`alter table "documents" add column "status" varchar(255) not null default 'draft'`, m)
	if len(a.DDL) != 1 || a.DDLError != "" {
		t.Fatalf("add column: %+v", a)
	}
	a = analyze(`create unique index "documents_title_unique" on "documents" ("title")`, m)
	if len(a.DDL) != 1 || a.Rewritten != `SELECT lokadb.create_index('documents', 'CREATE UNIQUE INDEX __lokadb_idx__ ON __lokadb_part__ USING btree (title)', 'documents_title_unique')` {
		t.Fatalf("unique index: %+v", a)
	}
	a = analyze(`alter table documents add constraint fk foreign key (project_id) references projects(id)`, m)
	if a.DDLError == "" {
		t.Fatalf("constraint should be rejected")
	}
	a = analyze(`BEGIN; ALTER TABLE documents ADD COLUMN x int; COMMIT`, m)
	if !a.Commit || len(a.DDL) != 1 {
		t.Fatalf("multi: %+v", a)
	}
}

func TestTarget(t *testing.T) {
	m := testMeta()
	for _, c := range []struct{ sql, want string }{
		// GORM qualifies columns with the table name: the alias keeps them valid
		{`UPDATE "documents" SET "title"=$1 WHERE "documents"."id" = $2 RETURNING "id"`,
			`UPDATE "documents__org1" documents SET title = $1 WHERE documents.id = $2 AND documents.organization_id = 1 RETURNING id`},
		// Laravel
		{`delete from "documents" where "id" = $1`,
			`DELETE FROM "documents__org1" documents WHERE id = $1 AND documents.organization_id = 1`},
		{`update documents d set title = 'x' where d.organization_id = 1 and d.id = 2`,
			`UPDATE "documents__org1" d SET title = 'x' WHERE (d.organization_id = 1 AND d.id = 2) AND d.organization_id = 1`},
		// Prisma and SQLAlchemy qualify columns with the schema (P1)
		{`UPDATE "public"."documents" SET "title" = $1 WHERE ("public"."documents"."id" = $2 AND 1=1) RETURNING "public"."documents"."id", "public"."documents"."title"`,
			`UPDATE "documents__org1" documents SET title = $1 WHERE (documents.id = $2 AND 1 = 1) AND documents.organization_id = 1 RETURNING documents.id, documents.title`},
		{`DELETE FROM "public"."documents" WHERE ("public"."documents"."id" IN ($1,$2) AND 1=1)`,
			`DELETE FROM "documents__org1" documents WHERE (documents.id IN ($1, $2) AND 1 = 1) AND documents.organization_id = 1`},
		// moving a row to another organization goes through the parent
		{`update documents set organization_id = 2 where id = 5`, ""},
		{`update documents set title = p.name from projects p where p.id = documents.project_id`, ""},
		{`delete from documents where id in (select document_id from document_versions)`, ""},
		{`update documents set title = 'x' where id = 1; select 1`, ""},
		// a SELECT of one table: GROUP BY, aggregates, ORDER BY … LIMIT reach the organization's database
		{`select * from documents where id = 1`,
			`SELECT documents.id, documents.organization_id, documents.project_id, documents.slug, documents.title FROM "documents__org1" documents WHERE id = 1 AND documents.organization_id = 1`},
		{`SELECT "status", count(*) FROM "public"."documents" WHERE "public"."documents"."organization_id" = $1 GROUP BY "status" ORDER BY 2 DESC LIMIT 5`,
			`SELECT status, count(*) FROM "documents__org1" documents WHERE documents.organization_id = $1 AND documents.organization_id = 1 GROUP BY status ORDER BY 2 DESC LIMIT 5`},
		{`select d.id from documents d where d.organization_id = 1 and d.user_id in (select id from users where name = 'x') for update`,
			`SELECT d.id FROM "documents__org1" d WHERE (d.organization_id = 1 AND d.user_id IN (SELECT id FROM users WHERE name = 'x')) AND d.organization_id = 1 FOR UPDATE`},
		{`select * from documents d join users u on u.id = d.user_id where d.organization_id = 1`, ""}, // a bare * would come out in the partition's order
		// a join with cloud tables (Django's select_related): the partition takes the table's place
		{`SELECT "documents"."id", "documents"."title", "projects"."name" FROM "documents" INNER JOIN "projects" ON ("documents"."project_id" = "projects"."id") WHERE "documents"."organization_id" = $1 ORDER BY "documents"."id" DESC LIMIT 20`,
			`SELECT documents.id, documents.title, projects.name FROM "documents__org1" documents JOIN projects ON documents.project_id = projects.id WHERE documents.organization_id = $1 AND documents.organization_id = 1 ORDER BY documents.id DESC LIMIT 20`},
		{`select d.*, u.name from users u, documents d where u.id = d.user_id and d.organization_id = 1`,
			`SELECT d.id, d.organization_id, d.project_id, d.slug, d.title, u.name FROM users u, "documents__org1" d WHERE (u.id = d.user_id AND d.organization_id = 1) AND d.organization_id = 1`},
		{`select d.id, p.name from documents d left join projects p on p.id = d.project_id where d.organization_id = 1`,
			`SELECT d.id, p.name FROM "documents__org1" d LEFT JOIN projects p ON p.id = d.project_id WHERE d.organization_id = 1 AND d.organization_id = 1`},
		{`select d.id, p.name from projects p right join documents d on p.id = d.project_id where d.organization_id = 1`,
			`SELECT d.id, p.name FROM projects p RIGHT JOIN "documents__org1" d ON p.id = d.project_id WHERE d.organization_id = 1 AND d.organization_id = 1`},
		{`select d.id, p.name from (projects p join users u on u.id = p.id) join documents d on d.project_id = p.id where d.organization_id = 1`,
			`SELECT d.id, p.name FROM projects p JOIN users u ON u.id = p.id JOIN "documents__org1" d ON d.project_id = p.id WHERE d.organization_id = 1 AND d.organization_id = 1`},
		{`select d.id, x.n from documents d join (select id, name as n from projects) x on x.id = d.project_id where d.organization_id = 1`,
			`SELECT d.id, x.n FROM "documents__org1" d JOIN (SELECT id, name AS n FROM projects) x ON x.id = d.project_id WHERE d.organization_id = 1 AND d.organization_id = 1`},
		// the organization in ON: the statement's own condition stays, lokadb's comes in WHERE
		{`select d.id, p.name from documents d join projects p on p.id = d.project_id and d.organization_id = 1`,
			`SELECT d.id, p.name FROM "documents__org1" d JOIN projects p ON p.id = d.project_id AND d.organization_id = 1 WHERE d.organization_id = 1`},
		// on the nullable side the condition in WHERE would drop the rows the join keeps: the parent
		{`select p.name, d.id from projects p left join documents d on d.project_id = p.id and d.organization_id = 1`, ""},
		{`select p.name, d.id from documents d right join projects p on d.project_id = p.id and d.organization_id = 1`, ""},
		{`select p.name, d.id from documents d full join projects p on d.project_id = p.id where d.organization_id = 1`, ""},
		{`select p.name, d.id from projects p left join (users u join documents d on d.user_id = u.id) on d.project_id = p.id where d.organization_id = 1`, ""},
		// a join with an alias hides the table's name; a LokaDB table in a subquery of the join; two LokaDB tables
		{`select j.id from (documents d join projects p on p.id = d.project_id) j where j.organization_id = 1`, ""},
		{`select p.name, x.id from projects p join (select id, project_id from documents where organization_id = 1) x on x.project_id = p.id`, ""},
		{`select d.id, v.id from documents d join document_versions v on v.document_id = d.id where d.organization_id = 1`, ""},
		{`select d.id, p.name from documents d join projects p on p.id = d.project_id where d.organization_id = 1 and p.id in (select project_id from document_versions)`, ""},
		// column aliases rename the organization column: through the parent, alone or in a join
		{`select * from documents d(i, o) where i = 1`, ""},
		{`select d.i, p.name from documents d(i, o) join projects p on p.id = d.i where d.o = 1`, ""},
		{`select id from documents where organization_id = 1 union select id from documents where organization_id = 1`, ""},
		{`select * into tmp from documents where organization_id = 1`, ""},
		{`with x as (select 1) select * from documents where organization_id = 1`, ""},
		{`select * from document_versions where document_id in (select id from documents where organization_id = 1)`, ""},
	} {
		a := analyze(c.sql, m)
		got := ""
		if a.targetText() != "" {
			got = a.targetSQL("documents__org1", "1")
			if _, err := pg.Parse(got); err != nil {
				t.Errorf("%s: target SQL does not parse: %v", c.sql, err)
			}
		}
		if got != c.want {
			t.Errorf("%s\n got: %s\nwant: %s", c.sql, got, c.want)
		}
	}
}

func TestUpsertRewrite(t *testing.T) {
	m := testMeta()
	for _, sql := range []string{
		// Laravel upsert
		`insert into "documents" ("organization_id", "project_id", "slug", "title") values ($1, $2, $3, $4), ($5, $6, $7, $8) on conflict ("organization_id", "slug") do update set "title" = "excluded"."title"`,
		// Django bulk_create(update_conflicts=True)
		`INSERT INTO "documents" ("organization_id", "project_id", "slug", "title") VALUES (1, 10, 'a-1', 'x') ON CONFLICT("organization_id", "slug") DO UPDATE SET "title" = EXCLUDED."title" RETURNING "documents"."id"`,
		// Django 5.2+ multi-row bulk_create(update_conflicts=True)
		`INSERT INTO "documents" ("organization_id", "project_id", "slug", "title") SELECT * FROM UNNEST(('{1,1}'::int2[])::bigint[], ('{1,1}'::int2[])::bigint[], (ARRAY['a-1', 'a-u'])::varchar(100)[], (ARRAY['x', 'y'])::varchar(200)[]) ON CONFLICT("organization_id", "slug") DO UPDATE SET "title" = EXCLUDED."title" RETURNING "documents"."id"`,
		// GORM upsert
		`INSERT INTO "documents" ("organization_id","project_id","slug","title") VALUES ($1,$2,$3,$4) ON CONFLICT ("organization_id","slug") DO UPDATE SET "title"="excluded"."title" RETURNING "id"`,
		// GORM association save: conflict on a generated id
		`INSERT INTO "document_versions" ("document_id","content") VALUES ($1,$2) ON CONFLICT ("id") DO UPDATE SET "document_id"="excluded"."document_id" RETURNING "id"`,
		// ON CONFLICT (cols) DO NOTHING
		`insert into documents (organization_id, project_id, slug, title) values (1, 10, 's', 't') on conflict (organization_id, slug) do nothing returning *`,
	} {
		a := analyze(sql, m)
		if a.Rewritten == "" {
			t.Errorf("not rewritten: %s", sql)
			continue
		}
		if _, err := pg.Parse(a.Rewritten); err != nil {
			t.Errorf("rewritten SQL does not parse: %v\n%s", err, a.Rewritten)
		}
		t.Logf("\nIN:  %s\nOUT: %s", sql, a.Rewritten)
	}
	if a := analyze(`insert into documents (organization_id, slug) values (1, 'x') on conflict do nothing`, m); a.Rewritten != "" {
		t.Errorf("plain DO NOTHING must pass through")
	}
}

func TestUpsertCastAndWhere(t *testing.T) {
	m := testMeta()
	a := analyze(`insert into documents (id, organization_id, title) values ($1, $2, $3) on conflict (id) do update set title = excluded.title`, m)
	if !strings.Contains(a.Rewritten, "CAST($3 AS character varying)") || strings.Contains(a.Rewritten, "varying(255)") {
		t.Errorf("varchar(255) must be cast without its length (an explicit cast cuts long values):\n%s", a.Rewritten)
	}
	a = analyze(`insert into documents (id, organization_id, title) values (1, 1, 'x') on conflict (id) do update set title = excluded.title where documents.title <> 'keep'`, m)
	if !strings.Contains(a.Rewritten, `hit AS (SELECT v.__lokadb_ord FROM v WHERE EXISTS (SELECT 1 FROM "documents" x WHERE x."id" = v."id" AND `) {
		t.Errorf("with DO UPDATE WHERE the insert must skip every existing key, updated or not:\n%s", a.Rewritten)
	}
	for typ, want := range map[string]string{
		"character varying(255)": "character varying", "character(3)": "character", "character varying(20)[]": "character varying[]",
		"bit(8)": "bit", "numeric(10,2)": "numeric(10,2)", "timestamp(3) without time zone": "timestamp(3) without time zone", "text": "text",
	} {
		if got := castType(typ); got != want {
			t.Errorf("castType(%q) = %q, want %q", typ, got, want)
		}
	}
}

// Prisma's upsert names columns through the schema in WHERE and RETURNING (P2): the
// rewritten statement names the table by an alias, the references must follow it.
func TestUpsertSchemaRefs(t *testing.T) {
	m := testMeta()
	for _, sql := range []string{
		`INSERT INTO "public"."documents" ("id","organization_id","title") VALUES ($1,$2,$3) ON CONFLICT ("id") DO UPDATE SET "title" = $4 WHERE ("public"."documents"."id" = $5 AND 1=1) RETURNING "public"."documents"."id", "public"."documents"."title"`,
		`INSERT INTO "public"."documents" ("id","organization_id","title") VALUES ($1,$2,$3) ON CONFLICT ("id") DO UPDATE SET "title" = "public"."documents"."title" || EXCLUDED."title" RETURNING "public"."documents"."id"`,
		`INSERT INTO "public"."documents" ("id","organization_id","title") VALUES ($1,$2,$3) ON CONFLICT ("id") DO NOTHING RETURNING "public"."documents"."id"`,
	} {
		a := analyze(sql, m)
		if a.Rewritten == "" {
			t.Errorf("not rewritten: %s", sql)
			continue
		}
		if strings.Contains(a.Rewritten, `"public"`) || strings.Contains(a.Rewritten, "public.") {
			t.Errorf("a reference through the schema is left:\n%s", a.Rewritten)
		}
		if _, err := pg.Parse(a.Rewritten); err != nil {
			t.Errorf("rewritten SQL does not parse: %v\n%s", err, a.Rewritten)
		}
	}
}

// P13: the upsert's own WITH is kept; RETURNING pairs the inserted rows with v by position,
// so a row with a NULL key (it never conflicts) is returned too.
func TestUpsertWithAndReturning(t *testing.T) {
	m := testMeta()
	a := analyze(`WITH src AS (SELECT 1::bigint AS o, 'a'::text AS s) INSERT INTO documents (organization_id, slug, title) SELECT o, s, 'x' FROM src ON CONFLICT (organization_id, slug) DO UPDATE SET title = excluded.title`, m)
	if !strings.HasPrefix(a.Rewritten, `WITH src AS (SELECT 1::bigint AS o, 'a'::text AS s), v AS (`) {
		t.Errorf("the statement's WITH must come first:\n%s", a.Rewritten)
	}
	a = analyze(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 3) INSERT INTO documents (organization_id, slug) SELECT 1, x::text FROM n ON CONFLICT (organization_id, slug) DO NOTHING`, m)
	if !strings.HasPrefix(a.Rewritten, `WITH RECURSIVE n(x) AS (`) {
		t.Errorf("WITH RECURSIVE must stay recursive:\n%s", a.Rewritten)
	}
	if a := analyze(`WITH v AS (SELECT 1) INSERT INTO documents (organization_id, slug) VALUES (1, 'a') ON CONFLICT (organization_id, slug) DO NOTHING`, m); a.Rewritten != "" {
		t.Errorf("a WITH query named like the rewrite's own is not rewritten:\n%s", a.Rewritten)
	}
	a = analyze(`INSERT INTO documents (organization_id, slug, title) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (organization_id, slug) DO UPDATE SET title = excluded.title RETURNING id`, m)
	for _, want := range []string{`f AS (SELECT * FROM v WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.__lokadb_ord = v.__lokadb_ord))`, `ON i.__lokadb_rn = f.__lokadb_rn`} {
		if !strings.Contains(a.Rewritten, want) {
			t.Errorf("RETURNING must pair by position (%s):\n%s", want, a.Rewritten)
		}
	}
	for _, s := range []string{a.Rewritten} {
		if _, err := pg.Parse(s); err != nil {
			t.Errorf("does not parse: %v\n%s", err, s)
		}
	}
}

// Item 9 (a): a cloud organization's upsert runs natively in its partition, DO UPDATE kept
// to the organization (DEFAULT holds every cloud organization and has no policy).
func TestLocalUpsert(t *testing.T) {
	m := testMeta()
	for _, c := range []struct{ sql, want string }{
		{`insert into "public"."documents" ("organization_id", "slug", "title") values ($1, $2, $3) on conflict ("organization_id", "slug") do update set "title" = "excluded"."title" returning "public"."documents"."id"`,
			`INSERT INTO "documents__cloud" AS documents (organization_id, slug, title) VALUES ($1, $2, $3) ON CONFLICT (organization_id, slug) DO UPDATE SET title = excluded.title WHERE documents.organization_id = 2 RETURNING documents.id`},
		{`insert into documents as d (organization_id, slug, title) values (2, 's', 't') on conflict (slug) do update set title = d.title || excluded.title where d.title <> 'keep'`,
			`INSERT INTO "documents__cloud" AS d (organization_id, slug, title) VALUES (2, 's', 't') ON CONFLICT (slug) DO UPDATE SET title = d.title || excluded.title WHERE d.title <> 'keep' AND d.organization_id = 2`},
		{`insert into documents (organization_id, slug) values (2, 's') on conflict (organization_id, slug) do nothing`,
			`INSERT INTO "documents__cloud" AS documents (organization_id, slug) VALUES (2, 's') ON CONFLICT (organization_id, slug) DO NOTHING`},
		// a subquery over a LokaDB table needs the parent
		{`insert into documents (organization_id, slug) select organization_id, slug from documents where organization_id = 2 on conflict (organization_id, slug) do nothing`, ""},
		// the organization found from the parent's key, not from the row's own column: the
		// parent's policy has to check the row
		{`insert into document_versions (id, document_id, content) values (1, 5, 'x') on conflict (id) do update set content = excluded.content`, ""},
		{`insert into document_versions (id, document_id, organization_id, content) values (1, 5, 3 + 0, 'x') on conflict (id) do update set content = excluded.content`, ""},
	} {
		a := analyze(c.sql, m)
		got := ""
		if a.TargetLocal {
			got = a.targetSQL("documents__cloud", "2")
		}
		if got != c.want || c.want != "" && a.Rewritten == "" {
			t.Errorf("%s\n got: %s\nwant: %s", c.sql, got, c.want)
		}
	}
}

// A GRANT or REVOKE on a LokaDB table goes through lokadb.grant_sync, which gives the foreign
// partitions the parent's privileges; the client gets the statement's own tag.
func TestGrantSync(t *testing.T) {
	m := testMeta()
	m.GrantSync = true
	for _, c := range []struct{ sql, want, tag string }{
		{`GRANT SELECT ON documents TO reader`, `SELECT lokadb.grant_sync('GRANT SELECT ON documents TO reader', ARRAY['documents']::text[])`, "GRANT"},
		{`revoke update on public.documents, projects from "Reader"`, `SELECT lokadb.grant_sync('revoke update on public.documents, projects from "Reader"', ARRAY['documents']::text[])`, "REVOKE"},
		{`GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader`, `SELECT lokadb.grant_sync('GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader', ARRAY['document_versions', 'documents']::text[])`, "GRANT"},
		{`GRANT SELECT ON projects TO reader`, "", ""},
		{`GRANT USAGE ON SCHEMA lokadb TO reader`, "", ""},
	} {
		a := analyze(c.sql, m)
		if a.Rewritten != c.want || indexTag(a) != c.tag {
			t.Errorf("%s\n got: %s (tag %q)\nwant: %s (tag %q)", c.sql, a.Rewritten, indexTag(a), c.want, c.tag)
		}
	}
	// a database without the function: the statement as written
	if a := analyze(`GRANT SELECT ON documents TO reader`, testMeta()); a.Rewritten != "" {
		t.Errorf("without grant_sync: %s", a.Rewritten)
	}
}

// Item 9 (b): a rewritten upsert runs under the lock of its table and organization; the
// native one in a local partition and a string of several statements take none.
func TestUpsertLock(t *testing.T) {
	m := testMeta()
	for _, c := range []struct{ sql, part, want string }{
		{`insert into documents (organization_id, slug) values (1, 's') on conflict (organization_id, slug) do nothing`, "", "documents/1"},
		{`insert into documents (organization_id, slug) values (2, 's') on conflict (organization_id, slug) do nothing`, "documents__cloud", ""},
		{`insert into documents (organization_id, slug) values (1, 's')`, "", ""},
		{`insert into documents (organization_id, slug) values (1, 's') on conflict (organization_id, slug) do nothing; select 1`, "", ""},
	} {
		if got := upsertLock(analyze(c.sql, m), "1", c.part); got != c.want {
			t.Errorf("%s (part %q): got %q, want %q", c.sql, c.part, got, c.want)
		}
	}
}

// Item 7: an UPDATE that sets the organization column calls lokadb.same_org, when the
// database has it.
func TestSameOrg(t *testing.T) {
	m := testMeta()
	sql := `UPDATE "documents" SET "organization_id" = $1, "title" = $2 WHERE "documents"."id" = $3`
	if a := analyze(sql, m); a.Rewritten != "" {
		t.Errorf("without lokadb.same_org the UPDATE stays: %s", a.Rewritten)
	}
	m.SameOrg = true
	a := analyze(sql, m)
	if a.Rewritten != `UPDATE documents SET organization_id = lokadb.same_org($1, documents.organization_id), title = $2 WHERE documents.id = $3` || !a.MovesOrg || len(a.Hints) != 1 {
		t.Errorf("same_org: %q moves %v hints %+v", a.Rewritten, a.MovesOrg, a.Hints)
	}
	if a := analyze(`update documents d set title = 'x' where d.id = 1`, m); a.Rewritten != "" || a.targetText() == "" {
		t.Errorf("an UPDATE that leaves the organization alone is not rewritten: %q", a.Rewritten)
	}
}

// COPY of a LokaDB table (REALISTIC_REVIEW 1.6, block 14 LD1): TO reads the partitions as a
// SELECT would; FROM is refused with a lokadb message instead of Postgres' RLS error.
func TestCopyTable(t *testing.T) {
	m := testMeta()
	for in, want := range map[string]string{
		`COPY documents TO STDOUT`: `COPY (SELECT * FROM documents) TO STDOUT`,
		`copy public.documents (id, title) to stdout with (format csv, header)`: `COPY (SELECT id, title FROM public.documents) TO STDOUT WITH (FORMAT CSV, HEADER)`,
	} {
		a := analyze(in, m)
		if a.Rewritten != want || !a.Lokadb || !a.Unhinted || a.Forbidden != "" {
			t.Errorf("%s\n got: %s (lokadb %v unhinted %v forbidden %q)\nwant: %s", in, a.Rewritten, a.Lokadb, a.Unhinted, a.Forbidden, want)
		}
	}
	// COPY FROM loads a staging table; its trigger inserts the rows through the parent
	a := analyze(`COPY documents (organization_id, title) FROM STDIN WITH (FORMAT csv)`, m)
	if a.Rewritten != `COPY pg_temp.__lokadb_copy_documents(organization_id, title) FROM STDIN CSV` || !a.Write || a.Forbidden != "" || a.Unhinted || len(a.Prelude) != 4 ||
		!strings.Contains(a.Prelude[2], `INSERT INTO public."documents" ("organization_id", "title") SELECT s."organization_id", s."title" FROM pg_temp."__lokadb_copy_documents" s`) {
		t.Errorf("COPY FROM: %q forbidden %q write %v prelude %q", a.Rewritten, a.Forbidden, a.Write, a.Prelude)
	}
	// a child table: the organization from the parent's refs
	a = analyze(`COPY document_versions (document_id, content) FROM STDIN`, m)
	if len(a.Prelude) != 4 || !strings.Contains(a.Prelude[2], `INSERT INTO public."document_versions" ("document_id", "content", "organization_id") SELECT s."document_id", s."content", CASE WHEN s."document_id" IS NULL THEN NULL ELSE coalesce((SELECT r.tenant FROM "documents__refs" r WHERE r.id = s."document_id")`) {
		t.Errorf("COPY FROM into a child table: %q", a.Prelude)
	}
	for _, p := range a.Prelude {
		if _, err := pg.Parse(p); err != nil {
			t.Errorf("prelude does not parse: %v\n%s", err, p)
		}
	}
	m.OrgLock = true
	if a := analyze(`COPY documents (organization_id, title) FROM STDIN`, m); !a.OwnLock || !strings.Contains(a.Prelude[2], "BEGIN PERFORM pg_advisory_xact_lock_shared(lokadb.org_lock(NULL)); INSERT INTO") {
		t.Errorf("COPY FROM takes the pause lock in its trigger: own %v %q", a.OwnLock, a.Prelude)
	}
	m.OrgLock = false
	if a := analyze(`COPY documents FROM '/tmp/x.csv'`, m); !strings.HasPrefix(a.Forbidden, "lokadb: COPY documents FROM a file") {
		t.Errorf("COPY FROM a file: %q", a.Forbidden)
	}
	if a := analyze(`COPY (SELECT id FROM documents WHERE organization_id = 1) TO STDOUT`, m); a.Rewritten != "" || len(a.Hints) != 1 {
		t.Errorf("COPY (query) is analyzed as it is: %+v", a)
	}
	if a := analyze(`COPY users TO STDOUT`, m); a.Rewritten != "" || a.Lokadb {
		t.Errorf("COPY of a cloud table is left alone: %+v", a)
	}
}

// MERGE into a LokaDB table (REALISTIC_REVIEW P5): analyzed for the organization and the
// policy, rewritten (Postgres refuses MERGE on a table with foreign partitions) or refused.
func TestMerge(t *testing.T) {
	m := testMeta()
	for _, c := range []struct{ sql, has string }{
		{`MERGE INTO documents d USING (VALUES ($1::bigint, $2::bigint, $3)) s(id, org, title) ON d.id = s.id AND d.organization_id = 1
			WHEN MATCHED THEN UPDATE SET title = s.title WHEN NOT MATCHED THEN INSERT (id, organization_id, title) VALUES (s.id, s.org, s.title)`,
			`m AS (UPDATE "documents" AS "d" SET "title" = s.title FROM "s" WHERE (d.id = s.id AND d.organization_id = 1) AND "d"."id" = ANY (ARRAY(SELECT "s"."id" FROM "s")) AND (SELECT count(*) FROM hit) >= 0 RETURNING 1) INSERT INTO "documents" AS "d" ("id", "organization_id", "title") SELECT s.id, s.org, s.title FROM "s" WHERE "s".__lokadb_ord NOT IN (SELECT __lokadb_ord FROM hit) AND (SELECT count(*) FROM m) >= 0`},
		// Hibernate 6 style: the target named through the schema
		{`merge into public.documents as t using (select $1::bigint as id, $2 as title) as s on (t.id = s.id) when matched and t.title is distinct from s.title then update set title = s.title when not matched then do nothing`,
			`UPDATE "documents" AS "t" SET "title" = s.title FROM "s" WHERE (t.id = s.id) AND "t"."id" = ANY (ARRAY(SELECT "s"."id" FROM "s")) AND (t.title IS DISTINCT FROM s.title)`},
		{`MERGE INTO "public"."documents" USING staged ON "public"."documents"."id" = staged.id WHEN MATCHED THEN DELETE`,
			`DELETE FROM "documents" AS "documents" USING "staged" WHERE (documents.id = staged.id) AND "documents"."id" = ANY (ARRAY(SELECT "staged"."id" FROM "staged"))`},
	} {
		a := analyze(c.sql, m)
		if !strings.Contains(a.Rewritten, c.has) || !a.Lokadb || !a.Write || a.Forbidden != "" {
			t.Errorf("%s\n got: %s (lokadb %v write %v forbidden %q)\nwant: …%s…", c.sql, a.Rewritten, a.Lokadb, a.Write, a.Forbidden, c.has)
		}
		if _, err := pg.Parse(a.Rewritten); err != nil {
			t.Errorf("rewritten MERGE does not parse: %v\n%s", err, a.Rewritten)
		}
	}
	if a := analyze(`MERGE INTO documents d USING src s ON d.id = s.id AND d.organization_id = 2 WHEN MATCHED THEN DELETE`, m); len(a.Hints) != 1 || a.Hints[0].Column != "organization_id" {
		t.Errorf("the organization of the ON condition is a hint: %+v", a.Hints)
	}
	for _, sql := range []string{
		`MERGE INTO documents d USING src s ON d.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE`,
		`MERGE INTO documents d USING src s ON d.id = s.id WHEN MATCHED AND s.x THEN DELETE WHEN MATCHED THEN UPDATE SET title = s.title`,
		`MERGE INTO documents d USING src s ON d.id = s.id WHEN MATCHED THEN DELETE RETURNING merge_action()`,
	} {
		if a := analyze(sql, m); !strings.HasPrefix(a.Forbidden, "lokadb: MERGE into documents") || a.Rewritten != "" {
			t.Errorf("%s: must be refused, got forbidden %q rewritten %q", sql, a.Forbidden, a.Rewritten)
		}
	}
	if a := analyze(`MERGE INTO users u USING src s ON u.id = s.id WHEN MATCHED THEN DELETE`, m); a.Rewritten != "" || a.Lokadb {
		t.Errorf("MERGE into a cloud table is left alone: %+v", a)
	}
}

// The rewritten upsert keeps every scan of the table to the keys it inserts
// (REALISTIC_REVIEW 1.2): a join with v alone reaches an organization's database as a read
// of its whole table.
func TestUpsertReach(t *testing.T) {
	m := testMeta()
	keys := `"x"."organization_id" = ANY (ARRAY(SELECT v."organization_id" FROM v)) AND "x"."slug" = ANY (ARRAY(SELECT v."slug" FROM v))`
	for _, c := range []struct{ sql, alias string }{
		{`insert into documents (organization_id, slug, title) values ($1, $2, $3) on conflict (organization_id, slug) do update set title = excluded.title`, `"documents"`},
		{`insert into documents as d (organization_id, slug, title) values ($1, $2, $3) on conflict (organization_id, slug) do update set title = excluded.title where d.title <> 'keep'`, `"d"`},
		{`insert into documents (organization_id, slug, title) select * from unnest($1::bigint[], $2::text[], $3::text[]) on conflict (organization_id, slug) do nothing`, ""},
	} {
		a := analyze(c.sql, m)
		if c.alias != "" && !strings.Contains(a.Rewritten, strings.ReplaceAll(keys, `"x"`, c.alias)) {
			t.Errorf("the UPDATE must be kept to the keys:\n%s", a.Rewritten)
		}
		if strings.Contains(c.sql, "do nothing") || strings.Contains(c.sql, "where d.") {
			if !strings.Contains(a.Rewritten, strings.ReplaceAll(keys, `"x"`, "x")) {
				t.Errorf("the NOT EXISTS must be kept to the keys:\n%s", a.Rewritten)
			}
		}
		if _, err := pg.Parse(a.Rewritten); err != nil {
			t.Errorf("rewritten SQL does not parse: %v\n%s", err, a.Rewritten)
		}
	}
}

func TestUtility(t *testing.T) {
	m := testMeta()
	for sql, want := range map[string][2]bool{ // utility, resets org
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE": {true, false},
		"SHOW search_path":                   {true, false},
		"VACUUM documents":                   {true, false},
		"LISTEN jobs":                        {true, false},
		"SET lokadb.org = '2'":               {true, true},
		"RESET ALL":                          {true, true},
		"DISCARD ALL":                        {true, true},
		"SELECT 1":                           {false, false},
		"SET search_path = public; SELECT 1": {false, false},
		"SELECT set_config('lokadb.org', '2', false)":                      {false, true},
		"SELECT set_config('LOKADB.ORG'::text, '2', true)":                 {false, true},
		"SELECT pg_catalog.set_config($1, $2, false)":                      {false, true},
		"SELECT set_config('search_path', 'x', false)":                     {false, false},
		"SELECT myschema.set_config('lokadb.org', '2', false)":             {false, false},
		"UPDATE pg_settings SET setting = '2' WHERE name = 'lokadb.org'":   {false, true},
		"DO $$ BEGIN PERFORM set_config('lokadb.org', '2', false); END $$": {false, true},
		"DO $$ BEGIN PERFORM 1; END $$":                                    {false, false},
		// lokadb.tenant is the application's (the cooperative mode): not a change of lokadb.org
		"SET LOCAL lokadb.tenant = 1":                                                        {true, false},
		"RESET lokadb.tenant":                                                                {true, false},
		"SET lokadb.other = 1":                                                               {true, true},
		"SELECT set_config('lokadb.tenant', $1, true)":                                       {false, false},
		"SELECT set_config(' LOKADB.TENANT ', '1', true)":                                    {false, false},
		"SELECT set_config('LOKADB.Org', '2', false)":                                        {false, true},
		"SELECT set_config('lokadb.other', '2', false)":                                      {false, true},
		"SELECT set_config('lokadb.tenant', '1', true), set_config('lokadb.org', '2', true)": {false, true},
	} {
		a := analyze(sql, m)
		if a.Utility != want[0] || a.ResetsOrg != want[1] {
			t.Errorf("%s: utility %v resets %v, want %v", sql, a.Utility, a.ResetsOrg, want)
		}
	}
	for in, want := range map[string]string{"007": "7", "-0": "0", "42": "42", "9223372036854775807": "9223372036854775807"} {
		if got := canonicalInt(in); got != want {
			t.Errorf("canonicalInt(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestGaps(t *testing.T) {
	m := testMeta()
	cases := []struct {
		name, sql string
		gaps      []Gap
	}{
		{"no condition", `select * from documents`, []Gap{{Kind: GapNoCondition, Table: "documents"}}},
		{"unknown column", `select * from documents where slug = $1`,
			[]Gap{{Kind: GapUnknownColumn, Table: "documents", Column: "slug", Form: "="}}},
		{"unknown column in", `select * from documents where slug in ($1, $2)`,
			[]Gap{{Kind: GapUnknownColumn, Table: "documents", Column: "slug", Form: "IN"}}},
		{"unknown column other operator", `select * from documents where title > $1`,
			[]Gap{{Kind: GapUnknownColumn, Table: "documents", Column: "title", Form: ">"}}},
		{"<> any", `select * from documents where id <> any($1)`,
			[]Gap{{Kind: GapUnsupportedForm, Table: "documents", Column: "id", Form: "<> ANY"}}},
		{"or", `select * from documents where id = 1 or id = 2`,
			[]Gap{{Kind: GapUnsupportedForm, Table: "documents", Column: "id", Form: "OR"}}},
		{"in subquery without organization", `select * from document_versions where document_id in (select id from documents where slug = $1)`,
			[]Gap{{Kind: GapUnsupportedForm, Table: "document_versions", Column: "document_id", Form: "IN (SELECT …)"}, {Kind: GapUnknownColumn, Table: "documents", Column: "slug", Form: "="}}},
		{"unreadable", `select * from documents where organization_id = lower($1)::bigint`,
			[]Gap{{Kind: GapUnreadableValue, Table: "documents", Column: "organization_id", Form: "="}}},
		{"insert select", `insert into documents (title, slug) select title, slug from documents_archive`,
			[]Gap{{Kind: GapUnsupportedForm, Table: "documents", Form: "INSERT … SELECT"}}},
		{"insert unreadable", `insert into documents (organization_id, slug) values (coalesce($1, 1), $2)`,
			[]Gap{{Kind: GapUnreadableValue, Table: "documents", Column: "organization_id", Form: "INSERT"}}},
		{"insert unknown", `insert into documents (slug, title) values ($1, $2)`,
			[]Gap{{Kind: GapUnknownColumn, Table: "documents", Column: "slug", Form: "INSERT"}, {Kind: GapUnknownColumn, Table: "documents", Column: "title", Form: "INSERT"}}},
		{"routed", `select * from documents where id = $1`, nil},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.ParseError || a.Unhinted != (c.gaps != nil) || !reflect.DeepEqual(a.Gaps, c.gaps) {
			t.Errorf("%s: unhinted=%v gaps=%+v, want %+v", c.name, a.Unhinted, a.Gaps, c.gaps)
		}
	}
}

// A value bound to one side of an equality between columns binds the other side too.
func TestJoinPropagation(t *testing.T) {
	m := testMeta()
	cases := []struct {
		name, sql string
		hints     []Hint
	}{
		{"through a cloud table", `select d.* from documents d join projects p on d.project_id = p.id where p.id = $1`, nil},
		{"version by its document", `select v.* from document_versions v join documents d on v.document_id = d.id where d.slug = $1 and v.document_id = $2`,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Param: 2}}}}},
		{"document by its versions' key", `select d.* from documents d, document_versions v where v.document_id = d.id and d.id = $1`,
			[]Hint{{"documents", "id", []ValueRef{{Param: 1}}}}},
		{"chain", `select d.* from documents d join document_versions v on v.document_id = d.id join projects p on p.id = d.project_id where v.document_id = x.y and p.id = 1 and d.id = 7`,
			[]Hint{{"documents", "id", []ValueRef{{Const: "7"}}}}},
		{"propagated", `select v.* from document_versions v join projects p on true join documents d on d.id = p.doc where v.document_id = p.doc and p.doc = 5`,
			[]Hint{{"document_versions", "document_id", []ValueRef{{Const: "5"}}}, {"documents", "id", []ValueRef{{Const: "5"}}}}},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.ParseError || a.Unhinted != (c.hints == nil) || !reflect.DeepEqual(a.Hints, c.hints) {
			t.Errorf("%s: unhinted=%v hints=%+v gaps=%+v, want %+v", c.name, a.Unhinted, a.Hints, a.Gaps, c.hints)
		}
	}
	// the ON condition of an outer join restricts one side only: not a hint
	if a := analyze(`select * from documents d left join document_versions v on v.document_id = d.id and v.document_id = 1`, m); !a.Unhinted {
		t.Errorf("left join: hints %+v", a.Hints)
	}
	// an inner join's ON is as good as WHERE
	if a := analyze(`select * from documents d join document_versions v on v.document_id = d.id and d.organization_id = 1`, m); a.Unhinted {
		t.Errorf("inner join ON: gaps %+v", a.Gaps)
	}
}

// Rails' where(id: relation): the subquery names the organization.
func TestSubqueryHint(t *testing.T) {
	m := testMeta()
	for _, c := range []struct {
		sql      string
		unhinted bool
	}{
		{`SELECT "document_versions".* FROM "document_versions" WHERE "document_versions"."document_id" IN (SELECT "documents"."id" FROM "documents" WHERE "documents"."organization_id" = $1)`, false},
		{`select * from documents where id in (select document_id from document_versions where organization_id = 2)`, false},
		{`select * from document_versions where document_id in (select id from documents where slug = $1)`, true},
		{`select * from document_versions where document_id in (select project_id from documents where organization_id = 1)`, true}, // another lookup
		{`select * from document_versions where document_id not in (select id from documents where organization_id = 1)`, true},
	} {
		a := analyze(c.sql, m)
		if a.Unhinted != c.unhinted {
			t.Errorf("%s: unhinted %v, hints %+v gaps %+v", c.sql, a.Unhinted, a.Hints, a.Gaps)
		}
	}
}

// P4, item 14: an INSERT's own id is not looked up (the row is new); an upsert's is.
func TestInsertOwnID(t *testing.T) {
	m := testMeta()
	a := analyze(`insert into document_versions (id, document_id, content) values ($1, $2, $3)`, m)
	if a.Unhinted || !reflect.DeepEqual(a.Hints, []Hint{{"document_versions", "document_id", []ValueRef{{Param: 2}}}}) {
		t.Errorf("child with id: %+v", a.Hints)
	}
	a = analyze(`insert into documents (id, organization_id) values (7, 1)`, m)
	if !reflect.DeepEqual(a.Hints, []Hint{{"documents", "organization_id", []ValueRef{{Const: "1"}}}}) {
		t.Errorf("root with id: %+v", a.Hints)
	}
	a = analyze(`insert into document_versions (id, content) values (7, 'x')`, m)
	if !a.Unhinted || !reflect.DeepEqual(a.Gaps, []Gap{{Kind: GapUnknownColumn, Table: "document_versions", Column: "content", Form: "INSERT"}}) {
		t.Errorf("only id: unhinted %v hints %+v gaps %+v", a.Unhinted, a.Hints, a.Gaps)
	}
	a = analyze(`insert into documents (id, title) values (7, 'x') on conflict (id) do update set title = excluded.title`, m)
	if a.Unhinted || len(a.Hints) != 1 || a.Hints[0].Column != "id" {
		t.Errorf("upsert by id: %+v", a.Hints)
	}
}

// Item 13: views, functions and DO blocks over LokaDB tables are LokaDB statements.
func TestViewsFuncsDo(t *testing.T) {
	m := testMeta()
	m.Views = map[string][]string{"recent_documents": {"documents"}}
	m.Funcs = map[string]bool{"archive_documents": true}
	cases := []struct {
		sql      string
		unhinted bool
		gap      Gap
	}{
		{`select * from recent_documents where organization_id = $1`, true, Gap{Kind: GapUnsupportedForm, Table: "recent_documents", Form: "view of documents"}},
		{`select * from recent_documents r join documents d on d.id = r.id where d.organization_id = 1`, false, Gap{}},
		{`select archive_documents($1)`, true, Gap{Kind: GapUnsupportedForm, Form: "function archive_documents()"}},
		{`select public.archive_documents(id) from documents where organization_id = 1`, false, Gap{}},
		{`DO $$ BEGIN DELETE FROM documents WHERE title = 'x'; END $$`, true, Gap{Kind: GapUnsupportedForm, Table: "documents", Form: "DO"}},
		{`insert into recent_documents (title) values ('x')`, true, Gap{Kind: GapUnsupportedForm, Table: "recent_documents", Form: "INSERT into a view of documents"}},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if !a.Lokadb || a.Unhinted != c.unhinted || c.unhinted && (len(a.Gaps) == 0 || a.Gaps[0] != c.gap) {
			t.Errorf("%s: lokadb %v unhinted %v gaps %+v", c.sql, a.Lokadb, a.Unhinted, a.Gaps)
		}
	}
	for _, sql := range []string{`DO $$ BEGIN PERFORM 1; END $$`, `select * from projects_view`, `select now()`} {
		if a := analyze(sql, m); a.Lokadb {
			t.Errorf("%s: lokadb", sql)
		}
	}
}

// Item 6: a cloud table that knows the organization (routing.cloud_tables).
func TestCloudTable(t *testing.T) {
	m := testMeta()
	m.Cloud = map[string]string{"projects": "account_id"}
	m.Lookups["projects"] = map[string]string{"account_id": "", "id": "projects"}
	cases := []struct {
		sql   string
		hints []Hint
	}{
		{`select d.* from documents d join projects p on p.id = d.project_id where p.account_id = $1`, []Hint{{"projects", "account_id", []ValueRef{{Param: 1}}}}},
		{`select d.* from documents d join projects p on d.project_id = p.id where p.id = 5`, []Hint{{"projects", "id", []ValueRef{{Const: "5"}}}}},
		{`select d.* from documents d where d.project_id = 5`, nil},
	}
	for _, c := range cases {
		a := analyze(c.sql, m)
		if a.Unhinted != (c.hints == nil) || !reflect.DeepEqual(a.Hints, c.hints) {
			t.Errorf("%s: unhinted %v hints %+v gaps %+v", c.sql, a.Unhinted, a.Hints, a.Gaps)
		}
	}
	if a := analyze(`select * from projects where account_id = 1`, m); a.Lokadb || len(a.Hints) > 0 {
		t.Errorf("a cloud table alone: %+v", a)
	}
	// undeclared: the gap names the cloud table's column
	m = testMeta()
	a := analyze(`select d.* from documents d join projects p on p.id = d.project_id where p.account_id = $1`, m)
	if !a.Unhinted || !reflect.DeepEqual(a.Gaps, []Gap{{Kind: GapUnknownColumn, Table: "projects", Column: "account_id", Form: "=", Cloud: true}, {Kind: GapNoCondition, Table: "documents"}}) {
		t.Errorf("undeclared: %+v", a.Gaps)
	}
}

// P6, item 10: what the parser refuses is not taken for a cloud statement when it names a LokaDB table.
func TestUnparsed(t *testing.T) {
	m := testMeta()
	a := analyze(`select * from documents where title is json object frobnicate`, m)
	if !a.ParseError || !a.Lokadb || !a.Unhinted || len(a.Gaps) != 1 || a.Gaps[0].Table != "documents" {
		t.Errorf("LokaDB: %+v", a)
	}
	if a := analyze(`selec 1 from projects`, m); !a.ParseError || a.Lokadb {
		t.Errorf("cloud: %+v", a)
	}
}

// P7, item 11: statements that may change what a lookup answers.
func TestChangesLookups(t *testing.T) {
	m := testMeta()
	m.Lookups["documents"]["slug"] = "lokadb.documents__key__slug"
	m.Cloud = map[string]string{"projects": "account_id"}
	for sql, want := range map[string]bool{
		`update documents set organization_id = 2 where id = 1`: true,
		`update documents set slug = 'x' where id = 1`:          true,
		`update documents set title = 'x' where id = 1`:         false,
		`delete from documents where id = 1`:                    true,
		`delete from document_versions where id = 1`:            false,
		`update projects set account_id = 2 where id = 1`:       true,
		`select * from documents where id = 1`:                  false,
	} {
		if got := analyze(sql, m).ChangesLookups; got != want {
			t.Errorf("%s: %v", sql, got)
		}
	}
	// the version of "p7" as it is now: a run before (-count) has moved it past 0
	s := &session{db: "p7", orgCache: map[string]map[string]string{"documents__refs": {"1": "1"}}, orgVer: orgVersion("p7").Load(), orgAt: time.Now()}
	s.freshOrgs(&Analysis{})
	if len(s.orgCache) != 1 {
		t.Fatalf("emptied without a change")
	}
	other := &session{db: "p7", orgAt: time.Now()}
	other.freshOrgs(&Analysis{ChangesLookups: true}) // another session moves a row
	if s.freshOrgs(&Analysis{}); len(s.orgCache) != 0 {
		t.Errorf("kept after another session's change: %v", s.orgCache)
	}
}

// Item 8: INSERT … SELECT and correlated subqueries name the organization of their source.
func TestInsertSelectCorrelated(t *testing.T) {
	m := testMeta()
	for _, c := range []struct {
		sql      string
		unhinted bool
	}{
		// a copy within the organization: the source names it
		{`insert into document_versions (document_id, content) select id, title from documents where organization_id = $1`, false},
		{`insert into documents (organization_id, title) select organization_id, title from documents where id = $1`, false},
		// a constant organization: a hint as in VALUES
		{`insert into documents (organization_id, title) select 3, title from projects`, false},
		// the source names no organization
		{`insert into documents (organization_id, title) select organization_id, title from documents where slug = $1`, true},
		// the column is not the organization's
		{`insert into documents (organization_id, title) select project_id, title from documents where id = $1`, true},
		// correlated: the versions of the organization's documents
		{`select d.* from documents d where d.organization_id = 1 and exists (select 1 from document_versions v where v.document_id = d.id)`, false},
		{`select d.id, (select count(*) from document_versions v where v.document_id = d.id) from documents d where d.id = $1`, true}, // a subquery outside the conditions
		{`select d.* from documents d where d.slug = $1 and exists (select 1 from document_versions v where v.document_id = d.id)`, true},
		{`select d.* from documents d where d.organization_id = 1 and exists (select 1 from document_versions v where v.content = d.title)`, true},
	} {
		a := analyze(c.sql, m)
		if a.ParseError || a.Unhinted != c.unhinted {
			t.Errorf("%s: unhinted %v hints %+v gaps %+v", c.sql, a.Unhinted, a.Hints, a.Gaps)
		}
	}
}

// R6 of the routing review: the native upsert into a cloud organization's partition has neither
// the parent's policy nor lokadb.same_org, so a DO UPDATE that assigns the organization column
// stays with the parent (its policy stops the row moving). Rails' upsert_all writes
// excluded.organization_id, the one organization of the statement: that stays native.
func TestLocalUpsertSetsOrg(t *testing.T) {
	m := testMeta()
	m.SameOrg = true
	const head = "INSERT INTO documents (organization_id, slug, title) VALUES (1, 's', 'a') ON CONFLICT (slug) DO UPDATE SET "
	for _, c := range []struct {
		set    string
		native bool
	}{
		{"title = excluded.title", true},
		{"organization_id = excluded.organization_id, title = excluded.title", true},
		{"organization_id = 2", false},
		{"organization_id = 2, title = excluded.title", false},
		{`"organization_id" = documents.organization_id + 1`, false},
		{"organization_id = excluded.project_id", false},
		{"(organization_id, title) = (2, 'b')", false},
		{"organization_id = (SELECT 2)", false},
	} {
		a := analyze(head+c.set, m)
		if a.TargetLocal != c.native {
			t.Errorf("SET %s: native %v, want %v (TargetSQL %s)", c.set, a.TargetLocal, c.native, a.TargetSQL)
		}
		// not native: through the parent, where the rewritten upsert (or, for a form
		// rewriteUpsert does not take, the statement as written) meets its policy
		if !c.native && a.TargetSQL != "" {
			t.Errorf("SET %s: not native, yet TargetSQL %s", c.set, a.TargetSQL)
		}
	}
}

// PF4 of the performance review: the statement aimed at an organization's partition (TargetSQL)
// is a copy of the tree and a Deparse, 43% of the analysis of a new text, and most statements
// never reach a moved organization. The analysis decides only that the partition may take the
// statement (Target); the text is built when a partition is first addressed, once.
func TestTargetIsLazy(t *testing.T) {
	m := testMeta()
	m.Foreign = map[string]map[string]string{"documents": {"1": "documents__org1"}}
	m.Direct = map[string]bool{"documents__org1": true}
	s := &session{meta: m}
	const sql = `select id, title from documents where organization_id = $1 and id = $2`
	a := analyze(sql, m)
	if a.Target != "documents" || a.TargetSQL != "" || a.lazy == nil || a.lazy.text != "" {
		t.Fatalf("after the analysis: Target %q, TargetSQL %q, lazy %v", a.Target, a.TargetSQL, a.lazy != nil)
	}
	// an organization with no partition of its own (a cloud organization): nothing is built
	for _, org := range []string{"2", "", "-1"} {
		if part := s.foreignPart(a, org); part != "" || a.lazy.text != "" {
			t.Fatalf("org %q: partition %q, text built %q", org, part, a.lazy.text)
		}
	}
	// a moved organization: built once, the text the eager analysis made
	part := s.foreignPart(a, "1")
	if part != "documents__org1" || a.lazy.text == "" {
		t.Fatalf("org 1: partition %q, text %q", part, a.lazy.text)
	}
	// the text main's eager analysis made for this statement (compared when PF4 was written)
	want := `SELECT id, title FROM __lokadb_target__ documents WHERE (organization_id = $1 AND id = $2) AND documents.organization_id = __lokadb_org__`
	if got := a.targetText(); got != want {
		t.Fatalf("text:\n got %s\nwant %s", got, want)
	}
	if got := a.targetSQL("documents__org1", "1"); got != `SELECT id, title FROM "documents__org1" documents WHERE (organization_id = $1 AND id = $2) AND documents.organization_id = 1` {
		t.Fatalf("aimed at the partition: %s", got)
	}
	// a text cannot become one the partition does not take
	if b := analyze(`select d.id from projects p left join documents d on p.id = d.project_id where d.id = $1`, m); b.Target != "" || b.targetText() != "" {
		t.Fatalf("the nullable side of a join: Target %q", b.Target)
	}
}

// The analysis of a one-table text allocates what the review measured less the copy and the
// Deparse (529 allocations before PF4, 377 after): a guard against building it again.
func TestAnalyzeAllocs(t *testing.T) {
	m := testMeta()
	const sql = `select id, title from documents where organization_id = $1 and id = $2`
	if n := testing.AllocsPerRun(20, func() { _ = analyze(sql, m) }); n > 450 {
		t.Errorf("analyze of a point statement: %.0f allocations, want at most 450 (529 when the partition's text was built with it)", n)
	}
}

// The analysis of a text the session has not seen (every statement of an application that
// inlines its literals): go test -bench BenchmarkAnalyzeOneTable -benchmem. Before PF4: 529 and
// 477 allocations, 17.6 and 15.5 KB, about 210 µs; after: 377 and 333, 11.5 and 9.8 KB, 130 µs.
func BenchmarkAnalyzeOneTable(b *testing.B) {
	m := testMeta()
	for _, c := range []struct{ name, sql string }{
		{"point_param", `SELECT id, title FROM documents WHERE organization_id = $1 AND id = $2`},
		{"point_literal", `SELECT "documents"."id", "documents"."title" FROM "documents" WHERE "documents"."id" = 123456 LIMIT 21`},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = analyze(c.sql, m)
			}
		})
	}
}

// Review 2 of routing: row security of a LokaDB table. Turning it off or making it not forced
// is refused (only column changes pass); the application's own CREATE and DROP POLICY run
// with lokadb.tenant_policy after them, which keeps lokadb_tenant restrictive next to them (a
// permissive USING (true) next to a permissive lokadb_tenant let every organization through).
func TestPolicyStmt(t *testing.T) {
	m := testMeta()
	for _, q := range []string{
		"ALTER TABLE documents DISABLE ROW LEVEL SECURITY",
		"ALTER TABLE documents NO FORCE ROW LEVEL SECURITY",
		"ALTER TABLE public.documents ENABLE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY",
	} {
		if a := Analyze(q, m); a.DDLError == "" {
			t.Errorf("%s: not refused", q)
		}
	}
	cases := []struct{ q, tag, want string }{
		{"CREATE POLICY mine ON documents USING (true)", "CREATE POLICY",
			"DO $lokadb$ BEGIN EXECUTE 'CREATE POLICY mine ON documents USING (true)'; PERFORM lokadb.tenant_policy('documents', 'organization_id'); END $lokadb$"},
		{"DROP POLICY IF EXISTS mine ON public.documents;", "DROP POLICY",
			"DO $lokadb$ BEGIN EXECUTE 'DROP POLICY IF EXISTS mine ON public.documents'; PERFORM lokadb.tenant_policy('documents', 'organization_id'); END $lokadb$;"},
		{"CREATE POLICY mine ON projects USING (true)", "", ""},
	}
	for _, c := range cases {
		a := Analyze(c.q, m)
		if a.Rewritten != c.want || a.Tag != c.tag {
			t.Errorf("%s:\n got %q (tag %q)\nwant %q (tag %q)", c.q, a.Rewritten, a.Tag, c.want, c.tag)
		}
	}
}
