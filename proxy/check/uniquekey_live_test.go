package check

// A table without a primary key and without id (Prisma's implicit many-to-many "_AToB": A and
// B NOT NULL, UNIQUE (A, B), an index of B) on a live Postgres (skipped without one):
//   LOKADB_REVIEW3_DSN=postgres://postgres:pw@host:5432 go test ./check -run UniqueKey

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
)

// check and enable say the same: the unique index of NOT NULL columns is the key (check:
// ready, enable: through, a composite key (A, B)); one whose column allows NULL is refused by
// enable, and check reports it unsupported with enable's words.
func TestLiveUniqueKey(t *testing.T) {
	dsn := liveDB(t, "b4_unique_key")
	if err := liveExec(t, dsn,
		`CREATE TABLE "Document" (id serial PRIMARY KEY, org_id bigint NOT NULL, title text)`,
		`CREATE TABLE "Tag" (id serial PRIMARY KEY, name text)`,
		`CREATE TABLE "_DocumentToTag" ("A" integer NOT NULL REFERENCES "Document"(id) ON DELETE CASCADE, "B" integer NOT NULL REFERENCES "Tag"(id) ON DELETE CASCADE)`,
		`CREATE UNIQUE INDEX "_DocumentToTag_AB_unique" ON "_DocumentToTag"("A", "B")`,
		`CREATE INDEX "_DocumentToTag_B_index" ON "_DocumentToTag"("B")`,
		`INSERT INTO "Document" (org_id, title) VALUES (1, 'a'), (2, 'b')`, `INSERT INTO "Tag" (name) VALUES ('x'), ('y')`,
		`INSERT INTO "_DocumentToTag" VALUES (1, 1), (1, 2), (2, 1)`); err != nil {
		t.Fatal(err)
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "Document", TenantColumn: "org_id"},
		{Name: "_DocumentToTag", ParentTable: "Document", ParentColumn: "A"}}}
	rep := liveCheck(t, dsn, spec)
	if named(rep, "_DocumentToTag") && rep.Verdict != Ready && rep.Verdict != Review {
		t.Errorf("check: %s, and it names _DocumentToTag", rep.Verdict)
	}
	for _, f := range rep.Findings {
		if f.Code == "id_missing" {
			t.Errorf("check reports id_missing for a table enable takes: %s", f.Message)
		}
	}
	if err := liveEnable(dsn, spec); err != nil {
		t.Fatalf("enable: %v", err)
	}
	var key string
	if err := liveRow(t, dsn, `SELECT key_column FROM lokadb.tables WHERE table_name = '_DocumentToTag'`, &key); err != nil || key != `["A","B"]` {
		t.Errorf("the key of _DocumentToTag: %q (%v), want [\"A\",\"B\"]", key, err)
	}

	// the same with B nullable: a unique index that does not tell rows apart
	dsn = liveDB(t, "b4_unique_null")
	if err := liveExec(t, dsn,
		`CREATE TABLE "Document" (id serial PRIMARY KEY, org_id bigint NOT NULL)`,
		`CREATE TABLE "_Loose" ("A" integer NOT NULL REFERENCES "Document"(id), "B" integer)`,
		`CREATE UNIQUE INDEX "_Loose_AB_unique" ON "_Loose"("A", "B")`); err != nil {
		t.Fatal(err)
	}
	spec = admin.Spec{Tables: []admin.SpecTable{{Name: "Document", TenantColumn: "org_id"},
		{Name: "_Loose", ParentTable: "Document", ParentColumn: "A"}}}
	rep = liveCheck(t, dsn, spec)
	err := liveEnable(dsn, spec)
	if err == nil || !strings.Contains(err.Error(), "allows NULL") {
		t.Fatalf("enable of a table whose unique index has a nullable column: %v, want a refusal", err)
	}
	said := ""
	for _, f := range rep.Findings {
		if f.Code == "id_missing" {
			said = f.Message
		}
	}
	if said == "" || !strings.Contains(err.Error(), said) {
		t.Errorf("check says %q (verdict %s), enable refuses with %q: not the same", said, rep.Verdict, err)
	}
	if rep.Verdict == Ready || rep.Verdict == Review {
		t.Errorf("check verdict %s for a table enable refuses", rep.Verdict)
	}
}

func liveRow(t *testing.T, dsn, sql string, v *string) error {
	t.Helper()
	ctx := t.Context()
	c, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	return c.QueryRow(ctx, sql).Scan(v)
}
