package migrate

import (
	"strings"
	"testing"
)

func TestIndexKey(t *testing.T) {
	same := []string{
		`CREATE INDEX documents_title_idx ON public.documents__cloud USING btree (title)`,
		`CREATE INDEX "Weird name" ON lokadb.documents USING btree (title)`,
		`CREATE INDEX x ON ONLY public."Docs ""q""" USING btree (title)`,
	}
	for _, d := range same {
		if got := indexKey(d); got != "USING btree (title)" {
			t.Errorf("%s: key %q", d, got)
		}
	}
	if got := indexKey(`CREATE UNIQUE INDEX u ON public.documents__cloud USING btree (slug) WHERE (slug IS NOT NULL)`); got != "UNIQUE USING btree (slug) WHERE (slug IS NOT NULL)" {
		t.Errorf("unique: %q", got)
	}
}

func TestCompare(t *testing.T) {
	cl := &table{exists: true, cols: map[string]column{
		"id":    {name: "id", typ: "bigint", notnull: "true", def: "nextval('documents_id_seq'::regclass)"},
		"title": {name: "title", typ: "text", notnull: "false"},
		"state": {name: "state", typ: "text", notnull: "true", def: "'new'::text"},
	}, order: []string{"id", "title", "state"},
		idx:    []index{{"documents_title_idx", "CREATE INDEX documents_title_idx ON public.documents__cloud USING btree (title)", "USING btree (title)"}},
		checks: []constraint{{"title_len", "CHECK ((length(title) < 100))", "title_len CHECK ((length(title) < 100))"}},
	}
	or := &table{exists: true, cols: map[string]column{
		"id":    {name: "id", typ: "bigint", notnull: "true"},
		"title": {name: "title", typ: "character varying(20)", notnull: "true"},
		"junk":  {name: "junk", typ: "int", notnull: "false"},
	}}
	ds := compare("1", "lokadb", "documents", "organization_id", cl, or)
	var got []string
	for _, d := range ds {
		got = append(got, d.Kind+" "+d.Object+": "+d.Fix)
	}
	want := []string{
		`column_type title: ALTER TABLE "lokadb"."documents" ALTER COLUMN "title" TYPE text USING "title"::text`,
		`not_null title: ALTER TABLE "lokadb"."documents" ALTER COLUMN "title" DROP NOT NULL`,
		`missing_column state: ALTER TABLE "lokadb"."documents" ADD COLUMN "state" text DEFAULT 'new'::text NOT NULL`,
		`extra_column junk: ALTER TABLE "lokadb"."documents" DROP COLUMN "junk"`,
		`missing_index documents_title_idx: CREATE INDEX "documents_title_idx" ON "lokadb"."documents" USING btree (title)`,
		`missing_check title_len: ALTER TABLE "lokadb"."documents" ADD CONSTRAINT "title_len" CHECK ((length(title) < 100))`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("got\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if !ds[3].Drop || ds[0].Drop {
		t.Error("only what the organization has more is a drop")
	}
}
