package main

// A1 of research/REALISTIC_REVIEW.md: tables whose names need quotes ("order", "Doc" as Prisma
// creates them). Their FKs were missed by enable and check, which said ready.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "Q1", name: `tables named "Doc" and "order": check, enable, a move and a cascade keep their FK and organizations`, run: checkQuotedNames},
	)
}

const quotedSpec = `{"tables": [
  {"name": "Doc", "tenant_column": "Org"},
  {"name": "order", "parent_table": "Doc", "parent_column": "doc_id"}
]}`

func checkQuotedNames(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE "Doc" (id bigserial PRIMARY KEY, "Org" bigint NOT NULL, title text)`,
		`CREATE TABLE "order" (id bigserial PRIMARY KEY, doc_id bigint REFERENCES "Doc" ON DELETE CASCADE, note text)`,
		`CREATE INDEX "Doc_title_idx" ON "Doc" (title)`,
		`ALTER TABLE "Doc" OWNER TO app`, `ALTER TABLE "order" OWNER TO app`,
		`INSERT INTO "Doc" ("Org", title) SELECT 1 + g % 2, 'd' || g FROM generate_series(1, 100) g`,
		`INSERT INTO "order" (doc_id, note) SELECT id, 'o' || id FROM "Doc"`); err != nil {
		return false, "", err
	}
	e.spec = quotedSpec
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
	out, code := e.checkWith(quotedSpec)
	note(!reported(out, "child_without_fk") && code != 3 && code != 5,
		fmt.Sprintf("check: %s (exit %d), child_without_fk %v", verdict(out), code, reported(out, "child_without_fk")))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	fk := val(e.cloud(), `SELECT count(*) FROM pg_constraint WHERE conrelid = '"order__cloud"'::regclass AND confrelid = '"Doc__refs"'::regclass`)
	lookup := val(e.cloud(), `SELECT count(*) FROM lokadb.lookups WHERE table_name = 'order' AND column_name = 'doc_id' AND refs_table = 'Doc__refs'`)
	note(fk == "1" && lookup == "1", fmt.Sprintf("after enable: FK order__cloud -> Doc__refs %s, lookup order.doc_id %s", fk, lookup))
	if _, err := e.move(1); err != nil {
		note(false, "move-org failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	docs, orders := val(e.customer(), `SELECT count(*) FROM "Doc"`), val(e.customer(), `SELECT count(*) FROM "order"`)
	custFK := val(e.customer(), `SELECT count(*) FROM pg_constraint WHERE conrelid = '"order"'::regclass AND confrelid = '"Doc"'::regclass AND contype = 'f'`)
	idx := val(e.customer(), `SELECT count(*) FROM pg_indexes WHERE tablename = 'Doc' AND indexname = 'Doc_title_idx'`)
	note(docs == "50" && orders == "50" && custFK == "1" && idx == "1",
		fmt.Sprintf("at organization 1: Doc %s, order %s, FK order -> Doc %s, index Doc_title_idx %s", docs, orders, custFK, idx))
	// a cascade from a document of organization 2 stays in organization 2
	before1 := val(e.cloud(), `SELECT count(*) FROM "order" WHERE "Org" = 1`)
	if err := execSQL(e.ctx, e.cloud(), `DELETE FROM "Doc" WHERE id = (SELECT min(id) FROM "Doc" WHERE "Org" = 2)`); err != nil {
		note(false, "delete in organization 2: "+firstErrLine(err))
	}
	after1, after2 := val(e.cloud(), `SELECT count(*) FROM "order" WHERE "Org" = 1`), val(e.cloud(), `SELECT count(*) FROM "order" WHERE "Org" = 2`)
	note(before1 == "50" && after1 == "50" && after2 == "49",
		fmt.Sprintf("a document of organization 2 deleted: orders of 1 %s -> %s, of 2 -> %s", before1, after1, after2))
	// through the proxy: a lookup by the parent id routes to organization 1's database
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
	doc1 := val(e.cloud(), `SELECT min(id) FROM "Doc" WHERE "Org" = 1`)
	res, err := simple(e.ctx, c, `SELECT note FROM "order" WHERE doc_id = `+doc1)
	got := "error"
	if err == nil && len(res) == 1 && len(res[0].Rows) == 1 {
		got = string(res[0].Rows[0][0])
	} else if err != nil {
		got = "error: " + firstErrLine(err)
	}
	note(got == "o"+doc1, fmt.Sprintf("through the proxy the order of document %s: %s", doc1, got))
	return ok, strings.Join(notes, "; "), nil
}
