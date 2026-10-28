package main

// Block 4, item 2: an FK onto a unique column of a LokaDB table other than its key. enable kept
// references by the key only and refused the table (fk_not_to_id); now the FK points at a map
// of that column (value -> organization), kept like the refs table.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X19", name: "FKs onto docs.code, a unique column other than the key (a LokaDB child, a cloud table): check and enable take them, they hold after a move, CASCADE and RESTRICT act, the proxy routes a child by the code", run: checkFKUnique},
	)
}

const fkUniqueSpec = `{"tables": [
  {"name": "docs", "tenant_column": "org"},
  {"name": "notes", "tenant_column": "org"}
]}`

// X19.
func checkFKUnique(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		`CREATE TABLE docs (id bigserial PRIMARY KEY, org bigint NOT NULL, code text UNIQUE, title text)`,
		`CREATE TABLE notes (id bigserial PRIMARY KEY, org bigint NOT NULL, doc_code text REFERENCES docs (code) ON DELETE CASCADE, body text)`,
		`CREATE TABLE links (id bigserial PRIMARY KEY, doc_code text REFERENCES docs (code) ON DELETE RESTRICT)`,
		`ALTER TABLE docs OWNER TO app`, `ALTER TABLE notes OWNER TO app`, `ALTER TABLE links OWNER TO app`,
		`INSERT INTO docs (org, code, title) SELECT 1 + g % 2, 'c' || g, 'd' || g FROM generate_series(1, 40) g`,
		`INSERT INTO docs (org, code, title) VALUES (1, NULL, 'no code'), (2, NULL, 'no code')`,
		`INSERT INTO notes (org, doc_code, body) SELECT org, code, 'n-' || code FROM docs WHERE code IS NOT NULL`,
		`INSERT INTO links (doc_code) VALUES ('c3'), ('c4')`); err != nil {
		return false, "", err
	}
	e.spec = fkUniqueSpec
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
	exec := func(d, sql string) string {
		if err := execSQL(e.ctx, d, sql); err != nil {
			return "refused: " + firstErrLine(err)
		}
		return "accepted"
	}
	out, code := e.checkWith(fkUniqueSpec)
	note(!reported(out, "fk_not_to_id") && reported(out, "fk_to_unique_column") && code != 3 && code != 5,
		fmt.Sprintf("check: %s (exit %d), fk_not_to_id %v, fk_to_unique_column %v", verdict(out), code, reported(out, "fk_not_to_id"), reported(out, "fk_to_unique_column")))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	mapRows := val(e.cloud(), `SELECT count(*) FROM docs__refs__code`)
	missing := exec(e.cloud(), `INSERT INTO notes (org, doc_code, body) VALUES (1, 'nope', 'x')`)
	noCode := exec(e.cloud(), `INSERT INTO docs (org, code, title) VALUES (1, NULL, 'another without code')`)
	note(mapRows == "40" && strings.HasPrefix(missing, "refused") && noCode == "accepted",
		fmt.Sprintf("after enable: the map of docs.code %s rows; a note onto a missing code %s; a doc without code %s", mapRows, missing, noCode))

	if mv, err := e.move(1); err != nil {
		note(false, "move-org failed: "+lastErr(mv))
		return false, strings.Join(notes, "; "), nil
	}
	orgFK := val(e.customer(), `SELECT count(*) FROM pg_constraint WHERE conrelid = 'notes'::regclass AND confrelid = 'docs'::regclass AND contype = 'f'`)
	orgNotes := val(e.customer(), `SELECT count(*) FROM notes`)
	missing1 := exec(e.cloud(), `INSERT INTO notes (org, doc_code, body) VALUES (1, 'nope', 'x')`)
	note(orgFK == "1" && orgNotes == "20" && strings.HasPrefix(missing1, "refused"),
		fmt.Sprintf("after the move of organization 1: FK notes -> docs there %s, its notes %s; a note of organization 1 onto a missing code %s", orgFK, orgNotes, missing1))

	// CASCADE at the organization (its own FK) and in the cloud (onto the map); RESTRICT of
	// a cloud table onto a moved organization's doc and a cloud one's
	c1 := exec(e.cloud(), `DELETE FROM docs WHERE org = 1 AND code = 'c2'`)
	n1 := val(e.customer(), `SELECT count(*) FROM notes WHERE doc_code = 'c2'`)
	c2 := exec(e.cloud(), `DELETE FROM docs WHERE org = 2 AND code = 'c1'`)
	n2 := val(e.cloud(), `SELECT count(*) FROM notes WHERE doc_code = 'c1'`)
	r1 := exec(e.cloud(), `DELETE FROM docs WHERE org = 1 AND code = 'c4'`)
	r2 := exec(e.cloud(), `DELETE FROM docs WHERE org = 2 AND code = 'c3'`)
	note(c1 == "accepted" && n1 == "0" && c2 == "accepted" && n2 == "0" && strings.HasPrefix(r1, "refused") && strings.HasPrefix(r2, "refused"),
		fmt.Sprintf("delete a doc of organization 1: %s, its notes %s; of 2: %s, its notes %s; a doc links reference, of 1: %s; of 2: %s", c1, n1, c2, n2, r1, r2))

	// through the proxy, a note found by its doc's code alone goes to that doc's organization
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
	got := map[string]string{}
	for _, cd := range []string{"c6", "c5"} { // organization 1 (moved), organization 2
		res, err := simple(e.ctx, c, `SELECT body FROM notes WHERE doc_code = '`+cd+`'`)
		switch {
		case err != nil:
			got[cd] = "error: " + firstErrLine(err)
		case len(res) == 1 && len(res[0].Rows) == 1:
			got[cd] = string(res[0].Rows[0][0])
		default:
			got[cd] = fmt.Sprintf("%d rows", len(res[0].Rows))
		}
	}
	note(got["c6"] == "n-c6" && got["c5"] == "n-c5", fmt.Sprintf("through the proxy (reject), a note by its doc's code: c6 (organization 1) %q, c5 (organization 2) %q", got["c6"], got["c5"]))
	c.Close(e.ctx)
	stop()

	// move-back and disable: the FKs onto docs (code) again, the map gone
	if _, err := e.run6(e.moveBackArgs(1)...); err != nil {
		note(false, "move-back failed: "+errLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if _, err := e.run6(e.disableArgs()...); err != nil {
		note(false, "disable failed: "+errLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	fks := val(e.cloud(), `SELECT string_agg(conrelid::regclass::text || ' -> ' || confrelid::regclass::text, ', ' ORDER BY conrelid::regclass::text)
		FROM pg_constraint WHERE contype = 'f' AND confrelid = 'docs'::regclass`)
	gone := val(e.cloud(), `SELECT (to_regclass('docs__refs__code') IS NULL)::text`)
	r3 := exec(e.cloud(), `DELETE FROM docs WHERE code = 'c4'`)
	m3 := exec(e.cloud(), `INSERT INTO notes (org, doc_code, body) VALUES (1, 'nope', 'x')`)
	note(fks == "links -> docs, notes -> docs" && gone == "true" && strings.HasPrefix(r3, "refused") && strings.HasPrefix(m3, "refused"),
		fmt.Sprintf("after move-back and disable: FKs onto docs %s, the map gone %s; deleting a doc links reference %s; a note onto a missing code %s", fks, gone, r3, m3))
	return ok, strings.Join(notes, "; "), nil
}
