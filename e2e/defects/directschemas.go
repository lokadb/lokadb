package main

// Block 4 with block 10: the direct path (fdw+direct) in a database with schemas. app.documents
// is a LokaDB table moved to organization 1's database (schema app there too); public.documents
// is a cloud table of the same name. Statements of organization 1 on app.documents, qualified
// or bare by the session's search_path, go straight to its database and change the right rows;
// a bare documents that the path resolves to public's goes to the cloud.

import (
	"fmt"
	"strings"
)

func init() {
	checks = append(checks,
		check{id: "X21", name: "fdw+direct with schemas: reads, UPDATE, INSERT of app.documents and DELETE of app.items (qualified, bare under SET search_path) go straight to the organization's database; a DELETE of app.documents (LokaDB children) and a bare documents of public stay in the cloud's way", run: checkDirectSchemas},
	)
}

// X21.
func checkDirectSchemas(e *env) (bool, string, error) {
	if err := schemaTables(e); err != nil {
		return false, "", err
	}
	e.spec = schemaSpec
	stop, err := e.directSetup(nil)
	if err != nil {
		return false, "", err
	}
	defer stop()
	if out, err := e.orgProfile(1, "fdw+direct"); err != nil {
		return false, "org-profile fdw+direct refused: " + lastErr(out), nil
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM app.documents WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxyPgx("x21")
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	at := func(sql string) string { // at the organization
		v, err := one(e.ctx, e.customer(), sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	// step runs sql (with args) and says what it returned and whether it went the direct way
	step := func(sql string, args ...any) (string, bool) {
		before := directCount(e.proxyRoutes(), "1")
		var got string
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") || strings.Contains(strings.ToUpper(sql), "RETURNING") {
			var vals []string
			for _, r := range rowsText(e, c, sql, args...) {
				if !strings.HasPrefix(r, "tag: ") { // rowsText ends with the command tag
					vals = append(vals, r)
				}
			}
			got = strings.Join(vals, ",")
		} else if tag, err := c.Exec(e.ctx, sql, args...); err != nil {
			got = "error: " + firstErrLine(err)
		} else {
			got = tag.String()
		}
		return got, directCount(e.proxyRoutes(), "1") > before
	}
	if _, err := c.Exec(e.ctx, "SET search_path TO public"); err != nil {
		return false, "", err
	}
	v, d := step("SELECT title FROM app.documents WHERE org = $1 AND id = $2", 1, doc)
	note(v != "" && !strings.HasPrefix(v, "error") && d, fmt.Sprintf("qualified read of organization 1: %q, direct %v", v, d))
	v, d = step("UPDATE app.documents SET title = $1 WHERE org = 1 AND id = $2", "x21-upd", doc)
	note(d && at(fmt.Sprintf("SELECT title FROM app.documents WHERE id = %d", atoi(doc))) == "x21-upd", fmt.Sprintf("qualified UPDATE: %s, direct %v", v, d))
	v, d = step("SELECT count(*) FROM documents") // public.documents: the cloud's table
	note(v == "200" && !d, fmt.Sprintf("bare documents under search_path public (the cloud's public.documents): %s rows, direct %v", v, d))

	if _, err := c.Exec(e.ctx, "SET search_path TO app, public"); err != nil {
		return false, "", err
	}
	v, d = step("SELECT title FROM documents WHERE org = $1 AND id = $2", 1, doc)
	note(v == "x21-upd" && d, fmt.Sprintf("bare read under search_path app, public: %q, direct %v", v, d))
	v, d = step("INSERT INTO documents (org, title) VALUES ($1, $2) RETURNING id::text", 1, "x21-ins")
	nid := v
	note(d && at("SELECT count(*) FROM app.documents WHERE title = 'x21-ins'") == "1", fmt.Sprintf("bare INSERT: id %s, direct %v, at the organization %s", v, d, at("SELECT count(*) FROM app.documents WHERE title = 'x21-ins'")))
	// documents has LokaDB children (app.items, remarks: FKs onto its refs): its DELETE goes the
	// cloud's way, which runs their cascade (block 10, DP18); a leaf's DELETE goes straight
	v, d = step("DELETE FROM documents WHERE org = 1 AND id = $1", atoi(nid))
	note(!d && at("SELECT count(*) FROM app.documents WHERE title = 'x21-ins'") == "0", fmt.Sprintf("bare DELETE of documents (it has LokaDB children): %s, direct %v (want false)", v, d))
	item, err := one(e.ctx, e.cloud(), "SELECT max(id) FROM app.items WHERE org = 1")
	if err != nil {
		return false, "", err
	}
	v, d = step("DELETE FROM items WHERE org = 1 AND id = $1", atoi(item))
	note(d && at("SELECT count(*) FROM app.items WHERE id = "+item) == "0", fmt.Sprintf("bare DELETE of an item (a leaf): %s, direct %v", v, d))
	// the cloud's public.documents is untouched by all of it
	pub, err := one(e.ctx, e.cloud(), "SELECT count(*) || ' ' || count(*) FILTER (WHERE title LIKE 'x21%') FROM public.documents")
	if err != nil {
		return false, "", err
	}
	note(pub == "200 0", "the cloud's public.documents: rows and rows the check wrote "+pub)
	return ok, strings.Join(notes, "; ") + e.proxyLines("level=WARN"), nil
}

func atoi(s string) int64 {
	var n int64
	fmt.Sscan(s, &n)
	return n
}
