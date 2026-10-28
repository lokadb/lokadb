package main

// Rows of no organization (NULL in the organization column, child rows without a parent):
// they stay in the cloud, the application keeps seeing them, no move takes them.

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "N1", name: "rows of no organization: check, enable, the proxy and a move keep them in the cloud", run: checkNoOrganization},
	)
}

func checkNoOrganization(e *env) (bool, string, error) {
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents ALTER COLUMN organization_id DROP NOT NULL",
		"INSERT INTO documents (organization_id, user_id, title) VALUES (NULL, 2, 'noorg-a'), (NULL, 2, 'noorg-b')",
		"INSERT INTO document_versions (document_id, content) SELECT id, 'v-noorg' FROM documents WHERE organization_id IS NULL",
		"INSERT INTO document_versions (document_id, content) VALUES (NULL, 'v-orphan')"); err != nil {
		return false, "", err
	}
	id := func(sql string) string { v, _ := one(e.ctx, e.cloud(), sql); return v }
	a, b := id("SELECT id FROM documents WHERE title = 'noorg-a'"), id("SELECT id FROM documents WHERE title = 'noorg-b'")
	of1, of2 := id("SELECT min(id) FROM documents WHERE organization_id = 1"), id("SELECT min(id) FROM documents WHERE organization_id = 2")
	gone := id("SELECT max(id) FROM documents WHERE organization_id = 2 AND id IN (SELECT document_id FROM document_versions)")

	out, _ := e.checkWith(spec)
	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	blockers := reported(out, "tenant_null") && strings.Contains(sectionOf(out, "Blockers"), "tenant_null") ||
		reported(out, "orphan_rows") && strings.Contains(sectionOf(out, "Blockers"), "orphan_rows")
	note(!blockers && reported(out, "tenant_null") && reported(out, "orphan_rows"), "check: "+verdict(out)+", tenant_null and orphan_rows reported, as blockers: "+fmt.Sprint(blockers))
	if err := e.enable(); err != nil {
		note(false, "enable failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	count := func(d, sql string) string {
		v, err := one(e.ctx, d, sql)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		return v
	}
	note(count(e.cloud(), "SELECT count(*) FROM documents__none") == "2" && count(e.cloud(), "SELECT count(*) FROM document_versions__none") == "3",
		"after enable documents__none/document_versions__none: "+count(e.cloud(), "SELECT count(*) FROM documents__none")+"/"+count(e.cloud(), "SELECT count(*) FROM document_versions__none"))

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
	ask := func(q string) string {
		res, err := simple(e.ctx, c, q)
		if err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
				return "refused: " + pe.Message
			}
			return "error: " + firstErrLine(err)
		}
		if len(res) > 0 && len(res[len(res)-1].Rows) == 1 {
			return string(res[len(res)-1].Rows[0][0])
		}
		return "ran"
	}
	note(ask("SELECT count(*) FROM documents") == "202", "proxy, all documents: "+ask("SELECT count(*) FROM documents"))
	note(ask("SELECT title FROM documents WHERE id = "+a) == "noorg-a", "proxy, a document of no organization by id: "+ask("SELECT title FROM documents WHERE id = "+a))
	note(ask("SELECT count(*) FROM document_versions WHERE document_id = "+a) == "1", "its versions: "+ask("SELECT count(*) FROM document_versions WHERE document_id = "+a))
	note(ask("SELECT count(*) FROM documents WHERE organization_id = 1") == "100", "organization 1: "+ask("SELECT count(*) FROM documents WHERE organization_id = 1"))

	if _, err := e.move(1); err != nil {
		note(false, "move-org failed: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	note(count(e.cloud(), "SELECT count(*) FROM documents__none") == "2" && count(e.customer(), "SELECT count(*) FROM documents") == "100" &&
		count(e.customer(), "SELECT count(*) FROM documents WHERE organization_id IS NULL") == "0",
		"after the move: in the cloud "+count(e.cloud(), "SELECT count(*) FROM documents__none")+" of no organization, at organization 1 "+count(e.customer(), "SELECT count(*) FROM documents"))
	// an organization's statement still reads its own partition only (and the small __none)
	plan, err := planAs(e, "1", "SELECT count(*) FROM documents")
	// (DEFAULT too: the policy's range never prunes it, as before)
	note(err == nil && strings.Contains(plan, "documents__org1") && !strings.Contains(plan, "documents__none"),
		fmt.Sprintf("plan of organization 1 reads documents__org1 %v, documents__none %v, DEFAULT %v", strings.Contains(plan, "documents__org1"), strings.Contains(plan, "documents__none"), strings.Contains(plan, "documents__cloud")))

	// new rows of no organization
	note(ask("INSERT INTO document_versions (document_id, content) VALUES (NULL, 'new-orphan')") == "ran", "insert a version without a document: "+ask("SELECT count(*) FROM document_versions WHERE content = 'new-orphan'"))
	note(ask("INSERT INTO document_versions (document_id, content) VALUES ("+b+", 'under-noorg')") == "ran", "insert a version of a document of no organization")
	note(ask("INSERT INTO documents (organization_id, user_id, title) VALUES (NULL, 2, 'noorg-c')") == "ran", "insert a document of no organization")
	// a new parent of another organization is refused, of the same (none) is not
	u1 := ask("UPDATE document_versions SET document_id = " + of1 + " WHERE content = 'v-orphan'")
	note(strings.HasPrefix(u1, "refused") && strings.Contains(u1, "belongs to organization 1"), "an orphan given a document of the moved organization 1: "+u1)
	note(ask("UPDATE document_versions SET document_id = "+b+" WHERE content = 'v-orphan'") == "ran", "an orphan given a document of no organization")
	u3 := ask("UPDATE document_versions SET document_id = " + of1 + " WHERE document_id = " + of2)
	note(strings.HasPrefix(u3, "refused") && strings.Contains(u3, "belongs to organization 1"), "a version of organization 2 given a document of 1: "+u3)
	// ON DELETE SET NULL: the version keeps its organization
	d := ask("DELETE FROM documents WHERE id = " + gone)
	note(d == "ran" && count(e.cloud(), "SELECT count(*) FROM document_versions WHERE document_id IS NULL AND organization_id = 2") != "0",
		"a document of organization 2 deleted: "+d+", its versions keep organization 2: "+count(e.cloud(), "SELECT count(*) FROM document_versions WHERE document_id IS NULL AND organization_id = 2"))
	note(count(e.cloud(), "SELECT count(*) FROM documents__none") == "3" && count(e.cloud(), "SELECT count(*) FROM document_versions__none") == "5",
		"in the end documents__none/document_versions__none: "+count(e.cloud(), "SELECT count(*) FROM documents__none")+"/"+count(e.cloud(), "SELECT count(*) FROM document_versions__none"))
	return ok, strings.Join(notes, "; "), nil
}

// sectionOf: the lines of one section of a check report.
func sectionOf(out, title string) string {
	_, rest, found := strings.Cut(out, "\n"+title)
	if !found {
		return ""
	}
	sec, _, _ := strings.Cut(rest, "\n\n")
	return sec
}

// planAs: EXPLAIN of sql as the application with lokadb.org = org.
func planAs(e *env, org, sql string) (string, error) {
	c, err := pgconn.Connect(e.ctx, dsn("cloud", e.db, "app"))
	if err != nil {
		return "", err
	}
	defer c.Close(e.ctx)
	res, err := c.Exec(e.ctx, "SELECT set_config('lokadb.org', '"+org+"', false); EXPLAIN "+sql).ReadAll()
	if err != nil {
		return "", err
	}
	var lines []string
	for _, r := range res[len(res)-1].Rows {
		lines = append(lines, string(r[0]))
	}
	return strings.Join(lines, "\n"), nil
}
