package main

// Block 1: a Rails-like schema without FKs. messages.conversation_id holds a conversation's
// id, but no FK says so; the conversation feed (WHERE conversation_id = $1) names no
// organization. lokadb check finds it, suggests the relation and proves on the data and on
// the workload that it closes the gap; enable takes the patch, and the proxy routes the feed
// under the reject policy, of a moved organization and of one in the cloud.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func init() {
	checks = append(checks,
		check{id: "K1", name: "a schema without FKs (Rails): check suggests the relation, the patched spec routes the feed under reject", run: checkRailsRelation},
	)
}

const railsSpec = `{"tables": [
  {"name": "conversations", "tenant_column": "account_id"},
  {"name": "messages", "tenant_column": "account_id"}
]}`

// the feed, as Rails sends it: the most frequent query of the application
const railsFeed = `SELECT "messages".* FROM "messages" WHERE "messages"."conversation_id" = $1 ORDER BY "messages"."id" ASC LIMIT $2`

func checkRailsRelation(e *env) (bool, string, error) {
	e.spec = railsSpec
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE conversations (id bigserial PRIMARY KEY, account_id bigint NOT NULL, title text)",
		"CREATE TABLE messages (id bigserial PRIMARY KEY, account_id bigint NOT NULL, conversation_id bigint, content text)",
		"CREATE INDEX ON messages (conversation_id)",
		// customer data with neither the organization nor an FK (Rails' contact_inboxes)
		"CREATE TABLE conversation_tags (id bigserial PRIMARY KEY, conversation_id bigint, tag text)",
		"INSERT INTO conversation_tags (conversation_id, tag) SELECT id, 'vip' FROM conversations",
		"ALTER TABLE conversations OWNER TO app", "ALTER TABLE messages OWNER TO app",
		"INSERT INTO conversations (account_id, title) SELECT 1 + g % 2, 'c' || g FROM generate_series(1, 20) g",
		"INSERT INTO messages (account_id, conversation_id, content) SELECT c.account_id, c.id, 'm' || g FROM conversations c, generate_series(1, 5) g"); err != nil {
		return false, "", err
	}
	dir := os.TempDir()
	rec := filepath.Join(dir, e.db+"-workload.json")
	w := map[string]any{"version": 1, "updated": "2026-10-23T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{
			{"sql": railsFeed, "count": 500},
			{"sql": `SELECT "conversations".* FROM "conversations" WHERE "conversations"."account_id" = $1 AND "conversations"."id" = $2 LIMIT $3`, "count": 100},
			{"sql": `INSERT INTO "messages" ("account_id", "conversation_id", "content") VALUES ($1, $2, $3) RETURNING "id"`, "count": 50},
		},
		"transactions": []any{},
	}}}
	cfg := filepath.Join(dir, e.db+"-check-proxy.json")
	for path, v := range map[string]any{rec: w, cfg: map[string]any{"fanout": map[string]any{"*": map[string]string{"mode": "reject"}}}} {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return false, "", err
		}
	}
	patch := filepath.Join(dir, e.db+"-patch.json")
	_ = os.Remove(patch)

	var notes []string
	ok := true
	note := func(good bool, s string) {
		if !good {
			ok = false
			s = "WRONG " + s
		}
		notes = append(notes, s)
	}
	// 1. check (the open one): the feed is refused, conversation_tags is told
	out, _ := e.checkWith(railsSpec, "-workload", rec, "-proxy-config", cfg)
	note(reported(out, "query_refused"), "check: the feed refused: "+finding(out, "query_refused"))
	note(reported(out, "unlinked_table"), "check: conversation_tags without organization and FK: "+finding(out, "unlinked_table"))
	p, _ := e.specFile(railsSpec)
	// 2. the relation is suggested with its data check (-suggest), and judged as if declared
	// (-patch): nothing is refused any more
	out, _ = e.checkWith(railsSpec, "-workload", rec, "-proxy-config", cfg, "-suggest", patch)
	b, err := os.ReadFile(patch)
	suggested := err == nil && strings.Contains(string(b), `"messages.conversation_id"`) && strings.Contains(string(b), `"conversations.id"`)
	note(suggested && reported(out, "routing_suggestion") && strings.Contains(out, "100 of 100 rows have the parent, 0 of them in another organization"),
		"check -suggest: "+finding(out, "routing_suggestion"))
	out, _ = e.checkWith(railsSpec, "-workload", rec, "-proxy-config", cfg, "-patch", patch)
	note(!reported(out, "query_refused") && reported(out, "relation_declared"), "check -patch: refused "+finding(out, "query_refused")+", "+finding(out, "relation_declared"))
	// 3. enable with the patch
	if _, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-patch", patch, "-owner", "app"); err != nil {
		note(false, "enable -patch: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	// move organization 1, the proxy under reject
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	feed := func(org int) string {
		conv, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM conversations WHERE account_id = $1", org)
		if err != nil {
			return "error: " + firstErrLine(err)
		}
		res := c.ExecParams(e.ctx, railsFeed, [][]byte{[]byte(conv), []byte("20")}, nil, nil, nil).Read()
		if res.Err != nil {
			var pe *pgconn.PgError
			if errors.As(res.Err, &pe) {
				return "refused: " + pe.Message
			}
			return "error: " + firstErrLine(res.Err)
		}
		return fmt.Sprintf("%d rows", len(res.Rows))
	}
	f1, f2 := feed(1), feed(2)
	note(f1 == "5 rows" && f2 == "5 rows", fmt.Sprintf("the feed through the proxy (reject): moved organization %s, cloud organization %s", f1, f2))
	// Rails' where(conversation_id: account.conversations): the subquery names the organization
	sub := c.ExecParams(e.ctx, `SELECT "messages".* FROM "messages" WHERE "messages"."conversation_id" IN (SELECT "conversations"."id" FROM "conversations" WHERE "conversations"."account_id" = $1)`,
		[][]byte{[]byte("1")}, nil, nil, nil).Read()
	if sub.Err == nil {
		note(len(sub.Rows) == 50, fmt.Sprintf("conversation_id IN (SELECT id … account_id = 1): %d rows", len(sub.Rows)))
	} else {
		note(false, "conversation_id IN (SELECT id … account_id = 1): "+firstErrLine(sub.Err))
	}
	conv, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM conversations WHERE account_id = 1")
	ins := c.ExecParams(e.ctx, `INSERT INTO "messages" ("account_id", "conversation_id", "content") VALUES ($1, $2, $3) RETURNING "id"`,
		[][]byte{[]byte("1"), []byte(conv), []byte("new")}, nil, nil, nil).Read()
	if ins.Err == nil {
		n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM messages WHERE content = 'new'")
		note(n == "1", "a new message of organization 1 in its database: "+n)
	} else {
		note(false, "insert: "+firstErrLine(ins.Err))
	}
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K2", name: "a view, a function and a DO block over a LokaDB table go through the fanout policy, not to every organization unasked", run: checkViewsFuncsDo},
	)
}

func checkViewsFuncsDo(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if err := execSQL(e.ctx, e.cloud(),
		"SET ROLE app",
		"CREATE VIEW recent_documents AS SELECT * FROM documents WHERE id > 0",
		"CREATE FUNCTION doc_count(org bigint) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN (SELECT count(*) FROM documents WHERE organization_id = org); END $$"); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
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
				return "refused"
			}
			return "error: " + firstErrLine(err)
		}
		if len(res) > 0 && len(res[len(res)-1].Rows) == 1 {
			return string(res[len(res)-1].Rows[0][0])
		}
		return "ok"
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
	v := ask("SELECT count(*) FROM recent_documents")
	note(v == "refused", "view without the organization: "+v)
	v = ask("SELECT count(*) FROM recent_documents r JOIN documents d ON d.id = r.id WHERE d.organization_id = 1")
	note(v == "100", "view joined with documents of organization 1: "+v)
	v = ask("SELECT doc_count(2)")
	note(v == "refused", "function over documents without the organization: "+v)
	v = ask("DO $$ BEGIN UPDATE documents SET title = title || '' WHERE id = 1; END $$")
	note(v == "refused", "DO block writing documents: "+v)
	v = ask("DO $$ BEGIN PERFORM 1; END $$")
	note(v == "ok", "DO block of nothing LokaDB: "+v)

	// check after enable reads the same views and functions as the proxy
	dir := os.TempDir()
	rec := filepath.Join(dir, e.db+"-workload.json")
	cfg := filepath.Join(dir, e.db+"-check-proxy.json")
	w := map[string]any{"version": 1, "updated": "2026-10-23T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{{"sql": "SELECT count(*) FROM recent_documents", "count": 5}}, "transactions": []any{}}}}
	for path, v := range map[string]any{rec: w, cfg: map[string]any{"fanout": map[string]any{"*": map[string]string{"mode": "reject"}}}} {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return false, "", err
		}
	}
	out, _ := e.runCheck("-db", e.cloud(), "-workload", rec, "-proxy-config", cfg)
	note(reported(out, "query_refused"), "check: the view query refused: "+finding(out, "query_refused"))
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K3", name: "a cloud table that knows the organization (projects.account_id): check suggests cloud_tables, the patched spec routes a JOIN through it", run: checkCloudTable},
	)
}

const cloudSpec = `{"tables": [{"name": "tasks", "tenant_column": "account_id"}]}`

// the board of a project: the condition is on the cloud table only
const projectBoard = `SELECT "tasks".* FROM "tasks" INNER JOIN "projects" ON "projects"."id" = "tasks"."project_id" WHERE "projects"."id" = $1`

func checkCloudTable(e *env) (bool, string, error) {
	e.spec = cloudSpec
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE projects (id bigserial PRIMARY KEY, account_id bigint NOT NULL, name text)",
		"CREATE TABLE tasks (id bigserial PRIMARY KEY, account_id bigint NOT NULL, project_id bigint REFERENCES projects, title text)",
		"ALTER TABLE projects OWNER TO app", "ALTER TABLE tasks OWNER TO app",
		"INSERT INTO projects (account_id, name) SELECT 1 + g % 2, 'p' || g FROM generate_series(1, 6) g",
		"INSERT INTO tasks (account_id, project_id, title) SELECT p.account_id, p.id, 't' || g FROM projects p, generate_series(1, 4) g"); err != nil {
		return false, "", err
	}
	dir := os.TempDir()
	rec := filepath.Join(dir, e.db+"-workload.json")
	cfg := filepath.Join(dir, e.db+"-check-proxy.json")
	patch := filepath.Join(dir, e.db+"-patch.json")
	_ = os.Remove(patch)
	w := map[string]any{"version": 1, "updated": "2026-10-23T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{{"sql": projectBoard, "count": 30}}, "transactions": []any{}}}}
	for path, v := range map[string]any{rec: w, cfg: map[string]any{"fanout": map[string]any{"*": map[string]string{"mode": "reject"}}}} {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return false, "", err
		}
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
	out, _ := e.checkWith(cloudSpec, "-workload", rec, "-proxy-config", cfg)
	note(reported(out, "query_refused"), "check: refused "+finding(out, "query_refused"))
	sout, _ := e.checkWith(cloudSpec, "-workload", rec, "-proxy-config", cfg, "-suggest", patch)
	b, err := os.ReadFile(patch)
	suggested := err == nil && strings.Contains(string(b), `"cloud_tables"`) && strings.Contains(string(b), `"projects"`)
	p, _ := e.specFile(cloudSpec)
	args := []string{"enable", "-db", e.cloud(), "-spec", p, "-owner", "app"}
	note(suggested, "check -suggest: "+finding(sout, "routing_suggestion"))
	out, _ = e.checkWith(cloudSpec, "-workload", rec, "-proxy-config", cfg, "-patch", patch)
	note(!reported(out, "query_refused") && reported(out, "cloud_table_declared"), "check -patch: refused "+finding(out, "query_refused"))
	if suggested {
		args = append(args, "-patch", patch)
	}
	if _, err := e.lokadb(args...); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, org := range []int{1, 2} {
		pid, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM projects WHERE account_id = $1", org)
		res := c.ExecParams(e.ctx, projectBoard, [][]byte{[]byte(pid)}, nil, nil, nil).Read()
		got := fmt.Sprintf("%d rows", len(res.Rows))
		if res.Err != nil {
			got = "error: " + firstErrLine(res.Err)
		}
		note(got == "4 rows", fmt.Sprintf("the board of a project of organization %d through the proxy (reject): %s", org, got))
	}
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K4", name: "a key (documents.slug): WHERE slug = $1 is routed, a changed slug follows, a slug stays unique across organizations", run: checkKey},
	)
}

func checkKey(e *env) (bool, string, error) {
	e.spec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"}
], "routing": {"keys": [{"table": "documents", "column": "slug"}]}}`
	if err := execSQL(e.ctx, e.cloud(),
		"ALTER TABLE documents ADD COLUMN slug text UNIQUE",
		"UPDATE documents SET slug = 'doc-' || id"); err != nil {
		return false, "", err
	}
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
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
	run := func(sql string, args ...string) string {
		var params [][]byte
		for _, a := range args {
			params = append(params, []byte(a))
		}
		res := c.ExecParams(e.ctx, sql, params, nil, nil, nil).Read()
		if res.Err != nil {
			var pe *pgconn.PgError
			if errors.As(res.Err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
				return "refused"
			}
			return "error: " + firstErrLine(res.Err)
		}
		if len(res.Rows) == 1 && len(res.Rows[0]) == 1 {
			return string(res.Rows[0][0])
		}
		return fmt.Sprintf("%d rows", len(res.Rows))
	}
	s1, _ := one(e.ctx, e.cloud(), "SELECT min(slug) FROM documents WHERE organization_id = 1")
	s2, _ := one(e.ctx, e.cloud(), "SELECT min(slug) FROM documents WHERE organization_id = 2")
	const bySlug = `SELECT organization_id FROM documents WHERE slug = $1`
	note(run(bySlug, s1) == "1" && run(bySlug, s2) == "2", "by slug (reject): organization 1 "+run(bySlug, s1)+", organization 2 "+run(bySlug, s2))
	id1, _ := one(e.ctx, e.cloud(), "SELECT id FROM documents WHERE slug = $1", s1)
	r := run(`UPDATE documents SET slug = $1 WHERE id = $2`, "renamed-1", id1)
	note(r == "0 rows", "rename the slug of a moved document: "+r)
	note(run(bySlug, "renamed-1") == "1", "by the new slug: "+run(bySlug, "renamed-1"))
	note(run(bySlug, s1) == "0 rows", "by the old slug: "+run(bySlug, s1))
	r = run(`INSERT INTO documents (organization_id, user_id, title, slug) VALUES (2, 2, 'dup', $1)`, "renamed-1")
	note(strings.HasPrefix(r, "error:") && strings.Contains(r, "duplicate key"), "the same slug in organization 2: "+r)
	r = run(`INSERT INTO documents (organization_id, user_id, title, slug) VALUES (2, 2, 'new', 'brand-new')`)
	note(r == "0 rows" && run(bySlug, "brand-new") == "2", "a new slug of organization 2: "+r+", found in "+run(bySlug, "brand-new"))
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K5", name: "INSERT … SELECT and a correlated subquery take the organization of their source under reject", run: checkInsertSelectCorrelated},
	)
}

func checkInsertSelectCorrelated(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	run := func(sql string, args ...string) string {
		var params [][]byte
		for _, a := range args {
			params = append(params, []byte(a))
		}
		res := c.ExecParams(e.ctx, sql, params, nil, nil, nil).Read()
		if res.Err != nil {
			var pe *pgconn.PgError
			if errors.As(res.Err, &pe) && strings.HasPrefix(pe.Message, "lokadb:") {
				return "refused"
			}
			return "error: " + firstErrLine(res.Err)
		}
		if len(res.Rows) == 1 && len(res.Rows[0]) == 1 {
			return string(res.Rows[0][0])
		}
		return fmt.Sprintf("%d rows", len(res.Rows))
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
	for _, org := range []string{"1", "2"} {
		r := run(`SELECT count(*) FROM documents d WHERE d.organization_id = $1 AND EXISTS (SELECT 1 FROM document_versions v WHERE v.document_id = d.id)`, org)
		note(r == "100", "organization "+org+", documents with a version (correlated EXISTS): "+r)
		doc, _ := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = $1", org)
		r = run(`INSERT INTO document_versions (document_id, content) SELECT id, 'copy-' || title FROM documents WHERE id = $1`, doc)
		n, _ := one(e.ctx, e.cloud(), "SELECT count(*) FROM document_versions WHERE content LIKE 'copy-%' AND organization_id = $1", org)
		note(r == "0 rows" && n == "1", "organization "+org+", INSERT … SELECT of a version: "+r+", rows of the organization: "+n)
	}
	n, _ := one(e.ctx, e.customer(), "SELECT count(*) FROM document_versions WHERE content LIKE 'copy-%'")
	note(n == "1", "copies in organization 1's database: "+n)
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K6", name: "a relation to a table whose key is not id (customers.customer_id) routes; one to a column that is not the key is refused by enable", run: checkRelationToKey},
	)
}

const keySpec = `{"tables": [
  {"name": "customers", "tenant_column": "account_id"},
  {"name": "orders", "tenant_column": "account_id"}
]}`

const ordersOfCustomer = `SELECT "orders".* FROM "orders" WHERE "orders"."customer_id" = $1`

func checkRelationToKey(e *env) (bool, string, error) {
	e.spec = keySpec
	if err := execSQL(e.ctx, e.cloud(),
		"CREATE TABLE customers (customer_id bigserial PRIMARY KEY, account_id bigint NOT NULL, legacy_no bigint, name text)",
		"CREATE TABLE orders (id bigserial PRIMARY KEY, account_id bigint NOT NULL, customer_id bigint, total int)",
		"ALTER TABLE customers OWNER TO app", "ALTER TABLE orders OWNER TO app",
		"INSERT INTO customers (account_id, legacy_no, name) SELECT 1 + g % 2, g, 'c' || g FROM generate_series(1, 10) g",
		"INSERT INTO orders (account_id, customer_id, total) SELECT c.account_id, c.customer_id, g FROM customers c, generate_series(1, 3) g"); err != nil {
		return false, "", err
	}
	dir := os.TempDir()
	rec := filepath.Join(dir, e.db+"-workload.json")
	cfg := filepath.Join(dir, e.db+"-check-proxy.json")
	patch := filepath.Join(dir, e.db+"-patch.json")
	bad := filepath.Join(dir, e.db+"-bad-patch.json")
	_ = os.Remove(patch)
	w := map[string]any{"version": 1, "updated": "2026-10-23T00:00:00Z", "databases": map[string]any{e.db: map[string]any{
		"statements": []map[string]any{{"sql": ordersOfCustomer, "count": 90}}, "transactions": []any{}}}}
	for path, v := range map[string]any{rec: w, cfg: map[string]any{"fanout": map[string]any{"*": map[string]string{"mode": "reject"}}},
		bad: map[string]any{"routing": map[string]any{"relations": []map[string]string{{"from": "orders.customer_id", "to": "customers.legacy_no"}}}}} {
		b, _ := json.Marshal(v)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			return false, "", err
		}
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
	out, _ := e.checkWith(keySpec, "-workload", rec, "-proxy-config", cfg, "-suggest", patch)
	b, err := os.ReadFile(patch)
	suggested := err == nil && strings.Contains(string(b), `"customers.customer_id"`)
	note(suggested, "check -suggest to the key: "+finding(out, "routing_suggestion"))
	out, _ = e.checkWith(keySpec, "-workload", rec, "-proxy-config", cfg, "-patch", bad)
	note(reported(out, "routing_invalid"), "check -patch to a column that is not the key: "+finding(out, "routing_invalid"))
	p, _ := e.specFile(keySpec)
	eo, eerr := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-patch", bad, "-owner", "app")
	note(eerr != nil && strings.Contains(eo, "its key"), "enable refuses the relation to a column that is not the key: "+oneLine(eo))
	if !suggested {
		return false, strings.Join(notes, "; "), nil
	}
	if _, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-patch", patch, "-owner", "app"); err != nil {
		note(false, "enable -patch: "+firstErrLine(err))
		return false, strings.Join(notes, "; "), nil
	}
	if _, err := e.move(1); err != nil {
		return false, "", err
	}
	stop, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, org := range []int{1, 2} {
		cid, _ := one(e.ctx, e.cloud(), "SELECT min(customer_id) FROM customers WHERE account_id = $1", org)
		res := c.ExecParams(e.ctx, ordersOfCustomer, [][]byte{[]byte(cid)}, nil, nil, nil).Read()
		got := fmt.Sprintf("%d rows", len(res.Rows))
		if res.Err != nil {
			got = "error: " + firstErrLine(res.Err)
		}
		note(got == "3 rows", fmt.Sprintf("orders of a customer of organization %d through the proxy (reject): %s", org, got))
	}
	return ok, strings.Join(notes, "; "), nil
}

func init() {
	checks = append(checks,
		check{id: "K7", name: "keys and relations in enable -prepare: the map is filled under writes, the switch registers it; a relation is checked outside the lock", run: checkPrepareRouting},
	)
}

const prepRoutingSpec = `{"tables": [
  {"name": "documents", "tenant_column": "organization_id"},
  {"name": "document_versions", "parent_table": "documents", "parent_column": "document_id"},
  {"name": "notes", "tenant_column": "organization_id"}
], "routing": {
  "keys": [{"table": "documents", "column": "slug"}],
  "relations": [{"from": "notes.document_id", "to": "documents.id"}]
}}`

func checkPrepareRouting(e *env) (bool, string, error) {
	w, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer w.Close(e.ctx)
	for _, s := range []string{
		"ALTER TABLE documents ADD COLUMN slug text UNIQUE",
		"CREATE TABLE notes (id bigserial PRIMARY KEY, organization_id bigint NOT NULL, document_id bigint, body text)",
		"ALTER TABLE notes OWNER TO app",
		"INSERT INTO documents (organization_id, user_id, title) SELECT 1 + g % 2, 2, 'bulk' FROM generate_series(1, 50000) g",
		"UPDATE documents SET slug = 'doc-' || id",
		"INSERT INTO notes (organization_id, document_id, body) SELECT organization_id, id, 'n' FROM documents",
	} {
		if _, err := w.Exec(e.ctx, s); err != nil {
			return false, "", fmt.Errorf("%w\n  in: %s", err, s)
		}
	}
	e.spec = prepRoutingSpec
	p, err := e.specFile(prepRoutingSpec)
	if err != nil {
		return false, "", err
	}
	// the application keeps writing the slugs while -prepare runs
	var (
		stop   = make(chan struct{})
		wg     sync.WaitGroup
		writes int
		fails  []string
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			var err error
			switch i % 3 {
			case 0:
				_, err = w.Exec(e.ctx, "INSERT INTO documents (organization_id, user_id, title, slug) VALUES (1 + $1::int % 2, 2, 'live', 'live-' || $1::int)", i)
			case 1:
				_, err = w.Exec(e.ctx, "UPDATE documents SET slug = 'ren-' || id WHERE id = (SELECT min(id) FROM documents WHERE slug LIKE 'doc-%')")
			case 2:
				_, err = w.Exec(e.ctx, "DELETE FROM notes WHERE id = (SELECT max(id) FROM notes WHERE body = 'n'); DELETE FROM documents WHERE id = (SELECT max(id) FROM documents WHERE slug LIKE 'doc-%' AND id NOT IN (SELECT document_id FROM notes WHERE document_id IS NOT NULL) AND id NOT IN (SELECT document_id FROM document_versions WHERE document_id IS NOT NULL))")
			}
			if err != nil && len(fails) < 3 {
				fails = append(fails, firstErrLine(err))
			}
			writes++
			time.Sleep(2 * time.Millisecond)
		}
	}()
	_, prepErr := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app", "-prepare")
	close(stop)
	wg.Wait()
	if prepErr != nil {
		return false, "enable -prepare failed: " + oneLine(prepErr.Error()), nil
	}
	if _, err := e.lokadb("enable", "-db", e.cloud(), "-spec", p, "-owner", "app"); err != nil {
		return false, "the switch failed: " + oneLine(err.Error()), nil
	}
	var notes []string
	ok := len(fails) == 0
	notes = append(notes, fmt.Sprintf("%d writes during -prepare, failed: %v", writes, fails))
	expect := func(what, sql, want string) {
		got, err := one(e.ctx, e.cloud(), sql)
		if err != nil {
			got = "error: " + firstErrLine(err)
		}
		s := fmt.Sprintf("%s: %s", what, got)
		if got != want {
			ok = false
			s = "WRONG " + s + " (want " + want + ")"
		}
		notes = append(notes, s)
	}
	expect("map of slug differs from documents", "SELECT count(*) FROM documents d FULL JOIN lokadb.documents__key__slug m ON m.id = d.slug WHERE d.id IS NULL OR (d.slug IS NOT NULL AND (m.id IS NULL OR m.tenant IS DISTINCT FROM d.organization_id))", "0")
	expect("lookups registered", "SELECT string_agg(table_name || '.' || column_name || '->' || refs_table, ',' ORDER BY table_name, column_name) FROM lokadb.lookups WHERE column_name IN ('slug', 'document_id') AND table_name IN ('documents', 'notes')",
		"documents.slug->lokadb.documents__key__slug,notes.document_id->documents__refs")
	expect("routing rows left by -prepare", "SELECT count(*) FROM lokadb.routing WHERE kind IN ('key_prepared', 'relation_checked')", "0")
	expect("triggers of -prepare left", "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'lokadb\\_prep%'", "0")
	expect("triggers keeping the map on documents", "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'documents'::regclass AND tgname = 'lokadb_key_slug'", "1")
	// the key and the relation route under reject, for a moved organization
	if _, err := e.move(1); err != nil {
		return false, strings.Join(notes, "; ") + "; move-org: " + firstErrLine(err), nil
	}
	stop2, err := e.proxyCfg(map[string]string{}, map[string]any{"fanout": map[string]any{e.db: map[string]string{"mode": "reject"}}})
	if err != nil {
		return false, "", err
	}
	defer stop2()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	slug, _ := one(e.ctx, e.cloud(), "SELECT min(slug) FROM documents WHERE organization_id = 1 AND slug IS NOT NULL")
	res := c.ExecParams(e.ctx, "SELECT organization_id FROM documents WHERE slug = $1", [][]byte{[]byte(slug)}, nil, nil, nil).Read()
	got := fmt.Sprintf("%d rows", len(res.Rows))
	if res.Err != nil {
		got = "error: " + firstErrLine(res.Err)
	} else if len(res.Rows) == 1 {
		got = string(res.Rows[0][0])
	}
	if got != "1" {
		ok = false
		got = "WRONG " + got
	}
	notes = append(notes, "by slug through the proxy (reject): "+got)
	doc, _ := one(e.ctx, e.cloud(), "SELECT min(document_id) FROM notes WHERE organization_id = 1 AND document_id IS NOT NULL")
	res = c.ExecParams(e.ctx, "SELECT count(*) FROM notes WHERE document_id = $1", [][]byte{[]byte(doc)}, nil, nil, nil).Read()
	got = "error"
	if res.Err != nil {
		got = "error: " + firstErrLine(res.Err)
	} else if len(res.Rows) == 1 {
		got = string(res.Rows[0][0])
	}
	if got != "1" {
		ok = false
		got = "WRONG " + got
	}
	notes = append(notes, "notes of a document through the proxy (reject): "+got)
	return ok, strings.Join(notes, "; "), nil
}
