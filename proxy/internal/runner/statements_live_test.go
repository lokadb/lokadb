package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// The owner's path: the database is enabled, and he chooses "the column names the customer" (and a
// cloud table) for a statement of the review. Decide then runs ApplyRouting on prepared tables.
// Needs INIT_TEST_DSN, a superuser's DSN of a disposable Postgres.
func TestDecideOnEnabledDatabase(t *testing.T) {
	base := os.Getenv("INIT_TEST_DSN")
	if base == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	u, _ := url.Parse(base)
	u.Path = "/postgres"
	root, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close(ctx)
	_, _ = root.Exec(ctx, "DROP DATABASE IF EXISTS dec_en WITH (FORCE)")
	if _, err := root.Exec(ctx, "CREATE DATABASE dec_en"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec(context.Background(), "DROP DATABASE IF EXISTS dec_en WITH (FORCE)") })
	_, _ = root.Exec(ctx, "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dec_owner') THEN CREATE ROLE dec_owner; END IF; END $$")
	u.Path = "/dec_en"
	dsn := u.String()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, q := range []string{
		`CREATE TABLE builds (id bigint PRIMARY KEY, team_id bigint NOT NULL, slug text UNIQUE, note text)`,
		`CREATE TABLE plans (id bigint PRIMARY KEY, team_id bigint NOT NULL, name text)`,
		`INSERT INTO builds SELECT g, CASE WHEN g <= 3 THEN 7 ELSE 8 END, 'b' || g, 'n' FROM generate_series(1, 6) g`,
		`INSERT INTO plans VALUES (1, 7, 'free'), (2, 8, 'pro')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "builds", TenantColumn: "team_id"}}}
	if err := admin.Enable(ctx, dsn, spec, "dec_owner", quiet); err != nil {
		t.Fatalf("enable: %v", err)
	}
	// the proxy's view of the statement: before, it names no customer
	routed := func(sql string) bool {
		m, err := proxy.LoadMeta(func(q string) ([][]string, error) {
			rows, err := db.Query(ctx, q)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			var out [][]string
			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return nil, err
				}
				r := make([]string, len(vals))
				for i, v := range vals {
					if v != nil {
						r[i] = toText(v)
					}
				}
				out = append(out, r)
			}
			return out, rows.Err()
		})
		if err != nil {
			t.Fatal(err)
		}
		return !proxy.Analyze(sql, m).Unhinted
	}
	const bySlug = "SELECT * FROM builds WHERE slug = $1"
	if routed(bySlug) {
		t.Fatal("the statement is routed before anything was decided")
	}
	dir := useDir(t)
	c := &Config{State: t.TempDir(), DB: dsn, Log: quiet, found: &Found{DBName: "dec_en"}, spec: spec.Sorted()}
	o := &Ops{Cfg: c}
	slugItem := proxy.Unrouted{Fingerprint: "s1", SQL: bySlug, Full: bySlug, Count: 3, Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "builds", Column: "slug", Form: "="}}}
	planItem := proxy.Unrouted{Fingerprint: "p1", SQL: "SELECT * FROM plans WHERE id = $1", Full: "SELECT * FROM plans WHERE id = $1", Count: 2,
		Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "plans", Column: "id", Form: "=", Cloud: true}}}
	c.collectUnrouted([]proxy.Unrouted{slugItem, planItem}, 0)
	decide := func(fp, choice string) cloudproto.DecideResult {
		t.Helper()
		res, err := o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Fingerprint: fp, Choice: choice}}})
		if err != nil {
			t.Fatalf("decide %s %s: %v", fp, choice, err)
		}
		if len(res.Applied) != 1 || len(res.Failed) != 0 {
			t.Fatalf("decide %s %s: %+v", fp, choice, res)
		}
		return res
	}
	first := decide("s1", "key")
	if !routed(bySlug) {
		t.Fatal("after deciding, the statement still names no customer")
	}
	var sp admin.Spec
	if err := json.Unmarshal(first.Spec, &sp); err != nil || sp.Routing == nil || len(sp.Routing.Keys) != 1 {
		t.Fatalf("spec: %v %s", err, first.Spec)
	}
	mapRows := func() string {
		var n string
		if err := db.QueryRow(ctx, `SELECT count(*)::text FROM lokadb."`+admin.KeyTable("builds", "slug")+`"`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := mapRows(); n != "6" {
		t.Fatalf("the key's map has %s rows, want the 6 slugs", n)
	}
	// the application keeps working: inserts and updates reach the table and keep the map right
	for _, q := range []string{
		`INSERT INTO builds VALUES (7, 7, 'b7', 'new')`,
		`UPDATE builds SET slug = 'b7x' WHERE id = 7`,
		`UPDATE builds SET note = 'again' WHERE team_id = 7`,
		`DELETE FROM builds WHERE id = 1`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	if n := mapRows(); n != "6" { // +1 insert, -1 delete; the renamed slug replaces b7
		t.Fatalf("after inserts, updates and a delete the map has %s rows, want 6", n)
	}
	var row string
	if err := db.QueryRow(ctx, `SELECT to_jsonb(k)::text FROM lokadb."`+admin.KeyTable("builds", "slug")+`" k WHERE to_jsonb(k)::text LIKE '%"b7x"%'`).Scan(&row); err != nil {
		t.Errorf("the renamed slug is not in the map: %v", err)
	}
	t.Logf("map row of the renamed slug: %s", row)
	// the same column decided again: nothing changes
	c.unr.last["s1"] = 0 // what is counted again after the decision
	c.collectUnrouted([]proxy.Unrouted{slugItem}, 0)
	again := decide("s1", "key")
	if string(again.Spec) != string(first.Spec) {
		t.Errorf("the spec changed on the second decide:\n%s\n%s", first.Spec, again.Spec)
	}
	if n := mapRows(); n != "6" || !routed(bySlug) {
		t.Errorf("the second decide changed the map (%s rows) or the routing", n)
	}
	// a cloud table on the enabled database
	cloud := decide("p1", "cloud")
	if err := json.Unmarshal(cloud.Spec, &sp); err != nil || len(sp.Routing.CloudTables) != 1 || sp.Routing.CloudTables[0].Table != "plans" || sp.Routing.CloudTables[0].TenantColumn != "team_id" || len(sp.Routing.Keys) != 1 {
		t.Fatalf("spec after the cloud table: %v %s", err, cloud.Spec)
	}
	var kind string
	if err := db.QueryRow(ctx, `SELECT string_agg(kind || ':' || table_name, ',' ORDER BY kind, table_name) FROM lokadb.routing`).Scan(&kind); err != nil {
		t.Fatal(err)
	}
	t.Logf("lokadb.routing: %s", kind)
	if _, err := os.Stat(dir + "/spec.json"); err != nil {
		t.Error("spec.json not written")
	}
}

func toText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		if x {
			return "true"
		}
		return "false"
	}
	b, _ := json.Marshal(v)
	s := string(b)
	if len(s) > 1 && s[0] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// Shapes recorded under older metadata (an enable still running, a table declared later) do not stay in
// the open list for good: the ones the proxy routes by the metadata as it is now leave it. Needs INIT_TEST_DSN.
func TestStaleShapesLeaveTheList(t *testing.T) {
	base := os.Getenv("INIT_TEST_DSN")
	if base == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	u, _ := url.Parse(base)
	u.Path = "/postgres"
	root, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close(ctx)
	_, _ = root.Exec(ctx, "DROP DATABASE IF EXISTS stale_en WITH (FORCE)")
	if _, err := root.Exec(ctx, "CREATE DATABASE stale_en"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = root.Exec(context.Background(), "DROP DATABASE IF EXISTS stale_en WITH (FORCE)") })
	_, _ = root.Exec(ctx, "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dec_owner') THEN CREATE ROLE dec_owner; END IF; END $$")
	u.Path = "/stale_en"
	dsn := u.String()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, q := range []string{
		`CREATE TABLE repositories (id bigint PRIMARY KEY, team_id bigint NOT NULL)`,
		`CREATE TABLE commits (id bigint PRIMARY KEY, repository_id bigint NOT NULL REFERENCES repositories, sha text)`,
		`INSERT INTO repositories VALUES (1, 7), (2, 8)`,
		`INSERT INTO commits VALUES (1, 1, 'a'), (2, 2, 'b')`,
	} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	spec := admin.Spec{Tables: []admin.SpecTable{{Name: "repositories", TenantColumn: "team_id"}, {Name: "commits", ParentTable: "repositories", ParentColumn: "repository_id"}}}
	if err := admin.Enable(ctx, dsn, spec, "dec_owner", quiet); err != nil {
		t.Fatalf("enable: %v", err)
	}
	c := &Config{State: t.TempDir()}
	ins := `INSERT INTO commits (id, sha, repository_id) VALUES ($1, $2, $3)`
	byslug := `SELECT * FROM commits WHERE sha = $1`
	gap := []proxy.Gap{{Kind: proxy.GapNoCondition, Table: "commits", Form: "INSERT"}}
	c.collectUnrouted([]proxy.Unrouted{
		{Fingerprint: "ins", SQL: ins, Full: ins, Table: "commits", Gaps: gap, Write: true, Count: 4, Refused: true},
		{Fingerprint: "sel", SQL: byslug, Full: byslug, Table: "commits", Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "commits", Column: "sha", Form: "="}}, Count: 2, Refused: true},
		{Fingerprint: "long", SQL: "SELECT …", Table: "commits", Gaps: gap, Count: 1}, // too long to read again: stays
	}, 0)
	if n := c.pruneRouted(ctx, db); n != 1 {
		t.Errorf("pruned %d, want 1", n)
	}
	c.unr.mu.Lock()
	_, hasIns := c.unr.items["ins"]
	_, hasSel := c.unr.items["sel"]
	_, hasLong := c.unr.items["long"]
	c.unr.mu.Unlock()
	if hasIns || !hasSel || !hasLong {
		t.Errorf("after the prune: insert %v, lookup by sha %v, long %v", hasIns, hasSel, hasLong)
	}
	if got := c.Info()["unrouted_open"]; got != "2" {
		t.Errorf("open: %q", got)
	}
	// the list on disk is the pruned one
	again := &Config{State: c.State}
	again.unr.mu.Lock()
	again.loadUnrouted()
	n := len(again.unr.items)
	again.unr.mu.Unlock()
	if n != 2 {
		t.Errorf("after a restart: %d shapes", n)
	}
}
