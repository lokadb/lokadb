package runner

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// What the proxy counts is added up across its restarts, kept in the state directory, and counted open
// until decided.
func TestUnroutedCollected(t *testing.T) {
	c := &Config{State: t.TempDir()}
	gap := []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "builds", Column: "slug", Form: "="}}
	x := proxy.Unrouted{Fingerprint: "aa", SQL: "SELECT * FROM builds WHERE slug = $1", Full: "SELECT * FROM builds WHERE slug = $1", Table: "builds", Gaps: gap, Count: 5, Refused: true}
	c.collectUnrouted([]proxy.Unrouted{x}, 0)
	c.collectUnrouted([]proxy.Unrouted{x}, 0) // the same count again: nothing new
	x.Count = 8
	c.collectUnrouted([]proxy.Unrouted{x}, 0)
	x.Count = 2 // the proxy restarted: its count began again
	c.collectUnrouted([]proxy.Unrouted{x}, 0)
	if got := c.Info()["unrouted_open"]; got != "1" {
		t.Errorf("open: %q", got)
	}
	if n := c.unr.items["aa"].Count; n != 10 {
		t.Errorf("counted %d, want 10 (5 + 3 + 2)", n)
	}
	again := &Config{State: c.State} // a new process reads the list from the state directory
	again.unr.mu.Lock()
	again.loadUnrouted()
	n := again.unr.items["aa"].Count
	again.unr.mu.Unlock()
	if n != 10 {
		t.Errorf("after a restart: %d", n)
	}
	reason, table, col := reasonOf(gap)
	if table != "builds" || col != "slug" || !strings.Contains(reason, "looks builds up by slug") {
		t.Errorf("reason: %q %q %q", reason, table, col)
	}
}

// Allowing a statement writes it into the allow list of the application's database in proxy.json,
// keeping what is there, and asks a running proxy to restart.
func TestAllowByName(t *testing.T) {
	dir := useDir(t)
	embeddedDone.Store(true)
	t.Cleanup(func() { embeddedDone.Store(false) })
	write(t, dir+"/proxy.json", `{"listen": ":6432", "fanout": {"*": {"mode": "reject"}, "shop": {"mode": "log", "allow": ["SELECT 1"]}}}`)
	c := &Config{found: &Found{DBName: "shop"}}
	if err := c.addFanoutAllow([]string{"SELECT * FROM builds WHERE slug = $1", "SELECT 1"}); err != nil {
		t.Fatal(err)
	}
	var m struct {
		Listen string
		Fanout map[string]struct {
			Mode  string
			Allow []string
		}
	}
	if err := json.Unmarshal(read(t, dir+"/proxy.json"), &m); err != nil {
		t.Fatal(err)
	}
	shop := m.Fanout["shop"]
	if m.Listen != ":6432" || shop.Mode != "log" || len(shop.Allow) != 2 || m.Fanout["*"].Mode != "reject" || !c.RestartRequested() {
		t.Fatalf("proxy.json: %+v", m)
	}
	write(t, dir+"/proxy.json", `{"fanout": "reject"}`)
	if err := c.addFanoutAllow([]string{"SELECT 2"}); err == nil {
		t.Error("a fanout that is not an object was rewritten")
	}
}

// The choices a statement gets are the ones the database allows; deciding writes them into the spec.
func TestStatementChoices(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	dir := useDir(t)
	ctx := context.Background()
	admin0, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin0.Close(ctx)
	for _, q := range []string{"DROP TABLE IF EXISTS sc_builds, sc_plans", "CREATE TABLE sc_builds (id serial PRIMARY KEY, team_id int, slug text UNIQUE, note text)", "CREATE TABLE sc_plans (id int PRIMARY KEY, team_id int, name text)"} {
		if _, err := admin0.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = admin0.Exec(ctx, "DROP TABLE IF EXISTS sc_builds, sc_plans") })
	write(t, dir+"/proxy.json", `{"fanout": {"*": {"mode": "log"}}}`)
	c := &Config{State: t.TempDir(), DB: dsn, found: &Found{DBName: "x"}, spec: admin.Spec{Tables: []admin.SpecTable{{Name: "sc_builds", TenantColumn: "team_id"}}}}
	o := &Ops{Cfg: c}
	byslug := proxy.Unrouted{Fingerprint: "k1", SQL: "SELECT * FROM sc_builds WHERE slug = $1", Full: "SELECT * FROM sc_builds WHERE slug = $1", Count: 7,
		Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "sc_builds", Column: "slug", Form: "="}}}
	bynote := proxy.Unrouted{Fingerprint: "k2", SQL: "SELECT * FROM sc_builds WHERE note = $1", Full: "SELECT * FROM sc_builds WHERE note = $1", Count: 3,
		Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "sc_builds", Column: "note", Form: "="}}}
	plans := proxy.Unrouted{Fingerprint: "k3", SQL: "SELECT * FROM sc_plans WHERE id = $1", Full: "SELECT * FROM sc_plans WHERE id = $1", Count: 2,
		Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "sc_plans", Column: "id", Form: "=", Cloud: true}}}
	c.collectUnrouted([]proxy.Unrouted{byslug, bynote, plans}, 0)
	res, err := o.Statements(ctx, cloudproto.StatementsArgs{})
	if err != nil || len(res.Items) != 3 {
		t.Fatalf("%v %+v", err, res)
	}
	opts := map[string]string{}
	for _, s := range res.Items {
		opts[s.Fingerprint] = strings.Join(s.Options, ",")
	}
	if opts["k1"] != "key,allow" || opts["k2"] != "allow" || opts["k3"] != "cloud,allow" {
		t.Fatalf("options: %v", opts)
	}
	if res.Items[0].Fingerprint != "k1" || res.Items[0].Count != 7 {
		t.Errorf("not the most frequent first: %+v", res.Items[0])
	}
	dec, err := o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Fingerprint: "k1", Choice: "key"}, {Fingerprint: "k3", Choice: "cloud"}, {Fingerprint: "k2", Choice: "key"}}})
	if err != nil || len(dec.Applied) != 2 || dec.Failed["k2"] == "" {
		t.Fatalf("decide: %v %+v", err, dec)
	}
	var sp admin.Spec
	if err := json.Unmarshal(dec.Spec, &sp); err != nil || sp.Routing == nil || len(sp.Routing.Keys) != 1 || sp.Routing.Keys[0].Column != "slug" ||
		len(sp.Routing.CloudTables) != 1 || sp.Routing.CloudTables[0].TenantColumn != "team_id" {
		t.Fatalf("spec: %v %s", err, dec.Spec)
	}
	if c.Info()["unrouted_open"] != "1" {
		t.Errorf("open after deciding: %q", c.Info()["unrouted_open"])
	}
	if _, err := os.Stat(dir + "/spec.json"); err != nil {
		t.Error("spec.json not written")
	}
}
