package runner

import (
	"context"
	"encoding/json"
	"fmt"
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
	if err := c.addFanoutAllow([]string{"SELECT * FROM builds WHERE slug = $1", "SELECT 1"}, nil); err != nil {
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
	if err := c.addFanoutAllow([]string{"SELECT 2"}, nil); err == nil {
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

// The review by table: one line of why per table, reads and writes counted, and a decision about a table
// covers all its statements (allow and refuse go to proxy.json's "tables", the rest leave the open list).
func TestStatementsByTable(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	dir := useDir(t)
	ctx := context.Background()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, q := range []string{"DROP TABLE IF EXISTS bt_builds, bt_moments", "CREATE TABLE bt_builds (id serial PRIMARY KEY, user_id int, slug text UNIQUE)", "CREATE TABLE bt_moments (id serial PRIMARY KEY, user_id int, body text)"} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(ctx, "DROP TABLE IF EXISTS bt_builds, bt_moments") })
	write(t, dir+"/proxy.json", `{"listen": ":6432", "fanout": {"shop": {"mode": "reject"}}}`)
	c := &Config{State: t.TempDir(), DB: dsn, found: &Found{DBName: "shop"},
		spec: admin.Spec{Tables: []admin.SpecTable{{Name: "bt_builds", TenantColumn: "user_id"}, {Name: "bt_moments", TenantColumn: "user_id"}}}}
	o := &Ops{Cfg: c}
	none := func(tbl string) []proxy.Gap { return []proxy.Gap{{Kind: proxy.GapNoCondition, Table: tbl}} }
	var list []proxy.Unrouted
	for i := 0; i < 5; i++ {
		sql := fmt.Sprintf("SELECT * FROM bt_builds ORDER BY id LIMIT %d", i+1)
		list = append(list, proxy.Unrouted{Fingerprint: fmt.Sprint("b", i), SQL: sql, Full: sql, Count: int64(10 - i), Gaps: none("bt_builds")})
	}
	list = append(list, proxy.Unrouted{Fingerprint: "s", SQL: "SELECT * FROM bt_builds WHERE slug = $1", Full: "x", Count: 3,
		Gaps: []proxy.Gap{{Kind: proxy.GapUnknownColumn, Table: "bt_builds", Column: "slug", Form: "="}}})
	list = append(list, proxy.Unrouted{Fingerprint: "m", SQL: "DELETE FROM bt_moments WHERE body = $1", Full: "x", Count: 2, Write: true, Gaps: none("bt_moments")})
	list = append(list, proxy.Unrouted{Fingerprint: "w", SQL: "UPDATE bt_builds SET slug = $1", Full: "x", Count: 1, Write: true, Gaps: none("bt_builds")})
	c.collectUnrouted(list, 0)
	if c.Info()["unrouted_open_reads"] != "6" || c.Info()["unrouted_open_writes"] != "2" {
		t.Fatalf("info: %v", c.Info())
	}
	res, err := o.Statements(ctx, cloudproto.StatementsArgs{})
	if err != nil || len(res.Tables) != 2 || res.Reads != 6 || res.Writes != 2 {
		t.Fatalf("%v %+v", err, res)
	}
	b := res.Tables[0]
	if b.Table != "bt_builds" || b.Reads != 6 || b.Writes != 1 || len(b.SQL) != 7 || b.Column != "slug" || strings.Join(b.Options, ",") != "key,read,allow,refuse" ||
		b.Reason != "no condition on user_id, the customer column; looked up by slug" {
		t.Fatalf("builds: %+v", b)
	}
	dec, err := o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Table: "bt_builds", Choice: "read"}, {Table: "bt_moments", Choice: "refuse"}}})
	if err != nil || len(dec.Applied) != 2 {
		t.Fatalf("decide: %v %+v", err, dec)
	}
	// "read" decides the reads of bt_builds; its write stays open (the policy refuses it)
	if c.Info()["unrouted_open"] != "1" || c.Info()["unrouted_open_writes"] != "1" {
		t.Errorf("open after deciding both tables: %v", c.Info())
	}
	var m struct {
		Listen string
		Fanout map[string]struct {
			Mode   string
			Tables map[string]string
		}
	}
	if err := json.Unmarshal(read(t, dir+"/proxy.json"), &m); err != nil {
		t.Fatal(err)
	}
	if shop := m.Fanout["shop"]; m.Listen != ":6432" || shop.Mode != "reject" || shop.Tables["bt_builds"] != "read" || shop.Tables["bt_moments"] != "refuse" {
		t.Fatalf("proxy.json: %+v", m)
	}
}

// Under reject-writes the reads of a table already run, so "read" offered for a table that only writes
// changed nothing: its write was refused again and the table came back to the list with no word (KeepPlain,
// runner 0.1.12). Now "read" is not offered there, Decide refuses it with the reason, and "refuse" and
// "allow" take the table off the list for good.
func TestReadIsNotOfferedWhereItChangesNothing(t *testing.T) {
	dsn := os.Getenv("INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("INIT_TEST_DSN is not set")
	}
	dir := useDir(t)
	ctx := context.Background()
	db, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	for _, q := range []string{"DROP TABLE IF EXISTS wo_commits, wo_notices", "CREATE TABLE wo_commits (id serial PRIMARY KEY, user_id int, body text)", "CREATE TABLE wo_notices (id serial PRIMARY KEY, user_id int, body text)"} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(ctx, "DROP TABLE IF EXISTS wo_commits, wo_notices") })
	write(t, dir+"/proxy.json", `{"listen": ":6432", "fanout": {"shop": {"mode": "reject-writes"}}}`)
	c := &Config{State: t.TempDir(), DB: dsn, found: &Found{DBName: "shop"},
		spec: admin.Spec{Tables: []admin.SpecTable{{Name: "wo_commits", TenantColumn: "user_id"}, {Name: "wo_notices", TenantColumn: "user_id"}}}}
	o := &Ops{Cfg: c}
	none := func(tbl string) []proxy.Gap { return []proxy.Gap{{Kind: proxy.GapNoCondition, Table: tbl}} }
	c.collectUnrouted([]proxy.Unrouted{
		{Fingerprint: "c", SQL: "INSERT INTO wo_commits (body) VALUES ($1)", Full: "x", Count: 4, Write: true, Gaps: none("wo_commits")},
		{Fingerprint: "n", SQL: "INSERT INTO wo_notices (body) VALUES ($1)", Full: "x", Count: 3, Write: true, Gaps: none("wo_notices")},
	}, 0)
	res, err := o.Statements(ctx, cloudproto.StatementsArgs{})
	if err != nil || len(res.Tables) != 2 {
		t.Fatalf("%v %+v", err, res)
	}
	for _, tb := range res.Tables {
		if strings.Contains(strings.Join(tb.Options, ","), "read") || tb.Reads != 0 || tb.Writes != 1 {
			t.Fatalf("%s: read offered where there is no read to run: %+v", tb.Table, tb)
		}
	}
	dec, err := o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Table: "wo_commits", Choice: "read"}}})
	if err != nil || len(dec.Applied) != 0 || !strings.Contains(dec.Failed["table:wo_commits"], "already lets reads run") || !strings.Contains(dec.Failed["table:wo_commits"], "choose \"refuse\"") {
		t.Fatalf("read on a table that only writes: %v %+v", err, dec)
	}
	if c.Info()["unrouted_open"] != "2" {
		t.Fatalf("nothing was decided, both stay open: %v", c.Info())
	}
	dec, err = o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Table: "wo_commits", Choice: "refuse"}, {Table: "wo_notices", Choice: "allow"}}})
	if err != nil || len(dec.Applied) != 2 || len(dec.Failed) != 0 {
		t.Fatalf("refuse and allow: %v %+v", err, dec)
	}
	if c.Info()["unrouted_open"] != "0" {
		t.Fatalf("both tables leave the list: %v", c.Info())
	}
	// the proxy goes on counting what it saw before it restarted; the count of an old statement is a delta of zero
	// and does not bring the table back, and a restart of the proxy (c.restart) is what ends it
	c.collectUnrouted([]proxy.Unrouted{{Fingerprint: "c", SQL: "INSERT INTO wo_commits (body) VALUES ($1)", Full: "x", Count: 4, Write: true, Gaps: none("wo_commits")}}, 0)
	if c.Info()["unrouted_open"] != "0" {
		t.Fatalf("an unchanged count brought the table back: %v", c.Info())
	}
	var m struct {
		Fanout map[string]struct {
			Mode   string
			Tables map[string]string
		}
	}
	if err := json.Unmarshal(read(t, dir+"/proxy.json"), &m); err != nil {
		t.Fatal(err)
	}
	if tb := m.Fanout["shop"].Tables; m.Fanout["shop"].Mode != "reject-writes" || tb["wo_commits"] != "refuse" || tb["wo_notices"] != "allow" {
		t.Fatalf("proxy.json: %+v", m)
	}
}
