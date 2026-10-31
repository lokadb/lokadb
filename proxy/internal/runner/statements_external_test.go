package runner

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lokadb/lokadb/proxy/internal/admin"
	"github.com/lokadb/lokadb/proxy/internal/cloudproto"
	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// proxyAt serves what a proxy's GET /unrouted does, for the count in n (one shape: a write on documents).
func proxyAt(t *testing.T, n *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/unrouted" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []proxy.Unrouted{
			{DB: "shop", Fingerprint: "w1", SQL: "UPDATE documents SET body = $1", Full: "UPDATE documents SET body = $1", Table: "documents", Write: true, Refused: true, Count: n.Load(),
				Gaps: []proxy.Gap{{Kind: proxy.GapNoCondition, Table: "documents"}}},
			{DB: "other", Fingerprint: "o1", SQL: "SELECT 1 FROM x", Table: "x", Count: 5}, // another database of the same proxy
		}, "overflow": 0})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A runner whose proxy runs outside it lists the statements over the proxy's /unrouted (next to its /status),
// adds up only what is new since the last look, takes only its own database's, and says when it cannot look.
func TestExternalProxyStatementsAreListed(t *testing.T) {
	useDir(t)
	var n atomic.Int64
	n.Store(3)
	srv := proxyAt(t, &n)
	c := &Config{State: t.TempDir(), DB: "host=cloud dbname=shop user=postgres password=pw", ProxyStatus: []string{srv.URL + "/status"}}
	c.lookExternal(context.Background())
	if c.Info()["unrouted_visible"] != "yes" || c.Info()["unrouted_open"] != "1" || c.Info()["unrouted_open_writes"] != "1" {
		t.Fatalf("after the first look: %v", c.Info())
	}
	count := func() int64 {
		c.unr.mu.Lock()
		defer c.unr.mu.Unlock()
		return c.unr.items["w1"].Count
	}
	c.lookExternal(context.Background()) // nothing new
	if got := count(); got != 3 {
		t.Fatalf("the same count seen twice was added twice: %d", got)
	}
	n.Store(7)
	c.lookExternal(context.Background())
	if got := count(); got != 7 {
		t.Fatalf("4 more runs: count %d, want 7", got)
	}
	srv.Close()
	c.lookExternal(context.Background())
	if !strings.HasPrefix(c.Info()["unrouted_visible"], "no") && c.Info()["unrouted_visible"] != "yes" {
		t.Fatalf("visible: %q", c.Info()["unrouted_visible"])
	}
	if c.Info()["unrouted_open"] != "1" {
		t.Fatalf("a failed look lost the list: %v", c.Info())
	}
}

func TestExternalRunnerWithoutProxyStatusSaysItCannotSee(t *testing.T) {
	c := &Config{DB: "host=cloud dbname=shop"}
	v := c.Info()["unrouted_visible"]
	if !strings.HasPrefix(v, "no") || !strings.Contains(v, "proxy_status") {
		t.Fatalf("a runner that cannot look must say so: %q", v)
	}
	if (&Config{Embedded: true}).Info()["unrouted_visible"] != "yes" {
		t.Fatal("an embedded runner sees its own proxy")
	}
}

// The decisions go to the config that proxy reads (proxy_config), not to the runner's own directory; a proxy
// outside the runner is not restarted by it (RestartProxy); and a config the runner cannot write is not a lost
// decision: it stays open, with the block to put in by hand.
func TestExternalProxyDecisionsGoToProxyConfig(t *testing.T) {
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
	for _, q := range []string{"DROP TABLE IF EXISTS ex_docs, ex_notes", "CREATE TABLE ex_docs (id serial PRIMARY KEY, user_id int, body text)", "CREATE TABLE ex_notes (id serial PRIMARY KEY, user_id int, body text)"} {
		if _, err := db.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _, _ = db.Exec(ctx, "DROP TABLE IF EXISTS ex_docs, ex_notes") })
	pc := filepath.Join(t.TempDir(), "proxy.json")
	c := &Config{State: t.TempDir(), DB: dsn, ProxyConfig: pc, ProxyStatus: []string{"http://proxy:7490/status"},
		spec: admin.Spec{Tables: []admin.SpecTable{{Name: "ex_docs", TenantColumn: "user_id"}, {Name: "ex_notes", TenantColumn: "user_id"}}}}
	// the application's database is the DSN's; the proxy.json entry is keyed by its name
	name := c.appDB()
	if name == "" {
		t.Fatal("no database name from the DSN")
	}
	write(t, pc, `{"listen": ":6432", "fanout": {"`+name+`": {"mode": "reject-writes"}}}`)
	o := &Ops{Cfg: c}
	none := func(tbl string) []proxy.Gap { return []proxy.Gap{{Kind: proxy.GapNoCondition, Table: tbl}} }
	c.collectUnrouted([]proxy.Unrouted{
		{Fingerprint: "d", SQL: "UPDATE ex_docs SET body = $1", Full: "x", Count: 4, Write: true, Gaps: none("ex_docs")},
		{Fingerprint: "n", SQL: "UPDATE ex_notes SET body = $1", Full: "x", Count: 3, Write: true, Gaps: none("ex_notes")},
	}, 0)
	dec, err := o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Table: "ex_docs", Choice: "refuse"}}})
	if err != nil || len(dec.Applied) != 1 || len(dec.Failed) != 0 || !dec.RestartProxy {
		t.Fatalf("refuse: %v %+v", err, dec)
	}
	var m struct {
		Fanout map[string]struct {
			Mode   string
			Tables map[string]string
		}
	}
	if err := json.Unmarshal(read(t, pc), &m); err != nil {
		t.Fatal(err)
	}
	if e := m.Fanout[name]; e.Mode != "reject-writes" || e.Tables["ex_docs"] != "refuse" {
		t.Fatalf("the proxy's own config: %+v", m)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "proxy.json")); err == nil && strings.Contains(string(b), "ex_docs") {
		t.Fatal("the decision went to the runner's directory, which the proxy does not read")
	}
	// a config that cannot be written
	writeConfig = func(string, []byte) error { return errors.New("read-only file system") }
	t.Cleanup(func() { writeConfig = writeFileAtomicOrInPlace })
	dec, err = o.Decide(ctx, cloudproto.DecideArgs{Decisions: []cloudproto.Decision{{Table: "ex_notes", Choice: "allow"}}})
	if err != nil || len(dec.Applied) != 0 || dec.RestartProxy {
		t.Fatalf("allow where the config is read-only: %v %+v", err, dec)
	}
	if !strings.Contains(dec.Failed["table:ex_notes"], "read-only file system") || !strings.Contains(dec.Failed["table:ex_notes"], "restart the proxy") {
		t.Fatalf("failed: %v", dec.Failed)
	}
	if !strings.Contains(dec.Manual, `"fanout"`) || !strings.Contains(dec.Manual, `"ex_notes": "allow"`) || !strings.Contains(dec.Manual, `"ex_docs": "refuse"`) {
		t.Fatalf("the block to put in by hand: %s", dec.Manual)
	}
	if c.Info()["unrouted_open"] != "1" {
		t.Fatalf("the table whose choice was not saved must stay open: %v", c.Info())
	}
}
