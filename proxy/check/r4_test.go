package check

// Review 3 (R4), without a database.

import (
	"strings"
	"testing"

	"github.com/lokadb/lokadb/proxy/internal/proxy"
)

// R4-27: the texts of pg_stat_statements lose their values before they are judged and reported.
func TestR4WithoutValues(t *testing.T) {
	for _, s := range []string{
		"DECLARE c1 CURSOR FOR SELECT * FROM documents WHERE title = 'ann.secret@example.com'",
		"COPY (SELECT * FROM documents WHERE title = 'ann.secret@example.com' AND id = $1) TO STDOUT",
		"SELECT FROM documents WHERE title = 'ann.secret@example.com' AND id IN (", // does not parse
	} {
		if got := withoutValues(s); strings.Contains(got, "ann.secret") {
			t.Errorf("%q -> %q", s, got)
		}
	}
	if got := withoutValues("SELECT * FROM documents WHERE id = $1"); got != "SELECT * FROM documents WHERE id = $1" {
		t.Errorf("a normalized text changed: %q", got)
	}
}

// R4-39: a statement whose table is decided "refuse" (fanout.tables) is refused by the proxy
// (FanoutDenied), so check reports it as query_refused, under every mode.
func TestR4FanoutTableRefuse(t *testing.T) {
	c := testChecker()
	for _, mode := range []string{proxy.FanoutReject, proxy.FanoutLog, proxy.FanoutAllow} {
		cfg := proxy.Config{Fanout: map[string]proxy.FanoutPolicy{"app": {Mode: mode, Tables: map[string]string{"documents": proxy.TableRefuse}}}}
		var err error
		if c.fan, err = cfg.FanoutFor("app"); err != nil {
			t.Fatal(err)
		}
		sql := `select * from documents where slug = $1`
		if v := c.fan.Decide(sql, c.analyze(sql, c.meta)); v != proxy.FanoutDenied {
			t.Fatalf("mode %s: the proxy's verdict %d, want FanoutDenied", mode, v)
		}
		if k, note := c.fanoutDecision(sql); k != kRefused || note == "" {
			t.Errorf("mode %s: check kind %d note %q, want kRefused (%d) with a note", mode, k, note, kRefused)
		}
	}
}
