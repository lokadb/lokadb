package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Second review of routing: a FETCH string over cursors of two organizations, and a COMMIT that
// fills cursors WITH HOLD of two, ran under lokadb.org = "" (every organization's rows), the
// cooperative mode included. Now: refused, or "" only where every organization is allowed
// outright.
func TestCursorsOfTwoOrganizations(t *testing.T) {
	ranAll := func(log []string) bool {
		for _, q := range log {
			if strings.HasPrefix(q, "SELECT set_config('lokadb.org', ''") || strings.HasPrefix(q, "SELECT set_config('lokadb.org', '',") {
				return true
			}
		}
		return false
	}
	ranFetch := func(log []string) bool {
		for _, q := range log {
			if strings.HasPrefix(q, "FETCH") {
				return true
			}
		}
		return false
	}
	for _, mode := range []struct {
		name    string
		a, b    []string // what declares cursor a, then cursor b
		holdA   string
		holdB   string
		fetchAB string
	}{
		{"by the statement", []string{"DECLARE a CURSOR FOR SELECT id FROM documents WHERE organization_id = 1"},
			[]string{"DECLARE b CURSOR FOR SELECT id FROM documents WHERE organization_id = 2"},
			"DECLARE ha CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 1",
			"DECLARE hb CURSOR WITH HOLD FOR SELECT id FROM documents WHERE organization_id = 2",
			"FETCH 1 FROM a; FETCH 1 FROM b"},
		{"cooperative", []string{"SELECT set_config('lokadb.tenant', '7', true)", "DECLARE a CURSOR FOR SELECT id FROM documents"},
			[]string{"SELECT set_config('lokadb.tenant', '8', true)", "DECLARE b CURSOR FOR SELECT id FROM documents"},
			"", "",
			"FETCH 1 FROM a; MOVE 1 IN b"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			f := &fakeUpstream{}
			fe, c := protoSession(t, f, testMeta())
			for _, q := range append(append([]string{"BEGIN"}, mode.a...), mode.b...) {
				sendAll(t, fe, &pgproto3.Query{String: q})
				expectMsgs(t, fe, c, 1)
			}
			n := len(f.log())
			sendAll(t, fe, &pgproto3.Query{String: mode.fetchAB})
			if code := errorCode(t, fe); code != "0A000" {
				t.Errorf("simple: %q over cursors of two organizations: code %q, want 0A000 (upstream ran %q)", mode.fetchAB, code, f.log()[n:])
			}
			if log := f.log()[n:]; ranAll(log) || ranFetch(log) {
				t.Errorf("simple: upstream ran %q, want nothing", log)
			}
			// the extended protocol (one Parse with two statements: Postgres refuses it, the proxy first)
			n = len(f.log())
			sendAll(t, fe, &pgproto3.Parse{Query: mode.fetchAB}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
			if code := errorCode(t, fe); code != "0A000" {
				t.Errorf("extended: code %q, want 0A000 (upstream ran %q)", code, f.log()[n:])
			}
			if log := f.log()[n:]; ranAll(log) {
				t.Errorf("extended: upstream ran %q, want no lokadb.org = ''", log)
			}
			if mode.holdA == "" {
				return
			}
			// cursors WITH HOLD of two organizations in one transaction: the second is refused
			for _, q := range []string{"ROLLBACK", "BEGIN", mode.holdA} {
				sendAll(t, fe, &pgproto3.Query{String: q})
				expectMsgs(t, fe, c, 1)
			}
			sendAll(t, fe, &pgproto3.Query{String: mode.holdB})
			if code := errorCode(t, fe); code != "0A000" {
				t.Errorf("a cursor WITH HOLD of organization 2 after one of 1: code %q, want 0A000 (upstream ran %q)", code, f.log())
			}
		})
	}
}

// fetchOrg and holdOrg alone: "" for cursors of several organizations only where every
// organization is allowed outright; never by default.
func TestCursorOrgsOfSeveral(t *testing.T) {
	two := map[string]cursorAt{"a": {org: "1", hold: true}, "b": {org: "2", hold: true}}
	s := &session{cursors: two}
	if org, ok, err := s.fetchOrg("FETCH 1 FROM a; FETCH 1 FROM b", ""); err == nil {
		t.Errorf("reject: fetchOrg = %q, %v, nil; want an error", org, ok)
	}
	if org, ok, err := s.fetchOrg("FETCH 1 FROM a", ""); err != nil || !ok || org != "1" {
		t.Errorf("one cursor: fetchOrg = %q, %v, %v; want 1", org, ok, err)
	}
	if org, ok, err := s.fetchOrg("FETCH 1 FROM a; FETCH 1 FROM b", "7"); err == nil {
		t.Errorf("cooperative: fetchOrg = %q, %v, nil; want an error", org, ok)
	}
	if org, _ := s.holdOrg(); org != "-1" {
		t.Errorf("holds of two organizations, none allowed every organization: holdOrg = %q, want -1", org)
	}
	s.fanout = &Fanout{Mode: FanoutAllow}
	if org, ok, err := s.fetchOrg("FETCH 1 FROM a; FETCH 1 FROM b", ""); err != nil || !ok || org != "" {
		t.Errorf("allow: fetchOrg = %q, %v, %v; want \"\"", org, ok, err)
	}
	s.cursors = map[string]cursorAt{"a": {org: "1", hold: true}, "b": {org: "2", hold: true, any: true}}
	if org, _ := s.holdOrg(); org != "" {
		t.Errorf("holds of two organizations, one declared with every organization allowed: holdOrg = %q, want \"\"", org)
	}
}
