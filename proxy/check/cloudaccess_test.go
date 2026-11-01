package check

import (
	"strings"
	"testing"
)

// cloud_direct_access: which pg_hba rules let the application's role in from somewhere else
// than the proxy.
func TestHbaTooWide(t *testing.T) {
	proxies, bad := parseHosts([]string{"10.1.1.10", "10.1.1.11/32", "fd00::5"})
	if len(bad) > 0 || len(proxies) != 3 {
		t.Fatalf("parseHosts: %v %v", proxies, bad)
	}
	for _, c := range []struct {
		addr, mask string
		allowed    bool
		wide       bool
	}{
		{"all", "", true, true},
		{"samenet", "", true, true},
		{"samehost", "", true, false},
		{"10.1.1.10", "255.255.255.255", true, false},
		{"10.1.1.0", "255.255.255.0", true, true},
		{"10.1.1.11", "", true, false},
		{"10.2.0.1", "255.255.255.255", true, true},
		{"fd00::5", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true, false},
		{"0.0.0.0", "0.0.0.0", true, true},
		{"10.1.1.0", "255.255.255.0", false, true}, // without -proxy-hosts: more than one host
		{"10.1.1.10", "255.255.255.255", false, false},
		{"127.0.0.1", "255.255.255.255", true, false}, // the database's own host
		{"::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true, false},
	} {
		var allowed = proxies
		if !c.allowed {
			allowed = nil
		}
		if got := hbaTooWide(c.addr, c.mask, allowed) != ""; got != c.wide {
			t.Errorf("%s %s (proxy hosts %v): too wide %v, want %v", c.addr, c.mask, c.allowed, got, c.wide)
		}
	}
	if !hbaMatchesAll("0.0.0.0", "0.0.0.0") || !hbaMatchesAll("all", "") || hbaMatchesAll("10.0.0.0", "255.0.0.0") {
		t.Error("hbaMatchesAll")
	}
	if !hbaUser([]string{"+apps"}, "app", map[string]bool{"app +apps": true}) || hbaUser([]string{"other"}, "app", nil) {
		t.Error("hbaUser")
	}
}

// The stand's pg_hba: the proxies, a reject for app, then the image's own lines (loopback
// trust, all scram). Nothing past the reject counts.
func TestHbaOpenOrder(t *testing.T) {
	proxies, _ := parseHosts([]string{"10.1.1.250", "10.1.1.251"})
	rules := [][]string{
		{"host", "app", "10.1.1.250", "255.255.255.255", "scram-sha-256", "1", "all"},
		{"host", "app", "10.1.1.251", "255.255.255.255", "scram-sha-256", "2", "all"},
		{"host", "app", "all", "", "reject", "3", "all"},
		{"local", "all", "", "", "trust", "120", "all"},
		{"host", "all", "127.0.0.1", "255.255.255.255", "trust", "122", "all"},
		{"host", "all", "all", "", "scram-sha-256", "131", "all"},
	}
	if got := hbaOpen(rules, "app_db", "app", nil, proxies); len(got) != 0 {
		t.Fatalf("the stand's rules: %q", got)
	}
	if got := hbaOpen(rules[3:], "app_db", "app", nil, proxies); len(got) != 1 || !strings.Contains(got[0], "line 131") {
		t.Fatalf("the image's rules alone: %q", got)
	}
}

// R4-38: a rule for another kind of connection or another database does not end the search.
func TestR4HbaTypeAndDatabase(t *testing.T) {
	for name, rules := range map[string][][]string{
		"hostnossl reject, then hostssl open": {
			{"hostnossl", "all", "0.0.0.0", "0.0.0.0", "reject", "1", "all"},
			{"hostssl", "all", "0.0.0.0", "0.0.0.0", "scram-sha-256", "2", "all"},
		},
		"a reject for another database, then open": {
			{"host", "all", "0.0.0.0", "0.0.0.0", "reject", "1", "otherdb"},
			{"host", "app", "0.0.0.0", "0.0.0.0", "scram-sha-256", "3", "all"},
		},
	} {
		if open := hbaOpen(rules, "app_db", "app", map[string]bool{}, nil); len(open) != 1 || !strings.Contains(open[0], "line 2") && !strings.Contains(open[0], "line 3") {
			t.Errorf("%s: %q", name, open)
		}
	}
	// a host reject for this database (by name) still ends it; a rule for another database is not reported
	rules := [][]string{
		{"host", "all", "0.0.0.0", "0.0.0.0", "scram-sha-256", "1", "otherdb"},
		{"host", "all", "all", "", "reject", "2", "app_db"},
		{"host", "all", "all", "", "scram-sha-256", "3", "all"},
	}
	if open := hbaOpen(rules, "app_db", "app", nil, nil); len(open) != 0 {
		t.Errorf("reject for app_db: %q", open)
	}
}
