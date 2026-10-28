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
		{"host", "app", "10.1.1.250", "255.255.255.255", "scram-sha-256", "1"},
		{"host", "app", "10.1.1.251", "255.255.255.255", "scram-sha-256", "2"},
		{"host", "app", "all", "", "reject", "3"},
		{"local", "all", "", "", "trust", "120"},
		{"host", "all", "127.0.0.1", "255.255.255.255", "trust", "122"},
		{"host", "all", "all", "", "scram-sha-256", "131"},
	}
	if got := hbaOpen(rules, "app", nil, proxies); len(got) != 0 {
		t.Fatalf("the stand's rules: %q", got)
	}
	if got := hbaOpen(rules[3:], "app", nil, proxies); len(got) != 1 || !strings.Contains(got[0], "line 131") {
		t.Fatalf("the image's rules alone: %q", got)
	}
}
