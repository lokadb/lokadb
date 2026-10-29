package runner

import "testing"

// A customers table that looks like people lists keys only unless the person turns names on.
func TestPeopleLikeTables(t *testing.T) {
	cols := func(c ...string) map[string]bool {
		m := map[string]bool{}
		for _, x := range c {
			m[x] = true
		}
		return m
	}
	for _, c := range []struct {
		table string
		cols  map[string]bool
		want  bool
	}{
		{"teams", cols("id", "name", "slug"), false},
		{"organizations", cols("id", "name", "domain"), false},
		{"tenants", cols("id", "name"), false},
		{"accounts", cols("id", "name", "plan"), true}, // accounts of individuals: a name
		{"accounts", cols("id", "plan"), false},
		{"shops", cols("id", "owner_email"), false},
		{"shops", cols("id", "email"), true},
		{"companies", cols("id", "first_name"), true},
	} {
		if got := peopleByColumns(c.table, c.cols); got != c.want {
			t.Errorf("%s %v: %v, want %v", c.table, c.cols, got, c.want)
		}
	}
	for _, n := range []string{"users", "members", "people", "customers"} {
		if !peopleTables[n] {
			t.Errorf("%s is not taken for people", n)
		}
	}
	for _, n := range []string{"teams", "organizations", "companies", "tenants", "workspaces"} {
		if peopleTables[n] {
			t.Errorf("%s is taken for people", n)
		}
	}
}
