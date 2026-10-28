package main

import "testing"

// -org N[,N] of the commands that list organizations: positive numbers; the commands that
// write take every moved organization only with -all.
func TestOrgList(t *testing.T) {
	for _, c := range []struct {
		flag       string
		write, all bool
		want       int // organizations; -1: an error
	}{
		{"", false, false, 0}, // a diff: every moved organization
		{"", true, false, -1}, // a sync without -org or -all
		{"", true, true, 0},   // -all
		{"3", true, false, 1},
		{"3,4", true, false, 2},
		{"3", true, true, -1},  // -org and -all
		{"0", true, false, -1}, // organization 0
		{"3,0", true, false, -1},
		{"-2", false, false, -1},
	} {
		got, err := orgList(c.flag, c.write, c.all)
		if (err != nil) != (c.want < 0) || err == nil && len(got) != c.want {
			t.Errorf("orgList(%q, write %v, all %v) = %v, %v; want %d", c.flag, c.write, c.all, got, err, c.want)
		}
	}
}
