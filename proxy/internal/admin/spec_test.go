package admin

import (
	"strings"
	"testing"
)

func TestSorted(t *testing.T) {
	names := func(s Spec) string {
		var out []string
		for _, t := range s.Tables {
			out = append(out, t.Name)
		}
		return strings.Join(out, " ")
	}
	cases := []struct {
		in   []SpecTable
		want string
	}{
		// children before parents in the file
		{[]SpecTable{{Name: "versions", ParentTable: "documents"}, {Name: "documents", ParentTable: "projects"}, {Name: "projects", TenantColumn: "org"}},
			"projects documents versions"},
		// already in order: unchanged
		{[]SpecTable{{Name: "projects", TenantColumn: "org"}, {Name: "documents", ParentTable: "projects"}, {Name: "tags", TenantColumn: "org"}},
			"projects documents tags"},
		// a parent missing from the spec keeps its child in place; a cycle goes last
		{[]SpecTable{{Name: "a", ParentTable: "b"}, {Name: "b", ParentTable: "a"}, {Name: "c", ParentTable: "nowhere"}, {Name: "d", TenantColumn: "org"}},
			"c d a b"},
	}
	for _, tc := range cases {
		if got := names(Spec{Tables: tc.in}.Sorted()); got != tc.want {
			t.Errorf("%v: %s, want %s", tc.in, got, tc.want)
		}
	}
}
