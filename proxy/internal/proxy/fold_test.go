package proxy

import (
	"strings"
	"testing"
)

func TestHasFold(t *testing.T) {
	for _, c := range []struct {
		s, sub string
		want   bool
	}{
		{"SELECT 1", "select", true}, {"x DiScArD all", "discard", true}, {"discar", "discard", false},
		{"", "a", false}, {"abc", "", true}, {"set_config", "set_config", true}, {"SET_CONFIG(", "set_config", true},
		{"offset 5", "set", true}, {"aab", "ab", true}, {"AAB", "ab", true}, {"search_path", "search_path", true},
	} {
		if got := hasFold(c.s, c.sub); got != c.want || got != strings.Contains(strings.ToLower(c.s), c.sub) {
			t.Errorf("hasFold(%q, %q) = %v, want %v", c.s, c.sub, got, c.want)
		}
	}
	if !hasPrefixFold("FETCH 1", "fetch") || hasPrefixFold("FET", "fetch") {
		t.Error("hasPrefixFold")
	}
}
