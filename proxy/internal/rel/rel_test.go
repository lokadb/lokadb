package rel

import (
	"strings"
	"testing"
)

// Block 4, item 3: a helper name that does not fit is shortened, never cut alike.
func TestHelperNames(t *testing.T) {
	if got := Helper("documents", "__cloud"); got != "documents__cloud" {
		t.Errorf("short: %s", got)
	}
	long := "documents_" + strings.Repeat("l", 48) // 58 bytes
	o1, o2, o12 := Helper(long, "__org1"), Helper(long, "__org2"), Helper(long, "__org12")
	if o1 == o2 || o1 == o12 || len(o1) != NameMax || !strings.HasSuffix(o12, "__org12") {
		t.Errorf("long: %s %s %s", o1, o2, o12)
	}
	if a, b := Helper(long+"a", "__cloud"), Helper(long+"b", "__cloud"); a == b {
		t.Errorf("two long names share a helper: %s", a)
	}
	if got := Helper("app."+long, "__org1"); !strings.HasPrefix(got, "app.") || len(got) != len("app.")+NameMax {
		t.Errorf("schema: %s", got)
	}
	if got := HelperName(strings.Repeat("\u044f", 40), "__cloud"); len(got) > NameMax || !strings.HasSuffix(got, "__cloud") {
		t.Errorf("multibyte: %s (%d bytes)", got, len(got))
	}
	for _, c := range []struct{ k, h, suffix string }{
		{"documents", "documents__cloud", "__cloud"},
		{long, Helper(long, "__org12"), "__org12"},
		{"app." + long, Helper("app."+long, "__o3_source"), "__o3_source"},
	} {
		if s, ok := HelperSuffix(c.k, c.h); !ok || s != c.suffix {
			t.Errorf("HelperSuffix(%s, %s) = %q %v", c.k, c.h, s, ok)
		}
	}
	if _, ok := HelperSuffix("documents", "documents_archive"); ok {
		t.Error("documents_archive taken for a helper")
	}
	if _, ok := HelperSuffix(long, Helper(long+"x", "__cloud")); ok {
		t.Error("another table's helper taken for this one's")
	}
}

// Block 4, item 4: a table of public with a dot in its name.
func TestDotName(t *testing.T) {
	if got := RelKey("", "acct.docs"); got != "public.acct.docs" {
		t.Errorf("RelKey: %s", got)
	}
	if s, n := SplitKey("public.acct.docs"); s != "public" || n != "acct.docs" {
		t.Errorf("SplitKey: %s %s", s, n)
	}
	if got := RelKey("", "docs"); got != "docs" {
		t.Errorf("RelKey of a plain name: %s", got)
	}
}
