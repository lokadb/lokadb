package buildinfo

import "testing"

func TestVersionStamped(t *testing.T) {
	if v := Version(); v == "" {
		t.Fatalf("unstamped Version() is empty")
	}
	defer func(old string) { version = old }(version)
	version = "0.1.0-rc.1"
	if v := Version(); v != "0.1.0-rc.1" {
		t.Fatalf("Version() = %q, want the stamped 0.1.0-rc.1", v)
	}
	version = "0.1.0 bad/chars"
	if v := Version(); v != "0.1.0badchars" {
		t.Fatalf("Version() = %q, want it cleaned", v)
	}
}
