// Package buildinfo names the build a binary came from, for status endpoints and the agent's
// version shown by the relay.
package buildinfo

import (
	"runtime/debug"
	"strings"
)

// version: the release a binary was built from, stamped at build time (the tag without "v"):
//
//	-ldflags "-X github.com/lokadb/lokadb/proxy/internal/buildinfo.version=0.1.0"
//
// Empty for a local build. Every binary of the repository reads it through Version.
var version string

// Version: the stamped release, else the module version, or "devel-<revision>" with "+dirty"
// for a modified tree, as far as the build records it ("unknown" without build information).
// It holds only letters, digits and ".+-_": it travels in a TLS ALPN name and a label of the
// metrics. The lokadb command, the runner, check's report, the proxy's /status and the agent
// all report it.
func Version() string {
	if version != "" {
		return Clean(version)
	}
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	v := bi.Main.Version
	if v == "" || v == "(devel)" {
		v = "devel"
		for _, s := range bi.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 12 {
				v += "-" + s.Value[:12]
			}
		}
	}
	for _, s := range bi.Settings {
		if s.Key == "vcs.modified" && s.Value == "true" {
			v += "+dirty"
		}
	}
	return Clean(v)
}

// Clean keeps the characters a version may carry and cuts it to 40.
func Clean(v string) string {
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '.', r == '+', r == '-', r == '_':
			b.WriteRune(r)
		}
		if b.Len() >= 40 {
			break
		}
	}
	return b.String()
}
