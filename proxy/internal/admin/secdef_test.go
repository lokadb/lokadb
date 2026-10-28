package admin

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Block 9, B9.12: schema lokadb is PUBLIC's to use (baseSQL), and a function is executable by
// PUBLIC unless revoked. Every SECURITY DEFINER function the package creates in lokadb must
// therefore have its EXECUTE revoked from PUBLIC somewhere in the package, or any role could
// run it with its owner's rights (move_log_entry wrote the move's log). A trigger function is
// exempt: it cannot be called directly. New functions of other blocks fall under it as they come.
func TestSecurityDefinerRevoked(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var src strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src.Write(b)
		src.WriteString("\n")
	}
	all := src.String()
	// a function is told by its name and argument types (move_log_entry has two)
	create := regexp.MustCompile(`(?is)CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+([a-z_.%"]+)\s*\(([^)]*)\)`)
	revoke := regexp.MustCompile(`(?is)REVOKE\s+(?:EXECUTE|ALL)\s+ON\s+FUNCTION\s+([a-z_.%"]+)\s*\(([^)]*)\)\s*FROM\s+PUBLIC`)
	sig := func(name, args string, named bool) string {
		var types []string
		for _, a := range strings.Split(args, ",") {
			f := strings.Fields(a)
			if named && len(f) > 1 { // "e_org bigint": the type after the argument's name
				f = f[1:]
			}
			if len(f) > 0 {
				types = append(types, strings.ToLower(strings.Join(f, " ")))
			}
		}
		return strings.ToLower(name) + "(" + strings.Join(types, ",") + ")"
	}
	revoked := map[string]bool{}
	for _, m := range revoke.FindAllStringSubmatch(all, -1) {
		revoked[sig(m[1], m[2], false)] = true
	}
	seen := 0
	for _, m := range create.FindAllStringSubmatchIndex(all, -1) {
		name, args := all[m[2]:m[3]], all[m[4]:m[5]]
		// the function's header: up to its body
		head := all[m[1]:]
		if i := strings.Index(head, "$$"); i >= 0 {
			head = head[:i]
		}
		up := strings.ToUpper(head)
		if !strings.Contains(up, "SECURITY DEFINER") || strings.Contains(up, "RETURNS TRIGGER") {
			continue
		}
		seen++
		if s := sig(name, args, true); !revoked[s] {
			t.Errorf("SECURITY DEFINER function %s: no REVOKE EXECUTE … FROM PUBLIC for it in the package", s)
		}
	}
	if seen == 0 {
		t.Fatal("no SECURITY DEFINER function found: the scan is broken")
	}
}
