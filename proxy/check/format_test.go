package check

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The JSON report is an interface (tasks/check-report-format.md): CI of the customers, the AI
// advisor and the playground read it. These tests pin it. When one fails:
//   - a field or code was added: add it to testdata and to tasks/check-report-format.md;
//   - a field was removed, renamed or changed its type, or a code is gone: that is an
//     incompatible change. Keep the old one, or raise SchemaVersion and say so in the format.
// UPDATE_FORMAT=1 go test ./check -run Format rewrites testdata (then review the diff).

// TestFormatFields: every field of the JSON report, its path and type.
func TestFormatFields(t *testing.T) {
	var lines []string
	var walk func(path string, ty reflect.Type, seen map[reflect.Type]bool)
	walk = func(path string, ty reflect.Type, seen map[reflect.Type]bool) {
		for ty.Kind() == reflect.Pointer {
			ty = ty.Elem()
		}
		switch ty.Kind() {
		case reflect.Slice:
			if ty.Elem().Kind() == reflect.Struct || ty.Elem().Kind() == reflect.Pointer {
				walk(path+"[]", ty.Elem(), seen)
				return
			}
			lines = append(lines, path+" []"+jsonType(ty.Elem()))
			return
		case reflect.Map:
			walk(path+"{}", ty.Elem(), seen)
			return
		case reflect.Struct:
			if seen[ty] {
				lines = append(lines, path+" (recursive "+ty.Name()+")")
				return
			}
			seen = copyMap(seen)
			seen[ty] = true
			for i := 0; i < ty.NumField(); i++ {
				f := ty.Field(i)
				name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
				if name == "-" || !f.IsExported() {
					continue
				}
				if name == "" {
					name = f.Name
				}
				p := name
				if path != "" {
					p = path + "." + name
				}
				walk(p, f.Type, seen)
			}
			return
		case reflect.Interface:
			lines = append(lines, path+" any")
			return
		}
		lines = append(lines, path+" "+jsonType(ty))
	}
	walk("", reflect.TypeOf(Report{}), map[reflect.Type]bool{})
	sort.Strings(lines)
	golden(t, "format-fields.txt", lines, true)
}

// TestFormatCodes: every finding code the package can report. A code is never renamed.
func TestFormatCodes(t *testing.T) {
	fset := token.NewFileSet()
	// check's own files and its extensions' (ext/: -rtt, -suggest, feed_index)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	more, err := filepath.Glob(filepath.Join("ext", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, more...)
	codes := map[string]bool{}
	lit := func(e ast.Expr) {
		if b, ok := e.(*ast.BasicLit); ok && b.Kind == token.STRING {
			if s, err := strconv.Unquote(b.Value); err == nil && s != "" {
				codes[s] = true
			}
		}
	}
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				// c.add(sev, "code", ...), c.addOn(sev, "code", ...), report(k, sev, "code", ...),
				// unsupported("code", ...) / change("code", ...), specErr(t, "code", ...), unsupported(t, "code", ...)
				var fn string
				switch f := n.Fun.(type) {
				case *ast.SelectorExpr:
					fn = f.Sel.Name
				case *ast.Ident:
					fn = f.Name
				}
				switch fn {
				case "add", "addOn", "Add": // Add: an extension's Session.Add
					if len(n.Args) > 1 {
						lit(n.Args[1])
					}
				case "report":
					if len(n.Args) > 2 {
						lit(n.Args[2])
					}
				case "unsupported", "change", "specErr":
					for _, a := range n.Args[:min(2, len(n.Args))] {
						if b, ok := a.(*ast.BasicLit); ok && b.Kind == token.STRING {
							lit(a)
							break
						}
					}
				}
			case *ast.KeyValueExpr: // Finding{Code: "code"}
				if id, ok := n.Key.(*ast.Ident); ok && id.Name == "Code" {
					lit(n.Value)
				}
			case *ast.AssignStmt: // code := "customer_privileges"
				for i, l := range n.Lhs {
					if id, ok := l.(*ast.Ident); ok && id.Name == "code" && i < len(n.Rhs) {
						lit(n.Rhs[i])
					}
				}
			}
			return true
		})
	}
	var lines []string
	for c := range codes {
		lines = append(lines, c)
	}
	sort.Strings(lines)
	// A code the report no longer makes is retired (testdata/format-codes-retired.txt), not
	// deleted: consumers may still hold it in a baseline or a card. A retired code is never
	// reused for another finding.
	retired := map[string]bool{}
	if b, err := os.ReadFile(filepath.Join("testdata", "format-codes-retired.txt")); err == nil {
		for _, l := range strings.Fields(string(b)) {
			if l = strings.TrimSpace(l); l != "" {
				retired[l] = true
			}
		}
	}
	var live []string
	for _, l := range lines {
		if retired[l] {
			t.Errorf("code %q is retired (testdata/format-codes-retired.txt): do not reuse it for a finding", l)
			continue
		}
		live = append(live, l)
	}
	golden(t, "format-codes.txt", live, false)
}

// golden compares lines with testdata/name. exact: lines may not be added without updating
// it either; otherwise only lines missing from the code fail (a new code is fine to add).
func golden(t *testing.T, name string, lines []string, exact bool) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if os.Getenv("UPDATE_FORMAT") != "" {
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(b), "\r", "")), "\n")
	have := map[string]bool{}
	for _, l := range lines {
		have[l] = true
	}
	known := map[string]bool{}
	for _, l := range want {
		known[l] = true
		if !have[l] {
			t.Errorf("%s: %q is gone: an incompatible change of the report (raise SchemaVersion, or keep it)", name, l)
		}
	}
	for _, l := range lines {
		if !known[l] {
			if exact {
				t.Errorf("%s: %q is new: add it to testdata/%s and tasks/check-report-format.md", name, l, name)
			} else {
				t.Logf("%s: new %q: add it to testdata/%s", name, l, name)
			}
		}
	}
}

// jsonType: the JSON type of a Go type, so that int becoming int64 is no change of the format.
func jsonType(ty reflect.Type) string {
	switch ty.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Interface:
		return "any"
	}
	return ty.Kind().String()
}

func copyMap(m map[reflect.Type]bool) map[reflect.Type]bool {
	out := map[reflect.Type]bool{}
	for k, v := range m {
		out[k] = v
	}
	return out
}
