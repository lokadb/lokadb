package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The product was renamed to LokaDB before its launch (27.10): the old name is nowhere in the code,
// the tests, the stands and the documentation. What keeps it: the run VM's own files (/srv/…), the
// lock directory and the ssh key of the developers' machines, the hosted check's address and the
// feasibility study in research/ (research/ is history and keeps its words, research/bench aside).
func TestNoOldName(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	out, err := exec.Command("git", "-C", root, "ls-files", "-z", "--", "proxy", "e2e", "bench", "playground", "private",
		"docs", "research/bench", "README.md", ".gitignore", ".gitattributes").Output()
	if err != nil {
		t.Skipf("no git repository here: %v", err)
	}
	old := "byo" + "db"
	word := regexp.MustCompile(`(?i)` + old)
	kept := regexp.MustCompile(`/srv/` + old + `[\w.-]*|/e/dev/` + old + `-locks|\.ssh/` + old + `_vm|` + old + `\.keepplain\.com`)
	for _, rel := range strings.Split(string(out), "\x00") {
		if rel == "" {
			continue
		}
		if word.MatchString(rel) {
			t.Errorf("the old name in the path %s", rel)
		}
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil || bytes.IndexByte(b, 0) >= 0 {
			continue
		}
		for i, line := range strings.Split(string(b), "\n") {
			if word.MatchString(kept.ReplaceAllString(line, "")) {
				t.Errorf("the old name: %s:%d", rel, i+1)
			}
		}
	}
}
