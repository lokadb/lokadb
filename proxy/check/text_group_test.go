package check

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// One cause makes dozens of findings (a key type lokadb does not take, in every table): the text
// report says it once and names the tables of the rest; the JSON report keeps every finding.
func TestGroupFindings(t *testing.T) {
	var fs []Finding
	for i := 1; i <= 20; i++ {
		fs = append(fs, Finding{Severity: Unsupported, Code: "id_type", Object: fmt.Sprintf("t%d.id", i), Message: fmt.Sprintf("t%d.id is money", i)})
	}
	fs = append(fs, Finding{Severity: Unsupported, Code: "id_missing", Object: "lonely", Message: "lonely has no key"})
	// a code with a measure of its own for each is not folded
	for i := 1; i <= 7; i++ {
		fs = append(fs, Finding{Severity: Unsupported, Code: "orphan_rows", Object: fmt.Sprintf("o%d", i), Message: fmt.Sprintf("o%d has orphans", i), Measure: &Measure{Rows: int64(i)}})
	}
	var b bytes.Buffer
	r := &Report{Database: "d", Findings: fs}
	r.WriteText(&b)
	out := b.String()
	if n := strings.Count(out, "[id_type]"); n != 1 {
		t.Errorf("id_type written %d times:\n%s", n, out)
	}
	if !strings.Contains(out, "the same for 19 more: t2.id, t3.id") || !strings.Contains(out, ", … and 7 more") {
		t.Errorf("the rest of id_type is not named:\n%s", out)
	}
	if n := strings.Count(out, "[orphan_rows]"); n != 7 {
		t.Errorf("orphan_rows (a measure each) written %d times, want 7", n)
	}
	if !strings.Contains(out, "[id_missing] lonely has no key") {
		t.Errorf("a code of one finding is written as it is:\n%s", out)
	}
	if got := groupFindings(fs[:3]); len(got) != 3 || got[0].same != nil {
		t.Errorf("few findings of a code are not grouped: %+v", got)
	}
}
