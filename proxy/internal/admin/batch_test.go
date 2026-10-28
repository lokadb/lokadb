package admin

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

// lines cuts a COPY text stream into batches of whole rows.
func TestLinesBatches(t *testing.T) {
	src := "1\ta\n2\tb\\nc\n3\tlong " + strings.Repeat("x", 5000) + "\n4\td" // the last row without a newline
	br := bufio.NewReaderSize(strings.NewReader(src), 16)
	var got []string
	var rows int64
	for {
		if _, err := br.Peek(1); err == io.EOF {
			break
		}
		l := &lines{r: br, left: 2}
		b, err := io.ReadAll(l)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(b))
		rows += l.rows
		if l.bytes != int64(len(b)) {
			t.Fatalf("bytes %d, read %d", l.bytes, len(b))
		}
	}
	if len(got) != 2 || rows != 4 || strings.Join(got, "") != src {
		t.Fatalf("batches %d, rows %d", len(got), rows)
	}
	if !strings.HasPrefix(got[1], "3\tlong") {
		t.Fatalf("second batch starts with %q", got[1][:10])
	}
}

// The capture function of a table is generated for its columns.
func TestCaptureFuncDDL(t *testing.T) {
	mt := moveTable{name: "docs", tenant: "organization_id", org: 7, cols: [][]string{
		{"id", "bigint", "true", "", "", ""}, {"organization_id", "bigint", "true", "", "", ""},
		{"blob", "bytea", "false", "", "", ""}, {"note", "character varying(20)", "false", "", "", ""}}}
	ddl := captureFuncDDL(7, mt, "docs__o7")
	for _, want := range []string{`OLD."id"`, `NEW."id"`, `octet_length(NEW."blob")`, `octet_length(NEW."note")`, `pg_column_size(NEW."organization_id")`, "lokadb.move_log_entry(7, 'docs'"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("no %s in\n%s", want, ddl)
		}
	}
	t.Log(ddl)
}

// A key read back from COPY's text format.
func TestCopyUnescape(t *testing.T) {
	for in, want := range map[string]string{
		`abc`: "abc", `a\tb`: "a\tb", `a\\b`: `a\b`, `a\bc`: "a\bc", `x\nY`: "x\nY", `\101\x42`: "AB", `\x`: "x", `end\`: `end\`,
	} {
		if got := copyUnescape(in); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}
