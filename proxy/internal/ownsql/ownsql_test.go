package ownsql

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

func TestTag(t *testing.T) {
	for in, want := range map[string]string{
		"SELECT 1":                     "SELECT 1\n" + Mark,
		"COMMIT;":                      "COMMIT\n" + Mark,
		"SELECT 1 -- why\n":            "SELECT 1 -- why\n" + Mark,
		"SELECT 1\n" + Mark:            "SELECT 1\n" + Mark,
		"SELECT 1\n" + Mark + ";\n":    "SELECT 1\n" + Mark + ";\n",
		"SELECT '/* lokadb */' AS txt": "SELECT '/* lokadb */' AS txt\n" + Mark,
	} {
		if got := Tag(in); got != want {
			t.Errorf("Tag(%q) = %q, want %q", in, got, want)
		}
	}
	if IsOwn("SELECT tenant, org::text FROM lokadb.tenants") || !IsOwn(Tag("SELECT tenant, org::text FROM lokadb.tenants")) {
		t.Error("IsOwn")
	}
}

// chunks splits writes as a careless writer might: the tagging writer must pass the bytes of a
// message cut by a write on as they are, and keep reading messages after it.
type chunks struct {
	buf   bytes.Buffer
	sizes []int
}

func (c *chunks) Write(p []byte) (int, error) {
	c.sizes = append(c.sizes, len(p))
	return c.buf.Write(p)
}

func roundTrip(t *testing.T, split int, msgs ...pgproto3.FrontendMessage) []pgproto3.FrontendMessage {
	t.Helper()
	var raw []byte
	for _, m := range msgs {
		var err error
		if raw, err = m.Encode(raw); err != nil {
			t.Fatal(err)
		}
	}
	dst := &chunks{}
	w := &writer{w: dst}
	if split <= 0 || split >= len(raw) {
		split = len(raw)
	}
	for _, part := range [][]byte{raw[:split], raw[split:]} {
		if len(part) > 0 {
			if _, err := w.Write(part); err != nil {
				t.Fatal(err)
			}
		}
	}
	be := pgproto3.NewBackend(bytes.NewReader(dst.buf.Bytes()), io.Discard)
	var out []pgproto3.FrontendMessage
	if _, err := be.ReceiveStartupMessage(); err != nil {
		t.Fatal(err)
	}
	for {
		m, err := be.Receive()
		if err != nil {
			break
		}
		switch m := m.(type) { // the backend reuses its messages: copy what is checked
		case *pgproto3.Query:
			out = append(out, &pgproto3.Query{String: m.String})
		case *pgproto3.Parse:
			out = append(out, &pgproto3.Parse{Name: m.Name, Query: m.Query, ParameterOIDs: append([]uint32(nil), m.ParameterOIDs...)})
		case *pgproto3.CopyData:
			out = append(out, &pgproto3.CopyData{Data: append([]byte(nil), m.Data...)})
		default:
			out = append(out, m)
		}
	}
	return out
}

func TestWriter(t *testing.T) {
	startup := &pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "postgres"}}
	msgs := []pgproto3.FrontendMessage{startup,
		&pgproto3.Query{String: "SELECT 1"},
		&pgproto3.Parse{Name: "s1", Query: "SELECT $1::int", ParameterOIDs: []uint32{23}},
		&pgproto3.Bind{PreparedStatement: "s1", Parameters: [][]byte{[]byte("Q\x00\x00\x00\x09lokadb")}},
		&pgproto3.Execute{},
		&pgproto3.CopyData{Data: []byte("P\x00\x00\x00\x10x\x00SELECT 2\x00\x00\x00")},
		&pgproto3.Sync{},
	}
	whole := roundTrip(t, 0, msgs...)
	if len(whole) != 6 {
		t.Fatalf("messages: %d", len(whole))
	}
	if q := whole[0].(*pgproto3.Query).String; q != "SELECT 1\n"+Mark {
		t.Errorf("query %q", q)
	}
	if p := whole[1].(*pgproto3.Parse); p.Query != "SELECT $1::int\n"+Mark || p.Name != "s1" || len(p.ParameterOIDs) != 1 || p.ParameterOIDs[0] != 23 {
		t.Errorf("parse %+v", p)
	}
	if d := whole[4].(*pgproto3.CopyData); !strings.HasPrefix(string(d.Data), "P\x00") || strings.Contains(string(d.Data), Mark) {
		t.Errorf("copy data changed: %q", d.Data)
	}
	// a write that ends in the middle of the Parse: it passes untagged, the rest still parses
	for split := 1; split < 120; split += 7 {
		got := roundTrip(t, split, msgs...)
		if len(got) != 6 {
			t.Fatalf("split %d: %d messages", split, len(got))
		}
		if _, ok := got[3].(*pgproto3.Execute); !ok {
			t.Fatalf("split %d: %T", split, got[3])
		}
	}
}
