package proxy

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// rowsAnswer: the fake answers "SELECT rows" with n rows of cols columns.
func rowsAnswer(n, cols int) func(q string) ([][]string, string, bool) {
	return func(q string) ([][]string, string, bool) {
		if !strings.Contains(q, "SELECT rows") {
			return nil, "", false
		}
		out := make([][]string, n)
		for i := range out {
			r := make([]string, cols)
			for j := range r {
				r[j] = fmt.Sprintf("row %d col %d", i, j)
			}
			out[i] = r
		}
		return out, "", true
	}
}

// BACKLOG block 9, item 15: the rows of a statement whose answers are the client's go as their
// bytes; the client gets them unchanged, the relay reads frames once the Frontend is empty.
func TestRawRelayRows(t *testing.T) {
	f := &fakeUpstream{answer: rowsAnswer(2000, 3)}
	fe, c, s := protoSessionS(t, f, nil)
	for round := 0; round < 2; round++ {
		sendAll(t, fe, &pgproto3.Parse{Query: "SELECT rows"}, &pgproto3.Bind{}, &pgproto3.Execute{}, &pgproto3.Sync{})
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		n := 0
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if d, ok := m.(*pgproto3.DataRow); ok {
				if want := fmt.Sprintf("row %d col 2", n); len(d.Values) != 3 || string(d.Values[2]) != want {
					t.Fatalf("row %d: %q, want %q in column 3", n, d.Values, want)
				}
				n++
			}
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				break
			}
		}
		if n != 2000 {
			t.Fatalf("round %d: %d rows, want 2000", round, n)
		}
	}
	if s.upRaw == nil {
		t.Error("the relay never read the cloud's frames itself")
	}
}
