package proxy

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The client's ReadyForQuery is built in clientRFQ alone (block 9's review of 8a.2): one built
// with the cloud's status elsewhere would tell a client in a direct transaction that its
// transaction is over (libpq, JDBC and psycopg believe it). The startup's, before any
// session, is the exception.
func TestClientRFQOnly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, l := range strings.Split(string(b), "\n") {
			if !strings.Contains(l, "ReadyForQuery{TxStatus:") {
				continue
			}
			switch {
			case f == "direct_tx.go" && strings.Contains(l, "func rfq("):
			case f == "server.go" && strings.Contains(l, "TxStatus: 'I'"): // the startup's
			default:
				t.Errorf("%s:%d builds a ReadyForQuery outside clientRFQ: %s", f, i+1, strings.TrimSpace(l))
			}
		}
	}
}
