package admin

import (
	"errors"
	"strings"
	"testing"
)

// Block 14's stand: CREATE SUBSCRIPTION failed with a bare "server closed the connection
// unexpectedly" (the agent's -cloud-allow did not admit the database's address); move-org
// now says what to check.
func TestPublisherHint(t *testing.T) {
	for _, c := range []struct{ err, want string }{
		{`ERROR: could not connect to the publisher: connection to server at "10.0.0.4", port 5454 failed: FATAL: lokadb agent: connection from 10.0.0.4:40112 to the cloud window refused; add it to -cloud-allow (SQLSTATE 08006)`, "add the address"},
		{`ERROR: could not connect to the publisher: connection to server at "10.0.0.4", port 5454 failed: server closed the connection unexpectedly (SQLSTATE 08006)`, "older than this one"},
		{`ERROR: could not connect to the publisher: connection to server at "10.0.0.1", port 5432 failed: FATAL: pg_hba.conf rejects connection for host "10.0.0.9", user "lokadb_repl_16384_org4", database "bench", no encryption (SQLSTATE 28000)`, "RELAY_ADDR/32"},
		{`ERROR: could not connect to the publisher: password authentication failed (SQLSTATE 08006)`, ""},
	} {
		got := publisherHint(errors.New(c.err))
		if c.want == "" && got != "" || c.want != "" && !strings.Contains(got, c.want) {
			t.Errorf("%s: %q", c.err, got)
		}
	}
}
