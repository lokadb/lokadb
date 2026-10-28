package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 14's measurements: a read by a cold key waited for a lookup in __refs, an exchange of
// its own, before its statement went. Now its organization is set within the statement's
// exchange by a statement of the proxy's own that reads the refs table; the proxy's own query
// with a Sync of its own is gone. A portal bound to run in parts, a warm key and the cooperative
// mode go the way they did.
func TestInlineLookup(t *testing.T) {
	const child = "SELECT content FROM document_versions WHERE document_id = $1"
	run := func(t *testing.T, maxRows uint32, pre ...string) (*fakeUpstream, []string, int) {
		t.Helper()
		f := &fakeUpstream{}
		fe, c := protoSession(t, f, testMeta())
		for _, q := range pre {
			sendAll(t, fe, &pgproto3.Query{String: q})
			expectMsgs(t, fe, c, 1)
		}
		n, syncs := len(f.log()), f.syncs()
		sendAll(t, fe, &pgproto3.Parse{Name: "c", Query: child}, &pgproto3.Bind{PreparedStatement: "c", Parameters: [][]byte{[]byte("5")}},
			&pgproto3.Execute{MaxRows: maxRows}, &pgproto3.Sync{})
		expectMsgs(t, fe, c, 1)
		return f, f.log()[n:], f.syncs() - syncs
	}
	t.Run("cold key", func(t *testing.T) {
		_, log, syncs := run(t, 0)
		if syncs != 1 {
			t.Errorf("Syncs to the cloud %d, want the client's alone (no lookup of its own): %q", syncs, log)
		}
		if countContaining(log, `FROM "documents__refs" WHERE id = 5::bigint`) != 1 && countContaining(log, `FROM "documents__refs" WHERE id = 5`) != 1 {
			t.Errorf("no statement reading the key's organization within the exchange: %q", log)
		}
		if i, j := indexContaining(log, "documents__refs"), indexContaining(log, "FROM document_versions"); i < 0 || j < 0 || i > j {
			t.Errorf("the organization is not set before the statement: %q", log)
		}
	})
	t.Run("a portal run in parts", func(t *testing.T) {
		_, log, syncs := run(t, 10)
		if syncs != 2 || indexContaining(log, "SELECT id::text, coalesce(tenant::text") < 0 {
			t.Errorf("a portal with a row limit: want the lookup as before (2 Syncs), got %d: %q", syncs, log)
		}
	})
	t.Run("the cooperative mode", func(t *testing.T) {
		_, log, _ := run(t, 0, "BEGIN", "SET LOCAL lokadb.tenant = '1'")
		if countContaining(log, "WHERE id = 5::bigint") != 0 {
			t.Errorf("the cooperative mode took the inline lookup: %q", log)
		}
	})
}

func indexContaining(log []string, sub string) int {
	for i, l := range log {
		if strings.Contains(l, sub) {
			return i
		}
	}
	return -1
}
