package proxy

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Block 14's measurements: keyCond (parse, walk, deparse) ran at every Bind of a prepared
// statement, half of the proxy's CPU on a read by id with 32 clients. The text with the key's
// condition comes from the session's cache: a second Bind does not make it again (here: the
// metadata replaced by one where keyCond adds nothing, the cached text still serves), and a new
// metadata empties the cache (refresh).
func TestKeyedBindCached(t *testing.T) {
	m := testMeta()
	partitioned(m)
	s := &session{meta: m, srv: &Server{}}
	st := &stmtInfo{sql: "SELECT title FROM documents WHERE organization_id = $1 AND id = $2"}
	first := s.keyedBind(st, st.sql, &pgproto3.Bind{}, "7")
	if len(first) == 0 {
		t.Fatal("no keyed statement for organization 7")
	}
	p, _ := first[len(first)-1].msg.(*pgproto3.Parse)
	if p == nil || !strings.Contains(p.Query, "COALESCE(documents.organization_id, -2) =") {
		t.Fatalf("the keyed statement: %q", p.Query)
	}
	s.meta = &Meta{} // keyCond would add nothing now
	b := &pgproto3.Bind{}
	if again := s.keyedBind(st, st.sql, b, "7"); again == nil || b.PreparedStatement == "" {
		t.Errorf("the second Bind made the text again instead of taking the cached one: %v, statement %q", again, b.PreparedStatement)
	}
}
