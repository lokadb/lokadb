package proxy

import (
	"net"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// unmark drops LokaDB's own mark (internal/ownsql) from a statement the proxy sent, so that a
// fake server sees it as written; it returns the statement as sent when it had the mark.
func unmark(m pgproto3.FrontendMessage) string {
	switch m := m.(type) {
	case *pgproto3.Query:
		if ownsql.IsOwn(m.String) {
			sent := m.String
			m.String = strings.TrimSuffix(m.String, "\n"+ownsql.Mark)
			return sent
		}
	case *pgproto3.Parse:
		if ownsql.IsOwn(m.Query) {
			sent := m.Query
			m.Query = strings.TrimSuffix(m.Query, "\n"+ownsql.Mark)
			return sent
		}
	}
	return ""
}

// The statements the proxy sends on its own (a lookup in the refs table, the organization's
// setting) carry LokaDB's mark, so that pg_stat_statements and lokadb check tell them from the
// application's; the client's statement does not.
func TestOwnStatementsMarked(t *testing.T) {
	f := &fakeUpstream{}
	fe, c := protoSession(t, f, testMeta())
	sendAll(t, fe, &pgproto3.Query{String: "SELECT * FROM documents WHERE id = 7"})
	expectMsgs(t, fe, c, 1)
	f.mu.Lock()
	own := append([]string(nil), f.own...)
	f.mu.Unlock()
	lookup, set := false, false
	for _, q := range own {
		if strings.Contains(q, "documents__refs") {
			lookup = true
		}
		if strings.Contains(q, "set_config('lokadb.org'") {
			set = true
		}
		if strings.Contains(q, "SELECT * FROM documents") {
			t.Errorf("the client's statement is marked as LokaDB's: %q", q)
		}
	}
	if !lookup || !set {
		t.Errorf("own statements %q: want the lookup and the organization's setting marked", own)
	}
	ran := strings.Join(f.log(), "|")
	if !strings.Contains(ran, "SELECT * FROM documents WHERE id = 7") {
		t.Errorf("upstream ran %q", f.log())
	}
}

// The metadata the proxy reads at a client's connection (lokadb.orgs, lokadb.tenants, LISTEN)
// goes on the application's own connection before the session takes it: it is marked too (a
// real database's pg_stat_statements held the read of lokadb.tenants under the application's
// role, and check gave it as a query_forbidden blocker).
func TestFrontQueryMarked(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	got := make(chan string, 1)
	go func() {
		be := pgproto3.NewBackend(server, server)
		m, err := be.Receive()
		if err != nil {
			got <- "error: " + err.Error()
			return
		}
		q, _ := m.(*pgproto3.Query)
		got <- q.String
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
	}()
	hj := &pgconn.HijackedConn{Conn: client, Frontend: pgproto3.NewFrontend(client, client)}
	if _, err := frontQuery(hj, "SELECT tenant, org::text FROM lokadb.tenants"); err != nil {
		t.Fatal(err)
	}
	if q := <-got; !ownsql.IsOwn(q) {
		t.Errorf("the metadata read went unmarked: %q", q)
	}
}
