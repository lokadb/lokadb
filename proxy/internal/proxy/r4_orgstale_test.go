package proxy

import (
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// R4 protocol review: orgVersion moves when a statement that changes lookups runs, not when its
// transaction commits. A lookup by another session between the two reads the committed (old)
// organization and keeps it under the new version: after the COMMIT every session of the
// process routes the id to its old organization, until orgCacheTTL.
func TestR4OrgCacheStaleAfterCommit(t *testing.T) {
	srv := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil)), orgs: newOrgCache(0)}
	db := "r4stale-" + t.Name()
	var committed atomic.Bool // the move of document 7 to organization 2 committed
	refs := func(q string) ([][]string, string, bool) {
		if strings.Contains(q, `FROM "documents__refs" WHERE id IN`) {
			if committed.Load() {
				return [][]string{{"7", "2"}}, "", true
			}
			return [][]string{{"7", "1"}}, "", true
		}
		return nil, "", false
	}
	start := func(f *fakeUpstream) (*pgproto3.Frontend, func()) {
		cli, cliSrv := tcpPair(t)
		up, upSrv := tcpPair(t)
		f.be = pgproto3.NewBackend(upSrv, upSrv)
		f.stmts, f.portals, f.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
		go f.run()
		hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
		s := newSession(srv, db, cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, testMeta(), nil)
		go s.run()
		return pgproto3.NewFrontend(cli, cli), func() { _ = cli.SetReadDeadline(time.Now().Add(3 * time.Second)) }
	}
	ask := func(fe *pgproto3.Frontend, dl func(), sql string) {
		t.Helper()
		sendAll(t, fe, &pgproto3.Query{String: sql})
		dl()
		for {
			m, err := fe.Receive()
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := m.(*pgproto3.ReadyForQuery); ok {
				return
			}
		}
	}
	child := "SELECT content FROM document_versions WHERE document_id = 7"
	a := &fakeUpstream{answer: refs}
	feA, dlA := start(a)
	b := &fakeUpstream{answer: refs}
	feB, dlB := start(b)
	c := &fakeUpstream{answer: refs}
	feC, dlC := start(c)
	ask(feA, dlA, "BEGIN")
	ask(feA, dlA, "UPDATE documents SET organization_id = 2 WHERE id = 7") // orgVersion moves now
	ask(feB, dlB, child)                                                   // reads the committed refs row: organization 1
	ask(feA, dlA, "COMMIT")
	committed.Store(true)
	ask(feC, dlC, child)
	logC := strings.Join(c.log(), "\n")
	t.Logf("session C ran:\n%s", logC)
	if strings.Contains(logC, "set_config('lokadb.org', '1'") {
		t.Errorf("after the move committed, session C routes document 7 to organization 1 from the shared cache (no lookup of its own)")
	}
}
