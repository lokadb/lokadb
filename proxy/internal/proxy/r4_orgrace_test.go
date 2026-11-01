package proxy

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// R4 protocol review: a lookup whose answer comes after another session ran a statement that
// changes lookups (orgVersion moved during the round trip) keeps nothing in the process cache,
// and resolveOrg's second pass then finds the id "missing": the statement runs under '-1'.
func TestR4LookupRacesOrgVersion(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "own"
		if shared {
			name = "shared"
		}
		t.Run(name, func(t *testing.T) {
			srv := &Server{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
			if shared {
				srv.orgs = newOrgCache(0)
			}
			db := "r4race-" + t.Name()
			f := &fakeUpstream{answer: func(q string) ([][]string, string, bool) {
				if strings.Contains(q, `FROM "documents__refs" WHERE id IN`) {
					orgVersion(db).Add(1) // another session's UPDATE of a cloud_tables table, meanwhile
					return [][]string{{"7", "1"}}, "", true
				}
				return nil, "", false
			}}
			cli, cliSrv := tcpPair(t)
			up, upSrv := tcpPair(t)
			f.be = pgproto3.NewBackend(upSrv, upSrv)
			f.stmts, f.portals, f.parseOIDs = map[string]string{}, map[string]string{}, map[string][]uint32{}
			go f.run()
			hj := &pgconn.HijackedConn{Conn: up, Frontend: pgproto3.NewFrontend(up, up)}
			s := newSession(srv, db, cliSrv, pgproto3.NewBackend(cliSrv, cliSrv), hj, testMeta(), nil)
			go s.run()
			fe := pgproto3.NewFrontend(cli, cli)
			sendAll(t, fe, &pgproto3.Query{String: "SELECT content FROM document_versions WHERE document_id = 7"})
			_ = cli.SetReadDeadline(time.Now().Add(3 * time.Second))
			for {
				m, err := fe.Receive()
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := m.(*pgproto3.ReadyForQuery); ok {
					break
				}
			}
			log := strings.Join(f.log(), "\n")
			t.Logf("upstream saw:\n%s", log)
			if !strings.Contains(log, "set_config('lokadb.org', '1'") {
				t.Errorf("document 7 is organization 1's, the statement did not run under it")
			}
		})
	}
}
