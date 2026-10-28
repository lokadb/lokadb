package proxy

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// review of routing, R3: customers_app of organization 1 names organization 2's database (two
// organizations mixed up in the config, an old address after a change of database). The
// direct path checks the lokadb comments of the tables there on connecting: the connection is
// not used, the statement goes the cloud's way, the error is logged, and no session dials it
// again for directRetry.
func TestDirectIdentityChecked(t *testing.T) {
	for _, c := range []struct {
		name  string
		marks []string
		want  string
	}{
		{"another organization's database", []string{identityMark("2")}, "holds the tables of organization 2"},
		{"a database with no table of move-org's", []string{}, "no table there is marked"},
		{"one of its own tables and one of another organization", []string{identityMark("1"), identityMark("2")}, "holds the tables of organization 2"},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := directSession(t, false)
			d.org1.marks = c.marks
			for i := 0; i < 2; i++ {
				code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1")
				if code != "" || !ranAny(d.cloud.log(), "documents__org1") {
					t.Fatalf("statement %d: error %q, cloud %q", i+1, code, d.cloud.log())
				}
				if ranAny(d.org1.log(), "documents") {
					t.Fatalf("statement %d ran in the other organization's database: %q", i+1, d.org1.log())
				}
			}
			if d.dials != 1 {
				t.Fatalf("dialled %d times: the mismatch keeps the organization off the direct path for a while", d.dials)
			}
			if l := d.logs.String(); !strings.Contains(l, "level=ERROR") || !strings.Contains(l, c.want) {
				t.Fatalf("log: %s", l)
			}
		})
	}
	// its own database: the direct path as before (the fake's tables carry organization 1's mark)
	d := directSession(t, false)
	if code := d.query(t, "SELECT title FROM documents WHERE organization_id = 1"); code != "" || !ranAny(d.org1.log(), "SELECT title FROM documents") {
		t.Fatalf("own database: error %q, cloud %q, org1 %q", code, d.cloud.log(), d.org1.log())
	}
}

func TestIdentityVerdict(t *testing.T) {
	for _, c := range []struct {
		n, dbID string
		marks   []string
		ok      bool
	}{
		{"1", "", []string{"lokadb: organization 1"}, true},
		{"1", "", []string{"lokadb: organization 1", "lokadb: something else"}, true}, // not an organization's mark
		{"1", "", []string{"lokadb: organization 10"}, false},
		{"10", "", []string{"lokadb: organization 1"}, false},
		{"1", "", nil, false},
		{"1", "", []string{"lokadb: organization 1", "lokadb: organization 2"}, false},
		// the database id (review of admin, A1): marks without one, with the cloud's, with another
		{"1", "", []string{"lokadb: organization 1 database 5f3a"}, true},      // the cloud records none
		{"1", "5f3a", []string{"lokadb: organization 1 database 5f3a"}, true},  // the database the cloud uses
		{"1", "5f3a", []string{"lokadb: organization 1 database 9c01"}, false}, // a copy: repointed elsewhere
		{"1", "5f3a", []string{"lokadb: organization 1"}, false},               // older than the id: not the one
		{"1", "5f3a", []string{"lokadb: organization 10 database 5f3a"}, false},
		{"10", "5f3a", []string{"lokadb: organization 1 database 5f3a"}, false},
	} {
		if err := identityVerdict(c.n, c.dbID, c.marks); (err == nil) != c.ok {
			t.Errorf("organization %s, database id %q, marks %q: %v", c.n, c.dbID, c.marks, err)
		}
	}
	if sql := identitySQL([]string{"public", "o'1"}); !strings.Contains(sql, "to_regnamespace('o''1')") {
		t.Fatalf("schemas not quoted: %s", sql)
	}
}

// The DDL replay checks the same before it runs the DDL in an organization's database: a
// database with another organization's tables gets no DDL and the connection is closed.
func TestReplayIdentityChecked(t *testing.T) {
	for _, c := range []struct {
		marks []string
		ok    bool
	}{{[]string{identityMark("1")}, true}, {[]string{identityMark("2")}, false}, {nil, false}} {
		dsn, ran := fakePG(t, c.marks)
		conn, err := pgconn.Connect(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		s := &session{srv: &Server{}, log: slog.New(slog.NewTextHandler(&syncBuf{}, nil)), meta: directMeta()}
		err = s.replayIdentity(context.Background(), conn, "1", "public")
		if (err == nil) != c.ok {
			t.Fatalf("marks %q: %v", c.marks, err)
		}
		if !c.ok && !conn.IsClosed() {
			t.Fatalf("marks %q: the connection stays open", c.marks)
		}
		if q := <-ran; !strings.Contains(q, "lokadb: identity") || !strings.Contains(q, "to_regnamespace('public')") {
			t.Fatalf("query: %s", q)
		}
		conn.Close(context.Background())
	}
}

// Through prepareCustomers, as a DDL replay prepares: customers of organization 1 names a
// database of organization 2's tables, the replay is refused before any DDL is sent there.
func TestReplayToAnotherOrganizationRefused(t *testing.T) {
	dsn, ran := fakePG(t, []string{identityMark("2")})
	s := &session{srv: &Server{cfg: Config{Customers: map[string]map[string]string{"db": {"1": dsn}}}}, db: "db",
		log: slog.New(slog.NewTextHandler(&syncBuf{}, nil)), meta: directMeta()}
	none := func(string) ([][]string, error) { return [][]string{{"none", "none", "false"}}, nil }
	_, err := s.prepareCustomers([]string{"ALTER TABLE documents ADD COLUMN x int"}, none)
	if err == nil || !strings.Contains(err.Error(), "holds the tables of organization 2") {
		t.Fatalf("replay into another organization's database: %v", err)
	}
	if q := <-ran; !strings.Contains(q, "lokadb: identity") {
		t.Fatalf("the first statement there was not the check: %s", q)
	}
}

// fakePG: a server that takes one connection without a password and answers its first query
// with marks, one per row; ran gets that query.
func fakePG(t *testing.T, marks []string) (string, chan string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	ran := make(chan string, 1)
	go func() {
		c, err := l.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		be := pgproto3.NewBackend(c, c)
		if _, err := be.ReceiveStartupMessage(); err != nil {
			return
		}
		be.Send(&pgproto3.AuthenticationOk{})
		be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
		m, err := be.Receive()
		if err != nil {
			return
		}
		unmark(m)
		q, _ := m.(*pgproto3.Query)
		if q == nil {
			return
		}
		ran <- q.String
		be.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("d"), DataTypeOID: 25, Format: 0}}})
		for _, mk := range marks {
			be.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(mk)}})
		}
		be.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
		_, _ = be.Receive() // Terminate or EOF
	}()
	return "host=127.0.0.1 port=" + strings.TrimPrefix(l.Addr().String(), "127.0.0.1:") + " user=u dbname=d sslmode=disable", ran
}
