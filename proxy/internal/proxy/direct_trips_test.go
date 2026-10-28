package proxy

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// Item 9: the exchanges a transaction of the direct path costs, with the cloud and with the
// organization's database (the proxy's writes there; the pause's release, which the proxy does
// not wait for, counted apart; the client's own statements in the cloud too).
func TestDirectTrips(t *testing.T) {
	count := func(d *directTest, qs ...string) (cloud, org, unlocks int) {
		settle(d) // the pause's release of the step before is sent unwaited: counted with that step
		c0, o0, l0 := d.cloudW.Load(), d.orgW.Load(), len(d.cloud.log())
		for _, q := range qs {
			if _, code, _ := d.exchange(t, q); code != "" {
				t.Fatalf("%s: %q", q, code)
			}
		}
		settle(d)
		for _, q := range d.cloud.log()[l0:] {
			if strings.Contains(q, "pg_advisory_unlock_shared") {
				unlocks++
			}
		}
		settle(d)
		return int(d.cloudW.Load() - c0), int(d.orgW.Load() - o0), unlocks
	}
	d, _ := txSession(t)
	d.exchange(t, "SELECT title FROM documents WHERE organization_id = 1") // the connection opened
	// before item 9 (25.10): autocommit read 2 + 1; transaction (read, UPDATE) 4 + 4, all of them
	// waited for in turn; read-only transaction 4 + 4
	for _, c := range []struct {
		name       string
		qs         []string
		cloud, org int
	}{
		{"autocommit read (the pause and the check; its release not waited for)", []string{"SELECT title FROM documents WHERE organization_id = 1"}, 2, 1},
		{"transaction: read, UPDATE (open; the UPDATE's check; COMMIT at once with the organization's)",
			[]string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1", "UPDATE documents SET title = 'y' WHERE organization_id = 1", "COMMIT"}, 3, 3},
		{"read-only transaction, two reads of one form (open; COMMIT; the organization's BEGIN with the first read)",
			[]string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1", "SELECT title FROM documents WHERE organization_id = 1", "COMMIT"}, 2, 3},
	} {
		cl, org, u := count(d, c.qs...)
		t.Logf("%s: cloud %d (release %d), organization %d", c.name, cl, u, org)
		if cl != c.cloud || org != c.org {
			t.Errorf("%s: cloud %d (want %d), organization %d (want %d)", c.name, cl, c.cloud, org, c.org)
		}
	}
}

// Block 9's review of item 9: the organization's BEGIN, sent with the transaction's first
// statement, fails: the statement does not run there (it would have committed on its own),
// the client gets the error, the transaction is aborted; by a simple query and by the extended
// protocol alike.
func TestDirectTxBeginFails(t *testing.T) {
	for _, extended := range []bool{false, true} {
		d, _ := txSession(t)
		d.org1.failBegin = true
		d.exchange(t, "BEGIN")
		q := "UPDATE documents SET title = 'y' WHERE organization_id = 1"
		var code string
		var st byte
		if extended {
			sendAll(t, d.fe, &pgproto3.Parse{Name: "u", Query: "UPDATE documents SET title = $1 WHERE organization_id = $2"}, &pgproto3.Sync{})
			if c := errorCode(t, d.fe); c != "" {
				t.Fatalf("prepare: %s", c)
			}
			learnt(d, "UPDATE documents SET title = $1 WHERE organization_id = $2")
			sendAll(t, d.fe, &pgproto3.Bind{PreparedStatement: "u", Parameters: [][]byte{[]byte("y"), []byte("1")}}, &pgproto3.Execute{}, &pgproto3.Sync{})
			_, code, st = d.answerAll(t)
		} else {
			_, code, st = d.exchange(t, q)
		}
		if code != "25001" || st != 'E' {
			t.Fatalf("extended %v: %q %q, org1 %q", extended, code, st, d.org1.log())
		}
		if ranAny(d.org1.log(), "UPDATE documents") {
			t.Fatalf("extended %v: the UPDATE ran after the failed BEGIN: %q", extended, d.org1.log())
		}
		if _, code, st := d.exchange(t, "ROLLBACK"); code != "" || st != 'I' {
			t.Fatalf("extended %v: ROLLBACK %q %q", extended, code, st)
		}
	}
}

// settle waits until the proxy has stopped writing to the cloud and to the organization's
// database (a message it sends without waiting, the pause's release, may come after the
// client's answer).
func settle(d *directTest) {
	last, same := int64(-1), 0
	for i := 0; i < 400 && same < 4; i++ {
		n := d.cloudW.Load() + d.orgW.Load()
		if n == last {
			same++
		} else {
			last, same = n, 0
		}
		time.Sleep(5 * time.Millisecond)
	}
}
