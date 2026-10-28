package proxy

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// commitLost: a direct transaction whose INSERT wrote refs rows in the cloud, organization 1's
// connection breaking at the step its onQuery names (the COMMIT on its way, or the question of
// its transaction id before it); status: what txid_status() answers on the new connection
// ("": that connection fails). It returns the client's error and what both sides ran.
func commitLost(t *testing.T, breakAt, status string) (code, msg string, l *sharedLog, d *directTest) {
	t.Helper()
	d, l = txSession(t, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	d.org1.answerSimple = true
	d.org1.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "SELECT txid_current_if_assigned()") {
			return [][]string{{"777"}}, "", true
		}
		return nil, "", false
	}
	d.org1.onQuery = func(q string) bool {
		if !strings.HasPrefix(q, breakAt) {
			return false
		}
		d.conns[len(d.conns)-1].Close()
		return true
	}
	d.probe.answer = func(q string) ([][]string, string, bool) {
		if strings.HasPrefix(q, "SELECT txid_status('777'::bigint)") && status != "" {
			return [][]string{{status}}, "", true
		}
		return nil, "", false
	}
	d.probe.onQuery = func(q string) bool { return status == "" } // no answer there either
	for _, q := range []string{"BEGIN", "SELECT title FROM documents WHERE organization_id = 1",
		"INSERT INTO documents (organization_id, title) VALUES (1, 'z')"} {
		if _, code, _ := d.exchange(t, q); code != "" {
			t.Fatalf("%s: error %q (ran %q)", q, code, l.get())
		}
	}
	if !ranAny(d.org1.log(), "INSERT") || !ranAny(d.cloud.log(), "documents__refs") {
		t.Fatalf("the INSERT did not go direct: cloud %q, org1 %q", d.cloud.log(), d.org1.log())
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "COMMIT"})
	code, msg = errorCodeMsg(t, d.fe)
	return code, msg, l, d
}

// cloudEnd: how the cloud's side ended after its SET CONSTRAINTS: COMMIT or ROLLBACK.
func cloudEnd(t *testing.T, l *sharedLog) string {
	t.Helper()
	all := l.get()
	after := all[first(all, "cloud: SET CONSTRAINTS"):]
	switch {
	case ranAny(after, "cloud: COMMIT"):
		return "COMMIT"
	case ranAny(after, "cloud: ROLLBACK"):
		return "ROLLBACK"
	}
	return ""
}

// Review of the protocol, PR-1: the organization's connection breaks with its COMMIT on the
// way. The proxy asks txid_status() on a new connection and the cloud does what the
// organization did; unknown, the client gets 08007 and the cloud keeps __refs a superset of
// the organization's rows (an INSERT's refs rows stay). Broken before the COMMIT, both roll back.
func TestDirectCommitLost(t *testing.T) {
	orgOutcomeWait = 500 * time.Millisecond
	defer func() { orgOutcomeWait = 5 * time.Second }()

	code, msg, l, _ := commitLost(t, "COMMIT", "committed")
	if code != "" || cloudEnd(t, l) != "COMMIT" {
		t.Errorf("committed at the organization: the client got %q %q, the cloud %s (%q)", code, msg, cloudEnd(t, l), l.get())
	}
	code, msg, l, _ = commitLost(t, "COMMIT", "aborted")
	if code != "08006" || !strings.Contains(msg, "rolled back there") || cloudEnd(t, l) != "ROLLBACK" {
		t.Errorf("rolled back at the organization: the client got %q %q, the cloud %s", code, msg, cloudEnd(t, l))
	}
	code, msg, l, _ = commitLost(t, "COMMIT", "in progress")
	if code != "08007" || cloudEnd(t, l) != "COMMIT" {
		t.Errorf("still in progress there: the client got %q %q, the cloud %s (want 08007, the INSERT's refs rows kept)", code, msg, cloudEnd(t, l))
	}
	code, msg, l, _ = commitLost(t, "COMMIT", "")
	if code != "08007" || cloudEnd(t, l) != "COMMIT" {
		t.Errorf("the outcome not to be asked: the client got %q %q, the cloud %s (want 08007, the INSERT's refs rows kept)", code, msg, cloudEnd(t, l))
	}
	code, msg, l, _ = commitLost(t, "SELECT txid_current_if_assigned()", "committed")
	if code != "08006" || !strings.Contains(msg, "before the COMMIT") || cloudEnd(t, l) != "ROLLBACK" {
		t.Errorf("broken before the COMMIT went: the client got %q %q, the cloud %s (want both rolled back)", code, msg, cloudEnd(t, l))
	}
}

// PR-1 for a direct INSERT outside a transaction, which commits by itself at the organization:
// its connection lost with the INSERT on its way, the outcome is unknown: 08007, and the cloud
// commits its refs rows (a superset) instead of rolling them back.
func TestDirectWriteLost(t *testing.T) {
	d, l := txSession(t, keyed)
	d.cloud.answer = func(q string) ([][]string, string, bool) {
		if strings.Contains(q, "WITH lokadb_k AS") {
			return [][]string{{"41"}}, "", true
		}
		return nil, "", false
	}
	d.org1.onQuery = func(q string) bool {
		if strings.HasPrefix(q, "INSERT") {
			d.conns[len(d.conns)-1].Close()
			return true
		}
		return false
	}
	sendAll(t, d.fe, &pgproto3.Query{String: "INSERT INTO documents (organization_id, title) VALUES (1, 'z')"})
	code, msg := errorCodeMsg(t, d.fe)
	all := l.get()
	refs := first(all, "cloud: WITH lokadb_k AS")
	if code != "08007" || refs < 0 || !ranAny(all[refs:], "cloud: COMMIT") || ranAny(all[refs:], "cloud: ROLLBACK") {
		t.Fatalf("a direct INSERT of unknown outcome: the client got %q %q, both sides ran %q (want 08007, the refs rows committed)", code, msg, all)
	}
}
