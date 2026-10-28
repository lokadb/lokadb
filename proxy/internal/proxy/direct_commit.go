package proxy

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// The organization's COMMIT and its connection breaking on the way (review of the protocol,
// PR-1, tasks/direct-path.md): the proxy does not know whether the organization committed, so
// it neither tells the client the commit failed nor rolls the cloud back blindly. A direct
// transaction learns its transaction id at the organization before the COMMIT (at once with the
// cloud's SET CONSTRAINTS, no exchange of its own) and asks txid_status() on a new connection:
// the cloud then does what the organization did. When the outcome stays unknown the client gets
// 08007 (transaction_resolution_unknown, as Postgres drivers do), and the cloud keeps __refs a
// superset of the organization's rows: a refs row of a row that is not there leads a lookup to
// an empty answer and refs-diff finds it, a row without its refs row is out of reach by its key.

// cloudBusy: the first statement of the cloud's transaction of a direct transaction, a direct
// write and a direct COPY. The cloud's side waits there, idle, while the statements run at the
// organization: an idle_in_transaction_session_timeout of the cloud's (its server's, its role's,
// or the client's own SET, which the proxy repeats) would end the session in the middle of one
// that the cloud's way runs to its end (review of the protocol, PR-5). SET LOCAL: the
// transaction alone, the session's value stays.
const cloudBusy = "SET LOCAL idle_in_transaction_session_timeout = 0"

// Outcomes of the organization's side of a commit.
const (
	orgCommitted = "committed"
	orgAborted   = "aborted"
	orgUnknown   = "unknown"
)

// orgOutcomeWait: how long the proxy asks txid_status() while it says "in progress" (the
// organization's backend has not seen its connection end yet).
var orgOutcomeWait = 5 * time.Second

// errUnknown: the client's error when the outcome of the organization's commit is unknown.
func errUnknown(org string, err error) *pgproto3.ErrorResponse {
	return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08007",
		Message: "lokadb: the connection to organization " + org + "'s database was lost with the COMMIT on its way: whether the transaction committed there is unknown: " + err.Error()}
}

// orgCommit commits dc's open transaction. xid: its transaction id there ("" with xidKnown:
// it has none, it wrote nothing there). failed: the error the client is to get instead of
// COMMIT; outcome: what became of the organization's side.
func (s *session) orgCommit(dc *directConn, xid string, xidKnown bool) (failed *pgproto3.ErrorResponse, outcome string) {
	_, _, failed, err := s.orgWrite(dc, []pgproto3.FrontendMessage{&pgproto3.Query{String: "COMMIT"}}, false, true)
	switch {
	case err == nil && failed == nil:
		return nil, orgCommitted
	case err == nil: // the server answered: the transaction is rolled back there
		return failed, orgAborted
	}
	s.dropDirect(dc)
	switch {
	case xidKnown && xid == "": // nothing written there: the outcome changes no row
		return nil, orgCommitted
	case !xidKnown:
		return errUnknown(dc.org, err), orgUnknown
	}
	switch st := s.orgTxStatus(dc.org, xid); st {
	case orgCommitted:
		s.log.Warn("the organization's connection was lost at the COMMIT, its transaction committed (txid_status)", "org", dc.org, "err", err)
		return nil, orgCommitted
	case orgAborted:
		return &pgproto3.ErrorResponse{Severity: "ERROR", Code: "08006",
			Message: "lokadb: the connection to organization " + dc.org + "'s database was lost at the COMMIT, and the transaction is rolled back there: " + err.Error()}, orgAborted
	}
	return errUnknown(dc.org, err), orgUnknown
}

// orgTxStatus asks organization org's database, on a connection of its own, what became of
// transaction xid: orgCommitted, orgAborted or orgUnknown (no connection, "in progress" for
// longer than orgOutcomeWait, no answer).
func (s *session) orgTxStatus(org, xid string) string {
	dial := s.srv.dialDirect
	if dial == nil {
		dial = pgDial
	}
	ctx, cancel := context.WithTimeout(context.Background(), orgOutcomeWait)
	defer cancel()
	link, err := dial(ctx, s.customersApp(org), map[string]string{})
	if err != nil {
		s.log.Warn("the outcome of the organization's commit: no connection", "org", org, "err", err)
		return orgUnknown
	}
	defer link.conn.Close()
	deadline, _ := ctx.Deadline()
	_ = link.conn.SetDeadline(deadline)
	for {
		link.fe.Send(&pgproto3.Query{String: "SELECT txid_status(" + sqlString(xid) + "::bigint)"})
		if err := link.fe.Flush(); err != nil {
			return orgUnknown
		}
		st := ""
		for done := false; !done; {
			m, err := link.fe.Receive()
			if err != nil {
				return orgUnknown
			}
			switch m := m.(type) {
			case *pgproto3.DataRow:
				if len(m.Values) == 1 {
					st = string(m.Values[0])
				}
			case *pgproto3.ErrorResponse:
				s.log.Warn("the outcome of the organization's commit", "org", org, "err", m.Message)
			case *pgproto3.ReadyForQuery:
				done = true
			}
		}
		switch st {
		case "committed":
			return orgCommitted
		case "aborted":
			return orgAborted
		case "in progress":
			if time.Now().Add(200 * time.Millisecond).After(deadline) {
				return orgUnknown
			}
			time.Sleep(200 * time.Millisecond)
			continue
		}
		return orgUnknown
	}
}

// unknownSide: what the cloud does with a direct transaction whose organization's commit has
// an unknown outcome: __refs a superset of the organization's rows. Its refs rows added stay
// (COMMIT), its refs rows deleted come back (ROLLBACK); a transaction that did both has no
// side that keeps the superset: the cloud commits and the log names refs-sync.
func (s *session) unknownSide(d *directTx) {
	if d.refsDeleted && !d.refsAdded {
		s.cloudRun("ROLLBACK")
		return
	}
	if c := s.cloudRun("COMMIT"); c.err != nil {
		s.log.Warn("a direct transaction of unknown outcome at the organization: the cloud's commit failed too (refs-diff / refs-sync)", "org", d.org, "err", c.err)
	}
	if d.refsDeleted {
		s.log.Error("a direct transaction that added and deleted refs rows lost its organization's connection at the COMMIT, the outcome unknown: if the organization did not commit, the rows it deleted have no refs rows. Run lokadb refs-diff / refs-sync", "org", d.org)
	}
}
