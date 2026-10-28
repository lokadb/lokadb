package main

// Checks of the extended protocol routing (P4) and of the switch (R2, R4): research/ANALYSIS.md 5.3, 5.4.

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "P4", name: "interleaved portals: each Execute runs under its own Bind's organization", run: checkInterleavedPortals},
		check{id: "P4", name: "a portal bound in an earlier batch reads its own organization", run: checkEarlierBind},
		check{id: "R2", name: "a transaction that read and then writes during the switch does not cancel the move", run: checkReadThenWrite},
		check{id: "R4", name: "Ctrl-C during the move leaves nothing behind and the organization in the cloud", run: checkInterrupted},
	)
}

// raw sends extended protocol messages through the proxy and reads up to ReadyForQuery
// (or, with until, up to that many responses). It returns the data rows and the first error.
func raw(e *env, c *pgconn.PgConn, until int, msgs ...pgproto3.FrontendMessage) ([][]string, error) {
	fe := c.Frontend()
	for _, m := range msgs {
		fe.Send(m)
	}
	if err := fe.Flush(); err != nil {
		return nil, err
	}
	var rows [][]string
	var first error
	for n := 0; until == 0 || n < until; n++ {
		msg, err := c.ReceiveMessage(e.ctx)
		if err != nil {
			return rows, err
		}
		switch m := msg.(type) {
		case *pgproto3.DataRow:
			var r []string
			for _, v := range m.Values {
				r = append(r, string(v))
			}
			rows = append(rows, r)
		case *pgproto3.ErrorResponse:
			if first == nil {
				first = errors.New(m.Message)
			}
		case *pgproto3.ReadyForQuery:
			return rows, first
		}
	}
	return rows, first
}

func p(v ...string) [][]byte {
	var out [][]byte
	for _, s := range v {
		out = append(out, []byte(s))
	}
	return out
}

// P4. lokadb.org was set before each Bind only: with Bind P1, Bind P2, Execute P1, Execute P2
// both Executes ran under P2's organization (a child row took it as its organization).
func checkInterleavedPortals(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	doc1, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	doc2, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 2")
	if err != nil {
		return false, "", err
	}
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	if _, err := raw(e, c, 0,
		&pgproto3.Parse{Name: "s1", Query: "INSERT INTO document_versions (document_id, content) VALUES ($1::bigint, $2)"},
		&pgproto3.Bind{DestinationPortal: "p1", PreparedStatement: "s1", Parameters: p(doc1, "p4-a")},
		&pgproto3.Bind{DestinationPortal: "p2", PreparedStatement: "s1", Parameters: p(doc2, "p4-b")},
		&pgproto3.Execute{Portal: "p1"},
		&pgproto3.Execute{Portal: "p2"},
		&pgproto3.Sync{}); err != nil {
		return false, "", err
	}
	a, err := one(e.ctx, e.cloud(), "SELECT organization_id FROM document_versions WHERE content = 'p4-a'")
	if err != nil {
		return false, "", err
	}
	b, err := one(e.ctx, e.cloud(), "SELECT organization_id FROM document_versions WHERE content = 'p4-b'")
	if err != nil {
		return false, "", err
	}
	return a == "1" && b == "2", fmt.Sprintf("version of organization 1's document stored under %s, of organization 2's under %s", a, b), nil
}

// P4. A portal bound in an earlier batch (here Bind + Flush inside a transaction) got no
// lokadb.org before its Bind: partitions were pruned for whatever organization was set then.
func checkEarlierBind(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	c, err := e.viaProxy()
	if err != nil {
		return false, "", err
	}
	defer c.Close(e.ctx)
	for _, q := range []string{"BEGIN", "SELECT count(*) FROM documents WHERE organization_id = 1"} {
		if _, err := simple(e.ctx, c, q); err != nil {
			return false, "", err
		}
	}
	const count = "SELECT count(*) FROM documents WHERE organization_id = $1::bigint"
	// batch 1: the portal for organization 2 is bound, not executed
	if _, err := raw(e, c, 2,
		&pgproto3.Parse{Name: "s2", Query: count},
		&pgproto3.Bind{DestinationPortal: "p3", PreparedStatement: "s2", Parameters: p("2")},
		&pgproto3.Flush{}); err != nil {
		return false, "", err
	}
	// batch 2: a statement of organization 1, then the portal of batch 1
	rows, err := raw(e, c, 0,
		&pgproto3.Bind{DestinationPortal: "p4", PreparedStatement: "s2", Parameters: p("1")},
		&pgproto3.Execute{Portal: "p4"},
		&pgproto3.Execute{Portal: "p3"},
		&pgproto3.Sync{})
	if err != nil {
		return false, "", err
	}
	if _, err := simple(e.ctx, c, "COMMIT"); err != nil {
		return false, "", err
	}
	if len(rows) != 2 {
		return false, fmt.Sprintf("%d rows back, want 2", len(rows)), nil
	}
	return rows[0][0] == "100" && rows[1][0] == "100", fmt.Sprintf("organization 1 counts %s documents, the earlier portal of organization 2 counts %s (both have 100)", rows[0][0], rows[1][0]), nil
}

// R2. The switch held SHARE ROW EXCLUSIVE and DETACH raised it after DROP SUBSCRIPTION: a
// transaction that had read the table and then wrote it deadlocked with DETACH, and when
// Postgres cancelled the switch there was no replica left to retry with.
func checkReadThenWrite(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	doc, err := one(e.ctx, e.cloud(), "SELECT min(id) FROM documents WHERE organization_id = 1")
	if err != nil {
		return false, "", err
	}
	t, err := pgx.Connect(e.ctx, e.cloud())
	if err != nil {
		return false, "", err
	}
	defer t.Close(e.ctx)
	var once sync.Once
	var txErr error
	done := make(chan struct{})
	moveErr := e.moveWatched(1, func(line string) {
		if !strings.Contains(line, "step 6/6") {
			return
		}
		once.Do(func() {
			// the application's transaction is already reading when the switch starts
			tx, err := t.Begin(e.ctx)
			if err == nil {
				_, err = tx.Exec(e.ctx, "SELECT count(*) FROM documents")
			}
			if err != nil {
				txErr = err
				close(done)
				return
			}
			go func() {
				defer close(done)
				time.Sleep(1200 * time.Millisecond) // the switch now waits for this transaction's read lock
				if _, txErr = tx.Exec(e.ctx, "UPDATE documents SET title = 'r2' WHERE organization_id = 1 AND id = "+doc); txErr != nil {
					_ = tx.Rollback(e.ctx)
					return
				}
				txErr = tx.Commit(e.ctx)
			}()
		})
	})
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
	tstate := "committed"
	if txErr != nil {
		tstate = "failed: " + oneLine(txErr.Error())
	}
	if moveErr != nil {
		return false, "move-org failed: " + oneLine(moveErr.Error()) + "; the application's transaction " + tstate, nil
	}
	title, err := one(e.ctx, e.customer(), "SELECT title FROM documents WHERE id = "+doc)
	if err != nil {
		return false, "", err
	}
	// the transaction is let ahead of the switch: it commits, and its write reaches the customer
	ok := txErr == nil && title == "r2"
	return ok, "move-org done; the application's transaction " + tstate + "; title at the customer: " + title, nil
}

// R4. Ctrl-C during the move killed move-org on the spot: the subscription and its slot
// (the cloud's WAL kept forever) stayed.
func checkInterrupted(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	cmd := exec.CommandContext(e.ctx, e.bin, e.moveArgs(1)...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return false, "", err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return false, "", err
	}
	var all strings.Builder
	sc := bufio.NewScanner(out)
	sent := false
	for sc.Scan() {
		all.WriteString(sc.Text() + "\n")
		if !sent && strings.Contains(sc.Text(), "initial copy done") {
			_ = cmd.Process.Signal(os.Interrupt)
			sent = true
		}
	}
	waitErr := cmd.Wait()
	if !sent {
		return false, "the move never reached the initial copy: " + tail(all.String()), nil
	}
	if waitErr == nil {
		return false, "move-org ignored Ctrl-C and finished", nil
	}
	time.Sleep(time.Second) // walsenders exit
	left, err := leftovers(e)
	if err != nil {
		return false, "", err
	}
	n, err := one(e.ctx, e.cloud(), orgInCloud)
	if err != nil {
		return false, "", err
	}
	return left == "" && n == "100", fmt.Sprintf("move-org stopped (%v); left behind: %q; organization 1 in the cloud: %s rows", waitErr, left, n), nil
}
