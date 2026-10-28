package main

// Robustness of the proxy (research/ANALYSIS.md 5.3): R1. The protocol cases (P1, P2, P6)
// are unit tests with a fake server: proxy/internal/proxy/protocol_test.go.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func init() {
	checks = append(checks,
		check{id: "R1", name: "clients that go away in the middle of a response do not take the proxy down", run: checkClientsGoAway},
	)
}

// R1. A client that disconnected while its response streamed in made the session's reader
// pop an emptied queue: a panic, and the whole proxy (every session) went down.
func checkClientsGoAway(e *env) (bool, string, error) {
	if err := e.enable(); err != nil {
		return false, "", err
	}
	stop, err := e.proxy()
	if err != nil {
		return false, "", err
	}
	defer stop()
	const tries = 30
	for i := 0; i < tries; i++ {
		c, err := e.viaProxy()
		if err != nil {
			return false, fmt.Sprintf("connection %d refused: the proxy is down", i+1), nil
		}
		fe := c.Frontend()
		fe.Send(&pgproto3.Query{String: "SELECT d.id, repeat('x', 200) FROM documents d, generate_series(1, 2000) g WHERE d.organization_id = 1"})
		_ = fe.Flush()
		time.Sleep(time.Duration(5+i%10) * time.Millisecond) // the answer is on its way
		_ = c.Conn().Close()
	}
	time.Sleep(500 * time.Millisecond)
	b, _ := os.ReadFile(filepath.Join(os.TempDir(), e.db+"-proxy.log"))
	panicked := strings.Contains(string(b), "panic:")
	c, err := e.viaProxy()
	if err != nil {
		return false, fmt.Sprintf("after %d clients that went away the proxy is down (panic in its log: %v)", tries, panicked), nil
	}
	defer c.Close(e.ctx)
	res, err := simple(e.ctx, c, "SELECT count(*) FROM documents WHERE organization_id = 2")
	if err != nil || len(res) != 1 || len(res[0].Rows) != 1 {
		return false, fmt.Sprintf("a new client after %d that went away: %v", tries, err), nil
	}
	n := string(res[0].Rows[0][0])
	return n == "100" && !panicked, fmt.Sprintf("%d clients went away mid-response; a new client then counts %s documents; panic in the proxy's log: %v", tries, n, panicked), nil
}
