package proxy

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// A panic in a connection's goroutine (here: a nil map written) is logged, and the process,
// with every other session, goes on. Without the recover the test binary itself dies.
func TestSurvive(t *testing.T) {
	var out bytes.Buffer
	s := &Server{log: slog.New(slog.NewTextHandler(&out, nil))}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer s.survive("connection setup")
		var m map[string]int
		m["x"] = 1
	}()
	<-done
	if !strings.Contains(out.String(), "proxy bug: connection setup aborted") || !strings.Contains(out.String(), "nil map") {
		t.Fatalf("log: %s", out.String())
	}
}
