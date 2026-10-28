package tunnel

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// P3-18's second half: an agent with -sessions 3 keeps three sessions to the relay; the
// organization's connections spread over them; one dropped, the others serve; a renewal moves
// the whole set to the new certificate, the old set finishing its own connections (R3-9).
func TestAgentSessionSet(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, CertTTL: "6s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	a.cfg.Sessions = 3
	go func() { _ = a.Run() }()
	live := func() (n int, sets map[string]bool) {
		sets = map[string]bool{}
		for _, si := range tr.r.Sessions() {
			if !si.Retiring {
				n++
			}
		}
		tr.r.mu.Lock()
		for _, as := range tr.r.sessions["1"] {
			sets[as.set] = true
		}
		tr.r.mu.Unlock()
		return n, sets
	}
	waitFor(t, "three sessions of one set", func() bool { n, s := live(); return n == 3 && len(s) == 1 })
	first := tr.session("1")

	// six long connections: spread over the three sessions (the least busy takes the next)
	var conns []net.Conn
	for i := 0; i < 6; i++ {
		c, err := net.Dial("tcp", org1)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		_, _ = c.Write([]byte("x\n"))
		buf := make([]byte, 16)
		if n, err := c.Read(buf); err != nil || string(buf[:n]) != "db:x\n" {
			t.Fatalf("connection %d: %q %v", i, buf[:n], err)
		}
	}
	tr.r.mu.Lock()
	per := []int{}
	for _, as := range tr.r.sessions["1"] {
		per = append(per, as.sess.NumStreams())
	}
	tr.r.mu.Unlock()
	for _, n := range per {
		if n != 2 {
			t.Fatalf("streams per session %v, want 2 each", per)
		}
	}
	for _, c := range conns {
		c.Close()
	}

	// one session drops: the agent dials that slot again, in the same set
	tr.r.mu.Lock()
	var one *agentSession
	for _, as := range tr.r.sessions["1"] {
		one = as
		break
	}
	tr.r.mu.Unlock()
	one.sess.Close()
	waitFor(t, "the slot back in the same set", func() bool { n, s := live(); return n == 3 && len(s) == 1 })
	if got := roundTripLine(t, org1); got != "db:y\n" {
		t.Fatalf("after one session dropped: %q", got)
	}

	// a renewal: a new set of three on the new certificate, the old set gone
	waitFor(t, "the set rolled over to the renewed certificate", func() bool {
		n, s := live()
		return n == 3 && len(s) == 1 && tr.session("1") != first && !strings.Contains(tr.session("1"), strings.Split(first, ",")[0])
	})
}

// roundTripLine: one line through addr, its answer.
func roundTripLine(t *testing.T, addr string) string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = c.Write([]byte("y\n"))
	buf := make([]byte, 16)
	n, _ := c.Read(buf)
	return string(buf[:n])
}

func TestPlaceSession(t *testing.T) {
	agents := map[string]*agentSession{}
	s := func(id, set, slot string) *agentSession { return &agentSession{id: id, set: set, slot: slot} }
	var mu sync.Mutex
	place := func(as *agentSession) ([]*agentSession, string) {
		mu.Lock()
		defer mu.Unlock()
		return placeSession(agents, as, 2)
	}
	for _, slot := range []string{"0", "1", "2"} {
		if old, why := place(s("a", "s1", slot)); why != "" || len(old) != 0 {
			t.Fatalf("slot %s: %v %q", slot, old, why)
		}
	}
	if old, _ := place(s("a", "s1", "1")); len(old) != 1 || len(agents) != 3 {
		t.Fatalf("the same slot again: replaced %d, %d sessions", len(old), len(agents))
	}
	if _, why := place(s("b", "", "")); why != "" {
		t.Fatalf("a second agent: %q", why)
	}
	if _, why := place(s("c", "", "")); why == "" {
		t.Fatal("a third agent beyond max_agents_per_org 2")
	}
	if old, _ := place(s("a", "s2", "0")); len(old) != 3 || len(agents) != 2 {
		t.Fatalf("a new set of the agent: replaced %d, left %d", len(old), len(agents))
	}
}
