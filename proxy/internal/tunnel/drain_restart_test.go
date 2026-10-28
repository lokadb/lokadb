package tunnel

import (
	"net"
	"path/filepath"
	"testing"
	"time"
)

// A relay restart (SIGTERM, then the same relay again): a short request on a connection open
// when the drain starts still gets its answer; the drain ends within drain_timeout (5 s by
// default, not a minute: block 14's agents scenario waited 59-77 s); the agent, told by the
// drain signal, is back on the new relay a second or two after it listens.
func TestRelayRestartReturn(t *testing.T) {
	const line = "q\n"
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	open, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	ask := func(c net.Conn) string {
		_ = c.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := c.Write([]byte(line)); err != nil {
			return err.Error()
		}
		buf := make([]byte, 16)
		n, _ := c.Read(buf)
		return string(buf[:n])
	}
	if got := ask(open); got != "db:"+line {
		t.Fatalf("before: %q", got)
	}
	t0 := time.Now()
	drained := make(chan struct{})
	go func() { tr.r.Drain(); close(drained) }()
	time.Sleep(200 * time.Millisecond)
	if got := ask(open); got != "db:"+line {
		t.Fatalf("a short request on an open connection while the relay drains: %q", got)
	}
	select {
	case <-drained:
	case <-time.After(8 * time.Second):
		t.Fatal("the drain took longer than its default 5 s")
	}
	if d := time.Since(t0); d > 7*time.Second {
		t.Fatalf("the drain took %v", d)
	}
	// the relay again, on the same agent and organization ports
	r2 := NewRelay(RelayConfig{Listen: tr.relayAddr, EnrollListen: freeAddr(t), Orgs: map[string]string{"1": org1}, OrgAllow: []string{"127.0.0.0/8"},
		Cert: filepath.Join(tr.dir, "relay.crt"), Key: filepath.Join(tr.dir, "relay.key"), CA: filepath.Join(tr.dir, "ca.crt"), CAKey: filepath.Join(tr.dir, "ca.key"),
		lenientRenew: true}, tr.store, tr.log)
	go func() { _ = r2.Run() }()
	waitFor(t, "the new relay listens", func() bool {
		c, err := net.Dial("tcp", org1)
		if err == nil {
			c.Close()
		}
		return err == nil
	})
	up := time.Now()
	waitFor(t, "the agent on the new relay", func() bool { return r2.serialOf("1") != "" })
	back := time.Since(up)
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := ask(c); got != "db:"+line {
		t.Fatalf("through the new relay: %q", got)
	}
	if d := back; d > 2500*time.Millisecond {
		t.Fatalf("the organization came back %v after the new relay listened (want 1-2 s)", d)
	}
	t.Logf("drain %v, back %v after the new relay listened", time.Since(t0)-time.Since(up), time.Since(up))
}
