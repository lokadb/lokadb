package tunnel

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// A relay with the drain signal and an agent of before it: the agent forwards the signal's 8
// bytes to its database like any stream, the database refuses them, and the agent's other
// connections and its session are not touched.
func TestDrainSignalOldAgent(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	// its database: echoes lines, records what a connection sent first
	var mu sync.Mutex
	var firsts [][]byte
	db, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	go func() {
		for {
			c, err := db.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 64)
				n, err := c.Read(buf)
				if err != nil {
					return
				}
				mu.Lock()
				firsts = append(firsts, append([]byte{}, buf[:n]...))
				mu.Unlock()
				if bytes.Equal(buf[:n], drainSignal) {
					return // a database refuses what is no startup
				}
				for {
					if _, err := c.Write(append([]byte("db:"), buf[:n]...)); err != nil {
						return
					}
					if n, err = c.Read(buf); err != nil {
						return
					}
				}
			}()
		}
	}()
	// the old agent: every stream of the relay straight to its database
	pool, _ := loadPool(filepath.Join(tr.dir, "ca.crt"))
	c, err := tls.Dial("tcp", tr.relayAddr, &tls.Config{Certificates: []tls.Certificate{*st.cert}, RootCAs: pool, ServerName: "localhost"})
	if err != nil {
		t.Fatal(err)
	}
	sess, err := yamux.Client(c, muxConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	go func() {
		for {
			s, err := sess.Accept()
			if err != nil {
				return
			}
			go func() {
				d, err := net.Dial("tcp", db.Addr().String())
				if err != nil {
					s.Close()
					return
				}
				go func() { _, _ = io.Copy(d, s); d.Close() }()
				_, _ = io.Copy(s, d)
				s.Close()
			}()
		}
	}()
	waitFor(t, "the old agent", func() bool { return tr.session("1") != "" })
	open, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer open.Close()
	ask := func(what string) string {
		_ = open.SetDeadline(time.Now().Add(3 * time.Second))
		_, _ = open.Write([]byte(what))
		buf := make([]byte, 64)
		n, _ := open.Read(buf)
		return string(buf[:n])
	}
	if got := ask("a\n"); got != "db:a\n" {
		t.Fatalf("before: %q", got)
	}
	go tr.r.Drain()
	waitFor(t, "the signal reached the old agent's database", func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, f := range firsts {
			if bytes.Equal(f, drainSignal) {
				return true
			}
		}
		return false
	})
	if got := ask("b\n"); got != "db:b\n" {
		t.Fatalf("the old agent's open connection after the signal: %q", got)
	}
	if sess.IsClosed() {
		t.Fatal("the old agent's session closed on the signal")
	}
}

// A relay of before the signal and an agent with it: the agent waits for the session to close
// (GoAway, then drain_timeout) and comes back to the new relay as before.
func TestNewAgentOldRelayDrain(t *testing.T) {
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, noDrainSignal: true, DrainTimeout: "1s"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	if got := roundTripLine(t, org1); got != "db:y\n" {
		t.Fatalf("before: %q", got)
	}
	tr.r.Drain()
	r2 := NewRelay(RelayConfig{Listen: tr.relayAddr, EnrollListen: freeAddr(t), Orgs: map[string]string{"1": org1}, OrgAllow: []string{"127.0.0.0/8"},
		Cert: filepath.Join(tr.dir, "relay.crt"), Key: filepath.Join(tr.dir, "relay.key"), CA: filepath.Join(tr.dir, "ca.crt"), CAKey: filepath.Join(tr.dir, "ca.key"),
		lenientRenew: true}, tr.store, tr.log)
	go func() { _ = r2.Run() }()
	waitFor(t, "the agent on the new relay", func() bool { return r2.serialOf("1") != "" })
	if got := roundTripLine(t, org1); got != "db:y\n" {
		t.Fatalf("through the new relay: %q", got)
	}
}
