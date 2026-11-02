package tunnel

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A process that dialed before a relay published a new organization's port (org-add or
// org-endpoint, then move-org at once) kept the relays' ports it read for 30 s: its connection to
// the new port went without the one-time code, and the relay, the SaaS database's address pinned,
// refused it (move-org left the organization on fdw: "profile fdw+direct was not set ... it is
// not the SaaS database").
func TestNewRelayPortIsAdmittedAtOnce(t *testing.T) {
	dsn := os.Getenv("LOKADB_REVIEW3_PG")
	if dsn == "" {
		t.Skip("LOKADB_REVIEW3_PG not set")
	}
	ctx := context.Background()
	store, err := OpenPGStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// the SaaS database's address is proven: the relay pins it, 127.0.0.1 is not it
	if err := store.ProveSource(ctx, "127.0.0.9", "probe"); err != nil {
		t.Fatal(err)
	}
	ProveSelfWith(store)
	defer ProveSelfWith(nil)
	// the process dials before the new port is there (move-org's first connections)
	if c, err := DialProving(nil)(ctx, "tcp", freeAddr(t)); err == nil {
		c.Close()
	}

	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	org, agentAddr, enroll := freeAddr(t), freeAddr(t), freeAddr(t)
	_, orgPort, _ := net.SplitHostPort(org)
	relay := NewRelay(RelayConfig{Listen: agentAddr, EnrollListen: enroll, Cert: filepath.Join(dir, "relay.crt"),
		Key: filepath.Join(dir, "relay.key"), CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"),
		Orgs: map[string]string{"1": org}, OrgAllow: []string{"127.0.0.0/8"}}, store, log)
	go func() { _ = relay.Run() }()

	// the organization's database: records what reaches it
	db, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var mu sync.Mutex
	got := 0
	go func() {
		for {
			c, err := db.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				b := make([]byte, 8)
				if n, _ := io.ReadAtLeast(c, b, 8); n == 8 { // not the agent's TCP probe of the database
					mu.Lock()
					got++
					mu.Unlock()
				}
			}()
		}
	}()
	tok, err := store.CreateToken(ctx, "1", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	tf := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tf, []byte(tok), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second) // the enrollment port
	ag := NewAgent(AgentConfig{Relay: agentAddr, Enroll: enroll, ServerName: "localhost", CA: filepath.Join(dir, "ca.crt"),
		StateDir: t.TempDir(), TokenFile: tf, DB: db.Addr().String()}, log)
	go func() { _ = ag.Run() }()
	for i := 0; ; i++ {
		ports, _ := store.relayPorts(ctx)
		if len(relay.Sessions()) > 0 && ports[orgPort] {
			break
		}
		if i > 300 {
			t.Fatal("no agent session, or the relay did not publish its port")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // the relay has read the pin (every 2 s)

	// move-org's connection to the new port, seconds after its first ones
	c, err := DialProving(nil)(ctx, "tcp", org)
	if err != nil {
		t.Fatalf("the new port: %v", err)
	}
	defer c.Close()
	if _, err := c.Write([]byte{0, 0, 0, 8, 1, 2, 3, 4}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(4 * time.Second)
	mu.Lock()
	defer mu.Unlock()
	if got != 1 {
		t.Fatalf("the connection to the organization's new port did not reach its database (refused at the relay: not admitted)")
	}
}
