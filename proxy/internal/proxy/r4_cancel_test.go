package proxy

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

	"github.com/lokadb/lokadb/proxy/internal/tunnel"
)

func r4free(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// R4 (tunnel): the proxy's CancelRequest for a statement on a direct connection (sendCancel)
// dials the relay's organization port with a plain net.DialTimeout, without the admission
// (tunnel.DialProving) every other connection of LokaDB's to that port has. Once the relay
// pins the SaaS database's address the cancel comes from an address that is not proven: the
// relay holds it 2.5 s, drops it, records the proxy as "not the SaaS database (a copy?)", and
// sendCancel reports success.
func TestR4_DirectCancelRefusedByPinnedRelay(t *testing.T) {
	dsn := os.Getenv("R4_PG")
	if dsn == "" {
		t.Skip("R4_PG not set")
	}
	ctx := context.Background()
	store, err := tunnel.OpenPGStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	// the SaaS database's address is proven (the runner's probe): the relay pins it
	if err := store.ProveSource(ctx, "127.0.0.9", "probe"); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := tunnel.GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	if os.Getenv("R4_LOG") != "" {
		log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	org, agentAddr, enroll := r4free(t), r4free(t), r4free(t)
	relay := tunnel.NewRelay(tunnel.RelayConfig{Listen: agentAddr, EnrollListen: enroll, Cert: filepath.Join(dir, "relay.crt"),
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
	var got [][]byte
	go func() {
		for {
			c, err := db.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
				b := make([]byte, 64)
				n, _ := io.ReadAtLeast(c, b, 8)
				if n == 0 { // the agent's TCP probe of the database (agentstatus.go)
					return
				}
				mu.Lock()
				got = append(got, b[:n])
				mu.Unlock()
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
	ag := tunnel.NewAgent(tunnel.AgentConfig{Relay: agentAddr, Enroll: enroll, ServerName: "localhost", CA: filepath.Join(dir, "ca.crt"),
		StateDir: t.TempDir(), TokenFile: tf, DB: db.Addr().String()}, log)
	go func() { _ = ag.Run() }()
	for i := 0; ; i++ {
		if len(relay.Sessions()) > 0 {
			break
		}
		if i > 300 {
			t.Fatal("no agent session")
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(3 * time.Second) // the relay has read the pin (every 2 s)

	tunnel.ProveSelfWith(store) // as the proxy's main does with the control plane
	defer tunnel.ProveSelfWith(nil)
	time.Sleep(time.Second) // the relay has written its ports
	start := time.Now()
	err = sendCancel(org, nil, false, 4242, []byte{1, 2, 3, 4}) // the proxy's cancel, from 127.0.0.1
	t.Logf("sendCancel: %v (took %v)", err, time.Since(start).Round(time.Millisecond))
	time.Sleep(4 * time.Second)
	mu.Lock()
	n := len(got)
	mu.Unlock()
	refused, _ := store.RefusedSources(ctx, time.Minute)
	t.Logf("bytes that reached the organization's database: %d connection(s); refused at the relay: %+v", n, refused)
	if err != nil || n != 1 || len(refused) != 0 {
		t.Fatalf("the cancel did not get through the pinned relay: err %v, reached %d, refused %v", err, n, refused)
	}
}
