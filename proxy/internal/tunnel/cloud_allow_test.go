package tunnel

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ownIP: a non-loopback address of this host's interfaces ("" when it has none).
func ownIP() string {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && !n.IP.IsLoopback() && n.IP.To4() != nil {
			return n.IP.String()
		}
	}
	return ""
}

// cloudListenAnswer: the agent's answer to a startup on -cloud-listen from address from ("" when it
// closed the connection without one).
func cloudListenAnswer(t *testing.T, from, to string) string {
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(from)}, Timeout: 2 * time.Second}
	c, err := d.Dial("tcp", to)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: map[string]string{"user": "mover", "database": "movedb", "replication": "database"}})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := fe.Receive()
	if err != nil {
		return ""
	}
	if e, ok := m.(*pgproto3.ErrorResponse); ok {
		return e.Message
	}
	return "?"
}

// Block 14's stand: the organization's Postgres runs on the agent's host (-db 127.0.0.1:...)
// and the move's subscription reaches -cloud-listen by the host's external address, so it
// comes from that address. The default allow list (the -db host and loopback) closed the
// connection without a word: "could not connect to the publisher: server closed the
// connection unexpectedly" at step 4/6. Now the host's own addresses are allowed by default,
// and a peer not allowed gets an error that says so.
func TestCloudListenOwnAddress(t *testing.T) {
	ip := ownIP()
	if ip == "" {
		t.Skip("no non-loopback address")
	}
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, Cloud: pgCloud(t), CloudAccess: "window"}, nil)
	start := func(allow []string) string {
		l, err := net.Listen("tcp", ip+":0")
		if err != nil {
			t.Fatal(err)
		}
		cl := l.Addr().String()
		l.Close()
		a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
		a.cfg.CloudListen, a.cfg.CloudAllow = cl, allow
		go func() { _ = a.Run() }()
		waitFor(t, "agent", func() bool {
			a.mu.Lock()
			defer a.mu.Unlock()
			return len(a.sess) > 0
		})
		return cl
	}
	// the default list: through to the relay, which answers for the window
	if msg := cloudListenAnswer(t, ip, start(nil)); !strings.Contains(msg, "not open to organization 1") {
		t.Fatalf("from the host's own address %s, default allow list: %q", ip, msg)
	}
	// an explicit list without it: refused, with the reason
	if msg := cloudListenAnswer(t, ip, start([]string{"127.0.0.1"})); !strings.Contains(msg, "refused; add it to -cloud-allow") {
		t.Fatalf("from %s, -cloud-allow 127.0.0.1: %q", ip, msg)
	}
}
