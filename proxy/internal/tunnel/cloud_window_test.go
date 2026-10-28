package tunnel

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// startupFor: a Postgres startup message of user to db (replication "database" when repl).
func startupFor(user, db string, repl bool) []byte {
	p := map[string]string{"user": user, "database": db}
	if repl {
		p["replication"] = "database"
	}
	b, _ := (&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: p}).Encode(nil)
	return b
}

// moverStartup: the startup of the move's subscription the tests' windows admit.
func moverStartup() []byte { return startupFor("mover", "movedb", true) }

// pgCloud: a cloud database that reads a startup message, answers "cloud:<user>@<db>\n" and
// then echoes every line with "cloud:" before it.
func pgCloud(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				m, err := pgproto3.NewBackend(c, c).ReceiveStartupMessage()
				sm, ok := m.(*pgproto3.StartupMessage)
				if err != nil || !ok {
					return
				}
				if _, err := c.Write([]byte("cloud:" + sm.Parameters["user"] + "@" + sm.Parameters["database"] + "\n")); err != nil {
					return
				}
				sc := bufio.NewScanner(c)
				for sc.Scan() {
					if _, err := c.Write([]byte("cloud:" + sc.Text() + "\n")); err != nil {
						return
					}
				}
			}()
		}
	}()
	return l.Addr().String()
}

// R3-13: a window to the cloud was raw TCP to the whole cloud database for the organization's
// agent. Now the relay reads the startup: only the move's role, to its database, for
// replication gets through; any other role, database or a plain connection gets 28000.
func TestCloudWindowAdmitsTheMoveOnly(t *testing.T) {
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, Cloud: pgCloud(t), CloudAccess: "window"}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return tr.session("1") != "" && len(a.sess) > 0
	})
	tr.store.SetCloud("1", true, "mover", "movedb")
	try := func(first ...[]byte) string {
		st, err := a.openCloud()
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		_ = st.SetDeadline(time.Now().Add(5 * time.Second))
		for _, b := range first {
			if _, err := st.Write(b); err != nil {
				return err.Error()
			}
			if len(b) == 8 { // an SSLRequest: its one-byte answer
				ans := make([]byte, 1)
				if _, err := st.Read(ans); err != nil || ans[0] != 'N' {
					return "ssl answer " + string(ans)
				}
			}
		}
		buf := make([]byte, 512)
		n, _ := st.Read(buf)
		return string(buf[:n])
	}
	if got := try(moverStartup()); got != "cloud:mover@movedb\n" {
		t.Fatalf("the move's subscription: %q", got)
	}
	if got := try(sslRequest, moverStartup()); got != "cloud:mover@movedb\n" {
		t.Fatalf("the move's subscription asking for TLS first: %q", got)
	}
	for what, first := range map[string][]byte{
		"another role":     startupFor("postgres", "movedb", true),
		"another database": startupFor("mover", "postgres", true),
		"not replication":  startupFor("mover", "movedb", false),
	} {
		if got := try(first); !strings.Contains(got, "28000") || !strings.Contains(got, "admits only the move's role") {
			t.Fatalf("%s through the window: %q", what, got)
		}
	}
	// a window opened without a role admits nobody
	tr.store.SetCloud("1", true)
	time.Sleep(1100 * time.Millisecond)
	if got := try(moverStartup()); !strings.Contains(got, "names no role and database") {
		t.Fatalf("a window without a role: %q", got)
	}
}

// moverGreeting: pgCloud's answer to moverStartup.
const moverGreeting = "cloud:mover@movedb\n"
