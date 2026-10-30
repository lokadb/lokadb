package tunnel

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

// pinMem: a MemStore that keeps the SaaS database's addresses as PGStore does.
type pinMem struct {
	*MemStore
	mu      sync.Mutex
	sources map[string]string // addr -> how
	codes   map[string]bool
	refused map[string]int
}

func (p *pinMem) Sources(context.Context) ([]Source, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Source
	for a, how := range p.sources {
		out = append(out, Source{Addr: a, How: how, Valid: how != "seen"})
	}
	return out, nil
}

func (p *pinMem) SeeSource(_ context.Context, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sources[addr] == "" {
		p.sources[addr] = "seen"
	}
	return nil
}

func (p *pinMem) TakeProbe(_ context.Context, code string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ok := p.codes[code]
	delete(p.codes, code)
	return ok, nil
}

func (p *pinMem) ProveSource(_ context.Context, addr, how string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sources[addr] = how
	return nil
}

func (p *pinMem) Refuse(_ context.Context, addr, _ string, pending bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !pending {
		p.refused[addr]++
	}
	return nil
}

func (p *pinMem) SetRelayPorts(context.Context, []string) error { return nil }
func (p *pinMem) TouchSource(context.Context, string) error     { return nil }

// pgFrom connects to addr from the local address ip, sends a startup message and returns the
// error the other side answers with ("" when it answers something else).
func pgFrom(t *testing.T, ip, addr string, params map[string]string) string {
	t.Helper()
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(ip)}, Timeout: 5 * time.Second}
	c, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	fe := pgproto3.NewFrontend(c, c)
	fe.Send(&pgproto3.StartupMessage{ProtocolVersion: pgproto3.ProtocolVersionNumber, Parameters: params})
	if err := fe.Flush(); err != nil {
		t.Fatal(err)
	}
	m, err := fe.Receive()
	if err != nil {
		return "no answer: " + err.Error()
	}
	if e, ok := m.(*pgproto3.ErrorResponse); ok {
		return e.Message
	}
	return ""
}

// R01: a copy of the SaaS database reached a customer's database through the relay. Before an
// address is proven the relay lets org_allow in and records what it sees; a probe proves the SaaS
// database's address; from then on another address is refused with what it means, unless a proof
// of it comes while it is held (the database at a new address goes on).
func TestPinSaaSDatabase(t *testing.T) {
	defer func(h time.Duration) { pinHold = h }(pinHold)
	pinHold = time.Second
	ps := &pinMem{sources: map[string]string{}, codes: map[string]bool{}, refused: map[string]int{}}
	orgAddr := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"7": orgAddr}}, func(m *MemStore) Store { ps.MemStore = m; return ps })
	waitFor(t, "organization 7's port", func() bool { return tr.r.listening("7", orgAddr) })
	user := map[string]string{"user": "lokadb_app_org7", "database": "cust"}
	notConnected := "the agent of organization 7 is not connected" // got through the pin

	// nothing proven: org_allow passes, the address is recorded as seen
	if got := pgFrom(t, "127.0.0.1", orgAddr, user); !strings.Contains(got, notConnected) {
		t.Fatalf("nothing proven: %q", got)
	}
	if ps.sources["127.0.0.1"] != "seen" {
		t.Fatalf("seen: %v", ps.sources)
	}

	// a probe with a good code proves the address; a used or unknown code proves nothing
	ps.codes["c0de"] = true
	if got := pgFrom(t, "127.0.0.1", orgAddr, map[string]string{"user": "c0de", "database": ProbeDatabase}); got != ProbeOK {
		t.Fatalf("probe: %q", got)
	}
	if got := pgFrom(t, "127.0.0.2", orgAddr, map[string]string{"user": "c0de", "database": ProbeDatabase}); !strings.Contains(got, "unknown or used probe code") {
		t.Fatalf("a used code: %q", got)
	}
	if ps.sources["127.0.0.1"] != "probe" || ps.sources["127.0.0.2"] != "" {
		t.Fatalf("proven: %v", ps.sources)
	}

	// the proven address passes at once; another one (a restored copy) is held, then refused
	if got := pgFrom(t, "127.0.0.1", orgAddr, user); !strings.Contains(got, notConnected) {
		t.Fatalf("the SaaS database: %q", got)
	}
	start := time.Now()
	got := pgFrom(t, "127.0.0.2", orgAddr, user)
	if !strings.Contains(got, "connection from 127.0.0.2 refused: it is not the SaaS database organization 7 was moved from") || !strings.Contains(got, "lokadb defuse") {
		t.Fatalf("a copy: %q", got)
	}
	if took := time.Since(start); took < pinHold {
		t.Fatalf("not held: %v", took)
	}
	if ps.refused["127.0.0.2"] != 1 {
		t.Fatalf("refusal not recorded: %v", ps.refused)
	}

	// the SaaS database at a new address: proven while held (the runner's probe), it goes on
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = ps.ProveSource(context.Background(), "127.0.0.3", "probe")
	}()
	if got := pgFrom(t, "127.0.0.3", orgAddr, user); !strings.Contains(got, notConnected) {
		t.Fatalf("a new address proven while held: %q", got)
	}
}
