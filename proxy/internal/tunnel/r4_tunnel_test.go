package tunnel

// Review 4 of the tunnel (R4-17, R4-18): the reviewer's scenarios, asserting what must hold.

import (
	"os"

	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// toggleProxy: a TCP proxy in front of the issuing service that can be "down" (an issuer
// restart or deploy): accepted connections are closed at once.
type toggleProxy struct {
	down   atomic.Bool
	ln     net.Listener
	target string
}

func newToggleProxy(t *testing.T, target string) *toggleProxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	p := &toggleProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			if p.down.Load() {
				c.Close()
				continue
			}
			go func() {
				u, err := net.Dial("tcp", p.target)
				if err != nil {
					c.Close()
					return
				}
				pipe(c, u)
			}()
		}
	}()
	return p
}

func (p *toggleProxy) url() string {
	_, port, _ := net.SplitHostPort(p.ln.Addr().String())
	return "https://localhost:" + port
}

func startIssuer(t *testing.T, dir, ttl string) string {
	addr := freeAddr(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	is := NewIssuer(IssuerConfig{Listen: addr, Cert: filepath.Join(dir, "relay.crt"), Key: filepath.Join(dir, "relay.key"),
		CA: filepath.Join(dir, "ca.crt"), CAKey: filepath.Join(dir, "ca.key"), CertTTL: ttl, Relays: []string{"localhost"}}, log)
	go func() { _ = is.Run() }()
	waitFor(t, "issuer up", func() bool {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			c.Close()
		}
		return err == nil
	})
	return addr
}

// R4-1 (direct API). renewTooOften counts a renewal before it is signed and recorded: three
// attempts that fail at the issuing service (or at the registry write) use up the certificate's
// renewals, and once the issuer is back the relay answers 429 "renewed already" to every later
// attempt, though no certificate was ever issued from it.
func TestR4_RenewalsUsedUpByFailedSignatures(t *testing.T) {
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	px := newToggleProxy(t, startIssuer(t, dir, "6s"))
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: filepath.Join(dir, "ca.crt"),
		IssuerURL: px.url(), EnrollFailPerMin: -1, strictRenewTest: true}, nil)
	state := agentDir(t)
	if err := tr.agent(t, state, tr.store.CreateToken("1"), "127.0.0.1:1", true).Run(); err != nil {
		t.Fatal(err)
	}
	st, err := loadState(state)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2700 * time.Millisecond) // a third of 6 s (+0.6 s of skew allowance) is gone

	px.down.Store(true) // the issuing service restarts for ~20 s: the agent's attempts every 10 s fail
	for i := 0; i < 3; i++ {
		code, out := tr.renewWith(t, *st.cert)
		t.Logf("issuer down, attempt %d: %d %q", i+1, code, out.Error)
		if code != http.StatusInternalServerError {
			t.Fatalf("attempt %d with the issuer down: %d %q", i+1, code, out.Error)
		}
	}
	px.down.Store(false) // the issuer is back
	code, out := tr.renewWith(t, *st.cert)
	if code != http.StatusOK {
		t.Fatalf("the issuer is back, three attempts failed before: %d %q (failed attempts must not use the certificate up)", code, out.Error)
	}
	if n := tr.store.Count("renew"); n != 1 {
		t.Fatalf("renewals recorded: %d", n)
	}
}

// R4-1 (the agent). The same with the agent's own renewal loop: the issuer is down for ~22 s
// at the agent's renewal time; afterwards the agent never renews, keeps retrying (429 is not a
// refusal it exits on), and after its certificate ends a network blip leaves it without a tunnel.
func TestR4_AgentLockedOutAfterIssuerOutage(t *testing.T) {
	if testing.Short() {
		t.Skip("about 50 s")
	}
	dir := t.TempDir()
	if err := GenerateCerts(dir, "localhost", nil); err != nil {
		t.Fatal(err)
	}
	px := newToggleProxy(t, startIssuer(t, dir, "3s"))
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": freeAddr(t)}, CA: filepath.Join(dir, "ca.crt"),
		IssuerURL: px.url(), EnrollFailPerMin: -1, strictRenewTest: true}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), echoServer(t, "db:"), false)
	exited := make(chan error, 1)
	go func() { exited <- a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	first := a.Serial()
	px.down.Store(true) // the agent renews at 2 s, then retries at ~12 s and ~22 s: all fail
	time.Sleep(23 * time.Second)
	px.down.Store(false)
	t.Logf("issuer back at %s", time.Now().Format("15:04:05"))
	time.Sleep(20 * time.Second) // two more attempts, the issuer up
	st := a.Status()
	t.Logf("agent: serial %s (first %s) renew_error %q state %s", st.Serial, first, st.RenewErr, st.State)
	if a.Serial() == first || tr.store.Count("renew") == 0 {
		t.Fatalf("the agent did not renew after the issuer came back: serial %s, renewals %d, renew error %q", a.Serial(), tr.store.Count("renew"), st.RenewErr)
	}
	select {
	case err := <-exited:
		t.Fatalf("the agent exited: %v", err)
	default:
	}
}

// R4-2. After a renewal the agent and the relay retire the old session: GoAway, then closed
// once its streams end, however long that takes: a connection through the tunnel that is still open
// 10 minutes after a renewal (a long move step, a table copy, a long query over postgres_fdw) lives on.
func TestR4_RenewalCutsLongConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("about 11 minutes")
	}
	org1 := freeAddr(t)
	tr := startRelay(t, RelayConfig{Orgs: map[string]string{"1": org1}, CertTTL: "1h", EnrollFailPerMin: -1}, nil)
	a := tr.agent(t, agentDir(t), tr.store.CreateToken("1"), lineEcho(t, "db:"), false)
	go func() { _ = a.Run() }()
	waitFor(t, "agent", func() bool { return tr.session("1") != "" })
	c, err := net.Dial("tcp", org1)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	rd := bufio.NewReader(c)
	ask := func() error {
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Write([]byte("q\n")); err != nil {
			return err
		}
		l, err := rd.ReadString('\n')
		if err == nil && l != "db:q\n" {
			err = errors.New("answer " + l)
		}
		return err
	}
	if err := ask(); err != nil {
		t.Fatal(err)
	}
	if err := a.renew(); err != nil { // the agent's renewal at two thirds of the certificate's life
		t.Fatal(err)
	}
	renewed := time.Now()
	waitFor(t, "the session on the new certificate", func() bool { return tr.session("1") == a.Serial() })
	for {
		time.Sleep(10 * time.Second)
		if err := ask(); err != nil {
			t.Fatalf("the connection opened before the renewal was cut %v after it: %v", time.Since(renewed).Round(time.Second), err)
		}
		if time.Since(renewed) > 12*time.Minute {
			return
		}
	}
}

// R4-36. The revocation check does not walk the renewal chain: with a long chain per agent it stays
// quick, and revoking an agent still revokes every certificate of its chain, also a renewal written after.
func TestR4_RevocationCheckIsFlatOnLongChains(t *testing.T) {
	dsn := os.Getenv("LOKADB_REVIEW3_PG")
	if dsn == "" {
		t.Skip("LOKADB_REVIEW3_PG: a Postgres DSN for a scratch registry")
	}
	ctx := context.Background()
	pg, err := OpenPGStore(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	org := fmt.Sprintf("r4chain%d", time.Now().UnixNano())
	// 100 agents, each with a chain of 2000 renewals (about 55 days at one an hour)
	if _, err := pg.db.Exec(ctx, `INSERT INTO lokadb_cp.agent_certs (serial, org, kind, not_after, parent)
		SELECT $1 || '-' || a || '-' || n, $1, CASE WHEN n = 1 THEN 'enroll' ELSE 'renew' END, now() + interval '1 hour',
		       CASE WHEN n = 1 THEN NULL ELSE $1 || '-' || a || '-' || (n - 1) END
		FROM generate_series(1, 100) a, generate_series(1, 2000) n`, org); err != nil {
		t.Fatal(err)
	}
	var tips []string
	for a := 1; a <= 100; a++ {
		tips = append(tips, fmt.Sprintf("%s-%d-2000", org, a))
	}
	start := time.Now()
	if rev, err := pg.Revoked(ctx, tips); err != nil || len(rev) != 0 {
		t.Fatalf("revoked %v, err %v", rev, err)
	}
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("checking 100 agents with chains of 2000 took %v", d)
	}
	// revoking one agent by a serial in the middle of its chain revokes the chain, and a renewal of it written after
	if _, _, err := pg.Revoke(ctx, org, org+"-7-1000"); err != nil {
		t.Fatal(err)
	}
	if err := pg.RecordCert(ctx, org+"-7-2001", org, time.Now().Add(time.Hour), "renew", "x", org+"-7-2000"); err != nil {
		t.Fatal(err)
	}
	rev, err := pg.Revoked(ctx, []string{org + "-7-1", org + "-7-2000", org + "-7-2001", org + "-8-2000"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rev) != 3 {
		t.Errorf("revoked %v, want the three of agent 7 and not agent 8", rev)
	}
	_, _ = pg.db.Exec(ctx, `DELETE FROM lokadb_cp.agent_certs WHERE org = $1`, org)
}
