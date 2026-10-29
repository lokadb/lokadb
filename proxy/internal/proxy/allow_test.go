package proxy

import (
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"
)

type addrConn struct {
	net.Conn
	ip string
}

func (c addrConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.ParseIP(c.ip), Port: 5555} }

func TestAllowList(t *testing.T) {
	a, err := parseAllow([]string{"203.0.113.7", "198.51.100.0/24", "2001:db8::/32"})
	if err != nil {
		t.Fatal(err)
	}
	for ip, want := range map[string]bool{"203.0.113.7": true, "203.0.113.8": false, "198.51.100.200": true, "198.51.101.1": false, "2001:db8::5": true, "2001:db9::1": false, "::ffff:203.0.113.7": true, "10.0.0.1": false} {
		if _, ok := a.allows(addrConn{ip: ip}); ok != want {
			t.Errorf("%s: %v, want %v", ip, ok, want)
		}
	}
	var none *allowList
	if _, ok := none.allows(addrConn{ip: "1.2.3.4"}); !ok {
		t.Error("no list must let everyone in")
	}
	if l, err := parseAllow(nil); l != nil || err != nil {
		t.Errorf("empty: %v %v", l, err)
	}
	for _, bad := range []string{"0.0.0.0/0", "::/0", "not-an-ip", "300.1.1.1"} {
		if _, err := parseAllow([]string{"10.0.0.1", bad}); err == nil {
			t.Errorf("%q accepted", bad)
		} else if bad == "0.0.0.0/0" && !strings.Contains(err.Error(), "whole internet") {
			t.Errorf("world-open said: %v", err)
		}
	}
	// a flood from one address is logged once a minute
	ip, _ := a.allows(addrConn{ip: "192.0.2.9"})
	if !a.shouldTell(ip) || a.shouldTell(ip) {
		t.Error("refusals of one address must be logged once")
	}
}

// The proxy with an allow list: an address that is not on it is closed before a byte is read; one that
// is gets the answer to an SSLRequest. And a list that means everybody does not start the proxy.
func TestProxyAllowList(t *testing.T) {
	cert, key := selfSigned(t, t.TempDir(), "proxy")
	probe := func(addr string) string { // the answer to an SSLRequest: "S", "N", or what a closed connection gives
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return "dial: " + err.Error()
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = c.Write([]byte{0, 0, 0, 8, 4, 210, 22, 47})
		b := make([]byte, 1)
		if _, err := c.Read(b); err != nil {
			return "closed"
		}
		return string(b)
	}
	start := func(allow []string) string {
		l, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := l.Addr().String()
		l.Close()
		s := NewServer(Config{Listen: addr, Upstream: "host=127.0.0.1 port=1 sslmode=disable", TLS: &TLSConfig{Cert: cert, Key: key, Require: true}, Allow: allow},
			slog.New(slog.NewTextHandler(io.Discard, nil)))
		go func() { _ = s.ListenAndServe() }()
		for i := 0; i < 100; i++ {
			if c, err := net.Dial("tcp", addr); err == nil {
				c.Close()
				return addr
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("proxy did not start")
		return ""
	}
	if got := probe(start([]string{"127.0.0.0/8"})); got != "S" {
		t.Errorf("a listed address: %q, want the TLS answer S", got)
	}
	if got := probe(start([]string{"203.0.113.7"})); got != "closed" {
		t.Errorf("an address not on the list: %q, want the connection closed", got)
	}
	if got := probe(start(nil)); got != "S" {
		t.Errorf("no list: %q", got)
	}
	s := NewServer(Config{Listen: "127.0.0.1:0", Upstream: "host=127.0.0.1 port=1", Allow: []string{"0.0.0.0/0"}}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := s.ListenAndServe(); err == nil || !strings.Contains(err.Error(), "whole internet") {
		t.Errorf("a list of everybody: %v", err)
	}
}
