package proxy

import (
	"context"
	"net"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// PR-6: a direct connection is dialed with TCP_USER_TIMEOUT and keepalive on.
func TestDirectNetDial(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := directNetDial(context.Background(), "tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	raw, err := c.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var ut, ka int
	_ = raw.Control(func(fd uintptr) {
		ut, _ = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout)
		ka, _ = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_KEEPALIVE)
	})
	if ut != int(directUserTimeout/time.Millisecond) || ka != 1 {
		t.Fatalf("TCP_USER_TIMEOUT %d ms, SO_KEEPALIVE %d: want %d, 1", ut, ka, directUserTimeout/time.Millisecond)
	}
}

// Third review of the protocol, 7: an organization's database by a unix socket (a host that is
// a directory): no TCP_USER_TIMEOUT there, the connection opens.
func TestDirectNetDialUnix(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".s.PGSQL.5432")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		if c, err := l.Accept(); err == nil {
			c.Close()
		}
	}()
	c, err := directNetDial(context.Background(), "unix", path)
	if err != nil {
		t.Fatalf("a unix socket: %v", err)
	}
	c.Close()
}
