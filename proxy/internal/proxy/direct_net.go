package proxy

import (
	"context"
	"net"
	"strings"
	"syscall"
	"time"
)

// The direct connections' network (review of the protocol, PR-6): a connection whose network
// goes silent (a NAT that drops packets without a RST, a relay or agent gone) would wait for
// TCP's own retries, some 15 minutes, holding the organization's pause in the cloud. A deadline
// on reads would cut statements that run long on purpose; these catch the silent network alone:
// keepalive probes while the server computes and nothing is in flight, and TCP_USER_TIMEOUT
// (Linux) when what the proxy sent is not acknowledged. Either ends the connection: 08006, as
// any broken direct connection.
const (
	directKeepIdle     = 15 * time.Second // keepalive: the first probe after this long of silence
	directKeepInterval = 5 * time.Second
	directKeepCount    = 4
	directUserTimeout  = 30 * time.Second // data sent and not acknowledged for this long: the connection is dead
)

// directNetDial dials a direct connection with the keepalive and TCP_USER_TIMEOUT above.
func directNetDial(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{
		KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: directKeepIdle, Interval: directKeepInterval, Count: directKeepCount},
		Control: func(network, address string, c syscall.RawConn) error {
			if !strings.HasPrefix(network, "tcp") { // a unix socket has no TCP_USER_TIMEOUT (third review of the protocol, 7)
				return nil
			}
			var serr error
			if err := c.Control(func(fd uintptr) { serr = setUserTimeout(fd, directUserTimeout) }); err != nil {
				return err
			}
			return serr
		},
	}
	return d.DialContext(ctx, network, addr)
}
