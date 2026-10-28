package proxy

import (
	"syscall"
	"time"
)

// tcpUserTimeout: TCP_USER_TIMEOUT of linux/tcp.h (package syscall does not name it).
const tcpUserTimeout = 0x12

func setUserTimeout(fd uintptr, d time.Duration) error {
	return syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout, int(d/time.Millisecond))
}
