//go:build !linux

package proxy

import "time"

// setUserTimeout: TCP_USER_TIMEOUT is Linux's; elsewhere keepalive alone.
func setUserTimeout(fd uintptr, d time.Duration) error { return nil }
