package proxy

import (
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"time"
)

// allowList: the addresses that may connect (Config.Allow). Empty: every address (the proxy is
// then for a private network). Built from IPs and CIDRs; a list that names the whole internet is
// refused: an allowlist that lets everybody in is not one.
type allowList struct {
	prefixes []netip.Prefix
	mu       sync.Mutex
	told     map[netip.Addr]time.Time // the last time a refusal of this address was logged
}

func parseAllow(entries []string) (*allowList, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	a := &allowList{told: map[netip.Addr]time.Time{}}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		if e == "" {
			continue
		}
		p, err := netip.ParsePrefix(e)
		if err != nil {
			ip, ierr := netip.ParseAddr(e)
			if ierr != nil {
				return nil, fmt.Errorf("allow: %q is not an IP address or a CIDR range", e)
			}
			p = netip.PrefixFrom(ip.Unmap(), ip.Unmap().BitLen())
		}
		if p.Bits() == 0 {
			return nil, fmt.Errorf("allow: %q is the whole internet: name the addresses that may connect", e)
		}
		a.prefixes = append(a.prefixes, p.Masked())
	}
	if len(a.prefixes) == 0 {
		return nil, nil
	}
	return a, nil
}

// allows: whether the connection's remote address is on the list (a nil list allows all).
func (a *allowList) allows(c net.Conn) (netip.Addr, bool) {
	if a == nil {
		return netip.Addr{}, true
	}
	ta, ok := c.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return netip.Addr{}, false
	}
	ip, ok := netip.AddrFromSlice(ta.IP)
	if !ok {
		return netip.Addr{}, false
	}
	ip = ip.Unmap()
	for _, p := range a.prefixes {
		if p.Contains(ip) {
			return ip, true
		}
	}
	return ip, false
}

// shouldTell: log a refusal of this address at most once a minute (a scan must not fill the log).
func (a *allowList) shouldTell(ip netip.Addr) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.told) > 4096 {
		a.told = map[netip.Addr]time.Time{}
	}
	if t, ok := a.told[ip]; ok && time.Since(t) < time.Minute {
		return false
	}
	a.told[ip] = time.Now()
	return true
}
