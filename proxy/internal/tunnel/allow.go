package tunnel

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// allowList decides which peers may connect to a listening port: IPs, CIDRs and host names
// (resolved again at most every 30 s, so a restarted container with a new address is let in).
// An empty list allows everybody.
type allowList struct {
	nets  []*net.IPNet
	hosts []string
	// self: also the addresses of this host's own interfaces (a database on the agent's host
	// that reaches -cloud-listen by the host's external address comes from that address)
	self bool

	mu       sync.Mutex
	resolved []net.IP
	at       time.Time
	looking  bool
	first    sync.Mutex // the first lookup: callers wait for it (there is no answer to take yet)
}

func newAllowList(entries []string) (*allowList, error) {
	a := &allowList{}
	for _, e := range entries {
		e = strings.TrimSpace(e)
		switch {
		case e == "":
		case strings.Contains(e, "/"):
			_, n, err := net.ParseCIDR(e)
			if err != nil {
				return nil, fmt.Errorf("allow list: %w", err)
			}
			a.nets = append(a.nets, n)
		case net.ParseIP(e) != nil:
			ip := net.ParseIP(e)
			a.nets = append(a.nets, &net.IPNet{IP: ip, Mask: net.CIDRMask(len(ip)*8, len(ip)*8)})
		default:
			a.hosts = append(a.hosts, e)
		}
	}
	return a, nil
}

func (a *allowList) empty() bool { return len(a.nets) == 0 && len(a.hosts) == 0 }

// allows reports whether the peer at addr (host:port) may connect.
func (a *allowList) allows(addr net.Addr) bool {
	if a.empty() {
		return true
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, n := range a.nets {
		if n.Contains(ip) {
			return true
		}
	}
	for _, r := range a.hostIPs() {
		if r.Equal(ip) {
			return true
		}
	}
	return a.self && ownAddress(ip)
}

// ownAddress: ip is an address of one of this host's interfaces (read on each call: the
// connections checked are few, and an address added later counts).
func ownAddress(ip net.IP) bool {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok && n.IP.Equal(ip) {
			return true
		}
	}
	return false
}

func (a *allowList) hostIPs() []net.IP {
	if len(a.hosts) == 0 {
		return nil
	}
	a.first.Lock()
	a.mu.Lock()
	if a.at.IsZero() { // nothing resolved yet: this caller looks, under first
		a.mu.Unlock()
		defer a.first.Unlock()
	} else {
		a.mu.Unlock()
		a.first.Unlock()
	}
	a.mu.Lock()
	if !a.at.IsZero() && (time.Since(a.at) < 30*time.Second || a.looking) {
		ips := a.resolved
		a.mu.Unlock()
		return ips // one lookup at a time, the others take the last answer (review 3, R3-20)
	}
	a.looking = true
	a.mu.Unlock()
	var ips []net.IP
	failed := false
	for _, h := range a.hosts {
		addrs, err := net.LookupIP(h)
		if err != nil {
			failed = true
			continue
		}
		ips = append(ips, addrs...)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.looking = false
	if failed && len(a.resolved) > 0 { // a DNS failure keeps the last good answer
		a.at = time.Now()
		return a.resolved
	}
	a.resolved, a.at = ips, time.Now()
	return ips
}
