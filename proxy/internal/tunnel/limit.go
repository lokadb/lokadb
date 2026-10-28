package tunnel

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// failLimiter counts failed enrollment and renewal attempts per client host: each host has
// a bucket of perMin failures that refills at perMin a minute. An empty bucket answers 429
// before the registry is asked, so guessing tokens (or flooding the registry with bad ones)
// costs one failure per perMin-th of a minute. Successful requests cost nothing, so many
// agents behind one NAT enroll freely.
//
// A client that shows a certificate of the CA (a renewal) is counted as its agent, not by
// host: agents behind one NAT do not use up each other's failures (review of the tunnel,
// N-2); an IPv6 host is counted by its /64.
type failLimiter struct {
	mu      sync.Mutex
	perMin  float64
	buckets map[string]*bucket
	now     func() time.Time
	swept   time.Time // the last pass over a full map (at most one per 10 s)
}

type bucket struct {
	left float64
	at   time.Time
}

const maxBuckets = 100000

func newFailLimiter(perMin int) *failLimiter {
	return &failLimiter{perMin: float64(perMin), buckets: map[string]*bucket{}, now: time.Now}
}

func hostOf(remote string) string {
	h, _, err := net.SplitHostPort(remote)
	if err != nil {
		h = remote
	}
	if ip := net.ParseIP(h); ip != nil && ip.To4() == nil {
		return ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
	}
	return h
}

// refill brings the host's bucket up to now; the caller holds mu.
func (l *failLimiter) refill(host string) *bucket {
	now := l.now()
	b := l.buckets[host]
	if b == nil {
		if len(l.buckets) >= maxBuckets && now.Sub(l.swept) > 10*time.Second {
			l.swept = now
			for h, o := range l.buckets { // forget hosts whose bucket is full again
				if o.left+now.Sub(o.at).Minutes()*l.perMin >= l.perMin {
					delete(l.buckets, h)
				}
			}
		}
		b = &bucket{left: l.perMin, at: now}
		l.buckets[host] = b
	}
	b.left = min(l.perMin, b.left+now.Sub(b.at).Minutes()*l.perMin)
	b.at = now
	return b
}

// limitKey: who a request's failures count against: its agent when it shows a certificate,
// else its host.
func limitKey(req *http.Request) string {
	if req.TLS != nil && len(req.TLS.PeerCertificates) > 0 {
		return "agent " + agentID(req.TLS.PeerCertificates[0])
	}
	return hostOf(req.RemoteAddr)
}

// blocked reports whether the key (limitKey) has used up its failures.
func (l *failLimiter) blocked(remote string) bool {
	if l == nil || l.perMin <= 0 {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.refill(remote).left < 1
}

// fail records a failed attempt of the host.
func (l *failLimiter) fail(remote string) {
	if l == nil || l.perMin <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.refill(remote)
	b.left = max(0, b.left-1)
}

// cappedListener closes a connection beyond max open at once as soon as it is accepted: the
// enrollment port faces the internet and shares the process's descriptors with every
// organization's tunnel (review 3 of the tunnel, R3-5).
type cappedListener struct {
	net.Listener
	open chan struct{}
}

func capListener(l net.Listener, max int) net.Listener {
	if max < 0 {
		return l
	}
	return &cappedListener{Listener: l, open: make(chan struct{}, max)}
}

func (l *cappedListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if take(l.open) {
			return &cappedConn{Conn: c, l: l}, nil
		}
		c.Close()
	}
}

type cappedConn struct {
	net.Conn
	l    *cappedListener
	once sync.Once
}

func (c *cappedConn) Close() error {
	c.once.Do(func() { give(c.l.open) })
	return c.Conn.Close()
}
