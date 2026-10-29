package proxy

import (
	"sort"
	"sync"
	"time"
)

// SeenUser: a role that connected to the proxy, when last and how many times. The setup wizard
// uses it to tell whether the application really goes through LokaDB.
type SeenUser struct {
	User, DB string
	Last     time.Time
	Total    int64
}

type roleSeen struct {
	mu   sync.Mutex
	m    map[string]*SeenUser
	pids map[uint32]time.Time // backends of the proxy's own connections to the database
}

func (s *roleSeen) note(user, db string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = map[string]*SeenUser{}
	}
	k := user + "\x00" + db
	u := s.m[k]
	if u == nil {
		if len(s.m) >= 256 { // a flood of made-up names must not grow it without end
			return
		}
		u = &SeenUser{User: user, DB: db}
		s.m[k] = u
	}
	u.Last, u.Total = time.Now(), u.Total+1
}

// Seen: the roles that connected to this proxy, most recent first.
func (s *Server) Seen() []SeenUser {
	s.seen.mu.Lock()
	defer s.seen.mu.Unlock()
	out := make([]SeenUser, 0, len(s.seen.m))
	for _, u := range s.seen.m {
		out = append(out, *u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Last.After(out[j].Last) })
	return out
}

func (s *roleSeen) notePID(pid uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pids == nil {
		s.pids = map[uint32]time.Time{}
	}
	now := time.Now()
	if len(s.pids) >= 8192 { // forget the oldest day: a backend's number is reused only after a long time
		for p, t := range s.pids {
			if now.Sub(t) > 24*time.Hour {
				delete(s.pids, p)
			}
		}
	}
	s.pids[pid] = now
}

// UpstreamPIDs: the backend process numbers of the connections this proxy opened to the database
// (connections that ended may still be in it). A connection of the application's role whose backend
// is not in it did not come through the proxy.
func (s *Server) UpstreamPIDs() []int32 {
	s.seen.mu.Lock()
	defer s.seen.mu.Unlock()
	out := make([]int32, 0, len(s.seen.pids))
	for p := range s.seen.pids {
		out = append(out, int32(p))
	}
	return out
}
