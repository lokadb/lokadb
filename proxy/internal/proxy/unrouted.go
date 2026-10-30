package proxy

import (
	"sort"
	"sync"
	"time"

	pg "github.com/pganalyze/pg_query_go/v6"
)

// Unrouted: a statement shape that names no organization, which the fanout policy refused or let run
// in every organization's database, counted since the proxy started. SQL has its literals replaced
// by $n, so no customer's data is in it. The setup wizard shows these before the first customer
// moves, so each can be given its organization, allowed by name, or turned into a cloud table.
type Unrouted struct {
	Fingerprint string    `json:"fingerprint"`
	SQL         string    `json:"sql"`
	Full        string    `json:"full,omitempty"` // the same, not cut: what an allow-list entry is made of
	Table       string    `json:"table"`
	Gaps        []Gap     `json:"gaps,omitempty"`
	Write       bool      `json:"write,omitempty"`
	Refused     bool      `json:"refused,omitempty"` // the last time: the client got an error
	Count       int64     `json:"count"`
	First, Last time.Time `json:"-"`

	loggedAt time.Time // the last time this shape was written to the log
	quiet    int64     // the times since then it was not
}

const unroutedMax = 500 // shapes kept; more are counted in Overflow

type unroutedSet struct {
	mu       sync.Mutex
	m        map[string]*Unrouted
	overflow int64
	runs     int64     // the statements run without a customer since the start
	summary  time.Time // the last summary line
}

// How often the log tells a statement without a customer: a shape that runs once (it is in the panel
// from then on), a refused shape once a minute with how many times since, and a summary of all every
// ten minutes.
const (
	unroutedRefusedEvery = time.Minute
	unroutedSummaryEvery = 10 * time.Minute
)

// unroutedLog: what the session writes to the log for this statement; zero: nothing.
type unroutedLog struct {
	first   bool  // the first time this shape is seen
	refused bool  // a refused shape whose minute is up
	since   int64 // the times it was not written since the last line
	// a summary: the shapes and the runs since the start
	summary      bool
	shapes, runs int64
	start        time.Time
}

func (u *unroutedSet) note(sql string, an *Analysis, refused bool) (out unroutedLog) {
	fp := an.fp
	if fp == "" {
		fp, _ = pg.Fingerprint(sql)
		if fp == "" {
			fp = "-"
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.m == nil {
		u.m = map[string]*Unrouted{}
	}
	x := u.m[fp]
	if x == nil {
		if len(u.m) >= unroutedMax {
			u.overflow++
			return out
		}
		x = &Unrouted{Fingerprint: fp, SQL: brief(sql), Full: full(sql), Table: fanoutTable(an), Gaps: append([]Gap(nil), an.Gaps...), Write: an.Write, First: time.Now()}
		u.m[fp] = x
		out.first = true
	}
	now := time.Now()
	x.Count++
	x.Refused, x.Last = refused, now
	if !refused {
		u.runs++
	}
	switch {
	case out.first:
		x.loggedAt = now
	case refused && now.Sub(x.loggedAt) >= unroutedRefusedEvery:
		out.refused, out.since = true, x.quiet
		x.loggedAt, x.quiet = now, 0
	default:
		x.quiet++
	}
	if u.summary.IsZero() {
		u.summary = now
	} else if now.Sub(u.summary) >= unroutedSummaryEvery {
		u.summary = now
		out.summary, out.shapes, out.runs = true, int64(len(u.m))+u.overflow, u.runs
		for _, y := range u.m {
			if out.start.IsZero() || y.First.Before(out.start) {
				out.start = y.First
			}
		}
	}
	return out
}

// Unrouted: the statement shapes without an organization seen since the start, most often first,
// and how many shapes did not fit.
func (s *Server) Unrouted() ([]Unrouted, int64) {
	s.unrouted.mu.Lock()
	defer s.unrouted.mu.Unlock()
	out := make([]Unrouted, 0, len(s.unrouted.m))
	for _, x := range s.unrouted.m {
		out = append(out, *x)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Count > out[j].Count || out[i].Count == out[j].Count && out[i].Fingerprint < out[j].Fingerprint
	})
	return out, s.unrouted.overflow
}

// full: sql with its literals replaced, whole (up to 4 KB; longer is not kept for an allow list).
func full(sql string) string {
	s := redact(sql)
	if len(s) > 4096 {
		return ""
	}
	return s
}
