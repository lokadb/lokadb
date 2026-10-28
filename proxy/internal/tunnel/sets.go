package tunnel

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/lokadb/lokadb/proxy/internal/buildinfo"
)

// Several sessions of one agent to one relay (BACKLOG block 8, item 12; review 3 of the
// performance, P3-18): all of an organization's connections in one TCP connection wait for
// each other on a lost packet. An agent with -sessions N keeps N sessions to every relay, a
// set: they share a set id, each has its slot, both sent in the TLS handshake (ALPN
// "lokadb-s/<set>/<slot>/<version>"). The relay keeps the sessions of the agent's newest set: a session
// of a new set (a renewal rolls every session over, a restart starts a new set) retires the
// whole earlier set, as one session retired the one it replaced before (R3-9); a session of
// the same set and slot replaces that slot. An agent without ALPN is a set of its own.

const (
	alpnSet           = "lokadb-s/"
	maxSessionsPerSet = 8
)

func newSetID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// setProto: the ALPN name of a session: the set, the slot and the agent's version ("lokadb-s/<set>/<slot>/<version>").
// A relay from before the version took "<slot>/<version>" for the slot: the key stays unique
// per session, and the agent's restart (the only time its version changes) starts a new set anyway.
func setProto(set string, slot int, version string) string {
	p := alpnSet + set + "/" + strconv.Itoa(slot)
	if v := buildinfo.Clean(version); v != "" {
		p += "/" + v
	}
	return p
}

// parseSet: the set, slot and agent version of a negotiated ALPN protocol ("" for none).
func parseSet(proto string) (set, slot, version string) {
	rest, ok := strings.CutPrefix(proto, alpnSet)
	if !ok {
		return "", "", ""
	}
	set, rest, _ = strings.Cut(rest, "/")
	slot, version, _ = strings.Cut(rest, "/")
	return set, slot, version
}

// key: where a session is in an organization's map: its agent, set and slot.
func (as *agentSession) key() string {
	if as.set == "" {
		return as.id
	}
	return as.id + "/" + as.set + "/" + as.slot
}

// placeSession puts as among an organization's sessions (the caller holds r.mu) and returns
// the sessions it replaces, or why it is refused.
func placeSession(agents map[string]*agentSession, as *agentSession, maxAgents int) (old []*agentSession, why string) {
	ids, inSet := map[string]bool{}, 0
	for _, o := range agents {
		ids[o.id] = true
		if o.id == as.id && o.set == as.set && o.key() != as.key() {
			inSet++
		}
	}
	if !ids[as.id] && len(ids) >= maxAgents {
		return nil, "the organization has max_agents_per_org agents connected"
	}
	if as.set != "" && inSet >= maxSessionsPerSet {
		return nil, "the agent has its maximum of sessions to this relay"
	}
	for k, o := range agents {
		if o.id == as.id && (o.set != as.set || k == as.key()) {
			old = append(old, o)
			delete(agents, k)
		}
	}
	agents[as.key()] = as
	return old, ""
}
