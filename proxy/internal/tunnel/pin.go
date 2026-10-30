package tunnel

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

// The SaaS database the organizations were moved from, pinned by the addresses it connects from
// (R01). A dump of it carries each organization's FDW server and the password of the customer's
// role: restored on a staging server or a laptop, the copy reached the customer's database and
// its DELETE removed the customer's rows. org_allow is a network (container addresses change), and
// a copy in that network passed it. Now, once an address is proven, an organization's port takes
// connections from the proven addresses only.
//
// A proof is a probe: lokadb pin, connected to the SaaS database, makes in a transaction it rolls
// back a postgres_fdw server to the relay with a one-time code from the registry as the role, and
// connects once. The relay reads the code in the startup message, records the address the
// connection came from, and answers with an error. Nothing of it stays in the SaaS database (a
// dump has no code, and a code works once), and the address is the one the database's own
// connections leave from, behind any NAT, in any container or pod. A connection from an address
// not proven is held for a few seconds while lokadb pin (the runner, watching cloud_refused) may
// prove it: a SaaS database that came back at a new address goes on without an error.
//
// LokaDB's own components are admitted connection by connection with a one-time code (self.go),
// never by their address.
//
// Before anything is proven (no lokadb pin: the CLI without a runner) the relay lets org_allow in
// as before and records the addresses it sees ("seen"), to be confirmed (lokadb pin -confirm, or
// the panel): a warning until then, not a silent trust.

// Source: an address the SaaS database connects from.
type Source struct {
	Addr, How string // How: probe, manual, seen
	First     time.Time
	Last      time.Time
	Valid     bool // proven or used in the last hour, or the latest proof (an address the database left lapses)
}

// Refused: connections from an address that is not the SaaS database's.
type Refused struct {
	Addr, Org   string
	First, Last time.Time
	Count       int64
	Pending     bool
}

// pinStore: the registry's side of the pin (PGStore); a store without it pins nothing.
type pinStore interface {
	Sources(ctx context.Context) ([]Source, error)
	SeeSource(ctx context.Context, addr string) error
	TakeProbe(ctx context.Context, nonce string) (bool, error)
	ProveSource(ctx context.Context, addr, how string) error
	Refuse(ctx context.Context, addr, org string, pending bool) error
	SetRelayPorts(ctx context.Context, ports []string) error
	TouchSource(ctx context.Context, addr string) error
}

// pinHold: how long a connection from an address not proven waits for a proof.
var pinHold = 2500 * time.Millisecond

// ProbeDatabase: the database name a probe's startup message carries; its user is the code.
const ProbeDatabase = "lokadb_probe"

// ProbeOK: the error a recorded probe is answered with (lokadb pin looks for it).
const ProbeOK = "lokadb relay: probe recorded"

func (s *PGStore) Sources(ctx context.Context) ([]Source, error) {
	rows, err := s.db.Query(ctx, `SELECT addr, how, first_at, last_at,
		how <> 'seen' AND (last_at >= now() - interval '1 hour'
			OR last_at = (SELECT max(last_at) FROM lokadb_cp.cloud_sources WHERE how <> 'seen'))
		FROM lokadb_cp.cloud_sources ORDER BY last_at DESC`)
	if err != nil {
		return nil, err
	}
	var out []Source
	var x Source
	_, err = pgx.ForEachRow(rows, []any{&x.Addr, &x.How, &x.First, &x.Last, &x.Valid}, func() error { out = append(out, x); return nil })
	return out, err
}

// SeeSource records an address seen while nothing is proven (its first and last time).
func (s *PGStore) SeeSource(ctx context.Context, addr string) error {
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.cloud_sources (addr, how) VALUES ($1, 'seen')
		ON CONFLICT (addr) DO UPDATE SET last_at = now()`, addr)
	return err
}

// ProveSource: addr is the SaaS database's (how: probe, manual); a refusal of it is over.
func (s *PGStore) ProveSource(ctx context.Context, addr, how string) error {
	return pgx.BeginFunc(ctx, s.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO lokadb_cp.cloud_sources (addr, how) VALUES ($1, $2)
			ON CONFLICT (addr) DO UPDATE SET how = excluded.how, last_at = now()`, addr, how); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM lokadb_cp.cloud_refused WHERE addr = $1`, addr)
		return err
	})
}

// TouchSource: a proven address is in use (it stays valid while it is).
func (s *PGStore) TouchSource(ctx context.Context, addr string) error {
	_, err := s.db.Exec(ctx, `UPDATE lokadb_cp.cloud_sources SET last_at = now() WHERE addr = $1 AND how <> 'seen'`, addr)
	return err
}

// touch marks a proven address in use, once every 10 minutes: a proof stays valid while the
// address connects, and lapses an hour after it stopped (unless it is the latest).
func (r *Relay) touch(ps pinStore, ip string) {
	r.mu.Lock()
	if r.touched == nil {
		r.touched = map[string]time.Time{}
	}
	if time.Since(r.touched[ip]) < 10*time.Minute {
		r.mu.Unlock()
		return
	}
	r.touched[ip] = time.Now()
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	defer cancel()
	_ = ps.TouchSource(ctx, ip)
}

// ForgetSource: addr is not the SaaS database's any more (lokadb pin -forget, the panel).
func (s *PGStore) ForgetSource(ctx context.Context, addr string) error {
	_, err := s.db.Exec(ctx, `DELETE FROM lokadb_cp.cloud_sources WHERE addr = $1`, addr)
	return err
}

// NewProbe: a one-time code for a probe, valid for ttl.
func (s *PGStore) NewProbe(ctx context.Context, ttl time.Duration) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	nonce := hex.EncodeToString(b)
	if _, err := s.db.Exec(ctx, `DELETE FROM lokadb_cp.cloud_probes WHERE expires_at < now()`); err != nil {
		return "", err
	}
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.cloud_probes (nonce, expires_at) VALUES ($1, now() + $2::interval)`, nonce, fmt.Sprintf("%d milliseconds", ttl.Milliseconds()))
	return nonce, err
}

// TakeProbe uses a probe's code: true once, while it is valid.
func (s *PGStore) TakeProbe(ctx context.Context, nonce string) (bool, error) {
	tag, err := s.db.Exec(ctx, `DELETE FROM lokadb_cp.cloud_probes WHERE nonce = $1 AND expires_at > now()`, nonce)
	return err == nil && tag.RowsAffected() == 1, err
}

func (s *PGStore) Refuse(ctx context.Context, addr, org string, pending bool) error {
	_, err := s.db.Exec(ctx, `INSERT INTO lokadb_cp.cloud_refused (addr, org, pending) VALUES ($1, $2, $3)
		ON CONFLICT (addr) DO UPDATE SET org = excluded.org, last_at = now(), count = lokadb_cp.cloud_refused.count + 1, pending = excluded.pending`, addr, org, pending)
	return err
}

// RefusedSources: the addresses refused (or held) lately, newest first.
func (s *PGStore) RefusedSources(ctx context.Context, since time.Duration) ([]Refused, error) {
	rows, err := s.db.Query(ctx, `SELECT addr, org, first_at, last_at, count, pending FROM lokadb_cp.cloud_refused
		WHERE last_at > now() - $1::interval ORDER BY last_at DESC`, fmt.Sprintf("%d milliseconds", since.Milliseconds()))
	if err != nil {
		return nil, err
	}
	var out []Refused
	var x Refused
	_, err = pgx.ForEachRow(rows, []any{&x.Addr, &x.Org, &x.First, &x.Last, &x.Count, &x.Pending}, func() error { out = append(out, x); return nil })
	return out, err
}

// pinState: what the relay knows of the SaaS database's addresses (reloaded with the endpoints).
type pinState struct {
	known    bool            // the registry answered once
	enforced bool            // an address is proven: only those pass
	allowed  map[string]bool // the proven ones
	seen     map[string]bool // recorded already (nothing proven yet)
}

func (r *Relay) pins() pinStore {
	if r.cfg.OrgPin == "off" {
		return nil
	}
	ps, _ := r.store.(pinStore)
	return ps
}

// loadPins reads the sources; a registry that does not answer keeps the last state.
func (r *Relay) loadPins() {
	ps := r.pins()
	if ps == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	src, err := ps.Sources(ctx)
	cancel()
	if err != nil {
		return
	}
	st := pinState{known: true, allowed: map[string]bool{}, seen: map[string]bool{}}
	for _, s := range src {
		if s.Valid {
			st.allowed[s.Addr], st.enforced = true, true
		}
		if s.How == "seen" {
			st.seen[s.Addr] = true
		}
	}
	r.mu.Lock()
	r.pin = st
	var cut []net.Conn
	if st.enforced { // a connection opened before the address was proven (a copy's, cached by postgres_fdw) ends now
		for c, ip := range r.orgConns {
			if !st.allowed[ip] {
				cut = append(cut, c)
			}
		}
	}
	r.mu.Unlock()
	for _, c := range cut {
		r.log.Warn("organization endpoint: an open connection not from the SaaS database closed", "remote", c.RemoteAddr())
		c.Close()
	}
}

// trackOrgConn keeps c among the open connections to the organizations' ports until the
// returned func is called.
func (r *Relay) trackOrgConn(c net.Conn) func() {
	r.mu.Lock()
	if r.orgConns == nil {
		r.orgConns = map[net.Conn]string{}
	}
	r.orgConns[c] = ipOf(c.RemoteAddr())
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.orgConns, c)
		r.mu.Unlock()
	}
}

func (r *Relay) pinNow() pinState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pin
}

func ipOf(a net.Addr) string {
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return a.String()
	}
	return host
}

// replayConn: a connection whose first message was read already; reads give it again first.
type replayConn struct {
	net.Conn
	first []byte
}

func (c *replayConn) Read(p []byte) (int, error) {
	if len(c.first) > 0 {
		n := copy(p, c.first)
		c.first = c.first[n:]
		return n, nil
	}
	return c.Conn.Read(p)
}

// firstMessage reads the client's first message whole (an SSLRequest, a GSSENCRequest, a cancel
// or a startup message); nil when there is none in 10 s or it is not one.
func firstMessage(c net.Conn) []byte {
	_ = c.SetReadDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	head := make([]byte, 4)
	if _, err := io.ReadFull(c, head); err != nil {
		return nil
	}
	n := binary.BigEndian.Uint32(head)
	if n < 8 || n > 10000 {
		return nil
	}
	rest := make([]byte, n-4)
	if _, err := io.ReadFull(c, rest); err != nil {
		return nil
	}
	return append(head, rest...)
}

// probeCode: the code of a probe's startup message ("" for anything else), and whether it is a
// component's admission (self.go) rather than the SaaS database's probe.
func probeCode(msg []byte) (string, bool) {
	if len(msg) < 8 || binary.BigEndian.Uint32(msg[4:]) != 196608 {
		return "", false
	}
	var sm pgproto3.StartupMessage
	if err := sm.Decode(msg[4:]); err != nil {
		return "", false
	}
	if sm.Parameters["database"] != ProbeDatabase {
		return "", false
	}
	return sm.Parameters["user"], sm.Parameters[AdmitParam] == "1"
}

// admitted: a component's admission (self.go): its code used, a notice back, and the connection's
// next message read as its first. nil: refused (answered and closed).
func (r *Relay) admitted(org string, c net.Conn, code string) net.Conn {
	ps := r.pins()
	ok := false
	if ps != nil {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
		ok, _ = ps.TakeProbe(ctx, code)
		cancel()
	}
	be := pgproto3.NewBackend(c, c)
	if !ok {
		be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000", Message: "lokadb relay: unknown or used admission code"})
		_ = be.Flush()
		c.Close()
		return nil
	}
	be.Send(&pgproto3.NoticeResponse{Severity: "NOTICE", Code: "00000", Message: AdmitOK})
	if err := be.Flush(); err != nil {
		c.Close()
		return nil
	}
	msg := firstMessage(c)
	if msg == nil {
		c.Close()
		return nil
	}
	return &replayConn{Conn: c, first: msg}
}

// answerProbe records the address of a probe whose code is good, and answers it.
func (r *Relay) answerProbe(org string, c net.Conn, code string) {
	ps := r.pins()
	msg := "lokadb relay: unknown or used probe code"
	if ps != nil {
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
		ok, err := ps.TakeProbe(ctx, code)
		if ok {
			err = ps.ProveSource(ctx, ipOf(c.RemoteAddr()), "probe")
		}
		cancel()
		if ok && err == nil {
			msg = ProbeOK
			r.log.Info("the SaaS database's address proven by lokadb pin", "org", org, "addr", ipOf(c.RemoteAddr()))
			r.loadPins()
		} else if err != nil {
			msg = "lokadb relay: the registry did not answer the probe: " + err.Error()
		}
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	be := pgproto3.NewBackend(c, c)
	be.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28000", Message: msg})
	_ = be.Flush()
}

// pinAllows: whether a connection to an organization's port comes from the SaaS database. Nothing
// proven: yes (its address is recorded as seen). Proven: the proven addresses; another one is held
// for pinHold while a proof may come, then refused with what it means.
func (r *Relay) pinAllows(org string, c net.Conn) bool {
	ps := r.pins()
	if ps == nil {
		return true
	}
	ip := ipOf(c.RemoteAddr())
	st := r.pinNow()
	if !st.enforced {
		if st.known && !st.seen[ip] {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
			if err := ps.SeeSource(ctx, ip); err == nil {
				r.log.Warn("organization endpoint: the SaaS database's address is not proven (lokadb pin); trusting the first ones seen", "org", org, "addr", ip)
			}
			cancel()
			r.mu.Lock()
			if r.pin.seen != nil {
				r.pin.seen[ip] = true
			}
			r.mu.Unlock()
		}
		return true
	}
	if st.allowed[ip] {
		r.touch(ps, ip)
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	_ = ps.Refuse(ctx, ip, org, true)
	cancel()
	for deadline := time.Now().Add(pinHold); time.Now().Before(deadline); {
		time.Sleep(250 * time.Millisecond)
		r.loadPins()
		if r.pinNow().allowed[ip] {
			r.log.Info("organization endpoint: a new address of the SaaS database proven while held", "org", org, "addr", ip)
			return true
		}
	}
	ctx, cancel = context.WithTimeout(context.Background(), r.cfg.registryTimeout)
	_ = ps.Refuse(ctx, ip, org, false)
	cancel()
	r.log.Warn("organization endpoint: connection refused, not from the SaaS database (a copy of it?)", "org", org, "remote", ip)
	refuse(c, fmt.Sprintf("lokadb relay: connection from %s refused: it is not the SaaS database organization %s was moved from. "+
		"A copy of that database (a restored dump) must not reach the customer's database: run lokadb defuse on the copy. "+
		"If this is your SaaS database at a new address, run lokadb pin from it (the runner does by itself) or confirm the address in LokaDB's panel.", ip, org))
	return false
}
