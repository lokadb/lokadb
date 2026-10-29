package proxy

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/lokadb/lokadb/proxy/internal/ownsql"
)

// The direct path's statements of its own in the cloud, prepared once per cloud connection
// (third review of performance, perf3-11): the gate (the pause lock and gateCheck), its release
// (ungate), the transaction's lock, BEGIN and cloudBusy were parsed and planned by the cloud
// for every statement of the client, about as much work there as the client's statement itself
// on the cloud's way, although that statement runs in the organization's database. Their texts
// name no organization (it is a parameter, so are its partitions' names: gateStmt), one
// prepared statement serves every organization of a form.

// cloudStmt: a statement the proxy runs in the cloud for itself (cloudStartStmts). prep: by a
// statement prepared once on the connection (prepCache); params: its parameters (nil: NULL).
type cloudStmt struct {
	sql    string
	params [][]byte
	prep   bool
}

// plain: a statement parsed where it is sent (its text names values: refs rows, keys).
func plain(q string) cloudStmt { return cloudStmt{sql: q} }

// prepared: a statement without values in its text, prepared once on the connection.
func prepared(q string, params ...[]byte) cloudStmt {
	return cloudStmt{sql: q, params: params, prep: true}
}

// maxPrepared: the statements prepCache keeps prepared on one cloud connection; past it they
// are closed, and prepared again as they are used.
const maxPrepared = 256

// prepCache: the texts the session prepared on its cloud connection and their names. ok: the
// server answered the statement's Parse (set by the relay goroutine, onOK). epoch: the
// session's svcEpoch when the names were made: the client's DISCARD ALL or DEALLOCATE ALL
// drops them upstream (forgetOnOK counts it), and they are prepared again.
type prepCache struct {
	mu    sync.Mutex
	epoch uint32
	names map[string]string
	ok    map[string]bool
	n     int
}

// lookup: the name of q's statement; parse: it is to be Parsed (not prepared yet, or its Parse
// not answered: Closed first, so a Parse in flight does not clash); closes: the names to Close
// first (the cache was full).
func (p *prepCache) lookup(q string, epoch uint32) (name string, parse bool, closes []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.names == nil || p.epoch != epoch { // none yet, or dropped upstream already
		p.names, p.ok, p.epoch = map[string]string{}, map[string]bool{}, epoch
	}
	if name, ok := p.names[q]; ok {
		return name, !p.ok[name], nil
	}
	if len(p.names) >= maxPrepared {
		for _, n := range p.names {
			closes = append(closes, n)
		}
		p.names, p.ok = map[string]string{}, map[string]bool{}
	}
	p.n++
	name = fmt.Sprintf("lokadb_q%d", p.n)
	p.names[q] = name
	return name, true, closes
}

// confirm: the server answered name's Parse, in epoch.
func (p *prepCache) confirm(name string, epoch uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.epoch == epoch && p.ok != nil {
		p.ok[name] = true
	}
}

// cloudStartStmts is cloudStart with statements prepared once on the connection where they
// say so, and parameters: the answers are captured in order, as cloudStart's.
func (s *session) cloudStartStmts(items ...cloudStmt) *capture {
	c := &capture{done: make(chan struct{}), untilSync: true}
	var out []outMsg
	add := func(m pgproto3.FrontendMessage) { out = append(out, outMsg{msg: m, mode: mCapture, cap: c}) }
	for i, it := range items {
		portal := fmt.Sprintf("lokadb_wp%d", i)
		if !it.prep {
			st := fmt.Sprintf("lokadb_w%d", i)
			add(&pgproto3.Close{ObjectType: 'S', Name: st})
			add(&pgproto3.Close{ObjectType: 'P', Name: portal})
			add(&pgproto3.Parse{Name: st, Query: ownsql.Tag(it.sql)})
			add(&pgproto3.Bind{DestinationPortal: portal, PreparedStatement: st, Parameters: it.params})
			add(&pgproto3.Execute{Portal: portal})
			continue
		}
		epoch := s.svcEpoch.Load()
		name, parse, closes := s.prep.lookup(it.sql, epoch)
		for _, n := range closes {
			add(&pgproto3.Close{ObjectType: 'S', Name: n})
		}
		add(&pgproto3.Close{ObjectType: 'P', Name: portal})
		if parse {
			add(&pgproto3.Close{ObjectType: 'S', Name: name})
			out = append(out, outMsg{msg: &pgproto3.Parse{Name: name, Query: ownsql.Tag(it.sql)}, mode: mCapture, cap: c,
				onOK: func() { s.prep.confirm(name, epoch) }})
		}
		add(&pgproto3.Bind{DestinationPortal: portal, PreparedStatement: name, Parameters: it.params})
		add(&pgproto3.Execute{Portal: portal})
	}
	out = append(out, outMsg{msg: &pgproto3.Sync{}, mode: mCapture, cap: c})
	if err := s.send(out); err != nil {
		c.fail(err)
	}
	return c
}

// cloudRunStmts: cloudStartStmts, waited for.
func (s *session) cloudRunStmts(items ...cloudStmt) *capture {
	c := s.cloudStartStmts(items...)
	<-c.done
	return c
}

// lockStmt: call (a function of the pause lock: "SELECT FROM pg_advisory_lock_shared" and the
// like) of org's lock key, the key a parameter (lockCall's forms: NULL, an integer, a uuid).
func (s *session) lockStmt(call, org string) cloudStmt {
	switch {
	case org == "NULL":
		return prepared(call + "(lokadb.org_lock(NULL))")
	case !s.meta.valueOrgs():
		return prepared(call+"(lokadb.org_lock($1))", []byte(org))
	}
	return prepared(call+"(lokadb.org_lock_v($1))", []byte(org))
}
