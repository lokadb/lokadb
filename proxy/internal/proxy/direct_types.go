package proxy

import (
	"fmt"
	"sync"
	"time"
)

// The parameter and column types of the statements the direct path decides on, kept for the
// whole process (third review of performance, perf3-14): a client that Parses its statements
// unnamed with literals (JDBC before prepareThreshold, pgx in exec mode, a PHP connection per
// request) made the cloud Describe each new text in each session, one more exchange with the
// cloud and a parse there for every statement. The types depend on the text, the parameter
// types the client gave, the search path and the schema: the key has the first three, the
// database's schema version (DDL seen through the proxy) and an age (DDL made elsewhere)
// the last.

// typesTTL: how long the types of a text serve without being asked again.
const typesTTL = 10 * time.Minute

// maxTypes: the texts the cache keeps; past it, it starts again.
const maxTypes = 10000

type typesEntry struct {
	ptypes, rtypes []uint32 // nil: not known yet
	ver            int64    // the database's schema version they were read in
	at             time.Time
}

// typeCache: the types by database, search path and text (Server.types).
type typeCache struct {
	mu sync.Mutex
	m  map[string]*typesEntry
}

// typesKey: the cache's key of sql with the parameter types oids in the session's database
// and search path.
func (s *session) typesKey(sql string, oids []uint32) string {
	return s.db + "\x00" + pathKey(s.searchPathOrDefault()) + "\x00" + fmt.Sprint(oids) + "\x00" + sql
}

// cachedTypes: the types of sql (oids: the client's parameter types) another session read,
// in the schema as it is now; nil for one not known.
func (s *session) cachedTypes(sql string, oids []uint32) (ptypes, rtypes []uint32) {
	c := &s.srv.types
	ver := s.srv.schemaVersion(s.db).Load()
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.m[s.typesKey(sql, oids)]
	if e == nil || e.ver != ver || time.Since(e.at) > typesTTL {
		return nil, nil
	}
	return e.ptypes, e.rtypes
}

// storeTypes: the types of sql the session read (nil: not read, the known ones stay).
func (s *session) storeTypes(sql string, oids, ptypes, rtypes []uint32) {
	c := &s.srv.types
	ver := s.srv.schemaVersion(s.db).Load()
	k := s.typesKey(sql, oids)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil || len(c.m) >= maxTypes {
		c.m = map[string]*typesEntry{}
	}
	e := c.m[k]
	if e == nil || e.ver != ver || time.Since(e.at) > typesTTL {
		e = &typesEntry{ver: ver, at: time.Now()}
		c.m[k] = e
	}
	if ptypes != nil {
		e.ptypes = ptypes
	}
	if rtypes != nil {
		e.rtypes = rtypes
	}
}

// stmtParamTypes: st's parameter types: the process's cache, or the cloud's answer for name
// (paramTypes), kept for the other sessions.
func (s *session) stmtParamTypes(st *stmtInfo, name string) []uint32 {
	if p, _ := s.cachedTypes(st.sql, st.oids); p != nil {
		return p
	}
	p := s.paramTypes(name)
	if p != nil {
		s.storeTypes(st.sql, st.oids, p, nil)
	}
	return p
}

// stmtResultTypes: st's column types, as stmtParamTypes.
func (s *session) stmtResultTypes(st *stmtInfo, name string) []uint32 {
	if _, r := s.cachedTypes(st.sql, st.oids); r != nil {
		return r
	}
	r := s.resultTypes(name)
	if r != nil {
		s.storeTypes(st.sql, st.oids, nil, r)
	}
	return r
}
