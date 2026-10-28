package proxy

import (
	"sync"
	"time"
)

// The organizations of ids, shared by the sessions of the process (BACKLOG block 9, items 12
// and 13; third review of performance, perf3-10, perf3-16). A session kept its own cache: a cold key
// was a lookup in the cloud for every session that met it, and a client connecting for every
// request never had a warm one; up to 100 000 keys a session, outside any bound.
//
// The key is the database, the role the metadata is read under (what a lookup sees is the
// role's), the refs table and the id. A database's entries go at once when a statement through
// this proxy may change what a lookup answers (orgVersion, as the sessions' caches did), when
// a session hears that the metadata changed (enable, disable: refs tables under the same names
// may be others), and every orgCacheTTL (other proxies, writes past the proxy). The whole cache
// holds at most limit bytes (an estimate per entry) and is emptied when one more does not fit.
//
// A session whose transaction changed what lookups answer (an UPDATE of an organization or
// a key, Analysis.ChangesLookups) uses its own cache until the transaction ends: it sees its
// changes before they are committed, and they must not reach the other sessions (a rollback
// would leave them wrong), nor may their values, read before the change, reach it.

// defaultOrgCacheBytes: the process cache's bound (Config.OrgCacheBytes).
const defaultOrgCacheBytes = 64 << 20

type orgCache struct {
	mu    sync.RWMutex
	dbs   map[string]*orgDB
	bytes int
	limit int
}

type orgDB struct {
	ver int64     // orgVersion(db) the entries are valid for
	at  time.Time // when the database's entries were last emptied
	m   map[string]string
}

func newOrgCache(limit int) *orgCache {
	if limit <= 0 {
		limit = defaultOrgCacheBytes
	}
	return &orgCache{dbs: map[string]*orgDB{}, limit: limit}
}

func orgKey(role, refs, id string) string { return role + "\x00" + refs + "\x00" + id }

// orgEntryBytes: what an entry holds, estimated (the strings and the map's own share).
func orgEntryBytes(key, org string) int { return len(key) + len(org) + 64 }

// get: the organization of id in refs, for role in db; false when not known (or no longer
// valid: the database's entries are emptied here).
func (c *orgCache) get(db, role, refs, id string) (string, bool) {
	ver := orgVersion(db).Load()
	c.mu.RLock()
	d := c.dbs[db]
	if d != nil && d.ver == ver && time.Since(d.at) <= orgCacheTTL {
		o, ok := d.m[orgKey(role, refs, id)]
		c.mu.RUnlock()
		return o, ok
	}
	c.mu.RUnlock()
	if d != nil {
		c.mu.Lock()
		if d := c.dbs[db]; d != nil && (d.ver != ver || time.Since(d.at) > orgCacheTTL) {
			c.dropDB(db)
		}
		c.mu.Unlock()
	}
	return "", false
}

// put keeps the organizations of ids read by one lookup, if the database's entries are still
// those of ver (the version when the lookup began: a change since may have made them old).
func (c *orgCache) put(db, role, refs string, ver int64, rows map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if orgVersion(db).Load() != ver {
		return
	}
	d := c.dbs[db]
	if d == nil || d.ver != ver || time.Since(d.at) > orgCacheTTL {
		c.dropDB(db)
		d = &orgDB{ver: ver, at: time.Now(), m: map[string]string{}}
		c.dbs[db] = d
	}
	for id, org := range rows {
		k := orgKey(role, refs, id)
		n := orgEntryBytes(k, org)
		if c.bytes+n > c.limit {
			c.dbs, c.bytes = map[string]*orgDB{}, 0 // emptied whole, as the session caches are
			d = &orgDB{ver: ver, at: time.Now(), m: map[string]string{}}
			c.dbs[db] = d
		}
		if old, ok := d.m[k]; ok {
			c.bytes -= orgEntryBytes(k, old)
		}
		d.m[k] = org
		c.bytes += n
	}
}

// dropDB forgets db's entries (under mu).
func (c *orgCache) dropDB(db string) {
	if d := c.dbs[db]; d != nil {
		for k, o := range d.m {
			c.bytes -= orgEntryBytes(k, o)
		}
		delete(c.dbs, db)
	}
}

// orgRole: the role a session's lookups are read under (the key of its entries).
func (s *session) orgRole() string {
	if s.metaRole != "" {
		return s.metaRole
	}
	return s.user
}

// orgOf: the organization of id in refs as the session knows it: its own cache while its
// transaction changed lookups (or with no process cache), else the process's.
func (s *session) orgOf(refs, id string) (string, bool) {
	if s.srv == nil || s.srv.orgs == nil || s.ownOrgs {
		o, ok := s.orgCache[refs][id]
		return o, ok
	}
	return s.srv.orgs.get(s.db, s.orgRole(), refs, id)
}

// keepOrgs keeps what one lookup read (ver: orgVersion when it began).
func (s *session) keepOrgs(refs string, ver int64, rows map[string]string) {
	if s.srv == nil || s.srv.orgs == nil || s.ownOrgs {
		if s.orgCache[refs] == nil {
			s.orgCache[refs] = map[string]string{}
		}
		for id, o := range rows {
			s.orgCache[refs][id] = o
		}
		return
	}
	s.srv.orgs.put(s.db, s.orgRole(), refs, ver, rows)
}
