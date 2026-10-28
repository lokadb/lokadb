package proxy

import (
	"reflect"
	"strings"
	"sync"
	"time"
)

// The shared cache of the lokadb metadata (block 9, item 5, step 3; tasks/proxy-pooling.md,
// section 3a): loadMeta is some 25 queries, and a client that connects for every request paid
// them on every connection. Off unless the config has a section meta_cache.
//
// The key is the database and the role the session reads under (current_user), not the database
// alone: the metadata is read under the role, and part of it is the role's own (Writable, Direct: its rights on the
// partitions; Denied: it cannot read lokadb.*). A session gets what its own role read, never what
// another role did. A Meta is not changed once loaded (only its loaders write it), so sessions
// of one role share the pointer.
//
// An entry is used only while its stamp (metaStampSQL, one query under the session's role) is
// the database's: enable, move-org, move-back, disable and org-profile change what the stamp
// covers in the transaction that notifies lokadb_meta, so a notification nobody heard (no live
// session, connections of the cache do not listen) cannot leave a stale entry in use; a GRANT or
// REVOKE on a LokaDB table or its partitions changes it too. What the stamp does not cover (DDL
// straight in the cloud, past the proxy; a role granted to another role) is read again within
// metaTTL, as a live session's metadata always was. A role the stamp is refused to (no
// rights on lokadb.*) reads for itself, as without the cache.

// metaStampSQL: in a database whose changes lokadb records (lokadb.meta_changes: a row per change
// of lokadb's tables, by a trigger, and per DDL, GRANT or REVOKE, by an event trigger; BACKLOG
// block 9, item 10), their count and last id, and the direct path's forms (counts): a few rows
// read whatever the number of organizations. Else (the event trigger could not be created:
// not a superuser; enabled by an older version) what the metadata is made of, hashed: the lokadb tables' rows, the partitions of
// the LokaDB tables (foreign or local), the rights on them and their parents (relacl, the
// parents' column rights: Writable, Direct follow a GRANT or REVOKE at once), their triggers,
// the organizations' numbers of uuid ids (lokadb.tenants: count and the last number), the forms
// the direct path learnt or refused (counts). Tables that may be missing are read through
// query_to_xml under a CASE, so that the statement parses in a database without them. The LokaDB
// tables are found by their key as readMeta finds them (keyOf: schema.name outside public;
// second review of the protocol, P4: by relname alone a table outside public was never found,
// and a GRANT, a new partition or a trigger of it left the stamp as it was).
var metaStampSQL = `SELECT CASE WHEN to_regclass('lokadb.tables') IS NULL THEN 'none'
  WHEN CASE WHEN EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'lokadb_meta_ddl' AND evtenabled <> 'D')
      AND coalesce(has_table_privilege(to_regclass('lokadb.meta_tracking'), 'SELECT') AND has_table_privilege(to_regclass('lokadb.meta_changes'), 'SELECT'), false)
    THEN query_to_xml('SELECT ddl_tracked FROM lokadb.meta_tracking', false, false, '')::text LIKE '%<ddl_tracked>true</ddl_tracked>%'
    ELSE false END
  THEN concat_ws('|', 'v', query_to_xml('SELECT count(*), max(id) FROM lokadb.meta_changes', false, false, '')::text,
    CASE WHEN to_regclass('lokadb.direct_cloud_first') IS NULL THEN '' ELSE query_to_xml('SELECT count(*), max(at) FROM lokadb.direct_cloud_first', false, false, '')::text END,
    CASE WHEN to_regclass('lokadb.direct_learnt') IS NULL THEN '' ELSE query_to_xml('SELECT count(*) FROM lokadb.direct_learnt', false, false, '')::text END)
  ELSE md5(concat_ws('|',
  query_to_xml('SELECT * FROM lokadb.tables ORDER BY 1', false, false, '')::text,
  CASE WHEN to_regclass('lokadb.lookups') IS NULL THEN '' ELSE query_to_xml('SELECT * FROM lokadb.lookups ORDER BY 1, 2', false, false, '')::text END,
  CASE WHEN to_regclass('lokadb.orgs') IS NULL THEN '' ELSE query_to_xml('SELECT * FROM lokadb.orgs ORDER BY 1', false, false, '')::text END,
  CASE WHEN to_regclass('lokadb.tenants') IS NULL THEN '' ELSE query_to_xml('SELECT count(*), max(org) FROM lokadb.tenants', false, false, '')::text END,
  query_to_xml($q$SELECT p.oid, c.oid, c.relkind, pg_get_expr(c.relpartbound, c.oid) FROM pg_inherits i
    JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
    JOIN lokadb.tables t ON t.table_name = ` + keyOf("p") + ` ORDER BY 1, 2$q$, false, false, '')::text,
  query_to_xml($q$SELECT c.oid, c.relacl FROM pg_class c JOIN lokadb.tables t ON t.table_name = ` + keyOf("c") + `
    UNION ALL SELECT c.oid, c.relacl FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
    JOIN pg_class p ON p.oid = i.inhparent JOIN lokadb.tables t ON t.table_name = ` + keyOf("p") + ` ORDER BY 1$q$, false, false, '')::text,
  query_to_xml($q$SELECT a.attrelid, a.attnum, a.attacl FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
    JOIN lokadb.tables t ON t.table_name = ` + keyOf("c") + ` WHERE a.attacl IS NOT NULL ORDER BY 1, 2$q$, false, false, '')::text,
  query_to_xml($q$SELECT g.tgrelid, g.tgname, g.tgtype, g.tgenabled FROM pg_trigger g
    JOIN pg_class p ON p.oid = g.tgrelid JOIN lokadb.tables t ON t.table_name = ` + keyOf("p") + `
    WHERE NOT g.tgisinternal ORDER BY 1, 2$q$, false, false, '')::text,
  CASE WHEN to_regclass('lokadb.direct_cloud_first') IS NULL THEN '' ELSE query_to_xml('SELECT count(*), max(at) FROM lokadb.direct_cloud_first', false, false, '')::text END,
  CASE WHEN to_regclass('lokadb.direct_learnt') IS NULL THEN '' ELSE query_to_xml('SELECT count(*) FROM lokadb.direct_learnt', false, false, '')::text END
)) END, current_user::text`

// metaCache: entries by database and role.
type metaCache struct {
	mu sync.Mutex
	e  map[string]*metaEntry
	// verifyFor (meta_cache.verify) and alias: the role a login was last read under, so that a
	// connection with no options can find its entry without asking who it is
	verifyFor time.Duration
	alias     map[string]string
}

type metaEntry struct {
	meta     *Meta
	loaded   bool // meta is read (it may be nil: a database that is not enabled)
	stamp    string
	ver      int64         // the database's schemaVersion when read (DDL the proxy replayed)
	at       time.Time     // when it was read
	verified time.Time     // when a session last found its stamp the database's (or read it)
	loading  chan struct{} // while one session reads it: the others wait for that read, or use it as it is
	stamping chan struct{} // while one session asks the stamp for the sessions due a look (sharedStamp)
}

// metaWait: how long a session waits for another session's read or stamp of its key before it
// asks for itself (second review of the protocol, P5: a read that hangs on a cloud connection
// held every connection and reload of the role for good).
var metaWait = 15 * time.Second // a variable: tests shorten it

// stampEvery: how long a session that already holds a role's metadata trusts the shared entry's
// last verification instead of asking the cloud for the stamp again (PF1: the stamp costs
// 20-200 ms of the cloud's work at a thousand organizations, and a thousand sessions asked it
// every metaTTL each). Changes that reach a live session by NOTIFY (enable, a move, org-profile)
// make it ask at once; a new connection always asks.
const stampEvery = 5 * time.Second

// maxMetaEntries: databases × roles kept; past it the oldest entry goes.
const maxMetaEntries = 1000

func newMetaCache() *metaCache {
	return &metaCache{e: map[string]*metaEntry{}, alias: map[string]string{}}
}

// recent: the entry of the login's role when a session verified it within verifyFor (the
// stamp need not be asked: meta_cache.verify), with the role.
func (c *metaCache) recent(db, login string, ver int64) (*Meta, string, bool) {
	if c.verifyFor <= 0 {
		return nil, "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	role, ok := c.alias[db+"\x00"+login]
	if !ok {
		return nil, "", false
	}
	e := c.e[db+"\x00"+role]
	if e == nil || !e.loaded || e.stamp == "" || e.ver != ver || time.Since(e.verified) > c.verifyFor || time.Since(e.at) >= metaTTL {
		return nil, "", false
	}
	return e.meta, role, true
}

// recheck: the metadata of db for role (the role the session reads under: current_user, as
// load returned it) without asking the cloud anything, for a session that holds them already
// and is due a look: the entry, when a session has verified its stamp within stampEvery, no DDL
// was replayed since, and it is younger than metaTTL, or an older one is being read at this
// moment (a reload every metaTTL is one session's, the others go on with what they have).
func (c *metaCache) recheck(db, role string, ver int64) (m *Meta, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.e[db+"\x00"+role]
	if e == nil || !e.loaded || e.stamp == "" || e.ver != ver || time.Since(e.verified) > stampEvery {
		return nil, false
	}
	if age := time.Since(e.at); age < metaTTL || e.loading != nil && age < 2*metaTTL {
		return e.meta, true
	}
	return nil, false
}

// load: the metadata of db for the session's login role, read with q, and the role it was read
// under (current_user): the entry when its stamp is the database's, it is younger than metaTTL
// and no DDL was replayed since; else read (one session for the key; the others wait, or go on
// with the entry if its stamp is still the database's) and kept. A stamp that cannot be read:
// loadMeta, nothing kept, the login's name for the role.
func (c *metaCache) load(db, login string, ver int64, q queryFunc) (*Meta, string, error) {
	rows, err := q(metaStampSQL)
	if err != nil || len(rows) != 1 || len(rows[0]) != 2 {
		m, err := loadMeta(q)
		return m, login, err
	}
	// The key's role is the one the metadata is read under (current_user, read with the stamp),
	// not the login's: with options=-c role=x the login role's key would hold x's metadata for
	// the sessions that log in as it plainly (block 9, review PR-15).
	stamp, role := rows[0][0], rows[0][1]
	key := db + "\x00" + role
	c.mu.Lock()
	c.alias[db+"\x00"+login] = role
	c.mu.Unlock()
	for {
		c.mu.Lock()
		e := c.e[key]
		if e != nil && e.loading != nil {
			// a reload is on: a session whose stamp is the entry's has nothing to wait for (the
			// entry is only old, not changed); one whose database changed waits for the new read
			if e.loaded && e.stamp == stamp && e.ver == ver && time.Since(e.at) < 2*metaTTL {
				e.verified = time.Now()
				m := e.meta
				c.mu.Unlock()
				return m, role, nil
			}
			ch := e.loading
			c.mu.Unlock()
			select {
			case <-ch:
				continue
			case <-time.After(metaWait): // the read is stuck: this session reads for itself, keeps nothing
				m, err := loadMeta(q)
				return m, role, err
			}
		}
		if e != nil && e.loaded && e.stamp == stamp && e.ver == ver && time.Since(e.at) < metaTTL {
			e.verified = time.Now()
			m := e.meta
			c.mu.Unlock()
			return m, role, nil
		}
		if e == nil {
			if len(c.e) >= maxMetaEntries {
				c.dropOldest()
			}
			e = &metaEntry{}
			c.e[key] = e
		}
		ch := make(chan struct{})
		e.loading = ch
		c.mu.Unlock()
		m, err := c.fill(e, ch, stamp, ver, q)
		return m, role, err
	}
}

// fill reads the metadata into e, which the caller marked loading with ch. Whatever the read
// does (an error; a panic, which the session or survive recovers), e is no longer loading and
// ch is closed when it ends (P5: the others waited on ch for good).
func (c *metaCache) fill(e *metaEntry, ch chan struct{}, stamp string, ver int64, q queryFunc) (m *Meta, err error) {
	kept, same := false, false
	defer func() {
		c.mu.Lock()
		if kept {
			if same {
				m = e.meta // read again and the same: the sessions keep the pointer, and their analyses with it
			}
			now := time.Now()
			e.meta, e.loaded, e.stamp, e.ver, e.at, e.verified = m, true, stamp, ver, now, now
		} else {
			e.stamp = "" // not kept: the next session reads again
		}
		e.loading = nil
		close(ch)
		c.mu.Unlock()
	}()
	m, err = loadMeta(q)
	kept = err == nil && (m == nil || !m.Retry)
	if kept {
		// compared outside mu (third review of performance, perf3-15: 4-13 ms at a thousand
		// organizations, every role's connections waiting): e is this read's alone while it is
		// loading, only its loader writes it
		c.mu.Lock()
		old, oldOK := e.meta, e.loaded && e.stamp == stamp
		c.mu.Unlock()
		same = oldOK && reflect.DeepEqual(old, m)
	}
	return m, err
}

// sharedStamp: the metadata of db for role, for a session that holds them and is due a look
// (recheck found no verification within stampEvery), asking the cloud for the stamp once for
// every such session of the key (second review of performance, F8: sessions that connected
// together fall due together every metaTTL, and each asked the stamp, 175 ms of the cloud's
// work at 3000 organizations, 100 of them at once). One asks; the others wait for its answer (at
// most metaWait) and take the entry it verified. The stamp the same and the entry metaTTL old:
// the asker reads it again and the others go on with it meanwhile, as in load. ok false: the
// stamp changed, could not be read, or nothing to share: the session loads as before. err: the
// asker's read failed.
func (c *metaCache) sharedStamp(db, role string, ver int64, q queryFunc) (m *Meta, ok bool, err error) {
	key := db + "\x00" + role
	c.mu.Lock()
	e := c.e[key]
	if e == nil || !e.loaded || e.stamp == "" || e.ver != ver {
		c.mu.Unlock()
		return nil, false, nil
	}
	if ch := e.stamping; ch != nil {
		c.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(metaWait):
			return nil, false, nil
		}
		m, ok := c.recheck(db, role, ver)
		return m, ok, nil
	}
	ch := make(chan struct{})
	e.stamping = ch
	old := e.stamp
	c.mu.Unlock()
	release := func() { // under mu
		if e.stamping == ch {
			e.stamping = nil
			close(ch)
		}
	}
	defer func() { // a panic in the stamp too
		c.mu.Lock()
		release()
		c.mu.Unlock()
	}()
	rows, qerr := q(metaStampSQL)
	if qerr != nil || len(rows) != 1 || len(rows[0]) != 2 || rows[0][0] != old || rows[0][1] != role {
		return nil, false, nil
	}
	c.mu.Lock()
	if c.e[key] != e || e.stamp != old || e.ver != ver { // flushed or read again meanwhile
		c.mu.Unlock()
		return nil, false, nil
	}
	e.verified = time.Now()
	if time.Since(e.at) < metaTTL || e.loading != nil {
		m = e.meta
		c.mu.Unlock()
		return m, true, nil
	}
	lch := make(chan struct{})
	e.loading = lch
	release() // the waiters take the entry being read, as recheck gives it
	c.mu.Unlock()
	m, err = c.fill(e, lch, old, ver, q)
	return m, true, err
}

// flush forgets every entry (the next session of each role reads its metadata again); a read in
// progress ends into the entry it holds, which is no longer in the map.
func (c *metaCache) flush() {
	c.mu.Lock()
	c.e = map[string]*metaEntry{}
	c.mu.Unlock()
}

func (c *metaCache) dropOldest() { // under mu
	var oldest string
	var at time.Time
	for k, e := range c.e {
		if e.loading == nil && (oldest == "" || e.at.Before(at)) {
			oldest, at = k, e.at
		}
	}
	if oldest != "" {
		delete(c.e, oldest)
	}
}

// loadMetaFor: the session's metadata read for its role, through the cache when it is on (the
// cache is on unless the config says "meta_cache": {"off": true}); the role it was read under.
func (srv *Server) loadMetaFor(db, login string, q queryFunc) (*Meta, string, error) {
	if srv.metaCache == nil {
		m, err := loadMeta(q)
		return m, login, err
	}
	return srv.metaCache.load(db, login, srv.schemaVersion(db).Load(), q)
}

// namesRole: the startup parameters may set another role than the login: options, or role in
// any case (a setting's name is case-insensitive: ROLE=x sets the role; second review of the
// protocol, P11).
func namesRole(params map[string]string) bool {
	for k, v := range params {
		if v != "" && (strings.EqualFold(k, "role") || strings.EqualFold(k, "options")) {
			return true
		}
	}
	return false
}

// loadMetaAtConnect: loadMetaFor for a new connection of a client with the startup parameters
// params: with meta_cache.verify, the entry a connection verified a moment ago, without a
// question to the cloud, for a client that names no options (a role set by options is another
// role than the login, which only the stamp tells).
func (srv *Server) loadMetaAtConnect(db, login string, params map[string]string, q queryFunc) (*Meta, string, error) {
	if mc := srv.metaCache; mc != nil && mc.verifyFor > 0 && !namesRole(params) {
		if m, role, ok := mc.recent(db, login, srv.schemaVersion(db).Load()); ok {
			return m, role, nil
		}
	}
	return srv.loadMetaFor(db, login, q)
}
