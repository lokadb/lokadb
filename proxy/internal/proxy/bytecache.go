package proxy

import (
	"hash/maphash"
	"sync/atomic"
)

// Caches of a session bounded in bytes (block 9, review PF7). A session kept up to 5000 analyses
// by their text: 4.5 MB of point texts, 78 MB of 100-row INSERTs, per session, with no limit
// across sessions. The limit is the memory the entries hold (an estimate from the text:
// textBytes), and a text is kept from its second sighting (seenSet): an application that inlines
// its literals sends most texts once, and keeping them only moved memory from the garbage
// collector to the cache.

// defaultCacheBytes: what the caches of one session may hold, together (Config.CacheBytes).
const defaultCacheBytes = 8 << 20

// textBytes: what the analysis of sql, and what hangs off it, holds, estimated (the review's
// measure: 120 characters of a point text ≈ 0.9 KB, 4 KB of a 100-row INSERT ≈ 15 KB).
func textBytes(sql string) int { return 4*len(sql) + 768 }

type cacheEntry[V any] struct {
	v    V
	size int
}

// byteCache: a map that holds at most limit bytes of estimated entries and is emptied whole when
// one more does not fit (as the maps it replaces were at 5000 entries).
type byteCache[K comparable, V any] struct {
	m     map[K]cacheEntry[V]
	bytes int
	limit int
	// total, totalLimit: what the caches of every session hold together and may hold
	// (Config.CacheBytesTotal; BACKLOG block 9, item 13: 8 MiB a session was a bound of
	// gigabytes at a thousand clients); nil: no bound beyond limit
	total      *atomic.Int64
	totalLimit int64
}

// defaultCacheBytesTotal: what the caches of every session may hold together.
const defaultCacheBytesTotal = 512 << 20

// shared: c counted in total, which may hold at most limit bytes.
func (c *byteCache[K, V]) shared(total *atomic.Int64, limit int64) *byteCache[K, V] {
	c.total, c.totalLimit = total, limit
	return c
}

func newByteCache[K comparable, V any](limit int) *byteCache[K, V] {
	return &byteCache[K, V]{m: map[K]cacheEntry[V]{}, limit: limit}
}

// The methods take a nil cache for one that keeps nothing (a session made by a test).
func (c *byteCache[K, V]) get(k K) (v V, ok bool) {
	if c == nil {
		return v, false
	}
	e, ok := c.m[k]
	return e.v, ok
}

// max: the most one entry may hold.
func (c *byteCache[K, V]) max() int {
	if c == nil {
		return 0
	}
	return c.limit / 8
}

// put keeps v of the estimated size; false: it is not kept (one entry may hold at most an eighth
// of the limit: a huge VALUES list is rarely sent twice).
func (c *byteCache[K, V]) put(k K, v V, size int) bool {
	if c == nil || size > c.limit/8 {
		return false
	}
	if old, ok := c.m[k]; ok {
		c.bytes -= old.size
		c.count(-old.size)
	} else if c.bytes+size > c.limit {
		c.reset()
	}
	if c.total != nil && c.total.Load()+int64(size) > c.totalLimit {
		c.reset() // the process's bound: this session's own entries go
		if c.total.Load()+int64(size) > c.totalLimit {
			return false
		}
	}
	c.m[k] = cacheEntry[V]{v, size}
	c.bytes += size
	c.count(size)
	return true
}

func (c *byteCache[K, V]) reset() {
	if c == nil {
		return
	}
	c.count(-c.bytes)
	c.m = map[K]cacheEntry[V]{}
	c.bytes = 0
}

func (c *byteCache[K, V]) count(n int) {
	if c.total != nil {
		c.total.Add(int64(n))
	}
}

func (c *byteCache[K, V]) len() int {
	if c == nil {
		return 0
	}
	return len(c.m)
}

var seenSeed = maphash.MakeSeed()

// maxSeen: texts a session remembers having met once.
const maxSeen = 4096

// seenSet remembers the hash of a text met once, so that its second sighting is known. When
// full, an arbitrary one goes (map order), so a text that comes back within a few thousand
// others is still found.
type seenSet struct{ m map[uint64]struct{} }

// again: text was met before (and is forgotten now: the caller keeps it); else it is noted.
func (s *seenSet) again(text string) bool {
	h := maphash.String(seenSeed, text)
	if _, ok := s.m[h]; ok {
		delete(s.m, h)
		return true
	}
	if s.m == nil {
		s.m = map[uint64]struct{}{}
	}
	if len(s.m) >= maxSeen {
		for k := range s.m {
			delete(s.m, k)
			break
		}
	}
	s.m[h] = struct{}{}
	return false
}
