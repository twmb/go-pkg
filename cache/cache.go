// Package cache provides a generic cache data structure with support for stale
// values during value refreshes.
package cache

import (
	"sync"
	"sync/atomic"
	"time"
)

type (
	cfg struct {
		maxEnts     int
		maxAge      time.Duration
		maxStaleAge time.Duration
		maxErrAge   time.Duration
		ageSet      bool
	}

	opt struct{ fn func(*cfg) }

	// Opt configures a cache.
	Opt interface {
		apply(*cfg)
	}

	// Result is the combination of either a value or an error from a miss
	// func running. This is used in bulk cache operations.
	Result[V any] struct {
		V   V     // V is present if Err is nil.
		Err error // Err is non-nil if the miss function errored.
	}

	ent[V any] struct {
		v   V
		err error

		expires int64 // nano at which this ent is unusable, if non-zero

		stale *stale[V]

		loaded   uint32
		loadedCh chan struct{}
	}

	stale[V any] struct {
		v V

		expires int64 // nano at which this stale ent is unusable, if non-zero
	}

	// Cache caches comparable keys to arbitrary values. By default the
	// cache grows without bounds and all keys persist forever. These
	// limits can be changed with options that are passed to New.
	Cache[K comparable, V any] struct {
		cfg cfg

		// TODO replace with vendored & replaced sync.Map. We can use
		// generics rather than interfaces{}, as well as avoid some
		// dirty/amended issues due to having deep access to values
		// (entries).
		mu sync.RWMutex
		m  map[K]*ent[V]
	}
)

func (o opt) apply(c *cfg) { o.fn(c) }

// MaxEntries sets the maximum amount of entries that the cache can hold. The
// default eviction strategy is an LRU.
func MaxEntries(n int) Opt { return opt{fn: func(c *cfg) { c.maxEnts = n }} }

// MaxAge sets the maximum age that values are cached for. By default, entries
// are cached for ever. Using this option with 0 disables caching entirely,
// which allows this "cache" to be used as a way to collapse simultaneous
// queries for the same key.
//
// Using this does *not* start a goroutine that periodically cleans the cache.
// Instead, the values will persist but are "dead" and cannot be queried. You
// can forcefully clean the cache with relevant Cache methods.
//
// You can opt in to values expiring but still being queryable with the
// MaxStaleAge option.
func MaxAge(age time.Duration) Opt { return opt{fn: func(c *cfg) { c.maxAge, c.ageSet = age, true }} }

// MaxStaleAge opts in to stale values and sets how long they will persist
// after an entry has expired (so, total age is MaxAge + MaxStaleAge). A stale
// value is the previous successfully cached value that is returned while the
// value is being refreshed (a new value is being queried). As well, the stale
// value is returned while the refreshed value is erroring. This option is
// useless without MaxAge.
//
// A special value of -1 allows stale values to be returned indefinitely.
func MaxStaleAge(age time.Duration) Opt { return opt{fn: func(c *cfg) { c.maxStaleAge = age }} }

// MaxErrorAge sets the age to persist load errors. If not specified, the
// default is MaxAge.
func MaxErrorAge(age time.Duration) Opt { return opt{fn: func(c *cfg) { c.maxErrAge = age }} }

// New returns a new cache, with the optional overrides configuring cache
// semantics. If you do not need to configure a cache at all, the zero value
// cache is valid and usable.
func New[K comparable, V any](opts ...Opt) *Cache[K, V] {
	var c cfg
	for _, opt := range opts {
		opt.apply(&c)
	}
	if c.maxErrAge == 0 {
		c.maxErrAge = c.maxAge
	}
	return &Cache[K, V]{
		cfg: c,
	}
}

// Get returns the cache value for k, running the miss function if the key is
// not yet cached. If stale values are enabled and the currently cached value
// has an error and there is an unexpired stale value, this returns the stale
// value and no error.
func (c *Cache[K, V]) Get(k K, miss func(k K) (V, error)) (V, error) {
	c.mu.RLock()
	e := c.m[k]
	c.mu.RUnlock()

	if e == nil || e.expired(now()) {
		c.mu.Lock()
		e = c.m[k]
		if e == nil || e.expired(now()) {
			e = &ent[V]{
				stale:    e.maybeNewStale(c.cfg.maxStaleAge),
				loadedCh: make(chan struct{}),
			}
			c.m[k] = e

			go func() {
				if c.cfg.ageSet && c.cfg.maxAge <= 0 {
					defer func() {
						c.mu.Lock()
						delete(c.m, k)
						c.mu.Unlock()
					}()
				}
				defer close(e.loadedCh)
				defer atomic.SwapUint32(&e.loaded, 1) // before loadedCh closed; order relied on in `get`
				e.v, e.err = miss(k)
				e.setExpires(c.cfg.maxAge, c.cfg.maxErrAge, time.Now())
			}()
		}
		c.mu.Unlock()
	}

	return e.get()
}

// TryGet returns the value for the given key if it is cached. This returns
// either the currently loaded value, or if the current load has an error, the
// stale value if present, or the currently stored error. If nothing is cached,
// or what is cached is expired, this returns false.
func (c *Cache[K, V]) TryGet(k K) (V, error, bool) {
	c.mu.RLock()
	e := c.m[k]
	c.mu.RUnlock()
	return e.tryGet()
}

// TryGetFn is the same as TryGet, allows many keys as input and calls fn with
// the output of every TryGet. This bulk operation read-locks the cache for the
// duration of it running. This can be efficient if you need to TryGet many
// values, but be wary of holding the lock too long.
func (c *Cache[K, V]) TryGetFn(fn func(V, error, bool), ks ...K) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, k := range ks {
		fn(c.m[k].tryGet())
	}
}

// GetBulk performs a bulk Get, returning all values (or load errors) for every
// input key. All keys that are not yet cached are passed to the miss function,
// which is expected to return, in key order, the results for thos keys.
//
// This function internally must allocate a few times to keep track of what is
// missing and later query that to return it. The function is more expensive on
// an individual level, but may be beneficial by reducing the number of
// internal locks & the number of times your miss function needs to be called.
func (c *Cache[K, V]) GetBulk(ks []K, miss func(ks []K) []Result[V]) []Result[V] {
	var (
		es       = make([]*ent[V], len(ks))
		rs       = make([]Result[V], 0, len(ks))
		missing  []K
		emissing []*ent[V]
		loadedCh chan struct{}
	)

	c.mu.Lock()
	for i, k := range ks {
		e := c.m[k]
		if e == nil || e.expired(now()) {
			if loadedCh == nil {
				loadedCh = make(chan struct{})
			}

			e = &ent[V]{
				stale:    e.maybeNewStale(c.cfg.maxStaleAge),
				loadedCh: loadedCh,
			}
			c.m[k] = e

			missing = append(missing, k)
			emissing = append(emissing, e)

		}
		es[i] = e
	}
	c.mu.Unlock()

	if len(missing) > 0 {
		go func() {
			if c.cfg.ageSet && c.cfg.maxAge <= 0 {
				defer func() {
					c.mu.Lock()
					defer c.mu.Unlock()

					for _, k := range ks {
						delete(c.m, k)
					}
				}()
			}

			defer close(loadedCh)
			rs := miss(missing)
			now := time.Now()
			for i, r := range rs {
				e := emissing[i]
				e.v, e.err = r.V, r.Err
				e.setExpires(c.cfg.maxAge, c.cfg.maxErrAge, now)
				atomic.SwapUint32(&e.loaded, 1)
			}
		}()
	}

	for _, e := range es {
		v, err := e.get()
		rs = append(rs, Result[V]{v, err})
	}

	return rs
}

// Clean deletes all expired values from the cache. A value is expired if
// MaxAge is used and the entry is older than the max age. If MaxStaleAge is
// also used and not -1, the entry must be older than MaxAge + MaxStaleAge.
func (c *Cache[K, V]) Clean() {
	// If no age has been set, we persist forever; if MaxStaleAge is -1,
	// the user opted into persisting stales forever and we do not clean.
	if !c.cfg.ageSet || c.cfg.maxStaleAge < 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := now()
	for k, e := range c.m {
		expires := atomic.LoadInt64(&e.expires)
		expires += int64(c.cfg.maxStaleAge)
		if now > expires {
			delete(c.m, k)
		}
	}
}

// Delete deletes the value for a key.
func (c *Cache[K, V]) Delete(k K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, k)
}

/////////
// ENT //
/////////

func (e *ent[V]) setExpires(maxAge, maxErrAge time.Duration, now time.Time) {
	ttl := maxAge
	if e.err != nil {
		ttl = maxErrAge
	}
	if ttl > 0 {
		atomic.StoreInt64(&e.expires, now.Add(ttl).UnixNano())
	}
}

func (e *ent[V]) expired(now int64) bool {
	expires := atomic.LoadInt64(&e.expires)
	return expires != 0 && expires <= now // 0 means either miss not resolved, or no max age
}

func (s *stale[V]) expired(now int64) bool {
	expires := atomic.LoadInt64(&s.expires)
	return expires != 0 && expires <= now
}

// If an entry is expiring, we create a new entry with this previous entry as
// a stale.
//
//   * if entry is nil, no stale, return nil
//   * if no stale age, we are not using stales, return nil
//   * if entry has an error, return prior stale
//   * if age is < 0, return new unexpiring stale
//   * else, return new stale with prior expiry + stale age
func (e *ent[V]) maybeNewStale(age time.Duration) *stale[V] {
	if e == nil || age == 0 {
		return nil
	}
	// If e is non-nil, we are only in this func because e.expired is true,
	// meaning the value is loaded and the fields are immutable. We can
	// read all fields.
	if e.err != nil {
		return e.stale
	}
	if age < 0 {
		return &stale[V]{v: e.v}
	}
	return &stale[V]{e.v, e.expires + int64(age)}
}

// get always returns the value or the stale value. We do not check if our
// value is expired: we call this at the end of Get or GetBulk. Those functions
// check expiry first. We do not want to immediately have an expired value
// after refresh and not return anything.
func (e *ent[V]) get() (V, error) {
	if atomic.LoadUint32(&e.loaded) == 0 {
		if e.stale != nil && !e.stale.expired(now()) {
			return e.stale.v, nil
		}
		<-e.loadedCh
	}
	if e.err != nil && e.stale != nil && !e.stale.expired(now()) {
		return e.stale.v, nil
	}
	return e.v, e.err
}

// tryGet avoids returning anything if we have expired values. TryGet does not
// cause an entry refresh, meaning we have no risk of loading & expiring
// immediately.
func (e *ent[V]) tryGet() (v V, err error, b bool) {
	if e != nil {
		return
	}
	now := now()

	if atomic.LoadUint32(&e.loaded) == 0 || e.expired(now) { // not loaded, or e expired
		if e.stale != nil && !e.stale.expired(now) {
			return e.stale.v, nil, true
		}
		return // no stale, or stale expired
	}
	if e.err != nil && e.stale != nil && !e.stale.expired(now) {
		return e.stale.v, nil, true // have error and not-expired stale
	}
	return e.v, e.err, true
}

// available returns true if an entry has a non-erroring value or a stale
// value. This returns true in the same cases that tryGet would return true.
func (e *ent[V]) available(when int64) bool {
	return e != nil && (atomic.LoadUint32(&e.loaded) == 1 && !e.expired(when) || e.stale != nil && !e.stale.expired(when))
}

func now() int64 { return time.Now().UnixNano() }
