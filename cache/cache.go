// Package cache provides a generic cache data structure with support for stale
// values during value refreshes.
package cache

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	kindUnknown = iota
	kindLoading
	kindLoaded
)

// KeyState is returned from cache operations to indicate how a key existed in
// the map, if at all.
type KeyState uint8

// IsHit returns whether the key state is Hit or Stale, meaning that the value
// is loaded.
func (s KeyState) IsHit() bool { return s != Miss }

// IsMiss returns if the key state is Miss.
func (s KeyState) IsMiss() bool { return s == Miss }

const (
	// Miss indicates that the key was not present in the map.
	Miss KeyState = iota
	// Hit indicates that the key was present in the map and we are using
	// the latest values for the key.
	Hit
	// Stale indicates that the key was present in the map, but either was
	// expired or had an error, and we returned a valid stale value.
	Stale
)

type (
	stale[V any] struct {
		v       V
		expires int64 // nano at which this stale ent is unusable, if non-zero
	}
	lcheck struct {
		kind uint8 // we check the kind with lcheck first
	}
	loaded[V any] struct {
		kind    uint8 // == kindLoaded
		v       V
		err     error
		expires int64     // nano at which this ent is unusable, if non-zero
		stale   *stale[V] // *stale[V]
	}
	loading[V any] struct {
		kind uint8 // == kindLoading
		wg   sync.WaitGroup

		wgmiss sync.WaitGroup
		once   sync.Once
		v      V
		err    error

		stale  *stale[V] // *stale[V]
		loaded *loaded[V]
	}
	ent[V any] struct {
		p unsafe.Pointer // [nil | promotingDelete | *loading | *loaded]
	}
	read[K comparable, V any] struct {
		m          map[K]*ent[V]
		incomplete bool
	}

	// Cache caches comparable keys to arbitrary values. By default the
	// cache grows without bounds and all keys persist forever. These
	// limits can be changed with options that are passed to New.
	Cache[K comparable, V any] struct {
		cfg cfg

		r     unsafe.Pointer // *read
		mu    sync.Mutex
		dirty map[K]*ent[V]

		misses int
	}

	cfg struct {
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
)

func now() int64 { return time.Now().UnixNano() }

func (cfg *cfg) newExpires(err error) int64 {
	ttl := cfg.maxAge
	if err != nil {
		ttl = cfg.maxErrAge
	}
	if ttl > 0 {
		return time.Now().Add(ttl).UnixNano()
	}
	return 0
}

func (o opt) apply(c *cfg) { o.fn(c) }

// MaxAge sets the maximum age that values are cached for. By default, entries
// are cached forever. Using this option with 0 disables caching entirely,
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

// Get returns the cache value for k, running the miss function in a goroutine
// if the key is not yet cached. If stale values are enabled, the currently
// cached value has an error, and there is an unexpired stale value, this
// returns the stale value and no error.
func (c *Cache[K, V]) Get(k K, miss func() (V, error)) (v V, err error, s KeyState) {
	r := c.read()
	e := r.m[k]
	if v, err, s = e.get(); s == Hit {
		return
	}

	// We missed in the read map. We lock and check again to guard against
	// something concurrent. Odds are we are falling into the logic below.
	c.mu.Lock()
	r = c.read()
	e = r.m[k]
	if v, err, s = e.get(); s == Hit {
		c.mu.Unlock()
		return
	}

	// We could have an entry in our read map that was deleted and has not
	// yet gone through the promote&clear process. We only check the dirty
	// map if the entry is nil.
	if e == nil && r.incomplete {
		e = c.dirty[k]
		c.missed(r)
		r = c.read()
		if v, err, s = e.get(); s == Hit {
			c.mu.Unlock()
			return
		}
	}

	loading := &loading[V]{
		kind:  kindLoading,
		stale: e.maybeNewStale(c.cfg.maxStaleAge),
	}
	loading.wg.Add(1)
	loading.wgmiss.Add(1)

	// If we have no entry, this is completely new. If we had an entry, it
	// was in the read map and not yet deleted through promotion. We know
	// the pointer is not promotingDelete, since that is only set while
	// promoting which requires the cache lock which we have right now.
	//
	// In the worst case we race with a concurrent Set and we override its
	// results.
	if e != nil {
		atomic.StorePointer(&e.p, unsafe.Pointer(loading))
	} else {
		e = &ent[V]{p: unsafe.Pointer(loading)}
		c.storeDirty(r, k, e)
	}
	c.mu.Unlock()

	// If our expiry's are so small, it is possible that we could expire
	// before `get` runs below. If this is the case, we still want to
	// return what the actual miss values were. We do some closure magic:
	// capture the miss returns, and also use that for the value we save
	// (which could expire immediately).
	go c.loadEnt(k, e, loading, miss)

	// We could have set our own stale value which can be returned
	// immediately rather than waiting for the get. If there is a valid
	// stale to be returned now, `get` returns it. If `get` returns a Hit,
	// we ended up waiting for ourself and we return a miss. There should
	// be no Miss returns from `get`.
	v, err, s = e.get()
	switch s {
	case Miss, Hit:
		return loading.loaded.v, loading.loaded.err, Miss
	}
	return v, err, Stale
}

func (c *Cache[K, V]) tryLoadEnt(k K, dirty func()) *ent[V] {
	r := c.read()
	e := r.m[k]
	if e == nil && r.incomplete {
		c.mu.Lock()
		r = c.read()
		e = r.m[k]
		if e == nil && r.incomplete {
			e = c.dirty[k]
			if dirty != nil {
				dirty()
			}
			c.missed(r)
		}
		c.mu.Unlock()
	}
	return e
}

// TryGet returns the value for the given key if it is cached. This returns
// either the currently loaded value, or if the current load has an error, the
// stale value if present, or the currently stored error. If nothing is cached,
// or what is cached is expired, this returns false.
func (c *Cache[K, V]) TryGet(k K) (V, error, KeyState) {
	e := c.tryLoadEnt(k, nil)
	return e.tryGet()
}

// Delete deletes the value for a key and returns the prior value, if loaded
// (i.e. the return from TryGet).
func (c *Cache[K, V]) Delete(k K) (V, error, KeyState) {
	e := c.tryLoadEnt(k, func() { delete(c.dirty, k) })
	defer e.del()
	return e.tryGet()
}

// Expire sets a loaded value to expire immediately, meaning the next Get will
// be a miss. If stale values are enabled, the next Get will trigger the miss
// function but still allow the now-stale value to be returned.
func (c *Cache[K, V]) Expire(k K) {
	e := c.tryLoadEnt(k, nil)
	_, loaded := e.load()
	if loaded == nil {
		return
	}
	atomic.SwapInt64(&loaded.expires, now())
}

// Each calls fn for every cached value. If fn returns false, iteration stops.
func (c *Cache[K, V]) Range(fn func(K, V, error) bool) {
	c.each(func(k K, e *ent[V]) bool {
		v, err, s := e.tryGet()
		if s.IsMiss() {
			return true
		}
		return fn(k, v, err)
	})
}

// Similar to sync.Map, we promote on range because range is O(N) (usually) and
// this amortizes out.
func (c *Cache[K, V]) each(fn func(K, *ent[V]) bool) {
	r := c.read()
	if r.incomplete {
		c.mu.Lock()
		r = c.read()
		if r.incomplete {
			c.promote()
			r = c.read()
		}
		c.mu.Unlock()
	}
	for k, e := range r.m {
		if !fn(k, e) {
			return
		}
	}
}

// Clean deletes all expired values from the cache. A value is expired if
// MaxAge is used and the entry is older than the max age, or if you manually
// expired a key. If MaxStaleAge is used and not -1, the entry must be older
// than MaxAge + MaxStaleAge. If MaxStaleAge is -1, Clean returns immediately.
func (c *Cache[K, V]) Clean() {
	// If MaxStaleAge is -1, the user opted into persisting stales forever
	// and we do not clean.
	if c.cfg.maxStaleAge < 0 {
		return
	}
	now := now()
	c.each(func(k K, e *ent[V]) bool {
		_, loaded := e.load()
		if loaded != nil {
			expires := atomic.LoadInt64(&loaded.expires)
			if expires != 0 && now > expires+int64(c.cfg.maxStaleAge) {
				c.Delete(k)
			}
		}
		return true
	})
}

// Set sets a value for a key. If the key is currently loading via Get, the
// load is canceled and Get returns the value from Set.
func (c *Cache[K, V]) Set(k K, v V) {
	loaded := &loaded[V]{
		kind:    kindLoaded,
		v:       v,
		expires: c.cfg.newExpires(nil),
	}
	if c.cfg.maxStaleAge != 0 {
		loaded.stale = newStale(v, now(), c.cfg.maxStaleAge)
	}
	p := unsafe.Pointer(loaded)

	var was unsafe.Pointer

	r := c.read()
	e, ok := r.m[k]

	defer func() {
		if was == nil {
			return
		}
		if loading, _ := pointerToLoad[V](was); loading != nil {
			loading.setve(v, nil)
		}
	}()

	if ok {
		for {
			rm := atomic.LoadPointer(&e.p)
			if rm == promotingDelete { // deleted & currently being ignored in a promote
				break
			}
			was = rm
			if atomic.CompareAndSwapPointer(&e.p, rm, p) {
				return
			}
		}
	}

	c.mu.Lock()
	r = c.read()
	if e = r.m[k]; e != nil {
		was = atomic.SwapPointer(&e.p, p) // was not in read, but promoted by the time we entered the lock and is now in read
	} else if e = c.dirty[k]; e != nil {
		was = atomic.SwapPointer(&e.p, p)
	} else {
		c.storeDirty(r, k, &ent[V]{p: p})
	}
	c.mu.Unlock()
}

func (l *loading[V]) setve(v V, err error) {
	l.once.Do(func() {
		l.v, l.err = v, err
		l.wgmiss.Done()
	})
}

func (c *Cache[K, V]) loadEnt(k K, e *ent[V], loading *loading[V], miss func() (V, error)) {
	defer loading.wg.Done()

	// We have created a new entry: we run miss. If we are configured to
	// not cache, we clear this entry upon return.
	if c.cfg.ageSet && c.cfg.maxAge <= 0 {
		defer c.Delete(k)
	}

	go func() { loading.setve(miss()) }()
	loading.wgmiss.Wait()

	loaded := &loaded[V]{kind: kindLoaded}
	loaded.v, loaded.err = loading.v, loading.err
	loaded.expires = c.cfg.newExpires(loaded.err)
	loaded.stale = loading.stale
	loading.loaded = loaded

	// If we cannot swap this value in, something else Set over us.
	atomic.CompareAndSwapPointer(&e.p, unsafe.Pointer(loading), unsafe.Pointer(loaded))
}

//////////////////////
// CACHE READ/DIRTY //
//////////////////////

var promotingDelete = unsafe.Pointer(new(any))

func (c *Cache[K, V]) read() read[K, V] {
	p := atomic.LoadPointer(&c.r)
	if p == nil {
		return read[K, V]{}
	}
	return *(*read[K, V])(p)
}
func (c *Cache[K, V]) storeRead(r read[K, V]) { atomic.StorePointer(&c.r, unsafe.Pointer(&r)) }

func (c *Cache[K, V]) storeDirty(r read[K, V], k K, e *ent[V]) {
	if !r.incomplete {
		if c.dirty == nil {
			c.dirty = make(map[K]*ent[V])
		}
		c.storeRead(read[K, V]{m: r.m, incomplete: true})
	}
	c.dirty[k] = e
}

func (c *Cache[K, V]) missed(r read[K, V]) {
	c.misses++
	if len(c.dirty) == 0 || c.misses > len(r.m)>>1 {
		c.promote()
	}
}

func (c *Cache[K, V]) promote() {
	r := c.read()
	keep := r.m

	defer func() {
		c.storeRead(read[K, V]{m: keep})
		c.misses = 0
	}()

	if len(c.dirty) == 0 {
		return
	}

	keep = make(map[K]*ent[V], len(keep)+len(c.dirty))
	for k, e := range c.dirty {
		keep[k] = e
	}
	c.dirty = nil

outer:
	for k, e := range r.m {
		p := atomic.LoadPointer(&e.p)
		for p == nil {
			if atomic.CompareAndSwapPointer(&e.p, nil, promotingDelete) {
				continue outer
			}
			p = atomic.LoadPointer(&e.p) // concurrently deleted while promoting
		}
		if p != promotingDelete {
			keep[k] = e
		}
	}
}

/////////
// ENT //
/////////

func (e *ent[V]) del() {
	if e == nil {
		return
	}
	for {
		p := atomic.LoadPointer(&e.p)
		if p == nil || p == promotingDelete {
			return
		}
		if atomic.CompareAndSwapPointer(&e.p, p, nil) {
			return
		}
	}
}

func pointerToLoad[V any](p unsafe.Pointer) (*loading[V], *loaded[V]) {
	switch ((*lcheck)(p)).kind {
	case kindLoading:
		return (*loading[V])(p), nil
	default: // kindLoading, only other option
		return nil, (*loaded[V])(p)
	}
}

func (e *ent[V]) load() (*loading[V], *loaded[V]) {
	if e == nil {
		return nil, nil
	}
	p := atomic.LoadPointer(&e.p)
	if p == nil || p == promotingDelete {
		return nil, nil
	}
	return pointerToLoad[V](p)
}

func (l *loaded[V]) expired(now int64) bool {
	expires := atomic.LoadInt64(&l.expires)
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
	_, loaded := e.load()
	if loaded == nil {
		return nil
	}
	if loaded.err != nil {
		return loaded.stale
	}
	return newStale(loaded.v, loaded.expires, age)
}

// Actually returns the stale; age must be non-zero.
func newStale[V any](v V, expires int64, age time.Duration) *stale[V] {
	if age < 0 {
		return &stale[V]{v: v}
	}
	return &stale[V]{v, expires + int64(age)}
}

// get always returns the value or the stale value. We do not check if our
// value is expired: we call this at the end of Get, we must always return
// something even if it is to be immediately expired.
func (e *ent[V]) get() (v V, err error, s KeyState) {
	loading, loaded := e.load()
	var waited bool
	if loading != nil {
		if loading.stale != nil && !loading.stale.expired(now()) {
			return loading.stale.v, nil, Stale
		}
		loading.wg.Wait()
		loaded = loading.loaded
		waited = true
	} else if loaded == nil {
		// No loading nor loaded: entry could be nil if deleted while
		// in the read map, or promotingDelete.
		return
	}

	// If we did not wait and our entry is expired (value or error), or if
	// our entry is not expired but has errored, we potentially return the
	// stale entry.
	//
	// If we waited, we could immediately be expired due to time sync, or
	// if the user is configured to never cache and they're just using
	// request collapsing: we still want to return the now expired value.
	now := now()
	if !waited && loaded.expired(now) || loaded.err != nil {
		if loaded.stale != nil && !loaded.stale.expired(now) {
			return loaded.stale.v, nil, Stale
		}
		// The stale value is expired: if our entry is not expired,
		// this must be an error we waited on.
		if !loaded.expired(now) {
			return loaded.v, loaded.err, Hit
		}
		return
	}
	return loaded.v, loaded.err, Hit
}

func (e *ent[V]) tryGet() (v V, err error, s KeyState) {
	if e == nil {
		return
	}
	loading, loaded := e.load()
	now := now()
	// If we are loading but there is a valid stale, return it, otherwise
	// return immediately: no get.
	if loading != nil {
		if loading.stale != nil && !loading.stale.expired(now) {
			return loading.stale.v, nil, Stale
		}
		return
	}
	// If not loading and not loaded, this is either deleted or
	// promotingDelete.
	if loaded == nil {
		return
	}

	// If we have an error or we are expired, we maybe return the stale.
	if loaded.err != nil || loaded.expired(now) {
		if loaded.stale != nil && !loaded.stale.expired(now) {
			return loaded.stale.v, nil, Stale
		}
	}
	if loaded.expired(now) {
		return
	}
	return loaded.v, loaded.err, Hit
}
