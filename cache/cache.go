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
		expires int64 // nano at which this ent is unusable, if non-zero
	}
	loading[V any] struct {
		kind     uint8 // == kindLoading
		done     chan struct{}
		override chan V
	}
	ent[V any] struct {
		p     unsafe.Pointer // [nil | promotingDelete | *loading | *loaded]
		stale *stale[V]
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
func (c *Cache[K, V]) Get(k K, miss func() (V, error)) (V, error) {
	r := c.read()
	e := r.m[k]
	if e != nil && !e.expired(now()) {
		return e.get()
	}

	c.mu.Lock()
	r = c.read()
	e = r.m[k]
	if e != nil && !e.expired(now()) {
		c.mu.Unlock()
		return e.get()
	}

	if r.incomplete {
		e = c.dirty[k]
		c.missed(r)
		if e != nil && !e.expired(now()) {
			c.mu.Unlock()
			return e.get()
		}
	}

	loading := &loading[V]{
		kind:     kindLoading,
		done:     make(chan struct{}),
		override: make(chan V),
	}
	e = &ent[V]{
		p:     unsafe.Pointer(loading),
		stale: e.maybeNewStale(c.cfg.maxStaleAge),
	}
	c.storeDirty(r, k, e)
	c.mu.Unlock()

	c.loadEnt(k, e, loading, func() (V, error) {
		done := make(chan struct{})
		var v V
		var err error
		go func() {
			defer close(done)
			v, err = miss()
		}()

		select {
		case o := <-loading.override:
			return o, nil
		case <-done:
			return v, err
		}
	})

	return e.get()
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
func (c *Cache[K, V]) TryGet(k K) (V, error, bool) {
	e := c.tryLoadEnt(k, nil)
	return e.tryGet()
}

// Delete deletes the value for a key.
func (c *Cache[K, V]) Delete(k K) {
	e := c.tryLoadEnt(k, func() { delete(c.dirty, k) })
	e.del()
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
	now := now()
	for {
		expires := atomic.LoadInt64(&loaded.expires)
		if expires < now {
			return
		}
		atomic.CompareAndSwapInt64(&loaded.expires, expires, now)
	}
}

// Each calls fn for every cached value. If fn returns false, iteration stops.
func (c *Cache[K, V]) Each(fn func(K, V, error) bool) {
	c.each(func(k K, e *ent[V]) bool {
		v, err, ok := e.tryGet()
		if !ok {
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
// MaxAge is used and the entry is older than the max age. If MaxStaleAge is
// also used and not -1, the entry must be older than MaxAge + MaxStaleAge.
func (c *Cache[K, V]) Clean() {
	// If no age has been set, we persist forever; if MaxStaleAge is -1,
	// the user opted into persisting stales forever and we do not clean.
	if !c.cfg.ageSet || c.cfg.maxStaleAge < 0 {
		return
	}
	now := now()
	c.each(func(k K, e *ent[V]) bool {
		_, loaded := e.load()
		if loaded != nil {
			expires := atomic.LoadInt64(&loaded.expires) + int64(c.cfg.maxStaleAge)
			if now > expires {
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
	p := unsafe.Pointer(loaded)

	var was unsafe.Pointer
	defer func() {
		if was == nil {
			return
		}
		if loading, _ := pointerToLoad[V](was); loading != nil {
			select {
			case loading.override <- v:
			case <-loading.done:
			}
		}
	}()

	r := c.read()
	if e, ok := r.m[k]; ok {
		for {
			p := atomic.LoadPointer(&e.p)
			if p == promotingDelete { // deleted & currently being ignored in a promote
				break
			}
			was = p
			if atomic.CompareAndSwapPointer(&e.p, p, p) {
				return
			}
		}
	}

	c.mu.Lock()
	r = c.read()
	if e := r.m[k]; e != nil {
		was = atomic.SwapPointer(&e.p, unsafe.Pointer(p))
	} else if e := c.dirty[k]; e != nil {
		was = atomic.SwapPointer(&e.p, unsafe.Pointer(p))
	} else {
		c.storeDirty(r, k, &ent[V]{p: p})
	}
	c.mu.Unlock()
}

func (c *Cache[K, V]) loadEnt(k K, e *ent[V], loading *loading[V], fn func() (V, error)) {
	defer close(loading.done)

	// We have created a new entry: we run miss. If we are configured to
	// not cache, we clear this entry upon return.
	if c.cfg.ageSet && c.cfg.maxAge <= 0 {
		defer c.Delete(k)
	}

	loaded := &loaded[V]{kind: kindLoaded}
	loaded.v, loaded.err = fn()
	loaded.expires = c.cfg.newExpires(loaded.err)

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
		delete(c.dirty, k)
	}

outer:
	for k, e := range r.m {
		p := atomic.LoadPointer(&e.p)
		for p == nil {
			if atomic.CompareAndSwapPointer(&e.p, nil, promotingDelete) {
				continue outer
			}
			p = atomic.LoadPointer(&e.p)
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
	default:
		return nil, nil
	case kindLoading:
		return (*loading[V])(p), nil
	case kindLoaded:
		return nil, (*loaded[V])(p)
	}
}

func (e *ent[V]) load() (*loading[V], *loaded[V]) {
	p := atomic.LoadPointer(&e.p)
	if p == nil || p == promotingDelete {
		return nil, nil
	}
	return pointerToLoad[V](p)
}

func (e *ent[V]) expired(now int64) bool {
	_, loaded := e.load()
	if loaded != nil {
		return loaded.expired(now)
	}
	return false
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
	// If e is non-nil, we are only in this func because e.expired is true,
	// meaning the value is loaded and the fields are immutable. We can
	// read all fields.
	_, loaded := e.load()
	if loaded == nil || loaded.err != nil {
		return e.stale
	}
	if age < 0 {
		return &stale[V]{v: loaded.v}
	}
	return &stale[V]{loaded.v, loaded.expires + int64(age)}
}

// get always returns the value or the stale value. We do not check if our
// value is expired: we call this at the end of Get, we must always return
// something even if it is to be immediately expired.
func (e *ent[V]) get() (V, error) {
	loading, loaded := e.load()
	if loading != nil {
		if e.stale != nil && !e.stale.expired(now()) {
			return e.stale.v, nil
		}
		<-loading.done
		_, loaded = e.load()
	}
	if loaded.err != nil && e.stale != nil && !e.stale.expired(now()) {
		return e.stale.v, nil
	}
	return loaded.v, loaded.err
}

func (e *ent[V]) tryGet() (v V, err error, b bool) {
	if e == nil {
		return
	}
	now := now()
	_, loaded := e.load()
	if loaded == nil || loaded.expired(now) { // not loaded, or e expired
		if e.stale != nil && !e.stale.expired(now) {
			return e.stale.v, nil, true
		}
		return // no stale, or stale expired
	}
	if loaded.err != nil && e.stale != nil && !e.stale.expired(now) {
		return e.stale.v, nil, true // have error and not-expired stale
	}
	return loaded.v, loaded.err, true
}

// available returns true if an entry has a non-erroring value or a stale
// value. This returns true in the same cases that tryGet would return true.
func (e *ent[V]) available(when int64) bool {
	_, loaded := e.load()
	return e != nil && (loaded != nil && !loaded.expired(when) || e.stale != nil && !e.stale.expired(when))
}
