// Package ring provides a ring buffer that can help a work loop goroutine.
package ring

import (
	"fmt"
	"sync"
)

// Ring replaces a channel, but allows for not having a continually polling
// goroutine. On push, a ring can tell you if the push is the first item
// buffered (or dead). If your push is the first item buffered, you know a work
// loop needs to begin on the item that was just pushed. In the work loop, when
// you are done, you can DropPeek to drop the first buffered item and peek at
// the next. If there is another buffered item, you can continue on what you
// peeked, if not, the work loop can exit.
type Ring[T any] struct {
	mu sync.Mutex
	c  *sync.Cond

	elems []T

	head int
	tail int
	l    int
	dead bool
}

// New returns a new ring of capacity n. If n is less than or equal to zero,
// this panics.
func New[T any](n int) *Ring[T] {
	if n <= 0 {
		panic(fmt.Sprintf("ring.New: invalid size %d", n))
	}
	return &Ring[T]{elems: make([]T, n)}
}

// Close closes the ring, making all future pushes fail,
func (r *Ring[T]) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dead = true
	if r.c != nil {
		r.c.Broadcast()
	}
}

// Len returns the current number of buffered items.
func (r *Ring[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.elems)
}

// DrainHow controls when the worker loop exits in PushLoop.
type DrainHow bool

var (
	// DrainClosed signifies that PushLoop should not quit until the ring
	// is either closed or empty. When either condition is encountered, the
	// worker loop will stil.
	DrainClosed DrainHow = false

	// DrainAll signifies that PushLoop should not quit until the ring is
	// empty, even if the ring is closed.
	DrainAll DrainHow = true
)

// Push pushes t to the ring. If t is the first element in the ring, this
// begins a work loop that calls each for each element in the ring until the
// ring is empty or until the ring is closed. drainHow controls when the loop
// quits: either when the ring is empty or closed, or strictly empty.
func (r *Ring[T]) PushLoop(drainHow DrainHow, t T, each func(T)) {
	first, _ := r.Push(t)
	if first {
		go r.loop(drainHow, t, each)
	}
}

func (r *Ring[T]) loop(drainHow DrainHow, t T, each func(T)) {
	if drainHow == DrainClosed {
		closed := false
		for more := true; more && !closed; {
			each(t)
			t, more, closed = r.DropPeek()
		}
	} else {
		for more := true; more; {
			each(t)
			t, more, _ = r.DropPeek()
		}
	}
}

// Push pushes an element to the buffer, blocking for space to be available to
// push into. The push fails if the ring is closed. This returns if this pushes
// the first element to an empty ring, which can be used to start a work loop.
func (r *Ring[T]) Push(e T) (first, closed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitN(len(r.elems))
	if r.dead {
		return false, true
	}

	r.elems[r.tail] = e
	r.tail = (r.tail + 1) % len(r.elems)
	r.l = r.l + 1

	return r.l == 1, false
}

// Pop returns the next buffered element, if any, or if the ring is closed.
// This will not return that the ring is closed unless all buffered elements
// are drained. This follows the same semantics as draining a channel.
func (r *Ring[T]) Pop() (t T, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitN(0)
	if r.l > 0 {
		t = r.elems[r.head]
		r.head = (r.head + 1) % len(r.elems)
		r.l--
		return t, true
	} else { // dead
		return t, false
	}
}

// TryPop returns the next buffered element, if any. This is similar to a
// select/default.
func (r *Ring[T]) TryPop() (t T, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.l > 0 {
		t = r.elems[r.head]
		r.head = (r.head + 1) % len(r.elems)
		r.l--
		return t, true
	} else {
		return t, false
	}
}

// DropPeek drops the next buffered element and returns: a peek of the next, if
// there even is a peek, and if the ring is closed. This will return closed as
// soon as Close is called; there may be buffered elements to drain (if you
// desire).
func (r *Ring[T]) DropPeek() (peek T, more, closed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.waitN(0)

	more = r.l > 0
	if more {
		r.elems[r.head] = peek // reset to default
		r.head = (r.head + 1) % len(r.elems)
		r.l--
		if r.c != nil {
			r.c.Signal()
		}
	}
	return r.elems[r.head], more, r.dead
}

func (r *Ring[T]) waitN(n int) {
	for r.l == n && !r.dead {
		if r.c == nil {
			r.c = sync.NewCond(&r.mu)
		}
		r.c.Wait()
	}
}
