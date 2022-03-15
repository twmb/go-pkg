package cache

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGet2x(t *testing.T) {
	var c Cache[string, int]

	{
		i, _, _ := c.Get("foo", func() (int, error) {
			return 3, nil
		})
		if i != 3 {
			t.Errorf("got %d != exp 3", i)
		}
	}

	{
		i, _, _ := c.Get("foo", func() (int, error) {
			panic("should have been cached")
		})
		if i != 3 {
			t.Errorf("got %d != exp 3", i)
		}
	}
}

func TestCollapsedGet(t *testing.T) {
	const niter = 1000

	var (
		c     Cache[string, *int]
		r     = new(int)
		calls int64
		ps    = make(chan *int)
	)
	for i := 0; i < niter; i++ {
		go func() {
			p, _, _ := c.Get("foo", func() (*int, error) {
				if atomic.AddInt64(&calls, 1) != 1 {
					t.Error("closure called multiple times")
				}
				time.Sleep(50 * time.Millisecond)
				return r, nil
			})
			ps <- p
		}()
	}
	for i := 0; i < niter; i++ {
		p := <-ps
		if p != r {
			t.Error("pointer mismatch")
		}
	}
}
