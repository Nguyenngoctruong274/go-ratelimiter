// Package memory is a single-process limiter. It enforces limits only within
// one instance; it is the reference for the Redis store, the fallback when
// Redis is unreachable, and fine on its own for a service with one replica.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/algo"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// state is keyed by key and limit together: the bucket level is expressed in
// units of the limit's period, so reading it under another limit would be
// meaningless. A key whose limit changes simply starts a fresh, full bucket.
type bucketKey struct {
	key   string
	limit limiter.Limit
}

// Limiter is a concurrency-safe in-memory token-bucket limiter.
type Limiter struct {
	clock limiter.Clock

	mu      sync.Mutex
	buckets map[bucketKey]algo.Bucket
}

var _ limiter.Limiter = (*Limiter)(nil)

// New returns a limiter that reads time from clock (limiter.SystemClock{}
// in production).
func New(clock limiter.Clock) *Limiter {
	return &Limiter{clock: clock, buckets: make(map[bucketKey]algo.Bucket)}
}

// Allow implements limiter.Limiter. The context is unused: the check never
// blocks on I/O.
func (m *Limiter) Allow(_ context.Context, key string, l limiter.Limit, n int64) (limiter.Decision, error) {
	now := m.clock.Now().UnixMicro()
	k := bucketKey{key: key, limit: l}

	m.mu.Lock()
	defer m.mu.Unlock()

	b, found := m.buckets[k]
	nb, d, err := algo.TokenBucket(b, found, l, now, n)
	if err != nil {
		return limiter.Decision{}, fmt.Errorf("allow %q: %w", key, err)
	}
	if d.ResetAfter == 0 {
		// Full again (a peek on an idle key): same as absent, store nothing.
		delete(m.buckets, k)
	} else {
		m.buckets[k] = nb
	}
	return d, nil
}

// Sweep drops buckets that have refilled completely, since a full bucket and
// a missing one behave the same. Call it periodically to bound memory by the
// number of keys active within one refill period rather than all keys ever
// seen. It returns the number of buckets removed.
func (m *Limiter) Sweep() int {
	now := m.clock.Now().UnixMicro()

	m.mu.Lock()
	defer m.mu.Unlock()
	removed := 0
	for k, b := range m.buckets {
		if algo.IsFull(b, k.limit, now) {
			delete(m.buckets, k)
			removed++
		}
	}
	return removed
}

// Len returns the number of buckets currently held.
func (m *Limiter) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.buckets)
}
