package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// manualClock is a clock that only moves when told to.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock() *manualClock {
	return &manualClock{now: time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func allow(t *testing.T, m *Limiter, key string, l limiter.Limit, n int64) limiter.Decision {
	t.Helper()
	d, err := m.Allow(context.Background(), key, l, n)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestAllowEnforcesTheLimitOverTime(t *testing.T) {
	clock := newManualClock()
	m := New(clock)
	l := limiter.PerSecond(2, 3)

	var allowed int
	// A request every 100ms from t=0 to t=10s inclusive (101 requests):
	// 3 (burst) + 2/s × 10s = 23 allowed.
	for i := 0; i <= 100; i++ {
		if allow(t, m, "user:1", l, 1).Allowed {
			allowed++
		}
		clock.Advance(100 * time.Millisecond)
	}
	if allowed != 23 {
		t.Fatalf("allowed %d of 101 requests, want 23", allowed)
	}
}

func TestKeysAndLimitsAreIndependent(t *testing.T) {
	m := New(newManualClock())
	l := limiter.PerSecond(1, 1)

	if !allow(t, m, "a", l, 1).Allowed || allow(t, m, "a", l, 1).Allowed {
		t.Fatal("key a: want allow then deny")
	}
	if !allow(t, m, "b", l, 1).Allowed {
		t.Fatal("key b is limited by key a's traffic")
	}
	// The same key under a different limit is a different bucket.
	if !allow(t, m, "a", limiter.PerSecond(1, 2), 1).Allowed {
		t.Fatal("new limit for key a reused the old bucket")
	}
}

func TestConcurrentCallersGetExactlyTheBurst(t *testing.T) {
	m := New(newManualClock()) // the clock never moves: nothing refills
	l := limiter.PerMinute(1, 50)

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
	)
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := m.Allow(context.Background(), "hot", l, 1)
			if err != nil {
				t.Error(err)
				return
			}
			if d.Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 50 {
		t.Fatalf("%d of 500 concurrent requests allowed, want exactly 50", got)
	}
}

func TestAllowWrapsErrors(t *testing.T) {
	m := New(newManualClock())
	_, err := m.Allow(context.Background(), "k", limiter.PerSecond(1, 1), 2)
	if !errors.Is(err, limiter.ErrInvalidCost) || err.Error() != `allow "k": invalid cost: cost 2 with burst 1` {
		t.Fatalf("error = %v", err)
	}
	if m.Len() != 0 {
		t.Fatal("a failed call stored state")
	}
}

func TestSweepDropsOnlyFullBuckets(t *testing.T) {
	clock := newManualClock()
	m := New(clock)
	slow := limiter.PerMinute(1, 1) // full again after 1 minute
	fast := limiter.PerSecond(1, 1) // full again after 1 second

	allow(t, m, "slow", slow, 1)
	allow(t, m, "fast", fast, 1)
	if m.Len() != 2 {
		t.Fatalf("Len = %d, want 2", m.Len())
	}

	clock.Advance(time.Second)
	if n := m.Sweep(); n != 1 || m.Len() != 1 {
		t.Fatalf("Sweep removed %d, Len %d; want 1 and 1", n, m.Len())
	}
	// The swept key behaves exactly like a full bucket.
	if !allow(t, m, "fast", fast, 1).Allowed {
		t.Fatal("swept key lost its tokens")
	}
	// The kept key still remembers it is empty.
	if allow(t, m, "slow", slow, 1).Allowed {
		t.Fatal("sweep reset a bucket that was not full")
	}
}

func TestPeekOnIdleKeyStoresNothing(t *testing.T) {
	m := New(newManualClock())
	d := allow(t, m, "idle", limiter.PerSecond(5, 10), 0)
	if !d.Allowed || d.Remaining != 10 || m.Len() != 0 {
		t.Fatalf("peek = %+v, Len %d", d, m.Len())
	}
}

func BenchmarkAllow(b *testing.B) {
	m := New(limiter.SystemClock{})
	l := limiter.PerSecond(1_000_000, 1_000)
	ctx := context.Background()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}

	b.Run("serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = m.Allow(ctx, keys[i%len(keys)], l, 1)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				_, _ = m.Allow(ctx, keys[next.Add(1)%int64(len(keys))], l, 1)
			}
		})
	})
}
