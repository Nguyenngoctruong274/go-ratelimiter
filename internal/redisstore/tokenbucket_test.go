package redisstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	mrand "math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/algo"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// newTestClient connects to RL_TEST_REDIS_URL or skips the test.
func newTestClient(t testing.TB) *redis.Client {
	t.Helper()
	url := os.Getenv("RL_TEST_REDIS_URL")
	if url == "" {
		t.Skip("RL_TEST_REDIS_URL not set; skipping Redis tests")
	}
	c, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// testPrefix returns a prefix unique to this test and deletes its keys
// afterwards, so tests can share one Redis database.
func testPrefix(t testing.TB, c *redis.Client) string {
	t.Helper()
	var b [6]byte
	_, _ = rand.Read(b[:])
	prefix := "rltest:" + hex.EncodeToString(b[:]) + ":"
	t.Cleanup(func() {
		ctx := context.Background()
		iter := c.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
	})
	return prefix
}

const sec = int64(1_000_000)

// TestTokenBucketMatchesReference replays the same random timelines through
// the Lua script and through algo.TokenBucket and requires identical
// decisions and identical stored state after every single call. The limits
// include the extremes Limit.Validate allows, where a float64 slip in the
// script would show up first.
func TestTokenBucketMatchesReference(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	maxHourBurst := int64((limiter.MaxScaled - 1) / 3_600_000_000)
	limits := []limiter.Limit{
		limiter.PerSecond(5, 10),
		limiter.PerSecond(100_000, 100),
		limiter.PerMinute(1, 1),
		limiter.PerMinute(7, 3),
		{Rate: 3, Period: 7 * time.Millisecond, Burst: 5},
		{Rate: 1, Period: time.Microsecond, Burst: 1},
		{Rate: 1, Period: time.Hour, Burst: maxHourBurst},                   // capacity ≈ 2^53
		{Rate: 999_983, Period: time.Hour, Burst: maxHourBurst - 1},         // large prime rate near the edge
		{Rate: limiter.MaxScaled/2 - 1, Period: time.Microsecond, Burst: 1}, // huge rate
	}
	rng := mrand.New(mrand.NewSource(42))
	for i := 0; i < 30; i++ {
		per := []time.Duration{time.Millisecond, time.Second, time.Minute, 13 * time.Second}[rng.Intn(4)]
		limits = append(limits, limiter.Limit{Rate: 1 + rng.Int63n(1000), Period: per, Burst: 1 + rng.Int63n(500)})
	}

	for li, l := range limits {
		t.Run(fmt.Sprintf("%d_%s", li, strings.ReplaceAll(l.String(), " ", "_")), func(t *testing.T) {
			if err := l.Validate(); err != nil {
				t.Fatal(err)
			}
			now := int64(1_760_000_000) * sec
			store := NewTokenBucket(c, testPrefix(t, c))
			store.now = func() int64 { return now }
			key := store.Key("k", l)

			var (
				ref   algo.Bucket
				found bool
			)
			per := l.PeriodMicros()
			for op := 0; op < 80; op++ {
				switch r := rng.Intn(10); {
				case r == 0: // clock behind the stored stamp
					now -= rng.Int63n(per + 1)
				case r < 7:
					now += rng.Int63n(per/4 + 1)
				default:
					now += rng.Int63n(3 * per)
				}
				var n int64
				switch rng.Intn(4) {
				case 0:
					n = 0
				case 1:
					n = l.Burst
				default:
					n = 1 + rng.Int63n(l.Burst)
				}

				nb, want, err := algo.TokenBucket(ref, found, l, now, n)
				if err != nil {
					t.Fatal(err)
				}
				got, err := store.Allow(ctx, "k", l, n)
				if err != nil {
					t.Fatal(err)
				}
				if got != want {
					t.Fatalf("op %d (now %d, n %d):\n lua %+v\n  go %+v", op, now, n, got, want)
				}

				// Stored state: absent exactly when the bucket is full.
				vals, err := c.HMGet(ctx, key, "l", "s").Result()
				if err != nil {
					t.Fatal(err)
				}
				if want.ResetAfter == 0 {
					if vals[0] != nil {
						t.Fatalf("op %d: full bucket still stored: %v", op, vals)
					}
					ref, found = algo.Bucket{}, false
					continue
				}
				level, _ := strconv.ParseInt(fmt.Sprint(vals[0]), 10, 64)
				stamp, _ := strconv.ParseInt(fmt.Sprint(vals[1]), 10, 64)
				if level != nb.Level || stamp != nb.Stamp {
					t.Fatalf("op %d: stored {l:%v s:%v}, reference %+v", op, vals[0], vals[1], nb)
				}
				ref, found = nb, true
			}
		})
	}
}

func TestTokenBucketUsesRedisClockAndExpires(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	store := NewTokenBucket(c, testPrefix(t, c))
	l := limiter.PerMinute(3, 3) // one token every 20s

	for i := 0; i < 3; i++ {
		if d, err := store.Allow(ctx, "user:1", l, 1); err != nil || !d.Allowed {
			t.Fatalf("request %d: %+v, %v", i, d, err)
		}
	}
	d, err := store.Allow(ctx, "user:1", l, 1)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || d.RetryAfter <= 19*time.Second || d.RetryAfter > 20*time.Second {
		t.Fatalf("4th request: %+v, want denied with RetryAfter just under 20s", d)
	}
	if d.ResetAfter <= 59*time.Second || d.ResetAfter > time.Minute {
		t.Fatalf("ResetAfter = %s, want just under 1m", d.ResetAfter)
	}

	// The key expires exactly when the bucket would be full again.
	ttl, err := c.PTTL(ctx, store.Key("user:1", l)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 59*time.Second || ttl > time.Minute {
		t.Fatalf("PTTL = %s, want just under 1m", ttl)
	}
}

func TestTokenBucketPeekStoresNothing(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	store := NewTokenBucket(c, testPrefix(t, c))
	l := limiter.PerSecond(5, 10)

	d, err := store.Allow(ctx, "idle", l, 0)
	if err != nil || !d.Allowed || d.Remaining != 10 || d.ResetAfter != 0 {
		t.Fatalf("peek = %+v, %v", d, err)
	}
	if n, _ := c.Exists(ctx, store.Key("idle", l)).Result(); n != 0 {
		t.Fatal("a peek on an idle key created state")
	}
}

// TestTokenBucketIsAtomicAcrossInstances simulates several service instances
// (separate connection pools) hammering one key. Redis runs each script to
// completion before the next, so exactly Burst requests may pass.
func TestTokenBucketIsAtomicAcrossInstances(t *testing.T) {
	first := newTestClient(t)
	prefix := testPrefix(t, first)
	l := limiter.PerMinute(1, 100) // effectively no refill during the test

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
	)
	for inst := 0; inst < 5; inst++ {
		store := NewTokenBucket(newTestClient(t), prefix)
		for g := 0; g < 40; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 5; i++ {
					d, err := store.Allow(context.Background(), "hot", l, 1)
					if err != nil {
						t.Error(err)
						return
					}
					if d.Allowed {
						allowed.Add(1)
					}
				}
			}()
		}
	}
	wg.Wait()
	if got := allowed.Load(); got != 100 {
		t.Fatalf("%d of 1000 requests from 5 instances allowed, want exactly 100", got)
	}
}

func TestTokenBucketReloadsFlushedScript(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	store := NewTokenBucket(c, testPrefix(t, c))
	l := limiter.PerSecond(1, 1)

	if _, err := store.Allow(ctx, "a", l, 1); err != nil {
		t.Fatal(err)
	}
	// A restarted or failed-over Redis has an empty script cache.
	if err := c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Allow(ctx, "b", l, 1); err != nil {
		t.Fatalf("after SCRIPT FLUSH: %v", err)
	}
}

func TestTokenBucketErrors(t *testing.T) {
	// Nothing listens on port 1: any call that reaches the network fails.
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer down.Close()
	store := NewTokenBucket(down, "")
	ctx := context.Background()

	// Invalid input is rejected before any round trip.
	if _, err := store.Allow(ctx, "k", limiter.PerSecond(1, 1), 2); !errors.Is(err, limiter.ErrInvalidCost) {
		t.Fatalf("cost above burst: %v", err)
	}
	if _, err := store.Allow(ctx, "k", limiter.Limit{}, 1); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("invalid limit: %v", err)
	}
	// A Redis failure is an error, never a silent "allowed".
	d, err := store.Allow(ctx, "k", limiter.PerSecond(1, 1), 1)
	if err == nil || !strings.Contains(err.Error(), `allow "k": redis:`) || d.Allowed {
		t.Fatalf("redis down: %+v, %v", d, err)
	}
}

func TestKeyIncludesTheLimit(t *testing.T) {
	s := NewTokenBucket(nil, "")
	got := s.Key("user:7", limiter.PerMinute(10, 20))
	if got != "rl:tb:10:60000000:20:user:7" {
		t.Fatalf("Key = %q", got)
	}
	if s.Key("user:7", limiter.PerMinute(10, 21)) == got {
		t.Fatal("different limits share a key")
	}
}

func BenchmarkTokenBucketRedis(b *testing.B) {
	c := newTestClient(b)
	store := NewTokenBucket(c, testPrefix(b, c))
	l := limiter.PerSecond(1_000_000, 1_000)
	ctx := context.Background()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}

	b.Run("serial", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := store.Allow(ctx, keys[i%len(keys)], l, 1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("parallel", func(b *testing.B) {
		var next atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := store.Allow(ctx, keys[next.Add(1)%int64(len(keys))], l, 1); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

// TestFloat64FloorDivIsExact checks the claim floordiv in tokenbucket.lua
// relies on, using Go's float64 (the same IEEE double as Lua's number):
// for integers 0 <= a < 2^53 and b > 0, floor(a/b) computed in float64
// equals integer division. The cases concentrate where it would break:
// huge numerators, quotients just below an integer, and large divisors.
func TestFloat64FloorDivIsExact(t *testing.T) {
	const top = int64(1) << 53
	rng := mrand.New(mrand.NewSource(7))
	check := func(a, b int64) {
		if got := int64(math.Floor(float64(a) / float64(b))); got != a/b {
			t.Fatalf("float64 floor(%d/%d) = %d, want %d", a, b, got, a/b)
		}
	}
	for i := 0; i < 200_000; i++ {
		b := 1 + rng.Int63n(top-1)
		if i%2 == 0 {
			b = 1 + rng.Int63n(1<<20) // small divisors, huge quotients
		}
		q := rng.Int63n((top-1)/b + 1)
		for _, a := range []int64{q*b - 1, q * b, q*b + 1, q*b + b - 1} {
			if a >= 0 && a < top {
				check(a, b)
			}
		}
	}
	check(top-1, 1)
	check(top-1, 3)
	check(top-2, top-1)
}

// TestTokenBucketStateExpires checks that Redis forgets a bucket once it
// would be full, and that a forgotten bucket grants the full burst again.
func TestTokenBucketStateExpires(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	store := NewTokenBucket(c, testPrefix(t, c))
	l := limiter.Limit{Rate: 1, Period: 20 * time.Millisecond, Burst: 2} // full 40ms after emptying

	if d, err := store.Allow(ctx, "k", l, 2); err != nil || !d.Allowed {
		t.Fatalf("drain: %+v, %v", d, err)
	}
	time.Sleep(60 * time.Millisecond)
	if n, _ := c.Exists(ctx, store.Key("k", l)).Result(); n != 0 {
		t.Fatal("state outlived the time to refill")
	}
	if d, err := store.Allow(ctx, "k", l, 2); err != nil || !d.Allowed {
		t.Fatalf("after expiry: %+v, %v", d, err)
	}
}
