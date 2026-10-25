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

// model is the Go reference for one script. step applies a call and
// returns the decision and the state the script must have stored, encoded
// the way stored reads it from Redis.
type model interface {
	step(now, n int64) (limiter.Decision, string, error)
	forget() // the store dropped the state
}

type tokenBucketModel struct {
	l     limiter.Limit
	b     algo.Bucket
	found bool
}

func (m *tokenBucketModel) step(now, n int64) (limiter.Decision, string, error) {
	b, d, err := algo.TokenBucket(m.b, m.found, m.l, now, n)
	m.b, m.found = b, true
	return d, fmt.Sprintf("%d:%d", b.Level, b.Stamp), err
}
func (m *tokenBucketModel) forget() { m.found = false }

type gcraModel struct {
	l     limiter.Limit
	t     algo.TAT
	found bool
}

func (m *gcraModel) step(now, n int64) (limiter.Decision, string, error) {
	t, d, err := algo.GCRA(m.t, m.found, m.l, now, n)
	if d.Allowed && n > 0 { // only grants write
		m.t, m.found = t, true
	}
	return d, fmt.Sprintf("%d:%d", t.Micros, t.Frac), err
}
func (m *gcraModel) forget() { m.found = false }

type slidingWindowModel struct {
	l     limiter.Limit
	w     algo.Window
	found bool
}

func (m *slidingWindowModel) step(now, n int64) (limiter.Decision, string, error) {
	w, d, err := algo.SlidingWindow(m.w, m.found, m.l, now, n)
	if d.Allowed && n > 0 { // only grants write
		m.w, m.found = w, true
	}
	return d, fmt.Sprintf("%d:%d:%d", w.Stamp, w.Curr, w.Prev), err
}
func (m *slidingWindowModel) forget() { m.found = false }

// algorithm describes one Redis limiter for the shared tests.
type algorithm struct {
	name   string
	new    func(rdb redis.Scripter, prefix string) *scripted
	model  func(l limiter.Limit) model
	stored func(ctx context.Context, c *redis.Client, key string) (string, error)
}

var algorithms = []algorithm{
	{
		name:  "token bucket",
		new:   func(rdb redis.Scripter, prefix string) *scripted { return &NewTokenBucket(rdb, prefix).scripted },
		model: func(l limiter.Limit) model { return &tokenBucketModel{l: l} },
		stored: func(ctx context.Context, c *redis.Client, key string) (string, error) {
			v, err := c.HMGet(ctx, key, "l", "s").Result()
			if err != nil || v[0] == nil {
				return "", err
			}
			return fmt.Sprintf("%v:%v", v[0], v[1]), nil
		},
	},
	{
		name:   "GCRA",
		new:    func(rdb redis.Scripter, prefix string) *scripted { return &NewGCRA(rdb, prefix).scripted },
		model:  func(l limiter.Limit) model { return &gcraModel{l: l} },
		stored: getString,
	},
	{
		name:   "sliding window",
		new:    func(rdb redis.Scripter, prefix string) *scripted { return &NewSlidingWindow(rdb, prefix).scripted },
		model:  func(l limiter.Limit) model { return &slidingWindowModel{l: l} },
		stored: getString,
	},
}

func getString(ctx context.Context, c *redis.Client, key string) (string, error) {
	v, err := c.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

// TestScriptsMatchReference replays the same random timelines through each
// Lua script and its Go reference in package algo and requires identical
// decisions and identical stored state after every single call. The limits
// include the extremes each algorithm accepts, where a float64 slip in a
// script would show up first.
func TestScriptsMatchReference(t *testing.T) {
	c := newTestClient(t)

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
		{Rate: 1, Period: time.Microsecond, Burst: 94_906_265},              // sliding window: burst × span ≈ 2^53
		{Rate: 7, Period: 13 * time.Second, Burst: 9_001},
	}
	rng := mrand.New(mrand.NewSource(42))
	for i := 0; i < 30; i++ {
		per := []time.Duration{time.Millisecond, time.Second, time.Minute, 13 * time.Second}[rng.Intn(4)]
		limits = append(limits, limiter.Limit{Rate: 1 + rng.Int63n(1000), Period: per, Burst: 1 + rng.Int63n(500)})
	}

	for _, tr := range transports(t, c) {
		for _, alg := range algorithms {
			t.Run(tr.name+"/"+alg.name, func(t *testing.T) {
				tested := 0
				for li, l := range limits {
					store := alg.new(tr.rdb, testPrefix(t, c))
					if store.check(l, 0) != nil {
						continue // beyond what this algorithm accepts
					}
					tested++
					t.Run(fmt.Sprintf("%d_%s", li, strings.ReplaceAll(l.String(), " ", "_")), func(t *testing.T) {
						replay(t, c, alg, store, l, rng)
					})
				}
				if tested < len(limits)-3 {
					t.Fatalf("only %d of %d limits were valid for %s", tested, len(limits), alg.name)
				}
			})
		}
	}
}

// transport is a way to send the scripts: straight through the client, or
// batched by a Pipeline. Every script must behave identically on both.
type transport struct {
	name string
	rdb  redis.Scripter
}

func transports(t testing.TB, c *redis.Client) []transport {
	p := NewPipeline(c, PipelineOptions{})
	t.Cleanup(func() { _ = p.Close() })
	return []transport{{"direct", c}, {"pipelined", p}}
}

func replay(t *testing.T, c *redis.Client, alg algorithm, store *scripted, l limiter.Limit, rng *mrand.Rand) {
	ctx := context.Background()
	now := int64(1_760_000_000) * sec
	store.now = func() int64 { return now }
	key := store.Key("k", l)
	ref := alg.model(l)

	// Move the clock on the scale of the time to refill the full burst, but
	// by at most ~12 days a step: Unix microseconds pass 2^53 in 2255.
	scale := min(1<<40, max(1, (l.Burst*l.PeriodMicros()+l.Rate-1)/l.Rate))
	for op := 0; op < 80; op++ {
		switch r := rng.Intn(10); {
		case r == 0: // clock behind the stored state
			now -= rng.Int63n(scale/2 + 1)
		case r < 7:
			now += rng.Int63n(scale/8 + 1)
		default:
			now += rng.Int63n(2 * scale)
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

		want, wantState, err := ref.step(now, n)
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

		// Stored state: absent exactly when the full burst is back.
		state, err := alg.stored(ctx, c, key)
		if err != nil {
			t.Fatal(err)
		}
		if want.ResetAfter == 0 {
			if state != "" {
				t.Fatalf("op %d: state %q kept at full burst", op, state)
			}
			ref.forget()
			continue
		}
		if state != wantState {
			t.Fatalf("op %d: stored %q, reference %q", op, state, wantState)
		}
	}
}

// TestScriptsUseRedisClockAndExpire drains a limit on the Redis clock and
// checks the waits and that the key's TTL is exactly the time until the
// full burst is back.
func TestScriptsUseRedisClockAndExpire(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	l := limiter.PerMinute(3, 3) // one request every 20s

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(c, testPrefix(t, c))
			for i := 0; i < 3; i++ {
				if d, err := store.Allow(ctx, "user:1", l, 1); err != nil || !d.Allowed {
					t.Fatalf("request %d: %+v, %v", i, d, err)
				}
			}
			d, err := store.Allow(ctx, "user:1", l, 1)
			if err != nil {
				t.Fatal(err)
			}
			if d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 2*time.Minute {
				t.Fatalf("4th request: %+v, want denied", d)
			}
			if alg.name != "sliding window" && (d.RetryAfter <= 19*time.Second || d.RetryAfter > 20*time.Second) {
				t.Fatalf("4th request: RetryAfter %s, want just under 20s", d.RetryAfter)
			}

			ttl, err := c.PTTL(ctx, store.Key("user:1", l)).Result()
			if err != nil {
				t.Fatal(err)
			}
			if diff := d.ResetAfter - ttl; diff < -time.Millisecond || diff > time.Second {
				t.Fatalf("PTTL = %s, ResetAfter = %s; want them equal", ttl, d.ResetAfter)
			}
		})
	}
}

func TestScriptsPeekStoresNothing(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	l := limiter.PerSecond(5, 10)

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(c, testPrefix(t, c))
			d, err := store.Allow(ctx, "idle", l, 0)
			if err != nil || !d.Allowed || d.Remaining != 10 || d.ResetAfter != 0 {
				t.Fatalf("peek = %+v, %v", d, err)
			}
			if n, _ := c.Exists(ctx, store.Key("idle", l)).Result(); n != 0 {
				t.Fatal("a peek on an idle key created state")
			}
		})
	}
}

// TestScriptsAreAtomicAcrossInstances simulates several service instances
// (separate connection pools) hammering one key. Redis runs each script to
// completion before the next, so exactly Burst requests may pass.
func TestScriptsAreAtomicAcrossInstances(t *testing.T) {
	first := newTestClient(t)
	l := limiter.PerMinute(1, 100) // effectively no refill during the test

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			prefix := testPrefix(t, first)
			var (
				wg      sync.WaitGroup
				allowed atomic.Int64
			)
			for inst := 0; inst < 5; inst++ {
				store := alg.new(newTestClient(t), prefix)
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
		})
	}
}

func TestScriptsReloadAfterFlush(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	l := limiter.PerSecond(1, 1)

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(c, testPrefix(t, c))
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
		})
	}
}

func TestScriptsStateExpires(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	l := limiter.Limit{Rate: 1, Period: 20 * time.Millisecond, Burst: 2} // a 40ms window

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(c, testPrefix(t, c))
			d, err := store.Allow(ctx, "k", l, 2)
			if err != nil || !d.Allowed {
				t.Fatalf("drain: %+v, %v", d, err)
			}
			time.Sleep(d.ResetAfter + 20*time.Millisecond)
			if n, _ := c.Exists(ctx, store.Key("k", l)).Result(); n != 0 {
				t.Fatalf("state outlived ResetAfter %s", d.ResetAfter)
			}
			if d, err := store.Allow(ctx, "k", l, 2); err != nil || !d.Allowed {
				t.Fatalf("after expiry: %+v, %v", d, err)
			}
		})
	}
}

// TestGCRADenialsDoNotWrite checks the property GCRA is chosen for: once a
// key is exhausted, turning requests away leaves its value and TTL alone.
func TestGCRADenialsDoNotWrite(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	store := NewGCRA(c, testPrefix(t, c))
	l := limiter.PerMinute(1, 1)
	key := store.Key("k", l)

	if d, err := store.Allow(ctx, "k", l, 1); err != nil || !d.Allowed {
		t.Fatalf("first: %+v, %v", d, err)
	}
	before, _ := c.Get(ctx, key).Result()
	// Shorten the TTL: a write would reset it to about a minute.
	if err := c.PExpire(ctx, key, 30*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if d, err := store.Allow(ctx, "k", l, 1); err != nil || d.Allowed {
			t.Fatalf("denial %d: %+v, %v", i, d, err)
		}
	}
	after, _ := c.Get(ctx, key).Result()
	ttl, _ := c.PTTL(ctx, key).Result()
	if after != before || ttl > 30*time.Second {
		t.Fatalf("denials wrote: value %q -> %q, PTTL %s", before, after, ttl)
	}
}

func TestScriptErrors(t *testing.T) {
	// Nothing listens on port 1: any call that reaches the network fails.
	down := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 200 * time.Millisecond})
	defer down.Close()
	ctx := context.Background()

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			store := alg.new(down, "")
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
		})
	}

	// The sliding window has its own range limit, also checked up front.
	huge := limiter.Limit{Rate: 1, Period: time.Hour, Burst: 1_000_000}
	if _, err := NewSlidingWindow(down, "").Allow(ctx, "k", huge, 1); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("sliding window too large: %v", err)
	}
}

func TestKeyIncludesTheAlgorithmAndLimit(t *testing.T) {
	l := limiter.PerMinute(10, 20)
	for _, tc := range []struct {
		store *scripted
		want  string
	}{
		{&NewTokenBucket(nil, "").scripted, "rl:tb:10:60000000:20:user:7"},
		{&NewGCRA(nil, "").scripted, "rl:gcra:10:60000000:20:user:7"},
		{&NewSlidingWindow(nil, "app:").scripted, "app:sw:10:60000000:20:user:7"},
	} {
		if got := tc.store.Key("user:7", l); got != tc.want {
			t.Errorf("Key = %q, want %q", got, tc.want)
		}
	}
	s := NewTokenBucket(nil, "")
	if s.Key("user:7", limiter.PerMinute(10, 21)) == s.Key("user:7", l) {
		t.Fatal("different limits share a key")
	}
}

func BenchmarkScriptsRedis(b *testing.B) {
	c := newTestClient(b)
	l := limiter.PerSecond(1_000_000, 1_000)
	ctx := context.Background()
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}

	for _, alg := range algorithms {
		store := alg.new(c, testPrefix(b, c))
		b.Run(alg.name+"/serial", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := store.Allow(ctx, keys[i%len(keys)], l, 1); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(alg.name+"/parallel", func(b *testing.B) {
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
}

// TestFloat64FloorDivIsExact checks the claim floordiv in the scripts
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
