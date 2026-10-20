package algo

import (
	"errors"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

const sec = int64(1_000_000) // one second in microseconds

// step is one call in a scenario: advance the clock, then request n tokens.
type step struct {
	advance time.Duration
	n       int64

	allowed    bool
	remaining  int64
	retryAfter time.Duration
	resetAfter time.Duration
}

func TestTokenBucketScenarios(t *testing.T) {
	fivePerSec := limiter.PerSecond(5, 10) // one token every 200ms, bursts of 10

	tests := []struct {
		name  string
		limit limiter.Limit
		steps []step
	}{
		{
			name:  "a new bucket starts full",
			limit: fivePerSec,
			steps: []step{
				{n: 1, allowed: true, remaining: 9, resetAfter: 200 * time.Millisecond},
			},
		},
		{
			name:  "burst, deny, then refill at the rate",
			limit: fivePerSec,
			steps: []step{
				{n: 10, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
				{n: 1, allowed: false, remaining: 0, retryAfter: 200 * time.Millisecond, resetAfter: 2 * time.Second},
				{advance: 199 * time.Millisecond, n: 1, allowed: false, retryAfter: time.Millisecond, resetAfter: 1801 * time.Millisecond},
				{advance: time.Millisecond, n: 1, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
			},
		},
		{
			name:  "a denial consumes nothing and promises an exact wait",
			limit: fivePerSec,
			steps: []step{
				{n: 9, allowed: true, remaining: 1, resetAfter: 1800 * time.Millisecond},
				{n: 4, allowed: false, remaining: 1, retryAfter: 600 * time.Millisecond, resetAfter: 1800 * time.Millisecond},
				{advance: 600 * time.Millisecond, n: 4, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
			},
		},
		{
			name:  "refill is capped at the burst",
			limit: fivePerSec,
			steps: []step{
				{n: 10, allowed: true, resetAfter: 2 * time.Second},
				{advance: time.Hour, n: 0, allowed: true, remaining: 10},
				{n: 10, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
				{n: 1, allowed: false, retryAfter: 200 * time.Millisecond, resetAfter: 2 * time.Second},
			},
		},
		{
			name:  "cost zero peeks without consuming",
			limit: fivePerSec,
			steps: []step{
				{n: 3, allowed: true, remaining: 7, resetAfter: 600 * time.Millisecond},
				{n: 0, allowed: true, remaining: 7, resetAfter: 600 * time.Millisecond},
				{n: 0, allowed: true, remaining: 7, resetAfter: 600 * time.Millisecond},
			},
		},
		{
			name:  "slow rates keep fractional progress",
			limit: limiter.PerMinute(1, 1), // one token per minute
			steps: []step{
				{n: 1, allowed: true, resetAfter: time.Minute},
				{advance: 30 * time.Second, n: 1, allowed: false, retryAfter: 30 * time.Second, resetAfter: 30 * time.Second},
				{advance: 29*time.Second + 999999*time.Microsecond, n: 1, allowed: false, retryAfter: time.Microsecond, resetAfter: time.Microsecond},
				{advance: time.Microsecond, n: 1, allowed: true, resetAfter: time.Minute},
			},
		},
		{
			name:  "fast rates refill within a millisecond",
			limit: limiter.PerSecond(100_000, 100), // 100 tokens per ms
			steps: []step{
				{n: 100, allowed: true, resetAfter: time.Millisecond},
				{advance: 500 * time.Microsecond, n: 51, allowed: false, remaining: 50, retryAfter: 10 * time.Microsecond, resetAfter: 500 * time.Microsecond},
				{advance: 10 * time.Microsecond, n: 51, allowed: true, remaining: 0, resetAfter: time.Millisecond},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				b     Bucket
				found bool
				now   = 1_700_000_000 * sec
			)
			for i, s := range tt.steps {
				now += s.advance.Microseconds()
				var (
					d   limiter.Decision
					err error
				)
				b, d, err = TokenBucket(b, found, tt.limit, now, s.n)
				if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				found = true
				want := limiter.Decision{Allowed: s.allowed, Limit: tt.limit, Remaining: s.remaining,
					RetryAfter: s.retryAfter, ResetAfter: s.resetAfter}
				if d != want {
					t.Fatalf("step %d:\n got %+v\nwant %+v", i, d, want)
				}
			}
		})
	}
}

func TestTokenBucketClockSkew(t *testing.T) {
	l := limiter.PerSecond(1, 2)
	now := 100 * sec
	b, _, _ := TokenBucket(Bucket{}, false, l, now, 2) // empty, stamped at 100s

	// Another instance, one second behind, sees the bucket next.
	b2, d, err := TokenBucket(b, true, l, now-sec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || b2 != b {
		t.Fatalf("a lagging clock refilled the bucket: %+v -> %+v (%+v)", b, b2, d)
	}
	// Its wait is measured on its own clock: 1s of lag plus 1s to earn a token.
	if d.RetryAfter != 2*time.Second || d.ResetAfter != 3*time.Second {
		t.Fatalf("lagging instance: RetryAfter %s ResetAfter %s, want 2s and 3s", d.RetryAfter, d.ResetAfter)
	}
	// Time catches up: exactly one token after one second past the stamp.
	if _, d, _ := TokenBucket(b2, true, l, now+sec, 1); !d.Allowed {
		t.Fatalf("token not available one period after the stamp: %+v", d)
	}
}

func TestTokenBucketRejectsBadInput(t *testing.T) {
	ok := limiter.PerSecond(1, 5)
	tests := []struct {
		name  string
		limit limiter.Limit
		n     int64
		want  error
	}{
		{"negative cost", ok, -1, limiter.ErrInvalidCost},
		{"cost above burst can never succeed", ok, 6, limiter.ErrInvalidCost},
		{"invalid limit", limiter.Limit{Rate: 1, Period: time.Second}, 1, limiter.ErrInvalidLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := TokenBucket(Bucket{}, false, tt.limit, 0, tt.n); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestIsFull(t *testing.T) {
	l := limiter.PerSecond(2, 4) // full again 2s after being emptied
	b, _, _ := TokenBucket(Bucket{}, false, l, 0, 4)
	for _, tc := range []struct {
		at   int64
		want bool
	}{{0, false}, {2*sec - 1, false}, {2 * sec, true}, {time.Hour.Microseconds(), true}} {
		if got := IsFull(b, l, tc.at); got != tc.want {
			t.Errorf("IsFull at %dµs = %t, want %t", tc.at, got, tc.want)
		}
	}
}

// FuzzTokenBucket drives a bucket with arbitrary limits, cost sequences and
// clock movements (including backwards jumps) and checks the invariants
// that make it a rate limiter:
//
//  1. the level stays within [0, burst] tokens;
//  2. the tokens granted never exceed burst + rate × elapsed / period;
//  3. a denied request succeeds after exactly RetryAfter, not a tick before;
//  4. ResetAfter is exactly the time until the bucket is full again.
func FuzzTokenBucket(f *testing.F) {
	f.Add(int64(5), int64(1_000_000), int64(10), []byte{0, 3, 10, 1, 200, 2, 7, 9})
	f.Add(int64(1), int64(60_000_000), int64(1), []byte{1, 1, 255, 1})
	f.Add(int64(1000), int64(1), int64(3), []byte{5, 2, 0, 3, 128, 1})

	f.Fuzz(func(t *testing.T, rate, periodUS, burst int64, ops []byte) {
		l := limiter.Limit{Rate: rate, Period: time.Duration(periodUS) * time.Microsecond, Burst: burst}
		if periodUS <= 0 || periodUS > int64(24*time.Hour/time.Microsecond) || l.Validate() != nil {
			t.Skip()
		}
		per := l.PeriodMicros()
		capacity := l.Burst * per

		var (
			b       Bucket
			found   bool
			start   = int64(1_000) * sec
			now     = start
			maxNow  = start
			origin  int64 // time of the first request, when the bucket is created full
			granted int64
		)
		for i := 0; i+1 < len(ops); i += 2 {
			// Advance by up to ~2 periods; values ≥ 240 jump backwards a little.
			if adv := int64(ops[i]); adv >= 240 {
				now -= (adv - 239) * per / 16
			} else {
				now += adv * per / 120
			}
			maxNow = max(maxNow, now)
			if !found {
				origin = now
			}
			n := int64(ops[i+1]) % (l.Burst + 1)

			nb, d, err := TokenBucket(b, found, l, now, n)
			if err != nil {
				t.Fatalf("op %d: %v", i/2, err)
			}

			if nb.Level < 0 || nb.Level > capacity {
				t.Fatalf("op %d: level %d outside [0, %d]", i/2, nb.Level, capacity)
			}
			if d.Allowed {
				granted += n
			} else {
				if d.RetryAfter <= 0 {
					t.Fatalf("op %d: denied with RetryAfter %s", i/2, d.RetryAfter)
				}
				// Nothing was consumed, and waiting RetryAfter is exactly enough.
				wait := d.RetryAfter.Microseconds()
				if _, early, _ := TokenBucket(nb, true, l, now+wait-1, n); early.Allowed {
					t.Fatalf("op %d: allowed 1µs before RetryAfter %s", i/2, d.RetryAfter)
				}
				if _, later, _ := TokenBucket(nb, true, l, now+wait, n); !later.Allowed {
					t.Fatalf("op %d: still denied after RetryAfter %s", i/2, d.RetryAfter)
				}
			}
			reset := d.ResetAfter.Microseconds()
			if !IsFull(nb, l, now+reset) || (reset > 0 && IsFull(nb, l, now+reset-1)) {
				t.Fatalf("op %d: ResetAfter %s is not the exact time to full (bucket %+v)", i/2, d.ResetAfter, nb)
			}
			if d.Remaining != nb.Level/per {
				t.Fatalf("op %d: Remaining %d, level says %d", i/2, d.Remaining, nb.Level/per)
			}
			b, found = nb, true
		}

		if ceiling := l.Burst + (maxNow-origin)*l.Rate/per; granted > ceiling {
			t.Fatalf("granted %d tokens, ceiling burst + rate×elapsed is %d", granted, ceiling)
		}
	})
}

func BenchmarkTokenBucket(b *testing.B) {
	l := limiter.PerSecond(1000, 100)
	var bk Bucket
	now := int64(0)
	for i := 0; i < b.N; i++ {
		now += 500
		bk, _, _ = TokenBucket(bk, i > 0, l, now, 1)
	}
}
