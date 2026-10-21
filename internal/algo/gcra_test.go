package algo

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

func TestGCRAScenarios(t *testing.T) {
	tests := []struct {
		name  string
		limit limiter.Limit
		steps []step
	}{
		{
			name:  "burst, deny, then one request per emission interval",
			limit: limiter.PerSecond(5, 10),
			steps: []step{
				{n: 10, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
				{n: 1, allowed: false, retryAfter: 200 * time.Millisecond, resetAfter: 2 * time.Second},
				{advance: 199 * time.Millisecond, n: 1, allowed: false, retryAfter: time.Millisecond, resetAfter: 1801 * time.Millisecond},
				{advance: time.Millisecond, n: 1, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
			},
		},
		{
			name:  "fractional emission interval stays exact",
			limit: limiter.Limit{Rate: 3, Period: 7 * time.Millisecond, Burst: 3}, // one per 2333⅓µs
			steps: []step{
				{n: 3, allowed: true, resetAfter: 7 * time.Millisecond},
				{advance: 2333 * time.Microsecond, n: 1, allowed: false, retryAfter: time.Microsecond, resetAfter: 4667 * time.Microsecond},
				{advance: time.Microsecond, n: 1, allowed: true, resetAfter: 7 * time.Millisecond},
				// The next one is due at 4666⅔µs: an interval rounded down to
				// 2333µs would admit it at 4666µs, rounded up at 4668µs.
				{advance: 2332 * time.Microsecond, n: 1, allowed: false, retryAfter: time.Microsecond, resetAfter: 4668 * time.Microsecond},
				{advance: time.Microsecond, n: 1, allowed: true, resetAfter: 7 * time.Millisecond},
			},
		},
		{
			name:  "past TAT equals no state",
			limit: limiter.PerMinute(1, 2),
			steps: []step{
				{n: 2, allowed: true, resetAfter: 2 * time.Minute},
				{advance: time.Hour, n: 0, allowed: true, remaining: 2},
				{n: 2, allowed: true, resetAfter: 2 * time.Minute},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				s     TAT
				found bool
				now   = 1_700_000_000 * sec
			)
			for i, st := range tt.steps {
				now += st.advance.Microseconds()
				var (
					d   limiter.Decision
					err error
				)
				s, d, err = GCRA(s, found, tt.limit, now, st.n)
				if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				found = true
				want := limiter.Decision{Allowed: st.allowed, Limit: tt.limit, Remaining: st.remaining,
					RetryAfter: st.retryAfter, ResetAfter: st.resetAfter}
				if d != want {
					t.Fatalf("step %d:\n got %+v\nwant %+v", i, d, want)
				}
			}
		})
	}
}

// TestGCRAMatchesTokenBucket replays random timelines through GCRA and
// TokenBucket. On a monotonic clock they are the same algorithm, so every
// decision must be identical.
func TestGCRAMatchesTokenBucket(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	limits := []limiter.Limit{
		limiter.PerSecond(5, 10),
		limiter.PerMinute(7, 3),
		{Rate: 3, Period: 7 * time.Millisecond, Burst: 5},
		{Rate: 1, Period: time.Hour, Burst: MaxGCRARefill / 3_600_000_000}, // refill ≈ 2^52µs
		{Rate: limiter.MaxScaled/2 - 1, Period: time.Microsecond, Burst: 1},
	}
	for i := 0; i < 50; i++ {
		limits = append(limits, limiter.Limit{Rate: 1 + rng.Int63n(1000), Period: time.Duration(1+rng.Int63n(5_000_000)) * time.Microsecond, Burst: 1 + rng.Int63n(300)})
	}
	for _, l := range limits {
		var (
			b      Bucket
			g      TAT
			found  bool
			now    = 1_700_000_000 * sec
			per    = l.PeriodMicros()
			denied int
		)
		for op := 0; op < 2000; op++ {
			if rng.Intn(3) == 0 {
				now += rng.Int63n(2 * per)
			} else {
				now += rng.Int63n(per/8 + 1)
			}
			n := rng.Int63n(l.Burst + 1)
			nb, want, err := TokenBucket(b, found, l, now, n)
			if err != nil {
				t.Fatal(err)
			}
			ng, got, err := GCRA(g, found, l, now, n)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("%s, op %d (n %d):\ngcra %+v\n  tb %+v", l, op, n, got, want)
			}
			if !got.Allowed {
				denied++
			}
			b, g, found = nb, ng, true
		}
		if denied == 0 {
			t.Errorf("%s: no request was denied; the timeline does not exercise the limit", l)
		}
	}
}

func TestGCRAClockSkew(t *testing.T) {
	l := limiter.PerSecond(1, 2)
	now := 100 * sec
	s, _, _ := GCRA(TAT{}, false, l, now, 2) // TAT = 102s

	// An instance one second behind sees three seconds of debt: more than
	// the burst, so it is denied, and its waits include the lag.
	s2, d, err := GCRA(s, true, l, now-sec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || s2 != s {
		t.Fatalf("a lagging clock was granted: %+v -> %+v (%+v)", s, s2, d)
	}
	if d.RetryAfter != 2*time.Second || d.ResetAfter != 3*time.Second {
		t.Fatalf("lagging instance: RetryAfter %s ResetAfter %s, want 2s and 3s", d.RetryAfter, d.ResetAfter)
	}
	if _, d, _ := GCRA(s, true, l, now-sec, 0); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("peek from a lagging clock: %+v", d)
	}
}

func TestGCRARejectsBadInput(t *testing.T) {
	// Valid as a token bucket, but the TAT could lie past 2^53µs.
	long := limiter.Limit{Rate: 1, Period: time.Hour, Burst: 2_000_000}
	if err := long.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := GCRA(TAT{}, false, long, 0, 1); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("refill longer than 2^52µs: %v", err)
	}

	if _, _, err := GCRA(TAT{}, false, limiter.PerSecond(1, 5), 0, 6); !errors.Is(err, limiter.ErrInvalidCost) {
		t.Fatalf("cost above burst: %v", err)
	}
	if _, _, err := GCRA(TAT{}, false, limiter.Limit{Rate: 1, Period: time.Second}, 0, 1); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("invalid limit: %v", err)
	}
}

// FuzzGCRA checks GCRA's own invariants, including backward clock jumps,
// where it is allowed to differ from TokenBucket:
//
//  1. the TAT never lies more than burst × period/rate past the latest time;
//  2. the tokens granted never exceed burst + rate × elapsed / period;
//  3. a denied request succeeds after exactly RetryAfter, not a tick before;
//  4. ResetAfter is exactly the time until the full burst is back;
//  5. denials and peeks leave the state untouched.
func FuzzGCRA(f *testing.F) {
	f.Add(int64(5), int64(1_000_000), int64(10), []byte{0, 3, 10, 1, 200, 2, 7, 9})
	f.Add(int64(3), int64(7_000), int64(2), []byte{1, 1, 255, 1, 250, 2, 0, 1})
	f.Add(int64(1000), int64(1), int64(3), []byte{5, 2, 0, 3, 128, 1})

	f.Fuzz(func(t *testing.T, rate, periodUS, burst int64, ops []byte) {
		l := limiter.Limit{Rate: rate, Period: time.Duration(periodUS) * time.Microsecond, Burst: burst}
		if periodUS <= 0 || periodUS > int64(24*time.Hour/time.Microsecond) || CheckGCRA(l, 0) != nil {
			t.Skip()
		}
		per := l.PeriodMicros()

		var (
			s       TAT
			found   bool
			start   = int64(1_000) * sec
			now     = start
			maxNow  = start
			minNow  = start // a clock jumping back can grant before the first call
			granted int64
		)
		for i := 0; i+1 < len(ops); i += 2 {
			if adv := int64(ops[i]); adv >= 240 {
				now -= (adv - 239) * per / 16
			} else {
				now += adv * per / 120
			}
			maxNow, minNow = max(maxNow, now), min(minNow, now)
			n := int64(ops[i+1]) % (l.Burst + 1)

			ns, d, err := GCRA(s, found, l, now, n)
			if err != nil {
				t.Fatalf("op %d: %v", i/2, err)
			}
			if ns.Frac < 0 || ns.Frac >= l.Rate {
				t.Fatalf("op %d: fraction %d outside [0, rate)", i/2, ns.Frac)
			}
			if found && ns.Micros-maxNow > ceilDiv(l.Burst*per, l.Rate) {
				t.Fatalf("op %d: TAT %d is more than the burst ahead of %d", i/2, ns.Micros, maxNow)
			}
			if d.Allowed && n > 0 {
				granted += n
			} else if ns != s {
				t.Fatalf("op %d: a denial or peek changed the state %+v -> %+v", i/2, s, ns)
			}
			if !d.Allowed {
				wait := d.RetryAfter.Microseconds()
				if wait <= 0 {
					t.Fatalf("op %d: denied with RetryAfter %s", i/2, d.RetryAfter)
				}
				if _, early, _ := GCRA(ns, true, l, now+wait-1, n); early.Allowed {
					t.Fatalf("op %d: allowed 1µs before RetryAfter %s", i/2, d.RetryAfter)
				}
				if _, later, _ := GCRA(ns, true, l, now+wait, n); !later.Allowed {
					t.Fatalf("op %d: still denied after RetryAfter %s", i/2, d.RetryAfter)
				}
			}
			reset := d.ResetAfter.Microseconds()
			if _, full, _ := GCRA(ns, true, l, now+reset, 0); full.Remaining != l.Burst || full.ResetAfter != 0 {
				t.Fatalf("op %d: not full after ResetAfter %s: %+v", i/2, d.ResetAfter, full)
			}
			if reset > 0 {
				if _, early, _ := GCRA(ns, true, l, now+reset-1, 0); early.ResetAfter == 0 {
					t.Fatalf("op %d: full 1µs before ResetAfter %s", i/2, d.ResetAfter)
				}
			}
			s, found = ns, true
		}

		if ceiling := l.Burst + (maxNow-minNow)*l.Rate/per; granted > ceiling {
			t.Fatalf("granted %d tokens, ceiling burst + rate×elapsed is %d", granted, ceiling)
		}
	})
}

func BenchmarkGCRA(b *testing.B) {
	l := limiter.PerSecond(1000, 100)
	var s TAT
	now := int64(0)
	for i := 0; i < b.N; i++ {
		now += 500
		s, _, _ = GCRA(s, i > 0, l, now, 1)
	}
}
