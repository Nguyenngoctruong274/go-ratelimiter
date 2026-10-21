package algo

import (
	"errors"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

func TestWindowSpan(t *testing.T) {
	tests := []struct {
		limit limiter.Limit
		want  int64
	}{
		{limiter.PerSecond(10, 10), sec},
		{limiter.PerSecond(5, 10), 2 * sec},
		{limiter.PerMinute(60, 1), sec},
		{limiter.Limit{Rate: 3, Period: 7 * time.Millisecond, Burst: 1}, 2334}, // 2333⅓ rounded up
	}
	for _, tt := range tests {
		if got, err := WindowSpan(tt.limit); err != nil || got != tt.want {
			t.Errorf("WindowSpan(%s) = %d, %v; want %d", tt.limit, got, err, tt.want)
		}
	}

	// Valid as a token bucket, but burst × span leaves the exact range.
	huge := limiter.Limit{Rate: 1, Period: time.Hour, Burst: 1_000_000}
	if err := huge.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := WindowSpan(huge); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("WindowSpan(%s) error = %v, want ErrInvalidLimit", huge, err)
	}
}

func TestSlidingWindowScenarios(t *testing.T) {
	tenPerSec := limiter.PerSecond(10, 10) // windows of one second

	tests := []struct {
		name  string
		steps []step
	}{
		{
			name: "full window, then the previous one fades out",
			steps: []step{
				{n: 10, allowed: true, remaining: 0, resetAfter: 2 * time.Second},
				// Next window: 10 × (1 - t) + 1 <= 10 from t = 100ms.
				{n: 1, allowed: false, retryAfter: 1100 * time.Millisecond, resetAfter: 2 * time.Second},
				{advance: 1100 * time.Millisecond, n: 1, allowed: true, remaining: 0, resetAfter: 1900 * time.Millisecond},
			},
		},
		{
			name: "the previous window counts in proportion to its overlap",
			steps: []step{
				{n: 4, allowed: true, remaining: 6, resetAfter: 2 * time.Second},
				{advance: 1500 * time.Millisecond, n: 0, allowed: true, remaining: 8, resetAfter: 500 * time.Millisecond},
				{n: 8, allowed: true, remaining: 0, resetAfter: 1500 * time.Millisecond},
				// 4 × (1 - t) + 8 + 1 <= 10 from t = 750ms, 250ms from now.
				{n: 1, allowed: false, retryAfter: 250 * time.Millisecond, resetAfter: 1500 * time.Millisecond},
				{advance: 249 * time.Millisecond, n: 1, allowed: false, retryAfter: time.Millisecond, resetAfter: 1251 * time.Millisecond},
				{advance: time.Millisecond, n: 1, allowed: true, remaining: 0, resetAfter: 1250 * time.Millisecond},
			},
		},
		{
			name: "a quiet window forgets everything",
			steps: []step{
				{n: 10, allowed: true, resetAfter: 2 * time.Second},
				{advance: 2 * time.Second, n: 10, allowed: true, resetAfter: 2 * time.Second},
			},
		},
		{
			// The weakness of the estimate: it assumes the previous window's
			// events were spread out. Here all ten came at its very end, so
			// the second ending at 1.5s really holds 15 events.
			name: "events bunched at the end of a window are underestimated",
			steps: []step{
				{advance: 900 * time.Millisecond, n: 10, allowed: true, resetAfter: 1100 * time.Millisecond},
				{advance: 100 * time.Millisecond, n: 1, allowed: false, retryAfter: 100 * time.Millisecond, resetAfter: time.Second},
				{advance: 100 * time.Millisecond, n: 1, allowed: true, resetAfter: 1900 * time.Millisecond},
				{advance: 400 * time.Millisecond, n: 4, allowed: true, resetAfter: 1500 * time.Millisecond},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				w     Window
				found bool
				now   = 1_700_000_000 * sec // a window boundary
			)
			for i, s := range tt.steps {
				now += s.advance.Microseconds()
				var (
					d   limiter.Decision
					err error
				)
				w, d, err = SlidingWindow(w, found, tenPerSec, now, s.n)
				if err != nil {
					t.Fatalf("step %d: %v", i, err)
				}
				found = true
				want := limiter.Decision{Allowed: s.allowed, Limit: tenPerSec, Remaining: s.remaining,
					RetryAfter: s.retryAfter, ResetAfter: s.resetAfter}
				if d != want {
					t.Fatalf("step %d:\n got %+v\nwant %+v", i, d, want)
				}
			}
		})
	}
}

func TestSlidingWindowClockSkew(t *testing.T) {
	l := limiter.PerSecond(2, 2)
	now := 100*sec + sec/2
	w, _, _ := SlidingWindow(Window{}, false, l, now, 2)

	// One second behind, the caller is treated as being at the stamp: the
	// window is still full, and the wait includes the lag.
	w2, d, err := SlidingWindow(w, true, l, now-sec, 1)
	if err != nil {
		t.Fatal(err)
	}
	if d.Allowed || w2 != w {
		t.Fatalf("a lagging clock was granted: %+v -> %+v (%+v)", w, w2, d)
	}
	// Room appears when 2 × (1 - t) + 1 <= 2, halfway into the next window:
	// 1s of lag + 500ms to the boundary + 500ms into it.
	if d.RetryAfter != 2*time.Second || d.ResetAfter != 2500*time.Millisecond {
		t.Fatalf("lagging instance: RetryAfter %s ResetAfter %s, want 2s and 2.5s", d.RetryAfter, d.ResetAfter)
	}
}

func TestSlidingWindowRejectsBadInput(t *testing.T) {
	if _, _, err := SlidingWindow(Window{}, false, limiter.PerSecond(1, 5), 0, 6); !errors.Is(err, limiter.ErrInvalidCost) {
		t.Fatalf("cost above burst: %v", err)
	}
	if _, _, err := SlidingWindow(Window{}, false, limiter.Limit{Rate: 1, Period: time.Hour, Burst: 1_000_000}, 0, 1); !errors.Is(err, limiter.ErrInvalidLimit) {
		t.Fatalf("window too large: %v", err)
	}
}

// FuzzSlidingWindow checks the sliding-window invariants under arbitrary
// limits, costs and clock movements, including backward jumps:
//
//  1. no fixed window ever counts more than burst events;
//  2. the events granted never exceed burst per window touched;
//  3. a denied request succeeds after exactly RetryAfter, not a tick before;
//  4. ResetAfter is exactly the time until the full burst is back;
//  5. denials and peeks leave the state untouched.
func FuzzSlidingWindow(f *testing.F) {
	f.Add(int64(10), int64(1_000_000), int64(10), []byte{0, 3, 10, 10, 200, 2, 7, 9})
	f.Add(int64(3), int64(7_000), int64(2), []byte{1, 1, 255, 1, 250, 2, 0, 1})
	f.Add(int64(1000), int64(1), int64(3), []byte{5, 2, 0, 3, 128, 1})

	f.Fuzz(func(t *testing.T, rate, periodUS, burst int64, ops []byte) {
		l := limiter.Limit{Rate: rate, Period: time.Duration(periodUS) * time.Microsecond, Burst: burst}
		if periodUS <= 0 || periodUS > int64(24*time.Hour/time.Microsecond) {
			t.Skip()
		}
		span, err := WindowSpan(l)
		if err != nil {
			t.Skip()
		}

		var (
			w       Window
			found   bool
			start   = int64(1_000) * sec
			now     = start
			maxNow  = start
			minNow  = start // a clock jumping back can grant before the first call
			granted int64
		)
		for i := 0; i+1 < len(ops); i += 2 {
			if adv := int64(ops[i]); adv >= 240 {
				now -= (adv - 239) * span / 16
			} else {
				now += adv * span / 60
			}
			maxNow, minNow = max(maxNow, now), min(minNow, now)
			n := int64(ops[i+1]) % (l.Burst + 1)

			nw, d, err := SlidingWindow(w, found, l, now, n)
			if err != nil {
				t.Fatalf("op %d: %v", i/2, err)
			}
			if nw.Curr < 0 || nw.Curr > l.Burst || nw.Prev < 0 || nw.Prev > l.Burst {
				t.Fatalf("op %d: counts %+v outside [0, %d]", i/2, nw, l.Burst)
			}
			if d.Allowed && n > 0 {
				granted += n
			} else if nw != w {
				t.Fatalf("op %d: a denial or peek changed the state %+v -> %+v", i/2, w, nw)
			}
			if !d.Allowed {
				wait := d.RetryAfter.Microseconds()
				if wait <= 0 {
					t.Fatalf("op %d: denied with RetryAfter %s", i/2, d.RetryAfter)
				}
				if _, early, _ := SlidingWindow(nw, true, l, now+wait-1, n); early.Allowed {
					t.Fatalf("op %d: allowed 1µs before RetryAfter %s (%+v)", i/2, d.RetryAfter, nw)
				}
				if _, later, _ := SlidingWindow(nw, true, l, now+wait, n); !later.Allowed {
					t.Fatalf("op %d: still denied after RetryAfter %s (%+v)", i/2, d.RetryAfter, nw)
				}
			}
			reset := d.ResetAfter.Microseconds()
			if _, full, _ := SlidingWindow(nw, true, l, now+reset, 0); full.Remaining != l.Burst || full.ResetAfter != 0 {
				t.Fatalf("op %d: not full after ResetAfter %s: %+v", i/2, d.ResetAfter, full)
			}
			if reset > 0 {
				if _, early, _ := SlidingWindow(nw, true, l, now+reset-1, 0); early.ResetAfter == 0 {
					t.Fatalf("op %d: full 1µs before ResetAfter %s", i/2, d.ResetAfter)
				}
			}
			w, found = nw, true
		}

		windows := floorDiv(maxNow, span) - floorDiv(minNow, span) + 1
		if ceiling := l.Burst * windows; granted > ceiling {
			t.Fatalf("granted %d events over %d windows, ceiling %d", granted, windows, ceiling)
		}
	})
}

func BenchmarkSlidingWindow(b *testing.B) {
	l := limiter.PerSecond(1000, 100)
	var w Window
	now := int64(0)
	for i := 0; i < b.N; i++ {
		now += 500
		w, _, _ = SlidingWindow(w, i > 0, l, now, 1)
	}
}
