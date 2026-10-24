package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type manualClock struct {
	mu  sync.Mutex
	now time.Time
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

type change struct{ from, to State }

func newBreaker(t *testing.T, threshold int, cooldown time.Duration) (*Breaker, *manualClock, *[]change) {
	t.Helper()
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	var (
		mu      sync.Mutex
		changes []change
	)
	b, err := New(Options{Threshold: threshold, Cooldown: cooldown, Clock: clock, OnChange: func(from, to State) {
		mu.Lock()
		defer mu.Unlock()
		changes = append(changes, change{from, to})
	}})
	if err != nil {
		t.Fatal(err)
	}
	return b, clock, &changes
}

// call runs one call through the breaker and reports whether it was let through.
func call(b *Breaker, r Result) bool {
	t, ok := b.Allow()
	if ok {
		b.Done(t, r)
	}
	return ok
}

func TestBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, changes := newBreaker(t, 3, time.Second)

	for _, r := range []Result{Failure, Failure, Success, Failure, Failure} {
		if !call(b, r) {
			t.Fatal("refused while closed")
		}
	}
	if got := b.State(); got != Closed {
		t.Fatalf("state = %s after a success broke the run, want closed", got)
	}
	call(b, Failure)
	if got := b.State(); got != Open {
		t.Fatalf("state = %s after 3 consecutive failures, want open", got)
	}
	if _, ok := b.Allow(); ok {
		t.Fatal("open breaker let a call through")
	}
	if want := []change{{Closed, Open}}; !equal(*changes, want) {
		t.Fatalf("changes = %v, want %v", *changes, want)
	}
}

func TestBreakerIgnoredResultsDoNotCount(t *testing.T) {
	b, _, _ := newBreaker(t, 2, time.Second)
	for i := 0; i < 10; i++ {
		call(b, Ignored)
	}
	call(b, Failure)
	if got := b.State(); got != Closed {
		t.Fatalf("state = %s, want closed: ignored calls must not count as failures", got)
	}
}

func TestBreakerHalfOpenProbe(t *testing.T) {
	tests := []struct {
		name  string
		probe Result
		want  State
		// wantNext says whether the call after the probe is let through.
		wantNext bool
	}{
		{"success closes", Success, Closed, true},
		{"failure reopens", Failure, Open, false},
		{"ignored lets another probe", Ignored, HalfOpen, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, clock, _ := newBreaker(t, 1, time.Second)
			call(b, Failure)

			clock.Advance(time.Second - time.Microsecond)
			if _, ok := b.Allow(); ok {
				t.Fatal("let a call through before the cooldown passed")
			}
			clock.Advance(time.Microsecond)

			probe, ok := b.Allow()
			if !ok || b.State() != HalfOpen {
				t.Fatalf("after cooldown: ok=%v state=%s, want a probe in half-open", ok, b.State())
			}
			if _, ok := b.Allow(); ok {
				t.Fatal("let a second call through while the probe is in flight")
			}
			b.Done(probe, tt.probe)
			if got := b.State(); got != tt.want {
				t.Fatalf("state after probe = %s, want %s", got, tt.want)
			}
			if got := call(b, Success); got != tt.wantNext {
				t.Fatalf("next call let through = %v, want %v", got, tt.wantNext)
			}
		})
	}
}

func TestBreakerReopenRestartsCooldown(t *testing.T) {
	b, clock, changes := newBreaker(t, 1, time.Second)
	call(b, Failure)
	clock.Advance(time.Second)
	call(b, Failure) // failed probe

	clock.Advance(time.Second - time.Microsecond)
	if _, ok := b.Allow(); ok {
		t.Fatal("cooldown after a failed probe must start from the probe")
	}
	clock.Advance(time.Microsecond)
	call(b, Success)
	want := []change{{Closed, Open}, {Open, HalfOpen}, {HalfOpen, Open}, {Open, HalfOpen}, {HalfOpen, Closed}}
	if !equal(*changes, want) {
		t.Fatalf("changes = %v, want %v", *changes, want)
	}
}

// A call that started before the breaker changed state reports into a
// generation that no longer exists: it must not move the breaker.
func TestBreakerStaleResultsAreDropped(t *testing.T) {
	b, clock, _ := newBreaker(t, 1, time.Second)

	slow, _ := b.Allow() // still running when the breaker opens
	call(b, Failure)
	b.Done(slow, Success)
	if got := b.State(); got != Open {
		t.Fatalf("a stale success moved the breaker to %s", got)
	}

	clock.Advance(time.Second)
	probe, _ := b.Allow()
	b.Done(probe, Success)
	stale, _ := b.Allow()
	call(b, Success)
	b.Done(stale, Failure) // same generation: counts, and threshold is 1
	if got := b.State(); got != Open {
		t.Fatalf("state = %s, want open: a current failure must count", got)
	}
}

func TestBreakerConcurrentProbeIsExclusive(t *testing.T) {
	b, clock, _ := newBreaker(t, 1, time.Second)
	call(b, Failure)
	clock.Advance(time.Second)

	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
		probes  = make(chan Ticket, 64)
	)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if t, ok := b.Allow(); ok {
				allowed.Add(1)
				probes <- t
			}
		}()
	}
	wg.Wait()
	if got := allowed.Load(); got != 1 {
		t.Fatalf("%d concurrent calls let through in half-open, want exactly 1", got)
	}
	b.Done(<-probes, Success)
	if got := b.State(); got != Closed {
		t.Fatalf("state = %s, want closed", got)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Options{Threshold: -1}); err == nil {
		t.Error("negative threshold accepted")
	}
	if _, err := New(Options{Cooldown: -time.Second}); err == nil {
		t.Error("negative cooldown accepted")
	}
	b, err := New(Options{})
	if err != nil || b.threshold != 5 || b.cooldown != 5*time.Second {
		t.Fatalf("defaults: %+v, %v", b, err)
	}
}

func TestStateString(t *testing.T) {
	for s, want := range map[State]string{Closed: "closed", Open: "open", HalfOpen: "half-open", State(9): "State(9)"} {
		if got := s.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(s), got, want)
		}
	}
}

func equal(a, b []change) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
