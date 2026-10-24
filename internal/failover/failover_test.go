package failover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
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

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

var errDown = errors.New("connection refused")

// fakePrimary returns err while it is set, and otherwise allows everything.
type fakePrimary struct {
	calls atomic.Int64
	mu    sync.Mutex
	err   error
	block bool // wait for the context instead of answering
}

func (p *fakePrimary) set(err error, block bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err, p.block = err, block
}

func (p *fakePrimary) Allow(ctx context.Context, _ string, l limiter.Limit, n int64) (limiter.Decision, error) {
	p.calls.Add(1)
	p.mu.Lock()
	err, block := p.err, p.block
	p.mu.Unlock()
	if block {
		<-ctx.Done()
		return limiter.Decision{}, fmt.Errorf("redis: %w", ctx.Err())
	}
	if err != nil {
		return limiter.Decision{}, err
	}
	if err := l.Check(n); err != nil {
		return limiter.Decision{}, err
	}
	return limiter.Decision{Allowed: true, Limit: l, Remaining: l.Burst - n}, nil
}

func newLimiter(t *testing.T, p limiter.Limiter, opts Options) (*Limiter, *manualClock) {
	t.Helper()
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	opts.Breaker.Clock = clock
	if opts.Breaker.Cooldown == 0 {
		opts.Breaker.Cooldown = time.Second
	}
	if opts.Breaker.Threshold == 0 {
		opts.Breaker.Threshold = 3
	}
	opts.Logger = quiet
	f, err := New(p, opts)
	if err != nil {
		t.Fatal(err)
	}
	return f, clock
}

var lim = limiter.PerSecond(10, 10)

func TestHealthyPrimaryDecides(t *testing.T) {
	p := &fakePrimary{}
	fb := memory.New(limiter.SystemClock{})
	f, _ := newLimiter(t, p, Options{Fallback: fb})
	for i := 0; i < 50; i++ {
		d, err := f.Allow(context.Background(), "k", lim, 1)
		if err != nil || !d.Allowed {
			t.Fatalf("call %d: %+v, %v", i, d, err)
		}
	}
	if p.calls.Load() != 50 || fb.Len() != 0 {
		t.Fatalf("primary calls = %d, fallback buckets = %d; want 50 and 0", p.calls.Load(), fb.Len())
	}
}

func TestWithoutFallbackFailsWithErrUnavailable(t *testing.T) {
	p := &fakePrimary{}
	p.set(errDown, false)
	f, _ := newLimiter(t, p, Options{})

	_, err := f.Allow(context.Background(), "k", lim, 1)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, errDown) {
		t.Fatalf("closed breaker: err = %v, want ErrUnavailable wrapping the cause", err)
	}
	for i := 0; i < 2; i++ {
		_, _ = f.Allow(context.Background(), "k", lim, 1)
	}
	_, err = f.Allow(context.Background(), "k", lim, 1)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, breaker.ErrOpen) {
		t.Fatalf("open breaker: err = %v, want ErrUnavailable wrapping ErrOpen", err)
	}
	if got := p.calls.Load(); got != 3 {
		t.Fatalf("primary called %d times, want 3: an open breaker must not call it", got)
	}
}

// The whole life of an outage: the breaker trips after Threshold failures,
// the fallback enforces the local share while it is open, a failed probe
// keeps it open, and a good probe hands decisions back to the primary.
func TestOutageAndRecovery(t *testing.T) {
	p := &fakePrimary{}
	clock := &manualClock{now: time.Unix(1_700_000_000, 0)}
	fb := memory.New(clock)
	var states []breaker.State
	f, bclock := newLimiter(t, p, Options{Fallback: fb, Replicas: 4, Breaker: breaker.Options{
		OnChange: func(_, to breaker.State) { states = append(states, to) },
	}})
	ctx := context.Background()

	p.set(errDown, false)
	allowed := 0
	for i := 0; i < 20; i++ {
		d, err := f.Allow(ctx, "k", lim, 1)
		if err != nil {
			t.Fatalf("call %d: %v; a fallback must hide the outage", i, err)
		}
		if d.Allowed {
			allowed++
		}
		if d.Limit != limiter.PerSecond(3, 3) {
			t.Fatalf("fallback limit = %s, want the 1/4 share 3/1s burst 3", d.Limit)
		}
	}
	if allowed != 3 {
		t.Fatalf("fallback allowed %d of 20, want its burst of 3", allowed)
	}
	if got := p.calls.Load(); got != 3 {
		t.Fatalf("primary called %d times during the outage, want 3 (the threshold)", got)
	}

	bclock.Advance(time.Second) // cooldown over: one probe, still failing
	_, _ = f.Allow(ctx, "k", lim, 1)
	if f.State() != breaker.Open || p.calls.Load() != 4 {
		t.Fatalf("after failed probe: state %s, calls %d; want open, 4", f.State(), p.calls.Load())
	}

	p.set(nil, false)
	bclock.Advance(time.Second)
	d, err := f.Allow(ctx, "k", lim, 1)
	if err != nil || d.Limit != lim || f.State() != breaker.Closed {
		t.Fatalf("after recovery: %+v, %v, state %s; want the primary's decision, closed", d, err, f.State())
	}
	want := []breaker.State{breaker.Open, breaker.HalfOpen, breaker.Open, breaker.HalfOpen, breaker.Closed}
	if fmt.Sprint(states) != fmt.Sprint(want) {
		t.Fatalf("states = %v, want %v", states, want)
	}
}

func TestSlowPrimaryTimesOut(t *testing.T) {
	p := &fakePrimary{}
	p.set(nil, true)
	f, _ := newLimiter(t, p, Options{Timeout: 5 * time.Millisecond, Breaker: breaker.Options{Threshold: 1}})

	start := time.Now()
	_, err := f.Allow(context.Background(), "k", lim, 1)
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want ErrUnavailable wrapping the deadline", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("check took %s with a 5ms timeout", took)
	}
	if f.State() != breaker.Open {
		t.Fatalf("state = %s, want open: a timeout is a failure", f.State())
	}
}

// Errors that say nothing about the health of the primary must not trip
// the breaker, however many there are.
func TestErrorsThatDoNotTrip(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() context.Context
		limit   limiter.Limit
		cost    int64
		block   bool
		wantErr error
	}{
		{"invalid limit", context.Background, limiter.Limit{Rate: 0, Period: time.Second, Burst: 1}, 1, false, limiter.ErrInvalidLimit},
		{"invalid cost", context.Background, lim, 11, false, limiter.ErrInvalidCost},
		{"caller canceled", func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, lim, 1, true, context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &fakePrimary{}
			p.set(nil, tt.block)
			f, _ := newLimiter(t, p, Options{Fallback: memory.New(limiter.SystemClock{}), Breaker: breaker.Options{Threshold: 1}})
			for i := 0; i < 5; i++ {
				_, err := f.Allow(tt.ctx(), "k", tt.limit, tt.cost)
				if !errors.Is(err, tt.wantErr) || errors.Is(err, ErrUnavailable) {
					t.Fatalf("err = %v, want %v unchanged", err, tt.wantErr)
				}
			}
			if f.State() != breaker.Closed {
				t.Fatalf("state = %s, want closed", f.State())
			}
		})
	}
}

func TestFallbackStillRejectsInvalidCost(t *testing.T) {
	p := &fakePrimary{}
	p.set(errDown, false)
	f, _ := newLimiter(t, p, Options{Fallback: memory.New(limiter.SystemClock{}), Replicas: 3})
	if _, err := f.Allow(context.Background(), "k", lim, 11); !errors.Is(err, limiter.ErrInvalidCost) {
		t.Fatalf("err = %v, want ErrInvalidCost: the local share must not admit a cost above the real burst", err)
	}
}

func TestLocalShare(t *testing.T) {
	tests := []struct {
		name     string
		l        limiter.Limit
		n        int64
		replicas int64
		want     limiter.Limit
	}{
		{"one replica keeps the limit", lim, 1, 1, lim},
		{"even split", limiter.PerMinute(600, 100), 1, 4, limiter.PerMinute(150, 25)},
		{"rounds up", limiter.PerSecond(10, 10), 1, 3, limiter.PerSecond(4, 4)},
		{"never below one", limiter.PerMinute(1, 1), 1, 50, limiter.PerMinute(1, 1)},
		{"burst covers the cost", limiter.PerMinute(100, 100), 10, 20, limiter.PerMinute(5, 10)},
		{"cost above burst stays invalid", limiter.PerMinute(100, 100), 101, 20, limiter.PerMinute(5, 100)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LocalShare(tt.l, tt.n, tt.replicas)
			if got != tt.want {
				t.Fatalf("LocalShare(%s, %d, %d) = %s, want %s", tt.l, tt.n, tt.replicas, got, tt.want)
			}
			if err := got.Validate(); err != nil {
				t.Fatalf("share of a valid limit is invalid: %v", err)
			}
		})
	}
}

func TestNewValidates(t *testing.T) {
	p := &fakePrimary{}
	for name, opts := range map[string]Options{
		"negative replicas": {Replicas: -1},
		"negative timeout":  {Timeout: -time.Second},
		"bad breaker":       {Breaker: breaker.Options{Threshold: -1}},
	} {
		if _, err := New(p, opts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := New(nil, Options{}); err == nil {
		t.Error("nil primary accepted")
	}
}
