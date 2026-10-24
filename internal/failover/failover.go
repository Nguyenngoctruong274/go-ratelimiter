// Package failover keeps rate limiting decisions fast and predictable when
// the shared limiter (Redis) is slow or down. It wraps the primary limiter
// with a per-check timeout and a circuit breaker, and decides from a local
// fallback, or fails with ErrUnavailable, while the primary is out.
package failover

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// ErrUnavailable reports that no decision could be made: the primary failed
// or its breaker is open, and there is no fallback. Callers choose what that
// means; the middleware fails closed (503) or, with FailOpen, lets the
// request through.
var ErrUnavailable = errors.New("rate limiter unavailable")

// Options configures a Limiter.
type Options struct {
	// Fallback decides while the primary is unavailable, typically a
	// memory.Limiter. Nil means fail: Allow returns ErrUnavailable.
	Fallback limiter.Limiter
	// Replicas is the number of instances that share the primary. While
	// they fall back, each one enforces 1/Replicas of every limit locally,
	// so together they grant about one limit rather than Replicas of them.
	// Defaults to 1.
	Replicas int
	// Timeout bounds each check on the primary; a check that takes longer
	// counts as a failure. Zero leaves it to the caller's context and the
	// Redis client's own timeouts.
	Timeout time.Duration
	// Breaker configures the circuit breaker in front of the primary. Its
	// OnChange, if set, is called in addition to logging the change.
	Breaker breaker.Options
	// Logger receives breaker state changes. Defaults to slog.Default().
	Logger *slog.Logger
}

// Limiter is a limiter.Limiter that survives the loss of its primary.
type Limiter struct {
	primary  limiter.Limiter
	fallback limiter.Limiter
	replicas int64
	timeout  time.Duration
	breaker  *breaker.Breaker
}

var _ limiter.Limiter = (*Limiter)(nil)

// New wraps primary.
func New(primary limiter.Limiter, opts Options) (*Limiter, error) {
	if primary == nil {
		return nil, errors.New("failover: primary limiter is required")
	}
	if opts.Replicas == 0 {
		opts.Replicas = 1
	}
	if opts.Replicas < 0 || opts.Timeout < 0 {
		return nil, fmt.Errorf("failover: replicas %d and timeout %s must not be negative", opts.Replicas, opts.Timeout)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	logger, onChange := opts.Logger, opts.Breaker.OnChange
	opts.Breaker.OnChange = func(from, to breaker.State) {
		level := slog.LevelInfo
		if to == breaker.Open {
			level = slog.LevelWarn
		}
		logger.Log(context.Background(), level, "rate limiter breaker changed state",
			slog.String("from", from.String()), slog.String("to", to.String()),
			slog.Bool("fallback", opts.Fallback != nil))
		if onChange != nil {
			onChange(from, to)
		}
	}
	b, err := breaker.New(opts.Breaker)
	if err != nil {
		return nil, fmt.Errorf("failover: %w", err)
	}
	return &Limiter{
		primary: primary, fallback: opts.Fallback, replicas: int64(opts.Replicas),
		timeout: opts.Timeout, breaker: b,
	}, nil
}

// Allow implements limiter.Limiter. While the breaker is closed it asks the
// primary. When the primary fails, or while the breaker is open, it asks
// the fallback under the local share of l; without a fallback it returns
// an error wrapping ErrUnavailable.
//
// Invalid limits and costs are returned as they are and do not count as
// failures of the primary: they are the caller's bug, not an outage. Nor
// does a check abandoned by the caller's own context.
func (f *Limiter) Allow(ctx context.Context, key string, l limiter.Limit, n int64) (limiter.Decision, error) {
	t, ok := f.breaker.Allow()
	if !ok {
		return f.degrade(ctx, key, l, n, breaker.ErrOpen)
	}

	pctx, cancel := ctx, context.CancelFunc(func() {})
	if f.timeout > 0 {
		pctx, cancel = context.WithTimeout(ctx, f.timeout)
	}
	d, err := f.primary.Allow(pctx, key, l, n)
	cancel()

	switch {
	case err == nil:
		f.breaker.Done(t, breaker.Success)
		return d, nil
	case errors.Is(err, limiter.ErrInvalidLimit), errors.Is(err, limiter.ErrInvalidCost):
		f.breaker.Done(t, breaker.Ignored)
		return limiter.Decision{}, err
	case ctx.Err() != nil:
		f.breaker.Done(t, breaker.Ignored)
		return limiter.Decision{}, err
	default:
		f.breaker.Done(t, breaker.Failure)
		return f.degrade(ctx, key, l, n, err)
	}
}

func (f *Limiter) degrade(ctx context.Context, key string, l limiter.Limit, n int64, cause error) (limiter.Decision, error) {
	if f.fallback == nil {
		return limiter.Decision{}, fmt.Errorf("%w: %w", ErrUnavailable, cause)
	}
	d, err := f.fallback.Allow(ctx, key, LocalShare(l, n, f.replicas), n)
	switch {
	case err == nil:
		return d, nil
	case errors.Is(err, limiter.ErrInvalidLimit), errors.Is(err, limiter.ErrInvalidCost):
		return limiter.Decision{}, err
	default:
		return limiter.Decision{}, fmt.Errorf("%w: fallback after %w: %w", ErrUnavailable, cause, err)
	}
}

// State reports the breaker's state: anything but Closed means decisions
// may currently come from the fallback.
func (f *Limiter) State() breaker.State { return f.breaker.State() }

// LocalShare returns the part of l one of replicas instances enforces on its
// own. Rate and burst are divided and rounded up, so every instance keeps
// some quota, and the burst never drops below a cost n that fits l, so such
// a request is not refused as impossible. The share is never larger than l,
// so it is valid whenever l is, and a cost l rejects is rejected by it too.
func LocalShare(l limiter.Limit, n, replicas int64) limiter.Limit {
	if replicas <= 1 {
		return l
	}
	s := l
	s.Rate = ceilDiv(l.Rate, replicas)
	s.Burst = max(ceilDiv(l.Burst, replicas), min(n, l.Burst))
	return s
}

func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return a
	}
	return (a-1)/b + 1
}
