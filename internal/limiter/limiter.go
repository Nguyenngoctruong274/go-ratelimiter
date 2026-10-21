// Package limiter defines what every rate limiter in this module promises:
// a Limit to enforce, a Decision describing the outcome, and the Limiter
// interface that the in-memory and Redis implementations both satisfy.
package limiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidLimit reports a Limit that cannot be enforced exactly.
	ErrInvalidLimit = errors.New("invalid limit")
	// ErrInvalidCost reports a negative cost, or a cost larger than the
	// burst, which no amount of waiting could ever satisfy.
	ErrInvalidCost = errors.New("invalid cost")
)

// MaxScaled bounds Burst × Period (in microseconds) plus Rate. Bucket state
// is kept as an integer number of "token-microseconds"; staying below 2^53
// keeps every intermediate value exact in a float64, which is the only
// number type Lua has inside Redis. The Go and Lua versions can therefore
// compute bit-for-bit identical decisions.
const MaxScaled = 1 << 53

// Limit allows Rate events per Period on average, with bursts of up to Burst
// events after a quiet period. Period must be a whole number of microseconds,
// the resolution of the limiter's clock.
type Limit struct {
	Rate   int64
	Period time.Duration
	Burst  int64
}

// PerSecond is shorthand for a limit of rate events per second.
func PerSecond(rate, burst int64) Limit {
	return Limit{Rate: rate, Period: time.Second, Burst: burst}
}

// PerMinute is shorthand for a limit of rate events per minute.
func PerMinute(rate, burst int64) Limit {
	return Limit{Rate: rate, Period: time.Minute, Burst: burst}
}

// PeriodMicros returns the period in microseconds.
func (l Limit) PeriodMicros() int64 { return l.Period.Microseconds() }

// Validate reports whether the limit can be enforced exactly.
func (l Limit) Validate() error {
	switch {
	case l.Rate <= 0:
		return fmt.Errorf("%w: rate must be positive, got %d", ErrInvalidLimit, l.Rate)
	case l.Burst <= 0:
		return fmt.Errorf("%w: burst must be positive, got %d", ErrInvalidLimit, l.Burst)
	case l.Period < time.Microsecond || l.Period%time.Microsecond != 0:
		return fmt.Errorf("%w: period must be a positive whole number of microseconds, got %s", ErrInvalidLimit, l.Period)
	}
	per := l.PeriodMicros()
	if l.Burst > (MaxScaled-l.Rate)/per {
		return fmt.Errorf("%w: burst %d × period %s is too large (limit 2^53 token-microseconds)", ErrInvalidLimit, l.Burst, l.Period)
	}
	return nil
}

// Check validates l together with a cost n. A cost is valid when
// 0 <= n <= Burst: a larger one could never be granted, however long the
// caller waited.
func (l Limit) Check(n int64) error {
	if err := l.Validate(); err != nil {
		return err
	}
	if n < 0 || n > l.Burst {
		return fmt.Errorf("%w: cost %d with burst %d", ErrInvalidCost, n, l.Burst)
	}
	return nil
}

func (l Limit) String() string {
	return fmt.Sprintf("%d/%s burst %d", l.Rate, l.Period, l.Burst)
}

// Decision is the outcome of one check.
type Decision struct {
	Allowed bool
	Limit   Limit
	// Remaining is how many more unit-cost events would be allowed right now.
	Remaining int64
	// RetryAfter is how long to wait before the same request can succeed.
	// It is zero when the request was allowed.
	RetryAfter time.Duration
	// ResetAfter is how long until the limiter is back to its full burst if
	// nothing else arrives.
	ResetAfter time.Duration
}

// Limiter decides whether an event of cost n for key may proceed under l.
// A cost of 0 inspects the current state without consuming anything.
// State is kept per (key, limit) pair, so changing a key's limit starts it
// with a fresh, full bucket instead of misreading the old state.
type Limiter interface {
	Allow(ctx context.Context, key string, l Limit, n int64) (Decision, error)
}

// Clock is the time source of a limiter; tests substitute a manual clock.
type Clock interface {
	Now() time.Time
}

// SystemClock reads the wall clock.
type SystemClock struct{}

// Now returns time.Now().
func (SystemClock) Now() time.Time { return time.Now() }
