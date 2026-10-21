// Package algo holds the rate-limiting algorithms as pure functions of
// (state, limit, now, cost). They do no I/O and read no clock, which makes
// them easy to test exhaustively and lets them serve as the reference that
// the Redis Lua scripts are checked against.
package algo

import (
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// Bucket is the stored state of one token bucket.
//
// Level counts tokens multiplied by the period in microseconds
// ("token-microseconds"). In those units a refill of Rate tokens per Period
// adds exactly Rate per elapsed microsecond, so no fractions ever appear and
// no precision is lost, however small the rate. Stamp is the time of the
// last refill, in microseconds since the Unix epoch.
type Bucket struct {
	Level int64
	Stamp int64
}

// TokenBucket checks a request of cost n at time now (Unix microseconds)
// against bucket b and returns the updated bucket and the decision. found is
// false when no state exists yet; a new bucket starts full.
//
// A denied request consumes nothing: callers that retry after RetryAfter get
// exactly the tokens they were promised. If the clock goes backwards (another
// instance with a slower clock wrote the state last) the bucket is simply
// not refilled until time catches up, so skew can delay tokens but never
// mint extra ones.
func TokenBucket(b Bucket, found bool, l limiter.Limit, now, n int64) (Bucket, limiter.Decision, error) {
	if err := l.Check(n); err != nil {
		return b, limiter.Decision{}, err
	}

	per := l.PeriodMicros()
	capacity := l.Burst * per
	if !found {
		b = Bucket{Level: capacity, Stamp: now}
	}
	b = refill(b, l.Rate, capacity, now)

	// Waits are measured on the caller's clock: when it lags behind the
	// stamp, refilling only starts once it reaches the stamp.
	lag := max(0, b.Stamp-now)

	d := limiter.Decision{Limit: l}
	if cost := n * per; b.Level >= cost {
		b.Level -= cost
		d.Allowed = true
	} else {
		d.RetryAfter = micros(lag + ceilDiv(cost-b.Level, l.Rate))
	}
	d.Remaining = b.Level / per
	if b.Level < capacity {
		d.ResetAfter = micros(lag + ceilDiv(capacity-b.Level, l.Rate))
	}
	return b, d, nil
}

// IsFull reports whether bucket b has refilled completely by now. A full
// bucket is indistinguishable from a missing one, so stores can drop it.
func IsFull(b Bucket, l limiter.Limit, now int64) bool {
	capacity := l.Burst * l.PeriodMicros()
	return refill(b, l.Rate, capacity, now).Level == capacity
}

// refill adds the tokens earned since b.Stamp, capped at capacity.
// Comparing elapsed time against the time needed to fill up, instead of
// multiplying first, keeps elapsed×rate from overflowing after long idles.
func refill(b Bucket, rate, capacity, now int64) Bucket {
	if now <= b.Stamp {
		return b
	}
	elapsed := now - b.Stamp
	if elapsed >= ceilDiv(capacity-b.Level, rate) {
		b.Level = capacity
	} else {
		b.Level += elapsed * rate
	}
	b.Stamp = now
	return b
}

// ceilDiv returns ⌈a/b⌉ for a ≥ 0, b > 0.
func ceilDiv(a, b int64) int64 {
	if a <= 0 {
		return 0
	}
	return (a + b - 1) / b
}

func micros(us int64) time.Duration { return time.Duration(us) * time.Microsecond }
