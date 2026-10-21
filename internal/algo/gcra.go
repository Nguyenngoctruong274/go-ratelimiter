package algo

import (
	"fmt"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// MaxGCRARefill bounds the time a GCRA limit takes to refill its whole
// burst, Burst × Period / Rate, at 2^52µs (about 142 years). The TAT lies up
// to that far ahead of now, and Unix time in microseconds stays below 2^52
// until 2112, so the TAT stays below 2^53 where Lua's numbers are exact.
const MaxGCRARefill = 1 << 52

// CheckGCRA validates l and a cost n for GCRA: Limit.Check plus the
// MaxGCRARefill bound.
func CheckGCRA(l limiter.Limit, n int64) error {
	if err := l.Check(n); err != nil {
		return err
	}
	if refill := ceilDiv(l.Burst*l.PeriodMicros(), l.Rate); refill > MaxGCRARefill {
		return fmt.Errorf("%w: GCRA refill time %dµs is too long (limit 2^52µs)", limiter.ErrInvalidLimit, refill)
	}
	return nil
}

// TAT is the stored state of a GCRA limiter: the theoretical arrival time,
// the moment the limiter is back to its full burst. A request of cost n
// pushes it n × Period/Rate further out, and is granted as long as that
// keeps it within Burst × Period/Rate of now.
//
// Period/Rate is rarely a whole number of microseconds (3 per 7ms is
// 2333⅓µs), so the TAT is kept as the exact fraction Micros + Frac/Rate,
// with 0 <= Frac < Rate. Rounding it instead would drift the limit by up to
// a microsecond per request.
type TAT struct {
	Micros int64
	Frac   int64
}

// GCRA checks a request of cost n at time now (Unix microseconds) against
// the generic cell rate algorithm and returns the new TAT and the decision.
// found is false when no state exists yet.
//
// GCRA is a token bucket described by a single timestamp instead of a level
// and a refill time: the debt (TAT - now) × Rate, in token-microseconds, is
// exactly Burst × Period minus the bucket's level. With a monotonic clock
// it therefore makes the same decisions as TokenBucket, which the tests
// check call by call. Two things differ, and they are why stores use it:
//
//   - Only a granted request changes the state. Denials and peeks are pure
//     reads, so a store under attack writes nothing for the requests it
//     turns away.
//   - The state expires on its own: once now passes the TAT it equals no
//     state at all, so a store can expire the key at the TAT.
//
// A clock behind the one that wrote the state sees more debt, not less:
// skew can delay tokens but never mint them.
func GCRA(t TAT, found bool, l limiter.Limit, now, n int64) (TAT, limiter.Decision, error) {
	if err := CheckGCRA(l, n); err != nil {
		return t, limiter.Decision{}, err
	}

	per := l.PeriodMicros()
	capacity := l.Burst * per
	cost := n * per
	d := limiter.Decision{Limit: l}

	// The TAT is q whole microseconds and frac/Rate ahead of now. A TAT in
	// the past is no debt; Frac/Rate < 1, so Micros < now means TAT < now.
	var q, frac int64
	if found && t.Micros >= now {
		q, frac = t.Micros-now, t.Frac
	}

	// Debt above capacity can only come from a lagging clock. Decide it
	// without computing q × Rate, which could leave the exact range when
	// the lag is large.
	if q > (capacity-frac)/l.Rate {
		d.Allowed = n == 0
		if !d.Allowed {
			d.RetryAfter = micros(q + ceilDivSigned(frac+cost-capacity, l.Rate))
		}
		d.ResetAfter = micros(q + ceilDiv(frac, l.Rate))
		return t, d, nil
	}

	debt := q*l.Rate + frac // <= capacity
	if debt <= capacity-cost {
		d.Allowed = true
		if n > 0 {
			debt += cost
			t = TAT{Micros: now + debt/l.Rate, Frac: debt % l.Rate}
		}
	} else {
		d.RetryAfter = micros(ceilDiv(debt-(capacity-cost), l.Rate))
	}
	d.Remaining = (capacity - debt) / per
	d.ResetAfter = micros(ceilDiv(debt, l.Rate))
	return t, d, nil
}

// ceilDivSigned returns ⌈a/b⌉ for any a and b > 0.
func ceilDivSigned(a, b int64) int64 {
	if a > 0 {
		return ceilDiv(a, b)
	}
	return -(-a / b)
}
