package algo

import (
	"fmt"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// Window is the stored state of a sliding-window counter: the number of
// events counted in two consecutive fixed windows. Stamp is the time of
// the last counted event (Unix microseconds); the window containing it is
// the current one, holding Curr events, and the one before it held Prev.
type Window struct {
	Stamp int64
	Curr  int64
	Prev  int64
}

// WindowSpan returns the window length, in microseconds, that a sliding
// window uses to enforce l: the time Burst events take at the average rate,
// ⌈Burst × Period / Rate⌉. With it, all three algorithms share one meaning
// of a Limit: Rate per Period on average, at most Burst at once. Rounding
// up adds under a microsecond and only ever makes the limit stricter.
//
// The counter compares event counts weighted by microseconds, so on top of
// Limit.Validate it needs Burst × span to stay within 2^53.
func WindowSpan(l limiter.Limit) (int64, error) {
	if err := l.Validate(); err != nil {
		return 0, err
	}
	span := ceilDiv(l.Burst*l.PeriodMicros(), l.Rate)
	if l.Burst > limiter.MaxScaled/span {
		return 0, fmt.Errorf("%w: sliding window of burst %d over %dµs is too large (limit 2^53 event-microseconds)",
			limiter.ErrInvalidLimit, l.Burst, span)
	}
	return span, nil
}

// CheckSlidingWindow validates l and a cost n for the sliding window:
// Limit.Check plus the range bound of WindowSpan.
func CheckSlidingWindow(l limiter.Limit, n int64) error {
	if err := l.Check(n); err != nil {
		return err
	}
	_, err := WindowSpan(l)
	return err
}

// SlidingWindow checks a request of cost n at time now (Unix microseconds)
// against a sliding-window counter and returns the new state and the
// decision. found is false when no state exists yet.
//
// Time is cut into fixed windows of WindowSpan(l). The number of events in
// the sliding window that ends now is estimated as all of the current
// window plus the share of the previous one that the sliding window still
// overlaps, assuming the previous window's events were spread evenly:
//
//	used = Prev × (span - offset) / span + Curr
//
// and a request is granted when used + n <= Burst. All of it is computed
// multiplied by span, in integers, so the estimate itself is exact. The
// assumption is not: when the previous window's events were bunched at its
// end, the true count over the sliding window can exceed Burst. The
// accuracy comparison in the tests measures by how much.
//
// Like the other algorithms, a denial consumes nothing, RetryAfter is the
// exact earliest time the same request succeeds, and a clock behind Stamp
// is treated as Stamp, so skew delays requests but never admits extra ones.
func SlidingWindow(w Window, found bool, l limiter.Limit, now, n int64) (Window, limiter.Decision, error) {
	if err := l.Check(n); err != nil {
		return w, limiter.Decision{}, err
	}
	span, err := WindowSpan(l)
	if err != nil {
		return w, limiter.Decision{}, err
	}

	stored := w
	if !found {
		w = Window{Stamp: now}
	}
	at := max(now, w.Stamp)
	lag := at - now

	// Roll the counts forward to the window containing at.
	start := floorDiv(at, span) * span
	switch last := floorDiv(w.Stamp, span) * span; start {
	case last:
	case last + span:
		w.Prev, w.Curr = w.Curr, 0
	default:
		w.Prev, w.Curr = 0, 0
	}
	off := at - start
	limit := l.Burst * span
	used := w.Prev*(span-off) + w.Curr*span // <= limit

	d := limiter.Decision{Limit: l}
	if n*span <= limit-used {
		d.Allowed = true
		if n > 0 {
			w.Curr += n
			w.Stamp = at
			used += n * span
			stored = w
		}
	} else {
		d.RetryAfter = micros(lag + windowWait(w, l.Burst, span, off, n))
	}
	d.Remaining = (limit - used) / span
	switch {
	case w.Curr > 0: // until the current window has slid out entirely
		d.ResetAfter = micros(lag + 2*span - off)
	case w.Prev > 0:
		d.ResetAfter = micros(lag + span - off)
	}
	return stored, d, nil
}

// windowWait returns how long after offset off a denied request of cost n
// first fits. The estimate only falls with time, so the earliest instant is
// found by solving for it in the current window, then in the next one,
// where the current count becomes the previous one; the window after that
// is empty and always has room.
func windowWait(w Window, burst, span, off, n int64) int64 {
	// Current window: Prev × (span - t) <= (burst - Curr - n) × span.
	if room := (burst - w.Curr - n) * span; room >= 0 && w.Prev > 0 {
		if k := room / w.Prev; k > 0 {
			return span - k - off
		}
	}
	// Next window: Curr × (span - t) <= (burst - n) × span.
	var t int64
	if w.Curr > 0 {
		t = max(0, span-(burst-n)*span/w.Curr)
	}
	if t < span {
		return span - off + t
	}
	return 2*span - off
}

// floorDiv returns ⌊a/b⌋ for any a and b > 0.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}
