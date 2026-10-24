// Package breaker is a circuit breaker for calls to a dependency that can
// fail as a whole, such as the Redis behind a rate limiter. After a run of
// consecutive failures it stops calling the dependency for a cooldown, then
// lets a single probe through to find out whether it has recovered.
package breaker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// ErrOpen is returned for calls the breaker refuses without trying.
var ErrOpen = errors.New("circuit breaker open")

// State is the position of the breaker.
type State int

// The states of a breaker. Closed lets every call through; Open lets none
// through until the cooldown has passed; HalfOpen lets one probe through
// and decides from its result.
const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("State(%d)", int(s))
	}
}

// Result is the outcome of a call, as reported to Done.
type Result int

// Success and Failure say whether the dependency worked. Ignored is for
// calls that ended without telling anything about the dependency, such as
// a caller that gave up: they neither trip nor close the breaker.
const (
	Success Result = iota
	Failure
	Ignored
)

// Options configures a Breaker.
type Options struct {
	// Threshold is how many consecutive failures open the breaker.
	// Defaults to 5.
	Threshold int
	// Cooldown is how long the breaker stays open before it lets a probe
	// through. Defaults to 5s.
	Cooldown time.Duration
	// Clock defaults to limiter.SystemClock{}.
	Clock limiter.Clock
	// OnChange, if set, is called after every state change, outside the
	// breaker's lock (so it may call State), possibly from several
	// goroutines at once.
	OnChange func(from, to State)
}

// Breaker is safe for concurrent use.
type Breaker struct {
	threshold int
	cooldown  time.Duration
	clock     limiter.Clock
	onChange  func(from, to State)

	mu       sync.Mutex
	state    State
	failures int       // consecutive failures while closed
	openedAt time.Time // when the breaker last opened
	probing  bool      // a half-open probe is in flight
	// gen changes with every state change. A call reports its result with
	// the generation it started in, so a slow call that began before the
	// breaker opened cannot close it, and a stale failure cannot reopen it.
	gen uint64
}

// New returns a closed breaker.
func New(opts Options) (*Breaker, error) {
	if opts.Threshold == 0 {
		opts.Threshold = 5
	}
	if opts.Cooldown == 0 {
		opts.Cooldown = 5 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = limiter.SystemClock{}
	}
	if opts.Threshold < 0 || opts.Cooldown < 0 {
		return nil, fmt.Errorf("breaker: threshold %d and cooldown %s must not be negative", opts.Threshold, opts.Cooldown)
	}
	return &Breaker{threshold: opts.Threshold, cooldown: opts.Cooldown, clock: opts.Clock, onChange: opts.OnChange}, nil
}

// Ticket identifies a call the breaker let through.
type Ticket struct {
	gen   uint64
	probe bool
}

// Allow reports whether a call may go ahead. If it may, the caller must
// pass the ticket to Done exactly once when the call ends.
func (b *Breaker) Allow() (Ticket, bool) {
	b.mu.Lock()
	from := b.state
	t, ok := b.allowLocked()
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
	return t, ok
}

func (b *Breaker) allowLocked() (Ticket, bool) {
	switch b.state {
	case Closed:
		return Ticket{gen: b.gen}, true
	case Open:
		if b.clock.Now().Sub(b.openedAt) < b.cooldown {
			return Ticket{}, false
		}
		b.setLocked(HalfOpen)
	}
	// Half-open: exactly one probe at a time.
	if b.probing {
		return Ticket{}, false
	}
	b.probing = true
	return Ticket{gen: b.gen, probe: true}, true
}

// Done records the result of a call that Allow let through.
func (b *Breaker) Done(t Ticket, r Result) {
	b.mu.Lock()
	from := b.state
	b.doneLocked(t, r)
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

func (b *Breaker) doneLocked(t Ticket, r Result) {
	if t.gen != b.gen {
		return // the state changed while the call was running
	}
	if t.probe {
		b.probing = false
		switch r {
		case Success:
			b.setLocked(Closed)
		case Failure:
			b.setLocked(Open)
		}
		return
	}
	switch r {
	case Success:
		b.failures = 0
	case Failure:
		b.failures++
		if b.failures >= b.threshold {
			b.setLocked(Open)
		}
	}
}

func (b *Breaker) setLocked(s State) {
	b.state = s
	b.gen++
	b.failures = 0
	b.probing = false
	if s == Open {
		b.openedAt = b.clock.Now()
	}
}

func (b *Breaker) notify(from, to State) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}

// State returns the current state. An open breaker whose cooldown has
// passed still reports Open until the next call probes the dependency.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}
