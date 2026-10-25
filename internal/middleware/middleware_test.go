package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
}

func byRemote(r *http.Request) (string, error) { return "c:" + r.RemoteAddr, nil }

func newHandler(t *testing.T, opts Options) http.Handler {
	t.Helper()
	if opts.Key == nil {
		opts.Key = byRemote
	}
	if opts.Logger == nil {
		opts.Logger = quiet
	}
	m, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return m(okHandler())
}

func do(h http.Handler, remote string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = remote
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec
}

func TestMiddlewareAllowsThenLimitsThenRecovers(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_760_000_000, 0)}
	h := newHandler(t, Options{
		Limiter: memory.New(clock),
		Limit:   limiter.PerMinute(6, 3), // a token every 10s, bursts of 3
		Policy:  "api",
	})

	wantRemaining := []string{"2", "1", "0"}
	for i, want := range wantRemaining {
		rec := do(h, "a")
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Fatalf("request %d: %d %q", i, rec.Code, rec.Body)
		}
		if got := rec.Header().Get("RateLimit"); got != `"api";r=`+want+";t="+strconv.Itoa(10*(i+1)) {
			t.Fatalf("request %d: RateLimit = %q", i, got)
		}
		if got := rec.Header().Get("RateLimit-Policy"); got != `"api";q=6;w=60` {
			t.Fatalf("RateLimit-Policy = %q", got)
		}
	}

	clock.Advance(500 * time.Millisecond)
	rec := do(h, "a")
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th request: %d", rec.Code)
	}
	// 9.5s left: Retry-After rounds up so an obedient client is never early.
	if got := rec.Header().Get("Retry-After"); got != "10" {
		t.Fatalf("Retry-After = %q, want 10", got)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "rate_limited" || body.Error.RetryAfterMS != 9500 {
		t.Fatalf("body = %+v", body)
	}
	if got := rec.Header().Get("RateLimit"); got != `"api";r=0;t=30` {
		t.Fatalf("RateLimit on 429 = %q", got)
	}

	if rec := do(h, "b"); rec.Code != http.StatusOK {
		t.Fatalf("another client was limited: %d", rec.Code)
	}

	clock.Advance(9500 * time.Millisecond)
	if rec := do(h, "a"); rec.Code != http.StatusOK {
		t.Fatalf("after Retry-After: %d", rec.Code)
	}
}

func TestMiddlewareCostAndLegacyHeaders(t *testing.T) {
	clock := &manualClock{now: time.Unix(1_760_000_000, 0)}
	h := newHandler(t, Options{
		Limiter:       memory.New(clock),
		Limit:         limiter.PerSecond(10, 10),
		LegacyHeaders: true,
		Cost: func(r *http.Request) int64 {
			n, _ := strconv.ParseInt(r.URL.Query().Get("n"), 10, 64)
			return n
		},
	})

	call := func(q string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/?n="+q, nil)
		r.RemoteAddr = "a"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		return rec
	}

	rec := call("4")
	if rec.Code != 200 || rec.Header().Get("X-RateLimit-Remaining") != "6" ||
		rec.Header().Get("X-RateLimit-Limit") != "10" || rec.Header().Get("X-RateLimit-Reset") != "1" {
		t.Fatalf("cost 4: %d %v", rec.Code, rec.Header())
	}
	if rec := call("0"); rec.Code != 200 || rec.Header().Get("X-RateLimit-Remaining") != "6" {
		t.Fatalf("cost 0 must not consume: %v", rec.Header())
	}
	if rec := call("11"); rec.Code != http.StatusBadRequest {
		t.Fatalf("cost above burst: %d %s", rec.Code, rec.Body)
	}
	if rec := call("7"); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("cost 7 with 6 left: %d", rec.Code)
	}
}

// failingLimiter returns a fixed error.
type failingLimiter struct{ err error }

func (f failingLimiter) Allow(context.Context, string, limiter.Limit, int64) (limiter.Decision, error) {
	return limiter.Decision{}, f.err
}

func TestMiddlewareLimiterFailures(t *testing.T) {
	down := failingLimiter{err: errors.New("redis: connection refused")}
	misconfigured := failingLimiter{err: limiter.ErrInvalidLimit}
	tests := []struct {
		name     string
		lim      limiter.Limiter
		failOpen bool
		want     int
	}{
		{"fail closed by default", down, false, http.StatusServiceUnavailable},
		{"fail open when chosen", down, true, http.StatusOK},
		{"misconfiguration is never failed open", misconfigured, true, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHandler(t, Options{Limiter: tt.lim, Limit: limiter.PerSecond(1, 1), FailOpen: tt.failOpen})
			rec := do(h, "a")
			if rec.Code != tt.want {
				t.Fatalf("status %d, want %d", rec.Code, tt.want)
			}
			if rec.Header().Get("RateLimit") != "" {
				t.Fatal("headers sent without a decision")
			}
		})
	}
}

// ctxLimiter fails the way a Redis call does when its context ends.
type ctxLimiter struct{}

func (ctxLimiter) Allow(ctx context.Context, _ string, _ limiter.Limit, _ int64) (limiter.Decision, error) {
	<-ctx.Done()
	return limiter.Decision{}, ctx.Err()
}

// TestMiddlewareClientGone: when the client disconnects during the check,
// the protected handler must not run, even with FailOpen, and nothing is
// logged as a limiter failure.
func TestMiddlewareClientGone(t *testing.T) {
	var logs strings.Builder
	var served atomic.Bool
	m, err := New(Options{
		Limiter: ctxLimiter{}, Limit: limiter.PerSecond(1, 1), Key: byRemote, FailOpen: true,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := m(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { served.Store(true) }))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	r.RemoteAddr = "a"
	h.ServeHTTP(httptest.NewRecorder(), r)

	if served.Load() {
		t.Fatal("handler ran for a client that had gone")
	}
	if strings.Contains(logs.String(), "level=ERROR") {
		t.Fatalf("logged as a failure:\n%s", logs.String())
	}
}

func TestMiddlewareRejectsUnidentifiableClients(t *testing.T) {
	h := newHandler(t, Options{
		Limiter: memory.New(limiter.SystemClock{}),
		Limit:   limiter.PerSecond(1, 1),
		Key:     ByHeader("X-API-Key"),
	})
	if rec := do(h, "a"); rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
}

func TestPoliciesDoNotShareState(t *testing.T) {
	lim := memory.New(&manualClock{now: time.Unix(0, 0)})
	l := limiter.PerMinute(1, 1)
	a := newHandler(t, Options{Limiter: lim, Limit: l, Policy: "search"})
	b := newHandler(t, Options{Limiter: lim, Limit: l, Policy: "upload"})
	if do(a, "x").Code != 200 || do(b, "x").Code != 200 {
		t.Fatal("one policy consumed the other's quota")
	}
	if do(a, "x").Code != http.StatusTooManyRequests {
		t.Fatal("policy search not enforced")
	}
}

func TestNewValidatesOptions(t *testing.T) {
	lim := memory.New(limiter.SystemClock{})
	tests := []struct {
		name string
		opts Options
	}{
		{"no limiter", Options{Limit: limiter.PerSecond(1, 1), Key: byRemote}},
		{"no key", Options{Limiter: lim, Limit: limiter.PerSecond(1, 1)}},
		{"bad limit", Options{Limiter: lim, Key: byRemote}},
		{"bad policy name", Options{Limiter: lim, Limit: limiter.PerSecond(1, 1), Key: byRemote, Policy: `a"b`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.opts); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestPolicyHeader(t *testing.T) {
	tests := []struct {
		limit limiter.Limit
		want  string
	}{
		{limiter.PerSecond(5, 10), `"p";q=5;w=1`},
		{limiter.PerMinute(100, 10), `"p";q=100;w=60`},
		{limiter.Limit{Rate: 3, Period: 100 * time.Millisecond, Burst: 3}, `"p";q=30;w=1`},
		{limiter.Limit{Rate: 2, Period: 300 * time.Millisecond, Burst: 2}, `"p";q=6;w=1`}, // 6.67 rounded down
		{limiter.Limit{Rate: 1, Period: 1500 * time.Millisecond, Burst: 1}, `"p";q=1;w=2`},
		{limiter.Limit{Rate: limiter.MaxScaled - 1, Period: time.Microsecond, Burst: 1}, `"p";q=9223372036854775807;w=1`},
	}
	for _, tt := range tests {
		if got := policyHeader("p", tt.limit); got != tt.want {
			t.Errorf("%s: %q, want %q", tt.limit, got, tt.want)
		}
	}
}

func BenchmarkMiddleware(b *testing.B) {
	m, err := New(Options{Limiter: memory.New(limiter.SystemClock{}), Limit: limiter.PerSecond(1_000_000, 1_000), Key: byRemote, Logger: quiet})
	if err != nil {
		b.Fatal(err)
	}
	h := m(okHandler())
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "203.0.113.7:1"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
}

// countingLimiter answers with a fixed decision and counts calls.
type countingLimiter struct {
	allow bool
	calls int
}

func (c *countingLimiter) Allow(_ context.Context, _ string, l limiter.Limit, _ int64) (limiter.Decision, error) {
	c.calls++
	d := limiter.Decision{Allowed: c.allow, Limit: l, Remaining: 3}
	if !c.allow {
		d.RetryAfter = 2 * time.Second
	}
	return d, nil
}

type staticResolver []Check

func (s staticResolver) Resolve(*http.Request) ([]Check, error) { return s, nil }

func TestEnforceStopsAtTheFirstRefusal(t *testing.T) {
	first, refuses, never := &countingLimiter{allow: true}, &countingLimiter{}, &countingLimiter{allow: true}
	l := limiter.PerSecond(1, 5)
	h := Enforce(staticResolver{
		{Policy: "a", Limiter: first, Limit: l, Key: "k", Cost: 1},
		{Policy: "b", Limiter: refuses, Limit: l, Key: "k", Cost: 1},
		{Policy: "c", Limiter: never, Limit: l, Key: "k", Cost: 1},
	}, EnforceOptions{Logger: quiet})(okHandler())

	rec := do(h, "x")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("status %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	if first.calls != 1 || refuses.calls != 1 || never.calls != 0 {
		t.Fatalf("calls a=%d b=%d c=%d: a policy after the refusal was charged", first.calls, refuses.calls, never.calls)
	}
	if got := rec.Header().Get("RateLimit"); got != `"a";r=3;t=0, "b";r=3;t=0` {
		t.Fatalf("RateLimit = %q", got)
	}
}

func TestEnforceFailOpenSkipsOnlyTheFailingPolicy(t *testing.T) {
	ok := &countingLimiter{allow: true}
	h := Enforce(staticResolver{
		{Policy: "down", Limiter: failingLimiter{err: errors.New("redis: timeout")}, Limit: limiter.PerSecond(1, 1), Key: "k", Cost: 1},
		{Policy: "local", Limiter: ok, Limit: limiter.PerSecond(1, 1), Key: "k", Cost: 1},
	}, EnforceOptions{FailOpen: true, Logger: quiet})(okHandler())

	rec := do(h, "x")
	if rec.Code != http.StatusOK || ok.calls != 1 || rec.Header().Get("RateLimit") != `"local";r=3;t=0` {
		t.Fatalf("status %d, calls %d, RateLimit %q", rec.Code, ok.calls, rec.Header().Get("RateLimit"))
	}
}

func TestEnforceWithNoMatchingPolicy(t *testing.T) {
	rec := do(Enforce(staticResolver{}, EnforceOptions{})(okHandler()), "x")
	if rec.Code != http.StatusOK || rec.Header().Get("RateLimit") != "" {
		t.Fatalf("status %d, headers %v", rec.Code, rec.Header())
	}
}
