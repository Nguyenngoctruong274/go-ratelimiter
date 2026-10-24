// Package middleware rate-limits net/http handlers with any limiter.Limiter
// (in-memory or Redis-backed) and tells clients where they stand through
// standard response headers.
package middleware

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// Options configures a rate-limiting middleware.
type Options struct {
	// Limiter makes the decisions. Required.
	Limiter limiter.Limiter
	// Limit is enforced per key. Required.
	Limit limiter.Limit
	// Key identifies the client. Required; see ByIP, ByHeader, ByUser, FirstOf.
	Key KeyFunc
	// Policy names the limit in the RateLimit headers and namespaces its
	// keys, so two middlewares with different policies never share state.
	// Defaults to "default".
	Policy string
	// Cost returns how many units a request consumes; nil means 1. A cost
	// of 0 lets a request through while still reporting the headers.
	Cost func(r *http.Request) int64
	// FailOpen lets requests through when the limiter itself fails (Redis
	// down). The default is to fail closed with 503: an outage of the
	// limiter must not silently turn into unlimited traffic unless the
	// operator chose that.
	// Wrap the limiter in failover.New for a circuit breaker and a local
	// fallback, so that an outage rarely reaches this choice.
	FailOpen bool
	// LegacyHeaders also sets X-RateLimit-Limit/-Remaining/-Reset, which
	// many existing clients read.
	LegacyHeaders bool
	// Logger receives limiter failures. Defaults to slog.Default().
	Logger *slog.Logger
}

// New returns middleware that enforces opts.Limit per key.
//
// Every limited response carries the RateLimit-Policy and RateLimit fields
// of the IETF draft "RateLimit header fields for HTTP"
// (draft-ietf-httpapi-ratelimit-headers):
//
//	RateLimit-Policy: "default";q=100;w=60
//	RateLimit: "default";r=42;t=17
//
// q/w are the quota per window in seconds, r the remaining quota and t the
// seconds until it is fully restored. A refused request gets 429 with
// Retry-After (RFC 9110) in whole seconds, rounded up so a client that
// obeys it is never refused again for being early.
func New(opts Options) (func(http.Handler) http.Handler, error) {
	if opts.Limiter == nil || opts.Key == nil {
		return nil, errors.New("middleware: Limiter and Key are required")
	}
	if err := opts.Limit.Validate(); err != nil {
		return nil, fmt.Errorf("middleware: %w", err)
	}
	if opts.Policy == "" {
		opts.Policy = "default"
	}
	if !ValidPolicyName(opts.Policy) {
		return nil, fmt.Errorf("middleware: policy name %q must be 1-64 characters of [a-zA-Z0-9_.-]", opts.Policy)
	}
	return Enforce(single{opts}, EnforceOptions{
		FailOpen: opts.FailOpen, LegacyHeaders: opts.LegacyHeaders, Logger: opts.Logger,
	}), nil
}

// single resolves every request to the one policy of Options.
type single struct{ opts Options }

func (s single) Resolve(r *http.Request) ([]Check, error) {
	key, err := s.opts.Key(r)
	if err != nil {
		return nil, err
	}
	cost := int64(1)
	if s.opts.Cost != nil {
		cost = s.opts.Cost(r)
	}
	return []Check{{Policy: s.opts.Policy, Limiter: s.opts.Limiter, Limit: s.opts.Limit, Key: key, Cost: cost}}, nil
}

// Check is one limit a request has to pass.
type Check struct {
	Policy  string // names the limit in headers and namespaces its keys
	Limiter limiter.Limiter
	Limit   limiter.Limit
	Key     string // client key, e.g. from a KeyFunc
	Cost    int64
}

// Resolver decides which limits apply to a request. An error means the
// client could not be identified and the request is refused with 400.
// The policy package implements it from a config file.
type Resolver interface {
	Resolve(r *http.Request) ([]Check, error)
}

// EnforceOptions configures Enforce; see the fields of Options.
type EnforceOptions struct {
	FailOpen      bool
	LegacyHeaders bool
	Logger        *slog.Logger
}

// Enforce returns middleware that runs every Check the resolver returns for
// a request, in order. The request goes through only if all of them allow
// it. Checking stops at the first refusal, so later policies are not
// charged for a request that will not be served; earlier ones have already
// been charged, as with any set of independent limits. Headers list every
// policy that was checked.
func Enforce(res Resolver, opts EnforceOptions) func(http.Handler) http.Handler {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			checks, err := res.Resolve(r)
			if err != nil {
				writeError(w, http.StatusBadRequest, "no_client_key", "cannot identify the client for rate limiting", 0)
				return
			}

			done := make([]checked, 0, len(checks))
			for _, c := range checks {
				d, err := c.Limiter.Allow(r.Context(), c.Policy+":"+c.Key, c.Limit, c.Cost)
				switch {
				case errors.Is(err, limiter.ErrInvalidLimit):
					// The limit is out of range for this limiter's algorithm (GCRA
					// and the sliding window have tighter bounds than
					// Limit.Validate). That is a configuration bug, not an
					// outage, so FailOpen does not apply.
					opts.Logger.ErrorContext(r.Context(), "rate limit misconfigured",
						slog.String("policy", c.Policy), slog.Any("error", err))
					writeError(w, http.StatusInternalServerError, "limiter_misconfigured", "rate limiter misconfigured", 0)
					return
				case errors.Is(err, limiter.ErrInvalidCost):
					writeError(w, http.StatusBadRequest, "invalid_cost",
						fmt.Sprintf("request cost %d exceeds the burst of %d", c.Cost, c.Limit.Burst), 0)
					return
				case err != nil:
					opts.Logger.ErrorContext(r.Context(), "rate limiter failed",
						slog.String("policy", c.Policy), slog.Bool("fail_open", opts.FailOpen), slog.Any("error", err))
					if opts.FailOpen {
						continue // this policy is skipped; the others still apply
					}
					writeError(w, http.StatusServiceUnavailable, "limiter_unavailable", "rate limiter unavailable", time.Second)
					return
				}
				done = append(done, checked{c, d})
				if !d.Allowed {
					setHeaders(w.Header(), done, opts.LegacyHeaders)
					writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded: "+c.Policy, d.RetryAfter)
					return
				}
			}
			if len(done) > 0 {
				setHeaders(w.Header(), done, opts.LegacyHeaders)
			}
			next.ServeHTTP(w, r)
		})
	}
}

type checked struct {
	Check
	d limiter.Decision
}

// setHeaders lists every checked policy. The legacy single-value headers
// describe the tightest one: the refused policy, or the one with the least
// quota left.
func setHeaders(h http.Header, done []checked, legacy bool) {
	var policies, states []string
	tight := done[0]
	for _, c := range done {
		policies = append(policies, policyHeader(c.Policy, c.Limit))
		states = append(states, `"`+c.Policy+`";r=`+strconv.FormatInt(c.d.Remaining, 10)+
			";t="+strconv.FormatInt(ceilSeconds(c.d.ResetAfter), 10))
		if !c.d.Allowed || (tight.d.Allowed && c.d.Remaining < tight.d.Remaining) {
			tight = c
		}
	}
	h.Set("RateLimit-Policy", strings.Join(policies, ", "))
	h.Set("RateLimit", strings.Join(states, ", "))
	if legacy {
		h.Set("X-RateLimit-Limit", strconv.FormatInt(tight.Limit.Rate, 10))
		h.Set("X-RateLimit-Remaining", strconv.FormatInt(tight.d.Remaining, 10))
		h.Set("X-RateLimit-Reset", strconv.FormatInt(ceilSeconds(tight.d.ResetAfter), 10))
	}
}

// policyHeader describes the limit as quota q per window w seconds. A rate
// over a sub-second period is scaled to one second, the smallest window the
// header can express; any remainder is rounded down so the advertised quota
// is never more generous than the real one.
func policyHeader(policy string, l limiter.Limit) string {
	q, w := l.Rate, ceilSeconds(l.Period)
	if l.Period < time.Second {
		// q = ⌊Rate × 10^6 / period⌋, split so it cannot overflow:
		// ⌊R·M/p⌋ = ⌊R/p⌋·M + ⌊(R mod p)·M/p⌋, saturating at MaxInt64.
		const million = int64(time.Second / time.Microsecond)
		per := l.PeriodMicros()
		if whole := l.Rate / per; whole > (math.MaxInt64-million)/million {
			q = math.MaxInt64
		} else {
			q = whole*million + (l.Rate%per)*million/per
		}
	}
	return `"` + policy + `";q=` + strconv.FormatInt(q, 10) + ";w=" + strconv.FormatInt(w, 10)
}

// ceilSeconds rounds d up to whole seconds.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// ValidPolicyName reports whether s can name a policy: 1-64 characters of
// [a-zA-Z0-9_.-], safe inside a header string and a Redis key.
func ValidPolicyName(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
		if !ok {
			return false
		}
	}
	return true
}

type errorBody struct {
	Error struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		RetryAfterMS int64  `json:"retry_after_ms,omitempty"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string, retryAfter time.Duration) {
	var body errorBody
	body.Error.Code, body.Error.Message = code, msg
	if retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.FormatInt(ceilSeconds(retryAfter), 10))
		body.Error.RetryAfterMS = int64((retryAfter + time.Millisecond - 1) / time.Millisecond)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
