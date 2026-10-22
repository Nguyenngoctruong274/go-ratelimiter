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
	if !validPolicyName(opts.Policy) {
		return nil, fmt.Errorf("middleware: policy name %q must be 1-64 characters of [a-zA-Z0-9_.-]", opts.Policy)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	m := &mw{opts: opts, policyHeader: policyHeader(opts.Policy, opts.Limit)}
	return m.wrap, nil
}

type mw struct {
	opts         Options
	policyHeader string
}

func (m *mw) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, err := m.opts.Key(r)
		if err != nil {
			writeError(w, http.StatusBadRequest, "no_client_key", "cannot identify the client for rate limiting", 0)
			return
		}
		cost := int64(1)
		if m.opts.Cost != nil {
			cost = m.opts.Cost(r)
		}

		d, err := m.opts.Limiter.Allow(r.Context(), m.opts.Policy+":"+key, m.opts.Limit, cost)
		switch {
		case errors.Is(err, limiter.ErrInvalidLimit):
			// The limit is out of range for this limiter's algorithm (GCRA and
			// the sliding window have tighter bounds than Limit.Validate).
			// That is a configuration bug, not an outage, so FailOpen does
			// not apply.
			m.opts.Logger.ErrorContext(r.Context(), "rate limit misconfigured",
				slog.String("policy", m.opts.Policy), slog.Any("error", err))
			writeError(w, http.StatusInternalServerError, "limiter_misconfigured", "rate limiter misconfigured", 0)
			return
		case errors.Is(err, limiter.ErrInvalidCost):
			writeError(w, http.StatusBadRequest, "invalid_cost",
				fmt.Sprintf("request cost %d exceeds the burst of %d", cost, m.opts.Limit.Burst), 0)
			return
		case err != nil:
			m.opts.Logger.ErrorContext(r.Context(), "rate limiter failed",
				slog.String("policy", m.opts.Policy), slog.Bool("fail_open", m.opts.FailOpen), slog.Any("error", err))
			if m.opts.FailOpen {
				next.ServeHTTP(w, r)
				return
			}
			writeError(w, http.StatusServiceUnavailable, "limiter_unavailable", "rate limiter unavailable", time.Second)
			return
		}

		m.setHeaders(w.Header(), d)
		if !d.Allowed {
			writeError(w, http.StatusTooManyRequests, "rate_limited", "rate limit exceeded", d.RetryAfter)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *mw) setHeaders(h http.Header, d limiter.Decision) {
	reset := strconv.FormatInt(ceilSeconds(d.ResetAfter), 10)
	remaining := strconv.FormatInt(d.Remaining, 10)
	h.Set("RateLimit-Policy", m.policyHeader)
	h.Set("RateLimit", `"`+m.opts.Policy+`";r=`+remaining+";t="+reset)
	if m.opts.LegacyHeaders {
		h.Set("X-RateLimit-Limit", strconv.FormatInt(m.opts.Limit.Rate, 10))
		h.Set("X-RateLimit-Remaining", remaining)
		h.Set("X-RateLimit-Reset", reset)
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

func validPolicyName(s string) bool {
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
