package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/policy"
)

// Policies finds a named policy at a tier; *policy.Store implements it.
type Policies interface {
	Lookup(name, tier string) (policy.Policy, bool)
}

const (
	maxCheckBody = 4 << 10
	maxKeyLen    = 256
)

// checkRequest is the body of POST /v1/check. The caller names a policy
// from the server's file and identifies the client; it cannot send a limit
// of its own, so a compromised caller cannot raise anyone's quota.
type checkRequest struct {
	Policy string `json:"policy"`
	Key    string `json:"key"`
	Tier   string `json:"tier,omitempty"`
	// Cost overrides the policy's cost when set; 0 peeks without
	// consuming anything.
	Cost *int64 `json:"cost,omitempty"`
}

type limitJSON struct {
	Rate     int64 `json:"rate"`
	PeriodMS int64 `json:"period_ms"`
	Burst    int64 `json:"burst"`
}

// checkResponse is the decision. Allowed and denied are both 200: the
// check itself succeeded either way, and a caller must not mistake a
// denial for an outage of the limiter or the other way round.
type checkResponse struct {
	Allowed      bool       `json:"allowed"`
	Policy       string     `json:"policy"`
	Limit        *limitJSON `json:"limit,omitempty"`
	Remaining    int64      `json:"remaining"`
	RetryAfterMS int64      `json:"retry_after_ms"`
	ResetAfterMS int64      `json:"reset_after_ms"`
	// Unlimited: the tier is exempt from the policy.
	Unlimited bool `json:"unlimited,omitempty"`
	// FailOpen: the limiter was unavailable and the server is configured
	// to allow in that case.
	FailOpen bool `json:"fail_open,omitempty"`
}

func (a *api) check(w http.ResponseWriter, r *http.Request) {
	var req checkRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCheckBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_body", "invalid JSON body: "+err.Error())
		return
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid_body", "body must hold one JSON object")
		return
	}
	switch {
	case req.Policy == "":
		writeError(w, http.StatusBadRequest, "invalid_body", "policy is required")
		return
	case req.Key == "" || len(req.Key) > maxKeyLen:
		writeError(w, http.StatusBadRequest, "invalid_body", fmt.Sprintf("key must be 1-%d bytes", maxKeyLen))
		return
	}

	p, ok := a.policies.Lookup(req.Policy, req.Tier)
	if !ok {
		writeError(w, http.StatusNotFound, "unknown_policy", fmt.Sprintf("no policy named %q", req.Policy))
		return
	}
	if p.Unlimited {
		writeJSON(w, http.StatusOK, checkResponse{Allowed: true, Policy: p.Name, Unlimited: true})
		return
	}
	cost := p.Cost
	if req.Cost != nil {
		cost = *req.Cost
	}

	// Same key namespace as the middleware, so a service calling this
	// endpoint and one using the middleware share the policy's state.
	d, err := p.Limiter.Allow(r.Context(), p.Name+":"+req.Key, p.Limit, cost)
	switch {
	case errors.Is(err, limiter.ErrInvalidCost):
		writeError(w, http.StatusBadRequest, "invalid_cost",
			fmt.Sprintf("cost %d must be between 0 and the burst of %d", cost, p.Limit.Burst))
		return
	case errors.Is(err, limiter.ErrInvalidLimit):
		a.logger.ErrorContext(r.Context(), "rate limit misconfigured", slog.String("policy", p.Name), slog.Any("error", err))
		writeError(w, http.StatusInternalServerError, "limiter_misconfigured", "rate limiter misconfigured")
		return
	case err != nil:
		a.logger.ErrorContext(r.Context(), "rate limiter failed",
			slog.String("policy", p.Name), slog.Bool("fail_open", a.failOpen), slog.Any("error", err))
		if a.failOpen {
			writeJSON(w, http.StatusOK, checkResponse{Allowed: true, Policy: p.Name, Limit: toLimitJSON(p.Limit), FailOpen: true})
			return
		}
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "limiter_unavailable", "rate limiter unavailable")
		return
	}

	writeJSON(w, http.StatusOK, checkResponse{
		Allowed:      d.Allowed,
		Policy:       p.Name,
		Limit:        toLimitJSON(d.Limit),
		Remaining:    d.Remaining,
		RetryAfterMS: ceilMillis(d.RetryAfter),
		ResetAfterMS: ceilMillis(d.ResetAfter),
	})
}

// toLimitJSON reports the limit the decision was made under. During a Redis
// outage with a local fallback that is the instance's share, not the
// configured limit.
func toLimitJSON(l limiter.Limit) *limitJSON {
	return &limitJSON{Rate: l.Rate, PeriodMS: ceilMillis(l.Period), Burst: l.Burst}
}

// ceilMillis rounds up, so a caller that waits RetryAfterMS is never early.
func ceilMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	var b errorBody
	b.Error.Code, b.Error.Message = code, msg
	writeJSON(w, status, b)
}
