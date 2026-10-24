package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/policy"
)

type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

type failingLimiter struct{ err error }

func (f failingLimiter) Allow(context.Context, string, limiter.Limit, int64) (limiter.Decision, error) {
	return limiter.Decision{}, f.err
}

// policies is a fixed table standing in for *policy.Store.
type policies map[string]policy.Policy

func (p policies) Lookup(name, tier string) (policy.Policy, bool) {
	if pol, ok := p[name+"/"+tier]; ok {
		return pol, true
	}
	pol, ok := p[name+"/"]
	return pol, ok
}

func newCheckHandler(lim limiter.Limiter, failOpen bool) http.Handler {
	return NewHandler(discardLogger(), Options{FailOpen: failOpen, Policies: policies{
		"api/":         {Name: "api", Limiter: lim, Limit: limiter.PerMinute(60, 3), Cost: 1},
		"api/internal": {Name: "api", Limiter: lim, Cost: 1, Unlimited: true},
		"export/":      {Name: "export", Limiter: lim, Limit: limiter.PerMinute(60, 10), Cost: 4},
		"broken/":      {Name: "broken", Limiter: lim, Limit: limiter.Limit{}, Cost: 1},
	}})
}

func postCheck(h http.Handler, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/check", strings.NewReader(body)))
	return rec
}

func TestCheckDecisions(t *testing.T) {
	h := newCheckHandler(memory.New(fixedClock{time.Unix(1_700_000_000, 0)}), false)

	steps := []struct {
		name string
		body string
		want string
	}{
		{"first", `{"policy":"api","key":"u1"}`,
			`{"allowed":true,"policy":"api","limit":{"rate":60,"period_ms":60000,"burst":3},"remaining":2,"retry_after_ms":0,"reset_after_ms":1000}`},
		{"peek costs nothing", `{"policy":"api","key":"u1","cost":0}`,
			`{"allowed":true,"policy":"api","limit":{"rate":60,"period_ms":60000,"burst":3},"remaining":2,"retry_after_ms":0,"reset_after_ms":1000}`},
		{"explicit cost", `{"policy":"api","key":"u1","cost":2}`,
			`{"allowed":true,"policy":"api","limit":{"rate":60,"period_ms":60000,"burst":3},"remaining":0,"retry_after_ms":0,"reset_after_ms":3000}`},
		{"denied is still 200", `{"policy":"api","key":"u1"}`,
			`{"allowed":false,"policy":"api","limit":{"rate":60,"period_ms":60000,"burst":3},"remaining":0,"retry_after_ms":1000,"reset_after_ms":3000}`},
		{"other key is independent", `{"policy":"api","key":"u2"}`,
			`{"allowed":true,"policy":"api","limit":{"rate":60,"period_ms":60000,"burst":3},"remaining":2,"retry_after_ms":0,"reset_after_ms":1000}`},
		{"policy cost by default", `{"policy":"export","key":"u1"}`,
			`{"allowed":true,"policy":"export","limit":{"rate":60,"period_ms":60000,"burst":10},"remaining":6,"retry_after_ms":0,"reset_after_ms":4000}`},
		{"unlimited tier", `{"policy":"api","key":"u1","tier":"internal"}`,
			`{"allowed":true,"policy":"api","remaining":0,"retry_after_ms":0,"reset_after_ms":0,"unlimited":true}`},
	}
	for _, s := range steps {
		rec := postCheck(h, s.body)
		if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != s.want {
			t.Fatalf("%s: %d %s\nwant 200 %s", s.name, rec.Code, rec.Body, s.want)
		}
	}
}

func TestCheckRejects(t *testing.T) {
	tests := []struct {
		name   string
		lim    limiter.Limiter
		body   string
		status int
		code   string
	}{
		{"not json", nil, `policy=api`, 400, "invalid_body"},
		{"unknown field", nil, `{"policy":"api","key":"k","limit":1000}`, 400, "invalid_body"},
		{"two objects", nil, `{"policy":"api","key":"k"}{}`, 400, "invalid_body"},
		{"no policy", nil, `{"key":"k"}`, 400, "invalid_body"},
		{"no key", nil, `{"policy":"api"}`, 400, "invalid_body"},
		{"key too long", nil, `{"policy":"api","key":"` + strings.Repeat("k", 257) + `"}`, 400, "invalid_body"},
		{"body too large", nil, `{"policy":"api","key":"k","tier":"` + strings.Repeat("t", 5000) + `"}`, 400, "invalid_body"},
		{"unknown policy", nil, `{"policy":"nope","key":"k"}`, 404, "unknown_policy"},
		{"cost above burst", nil, `{"policy":"api","key":"k","cost":4}`, 400, "invalid_cost"},
		{"negative cost", nil, `{"policy":"api","key":"k","cost":-1}`, 400, "invalid_cost"},
		{"bad limit", nil, `{"policy":"broken","key":"k"}`, 500, "limiter_misconfigured"},
		{"limiter down", failingLimiter{errors.New("redis: connection refused")}, `{"policy":"api","key":"k"}`, 503, "limiter_unavailable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lim := tt.lim
			if lim == nil {
				lim = memory.New(limiter.SystemClock{})
			}
			rec := postCheck(newCheckHandler(lim, false), tt.body)
			var body errorBody
			_ = json.Unmarshal(rec.Body.Bytes(), &body)
			if rec.Code != tt.status || body.Error.Code != tt.code {
				t.Fatalf("got %d %s, want %d %s", rec.Code, rec.Body, tt.status, tt.code)
			}
		})
	}
}

func TestCheckFailOpen(t *testing.T) {
	h := newCheckHandler(failingLimiter{errors.New("redis: connection refused")}, true)
	rec := postCheck(h, `{"policy":"api","key":"k"}`)
	var resp checkResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 200 || !resp.Allowed || !resp.FailOpen {
		t.Fatalf("got %d %s, want 200 allowed with fail_open", rec.Code, rec.Body)
	}
}

func TestCheckEndpointNeedsPolicies(t *testing.T) {
	rec := postCheck(NewHandler(discardLogger(), Options{}), `{"policy":"api","key":"k"}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d without policies, want 404", rec.Code)
	}
	h := newCheckHandler(memory.New(limiter.SystemClock{}), false)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/check", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status %d, want 405", rec.Code)
	}
}

func TestCeilMillis(t *testing.T) {
	for d, want := range map[time.Duration]int64{0: 0, -time.Second: 0, time.Microsecond: 1, time.Millisecond: 1, 1001 * time.Microsecond: 2} {
		if got := ceilMillis(d); got != want {
			t.Errorf("ceilMillis(%s) = %d, want %d", d, got, want)
		}
	}
}
