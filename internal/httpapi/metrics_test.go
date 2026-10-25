package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/metrics"
)

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: %d", rec.Code)
	}
	return rec.Body.String()
}

func TestMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	lim := memory.New(fixedClock{time.Unix(1_700_000_000, 0)})
	h := NewHandler(discardLogger(), Options{Metrics: reg, Policies: policies{
		"api/":         {Name: "api", Limiter: lim, Limit: limiter.PerMinute(60, 1), Cost: 1},
		"api/internal": {Name: "api", Limiter: lim, Cost: 1, Unlimited: true},
		"down/":        {Name: "down", Limiter: failingLimiter{errors.New("redis: i/o timeout")}, Limit: limiter.PerMinute(60, 1), Cost: 1},
	}})

	for _, body := range []string{
		`{"policy":"api","key":"k"}`,                   // allowed
		`{"policy":"api","key":"k"}`,                   // denied
		`{"policy":"api","key":"k","tier":"internal"}`, // unlimited
		`{"policy":"api","key":"k","cost":5}`,          // invalid cost
		`{"policy":"down","key":"k"}`,                  // unavailable
		`{"policy":"attacker-chosen-name","key":"k"}`,  // unknown policy
		`{"key":"k"}`, // invalid body
	} {
		postCheck(h, body)
	}
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/no/such/path", nil))

	out := scrape(t, h)
	for _, want := range []string{
		`ratelimit_checks_total{policy="api",result="allowed"} 1`,
		`ratelimit_checks_total{policy="api",result="denied"} 1`,
		`ratelimit_checks_total{policy="api",result="unlimited"} 1`,
		`ratelimit_checks_total{policy="api",result="invalid"} 1`,
		`ratelimit_checks_total{policy="down",result="unavailable"} 1`,
		`ratelimit_checks_total{policy="",result="unknown_policy"} 1`,
		`ratelimit_checks_total{policy="",result="invalid"} 1`,
		`http_requests_total{method="POST",route="/v1/check",status="200"} 3`,
		`http_requests_total{method="POST",route="/v1/check",status="503"} 1`,
		`http_requests_total{method="GET",route="unmatched",status="404"} 1`,
		`http_request_duration_seconds_count{method="POST",route="/v1/check"} 7`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(out, "attacker-chosen-name") || strings.Contains(out, "/no/such/path") {
		t.Error("request data leaked into metric labels")
	}
}

func TestMetricsEndpointOnlyWithRegistry(t *testing.T) {
	rec := httptest.NewRecorder()
	NewHandler(discardLogger(), Options{}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /metrics without a registry: %d, want 404", rec.Code)
	}
}
