// Package httpapi exposes ratelimitd over HTTP: the probes, and
// POST /v1/check for services that ask for a decision over the network
// instead of embedding the middleware.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/metrics"
)

// RequestIDHeader carries the request id in and out of the service.
const RequestIDHeader = "X-Request-Id"

// Options configures the handler. Zero values are valid.
type Options struct {
	// Ready reports whether the service can take traffic (dependencies up,
	// not draining). nil means always ready.
	Ready func(context.Context) error
	// Policies serves POST /v1/check; nil leaves the endpoint out.
	Policies Policies
	// FailOpen answers "allowed" when the limiter fails instead of 503.
	FailOpen bool
	// Metrics, when set, receives HTTP and check metrics and is served at
	// GET /metrics.
	Metrics *metrics.Registry
}

type api struct {
	logger   *slog.Logger
	ready    func(context.Context) error
	policies Policies
	failOpen bool

	requests *metrics.CounterVec
	latency  *metrics.HistogramVec
	checks   *metrics.CounterVec
}

// NewHandler returns the root handler of the service.
func NewHandler(logger *slog.Logger, opts Options) http.Handler {
	reg := opts.Metrics
	if reg == nil {
		reg = metrics.NewRegistry() // instrument anyway; nothing serves it
	}
	a := &api{
		logger: logger, ready: opts.Ready, policies: opts.Policies, failOpen: opts.FailOpen,
		requests: reg.NewCounterVec("http_requests_total",
			"HTTP requests by method, route pattern and status code.", "method", "route", "status"),
		latency: reg.NewHistogramVec("http_request_duration_seconds",
			"HTTP request latency by method and route pattern.", metrics.FastLatencyBuckets, "method", "route"),
		checks: reg.NewCounterVec("ratelimit_checks_total",
			"POST /v1/check results by policy: allowed, denied, unlimited, fail_open, unavailable, "+
				"invalid, unknown_policy, misconfigured. The policy is empty when the request named none that exists.",
			"policy", "result"),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("GET /readyz", a.readyz)
	if opts.Metrics != nil {
		mux.Handle("GET /metrics", opts.Metrics.Handler())
	}
	if a.policies != nil {
		mux.HandleFunc("POST /v1/check", a.check)
	}
	return a.withRequestID(a.instrument(mux))
}

// healthz answers as long as the process is serving: it is a liveness probe
// and must not depend on Redis, or a Redis blip would restart every pod.
func (a *api) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz fails while Redis is unreachable or the process is draining, so the
// load balancer stops sending traffic without the pod being restarted.
func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	if a.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := a.ready(ctx); err != nil {
			a.logger.WarnContext(ctx, "not ready", slog.Any("error", err))
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

type ctxKey struct{}

// RequestID returns the id assigned to the request, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

func (a *api) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" || len(id) > 128 {
			id = newRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument records the metrics of every request and logs it at debug
// level. Metrics are labelled with the route pattern, never the raw path,
// so arbitrary URLs cannot create new time series.
func (a *api) instrument(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		mux.ServeHTTP(rec, r)
		elapsed := time.Since(start)

		route := "unmatched"
		if _, pattern := mux.Handler(r); pattern != "" {
			if _, path, ok := strings.Cut(pattern, " "); ok {
				route = path
			} else {
				route = pattern
			}
		}
		a.requests.Inc(r.Method, route, strconv.Itoa(rec.status))
		a.latency.Observe(elapsed.Seconds(), r.Method, route)

		a.logger.DebugContext(r.Context(), "request",
			slog.String("request_id", RequestID(r.Context())),
			slog.String("method", r.Method), slog.String("path", r.URL.Path),
			slog.Int("status", rec.status), slog.Duration("duration", elapsed))
	})
}

func newRequestID() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
