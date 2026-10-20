package httpapi

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestProbes(t *testing.T) {
	down := func(context.Context) error { return errors.New("redis: connection refused") }
	up := func(context.Context) error { return nil }

	tests := []struct {
		name   string
		ready  func(context.Context) error
		method string
		path   string
		status int
		body   string
	}{
		{"healthz", nil, http.MethodGet, "/healthz", 200, "{\"status\":\"ok\"}\n"},
		{"healthz ignores redis", down, http.MethodGet, "/healthz", 200, "{\"status\":\"ok\"}\n"},
		{"healthz wrong method", nil, http.MethodPost, "/healthz", 405, ""},
		{"readyz without check", nil, http.MethodGet, "/readyz", 200, "{\"status\":\"ready\"}\n"},
		{"readyz up", up, http.MethodGet, "/readyz", 200, "{\"status\":\"ready\"}\n"},
		{"readyz down", down, http.MethodGet, "/readyz", 503, "{\"status\":\"unavailable\"}\n"},
		{"unknown path", nil, http.MethodGet, "/nope", 404, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHandler(discardLogger(), Options{Ready: tt.ready})
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(tt.method, tt.path, nil))
			if rec.Code != tt.status {
				t.Fatalf("status = %d, want %d", rec.Code, tt.status)
			}
			if tt.body != "" && rec.Body.String() != tt.body {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tt.body)
			}
		})
	}
}

func TestRequestID(t *testing.T) {
	var seen string
	h := (&api{logger: discardLogger()}).withRequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = RequestID(r.Context())
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if len(seen) != 24 || rec.Header().Get(RequestIDHeader) != seen {
		t.Fatalf("generated id %q, header %q", seen, rec.Header().Get(RequestIDHeader))
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set(RequestIDHeader, "client-abc")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if seen != "client-abc" || rec.Header().Get(RequestIDHeader) != "client-abc" {
		t.Fatalf("client id not propagated: %q", seen)
	}
}
