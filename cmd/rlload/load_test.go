package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeFleet starts n servers that answer /v1/check from one shared budget,
// the way ratelimitd instances share one Redis.
func fakeFleet(t *testing.T, n int, budget int64) []string {
	t.Helper()
	var left atomic.Int64
	left.Store(budget)
	var urls []string
	for range n {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct{ Policy, Key string }
			if r.URL.Path != "/v1/check" || json.NewDecoder(r.Body).Decode(&req) != nil || req.Policy == "" || req.Key == "" {
				http.Error(w, `{"error":{"code":"invalid_body"}}`, http.StatusBadRequest)
				return
			}
			fmt.Fprintf(w, `{"allowed":%t}`, left.Add(-1) >= 0)
		}))
		t.Cleanup(srv.Close)
		urls = append(urls, srv.URL)
	}
	return urls
}

func TestRunCountsOneSharedLimit(t *testing.T) {
	urls := fakeFleet(t, 3, 25)
	rep, err := Run(context.Background(), Config{
		Targets: urls, Policy: "login", Key: "demo", Keys: 1, Workers: 8, Requests: 300,
	}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Checks != 300 || rep.Allowed != 25 || rep.Denied != 275 || rep.Errors != 0 {
		t.Fatalf("report %+v", rep)
	}
	sum := 0
	for _, n := range rep.AllowedBy {
		sum += n
	}
	if sum != 25 || len(rep.AllowedBy) == 0 {
		t.Fatalf("allowed by target %v, want a total of 25", rep.AllowedBy)
	}
	if rep.P50 <= 0 || rep.P99 < rep.P50 || rep.Rate() <= 0 {
		t.Fatalf("latency p50 %s p99 %s rate %f", rep.P50, rep.P99, rep.Rate())
	}
}

func TestRunReportsFailures(t *testing.T) {
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, `{"error":{"code":"limiter_unavailable"}}`, http.StatusServiceUnavailable)
	}))
	defer down.Close()
	garbage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer garbage.Close()

	tests := []struct {
		name, url, want string
	}{
		{"unavailable", down.URL, "status 503"},
		{"not a decision", garbage.URL, "not a decision"},
		{"nothing listening", "http://127.0.0.1:1", "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep, err := Run(context.Background(), Config{
				Targets: []string{tt.url}, Policy: "p", Key: "k", Keys: 1, Workers: 2, Requests: 4,
			}, http.DefaultClient)
			if err != nil {
				t.Fatal(err)
			}
			if rep.Errors != 4 || rep.Allowed+rep.Denied != 0 || !strings.Contains(rep.FirstError, tt.want) {
				t.Fatalf("report %+v, want 4 errors mentioning %q", rep, tt.want)
			}
		})
	}
}

func TestRunStopsAfterDuration(t *testing.T) {
	urls := fakeFleet(t, 1, 1<<40)
	start := time.Now()
	rep, err := Run(context.Background(), Config{
		Targets: urls, Policy: "p", Key: "k", Keys: 10, Workers: 4, Duration: 100 * time.Millisecond,
	}, http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("ran for %s", took)
	}
	if rep.Checks == 0 || rep.Errors != 0 {
		t.Fatalf("report %+v: requests cut off by the deadline must not count as errors", rep)
	}
}

func TestConfigValidate(t *testing.T) {
	ok := Config{Targets: []string{"http://x"}, Policy: "p", Key: "k", Keys: 1, Workers: 1, Requests: 1}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"valid", func(*Config) {}, ""},
		{"duration only", func(c *Config) { c.Requests, c.Duration = 0, time.Second }, ""},
		{"no targets", func(c *Config) { c.Targets = nil }, "target"},
		{"no policy", func(c *Config) { c.Policy = "" }, "policy and key"},
		{"no workers", func(c *Config) { c.Workers = 0 }, "workers and keys"},
		{"no stop condition", func(c *Config) { c.Requests = 0 }, "set requests, duration"},
		{"negative", func(c *Config) { c.Duration = -1 }, "must not be negative"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ok
			tt.mutate(&c)
			err := c.validate()
			if (tt.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tt.want)) {
				t.Fatalf("validate() = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestRunCommand(t *testing.T) {
	urls := fakeFleet(t, 2, 5)
	var out strings.Builder
	args := []string{"-targets", strings.Join(urls, ","), "-policy", "login", "-key", "demo", "-requests", "40", "-want-allowed", "5"}
	if err := run(args, &out); err != nil {
		t.Fatalf("run: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "allowed 5, denied 35, errors 0") {
		t.Fatalf("output:\n%s", out.String())
	}

	// The budget is spent: a second run allows nothing, so expecting 5 fails.
	out.Reset()
	if err := run(args, &out); err == nil || !strings.Contains(err.Error(), "allowed 0, want exactly 5") {
		t.Fatalf("second run: %v", err)
	}
	if err := run([]string{"-targets", "", "-requests", "1"}, &out); err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("no targets: %v", err)
	}
}
