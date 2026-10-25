package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/metrics"
)

type stubLimiter struct{ err error }

func (s stubLimiter) Allow(context.Context, string, limiter.Limit, int64) (limiter.Decision, error) {
	return limiter.Decision{Allowed: s.err == nil}, s.err
}

func TestLimiterMetrics(t *testing.T) {
	reg := metrics.NewRegistry()
	m := newLimiterMetrics(reg)
	ok := m.wrap("gcra", stubLimiter{})
	down := m.wrap("token_bucket", stubLimiter{errors.New("redis: i/o timeout")})
	invalid := m.wrap("sliding_window", stubLimiter{fmt.Errorf("allow: %w", limiter.ErrInvalidCost)})

	ctx := context.Background()
	for range 3 {
		_, _ = ok.Allow(ctx, "k", limiter.PerSecond(1, 1), 1)
	}
	_, _ = down.Allow(ctx, "k", limiter.PerSecond(1, 1), 1)
	_, _ = invalid.Allow(ctx, "k", limiter.PerSecond(1, 1), 5)
	m.onBreakerChange("token_bucket")(breaker.Closed, breaker.Open)

	var b strings.Builder
	reg.Write(&b)
	out := b.String()
	for _, want := range []string{
		`ratelimit_redis_check_duration_seconds_count{algorithm="gcra"} 3`,
		`ratelimit_redis_check_duration_seconds_count{algorithm="token_bucket"} 1`,
		`ratelimit_redis_errors_total{algorithm="token_bucket"} 1`,
		`ratelimit_breaker_state{algorithm="gcra"} 0`,
		`ratelimit_breaker_state{algorithm="token_bucket"} 1`,
		`ratelimit_breaker_transitions_total{algorithm="token_bucket",to="open"} 1`,
	} {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("missing %s", want)
		}
	}
	// A bad cost never reached Redis: neither its latency nor an error.
	if strings.Contains(out, `ratelimit_redis_check_duration_seconds_count{algorithm="sliding_window"}`) ||
		strings.Contains(out, `ratelimit_redis_errors_total{algorithm="sliding_window"}`) {
		t.Errorf("invalid input recorded as a Redis call:\n%s", out)
	}
}

// TestServiceMetrics scrapes a fully wired instance after a few checks.
func TestServiceMetrics(t *testing.T) {
	c := startCluster(t, 1, 5)
	key := uniqueKey(t)
	for range 7 {
		c.check(t, 0, map[string]any{"policy": "gcra", "key": key})
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, c.urls[0]+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := string(raw)
	for _, want := range []string{
		`ratelimit_checks_total{policy="gcra",result="allowed"} 5`,
		`ratelimit_checks_total{policy="gcra",result="denied"} 2`,
		`ratelimit_redis_check_duration_seconds_count{algorithm="gcra"} 7`,
		`ratelimit_breaker_state{algorithm="gcra"} 0`,
		`ratelimit_breaker_state{algorithm="sliding_window"} 0`,
		`ratelimit_redis_pipelined_checks_total `,
		`ratelimit_redis_round_trips_total `,
		`ratelimit_policy_reloads_applied_total 1`,
		`ratelimit_policy_reload_failing 0`,
		`redis_pool_connections `,
		`go_goroutines `,
		`http_requests_total{method="POST",route="/v1/check",status="200"} 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %s", want)
		}
	}
}

// BenchmarkServiceCheck measures POST /v1/check end to end (HTTP, JSON,
// policy lookup, failover, Redis) on one instance, with and without
// pipelining, and reports checks/s and latency percentiles.
//
//	RL_TEST_REDIS_URL=redis://localhost:6379/15 go test -run '^$' -bench ServiceCheck ./cmd/ratelimitd
func BenchmarkServiceCheck(b *testing.B) {
	for _, callers := range []int{16, 128} {
		for _, senders := range []int{0, 4} {
			mode := "direct"
			if senders > 0 {
				mode = "pipelined"
			}
			b.Run(fmt.Sprintf("callers=%d/%s", callers, mode), func(b *testing.B) {
				c := startClusterWith(b, 1, 1_000, senders)
				c.client = &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: callers}}
				keys := make([]string, 1024)
				for i := range keys {
					keys[i] = fmt.Sprintf("%s:%d", uniqueKey(b), i)
				}
				lat := runCallers(b, callers, func(i int) {
					c.check(b, 0, map[string]any{"policy": "gcra", "key": keys[i%len(keys)]})
				})
				reportLatency(b, lat)
			})
		}
	}
}

// runCallers runs b.N calls of fn over a fixed number of goroutines and
// returns the latency of each call.
func runCallers(b *testing.B, callers int, fn func(i int)) []time.Duration {
	var (
		next atomic.Int64
		wg   sync.WaitGroup
		mu   sync.Mutex
		all  = make([]time.Duration, 0, b.N)
	)
	b.ResetTimer()
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lat []time.Duration
			for i := int(next.Add(1)) - 1; i < b.N; i = int(next.Add(1)) - 1 {
				start := time.Now()
				fn(i)
				lat = append(lat, time.Since(start))
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	b.StopTimer()
	return all
}

func reportLatency(b *testing.B, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "checks/s")
	slices.Sort(lat)
	pct := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Microseconds()) }
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
}
