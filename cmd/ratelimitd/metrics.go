package main

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/metrics"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/policy"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

// limiterMetrics instruments the Redis side of the limiters: how long a
// check takes on Redis, how often it fails, and what the breakers do.
type limiterMetrics struct {
	latency     *metrics.HistogramVec
	errors      *metrics.CounterVec
	state       *metrics.GaugeVec
	transitions *metrics.CounterVec
}

func newLimiterMetrics(reg *metrics.Registry) *limiterMetrics {
	return &limiterMetrics{
		latency: reg.NewHistogramVec("ratelimit_redis_check_duration_seconds",
			"Time of one check on Redis by algorithm, including waiting for a pipeline.",
			metrics.FastLatencyBuckets, "algorithm"),
		errors: reg.NewCounterVec("ratelimit_redis_errors_total",
			"Checks that failed on Redis (errors and timeouts) by algorithm.", "algorithm"),
		state: reg.NewGaugeVec("ratelimit_breaker_state",
			"Circuit breaker in front of Redis by algorithm: 0 closed, 1 open, 2 half-open.", "algorithm"),
		transitions: reg.NewCounterVec("ratelimit_breaker_transitions_total",
			"Circuit breaker state changes by algorithm and new state.", "algorithm", "to"),
	}
}

// wrap measures every check that reaches next, the Redis limiter of one
// algorithm.
func (m *limiterMetrics) wrap(algorithm string, next limiter.Limiter) limiter.Limiter {
	m.state.Set(float64(breaker.Closed), algorithm)
	return &timedLimiter{next: next, algorithm: algorithm, m: m}
}

// onBreakerChange keeps the breaker gauge of algorithm current.
func (m *limiterMetrics) onBreakerChange(algorithm string) func(from, to breaker.State) {
	return func(_, to breaker.State) {
		m.state.Set(float64(to), algorithm)
		m.transitions.Inc(algorithm, to.String())
	}
}

type timedLimiter struct {
	next      limiter.Limiter
	algorithm string
	m         *limiterMetrics
}

func (t *timedLimiter) Allow(ctx context.Context, key string, l limiter.Limit, n int64) (limiter.Decision, error) {
	start := time.Now()
	d, err := t.next.Allow(ctx, key, l, n)
	if errors.Is(err, limiter.ErrInvalidLimit) || errors.Is(err, limiter.ErrInvalidCost) {
		return d, err // rejected before reaching Redis
	}
	t.m.latency.Observe(time.Since(start).Seconds(), t.algorithm)
	if err != nil {
		t.m.errors.Inc(t.algorithm)
	}
	return d, err
}

// registerRedisPool exposes the connection pool, whose exhaustion shows up
// as timeouts long before Redis itself is the bottleneck.
func registerRedisPool(reg *metrics.Registry, rdb *redis.Client) {
	reg.NewGaugeFunc("redis_pool_connections", "Open connections to Redis.", func() float64 {
		return float64(rdb.PoolStats().TotalConns)
	})
	reg.NewGaugeFunc("redis_pool_idle_connections", "Idle connections to Redis.", func() float64 {
		return float64(rdb.PoolStats().IdleConns)
	})
	reg.NewCounterFunc("redis_pool_timeouts_total", "Waits for a free Redis connection that timed out.", func() float64 {
		return float64(rdb.PoolStats().Timeouts)
	})
}

// registerPipeline exposes how well checks are batched: checks per round
// trip is the ratio of the two counters.
func registerPipeline(reg *metrics.Registry, p *redisstore.Pipeline) {
	reg.NewCounterFunc("ratelimit_redis_round_trips_total", "Pipelines sent to Redis.", func() float64 {
		return float64(p.Stats().Batches)
	})
	reg.NewCounterFunc("ratelimit_redis_pipelined_checks_total", "Checks sent to Redis in pipelines.", func() float64 {
		return float64(p.Stats().Commands)
	})
}

// registerPolicies exposes policy reloads. A failing reload keeps the old
// rules in force, so it needs an alert rather than a log line nobody reads.
func registerPolicies(reg *metrics.Registry, s *policy.Store) {
	reg.NewCounterFunc("ratelimit_policy_reloads_applied_total", "Policy files loaded, the first one included.", func() float64 {
		return float64(s.ReloadStats().Applied)
	})
	reg.NewCounterFunc("ratelimit_policy_reloads_rejected_total", "Policy files rejected as invalid.", func() float64 {
		return float64(s.ReloadStats().Rejected)
	})
	reg.NewGaugeFunc("ratelimit_policy_reload_failing", "1 while the policy file on disk is invalid and older rules are enforced.", func() float64 {
		if s.ReloadStats().Failing {
			return 1
		}
		return 0
	})
}
