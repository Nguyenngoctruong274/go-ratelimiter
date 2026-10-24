package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/breaker"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/config"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/failover"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/httpapi"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/memory"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/policy"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/redisstore"
)

var errDraining = errors.New("shutting down")

// service is one ratelimitd instance, wired but not yet listening.
type service struct {
	handler  http.Handler
	draining atomic.Bool
	rdb      *redis.Client
}

// newService connects to Redis, loads the policies and builds the HTTP
// handler. Background work (policy reload, sweeping the local fallback)
// runs until ctx ends. Close releases the Redis client.
func newService(ctx context.Context, cfg config.Config, logger *slog.Logger) (*service, error) {
	startCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rdb, err := redisstore.Open(startCtx, cfg.RedisURL)
	if err != nil {
		return nil, err
	}
	s := &service{rdb: rdb}

	var local *memory.Limiter
	if cfg.OnRedisFailure == config.FailLocal {
		local = memory.New(limiter.SystemClock{})
		go sweep(ctx, local, time.Minute)
	}
	guard := func(name string, primary limiter.Limiter) (limiter.Limiter, error) {
		opts := failover.Options{
			Replicas: cfg.Replicas,
			Timeout:  cfg.RedisTimeout,
			Breaker:  breaker.Options{Threshold: cfg.BreakerThreshold, Cooldown: cfg.BreakerCooldown},
			Logger:   logger.With(slog.String("algorithm", name)),
		}
		if local != nil {
			opts.Fallback = local
		}
		return failover.New(primary, opts)
	}
	limiters := map[string]limiter.Limiter{
		policy.TokenBucket:   redisstore.NewTokenBucket(rdb, ""),
		policy.GCRA:          redisstore.NewGCRA(rdb, ""),
		policy.SlidingWindow: redisstore.NewSlidingWindow(rdb, ""),
	}
	for name, l := range limiters {
		if limiters[name], err = guard(name, l); err != nil {
			_ = rdb.Close()
			return nil, err
		}
	}

	store, err := policy.Open(cfg.PolicyFile, policy.Options{
		Limiters: limiters,
		// /v1/check callers name the client and tier in the body; these
		// only satisfy rules written for the middleware, whose keys and
		// tiers come from the HTTP request.
		User: func(*http.Request) (string, bool) { return "", false },
		Tier: func(*http.Request) string { return "" },
	}, logger)
	if err != nil {
		_ = rdb.Close()
		return nil, err
	}
	logger.Info("policies loaded", slog.String("path", cfg.PolicyFile), slog.Any("policies", store.Current().Names()))
	if cfg.PolicyReload > 0 {
		go func() { _ = store.Watch(ctx, cfg.PolicyReload) }()
	}

	ping := redisstore.Pinger(rdb)
	ready := func(ctx context.Context) error {
		if s.draining.Load() {
			return errDraining
		}
		// Only a fail-closed instance is useless without Redis. In open or
		// local mode it still answers, and failing readiness would make
		// the load balancer drop every instance at once.
		if cfg.OnRedisFailure != config.FailClosed {
			return nil
		}
		if err := ping(ctx); err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		return nil
	}

	s.handler = httpapi.NewHandler(logger, httpapi.Options{
		Ready:    ready,
		Policies: store,
		FailOpen: cfg.OnRedisFailure == config.FailOpen,
	})
	return s, nil
}

// Close releases the Redis client.
func (s *service) Close() error { return s.rdb.Close() }

func sweep(ctx context.Context, m *memory.Limiter, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.Sweep()
		}
	}
}
