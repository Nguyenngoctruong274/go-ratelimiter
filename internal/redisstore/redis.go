// Package redisstore keeps rate-limiter state in Redis so every instance of
// the service shares the same counters. Each limiter (TokenBucket, GCRA,
// SlidingWindow) runs every check as one atomic Lua script, a port of its
// reference in package algo; this file owns the connection.
package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Open parses a redis:// or rediss:// URL, connects and pings the server.
// Timeouts are short on purpose: a rate-limit check sits on the request path
// of the caller, so a slow Redis must turn into a fast error the caller can
// act on (fail open or closed) rather than a stalled request.
func Open(ctx context.Context, rawURL string) (*redis.Client, error) {
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 500 * time.Millisecond
	opts.WriteTimeout = 500 * time.Millisecond
	opts.PoolTimeout = time.Second

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis %s: %w", opts.Addr, err)
	}
	return client, nil
}

// Pinger adapts a client to a readiness check.
func Pinger(c *redis.Client) func(context.Context) error {
	return func(ctx context.Context) error { return c.Ping(ctx).Err() }
}
