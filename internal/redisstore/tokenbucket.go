package redisstore

import (
	"context"
	_ "embed" // for the Lua script
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

//go:embed tokenbucket.lua
var tokenBucketLua string

var tokenBucketScript = redis.NewScript(tokenBucketLua)

// DefaultPrefix namespaces every key this package writes.
const DefaultPrefix = "rl:"

// TokenBucket is a token-bucket limiter whose state lives in Redis, shared
// by every instance that points at the same server. Each check is a single
// Lua script, so concurrent checks from any number of instances are
// serialized by Redis and can never over-grant.
type TokenBucket struct {
	rdb    redis.Scripter
	prefix string

	// now, when set, overrides the Redis clock. Tests use it to replay the
	// same timeline against the script and the Go reference.
	now func() int64
}

var _ limiter.Limiter = (*TokenBucket)(nil)

// NewTokenBucket returns a limiter that stores buckets under prefix
// (DefaultPrefix when empty).
func NewTokenBucket(rdb redis.Scripter, prefix string) *TokenBucket {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return &TokenBucket{rdb: rdb, prefix: prefix}
}

// Key returns the Redis key of the bucket for key under l. The limit is part
// of the key for the same reason as in the in-memory limiter: the stored
// level is in units of the limit's period.
func (s *TokenBucket) Key(key string, l limiter.Limit) string {
	return s.prefix + "tb:" + strconv.FormatInt(l.Rate, 10) + ":" +
		strconv.FormatInt(l.PeriodMicros(), 10) + ":" + strconv.FormatInt(l.Burst, 10) + ":" + key
}

// Allow implements limiter.Limiter. It costs one round trip: EVALSHA, plus a
// one-time EVAL when the server does not have the script cached yet.
func (s *TokenBucket) Allow(ctx context.Context, key string, l limiter.Limit, n int64) (limiter.Decision, error) {
	if err := l.Validate(); err != nil {
		return limiter.Decision{}, fmt.Errorf("allow %q: %w", key, err)
	}
	if n < 0 || n > l.Burst {
		return limiter.Decision{}, fmt.Errorf("allow %q: %w: cost %d with burst %d", key, limiter.ErrInvalidCost, n, l.Burst)
	}

	args := []any{l.Rate, l.PeriodMicros(), l.Burst, n}
	if s.now != nil {
		args = append(args, s.now())
	}
	res, err := tokenBucketScript.Run(ctx, s.rdb, []string{s.Key(key, l)}, args...).Int64Slice()
	if err != nil {
		return limiter.Decision{}, fmt.Errorf("allow %q: redis: %w", key, err)
	}
	if len(res) != 4 {
		return limiter.Decision{}, fmt.Errorf("allow %q: %w: got %d values", key, errBadReply, len(res))
	}
	return limiter.Decision{
		Allowed:    res[0] == 1,
		Limit:      l,
		Remaining:  res[1],
		RetryAfter: time.Duration(res[2]) * time.Microsecond,
		ResetAfter: time.Duration(res[3]) * time.Microsecond,
	}, nil
}

var errBadReply = errors.New("unexpected script reply")
