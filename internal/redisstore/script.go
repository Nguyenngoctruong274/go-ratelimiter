package redisstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

// DefaultPrefix namespaces every key this package writes.
const DefaultPrefix = "rl:"

// scripted is what the Redis limiters have in common: each check is one
// atomic Lua script with the same arguments and the same reply, so they
// differ only in the script, the key tag and the limits they accept.
//
// Every script takes KEYS[1] = state key and ARGV = rate, period (µs),
// burst, cost and an optional "now" (µs) for tests, and returns
// {allowed (0|1), remaining, retry_after_us, reset_after_us}.
type scripted struct {
	rdb    redis.Scripter
	prefix string // DefaultPrefix or the caller's, plus the algorithm tag
	script *redis.Script
	check  func(l limiter.Limit, n int64) error

	// now, when set, overrides the Redis clock. Tests use it to replay the
	// same timeline against the script and the Go reference.
	now func() int64
}

func newScripted(rdb redis.Scripter, prefix, tag string, script *redis.Script, check func(limiter.Limit, int64) error) scripted {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return scripted{rdb: rdb, prefix: prefix + tag + ":", script: script, check: check}
}

// Key returns the Redis key of the state for key under l. The limit is part
// of the key for the same reason as in the in-memory limiter: the stored
// state is in units of the limit.
func (s *scripted) Key(key string, l limiter.Limit) string {
	return s.prefix + strconv.FormatInt(l.Rate, 10) + ":" +
		strconv.FormatInt(l.PeriodMicros(), 10) + ":" + strconv.FormatInt(l.Burst, 10) + ":" + key
}

// Allow implements limiter.Limiter. It costs one round trip: EVALSHA, plus a
// one-time EVAL when the server does not have the script cached yet.
func (s *scripted) Allow(ctx context.Context, key string, l limiter.Limit, n int64) (limiter.Decision, error) {
	if err := s.check(l, n); err != nil {
		return limiter.Decision{}, fmt.Errorf("allow %q: %w", key, err)
	}

	args := []any{l.Rate, l.PeriodMicros(), l.Burst, n}
	if s.now != nil {
		args = append(args, s.now())
	}
	res, err := s.script.Run(ctx, s.rdb, []string{s.Key(key, l)}, args...).Int64Slice()
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
