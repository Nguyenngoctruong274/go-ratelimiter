package redisstore

import (
	_ "embed" // for the Lua script

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

//go:embed tokenbucket.lua
var tokenBucketLua string

var tokenBucketScript = redis.NewScript(tokenBucketLua)

// TokenBucket is a token-bucket limiter whose state lives in Redis, shared
// by every instance that points at the same server. Each check is a single
// Lua script, so concurrent checks from any number of instances are
// serialized by Redis and can never over-grant.
type TokenBucket struct{ scripted }

var _ limiter.Limiter = (*TokenBucket)(nil)

// NewTokenBucket returns a limiter that stores buckets under prefix
// (DefaultPrefix when empty), as hashes at <prefix>tb:<limit>:<key>.
func NewTokenBucket(rdb redis.Scripter, prefix string) *TokenBucket {
	return &TokenBucket{newScripted(rdb, prefix, "tb", tokenBucketScript, limiter.Limit.Check)}
}
