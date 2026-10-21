package redisstore

import (
	_ "embed" // for the Lua script

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/algo"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

//go:embed gcra.lua
var gcraLua string

var gcraScript = redis.NewScript(gcraLua)

// GCRA is a GCRA limiter in Redis. It makes the same decisions as
// TokenBucket but keeps a single string per key, writes only when it grants
// a request, and lets Redis expire the key at the theoretical arrival time.
type GCRA struct{ scripted }

var _ limiter.Limiter = (*GCRA)(nil)

// NewGCRA returns a limiter that stores state under prefix (DefaultPrefix
// when empty), as strings at <prefix>gcra:<limit>:<key>. It accepts the
// limits algo.CheckGCRA does.
func NewGCRA(rdb redis.Scripter, prefix string) *GCRA {
	return &GCRA{newScripted(rdb, prefix, "gcra", gcraScript, algo.CheckGCRA)}
}
