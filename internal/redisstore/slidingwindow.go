package redisstore

import (
	_ "embed" // for the Lua script

	"github.com/redis/go-redis/v9"

	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/algo"
	"github.com/Nguyenngoctruong274/go-ratelimiter/internal/limiter"
)

//go:embed slidingwindow.lua
var slidingWindowLua string

var slidingWindowScript = redis.NewScript(slidingWindowLua)

// SlidingWindow is a sliding-window counter in Redis: two counts per key
// instead of a log of every event, at the price of an estimate. See
// algo.SlidingWindow for how far the estimate can be off.
type SlidingWindow struct{ scripted }

var _ limiter.Limiter = (*SlidingWindow)(nil)

// NewSlidingWindow returns a limiter that stores state under prefix
// (DefaultPrefix when empty), as strings at <prefix>sw:<limit>:<key>.
func NewSlidingWindow(rdb redis.Scripter, prefix string) *SlidingWindow {
	return &SlidingWindow{newScripted(rdb, prefix, "sw", slidingWindowScript, algo.CheckSlidingWindow)}
}
