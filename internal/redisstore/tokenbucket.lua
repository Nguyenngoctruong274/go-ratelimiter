-- Token bucket check, run atomically inside Redis.
--
-- This is a line-by-line port of algo.TokenBucket. State is a hash
-- {l = level in token-microseconds, s = stamp in microseconds}. Lua only has
-- float64 numbers; Limit.Validate keeps every value at or below 2^53, where
-- integer addition, subtraction and multiplication are exact, and floordiv
-- below explains why division is exact too. The result is bit-for-bit the
-- same as the Go reference, which the tests check.
--
-- KEYS[1]  bucket key
-- ARGV[1]  rate   (tokens per period)
-- ARGV[2]  period (microseconds)
-- ARGV[3]  burst  (tokens)
-- ARGV[4]  cost   (tokens, 0..burst)
-- ARGV[5]  optional "now" in microseconds; tests only. Production uses the
--          Redis clock, the one clock every instance shares. With an
--          injected clock no TTL is set: it would be measured on the Redis
--          clock and expire state the injected timeline still needs.
--
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}.

local rate = tonumber(ARGV[1])
local per = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local cost = tonumber(ARGV[4]) * per
local capacity = burst * per

local now
local injected = ARGV[5] ~= nil
if injected then
  now = tonumber(ARGV[5])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
end

-- floor(a / b) for integers 0 <= a < 2^53, 0 < b. Exact: when a/b is not an
-- integer it is at least 1/b away from the next one, while '/' rounds by at
-- most half an ulp of the quotient, which is below 2^53/b * 2^-53 = 1/b.
-- Rounding can therefore never cross an integer, and math.floor is exact.
local function floordiv(a, b)
  return math.floor(a / b)
end

local function ceildiv(a, b)
  if a <= 0 then
    return 0
  end
  return floordiv(a + b - 1, b)
end

local level, stamp
local state = redis.call('HMGET', KEYS[1], 'l', 's')
if state[1] then
  level = tonumber(state[1])
  stamp = tonumber(state[2])
else
  level = capacity
  stamp = now
end

-- Refill. A caller whose clock is behind the stamp earns nothing until it
-- catches up, so clock skew can delay tokens but never mint them.
if now > stamp then
  local elapsed = now - stamp
  if elapsed >= ceildiv(capacity - level, rate) then
    level = capacity
  else
    level = level + elapsed * rate
  end
  stamp = now
end

local lag = 0
if stamp > now then
  lag = stamp - now
end

local allowed, retry = 0, 0
if level >= cost then
  level = level - cost
  allowed = 1
else
  retry = lag + ceildiv(cost - level, rate)
end

local reset = 0
if level < capacity then
  reset = lag + ceildiv(capacity - level, rate)
end

if reset == 0 then
  -- A full bucket is the same as no bucket: keep nothing.
  redis.call('DEL', KEYS[1])
else
  -- %.0f, not tostring: tostring keeps only 14 significant digits.
  redis.call('HSET', KEYS[1], 'l', string.format('%.0f', level), 's', string.format('%.0f', stamp))
  -- Expire exactly when the bucket would be full again, rounded up to a
  -- millisecond: Redis then forgets idle keys on its own.
  if not injected then
    redis.call('PEXPIRE', KEYS[1], ceildiv(reset, 1000))
  end
end

return {allowed, floordiv(level, per), retry, reset}
