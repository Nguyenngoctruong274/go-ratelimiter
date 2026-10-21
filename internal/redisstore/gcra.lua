-- GCRA check, run atomically inside Redis.
--
-- A port of algo.GCRA; see tokenbucket.lua for why float64 arithmetic is
-- exact here. State is one string "<micros>:<frac>": the theoretical
-- arrival time (TAT) as the exact fraction micros + frac/rate. The key
-- expires at the TAT, when it would mean "full burst" anyway.
--
-- KEYS[1]  state key
-- ARGV[1]  rate   (tokens per period)
-- ARGV[2]  period (microseconds)
-- ARGV[3]  burst  (tokens)
-- ARGV[4]  cost   (tokens, 0..burst)
-- ARGV[5]  optional "now" in microseconds; tests only, and then no TTL.
--
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}.

local rate = tonumber(ARGV[1])
local per = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local n = tonumber(ARGV[4])
local cost = n * per
local capacity = burst * per

local now
local injected = ARGV[5] ~= nil
if injected then
  now = tonumber(ARGV[5])
else
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000000 + tonumber(t[2])
end

-- Exact for integers 0 <= a < 2^53 and b > 0: see tokenbucket.lua.
local function floordiv(a, b)
  return math.floor(a / b)
end

local function ceildiv(a, b)
  if a <= 0 then
    return 0
  end
  return floordiv(a + b - 1, b)
end

-- The TAT is q whole microseconds and frac/rate ahead of now; a TAT in the
-- past is no debt.
local q, frac = 0, 0
local state = redis.call('GET', KEYS[1])
if state then
  local m, f = string.match(state, '^(%d+):(%d+)$')
  m = tonumber(m)
  if m >= now then
    q, frac = m - now, tonumber(f)
  end
end

-- More debt than capacity only comes from a clock behind the writer's.
-- Decide without computing q * rate, which could leave the exact range.
if q > floordiv(capacity - frac, rate) then
  local allowed, retry = 0, 0
  if n == 0 then
    allowed = 1
  else
    -- ceil((frac + cost - capacity) / rate), for a possibly negative numerator
    local x = frac + cost - capacity
    if x > 0 then
      retry = q + ceildiv(x, rate)
    else
      retry = q - floordiv(-x, rate)
    end
  end
  local reset = q
  if frac > 0 then
    reset = q + 1
  end
  return {allowed, 0, retry, reset}
end

local debt = q * rate + frac
local allowed, retry, write = 0, 0, false
if debt <= capacity - cost then
  allowed = 1
  if n > 0 then
    debt = debt + cost
    write = true
  end
else
  retry = ceildiv(debt - (capacity - cost), rate)
end
local reset = ceildiv(debt, rate)

if reset == 0 then
  -- A TAT in the past is the same as none.
  redis.call('DEL', KEYS[1])
elseif write then
  -- Only a grant writes: denials and peeks are pure reads.
  local whole = floordiv(debt, rate)
  local tat = string.format('%.0f', now + whole) .. ':' .. string.format('%.0f', debt - whole * rate)
  if injected then
    redis.call('SET', KEYS[1], tat)
  else
    redis.call('SET', KEYS[1], tat, 'PX', ceildiv(reset, 1000))
  end
end

return {allowed, floordiv(capacity - debt, per), retry, reset}
