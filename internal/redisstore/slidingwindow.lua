-- Sliding-window counter check, run atomically inside Redis.
--
-- A port of algo.SlidingWindow; see tokenbucket.lua for why float64
-- arithmetic is exact here (algo.WindowSpan keeps burst * span below 2^53).
-- State is one string "<stamp>:<curr>:<prev>": the time of the last counted
-- event, the count of its fixed window and the count of the window before.
--
-- KEYS[1]  state key
-- ARGV[1]  rate   (events per period)
-- ARGV[2]  period (microseconds)
-- ARGV[3]  burst  (events)
-- ARGV[4]  cost   (events, 0..burst)
-- ARGV[5]  optional "now" in microseconds; tests only, and then no TTL.
--
-- Returns {allowed (0|1), remaining, retry_after_us, reset_after_us}.

local rate = tonumber(ARGV[1])
local per = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])
local n = tonumber(ARGV[4])

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

-- The time burst events take at the average rate.
local span = ceildiv(burst * per, rate)

local stamp, curr, prev = now, 0, 0
local state = redis.call('GET', KEYS[1])
if state then
  local s, c, p = string.match(state, '^(%d+):(%d+):(%d+)$')
  stamp, curr, prev = tonumber(s), tonumber(c), tonumber(p)
end

-- A clock behind the last event is treated as being at it.
local at = math.max(now, stamp)
local lag = at - now

-- Roll the counts forward to the window containing at.
local start = floordiv(at, span) * span
local last = floordiv(stamp, span) * span
if start == last + span then
  prev, curr = curr, 0
elseif start ~= last then
  prev, curr = 0, 0
end
local off = at - start
local limit = burst * span
local used = prev * (span - off) + curr * span

local allowed, retry, write = 0, 0, false
if n * span <= limit - used then
  allowed = 1
  if n > 0 then
    curr = curr + n
    used = used + n * span
    write = true
  end
else
  -- The estimate only falls with time. Earliest fit in this window:
  -- prev * (span - t) <= (burst - curr - n) * span.
  local wait
  local room = (burst - curr - n) * span
  if room >= 0 and prev > 0 and floordiv(room, prev) > 0 then
    wait = span - floordiv(room, prev) - off
  else
    -- In the next one, where curr becomes prev:
    -- curr * (span - t) <= (burst - n) * span; the one after is empty.
    local t = 0
    if curr > 0 then
      t = math.max(0, span - floordiv((burst - n) * span, curr))
    end
    if t < span then
      wait = span - off + t
    else
      wait = 2 * span - off
    end
  end
  retry = lag + wait
end

local reset = 0
if curr > 0 then
  reset = lag + 2 * span - off
elseif prev > 0 then
  reset = lag + span - off
end

if reset == 0 then
  redis.call('DEL', KEYS[1])
elseif write then
  local v = string.format('%.0f:%.0f:%.0f', at, curr, prev)
  if injected then
    redis.call('SET', KEYS[1], v)
  else
    -- Expire when both windows have slid out and the burst is back.
    redis.call('SET', KEYS[1], v, 'PX', ceildiv(reset, 1000))
  end
end

return {allowed, floordiv(limit - used, span), retry, reset}
