package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RateLimitStore struct {
	client *redis.Client
	script *redis.Script
}

func NewRedisRateLimitStore(c *redis.Client) *RateLimitStore {
	return &RateLimitStore{
		client: c,
		script: redis.NewScript(`
-- KEYS[1] = key
-- ARGV[1] = rate
-- ARGV[2] = period (ms)
-- ARGV[3] = burst

local rate   = tonumber(ARGV[1])
local period = tonumber(ARGV[2])
local burst  = tonumber(ARGV[3])

local time_result = redis.call("TIME")
local now = tonumber(time_result[1]) * 1000 + math.floor(tonumber(time_result[2]) / 1000)

local data = redis.call("HMGET", KEYS[1], "tokens", "ts")
local tokens = tonumber(data[1])
local ts = tonumber(data[2])

if tokens == nil then
	tokens = burst
	ts = now
end

local delta = math.max(0, now - ts)
local refill = (delta * rate) / period
tokens = math.min(burst, tokens + refill)

if tokens < 1 then
	redis.call("HMSET", KEYS[1], "tokens", tokens, "ts", now)
	redis.call("PEXPIRE", KEYS[1], period)
	return 0
end

tokens = tokens - 1
redis.call("HMSET", KEYS[1], "tokens", tokens, "ts", now)
redis.call("PEXPIRE", KEYS[1], period)

return 1
	`),
	}
}

func (s *RateLimitStore) Allow(
	ctx context.Context,
	key string,
	rate int,
	period time.Duration,
	burst int,
) (bool, error) {
	res, err := s.script.Run(
		ctx,
		s.client,
		[]string{key},
		rate,
		period.Milliseconds(),
		burst,
	).Int()

	if err != nil {
		return false, err
	}

	return res == 1, nil
}
