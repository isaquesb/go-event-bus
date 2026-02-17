---
layout: default
title: Redis Rate Limit Store
parent: Distributed Stores
nav_order: 2
---

# Redis Rate Limit Store

The `redis.RateLimitStore` implements the `invoker.RateLimitStore` interface using a Lua-based token bucket algorithm for atomic, distributed rate limiting.

## Setup

```go
import (
    "github.com/redis/go-redis/v9"
    eventredis "github.com/isaquesb/go-event-bus/redis"
)

rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

store := eventredis.NewRedisRateLimitStore(rdb)
```

Use with the rate limiter invoker:

```go
invoker.NewRateLimiter(store, invoker.RateLimitConfig{
    Rate:      100,
    Period:    time.Hour,
    Burst:     10,
    KeyPrefix: "orders:limit:",
}, metrics)
```

## Token Bucket Algorithm

The store uses a Lua script for atomic check-and-decrement:

1. Get current tokens and last timestamp from Redis hash
2. Calculate token refill based on elapsed time: `refill = (elapsed * rate) / period`
3. Cap tokens at burst limit
4. If tokens >= 1: decrement and allow
5. If tokens < 1: reject

## Redis Key Structure

Keys are Redis hashes with two fields:

| Field | Description |
|---|---|
| `tokens` | Current available tokens |
| `ts` | Last update timestamp (milliseconds) |

Keys expire after the configured period.

## Example Flow

Configuration: `rate=100, period=1h, burst=10`

```
Request 1:  tokens=10  → allowed, tokens=9
Request 2:  tokens=9   → allowed, tokens=8
...
Request 11: tokens=0   → REJECTED
(time passes, tokens refill)
Request 12: tokens=2.7 → allowed, tokens=1.7
```

## Atomicity

The entire check-refill-decrement operation runs as a single Lua script, ensuring atomicity even under high concurrency across multiple pods.
