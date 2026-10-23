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
})
```

## Token Bucket Algorithm

The store uses a Lua script for atomic check-and-decrement:

1. Read the current tokens and last timestamp from the Redis hash. A missing key starts full, at `Burst` tokens.
2. Calculate token refill based on elapsed time: `refill = (elapsed * rate) / period`
3. Cap tokens at the burst limit
4. If tokens >= 1: decrement and allow
5. If tokens < 1: reject

The time comes from the Redis server (`TIME`), not from the pod, so pods with different clocks share one consistent bucket.

## Redis Key Structure

Keys are Redis hashes with two fields:

| Field | Description |
|---|---|
| `tokens` | Current available tokens |
| `ts` | Last update timestamp (milliseconds) |

Every call, allowed or rejected, sets the key to expire after the configured period.

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

## Errors

`Allow` returns `(false, err)` when the script fails, for example when Redis is unreachable. The rate limiter invoker does not let the message through on a store error: it reports a `StoreError` signal (`Store` `rate_limit`, `Op` `allow`) and returns the error, so the handler does not run and the transport retries the delivery. A rejection by the bucket is different: it returns `ErrRateLimited`, see the [Rate Limiter]({{ site.baseurl }}/invoker-chain/rate-limiter/) page. The signal names are on the [Observability]({{ site.baseurl }}/observability/) pages.
