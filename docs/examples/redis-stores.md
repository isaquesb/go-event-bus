---
layout: default
title: Redis Stores
parent: Examples
nav_order: 6
---

# Redis Stores

Production setup with Redis-backed idempotency and rate limiting.

Source: [`examples/06_redis_stores.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/06_redis_stores.go)

## Event with Keys

```go
type OrderCreated struct {
    OrderID    string  `json:"order_id"`
    CustomerID string  `json:"customer_id"`
    Total      float64 `json:"total"`
}

func (e OrderCreated) Name() string              { return "order.created" }
func (e OrderCreated) IdempotencyKey() string     { return "order:" + e.OrderID }
func (e OrderCreated) RateLimitKey() string       { return "customer:" + e.CustomerID }
```

## Redis Setup

```go
rdb := redislib.NewClient(&redislib.Options{
    Addr:     "localhost:6379",
    PoolSize: 100,
})

idempotencyStore := redis.NewIdempotencyStore(rdb)
rateLimitStore := redis.NewRedisRateLimitStore(rdb)
```

## Invoker Chain

```go
chain := invoker.NewChain(
    invoker.NewMetrics(nil),

    invoker.NewRateLimiter(rateLimitStore, invoker.RateLimitConfig{
        Rate:      100,
        Period:    time.Hour,
        Burst:     10,
        KeyPrefix: "orders:limit:",
    }, nil),

    invoker.NewIdempotency(idempotencyStore, invoker.IdempotencyConfig{
        ProcessingTTL: 5 * time.Minute,
    }, nil),

    invoker.NewRetry(invoker.RetryPolicy{
        MaxAttempts: 3,
        BaseDelay:   100 * time.Millisecond,
        MaxDelay:    10 * time.Second,
    }, nil),

    invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
        FailureThreshold: 5,
        SuccessThreshold: 2,
        OpenTimeout:      30 * time.Second,
    }, nil),
)
```

## Redis Key Behavior

### Idempotency Keys

```
Key:   order:order-12345:process-order
Value: {"status":"completed","started_at":"...","ended_at":"..."}
TTL:   24h (completed), 5m (processing), 1h (failed)
```

### Rate Limit Keys

```
Key:    orders:limit:customer:cust-789
Fields: tokens=98, ts=1707100000000
TTL:    1h (period)
```
