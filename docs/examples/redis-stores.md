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
// OrderCreated represents an order creation event
type OrderCreated struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Total      float64 `json:"total"`
}

func (e OrderCreated) Name() string { return "order.created" }

// IdempotencyKey prevents duplicate order processing
func (e OrderCreated) IdempotencyKey() string {
	return "order:" + e.OrderID
}

// RateLimitKey limits order creation per customer
func (e OrderCreated) RateLimitKey() string {
	return "customer:" + e.CustomerID
}
```

## Redis Setup

```go
// Connect to Redis
rdb := redislib.NewClient(&redislib.Options{
	Addr:         "localhost:6379",
	Password:     "",
	DB:           0,
	PoolSize:     100,
	MinIdleConns: 10,
	MaxRetries:   3,
	DialTimeout:  5 * time.Second,
	ReadTimeout:  3 * time.Second,
	WriteTimeout: 3 * time.Second,
})

// Verify connection
if err := rdb.Ping(ctx).Err(); err != nil {
	slog.Error("failed to connect to Redis", "error", err)
	return
}
defer func() { _ = rdb.Close() }()

// Create Redis-backed stores
idempotencyStore := redis.NewIdempotencyStore(rdb)
rateLimitStore := redis.NewRedisRateLimitStore(rdb)
```

## Invoker Chain

```go
// Build production-ready invoker chain
chain := invoker.NewChain(
	// Rate limiting - prevent abuse
	invoker.NewRateLimiter(
		rateLimitStore,
		invoker.RateLimitConfig{
			Rate:      100,             // 100 orders
			Period:    time.Hour,       // per hour
			Burst:     10,              // burst up to 10
			KeyPrefix: "orders:limit:", // Redis key prefix
			OnLimit: func(ctx context.Context, evt event.Event, key string) {
				slog.Warn("customer rate limited",
					"key", key,
					"event", evt.Name(),
				)
			},
		},
	),

	// Idempotency - prevent duplicate processing
	invoker.NewIdempotency(
		idempotencyStore,
		invoker.IdempotencyConfig{
			ProcessingTTL: 5 * time.Minute, // Lock expires after 5 min
		},
	),

	// Retry with backoff
	invoker.NewRetry(
		invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    10 * time.Second,
		},
	),

	// Circuit breaker for downstream protection
	invoker.NewCircuitBreaker(
		invoker.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			OpenTimeout:      30 * time.Second,
		},
	),
)

// Create bus
bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	MaxConcurrent: 32,
	// Observer receives every terminal failure once (Observer.Failure). The slog
	// observer is also the default when Observer is nil.
	Observer: event.NewSlogObserver(nil),
})
```

This chain omits the Tracer and DLQ invokers for brevity but keeps the canonical relative order. See [Invoker Chain Composition]({{ site.baseurl }}/invoker-chain/composition/).

## Redis Key Behavior

### Idempotency Keys

```
Redis Key Structure:
  - Key: "order:order-12345:process-order"
  - Value: JSON { status, started_at, ended_at }
  - TTL: Based on status
    - processing: 5 minutes (lock recovery)
    - completed: 24 hours (duplicate prevention)
    - failed: 1 hour (allow retry)

Example Redis commands:
  SETNX order:order-12345:process-order '{"status":"processing","started_at":"..."}'
  EXPIRE order:order-12345:process-order 300

After completion:
  SET order:order-12345:process-order '{"status":"completed","started_at":"...","ended_at":"..."}'
  EXPIRE order:order-12345:process-order 86400
```

### Rate Limit Keys

```
Token Bucket Algorithm (Lua script):

1. Get current tokens and timestamp
2. Calculate refill since last request
3. If tokens available, decrement and allow
4. If no tokens, reject

Redis Key Structure:
  - Key: "orders:limit:customer:cust-789"
  - Value: Hash { tokens, ts }
  - TTL: Period duration

Example flow:
  Request 1: tokens=100 -> allowed, tokens=99
  Request 2: tokens=99  -> allowed, tokens=98
  ...
  Request 101: tokens=0 -> REJECTED

After period:
  Tokens refill based on elapsed time
```
