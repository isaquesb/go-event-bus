---
layout: default
title: Rate Limiter
parent: Invoker Chain
nav_order: 4
---

# Rate Limiter Invoker

The rate limiter controls event processing throughput using dynamic keys.

## Configuration

```go
invoker.NewRateLimiter(
    store,   // RateLimitStore implementation
    invoker.RateLimitConfig{
        Rate:      10,              // tokens per period
        Period:    time.Minute,     // window duration
        Burst:     15,              // max burst capacity
        KeyPrefix: "orders:limit:", // Redis key prefix
        OnLimit: func(ctx context.Context, evt event.Event, key string) {
            slog.Warn("rate limit exceeded", "key", key)
        },
    },
    metrics,
)
```

| Field | Type | Description |
|---|---|---|
| `Rate` | `int` | Tokens replenished per period |
| `Period` | `time.Duration` | Replenishment window |
| `Burst` | `int` | Maximum token capacity |
| `KeyPrefix` | `string` | Prefix for store keys |
| `OnLimit` | `func(...)` | Optional callback when limited |

## RateLimitStore Interface

```go
type RateLimitStore interface {
    Allow(ctx context.Context, key string, rate int, period time.Duration, burst int) (bool, error)
}
```

Implementations:
- **In-memory**: Build your own for testing
- **Redis**: `redis.NewRedisRateLimitStore(client)` (production)

## Dynamic Keys via WithRateLimitKey

Events implement `WithRateLimitKey` to define their rate limit scope:

```go
type OrderCreated struct {
    CustomerID string
}

func (e OrderCreated) RateLimitKey() string {
    return "customer:" + e.CustomerID
}
```

Events without `WithRateLimitKey` **skip rate limiting entirely** and proceed to the next invoker.

## Behavior

- If `RateLimitKey()` returns empty → skip
- If `Allow()` returns true → proceed
- If `Allow()` returns false → return `ErrRateLimited`

## Metrics

| Metric | Type | Description |
|---|---|---|
| `eventbus_ratelimit_allowed_total` | Counter | Requests allowed |
| `eventbus_ratelimit_blocked_total` | Counter | Requests blocked |
| `eventbus_ratelimit_error_total` | Counter | Store errors |
