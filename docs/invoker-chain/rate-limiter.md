---
layout: default
title: Rate Limiter
parent: Invoker Chain
nav_order: 4
---

# Rate Limiter Invoker

The rate limiter controls event processing throughput using dynamic keys. An event over its rate is rejected with `ErrRateLimited` before the handler runs, and the transport retries it later.

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
)
```

| Field | Type | Description |
|---|---|---|
| `Rate` | `int` | Tokens replenished per period |
| `Period` | `time.Duration` | Replenishment window |
| `Burst` | `int` | Maximum token capacity |
| `KeyPrefix` | `string` | Prefix for store keys |
| `OnLimit` | `func(...)` | Optional callback when an event is rejected |

The rate limiter sits right after the tracer, before Idempotency, so a rejected message takes no idempotency lock. The reason for each position is on the [composition page]({{ site.baseurl }}/invoker-chain/composition/).

## RateLimitStore Interface

```go
type RateLimitStore interface {
    Allow(ctx context.Context, key string, rate int, period time.Duration, burst int) (bool, error)
}
```

Implementations:
- **In-memory**: build your own for testing
- **Redis**: `redis.NewRedisRateLimitStore(client)` (production), see [Redis Rate Limit Store]({{ site.baseurl }}/distributed-stores/redis-rate-limit/)

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

- If `RateLimitKey()` returns empty: skip.
- If `Allow()` returns true: proceed.
- If `Allow()` returns false: call `OnLimit`, then return `ErrRateLimited`. The handler does not run.
- If `Allow()` returns an error: the store failed, which is not a rate limit. The error is returned as it is, the handler does not run, and a `StoreError` signal is emitted (below).

## What the transport does with ErrRateLimited

`ErrRateLimited` is a flow-control failure: the handler never ran, so the message is retried later, never lost on the first rejection.

- **JetStream**: the message is `NakWithDelay`ed with `BusOptions.RateLimitDelay` (5 s when unset, with a start-up warning). Set it to a value that fits your rate-limit window.
- **Final delivery**: the rate limiter sits outside the DLQ invoker, so the chain never sees the rejection as a candidate for dead-lettering. The **transport backstop** dead-letters it on the final delivery with the publisher set on the bus (`BusOptions.DLQPublisher`). Without one, the message is dropped and reported. See the [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/#publisher-on-the-invoker-and-on-the-bus) page.
- **LocalBus and NATS Core**: there is a single delivery, so a rejection is already the final one.

The result for every transport is in the [failure model]({{ site.baseurl }}/architecture/failure-model/).

## Signals

| Signal | When |
|---|---|
| `StoreError` | `Allow` returned an error. `Store` is `rate_limit` and `Op` is `allow` |
| `HandlerDone` | Reports the outcome of the invocation. A rejected delivery that will be retried is `rate_limited`. On the final delivery the outcome is what really happened to the message: `dead_lettered`, or `dropped`, or `failed` when the dead-letter publish failed |

A rejection emits no diagnostic signal of its own: it is a `HandlerDone` outcome. The signal and metric names are on the [Observability]({{ site.baseurl }}/observability/) pages. The rate limiter keeps no metrics.
