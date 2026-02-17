---
layout: default
title: Full Invoker Chain
parent: Examples
nav_order: 2
---

# Full Invoker Chain

A production-ready invoker chain with tracing, metrics, rate limiting, idempotency, retry, circuit breaker, and DLQ.

Source: [`examples/02_invoker_chain.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/02_invoker_chain.go)

## Event with All Interfaces

```go
type PaymentProcessed struct {
    PaymentID string  `json:"payment_id"`
    UserID    string  `json:"user_id"`
    Amount    float64 `json:"amount"`
    Currency  string  `json:"currency"`
}

func (e PaymentProcessed) Name() string           { return "payment.processed" }
func (e PaymentProcessed) Version() int            { return 1 }
func (e PaymentProcessed) IdempotencyKey() string  { return "payment:" + e.PaymentID }
func (e PaymentProcessed) RateLimitKey() string    { return "user:" + e.UserID }
```

## Building the Chain

```go
chain := invoker.NewChain(
    // 1. Tracing - creates spans for each handler
    telemetry.NewTracerInvoker(tracer),

    // 2. Metrics - records latency and success/failure
    invoker.NewMetrics(metrics),

    // 3. Rate Limiting - prevents abuse
    invoker.NewRateLimiter(rateLimitStore, invoker.RateLimitConfig{
        Rate:   10,
        Period: time.Minute,
        Burst:  15,
    }, metrics),

    // 4. Idempotency - prevents duplicate processing
    invoker.NewIdempotency(
        invoker.NewMemoryIdempotencyStore(),
        invoker.IdempotencyConfig{ProcessingTTL: 5 * time.Minute},
        metrics,
    ),

    // 5. Retry - handles transient failures
    invoker.NewRetry(invoker.RetryPolicy{
        MaxAttempts: 3,
        BaseDelay:   100 * time.Millisecond,
        MaxDelay:    5 * time.Second,
    }, metrics),

    // 6. Circuit Breaker - prevents cascade failures
    invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
        FailureThreshold: 5,
        SuccessThreshold: 2,
        OpenTimeout:      30 * time.Second,
    }, metrics),

    // 7. DLQ - handles terminal failures
    invoker.NewDLQ(dlqPublisher, metrics),
)
```

## Handler with Error Classification

```go
bus.Subscribe(ctx, "payment.processed", func(ctx context.Context, evt event.Event) error {
    payment := evt.(PaymentProcessed)

    if payment.Amount > 10000 {
        // Terminal error → DLQ
        return invoker.PermanentError{
            Err: errors.New("amount exceeds limit, requires manual review"),
        }
    }

    if payment.Amount < 0 {
        // Retryable error → will be retried
        return invoker.RetryableError{
            Err: errors.New("negative amount, possible race condition"),
        }
    }

    return nil
}, event.WithHandlerName("update-balance"))
```
