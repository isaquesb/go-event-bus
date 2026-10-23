---
layout: default
title: Full Invoker Chain
parent: Examples
nav_order: 2
---

# Full Invoker Chain

A production-style invoker chain with tracing, rate limiting, idempotency, DLQ, retry and a circuit breaker, in the recommended order:

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

The reason for each position is on the [Composition]({{ site.baseurl }}/invoker-chain/composition/) page. The code below is copied from the compiled example [`examples/02_invoker_chain.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/02_invoker_chain.go).

## Event with All Interfaces

```go
type PaymentProcessed struct {
	PaymentID string  `json:"payment_id"`
	UserID    string  `json:"user_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
}

func (e PaymentProcessed) Name() string { return "payment.processed" }

// Version for schema evolution
func (e PaymentProcessed) Version() int { return 1 }

// IdempotencyKey prevents duplicate processing
func (e PaymentProcessed) IdempotencyKey() string {
	return "payment:" + e.PaymentID
}

// RateLimitKey limits processing per user
func (e PaymentProcessed) RateLimitKey() string {
	return "user:" + e.UserID
}
```

## Mock Stores

The example uses in-memory stand-ins for the DLQ publisher and the rate limit store. Use the Redis stores and a real DLQ publisher in production.

```go
type mockDLQPublisher struct{}

func (m *mockDLQPublisher) Publish(ctx context.Context, rec event.DLQRecord) error {
	slog.Warn("event sent to DLQ",
		"event", rec.EventName,
		"reason", rec.Reason,
		"attempts", rec.Attempts,
		"delivery_count", rec.Delivery.Count,
		"cause", rec.Cause.Error(),
	)
	return nil
}

// mockRateLimitStore simulates rate limiting
type mockRateLimitStore struct {
	counts map[string]int
}

func (m *mockRateLimitStore) Allow(ctx context.Context, key string, rate int, period time.Duration, burst int) (bool, error) {
	if m.counts == nil {
		m.counts = make(map[string]int)
	}
	m.counts[key]++
	return m.counts[key] <= burst, nil
}
```

## Building the Chain

The same DLQ publisher goes to the chain (`invoker.NewDLQ`) and to the bus. The bus uses it as the backstop for failures the chain DLQ never sees, such as a rate-limit rejection on the final delivery.

```go
tracer := otel.Tracer("payment-service")

dlq := &mockDLQPublisher{}
chain := invoker.NewChain(
	// 1. Tracing - creates spans for each handler
	telemetry.NewTracerInvoker(tracer),

	// 2. Rate Limiting - prevents abuse
	invoker.NewRateLimiter(
		&mockRateLimitStore{},
		invoker.RateLimitConfig{
			Rate:      10,          // 10 requests
			Period:    time.Minute, // per minute
			Burst:     15,          // allow burst up to 15
			KeyPrefix: "payment:limit:",
			OnLimit: func(ctx context.Context, evt event.Event, key string) {
				slog.Warn("rate limit exceeded", "key", key)
			},
		},
	),

	// 3. Idempotency - prevents duplicate processing
	invoker.NewIdempotency(
		invoker.NewMemoryIdempotencyStore(), // Use Redis in production
		invoker.IdempotencyConfig{
			ProcessingTTL: 5 * time.Minute,
		},
	),

	// 4. DLQ - outside Retry: dead-letters the final outcome once
	invoker.NewDLQ(dlq),

	// 5. Retry - handles transient failures
	invoker.NewRetry(
		invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		},
	),

	// 6. Circuit Breaker - prevents cascade failures
	invoker.NewCircuitBreaker(
		invoker.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			OpenTimeout:      30 * time.Second,
		},
	),
)

// Create the bus with the chain. The same publisher is the bus backstop
// for failures the chain DLQ did not dead-letter.
bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	DLQPublisher:  dlq,
	MaxConcurrent: 16,
})
```

## Handler with Error Classification

```go
ctx := context.Background()

// Register handler
if err := bus.Subscribe(ctx, "payment.processed", func(ctx context.Context, evt event.Event) error {
	payment := evt.(PaymentProcessed)
	slog.Info("processing payment",
		"payment_id", payment.PaymentID,
		"amount", payment.Amount,
	)

	// Simulate different outcomes
	if payment.Amount > 10000 {
		// Terminal error - goes to DLQ
		return invoker.PermanentError{
			Err: errors.New("amount exceeds limit, requires manual review"),
		}
	}

	if payment.Amount < 0 {
		// Retryable error - will be retried
		return invoker.RetryableError{
			Err: errors.New("negative amount, possible race condition"),
		}
	}

	return nil
}, event.WithHandlerName("update-balance")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
```
