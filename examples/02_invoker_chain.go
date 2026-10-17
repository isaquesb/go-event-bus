// Example: Full Invoker Chain
//
// This example demonstrates how to build a production-ready invoker chain
// with all middleware components: tracing, rate limiting,
// idempotency, retry, circuit breaker, and DLQ.
package examples

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/isaquesb/go-event-bus/telemetry"

	"go.opentelemetry.io/otel"
)

// =============================================================================
// Event with all optional interfaces
// =============================================================================

// PaymentProcessed demonstrates an event with idempotency and rate limit keys
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

// =============================================================================
// Mock implementations for example
// =============================================================================

// mockDLQPublisher simulates DLQ publishing
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

// =============================================================================
// Building the Invoker Chain
// =============================================================================

func ExampleFullInvokerChain() {
	tracer := otel.Tracer("payment-service")

	// Build the chain in recommended order:
	// 1. Observability (tracing) - wrap everything
	// 2. Cheap rejections (rate limit, idempotency) - fail fast
	// 3. DLQ - outside Retry, so it dead-letters the final outcome once
	// 4. Resilience (retry, circuit breaker) - handle failures
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

	// Emit events

	// Normal payment - processed successfully
	if _, err := bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-001",
		UserID:    "user-123",
		Amount:    100.00,
		Currency:  "USD",
	}); err != nil {
		slog.Error("emit sync failed", "error", err)
	}

	// Same payment again - blocked by idempotency
	if _, err := bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-001",
		UserID:    "user-123",
		Amount:    100.00,
		Currency:  "USD",
	}); err != nil {
		slog.Error("emit sync failed", "error", err)
	}

	// Large payment - sent to DLQ
	if _, err := bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-002",
		UserID:    "user-123",
		Amount:    50000.00,
		Currency:  "USD",
	}); err != nil {
		slog.Error("emit sync failed", "error", err)
	}
}
