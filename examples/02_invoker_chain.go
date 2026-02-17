// Example: Full Invoker Chain
//
// This example demonstrates how to build a production-ready invoker chain
// with all middleware components: tracing, metrics, rate limiting,
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

func (m *mockDLQPublisher) Publish(ctx context.Context, evt event.Event, cause error) error {
	slog.Warn("event sent to DLQ",
		"event", evt.Name(),
		"cause", cause.Error(),
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

// mockMetricProvider collects metrics
type mockMetricProvider struct{}

func (m *mockMetricProvider) IncCounter(name string, n int64, labels invoker.Labels) {
	slog.Debug("metric counter", "name", name, "value", n, "labels", labels)
}

func (m *mockMetricProvider) ObserveHistogram(name string, value float64, labels invoker.Labels) {
	slog.Debug("metric histogram", "name", name, "value", value, "labels", labels)
}

func (m *mockMetricProvider) SetGauge(name string, value float64, labels invoker.Labels) {
	slog.Debug("metric gauge", "name", name, "value", value, "labels", labels)
}

// =============================================================================
// Building the Invoker Chain
// =============================================================================

func ExampleFullInvokerChain() {
	metrics := &mockMetricProvider{}
	tracer := otel.Tracer("payment-service")

	// Build the chain in recommended order:
	// 1. Observability (tracing, metrics) - wrap everything
	// 2. Cheap rejections (rate limit, idempotency) - fail fast
	// 3. Resilience (retry, circuit breaker) - handle failures
	// 4. Terminal (DLQ) - last resort
	chain := invoker.NewChain(
		// 1. Tracing - creates spans for each handler
		telemetry.NewTracerInvoker(tracer),

		// 2. Metrics - records latency and success/failure
		invoker.NewMetrics(metrics),

		// 3. Rate Limiting - prevents abuse
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
			metrics,
		),

		// 4. Idempotency - prevents duplicate processing
		invoker.NewIdempotency(
			invoker.NewMemoryIdempotencyStore(), // Use Redis in production
			invoker.IdempotencyConfig{
				ProcessingTTL: 5 * time.Minute,
			},
			metrics,
		),

		// 5. Retry - handles transient failures
		invoker.NewRetry(
			invoker.RetryPolicy{
				MaxAttempts: 3,
				BaseDelay:   100 * time.Millisecond,
				MaxDelay:    5 * time.Second,
			},
			metrics,
		),

		// 6. Circuit Breaker - prevents cascade failures
		invoker.NewCircuitBreaker(
			invoker.CircuitBreakerConfig{
				FailureThreshold: 5,
				SuccessThreshold: 2,
				OpenTimeout:      30 * time.Second,
			},
			metrics,
		),

		// 7. DLQ - handles terminal failures
		invoker.NewDLQ(&mockDLQPublisher{}, metrics),
	)

	// Create the bus with the chain
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 16,
	})

	ctx := context.Background()

	// Register handler
	bus.Subscribe(ctx, "payment.processed", func(ctx context.Context, evt event.Event) error {
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
	}, event.WithHandlerName("update-balance"))

	// Emit events

	// Normal payment - processed successfully
	bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-001",
		UserID:    "user-123",
		Amount:    100.00,
		Currency:  "USD",
	})

	// Same payment again - blocked by idempotency
	bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-001",
		UserID:    "user-123",
		Amount:    100.00,
		Currency:  "USD",
	})

	// Large payment - sent to DLQ
	bus.EmitSync(ctx, PaymentProcessed{
		PaymentID: "pay-002",
		UserID:    "user-123",
		Amount:    50000.00,
		Currency:  "USD",
	})
}
