// Example: Error Handling Patterns
//
// This example demonstrates the different error types and how
// they are handled by the invoker chain.
package examples

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// =============================================================================
// Error Classification
// =============================================================================

/*
Error Types and Their Behavior:

1. TRANSIENT ERRORS (default)
   - Retried up to MaxAttempts
   - Examples: network timeout, database connection, rate limit upstream
   - Use: invoker.RetryableError{Err: err}

2. PERMANENT ERRORS (Terminal)
   - Never retried
   - Sent directly to DLQ
   - Examples: validation error, business rule violation, malformed data
   - Use: invoker.PermanentError{Err: err}

3. POLICY ERRORS (Not retried, not DLQ)
   - Handled specially, not retried
   - invoker.ErrDuplicate - idempotency check failed
   - invoker.ErrRateLimited - rate limit exceeded
   - invoker.ErrCircuitOpen - circuit breaker is open
   - context.Canceled / context.DeadlineExceeded
*/

// =============================================================================
// Custom Error Types
// =============================================================================

// ValidationError represents a data validation failure
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("validation failed on %s: %s", e.Field, e.Message)
}

// Terminal marks this as a permanent error
func (e ValidationError) Terminal() bool { return true }

// NetworkError represents a transient network failure
type NetworkError struct {
	Operation string
	Err       error
}

func (e NetworkError) Error() string {
	return fmt.Sprintf("network error during %s: %v", e.Operation, e.Err)
}

func (e NetworkError) Unwrap() error { return e.Err }

// Retryable explicitly marks this as retryable
func (e NetworkError) Retryable() bool { return true }

// BusinessRuleViolation represents a business logic error
type BusinessRuleViolation struct {
	Rule    string
	Details string
}

func (e BusinessRuleViolation) Error() string {
	return fmt.Sprintf("business rule violated [%s]: %s", e.Rule, e.Details)
}

func (e BusinessRuleViolation) Terminal() bool { return true }

// =============================================================================
// Event Definition
// =============================================================================

type PaymentRequest struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
	CardToken string  `json:"card_token"`
}

func (e PaymentRequest) Name() string           { return "payment.request" }
func (e PaymentRequest) IdempotencyKey() string { return "payment:" + e.PaymentID }

// =============================================================================
// Handler with Error Classification
// =============================================================================

func ProcessPayment(ctx context.Context, evt event.Event) error {
	payment := evt.(PaymentRequest)

	// ==========================================================================
	// Validation Errors -> Permanent (goes to DLQ)
	// ==========================================================================
	if payment.Amount <= 0 {
		return ValidationError{
			Field:   "amount",
			Message: "must be positive",
		}
	}

	if payment.Currency == "" {
		return invoker.PermanentError{
			Err: errors.New("currency is required"),
		}
	}

	// ==========================================================================
	// Business Rule Violations -> Permanent (goes to DLQ)
	// ==========================================================================
	if payment.Amount > 10000 {
		return BusinessRuleViolation{
			Rule:    "MAX_TRANSACTION_LIMIT",
			Details: "single transaction cannot exceed $10,000",
		}
	}

	// ==========================================================================
	// Network Errors -> Retryable
	// ==========================================================================
	result, err := callPaymentGateway(ctx, payment)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return NetworkError{
				Operation: "payment_gateway",
				Err:       err,
			}
		}

		// Unknown network error - mark as retryable
		return invoker.RetryableError{Err: err}
	}

	// ==========================================================================
	// Gateway Response Handling
	// ==========================================================================
	switch result.Status {
	case "approved":
		slog.Info("payment approved", "payment_id", payment.PaymentID)
		return nil

	case "declined":
		// Declined is permanent - don't retry
		return invoker.PermanentError{
			Err: fmt.Errorf("payment declined: %s", result.Reason),
		}

	case "pending":
		// Pending might resolve - retry later
		return invoker.RetryableError{
			Err: errors.New("payment is pending, will retry"),
		}

	default:
		// Unknown status - be safe, retry
		return fmt.Errorf("unknown payment status: %s", result.Status)
	}
}

// Mock payment gateway
type GatewayResult struct {
	Status string
	Reason string
}

func callPaymentGateway(ctx context.Context, p PaymentRequest) (*GatewayResult, error) {
	// Simulate gateway call
	return &GatewayResult{Status: "approved"}, nil
}

// =============================================================================
// Example: Custom DLQ Publisher with Error Context
// =============================================================================

type EnhancedDLQPublisher struct {
	// Your DLQ backend (NATS, Kafka, database, etc.)
}

func (p *EnhancedDLQPublisher) Publish(ctx context.Context, evt event.Event, cause error) error {
	// Extract error details for debugging
	errorType := "unknown"
	isRetryable := true

	var validationErr ValidationError
	var businessErr BusinessRuleViolation
	var permErr invoker.PermanentError

	switch {
	case errors.As(cause, &validationErr):
		errorType = "validation"
		isRetryable = false
	case errors.As(cause, &businessErr):
		errorType = "business_rule"
		isRetryable = false
	case errors.As(cause, &permErr):
		errorType = "permanent"
		isRetryable = false
	}

	slog.Warn("publishing to DLQ",
		"event", evt.Name(),
		"error_type", errorType,
		"retryable", isRetryable,
		"cause", cause.Error(),
	)

	// ... publish to DLQ with metadata
	return nil
}

// =============================================================================
// Example Usage
// =============================================================================

func ExampleErrorHandling() {
	dlq := &EnhancedDLQPublisher{}

	chain := invoker.NewChain(
		invoker.NewMetrics(nil),

		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		}, nil),

		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			OpenTimeout:      30 * time.Second,
		}, nil),

		invoker.NewDLQ(dlq, nil),
	)

	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 16,
	})

	ctx := context.Background()

	bus.Subscribe(ctx, "payment.request", ProcessPayment, event.WithHandlerName("process-payment"))

	// Valid payment - succeeds
	slog.Info("=== Valid payment ===")
	bus.EmitSync(ctx, PaymentRequest{
		PaymentID: "pay-001",
		Amount:    100.00,
		Currency:  "USD",
		CardToken: "tok_xxx",
	})

	// Invalid amount - permanent error -> DLQ
	slog.Info("=== Invalid amount (permanent) ===")
	bus.EmitSync(ctx, PaymentRequest{
		PaymentID: "pay-002",
		Amount:    -50.00,
		Currency:  "USD",
		CardToken: "tok_xxx",
	})

	// Business rule violation - permanent -> DLQ
	slog.Info("=== Exceeds limit (business rule) ===")
	bus.EmitSync(ctx, PaymentRequest{
		PaymentID: "pay-003",
		Amount:    50000.00,
		Currency:  "USD",
		CardToken: "tok_xxx",
	})

	// Missing currency - permanent -> DLQ
	slog.Info("=== Missing currency (validation) ===")
	bus.EmitSync(ctx, PaymentRequest{
		PaymentID: "pay-004",
		Amount:    100.00,
		Currency:  "",
		CardToken: "tok_xxx",
	})
}

// =============================================================================
// Error Classification Summary
// =============================================================================

/*
+------------------------+-------------+--------+-----+
| Error Type             | Retried?    | DLQ?   | CB? |
+------------------------+-------------+--------+-----+
| Default (unknown)      | Yes         | After  | Yes |
| RetryableError         | Yes         | After  | Yes |
| PermanentError         | No          | Yes    | No  |
| Terminal interface     | No          | Yes    | No  |
| Retryable interface    | Depends     | After  | Yes |
| ErrDuplicate           | No          | No     | No  |
| ErrRateLimited         | No          | No     | No  |
| ErrCircuitOpen         | No          | No     | N/A |
| context.Canceled       | No          | No     | No  |
| context.DeadlineExc.   | No          | No     | No  |
| ErrSendToDLQ           | No          | N/A    | No  |
+------------------------+-------------+--------+-----+

CB? = Counts as failure for Circuit Breaker
After = Sent to DLQ only after retry exhaustion
*/
