---
layout: default
title: Error Handling
parent: Examples
nav_order: 7
---

# Error Handling

Demonstrates error classification and how different error types flow through the invoker chain.

Source: [`examples/07_error_handling.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/07_error_handling.go)

## Custom Error Types

### Validation Error (Terminal)

```go
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
```

### Network Error (Retryable)

```go
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
```

## Handler with Classification

```go
func ProcessPayment(ctx context.Context, evt event.Event) error {
	payment := evt.(PaymentRequest)

	// Validation Errors -> Permanent (goes to DLQ)
	if payment.Amount <= 0 {
		return ValidationError{
			Field:   "amount",
			Message: "must be positive",
		}
	}

	// Business Rule Violations -> Permanent (goes to DLQ)
	if payment.Amount > 10000 {
		return BusinessRuleViolation{
			Rule:    "MAX_TRANSACTION_LIMIT",
			Details: "single transaction cannot exceed $10,000",
		}
	}

	// Network Errors -> Retryable
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

	// Gateway Response Handling
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
```

## Classification Summary

```
+------------------------+-------------+-----------------------+-----+
| Error Type             | Retried?    | DLQ?                  | CB? |
+------------------------+-------------+-----------------------+-----+
| Default (unknown)      | Yes         | After exhaustion      | Yes |
| RetryableError         | Yes         | After exhaustion      | Yes |
| PermanentError         | No          | Immediately           | No  |
| Terminal interface     | No          | Immediately           | No  |
| Retryable interface    | Depends     | After exhaustion      | Yes |
| ErrDuplicate           | No          | No (Acked)            | No  |
| ErrDeadLettered        | No          | No (Acked, Failure)   | No  |
| ErrRateLimited         | No (delay)  | Final delivery only   | No  |
| ErrCircuitOpen         | No (delay)  | Final delivery only   | N/A |
| ErrInProgress          | No (delay)  | Final delivery only   | No  |
| context.Canceled       | No          | Never                 | No  |
| context.DeadlineExc.   | No          | Never                 | No  |
| ErrSendToDLQ           | No          | N/A                   | No  |
+------------------------+-------------+-----------------------+-----+

CB? = Counts as failure for Circuit Breaker
After exhaustion = retried in-process, dead-lettered when retries run out on
  the final delivery.
Immediately = dead-lettered on the first failure, no further attempts.
Acked = the message is acknowledged and not retried; a dead-lettered failure
  (ErrDeadLettered) is reported once through Observer.Failure with outcome
  dead_lettered.
No (delay) = not retried in-process; the broker redelivers the message after a
  delay and it is dead-lettered only on the final delivery.
Never = a cancelled subscription context is never dead-lettered (the handler
  was interrupted, not the message found bad).
LocalBus and NATS Core deliveries are always final, so a "final delivery
only" failure is dead-lettered on its first and only delivery.
```

A dead-lettered failure (`ErrDeadLettered`) is acknowledged and reported once through `Observer.Failure`; there is no bus-level error callback. For the broker action and reported outcome of every error on each transport, see the [Failure Model]({{ site.baseurl }}/architecture/failure-model/).

## Custom DLQ Publisher

The DLQ invoker sits outside Retry so it dead-letters the final outcome once. A custom publisher receives an `event.DLQRecord` that carries the cause, the reason and the attempt count.

```go
type EnhancedDLQPublisher struct {
	// Your DLQ backend (NATS, Kafka, database, etc.)
}

func (p *EnhancedDLQPublisher) Publish(ctx context.Context, rec event.DLQRecord) error {
	cause := rec.Cause

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
		"event", rec.EventName,
		"reason", rec.Reason,
		"attempts", rec.Attempts,
		"error_type", errorType,
		"retryable", isRetryable,
		"cause", cause.Error(),
	)

	// ... publish to DLQ with metadata
	return nil
}
```

## Wiring

```go
dlq := &EnhancedDLQPublisher{}

chain := invoker.NewChain(
	// DLQ sits outside Retry: it dead-letters the final outcome once.
	invoker.NewDLQ(dlq),

	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}),

	invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
	}),
)

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	MaxConcurrent: 16,
})
```

See also [DLQ & Replay]({{ site.baseurl }}/examples/dlq-replay/) and [Failure Hooks]({{ site.baseurl }}/examples/failure-hooks/).
