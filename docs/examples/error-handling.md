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
type ValidationError struct {
    Field   string
    Message string
}

func (e ValidationError) Error() string {
    return fmt.Sprintf("validation failed on %s: %s", e.Field, e.Message)
}

func (e ValidationError) Terminal() bool { return true }
```

### Network Error (Retryable)

```go
type NetworkError struct {
    Operation string
    Err       error
}

func (e NetworkError) Error() string   { return fmt.Sprintf("network error during %s: %v", e.Operation, e.Err) }
func (e NetworkError) Unwrap() error   { return e.Err }
func (e NetworkError) Retryable() bool { return true }
```

## Handler with Classification

```go
func ProcessPayment(ctx context.Context, evt event.Event) error {
    payment := evt.(PaymentRequest)

    // Validation → Terminal → DLQ
    if payment.Amount <= 0 {
        return ValidationError{Field: "amount", Message: "must be positive"}
    }

    // Business rule → Terminal → DLQ
    if payment.Amount > 10000 {
        return invoker.PermanentError{
            Err: errors.New("exceeds transaction limit"),
        }
    }

    // Network call
    result, err := callPaymentGateway(ctx, payment)
    if err != nil {
        // Network error → Retryable
        return invoker.RetryableError{Err: err}
    }

    switch result.Status {
    case "approved":
        return nil
    case "declined":
        return invoker.PermanentError{Err: fmt.Errorf("declined: %s", result.Reason)}
    case "pending":
        return invoker.RetryableError{Err: errors.New("pending, will retry")}
    default:
        return fmt.Errorf("unknown status: %s", result.Status) // default: retryable
    }
}
```

## Classification Summary

| Error Type | Retried? | DLQ? | Circuit Breaker? |
|---|---|---|---|
| Default (unknown) | Yes | After exhaustion | Yes |
| `RetryableError` | Yes | After exhaustion | Yes |
| `PermanentError` | No | Yes | No |
| `Terminal` interface | No | Yes | No |
| `ErrDuplicate` | No | No | No |
| `ErrRateLimited` | No | No | No |
| `ErrCircuitOpen` | No | No | N/A |
| `context.Canceled` | No | No | No |

## Custom DLQ Publisher

```go
type EnhancedDLQPublisher struct{}

func (p *EnhancedDLQPublisher) Publish(ctx context.Context, evt event.Event, cause error) error {
    var validationErr ValidationError
    var businessErr BusinessRuleViolation

    switch {
    case errors.As(cause, &validationErr):
        // Log validation details
    case errors.As(cause, &businessErr):
        // Log business rule details
    }

    // Publish to your DLQ backend
    return nil
}
```
