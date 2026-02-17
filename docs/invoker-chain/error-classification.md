---
layout: default
title: Error Classification
parent: Invoker Chain
nav_order: 8
---

# Error Classification

The invoker chain classifies errors to drive execution behavior. This classification determines whether an error is retried, sent to DLQ, or silently handled.

## Error Types

### RetryableError

Explicitly marks an error as retryable:

```go
return invoker.RetryableError{Err: errors.New("connection timeout")}
```

### PermanentError

Marks an error as terminal (never retried, sent to DLQ):

```go
return invoker.PermanentError{Err: errors.New("invalid data")}
```

### Retryable Interface

```go
type Retryable interface {
    Retryable() bool
}
```

### Terminal Interface

```go
type Terminal interface {
    Terminal() bool
}
```

## Sentinel Errors

| Error | Description |
|---|---|
| `ErrDuplicate` | Idempotency check failed (already processed) |
| `ErrRateLimited` | Rate limit exceeded |
| `ErrCircuitOpen` | Circuit breaker is open |
| `ErrSendToDLQ` | Internal: signals retry exhaustion for terminal errors |

## Classification Table

| Error Type | Retried? | DLQ? | Circuit Breaker? |
|---|---|---|---|
| Default (unknown) | Yes | After exhaustion | Yes |
| `RetryableError` | Yes | After exhaustion | Yes |
| `PermanentError` | No | Yes | No |
| `Terminal` interface | No | Yes | No |
| `Retryable` interface | Depends on return | After exhaustion | Yes |
| `ErrDuplicate` | No | No | No |
| `ErrRateLimited` | No | No | No |
| `ErrCircuitOpen` | No | No | N/A |
| `context.Canceled` | No | No | No |
| `context.DeadlineExceeded` | No | No | No |
| `ErrSendToDLQ` | No | N/A | No |

## Custom Error Types

You can create domain-specific error types:

```go
type ValidationError struct {
    Field   string
    Message string
}

func (e ValidationError) Error() string {
    return fmt.Sprintf("validation: %s - %s", e.Field, e.Message)
}

func (e ValidationError) Terminal() bool { return true }
```

```go
type NetworkError struct {
    Err error
}

func (e NetworkError) Error() string   { return e.Err.Error() }
func (e NetworkError) Unwrap() error   { return e.Err }
func (e NetworkError) Retryable() bool { return true }
```
