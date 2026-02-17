---
layout: default
title: Failure Model
parent: Architecture
nav_order: 4
---

# Failure Model

## Error Classification

Errors in Go Event Bus are classified into three categories that drive execution behavior:

### Transient Errors

- **Retried** up to `MaxAttempts`
- Examples: network timeout, database connection, upstream service unavailable
- Default behavior for unknown errors

```go
return invoker.RetryableError{Err: err}
// or simply return any error (default is retryable)
```

### Terminal Errors

- **Never retried**
- **Sent to DLQ** immediately
- Examples: validation error, business rule violation, malformed data

```go
return invoker.PermanentError{Err: errors.New("invalid data")}
// or implement Terminal() bool interface
```

### Policy Errors

- **Not retried, not sent to DLQ**
- Handled specially by the system
- Examples: `ErrDuplicate`, `ErrRateLimited`, `ErrCircuitOpen`

## Circuit Breaker Interaction

The circuit breaker reacts **only to transient failures**:
- Terminal errors (`ErrSendToDLQ`) do not count as failures
- Policy errors do not count as failures
- Only actual transient failures increment the failure counter

This prevents business-level errors (like validation failures) from tripping the circuit breaker, which should only open when downstream infrastructure is unhealthy.

## Error Flow Through the Chain

```
Handler returns error
    │
    ├─ classify(error)
    │   ├─ Terminal? → Retry returns ErrSendToDLQ → DLQ publishes → nil
    │   ├─ Retryable? → Retry re-executes (up to MaxAttempts)
    │   │   └─ Exhausted? → return last error → DLQ may capture
    │   └─ Policy? → return error (ErrDuplicate, ErrRateLimited, etc.)
    │
    └─ Circuit Breaker evaluates
        ├─ ErrSendToDLQ → does not count as failure
        └─ Other errors → increments failure counter
```

## Design Decisions

- **No hidden retries**: All retries are explicit and visible in the chain
- **No implicit DLQ**: Events only go to DLQ through the explicit DLQ invoker
- **Error classification is deterministic**: Same error always gets the same treatment
- **Context cancellation stops everything**: `context.Canceled` and `context.DeadlineExceeded` are never retried
