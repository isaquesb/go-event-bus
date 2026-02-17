---
layout: default
title: Retry
parent: Invoker Chain
nav_order: 2
---

# Retry Invoker

The retry invoker handles transient failures with exponential backoff.

## Configuration

```go
invoker.NewRetry(invoker.RetryPolicy{
    MaxAttempts: 3,
    BaseDelay:   100 * time.Millisecond,
    MaxDelay:    5 * time.Second,
}, metrics)
```

### RetryPolicy

| Field | Type | Description |
|---|---|---|
| `MaxAttempts` | `int` | Maximum number of attempts (including first) |
| `BaseDelay` | `time.Duration` | Initial delay before first retry |
| `MaxDelay` | `time.Duration` | Maximum delay cap |

## Backoff Strategy

Exponential backoff: `delay = 2^attempt * BaseDelay`, capped at `MaxDelay`.

```
Attempt 1: immediate
Attempt 2: 100ms
Attempt 3: 200ms
Attempt 4: 400ms (capped at MaxDelay)
```

## Error Classification

The retry invoker classifies errors to decide behavior:

```go
// Explicitly retryable
return invoker.RetryableError{Err: err}

// Explicitly permanent (never retried, sent to DLQ)
return invoker.PermanentError{Err: err}
```

### Classification Logic

1. `ErrDuplicate`, `ErrRateLimited`, `ErrCircuitOpen` → **not retried, not terminal**
2. `context.Canceled`, `context.DeadlineExceeded` → **not retried**
3. Implements `Terminal` interface with `Terminal() == true` → **terminal, sent to DLQ**
4. Implements `Retryable` interface → **retried if `Retryable() == true`**
5. Default (unknown errors) → **retried**

## Metrics

| Metric | Type | Description |
|---|---|---|
| `eventbus_retry_attempt_total` | Counter | Total retry attempts |
| `eventbus_retry_success_total` | Counter | Successes after retry |
| `eventbus_retry_terminal_total` | Counter | Terminal errors (to DLQ) |
| `eventbus_retry_exhausted_total` | Counter | All retries exhausted |
| `eventbus_retry_backoff_ms` | Histogram | Backoff delay duration |
