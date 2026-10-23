---
layout: default
title: Retry
parent: Invoker Chain
nav_order: 2
---

# Retry Invoker

The retry invoker handles transient failures with exponential backoff, inside one delivery. When it gives up it returns an `*event.FailureError` that says why and how many attempts ran, and that carries a bounded history of the attempts.

## Configuration

```go
invoker.NewRetry(invoker.RetryPolicy{
    MaxAttempts: 3,
    BaseDelay:   100 * time.Millisecond,
    MaxDelay:    5 * time.Second,
})
```

### RetryPolicy

| Field | Type | Description |
|---|---|---|
| `MaxAttempts` | `int` | Maximum number of attempts, including the first. A value below 1 still runs the handler once |
| `BaseDelay` | `time.Duration` | Delay before the second attempt |
| `MaxDelay` | `time.Duration` | Maximum delay cap |
| `HistorySize` | `int` | How many failed attempts the returned `FailureError` keeps. 0 means `invoker.DefaultHistorySize` (5); a negative value turns the history off and allocates nothing |

## Backoff Strategy

Exponential backoff: the delay after attempt `n` is `2^(n-1) * BaseDelay`, capped at `MaxDelay`. The wait honors the context: when it ends during a wait, `Retry` returns the context error.

```
Attempt 1: immediate
Attempt 2: after 100ms
Attempt 3: after 200ms
Attempt 4: after 400ms (capped at MaxDelay)
```

## Error classification

After each failed attempt `Retry` classifies the error, in this order:

1. `ErrDuplicate`, `ErrRateLimited`, `ErrCircuitOpen`, `ErrInProgress`, `ErrDeadLettered` and `ErrDeadLetterFailed`: not retried and returned unchanged. They are not attempts of the retry policy.
2. `context.Canceled` and `context.DeadlineExceeded`: not retried, returned unchanged.
3. An error with `Terminal() == true` (`invoker.PermanentError`, a panic): terminal, never retried.
4. An error with a `Retryable()` method: retried only when it returns true.
5. Anything else: retried.

The full table of errors, including what the circuit breaker and the DLQ do with each, is in [Error Classification]({{ site.baseurl }}/invoker-chain/error-classification/).

```go
// Explicitly retryable
return invoker.RetryableError{Err: err}

// Explicitly permanent: never retried, dead-lettered at once
return invoker.PermanentError{Err: err}
```

## What Retry returns

| Situation | Returned | `FailureError.Reason` |
|---|---|---|
| Success on any attempt | `nil` | |
| Terminal error | `*event.FailureError` | `terminal` |
| All attempts used on a retryable error | `*event.FailureError` | `retries_exhausted` |
| The breaker opens after real attempts ran | `*event.FailureError` | `circuit_open` |
| A flow-control or context error | the error, unchanged | |

`FailureError` carries `Cause` (the last handler error), `Reason`, `Attempts` and `History`. `errors.Is` matches the cause and, by reason, `ErrSendToDLQ` (terminal) or `ErrRetriesExhausted` (retries exhausted). `errors.As` recovers the `Attempts` count.

A panic is terminal: it is never retried, and the failure is reported with reason `panic`.

### The breaker opens in the middle of a retry run

`Retry` sits outside the circuit breaker, so an open breaker stops the retries. When that happens after at least one real attempt, `Retry` does not return the bare `ErrCircuitOpen`. It returns a `*event.FailureError` with reason `circuit_open`, `Attempts` set to the handler runs that really happened (the rejected call is not an attempt) and the `History` of those runs. `errors.Is(err, invoker.ErrCircuitOpen)` still holds, so the failure is classified as a flow-control rejection, and the DLQ record tells how many times the handler ran before the circuit opened. When the breaker is already open on the first call, nothing ran and the bare `ErrCircuitOpen` is returned.

### History

`History` holds the last `HistorySize` failed attempts of **this delivery**, oldest first: number, error text (cut to 256 bytes), class (`transient`, `terminal`, `timeout` or `panic`), start time, duration and the backoff waited after it. `Attempts` keeps the true total. Earlier broker deliveries appear only as `Delivery.Count`: the in-flight envelope is never rewritten.

The history reaches the DLQ record and the `Eventbus-Dlq-History` header, see [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/).

## Signals

`Retry` emits `RetryAttempt` to the bus Observer before each wait, so one signal per retry that is about to happen and none for the last attempt:

| Field | Meaning |
|---|---|
| `Handler`, `Event` | Handler and event name |
| `Attempt` | Number of the attempt that failed |
| `Class` | `transient`, `terminal`, `timeout` or `panic` |
| `Err` | The attempt error |
| `Delay` | The wait before the next attempt |
| `At` | When the attempt **failed** (its start plus its duration), not when the signal was emitted |

The metric and log names are on the [Observability]({{ site.baseurl }}/observability/) pages. There is no retry metric in the invoker itself.

## Retry and the broker

`Retry` does not look at the delivery: every delivery runs the whole policy. On JetStream, keep `MaxAttempts` low, or leave the delay to the consumer `BackOff`, because the attempts multiply with the redeliveries. The numbers and the reasoning are in the [composition page]({{ site.baseurl }}/invoker-chain/composition/#retry-and-maxdeliver).
