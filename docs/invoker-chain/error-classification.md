---
layout: default
title: Error Classification
parent: Invoker Chain
nav_order: 7
---

# Error Classification

Two components classify the error a handler returns. `Retry` decides whether another in-process attempt is worth making. `event.Dispose` decides what the transport does with the message: acknowledge it, retry it later, or end it (dead-letter, or drop when there is no publisher). The circuit breaker reuses `Dispose` to decide whether a failure counts. This page lists the errors and what each component does with them. What each transport does is in the [failure model]({{ site.baseurl }}/architecture/failure-model/).

## Marking an error

### Terminal and PermanentError

An error is **terminal** when retrying cannot help. Return `invoker.PermanentError`, or any error with a `Terminal() bool` method that returns true:

```go
return invoker.PermanentError{Err: errors.New("invalid data")}
```

```go
type Terminal interface {
    Terminal() bool
}
```

A terminal error is never retried by `Retry`, is dead-lettered on the first delivery, and is neutral for the circuit breaker. `Retry` wraps it in a `*event.FailureError` with reason `terminal`, which matches `ErrSendToDLQ`.

### Retryable and RetryableError

```go
return invoker.RetryableError{Err: errors.New("connection timeout")}
```

```go
type Retryable interface {
    Retryable() bool
}
```

`Retryable() == true` is retried. `Retryable() == false` on a non-terminal error is not retried in-process, and is returned as it is, for the transport to retry later. Both count for the circuit breaker. An error with neither method is retried.

## Sentinels and types

| Error | Retried by Retry? | Counts for the breaker? | Dead-lettered when? |
|---|---|---|---|
| Unclassified error, `RetryableError`, `Retryable() == true` | Yes | Yes | Final delivery, after the retries ran (`retries_exhausted`) |
| `Retryable() == false`, not terminal | No, returned unchanged | Yes | Final delivery |
| `PermanentError`, `Terminal() == true` | No | No | First delivery (`terminal`) |
| `ErrSendToDLQ` | Matched by a terminal `FailureError`. A bare `ErrSendToDLQ` from a handler is retried by `Retry`, because `Retry` classifies by the `Terminal` interface | No | First delivery (`Dispose` treats a match as terminal) |
| `ErrRetriesExhausted` | No, it is `Retry`'s own result | Not applicable: the breaker sits inside `Retry` and sees the raw handler errors | Final delivery |
| `ErrHandlerPanic`, `*event.PanicError` | No | No | First delivery (`panic`) |
| `ErrHandlerTimeout` | No: a handler that honors its context returns a context error, and `Retry` returns context errors unchanged | Yes, through the context cause | Final delivery. Transient, never an interruption |
| `ErrDuplicate` | No | No | Never: acknowledged |
| `ErrDeadLettered` | No | No | Already dead-lettered: acknowledged |
| `ErrDeadLetterFailed` | No | No | The publish failed: the transport retries with a delay and never acknowledges |
| `ErrInProgress` | No | No | Final delivery, by the transport backstop |
| `ErrRateLimited` | No | No | Final delivery, by the transport backstop |
| `ErrCircuitOpen` | No. After real attempts it becomes `FailureError` with reason `circuit_open` | Not applicable | Final delivery, by the chain DLQ |
| `ErrUnknownVersion` | The chain never sees it: it is raised by the decoder | No | Retried until the final delivery, then by the transport backstop |
| Decode errors (`ErrMalformedEnvelope`, `ErrUnknownEvent`, `ErrMissingUpcaster`, any other `*DecodeError`) | The chain never sees them | No | First delivery, by the transport backstop |
| `context.Canceled`, `context.DeadlineExceeded` | No | No (a bare deadline on a live context is neutral) | If the subscription context ended: never, the message is handed back. Otherwise treated as a transient failure |

"First delivery" means the transport ends the message at once. "Final delivery" means the last delivery the broker will make, as reported by `Delivery.Final`; before that the transport retries with a delay. "By the chain DLQ" and "by the transport backstop" are explained in the [DLQ invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/) page: the backstop needs the same publisher set on the bus.

## Panics

A panic in a handler is recovered by `event.InvokeHandler` and reaches every invoker as an ordinary returned `*event.PanicError`. It matches `event.ErrHandlerPanic` with `errors.Is`, and `Terminal()` reports true. `PanicError.Value` holds the panic value and `PanicError.Stack` the goroutine stack at the recovery.

- It is never retried and is neutral for the circuit breaker, even when the panicked value was a context error or a permanent error: `PanicError` has no `Unwrap`, so the value never changes the classification.
- It is dead-lettered on the first delivery with reason `panic`. A JetStream DLQ record carries the stack in `Eventbus-Dlq-Panic-Stack`, bounded to 1024 bytes.
- `event.ReasonOf` returns `panic` before it looks at anything else.

## Decode errors

A message that cannot be decoded fails before any handler runs, so the invoker chain never sees it. The error is a `*event.DecodeError` (read the event name and version with `errors.As`) wrapping one of four sentinels, matched with `errors.Is`:

| Sentinel | Meaning | Reason | Disposition |
|---|---|---|---|
| `ErrMalformedEnvelope` | Not a decodable envelope, the name is missing, or the version is negative | `decode_malformed` | Terminal |
| `ErrUnknownEvent` | The envelope names an unregistered event | `decode_unknown_event` | Terminal |
| `ErrMissingUpcaster` | No upcaster for an intermediate version | `decode_missing_upcaster` | Terminal |
| `ErrUnknownVersion` | The envelope version is newer than the latest registered one: a newer producer is ahead of this consumer | `unknown_version` | Retried with a delay, terminal on the final delivery |

Any other `*DecodeError` has reason `decode_failed` and is terminal. See [JSON Registry]({{ site.baseurl }}/serialization/json-registry/#decode-errors) for where each one comes from.

## Reasons

`event.ReasonOf(err)` returns the stable reason string used in DLQ headers, logs and signals. The strings never change once released:

| Reason | Meaning |
|---|---|
| `terminal` | A terminal error |
| `retries_exhausted` | `Retry` used all attempts on a retryable failure |
| `delivery_exhausted` | Anything else that reached the final delivery |
| `circuit_open` | The breaker rejected the call |
| `rate_limited` | The rate limiter rejected the call |
| `in_progress` | Another execution of the same event and handler is running |
| `unknown_version` | The envelope is newer than this consumer |
| `decode_malformed`, `decode_unknown_event`, `decode_missing_upcaster`, `decode_failed` | Decode failures |
| `duplicate` | The idempotency store already completed this execution |
| `interrupted` | The subscription context ended |
| `panic` | A recovered panic |
| `bus_closed`, `queue_full` | LocalBus rejected the emit |

A `*event.FailureError`, `*event.DeadLetteredError` or `*event.DeadLetterFailedError` carries its own `Reason`, and `ReasonOf` returns it unless the cause is a panic.

## Custom error types

Domain errors can mark themselves:

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

Both interfaces are found with `errors.As`, so a wrapped error keeps its classification.
