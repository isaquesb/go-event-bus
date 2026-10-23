---
layout: default
title: Failure Model
parent: Architecture
nav_order: 4
---

# Failure Model

Every transport settles a failed message with the same code. This page states the model and then gives, for each error and each transport, the broker action and the reported outcome.

## The model

1. `event.InvokeHandler` runs the invoker chain around the handler and emits `HandlerDone` to the bus Observer for every invocation.
2. `event.Settle` is called exactly once per handled message. It maps the result through `event.Dispose(err, Delivery)` to one of four dispositions:

| Disposition | Meaning |
|---|---|
| `Ack` | Success, a duplicate, or a message the chain already dead-lettered. |
| `Retry` | Redeliver at once. Only the interruption path returns it: the subscription context ended. `Dispose` itself never returns it. |
| `RetryDelayed` | Redeliver after a backoff. |
| `Terminal` | The message ends here. |

3. On `Terminal`, `Settle` publishes the record through the bus `DLQPublisher`, unless the chain already dead-lettered the message (the transport backstop, so a message is never published twice). Published: outcome `dead_lettered`. No publisher configured: outcome `dropped`. Publish failed: the disposition becomes `RetryDelayed`, and the outcome `dlq_publish_failed` is reported only on the final delivery. The publish is attempted once on a non-final delivery and up to three times, in process, on the final delivery.
4. Every terminal outcome reaches `Observer.Failure` exactly once, then the event's own `OnErr` method when the event implements it. An intermediate retry is never reported as a `Failure`.

### Deliveries and the final delivery

`Dispose` decides with the `Delivery` in the handler context. A delivery is final when `Max > 0` and `Count >= Max`:

- LocalBus and NATS Core deliver once (`Count 1, Max 1`), so every delivery is final and every failure that `Dispose` does not acknowledge is terminal.
- JetStream takes `Count` from `NumDelivered` and `Max` from the consumer `MaxDeliver`. `MaxDeliver <= 0` is unlimited and is never final.
- A context without a `Delivery` is treated as final.

### Errors are classified, never retried by default

- A **transient** error (any error the chain does not classify, `ErrRetriesExhausted`) is retried by the transport until the final delivery.
- A **terminal** error (`PermanentError`, `ErrSendToDLQ`, anything with `Terminal() == true`, a handler panic) is dead-lettered on any delivery. It is never retried.
- A **flow-control** error (`ErrCircuitOpen`, `ErrRateLimited`, `ErrInProgress`, `ErrUnknownVersion`) means the handler did not run. It is retried until the final delivery and then follows the terminal path.

```go
return invoker.RetryableError{Err: err}        // transient
return invoker.PermanentError{Err: err}        // terminal
```

## Terminal path

The cells below say "terminal path" when `Settle` ends the message:

- the record is published through the bus `DLQPublisher`: outcome `dead_lettered`, or no publisher: outcome `dropped` (the message is lost, only the `Failure` and the log remain);
- JetStream then calls `TermWithReason("<outcome>: <reason>")`. NATS Core and LocalBus take no broker action;
- a failing publish is not the terminal path: see the *DLQ publish failure* row.

## Disposition table

Reading the columns:

- **event.Disposition** is the result of `Dispose` on a non-final / final delivery.
- **LocalBus** and **NATS Core** have a single, final delivery. For NATS Core, `Subscribe` is fire-and-forget; `SubscribeSync` replies to the requester with the `Eventbus-Error` header carrying the reason code (and `: <message>` when `BusOptions.ReplyErrorMessage` is set).
- **JetStream non-final delivery** and **JetStream final delivery** show the broker action. `NakWithDelay` uses the `BackOff` schedule, `BackOff[min(Count-1, len-1)]`, unless the row names another delay.
- **Reported outcome / reason** shows `Observer.Failure` and `HandlerDone`.

| Error | event.Disposition (non-final / final) | LocalBus | NATS Core (Subscribe / SubscribeSync) | JetStream non-final delivery | JetStream final delivery | Reported outcome / reason |
|---|---|---|---|---|---|---|
| `nil` | Ack / Ack | `EmitSync` returns nil | nothing / reply with the encoded response | Ack | Ack | No `Failure`. `HandlerDone` `ok`. |
| `ErrDuplicate` | Ack / Ack | `EmitSync` returns nil | nothing / `Eventbus-Error: duplicate` | Ack | Ack | No `Failure`, no DLQ record. `HandlerDone` `duplicate`. |
| `ErrDeadLettered` (the chain DLQ invoker published) | Ack / Ack | `EmitSync` returns nil, no second record | nothing / `Eventbus-Error` with the dead-lettered reason | Ack | Ack | `Failure` `dead_lettered`, reason from the record, invoker `dlq`. `HandlerDone` `dead_lettered`. |
| `ErrCircuitOpen` | RetryDelayed / Terminal | terminal path, reason `circuit_open` | terminal path / `Eventbus-Error: circuit_open` | `NakWithDelay(CircuitOpenDelay)` | terminal path: `TermWithReason` `dead_lettered: circuit_open` or `dropped: circuit_open` | Non-final: no `Failure`, `HandlerDone` `circuit_open` (will be retried). Final: `Failure` `dead_lettered` or `dropped`, reason `circuit_open`, and `HandlerDone` with the same outcome. |
| `ErrRateLimited` | RetryDelayed / Terminal | terminal path, reason `rate_limited` | terminal path / `Eventbus-Error: rate_limited` | `NakWithDelay(RateLimitDelay)` | terminal path: `dead_lettered: rate_limited` or `dropped: rate_limited` | Non-final: no `Failure`, `HandlerDone` `rate_limited` (will be retried). Final: `Failure` `dead_lettered` or `dropped`, reason `rate_limited`, `HandlerDone` the same. |
| `ErrInProgress` | RetryDelayed / Terminal | terminal path, reason `in_progress` | terminal path / `Eventbus-Error: in_progress` | `NakWithDelay` on the `BackOff` schedule | terminal path: `dead_lettered: in_progress` or `dropped: in_progress` | Non-final: no `Failure`, `HandlerDone` `in_progress` (will be retried). Final: `Failure` `dead_lettered` or `dropped`, reason `in_progress`, `HandlerDone` the same. |
| `ErrUnknownVersion` (decode) | RetryDelayed / Terminal | not applicable: LocalBus does not decode | terminal path / `Eventbus-Error: unknown_version` | `NakWithDelay`; `DecodeFailed` with `Final` false | terminal path: `dead_lettered: unknown_version` or `dropped: unknown_version` | Non-final: `DecodeFailed` only. Final: `DecodeFailed` and `Failure` `decode_failed`, reason `unknown_version`. No `HandlerDone`: the handler never ran. |
| terminal: `PermanentError`, `ErrSendToDLQ`, `ErrHandlerPanic` | Terminal / Terminal | terminal path, reason `terminal` (`panic` for a panic) | terminal path / `Eventbus-Error: terminal` or `panic` | terminal path on any delivery: `dead_lettered: terminal` or `dropped: terminal` (`panic` for a panic) | same as non-final | `Failure` `dead_lettered` or `dropped`, reason `terminal` or `panic`. `HandlerDone` the same outcome. |
| transient / unclassified (any other error) | RetryDelayed / Terminal | terminal path, reason `delivery_exhausted` | terminal path / `Eventbus-Error: delivery_exhausted` | `NakWithDelay` | terminal path: `dead_lettered: delivery_exhausted` or `dropped: delivery_exhausted` | Non-final: no `Failure`, `HandlerDone` `retry`. Final: `Failure` and `HandlerDone` `dead_lettered` or `dropped`, reason `delivery_exhausted`. With `MaxDeliver <= 0` the delivery is never final. |
| `ErrRetriesExhausted` (the Retry invoker gave up, no DLQ invoker) | RetryDelayed / Terminal | terminal path, reason `retries_exhausted` | terminal path / `Eventbus-Error: retries_exhausted` | `NakWithDelay` | terminal path: `dead_lettered: retries_exhausted` or `dropped: retries_exhausted` | Non-final: no `Failure`, `HandlerDone` `retry`. Final: `Failure` and `HandlerDone` `dead_lettered` or `dropped`, reason `retries_exhausted`. |
| subscription context cancelled (interrupted) | not consulted: `Settle` returns Retry before `Dispose` | never dead-lettered; `EmitSync` returns a `*HandlerError` matching `context.Canceled` | nothing / `Eventbus-Error: interrupted` | plain `Nak()` | plain `Nak()` | Non-final: no `Failure`. Final: `Failure` `interrupted`, reason `interrupted`. `HandlerDone` `interrupted`. No DLQ record on any delivery. |
| handler timeout (the subscription context is alive) | RetryDelayed / Terminal | `LocalBusOptions.HandlerTimeout` (async handlers): the error matches `ErrHandlerTimeout`; terminal path, reason `delivery_exhausted` | terminal path / `Eventbus-Error: delivery_exhausted` | `NakWithDelay` | terminal path: `dead_lettered: delivery_exhausted` or `dropped: delivery_exhausted` | An ordinary transient error: never `interrupted`. Same reporting as the transient row. |
| `decode_malformed` | Terminal / Terminal | not applicable | terminal path on the raw bytes / `Eventbus-Error: decode_malformed` | terminal path on any delivery: `dead_lettered: decode_malformed` or `dropped: decode_malformed` | same as non-final | `DecodeFailed` (`Final` per delivery) and `Failure` `decode_failed`, reason `decode_malformed`. No `HandlerDone`. |
| `decode_unknown_event` | Terminal / Terminal | not applicable | terminal path on the raw bytes / `Eventbus-Error: decode_unknown_event` | terminal path on any delivery: `dead_lettered: decode_unknown_event` or `dropped: decode_unknown_event` | same as non-final | `DecodeFailed` and `Failure` `decode_failed`, reason `decode_unknown_event`. No `HandlerDone`. |
| `decode_missing_upcaster` | Terminal / Terminal | not applicable | terminal path on the raw bytes / `Eventbus-Error: decode_missing_upcaster` | terminal path on any delivery: `dead_lettered: decode_missing_upcaster` or `dropped: decode_missing_upcaster` | same as non-final | `DecodeFailed` and `Failure` `decode_failed`, reason `decode_missing_upcaster`. No `HandlerDone`. |
| `decode_failed` (any other decode error) | Terminal / Terminal | not applicable | terminal path on the raw bytes / `Eventbus-Error: decode_failed` | terminal path on any delivery: `dead_lettered: decode_failed` or `dropped: decode_failed` | same as non-final | `DecodeFailed` and `Failure` `decode_failed`, reason `decode_failed`. No `HandlerDone`. |
| DLQ publish failure (`ErrDeadLetterFailed`) | RetryDelayed / RetryDelayed | `EmitSync` returns a `*HandlerError` matching `ErrDeadLetterFailed` | no broker action / `Eventbus-Error` with the reason of the failed message (for example `terminal`) | `NakWithDelay` on the `BackOff` schedule, even when the cause is flow control; `DLQPublishFailed` with `Final` false | `NakWithDelay` on the `BackOff` schedule, never `TermWithReason`; `DLQPublishFailed` with `Final` true | Non-final: no `Failure`, `HandlerDone` `retry`. Final: `Failure` `dlq_publish_failed` once; `HandlerDone` `failed`. |
| no DLQ publisher configured | Terminal on a terminal error, or on the final delivery of any other error | outcome `dropped`; `EmitSync` returns a `*HandlerError` | outcome `dropped` / `Eventbus-Error` with the reason | terminal error: `TermWithReason` `dropped: <reason>`; other errors: `NakWithDelay` | `TermWithReason` `dropped: <reason>` | `Failure` `dropped` with the reason of the error. `HandlerDone` `dropped`. The message is lost: configure a publisher. |
| JetStream panic outside the handler (Decode, Settle, the DLQ publisher, an Observer) | not consulted: the callback recovers | not applicable | not applicable: NATS Core reports an `InternalError` (`panic`) and a `SubscribeSync` request gets `Eventbus-Error: panic` | `NakWithDelay` on the `BackOff` schedule, or `TermWithReason` `panic: recovered after settlement` when `Settle` already dead-lettered or reported the message | `Failure` `dropped`, reason `panic`, no event; then `TermWithReason` `panic: recovered on the final delivery before settlement`. If `Settle` had already dead-lettered or reported the message, the action is `NakWithDelay` and no second `Failure` fires | Non-final: an `InternalError` signal with `Op` panic and no `Failure`. Final: the `Failure` above. A handler panic is not this row: it is recovered inside the chain and is a terminal error. |

### HandlerDone outcome rule

A flow-control outcome (`circuit_open`, `rate_limited`, `in_progress`) in `HandlerDone` has two meanings, decided by the delivery:

- On a non-final delivery it is emitted at once and means "will be retried".
- On the final delivery the message is lost, so `HandlerDone` is deferred and completed by `Settle` with the settled outcome: `dead_lettered`, `dropped`, or `failed` when the DLQ publish failed. `HandlerDone` then agrees with the single `Observer.Failure`.

A terminal failure the chain did not dead-letter (outcome `failed`) is deferred the same way on every delivery. `Settle` completes the signal with what really happened to the message: `dead_lettered`, `dropped`, `retry` (the DLQ publish failed before the final delivery) or `failed` (the DLQ publish failed on the final delivery).

### A DLQ publish failure is never a Term

When the DLQ publish fails, `Settle` returns `RetryDelayed` and the transport calls `NakWithDelay`, on the final delivery too. JetStream never calls `TermWithReason` unless the record was published, or no publisher is configured and the message is therefore dropped. `Observer.Failure` still fires, once, on the final delivery (`dlq_publish_failed`), because the server will not redeliver and an operator has to act.

## Circuit breaker interaction

The circuit breaker counts only failures that point at an unhealthy downstream: unclassified and retryable errors, and the handler timeout a transport sets (`LocalBusOptions.HandlerTimeout`). It ignores terminal errors, panics, context cancellation, a bare `context.DeadlineExceeded` on a live context, and the flow-control errors. Business-level errors therefore never open the circuit.

## Design decisions

- **No hidden retries**: every in-process retry is an explicit `Retry` invoker in the chain. The transport redelivers, bounded by the final delivery.
- **No implicit DLQ in the chain**: the chain dead-letters only through the `DLQ` invoker. The bus `DLQPublisher` is the backstop for what the chain did not publish.
- **One mapping**: `Dispose` is the same function for every transport, so the same error settles the same way everywhere. The transports only differ in the broker action.
- **An interruption is not a failure of the message**: a subscription context that ended is never dead-lettered, and is reported only when no redelivery follows.

## Sources

Each cell was checked against `event.Dispose`, `event.Settle` and the broker actions of the transports (`apply` and `delayFor` in the JetStream bus, `settle` and the `Eventbus-Error` reply in the NATS bus), and against these tests:

- `TestDispose_TruthTable` in [`dispose_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/dispose_test.go): the disposition columns, for a non-final, a final and an unlimited delivery.
- `TestLocalBus_FailureMatrix` in [`local_bus_observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/local_bus_observer_test.go): the LocalBus column, in sync and async mode.
- `TestNatsObserver_FailureMatrix` in [`nats/observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/observer_test.go): the NATS Core columns for `Subscribe` and `SubscribeSync`, including the `Eventbus-Error` reply codes.
- `TestJetStreamObserver_FailureMatrix` in [`jetstream/observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/observer_test.go): the two JetStream columns, including the final-delivery DLQ publish failure that is reported and never termed.
- [`jetstream/disposition_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/disposition_test.go): the delays (`BackOff`, `CircuitOpenDelay`, `RateLimitDelay`) and the decode failures.
- `TestJetStreamBus_CallbackPanicRecoveredAndNaked` and `TestJetStreamBus_CallbackPanicOnFinalDeliveryReported` in [`jetstream/robust_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/robust_test.go): the panic outside the handler, on a non-final and on the final delivery.
- [`settle.go`](https://github.com/isaquesb/go-event-bus/blob/main/settle.go): `Dispose`, `Settle` and the outcome of `HandlerDone`.
