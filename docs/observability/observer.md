---
layout: default
title: Observer
parent: Observability
nav_order: 1
---

# Observer

`event.Observer` is the single contract through which the bus reports what happens to a message. Invokers and transports only call it; logs, metrics and traces are backends that implement it. There is one Observer per bus, set once:

```go
bus := event.NewLocalBus(event.LocalBusOptions{Observer: obs})
// nats.BusOptions.Observer and jetstream.BusOptions.Observer work the same way
```

The bus carries the Observer in the message context and calls it **synchronously, on the handler goroutine**, with the message context. Spans, deadlines and metadata are therefore available inside every method.

## The ten signals

| Method | Payload | Fires when |
|---|---|---|
| `RetryAttempt` | `RetryAttemptInfo` | An attempt failed and `Retry` will wait and try again. Not emitted for the last attempt |
| `BreakerTransition` | `BreakerTransition` | A circuit breaker changed state |
| `Failure` | `FailureContext` | A message ended and no redelivery follows. Exactly once per terminal outcome |
| `DeadLettered` | `DeadLetterInfo` | A dead-letter publish succeeded |
| `HandlerDone` | `HandlerResult` | One handler chain invocation ended. Every invocation, whatever the result |
| `DecodeFailed` | `DecodeFailure` | A message could not be decoded, so no handler ran |
| `DLQPublishFailed` | `DLQPublishFailure` | A dead-letter publish failed, after the bounded in-process retry |
| `StoreError` | `StoreErrorInfo` | An idempotency or rate-limit store call failed |
| `IdempotencyStale` | `IdempotencyStaleInfo` | A stale in-progress idempotency marker was taken over |
| `InternalError` | `InternalErrorInfo` | The library hit an error that no handler sees |

Raw payload bytes never appear in a signal. Every `Delivery` inside a payload is a copy with `Raw` set to `nil`, and `DeadLetterInfo` is a summary, never the `DLQRecord`.

## Failure

`Failure` is the signal for alerting. It fires **once per terminal outcome of a message, on every transport**, and never for an intermediate redelivery: a message that JetStream will deliver again produces `HandlerDone` with outcome `retry`, not `Failure`. A decode failure has no event and no handler, so `FailureContext.Event` is `nil` and `Handler` is empty there.

| Field | Meaning |
|---|---|
| `Handler` | The failing handler. Empty on a decode failure |
| `Event` | The decoded event. `nil` on a decode failure |
| `Metadata` | Identity of the event (`ID`, `CorrelationID`, `CausationID`, ...). Zero when unknown. On a decode failure it is still populated when the envelope could be read |
| `Outcome` | How the message ended, see the table below |
| `Reason` | The stable reason, for example `retries_exhausted`, `circuit_open`, `panic`. See [error classification]({{ site.baseurl }}/invoker-chain/error-classification/) |
| `Cause` | The reported error. `errors.Is` reaches the original cause, and `event.ErrDeadLettered` when the message was dead-lettered |
| `Invoker` | The part of the pipeline that ended the message: `retry`, `circuit_breaker`, `dlq` or `transport` |
| `Attempts` | In-process attempts made on this delivery |
| `Delivery` | Delivery count and maximum, subject, headers, stream and consumer. `Raw` is always `nil` |
| `Breaker` | Circuit breaker state when the message ended. Empty when no breaker ran |
| `History` | The last in-process attempts, oldest first (`Attempt`: number, truncated error text, class, start, duration, delay) |
| `At` | When the message ended |

`Outcome` is one of:

| Outcome | Meaning |
|---|---|
| `dead_lettered` | The message was published to the DLQ |
| `dropped` | The message ended with no DLQ publisher configured. This includes retries exhausted without a DLQ |
| `interrupted` | The subscription ended on the final delivery, so the broker will not redeliver |
| `decode_failed` | The message could not be decoded |
| `dlq_publish_failed` | The dead-letter publish failed on the final delivery, so the message is at risk |
| `rejected` | The event was refused before any handler ran, for example an emit after `Shutdown` |

The event's own `OnErr` method, when it has one, runs **after** `Observer.Failure`, once per terminal failure. It stays independent of the bus Observer. A panic in either one never reaches the transport.

## HandlerDone

`HandlerDone` fires for every invocation of a handler chain. It carries the handler, the event name, the `Outcome`, the `Duration`, the chain error and the end time. It is the source of handler latency and of the handler counters, because there is no metrics invoker.

| Outcome | Meaning |
|---|---|
| `ok` | The chain succeeded |
| `retry` | The delivery failed and the transport will redeliver it |
| `duplicate` | The idempotency store already completed this execution |
| `in_progress` | Another execution of the same event is running. The message will be retried |
| `rate_limited` | The rate limiter rejected the call. The message will be retried |
| `circuit_open` | The circuit breaker rejected the call. The message will be retried |
| `dead_lettered` | The message was dead-lettered |
| `dropped` | The message was lost with no DLQ configured |
| `interrupted` | The subscription or the bus ended while the handler ran |
| `failed` | The chain ended in a terminal failure that was not dead-lettered |

`in_progress`, `rate_limited` and `circuit_open` describe a message that will be tried again. On the **final delivery** nothing tries it again, so the signal is held back until the transport has settled the message and is then reported with what really happened: `dead_lettered`, `dropped`, or `failed` when the DLQ publish failed. `HandlerDone` therefore agrees with the single `Failure` of that delivery. The same holds for a terminal failure the chain did not dead-letter: it is reported as `dead_lettered`, `dropped`, `retry` or `failed` once the transport decided.

## InternalError

`InternalError` replaces the direct log calls that transports used to make per message. `Component` and `Op` are closed sets.

| `Component` | Where |
|---|---|
| `transport` | A transport adapter (LocalBus, NATS, JetStream) |
| `circuit_breaker` | The circuit breaker, for example a panic in `OnStateChange` |
| `event` | The root module, for example a panic in an event's `OnErr` |

| `Op` | Meaning |
|---|---|
| `consume` | Receiving or decoding a message |
| `ack` | Acknowledging a message |
| `nak` | Negatively acknowledging a message |
| `term` | Terminating a message |
| `reply` | Sending a reply (NATS request/reply) |
| `encode_reply` | Encoding a reply |
| `panic` | A recovered panic outside a handler |
| `unsubscribe` | Removing a subscription after the bus closed |
| `drain` | Draining a subscription during `Shutdown` |

## Writing an observer

Embed `event.NopObserver` and override only the methods you need. An embedding struct keeps compiling when the interface gains methods, the same way gRPC servers embed their `Unimplemented` type. Copied from [`examples/10_failure_hooks.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/10_failure_hooks.go):

```go
// billingAlerts embeds event.NopObserver, so it implements every Observer
// method and keeps compiling when the interface gains methods. It overrides
// only Failure.
type billingAlerts struct {
	event.NopObserver
}

// Failure is called once per terminal outcome, never on an intermediate retry.
func (a *billingAlerts) Failure(ctx context.Context, f event.FailureContext) {
	slog.Warn("message ended",
		"handler", f.Handler,
		"outcome", f.Outcome,
		"reason", f.Reason,
		"attempts", f.Attempts,
		"delivery", f.Delivery.Count,
		"max_delivery", f.Delivery.Max,
		"breaker", f.Breaker,
	)
}
```

The checked version, with its output, is `Example_failureObserver` in `examples/runnable_examples_test.go`.

## Combining observers

`event.MultiObserver(obs...)` fans every signal out to its children:

```go
bus := event.NewLocalBus(event.LocalBusOptions{
	// MultiObserver fans one signal out to several observers and isolates
	// the panic of any of them. The slog observer deduplicates repeated
	// records: within a 30s window the first is logged and the rest are
	// counted into one summary record.
	Observer: event.MultiObserver(
		event.NewSlogObserver(nil, event.WithLogEvery(30*time.Second)),
		&billingAlerts{},
	),
})
```

- Children are called **in the order given**. Nested multi observers are flattened and `nil` or `NopObserver` children are skipped. With no child left the result is `NopObserver{}`.
- A child that panics never stops the other children and the panic never reaches the handler goroutine. The first panic of each child and method is logged once through `slog.Default()`, the one direct log call in the observer path, because a failing observer cannot report through itself.
- A single child is still wrapped, so a user observer that panics is isolated too.

A bus always wraps the observer you give it with `MultiObserver`. Outside a bus, for an invoker used on its own or in a transport adapter, `event.WithObserver(ctx, o)` stores an observer in a context and isolates a raw observer the same way: a `NopObserver` or a `MultiObserver` result is stored unchanged and anything else is wrapped. `event.ObserverFromContext(ctx)` reads it back and returns `NopObserver` when the context carries none. There is deliberately no per-invoker observer option: the observer is configured in one place.

## Defaults

- A `nil` `Observer` in the bus options installs `event.NewSlogObserver(nil)`, so failures are never silent with zero configuration. See the logs page.
- `event.NopObserver{}` silences everything.
- The metric and trace backends are `prometheus.NewObserver` ([metrics]({{ site.baseurl }}/observability/metrics/), [Prometheus]({{ site.baseurl }}/observability/prometheus/)) and `telemetry.NewObserver` plus `telemetry.NewTracerInvoker` ([tracing]({{ site.baseurl }}/observability/tracing/)).

## Rules for implementations

- **Never block.** Every method runs on the handler goroutine, and the message waits for it. No network call, no unbuffered channel send, no lock held elsewhere. Hand slow work to a buffered channel with a non-blocking send, or to another goroutine, and drop the signal when the consumer is behind.
- **Do not assume an order across goroutines.** `BreakerTransition` can reach you from different goroutines. Sort by `Seq` within one `(Handler, BreakerID)`.
- **Never keep or read payload bytes.** They are not in the signals by design. Use the event name, the ids in `Metadata` and the stable `Reason` instead.
- **Keep metric labels bounded.** Use handler and event names and the closed outcome, reason and state vocabularies. Never an event id, a subject or error text.
