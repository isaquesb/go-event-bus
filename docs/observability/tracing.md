---
layout: default
title: Tracing
parent: Observability
nav_order: 5
---

# Tracing

Two pieces of the `telemetry` module produce traces:

- `telemetry.NewTracerInvoker` creates **one consumer span per handler invocation**.
- `telemetry.NewObserver` adds the in-chain signals (retry attempts, breaker transitions, dead letters, store errors) as **span events** on that span.

The transports add a producer span on every `Emit`, and the JSON registry carries the trace context across the wire.

## Setup

```go
import (
	"go.opentelemetry.io/otel"

	event "github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
	"github.com/isaquesb/go-event-bus/telemetry"
)

tracer := otel.Tracer("my-service")

chain := invoker.NewChain(
	telemetry.NewTracerInvoker(tracer),
	// RateLimit, Idempotency, DLQ, Retry, CircuitBreaker ...
)

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:  chain,
	Observer: telemetry.NewObserver(),
})
```

Place the tracer invoker **first**: the span must already be active when the other invokers emit their signals, otherwise the span events have no span to land on. The full chain order is on the [composition page]({{ site.baseurl }}/invoker-chain/composition/).

A `nil` tracer is accepted. The invoker then builds one from `WithTracerProvider(tp)`, or from the global provider, with the instrumentation scope `telemetry.ScopeName`.

### Options

| Option | Effect |
|---|---|
| `telemetry.WithTracerProvider(tp)` | The provider used when `NewTracerInvoker` receives a `nil` tracer. Without it the global provider is used |
| `telemetry.WithSpanDestination(fn)` | Overrides the destination that names the consumer span and fills `messaging.destination.name`. The default is the delivery subject, then the event name. Use it when subjects carry entity ids, so span names stay low cardinality |
| `telemetry.WithMeterProvider(mp)` | On `telemetry.NewObserver`. Turns on the OpenTelemetry metrics of the Observer, see the [metric reference]({{ site.baseurl }}/observability/metrics/). Without it the Observer records no metric, and there is no global fallback |

## The consumer span

Each handler invocation runs inside a span named `process <subject>`, with kind `Consumer`. `<subject>` is the destination described above.

| Attribute | Value |
|---|---|
| `messaging.system` | `nats` for NATS Core and JetStream, `eventbus` for LocalBus |
| `messaging.operation.name` | `process` |
| `messaging.operation.type` | `process` |
| `messaging.destination.name` | The destination: subject, or the result of `WithSpanDestination` |
| `messaging.message.id` | The event id, when there is one |
| `eventbus.handler` | The handler name |
| `eventbus.delivery_count` | The 1-based delivery number |
| `eventbus.correlation_id` | The correlation id, when there is one |
| `eventbus.replayed` | `true` on a message replayed from the DLQ |
| `eventbus.dlq.sequence` | The DLQ stream sequence of a replayed record |

The `messaging.*` attributes follow the OpenTelemetry messaging semantic conventions, pinned to semconv v1.43.0. The specification still marks them as Development, so they can change with a future semconv release.

On an error the span records it, sets `error.type` and sets the status to `Error`. **A duplicate is not an error**: `ErrDuplicate` is a handled outcome, so a span for a message the idempotency store skipped finishes with a normal status.

### Replayed messages

A message that was replayed from the DLQ gets span links:

| `eventbus.link.type` | Points at |
|---|---|
| `dead_letter` | The span where the message was dead-lettered, from `Eventbus-Replay-Failed-Traceparent` |
| `replay` | The replay span, from `Eventbus-Replay-Traceparent` |

The parent of the consumer span stays the original producer trace. A header that is not a valid W3C `traceparent` is ignored, so a forged header cannot break the handler.

## Producer spans

`Emit` on NATS Core and JetStream, and `EmitSync` on NATS Core, start a span named `publish <subject>` with kind `Producer`. It carries `messaging.system` (`nats`), `messaging.operation.name` (`publish`), `messaging.operation.type` (`send`), `messaging.destination.name` and, when the event has one, `messaging.message.id`. Only ids and the subject are attributes, never the payload.

Set `BusOptions.TracerProvider` on the bus to choose the provider. Without it the global provider is resolved at call time.

`jetstream.Replayer` wraps each replayed record in a `replay <subject>` producer span, linked to the dead-letter span with `eventbus.link.type` `dead_letter`. Choose its provider with `jetstream.WithReplayTracerProvider`. The replay is covered on the [DLQ page]({{ site.baseurl }}/invoker-chain/dlq-invoker/).

### Eventbus-Dlq-Traceparent

A broker DLQ record carries the header `Eventbus-Dlq-Traceparent`: the W3C `traceparent` of the span where the message was dead-lettered. The replayer parses it with the W3C propagator and uses it only as a **link**, never as the parent, because any publisher of the DLQ stream could have written the header. It copies it to `Eventbus-Replay-Failed-Traceparent` on the replayed message, since the replay strips the `Eventbus-Dlq-*` headers. The consumer span then links to it.

## Span events

`telemetry.NewObserver` adds these events to the active span, which is the consumer span when the tracer invoker is first in the chain. Without a recording span they do nothing.

| Event | Signal | Attributes |
|---|---|---|
| `eventbus.retry.attempt` | `RetryAttempt` | `eventbus.handler`, `eventbus.retry.attempt`, `eventbus.retry.class`, `eventbus.retry.delay_ms`, `error.type` |
| `eventbus.breaker.transition` | `BreakerTransition` | `eventbus.handler`, `eventbus.breaker.from`, `eventbus.breaker.to`, `eventbus.breaker.reason`, `eventbus.breaker.failures`, `eventbus.breaker.seq` |
| `eventbus.dead_lettered` | `DeadLettered` | `eventbus.handler`, `eventbus.reason`, `eventbus.delivery_count` |
| `eventbus.dlq.publish_failed` | `DLQPublishFailed` | `eventbus.handler`, `eventbus.reason`, `error.type` |
| `eventbus.store.error` | `StoreError` | `eventbus.handler`, `eventbus.store`, `eventbus.op`, `error.type` |
| `eventbus.idempotency.stale` | `IdempotencyStale` | `eventbus.handler` |
| `eventbus.internal_error` | `InternalError` | `eventbus.component`, `eventbus.op` |

One retry attempt is one span event, never a child span.

`Failure`, `HandlerDone` and `DecodeFailed` add no span event: by the time the transport reports them the consumer span has already ended, or never existed (a message that could not be decoded has no handler). Terminal outcomes show up in the metrics instead.

The span event `eventbus.retry.attempt` and the attribute key `eventbus.retry.attempt` (the attempt number) are the same string. They never collide, because one is an event name and the other is an attribute key. Both are kept as they are.

## Trace propagation

The trace context crosses transports in the envelope:

1. **Producer side**: `json.Registry.Encode()` injects `traceparent`, `tracestate` and `baggage` into the envelope `headers` from the current span context.
2. **Consumer side**: `json.Registry.Decode()` extracts them and restores the remote span context.

```
Producer Service                  Consumer Service
    │                                 │
    ├─ publish <subject> span         │
    │  └─ encode (embed trace)        │
    │     └─ transport ──────────────→│
    │                                 ├─ decode (restore trace)
    │                                 │  └─ process <subject> span
    │                                 │     └─ handler execution
```

### Sampling

The consumer honors the producer's sampling decision. The sampled flag in `traceparent` is kept, and nothing forces a consumer span to be sampled: a producer that is not sampled stays unsampled downstream, and with a parent-based sampler (the OpenTelemetry default) consumer traces follow the producer. There is no option to turn this off. The change from earlier releases is described in the [CHANGELOG](https://github.com/isaquesb/go-event-bus/blob/main/CHANGELOG.md).

### Older producers

An envelope from an older producer carries only `trace_id` and `span_id`, with no `traceparent`. It is read as a sampled remote parent, which also covers a malformed `traceparent`, so traces do not break while producers and consumers are upgraded at different times. Producers keep writing `trace_id` and `span_id` next to the W3C headers.

### Propagator

The registry uses a fixed W3C propagator (Trace Context plus Baggage). It does not use the process-global OpenTelemetry propagator. Pass `eventjson.WithPropagator(p)` to `eventjson.NewRegistry` to swap it. See [JSON Registry]({{ site.baseurl }}/serialization/json-registry/#options).

## Metrics

With `telemetry.WithMeterProvider(mp)` the same Observer also records the OpenTelemetry metrics of the bus:

```go
obs := telemetry.NewObserver(telemetry.WithMeterProvider(mp))
```

The instruments, units and labels are on the [metric reference]({{ site.baseurl }}/observability/metrics/). Use either this or the Prometheus Observer for metrics, not both.
