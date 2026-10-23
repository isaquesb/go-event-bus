---
layout: default
title: Tracing
parent: Observability
nav_order: 5
---

# Tracing

The `telemetry.TracerInvoker` creates OpenTelemetry spans for every handler invocation.

## Setup

```go
import (
    "go.opentelemetry.io/otel"
    "github.com/isaquesb/go-event-bus/telemetry"
)

tracer := otel.Tracer("my-service")
tracerInvoker := telemetry.NewTracerInvoker(tracer)
```

Place it as the **first invoker** in the chain for complete visibility:

```go
chain := invoker.NewChain(
    tracerInvoker,
    invoker.NewMetrics(metrics),
    // ... other invokers
)
```

## Span Attributes

Each span includes:

| Attribute | Value |
|---|---|
| `event.name` | Event's `Name()` return value |
| `handler.name` | Handler identifier |
| Span kind | `Consumer` |
| Operation name | `event.handle` |

On error, the span records the error and sets status to `Error`.

## Trace Propagation

Traces are propagated across transport boundaries via the W3C Trace Context headers in the envelope:

1. **Producer side**: `json.Registry.Encode()` injects `traceparent`, `tracestate` and `baggage` into the envelope `headers` from the current span context
2. **Consumer side**: `json.Registry.Decode()` extracts them and restores the remote span context

This enables end-to-end trace correlation:

```
Producer Service                  Consumer Service
    │                                 │
    ├─ emit span                      │
    │  └─ encode (embed trace)        │
    │     └─ transport ──────────────→│
    │                                 ├─ decode (restore trace)
    │                                 │  └─ event.handle span (linked)
    │                                 │     └─ handler execution
```

### Sampling

The consumer honors the producer's sampling decision: the sampled flag in `traceparent` is kept. A producer that is not sampled stays unsampled downstream. Previous versions forced every consumer span to be sampled; this is no longer the case, so with a parent-based sampler (the OpenTelemetry default) consumer traces now follow the producer's decision.

### Older producers

Envelopes from older producers carry only `trace_id` and `span_id`, with no `traceparent`. They are still read as sampled remote parents, so traces do not break while producers and consumers are upgraded at different times. New producers keep writing `trace_id` and `span_id` next to the W3C headers for one minor release.

### Propagator

The registry uses a fixed W3C propagator (Trace Context plus Baggage). It does not use the process-global OpenTelemetry propagator. Pass `eventjson.WithPropagator(p)` to `eventjson.NewRegistry` to swap it. See [JSON Registry]({{ site.baseurl }}/serialization/json-registry/#options).

## NATS/JetStream Transport Spans

The NATS and JetStream buses also create producer spans:

| Transport | Operation | Kind | Attributes |
|---|---|---|---|
| NatsBus | `eventbus.emit` | Producer | messaging.system, messaging.destination, event.name |
| NatsBus | `eventbus.emit_sync` | Producer | messaging.system, messaging.destination, event.name |
| JetStreamBus | `eventbus.emit` | Producer | messaging.system, messaging.destination, event.name |
