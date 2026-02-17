---
layout: default
title: Tracing
parent: Observability
nav_order: 1
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

Traces are propagated across transport boundaries via the envelope:

1. **Producer side**: `json.Registry.Encode()` extracts `TraceID` and `SpanID` from the current span context and embeds them in the envelope
2. **Consumer side**: `json.Registry.Decode()` restores the remote span context from the envelope

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

## NATS/JetStream Transport Spans

The NATS and JetStream buses also create producer spans:

| Transport | Operation | Kind | Attributes |
|---|---|---|---|
| NatsBus | `eventbus.emit` | Producer | messaging.system, messaging.destination, event.name |
| NatsBus | `eventbus.emit_sync` | Producer | messaging.system, messaging.destination, event.name |
| JetStreamBus | `eventbus.emit` | Producer | messaging.system, messaging.destination, event.name |
