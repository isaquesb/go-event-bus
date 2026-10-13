---
layout: default
title: Registry & Envelope
parent: Core Concepts
nav_order: 4
---

# Registry & Envelope

## Registry Interface

The `Registry` handles serialization and deserialization of events:

```go
type Registry interface {
    Register(name string, factory Factory, version int)
    Decode(ctx context.Context, data []byte) (context.Context, Event, error)
    Encode(ctx context.Context, evt Event) ([]byte, error)
}
```

Where `Factory` creates a new empty event instance:

```go
type Factory func() Event
```

### EnvelopeEncoder

A registry can also implement the optional `EnvelopeEncoder` interface:

```go
type EnvelopeEncoder interface {
    EncodeWith(ctx context.Context, evt Event, md Metadata) ([]byte, error)
}
```

Buses resolve the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) themselves at `Emit` and call `event.EncodeEvent(ctx, registry, evt, md)`, which uses `EncodeWith` when the registry implements it and falls back to `Registry.Encode` otherwise. Custom registries should implement `EnvelopeEncoder` so the id, correlation id, causation id and headers resolved by the bus reach the wire. A registry that only implements `Encode` still works, but it must resolve the Metadata itself.

## Envelope

Every serialized event is wrapped in an `Envelope`:

```go
type Envelope struct {
    Name          string            `json:"name"`
    Version       int               `json:"version"`
    Timestamp     time.Time         `json:"ts"`
    ID            string            `json:"id,omitempty"`
    CorrelationID string            `json:"correlation_id,omitempty"`
    CausationID   string            `json:"causation_id,omitempty"`
    Headers       map[string]string `json:"headers,omitempty"`
    TraceID       string            `json:"trace_id,omitempty"`
    SpanID        string            `json:"span_id,omitempty"`
    Payload       json.RawMessage   `json:"payload"`
}
```

Example JSON:

```json
{
  "name": "user.created",
  "version": 2,
  "ts": "2026-02-05T12:00:00Z",
  "id": "0194f3c2-8a41-7c3e-9d52-1b6f0e7a4c10",
  "correlation_id": "0194f3c2-7e10-7a2b-8c11-5d0e3f9b2a64",
  "causation_id": "0194f3c2-7e10-7a2b-8c11-5d0e3f9b2a64",
  "headers": {
    "tenant": "acme",
    "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
  },
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "span_id": "00f067aa0ba902b7",
  "payload": {"user_id": "123", "email": "john@example.com"}
}
```

The envelope enables:
- **Versioned schema evolution** via upcasters
- **Identity and causal lineage** (`id`, `correlation_id`, `causation_id`)
- **Trace correlation** across transport boundaries (W3C `traceparent`, `tracestate` and `baggage` headers)
- **Safe replay** and DLQ handling

`id`, `correlation_id`, `causation_id` and `headers` are optional on the wire (`omitempty`). Every envelope written by the JSON registry carries an `id`.

### Compatibility

The envelope is compatible in both directions:

- **Old to new:** envelopes written by v0.3.0, which have none of the new fields, decode unchanged on a new consumer. The handler receives Metadata with an empty `ID`, `CorrelationID` and `CausationID`.
- **New to old:** envelopes written by a new producer decode on a v0.3.0 consumer, which ignores the keys it does not know (`id`, `correlation_id`, `causation_id`, `headers`).
- **Trace fields:** `trace_id` and `span_id` are still written next to the W3C headers for one minor release, so a consumer that predates the headers keeps linking traces. New consumers read them only as a fallback when the envelope has no valid `traceparent`. See [Tracing]({{ site.baseurl }}/observability/tracing/).

### Headers

`headers` holds the user headers set with `event.WithHeader` plus the W3C trace headers:

- **User headers** are producer data that every consumer of the message can read. Do not put secrets in them.
- **System keys** `traceparent`, `tracestate` and `baggage` are written by the trace propagator. If a user header has one of these names, the system value replaces it without an error, and `Emit` returns no validation error for it.
- **Size:** headers count toward the broker `max_payload` (for example the NATS server limit). No header count or size limit is enforced by the library, so keep headers small.

## Upcaster Interface

Upcasters transform older event versions to newer ones:

```go
type Upcaster interface {
    EventName() string
    FromVersion() int
    ToVersion() int
    Upcast(ctx context.Context, raw json.RawMessage) (json.RawMessage, error)
}
```

Upcasters are pure functions and deterministic. They are chained automatically by the registry: V1 → V2 → V3.

See [Schema Versioning]({{ site.baseurl }}/serialization/schema-versioning/) for details.
