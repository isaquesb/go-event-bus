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

## Envelope

Every serialized event is wrapped in an `Envelope`:

```go
type Envelope struct {
    Name      string          `json:"name"`
    Version   int             `json:"version"`
    Timestamp time.Time       `json:"ts"`
    TraceID   string          `json:"trace_id,omitempty"`
    SpanID    string          `json:"span_id,omitempty"`
    Payload   json.RawMessage `json:"payload"`
}
```

Example JSON:

```json
{
  "name": "user.created",
  "version": 2,
  "ts": "2026-02-05T12:00:00Z",
  "payload": {"user_id": "123", "email": "john@example.com"},
  "trace_id": "abc123...",
  "span_id": "def456..."
}
```

The envelope enables:
- **Versioned schema evolution** via upcasters
- **Trace correlation** across transport boundaries
- **Safe replay** and DLQ handling

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
