---
layout: default
title: JSON Registry
parent: Serialization
nav_order: 1
---

# JSON Registry

The `json.Registry` implements `event.Registry` using JSON serialization with envelope wrapping and upcaster-based schema evolution.

## Setup

```go
import eventjson "github.com/isaquesb/go-event-bus/json"

registry := eventjson.NewRegistry()

// Register event factories with their version
registry.Register("user.created", func() event.Event { return &UserCreated{} }, 1)
registry.Register("user.created", func() event.Event { return &UserCreatedV2{} }, 2)

// Register upcasters for schema migration
registry.RegisterUpcaster(&UserCreatedV1ToV2{})
```

## Register

```go
registry.Register(name string, factory event.Factory, version int)
```

Registers an event factory for a given name and version. The registry tracks the latest version for each event name.

## Encode

```go
data, err := registry.Encode(ctx, event)
```

1. Marshals the event to JSON
2. Detects version (via `WithVersion` or defaults to 1)
3. Extracts trace context from the current span
4. Wraps everything in an `Envelope`
5. Marshals the envelope to JSON

## Decode

```go
ctx, event, err := registry.Decode(ctx, data)
```

1. Unmarshals the envelope
2. Restores trace context if `trace_id` and `span_id` are present
3. Runs upcaster chain if the version is not the latest
4. Creates the event via the factory for the latest version
5. Unmarshals the payload into the event

## Trace Propagation

The registry automatically:
- **On Encode**: Extracts `TraceID` and `SpanID` from the current OpenTelemetry span context
- **On Decode**: Restores the remote span context, enabling trace correlation across transport boundaries
