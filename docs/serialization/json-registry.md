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

// Optional: a custom propagator and id generator
// registry := eventjson.NewRegistry(
//     eventjson.WithPropagator(myPropagator),
//     eventjson.WithIDGenerator(myIDFunc),
// )

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

## Options

`NewRegistry(opts ...Option)` accepts optional options, so existing `NewRegistry()` calls compile unchanged.

| Option | Effect |
|---|---|
| `eventjson.WithPropagator(p)` | Replaces the propagator that writes and reads the trace headers. The default is `propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})` (W3C `traceparent`, `tracestate` and `baggage`). The process-global OpenTelemetry propagator is **not** used, because it is a no-op unless the application sets it. |
| `eventjson.WithIDGenerator(gen)` | Sets the function that mints the id of events that have neither an explicit id nor an `Id()` method. The default is `event.NewID` (UUIDv7). |

A `nil` argument keeps the default.

## Encode

```go
data, err := registry.Encode(ctx, evt)
```

1. Resolves the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) from `ctx`: if `ctx` carries the Metadata of a handled event, `evt` becomes its child (causation and correlation)
2. Marshals the event to JSON
3. Detects version (via `WithVersion` or defaults to 1)
4. Writes the user headers, then injects `traceparent`, `tracestate` and `baggage` from the current span context. System keys replace user headers of the same name
5. Writes `trace_id` and `span_id` for older consumers
6. Wraps everything in an `Envelope` and marshals it to JSON

Buses resolve the Metadata themselves at `Emit` and pass it explicitly with `EncodeWith`:

```go
md := event.ResolveOutbound(ctx, evt, event.EmitOptions{})
data, err := registry.EncodeWith(ctx, evt, md)
```

`EncodeWith` uses the id, correlation id, causation id, timestamp and headers of `md`. `Registry.Encode` is `EncodeWith` with Metadata resolved from `ctx`.

## Decode

```go
ctx, evt, err := registry.Decode(ctx, data)
```

1. Parses the envelope
2. Restores the trace context from the `traceparent`, `tracestate` and `baggage` headers (falling back to `trace_id` and `span_id` for envelopes from older producers)
3. Puts the envelope Metadata in the returned context, so the handler reads it with `event.MetadataFromContext`
4. Treats version `0`, or a missing version, as version 1
5. Runs the upcaster chain if the version is not the latest
6. Creates the event via the factory for the latest version
7. Unmarshals the payload into the event

`Decode` never panics on untrusted input. The registry lock is not held while upcasters run, so an upcaster may call `Register` or `Decode` on the same registry.

### Decode errors

Every failure is an `*event.DecodeError` that carries the envelope `Name` and the `Version` reached. It wraps one of four sentinels from the root module, so transports and your own code can classify failures without importing the `json` package:

| Sentinel | When |
|---|---|
| `event.ErrMalformedEnvelope` | The bytes are not a decodable envelope, the name is missing, the version is negative, or the payload does not fit the event |
| `event.ErrUnknownEvent` | The event name is not registered, or its factory is nil or returns nil |
| `event.ErrUnknownVersion` | The envelope version is newer than the latest registered version. Typical in a rolling deploy where a newer producer is ahead of this consumer |
| `event.ErrMissingUpcaster` | No upcaster is registered for an intermediate version on the path to the latest version |

An error returned by an upcaster is wrapped in `*event.DecodeError` without a sentinel, and so is an upcaster that does not advance the version.

```go
ctx, evt, err := registry.Decode(ctx, data)
switch {
case err == nil:
    // handle evt
case errors.Is(err, event.ErrUnknownVersion):
    // a newer producer: retrying after the deploy completes may succeed
case errors.Is(err, event.ErrUnknownEvent), errors.Is(err, event.ErrMalformedEnvelope):
    // will never decode: do not retry
}

var de *event.DecodeError
if errors.As(err, &de) {
    log.Printf("decode failed for %s v%d: %v", de.Name, de.Version, de.Err)
}
```

Use `errors.Is` for the four sentinels and `errors.As` for the catch-all `*event.DecodeError`. Once the envelope has parsed, the returned `ctx` carries its Metadata even when decoding fails.

## Trace Propagation

The registry writes and reads the W3C trace headers with its propagator:

- **On Encode**: injects `traceparent`, `tracestate` and `baggage` from the current OpenTelemetry context into the envelope `headers`
- **On Decode**: extracts them and restores the remote span context, enabling trace correlation across transport boundaries

See [Tracing]({{ site.baseurl }}/observability/tracing/) for the sampling semantics.
