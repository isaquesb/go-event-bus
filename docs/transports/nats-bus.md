---
layout: default
title: NATS Bus
parent: Transports
nav_order: 2
---

# NATS Bus

The NATS Bus provides fire-and-forget distributed messaging via NATS Core.

## Setup

```go
import (
    "github.com/nats-io/nats.go"
    eventjson "github.com/isaquesb/go-event-bus/json"
    eventnats "github.com/isaquesb/go-event-bus/nats"
)

nc, _ := nats.Connect(nats.DefaultURL)
registry := eventjson.NewRegistry()
// ... register events

bus := eventnats.NewNatsBus(nc, registry, eventnats.BusOptions{
    RequestTimeout: 5 * time.Second,
    Invoker:        chain,
    OnErr:          errorHandler,
})
```

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `RequestTimeout` | `time.Duration` | 5s | Default timeout for `EmitSync` |
| `Invoker` | `event.Invoker` | nil | Optional invoker chain |
| `OnErr` | `func(...)` | nil | Error callback |
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

## Publishing

### Fire-and-Forget (Emit)

```go
// Subject defaults to evt.Name()
err := bus.Emit(ctx, myEvent)

// Override subject
err = bus.Emit(ctx, myEvent, event.WithSubject("custom.subject"))
```

Encodes the event via the registry and publishes to `evt.Name()` by default. Use `WithSubject` to override.

`Emit` also accepts the [metadata options]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) `event.WithID`, `event.WithCorrelationID` and repeatable `event.WithHeader`:

```go
err := bus.Emit(ctx, myEvent,
    event.WithCorrelationID(requestID),
    event.WithHeader("tenant", "acme"),
)
```

Every envelope carries a message id, the W3C `traceparent` header and, when `ctx` is a handler context, the causal lineage of the handled event.

### Request/Reply (EmitSync)

```go
response, err := bus.EmitSync(ctx, myEvent)
```

Sends a NATS request and waits for a response. The response is decoded back into an event. Timeout is either from the context deadline or `RequestTimeout`.

`EmitSync` takes no emit options: its signature, `EmitSync(ctx, evt)`, matches `event.SyncBus`. Its Metadata comes from `ctx` only, so emitting from a handler with the handler context makes the request a child of the handled event.

## Subscribing

```go
bus.Subscribe(ctx, "user.created", handler, event.WithHandlerName("email-sender"))
```

For request/reply handlers:

```go
bus.SubscribeSync(ctx, "user.created", handler, event.WithHandlerName("responder"))
```

`SubscribeSync` decodes the incoming message, runs the handler through the invoker chain, then encodes and responds with the event.

The reply is a child of the request: it gets a **fresh message id** (never the request id, even if the event implements `WithId`), its `causation_id` is the request id and it inherits the request's correlation id.

## Metadata

`Subscribe` and `SubscribeSync` decode the envelope and put the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) in the handler context, together with the producer's trace context. Handlers read it with `event.MetadataFromContext(ctx)`.

## RegisterSubscribers

Bulk registration using the `Subscriber` interface — consistent with LocalBus and JetStreamBus:

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber(),
    NewAuditSubscriber(),
)
```

Subscribers implementing `SubscribeOptionsProvider` will have their options merged automatically.

## Tracing

The NATS Bus creates OpenTelemetry spans for both `Emit` and `EmitSync` operations with attributes:
- `messaging.system`: "nats"
- `messaging.destination`: subject name
- `event.name`: event name

## Close

`Close()` closes the underlying NATS connection.
