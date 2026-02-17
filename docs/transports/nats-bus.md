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

## Publishing

### Fire-and-Forget (Emit)

```go
err := bus.Emit(ctx, myEvent)
```

Encodes the event via the registry and publishes to the event's `Name()` subject.

### Request/Reply (EmitSync)

```go
response, err := bus.EmitSync(ctx, myEvent)
```

Sends a NATS request and waits for a response. The response is decoded back into an event. Timeout is either from the context deadline or `RequestTimeout`.

## Subscribing

```go
bus.Subscribe(ctx, "user.created", handler, event.WithHandlerName("email-sender"))
```

For request/reply handlers:

```go
bus.SubscribeSync(ctx, "user.created", handler, event.WithHandlerName("responder"))
```

`SubscribeSync` decodes the incoming message, runs the handler through the invoker chain, then encodes and responds with the event.

## Tracing

The NATS Bus creates OpenTelemetry spans for both `Emit` and `EmitSync` operations with attributes:
- `messaging.system`: "nats"
- `messaging.destination`: subject name
- `event.name`: event name

## Close

`Close()` closes the underlying NATS connection.
