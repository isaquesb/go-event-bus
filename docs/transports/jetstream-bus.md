---
layout: default
title: JetStream Bus
parent: Transports
nav_order: 3
---

# JetStream Bus

The JetStream Bus provides durable, distributed event processing with at-least-once delivery.

## Setup

```go
import (
    "github.com/nats-io/nats.go"
    natsjs "github.com/nats-io/nats.go/jetstream"
    "github.com/isaquesb/go-event-bus/jetstream"
    eventjson "github.com/isaquesb/go-event-bus/json"
)

nc, _ := nats.Connect(nats.DefaultURL)
js, _ := natsjs.New(nc)

registry := eventjson.NewRegistry()
// ... register events

bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
    Invoker:          chain,
    OnErr:            errorHandler,
    CircuitOpenDelay: 30 * time.Second,
    RateLimitDelay:   5 * time.Second,
})
```

### BusOptions

| Field | Type | Default | Description |
|---|---|---|---|
| `Invoker` | `event.Invoker` | required | Invoker chain |
| `OnErr` | `func(...)` | nil | Error callback |
| `CircuitOpenDelay` | `time.Duration` | 30s (warning) | NAK delay when circuit is open |
| `RateLimitDelay` | `time.Duration` | 5s (warning) | NAK delay when rate limited |

## Publishing

```go
err := bus.Emit(ctx, "chat.message.room-general", chatMessage)
```

The subject is explicit (not derived from `Name()`) to support hierarchical NATS subjects.

## Subscribing

```go
bus.Subscribe(ctx, "chat.message.*", handler,
    event.WithStream("CHAT_EVENTS"),
    event.WithHandlerName("persist-message"),
    event.WithConsumer("chat-processor"),
    event.WithMaxDeliver(5),
    event.WithBackOff([]time.Duration{1*time.Second, 5*time.Second, 30*time.Second}),
)
```

`WithStream` is **required**. If `MaxDeliver` or `BackOff` are not set, defaults are used with a logged warning.

## ACK/NAK Strategy

The bus classifies invoker errors to decide message acknowledgment:

| Error | Action |
|---|---|
| `nil` | `msg.Ack()` |
| `ErrDuplicate` | `msg.Ack()` (already processed) |
| `ErrCircuitOpen` | `msg.NakWithDelay(CircuitOpenDelay)` |
| `ErrRateLimited` | `msg.NakWithDelay(RateLimitDelay)` |
| Other errors | `msg.Nak()` (immediate retry, bounded by MaxDeliver/BackOff) |
| Decode failure | `msg.Term()` (malformed, never retried) |

## Stream Configuration

Streams must be created before subscribing. Example:

```go
js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
    Name:        "CHAT_EVENTS",
    Subjects:    []string{"chat.>"},
    Retention:   natsjs.LimitsPolicy,
    MaxAge:      7 * 24 * time.Hour,
    MaxBytes:    1 << 30,
    Replicas:    3,
    Storage:     natsjs.FileStorage,
})
```

## Replay

The `Replayer` replays events from one stream to another subject:

```go
replayer := jetstream.NewReplayer(js)
err := replayer.Replay(ctx, jetstream.ReplayOptions{
    FromStream: "CHAT_EVENTS_DLQ",
    ToSubject:  "chat.message",
    Limit:      100,
})
```

## DLQ Publisher

```go
dlq := jetstream.NewDQL(js, registry, "dlq")
// Use with: invoker.NewDLQ(dlq, metrics)
```

## Close

`Close()` drains all consumers and waits for in-flight messages to complete.
