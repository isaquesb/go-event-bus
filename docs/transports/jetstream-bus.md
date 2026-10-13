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
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

## Publishing

```go
// Subject defaults to evt.Name()
err := bus.Emit(ctx, chatMessage)

// Override subject (e.g. for hierarchical NATS subjects)
err = bus.Emit(ctx, chatMessage, event.WithSubject("chat.message.room-general"))
```

The subject defaults to `evt.Name()`. Use `WithSubject` to override it for hierarchical routing.

`Emit` also accepts the [metadata options]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) `event.WithID`, `event.WithCorrelationID` and repeatable `event.WithHeader`. Every envelope carries a message id, the W3C `traceparent` header and, when `ctx` is a handler context, the causal lineage of the handled event.

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

The decode failure is a typed `*event.DecodeError` wrapping `event.ErrMalformedEnvelope`, `event.ErrUnknownEvent`, `event.ErrUnknownVersion` or `event.ErrMissingUpcaster`; see [JSON Registry]({{ site.baseurl }}/serialization/json-registry/#decode-errors).

## Metadata

The consumer decodes the envelope and puts the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) in the handler context, together with the producer's trace context. Handlers read it with `event.MetadataFromContext(ctx)`.

The `OnErr` callback receives the **message context**, which carries the event Metadata and trace, not the subscription context. Use it to log the message id and correlation id of a failed delivery:

```go
OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
    md, _ := event.MetadataFromContext(ctx)
    slog.Error("handler failed", "handler", handler, "message", md.ID, "correlation", md.CorrelationID, "error", err)
},
```

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

## RegisterSubscribers

Bulk registration using the `Subscriber` interface:

```go
bus.RegisterSubscribers(ctx,
    NewChatSubscriber(),
    NewAuditSubscriber(),
)
```

For JetStream, subscribers must provide stream options via the `SubscribeOptionsProvider` interface:

```go
type ChatSubscriber struct{}

func (s *ChatSubscriber) Name() string { return "chat-subscriber" }

func (s *ChatSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{
        "chat.message.*": s.handleMessage,
    }
}

// SubscribeOptions provides JetStream-specific options for all handlers in this subscriber.
func (s *ChatSubscriber) SubscribeOptions() []event.SubscribeOption {
    return []event.SubscribeOption{
        event.WithStream("CHAT_EVENTS"),
        event.WithConsumer("chat-processor"),
    }
}
```

## Close

`Close()` drains all consumers and waits for in-flight messages to complete.
