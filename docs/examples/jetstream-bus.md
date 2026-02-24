---
layout: default
title: JetStream Bus
parent: Examples
nav_order: 4
---

# JetStream Bus

Durable, distributed event processing with NATS JetStream, including replay and DLQ.

Source: [`examples/04_jetstream_bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/04_jetstream_bus.go)

## Stream Setup

```go
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := natsjs.New(nc)

// Create stream
js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
    Name:     "CHAT_EVENTS",
    Subjects: []string{"chat.>"},
    MaxAge:   7 * 24 * time.Hour,
    MaxBytes: 1 << 30,
    Storage:  natsjs.FileStorage,
})
```

## Bus Setup

```go
registry := eventjson.NewRegistry()
registry.Register("chat.message", func() event.Event { return &ChatMessage{} }, 1)

chain := invoker.NewChain(
    invoker.NewMetrics(nil),
    invoker.NewIdempotency(invoker.NewMemoryIdempotencyStore(),
        invoker.IdempotencyConfig{ProcessingTTL: 5 * time.Minute}, nil),
    invoker.NewRetry(invoker.RetryPolicy{
        MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second,
    }, nil),
)

bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
    Invoker: chain,
})
```

## Subscribe and Publish

```go
bus.Subscribe(ctx, "chat.message.*", handler,
    event.WithStream("CHAT_EVENTS"),
    event.WithHandlerName("persist-message"),
    event.WithConsumer("chat-processor"),
)

// Subject defaults to evt.Name() ("chat.message")
bus.Emit(ctx, &ChatMessage{MessageID: "msg-1", RoomID: "room-general", Content: "Hello world"})

// Override subject for hierarchical routing
bus.Emit(ctx, &ChatMessage{MessageID: "msg-2", RoomID: "room-general", Content: "Hello again"},
    event.WithSubject("chat.message.room-general"),
)
```

## Replay from DLQ

```go
replayer := jetstream.NewReplayer(js)
replayer.Replay(ctx, jetstream.ReplayOptions{
    FromStream: "CHAT_EVENTS_DLQ",
    ToSubject:  "chat.message",
    Limit:      100,
})
```

## DLQ Publisher

```go
dlq := jetstream.NewDQL(js, nil, "")
chain := invoker.NewChain(
    invoker.NewRetry(policy, nil),
    invoker.NewDLQ(dlq, nil),
)
```
