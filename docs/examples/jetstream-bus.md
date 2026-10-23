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
// Connect to NATS
nc, err := nats.Connect(
	nats.DefaultURL,
	nats.Name("chat-service"),
	nats.ReconnectWait(time.Second),
	nats.MaxReconnects(-1),
)
if err != nil {
	slog.Error("failed to connect to NATS", "error", err)
	return
}
defer nc.Close()

// Get JetStream context
js, err := natsjs.New(nc)
if err != nil {
	slog.Error("failed to get JetStream", "error", err)
	return
}

// Create or update stream
_, err = js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
	Name:        StreamName,
	Description: "Chat events stream",
	Subjects:    []string{SubjectPattern},
	Retention:   natsjs.LimitsPolicy,
	MaxAge:      7 * 24 * time.Hour, // Keep 7 days
	MaxBytes:    1 << 30,            // 1 GB
	Replicas:    1,                  // Use 3 for production
	Storage:     natsjs.FileStorage,
})
if err != nil {
	slog.Error("failed to create stream", "error", err)
	return
}
```

## Bus Setup

```go
// Setup registry
// Note: json.Registry.Register signature is (name, factory, version)
registry := eventjson.NewRegistry()
registry.Register("chat.message", func() event.Event { return &ChatMessage{} }, 1)

// Setup invoker chain
chain := invoker.NewChain(
	invoker.NewIdempotency(
		invoker.NewMemoryIdempotencyStore(),
		invoker.IdempotencyConfig{ProcessingTTL: 5 * time.Minute},
	),
	invoker.NewRetry(
		invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		},
	),
)

// Create JetStream bus
bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
	Invoker: chain,
	// Observer receives every terminal failure once (Observer.Failure). The slog
	// observer is also the default when Observer is nil.
	Observer: event.NewSlogObserver(nil),
})
```

## Subscribe and Publish

```go
// Subscribe to events using base event options
err = bus.Subscribe(
	ctx,
	"chat.message.*", // Filter by room
	func(ctx context.Context, evt event.Event) error {
		msg := evt.(*ChatMessage)
		slog.Info("persisting message",
			"message_id", msg.MessageID,
			"room_id", msg.RoomID,
			"user_id", msg.UserID,
		)
		// ... persist to database
		return nil
	},
	event.WithStream(StreamName),
	event.WithHandlerName("persist-message"),
	event.WithConsumer(ConsumerName),
)
if err != nil {
	slog.Error("failed to subscribe", "error", err)
	return
}

// Publish events
for i := 0; i < 5; i++ {
	msg := &ChatMessage{
		MessageID: fmt.Sprintf("msg-%d", i),
		RoomID:    "room-general",
		UserID:    "user-123",
		Content:   fmt.Sprintf("Hello world #%d", i),
		Timestamp: time.Now(),
	}

	subject := fmt.Sprintf("chat.message.%s", msg.RoomID)
	if err := bus.Emit(ctx, msg, event.WithSubject(subject)); err != nil {
		slog.Error("failed to publish", "error", err)
	}
}
```

The subject defaults to `evt.Name()`; `event.WithSubject` overrides it for hierarchical routing.

## Replay from DLQ

```go
// Create replayer
replayer := jetstream.NewReplayer(js)

// Replay failed events from the DLQ back to their original subjects.
// ToSubject is an optional override that sends every record to one subject.
res, err := replayer.Replay(ctx, jetstream.ReplayOptions{
	FromStream: "CHAT_EVENTS_DLQ", // Source: DLQ stream
	Limit:      100,               // Replay up to 100 events
})
if err != nil {
	slog.Error("replay failed", "error", err, "replayed", res.Replayed, "failed", res.Failed)
	return
}

slog.Info("replay completed", "replayed", res.Replayed, "failed", res.Failed)
```

See [DLQ & Replay]({{ site.baseurl }}/examples/dlq-replay/) for the full flow and its real-server proof.

## DLQ Publisher

```go
// Create DLQ stream first
_, _ = js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
	Name:     "CHAT_EVENTS_DLQ",
	Subjects: []string{"dlq.>"},
	MaxAge:   30 * 24 * time.Hour, // Keep 30 days for investigation
})

// Create DLQ publisher. The empty prefix means the default "dlq.", which
// matches the "dlq.>" stream above.
dlq := jetstream.NewDLQ(js, "")

// In your invoker chain, DLQ sits outside Retry so it dead-letters the
// final outcome once:
chain := invoker.NewChain(
	invoker.NewDLQ(dlq), // Final failures go to DLQ
	invoker.NewRetry(
		invoker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second},
	),
)

// Pass opts to jetstream.NewBus. The same publisher is the bus backstop for
// undecodable messages and flow-control failures the chain did not dead-letter.
opts := jetstream.BusOptions{
	Invoker:          chain,
	DLQPublisher:     dlq,
	CircuitOpenDelay: 30 * time.Second,
	RateLimitDelay:   5 * time.Second,
}
```
