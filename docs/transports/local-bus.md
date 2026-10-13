---
layout: default
title: LocalBus
parent: Transports
nav_order: 1
---

# LocalBus

The LocalBus is an in-process event dispatcher for domain events and internal decoupling.

## Creating a LocalBus

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,
    MaxConcurrent: 16,
    OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
        slog.Error("handler failed", "event", evt.Name(), "handler", handler, "error", err)
    },
})
```

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `Invoker` | `event.Invoker` | required | The invoker chain for handler execution |
| `MaxConcurrent` | `int` | 16 | Maximum concurrent async handlers |
| `OnErr` | `func(...)` | nil | Global error callback |
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

## Dispatch Modes

### Asynchronous (Emit)

```go
bus.Emit(ctx, UserCreated{UserID: "123"})
```

Fire-and-forget. Handlers run in goroutines, limited by `MaxConcurrent`. The semaphore prevents unbounded goroutine growth.

### Synchronous (EmitSync)

```go
bus.EmitSync(ctx, UserCreated{UserID: "123"})
```

Waits for all handlers to complete before returning. Handlers run sequentially in the calling goroutine.

## Metadata

`Emit` and `EmitSync` resolve the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) (message id, correlation id, causation id, headers) and put it in the context of every handler. Handlers read it with `event.MetadataFromContext`:

```go
func handle(ctx context.Context, evt event.Event) error {
    md, _ := event.MetadataFromContext(ctx)
    log.Printf("message=%s correlation=%s", md.ID, md.CorrelationID)
    return nil
}
```

Both methods accept `event.WithID`, `event.WithCorrelationID` and `event.WithHeader`. When a handler emits a new event with its own `ctx`, the new event is a child of the handled one: its `causation_id` is the handled event's id and it inherits the correlation id.

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

LocalBus has no envelope, so there is no `traceparent` header: trace context flows through `ctx` as before.

## Concurrency Model

The `MaxConcurrent` setting controls a semaphore channel. When `Emit` is called:

1. For each handler, attempt to acquire a semaphore slot
2. If a slot is available, run the handler in a goroutine
3. If no slot is available, block until one frees up (or context is cancelled)
4. Release the slot when the handler completes

## Error Handling

Errors from handlers are reported via:
1. The `OnErr` callback (if set)
2. The event's `OnErr` method (if the event implements `OnErr`)

Errors do **not** propagate to the caller of `Emit` or `EmitSync`.

## RegisterSubscribers

Bulk registration for organized handler grouping:

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber(),
    NewBillingSubscriber(),
)
```

Each subscriber's `Name()` is used as the handler name. Subscribers implementing `SubscribeOptionsProvider` will have their options merged automatically.

## Close

`Close()` is a no-op for LocalBus since there are no external connections.
