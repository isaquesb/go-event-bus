---
layout: default
title: Bus Interface
parent: Core Concepts
nav_order: 2
---

# Bus Interface

## Bus

The core `Bus` interface defines the minimum contract for all event buses:

```go
type Bus interface {
    Subscribe(ctx context.Context, subject string, handler HandleFn, opts ...SubscribeOption) error
    Close() error
}
```

All transports (LocalBus, NatsBus, JetStreamBus) implement this interface.

## SyncBus

For buses that support synchronous dispatch:

```go
type SyncBus interface {
    EmitSync(ctx context.Context, e Event)
}
```

`LocalBus` implements `SyncBus` to wait for all handlers to complete before returning.

## RegisterSubscribers

Groups multiple event handlers into a single subscriber:

```go
type RegisterSubscribers interface {
    RegisterSubscribers(ctx context.Context, subs ...Subscriber) error
}
```

Where `Subscriber` is:

```go
type Subscriber interface {
    Name() string
    Events() map[string]HandleFn
}
```

This pattern enables clean, modular handler organization:

```go
type EmailSubscriber struct{}

func (s *EmailSubscriber) Name() string { return "email-subscriber" }

func (s *EmailSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{
        "user.created": s.handleUserCreated,
        "order.placed": s.handleOrderPlaced,
    }
}

// Register all handlers at once
bus.RegisterSubscribers(ctx, &EmailSubscriber{}, &BillingSubscriber{})
```

## LocalBus

The `LocalBus` is the simplest bus implementation for in-process event dispatch:

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,         // Required: invoker chain
    MaxConcurrent: 16,            // Concurrency limit (default: 16)
    OnErr:         errorHandler,  // Optional: global error callback
})
```

It provides:
- `Emit(ctx, event)` - Asynchronous, fire-and-forget dispatch
- `EmitSync(ctx, event)` - Synchronous dispatch, waits for handlers
- `Subscribe(ctx, subject, handler, opts...)` - Register handlers
- `RegisterSubscribers(ctx, subs...)` - Bulk handler registration
- `Close()` - No-op for LocalBus (no external connections)
