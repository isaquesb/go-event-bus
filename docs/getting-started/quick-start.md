---
layout: default
title: Quick Start
parent: Getting Started
nav_order: 2
---

# Quick Start

This guide walks you through creating a simple in-process event bus.

## 1. Define Events

Events are plain Go structs implementing the `Event` interface:

```go
type UserCreated struct {
    UserID string
    Email  string
}

func (e UserCreated) Name() string { return "user.created" }
```

## 2. Create the Bus

```go
import (
    event "github.com/isaquesb/go-event-bus"
    "github.com/isaquesb/go-event-bus/invoker"
)

chain := invoker.NewChain(
    invoker.NewMetrics(nil), // NoopProvider for development
)

bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,
    MaxConcurrent: 10,
    OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
        slog.Error("handler failed", "event", evt.Name(), "handler", handler, "error", err)
    },
})
```

## 3. Subscribe to Events

```go
ctx := context.Background()

bus.Subscribe(ctx, "user.created", func(ctx context.Context, evt event.Event) error {
    e := evt.(UserCreated)
    slog.Info("sending welcome email", "email", e.Email)
    return nil
}, event.WithHandlerName("send-welcome-email"))

bus.Subscribe(ctx, "user.created", func(ctx context.Context, evt event.Event) error {
    e := evt.(UserCreated)
    slog.Info("creating profile", "user_id", e.UserID)
    return nil
}, event.WithHandlerName("create-profile"))
```

Multiple handlers can subscribe to the same event. Each handler runs through the invoker chain.

## 4. Emit Events

```go
// Asynchronous - fire and forget
bus.Emit(ctx, UserCreated{UserID: "123", Email: "john@example.com"})

// Synchronous - waits for all handlers to complete
bus.EmitSync(ctx, UserCreated{UserID: "456", Email: "jane@example.com"})
```

## Next Steps

- Learn about the [Invoker Chain]({{ site.baseurl }}/invoker-chain/) for adding resilience
- Explore [Transports]({{ site.baseurl }}/transports/) for distributed messaging
- See full [Examples]({{ site.baseurl }}/examples/)
