---
layout: default
title: Quick Start
parent: Getting Started
nav_order: 2
---

# Quick Start

This guide walks you through creating a simple in-process event bus.

The code on this page is compiled from [`examples/01_basic_local_bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/01_basic_local_bus.go).

## 1. Define Events

Events are plain Go structs implementing the `Event` interface:

```go
type UserCreated struct {
	UserID   string
	Email    string
	UserName string
}

func (e UserCreated) Name() string { return "user.created" }
```

## 2. Create the Bus

```go
import (
	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// An empty chain here; the invoker chain page has the recommended order
// of the resilience invokers
chain := invoker.NewChain()

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	MaxConcurrent: 10,
	// Observer receives every terminal failure once (Observer.Failure). The slog
	// observer is also the default when Observer is nil.
	Observer: event.NewSlogObserver(nil),
})
```

## 3. Subscribe to Events

```go
func SendWelcomeEmail(ctx context.Context, evt event.Event) error {
	e := evt.(UserCreated)
	slog.Info("sending welcome email", "user_id", e.UserID, "email", e.Email)
	// ... send email logic
	return nil
}

func CreateUserProfile(ctx context.Context, evt event.Event) error {
	e := evt.(UserCreated)
	slog.Info("creating user profile", "user_id", e.UserID, "name", e.UserName)
	// ... create profile logic
	return nil
}

ctx := context.Background()

if err := bus.Subscribe(ctx, "user.created", SendWelcomeEmail, event.WithHandlerName("send-welcome-email")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
if err := bus.Subscribe(ctx, "user.created", CreateUserProfile, event.WithHandlerName("create-user-profile")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
```

Multiple handlers can subscribe to the same event. Each handler runs through the invoker chain.

## 4. Emit Events

```go
// Synchronous - waits for all handlers to complete
if _, err := bus.EmitSync(ctx, UserCreated{
	UserID:   "user-123",
	Email:    "john@example.com",
	UserName: "John Doe",
}); err != nil {
	slog.Error("emit sync failed", "error", err)
}

// Asynchronous - fire and forget
bus.Emit(ctx, UserCreated{UserID: "user-456", Email: "jane@example.com", UserName: "Jane Doe"})
```

A handler that returns an error is not lost: the bus settles it once, and the terminal outcome reaches the Observer (here, the log). The [Failure Model]({{ site.baseurl }}/architecture/failure-model/) says what happens to each error.

## Next Steps

- Learn about the [Invoker Chain]({{ site.baseurl }}/invoker-chain/) for adding resilience
- Explore [Transports]({{ site.baseurl }}/transports/) for distributed messaging
- Wire logs, metrics and traces with [Observability]({{ site.baseurl }}/observability/)
- See full [Examples]({{ site.baseurl }}/examples/)
