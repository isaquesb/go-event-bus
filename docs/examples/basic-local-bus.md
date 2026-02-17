---
layout: default
title: Basic LocalBus
parent: Examples
nav_order: 1
---

# Basic LocalBus

The simplest way to use Go Event Bus for in-process event-driven communication.

Source: [`examples/01_basic_local_bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/01_basic_local_bus.go)

## Define Events

```go
type UserCreated struct {
    UserID   string
    Email    string
    UserName string
}

func (e UserCreated) Name() string { return "user.created" }

type OrderPlaced struct {
    OrderID string
    UserID  string
    Total   float64
}

func (e OrderPlaced) Name() string { return "order.placed" }
```

## Define Handlers

```go
func SendWelcomeEmail(ctx context.Context, evt event.Event) error {
    e := evt.(UserCreated)
    slog.Info("sending welcome email", "user_id", e.UserID, "email", e.Email)
    return nil
}

func CreateUserProfile(ctx context.Context, evt event.Event) error {
    e := evt.(UserCreated)
    slog.Info("creating user profile", "user_id", e.UserID, "name", e.UserName)
    return nil
}
```

## Wire It Up

```go
chain := invoker.NewChain(
    invoker.NewMetrics(nil),
)

bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,
    MaxConcurrent: 10,
    OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
        slog.Error("handler failed", "event", evt.Name(), "handler", handler, "error", err)
    },
})

ctx := context.Background()

// Multiple handlers for the same event
bus.Subscribe(ctx, "user.created", SendWelcomeEmail, event.WithHandlerName("send-welcome-email"))
bus.Subscribe(ctx, "user.created", CreateUserProfile, event.WithHandlerName("create-user-profile"))
bus.Subscribe(ctx, "order.placed", NotifyWarehouse, event.WithHandlerName("notify-warehouse"))

// Synchronous dispatch
bus.EmitSync(ctx, UserCreated{
    UserID:   "user-123",
    Email:    "john@example.com",
    UserName: "John Doe",
})

// Asynchronous dispatch
bus.Emit(ctx, OrderPlaced{
    OrderID: "order-456",
    UserID:  "user-123",
    Total:   99.99,
})
```
