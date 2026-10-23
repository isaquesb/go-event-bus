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
// UserCreated is emitted when a new user registers
type UserCreated struct {
	UserID   string
	Email    string
	UserName string
}

func (e UserCreated) Name() string { return "user.created" }

// OrderPlaced is emitted when an order is placed
type OrderPlaced struct {
	OrderID string
	UserID  string
	Total   float64
}

func (e OrderPlaced) Name() string { return "order.placed" }
```

## Define Handlers

```go
// SendWelcomeEmail handles user.created events
func SendWelcomeEmail(ctx context.Context, evt event.Event) error {
	e := evt.(UserCreated)
	slog.Info("sending welcome email", "user_id", e.UserID, "email", e.Email)
	// ... send email logic
	return nil
}

// CreateUserProfile handles user.created events
func CreateUserProfile(ctx context.Context, evt event.Event) error {
	e := evt.(UserCreated)
	slog.Info("creating user profile", "user_id", e.UserID, "name", e.UserName)
	// ... create profile logic
	return nil
}
```

## Wire It Up

```go
// Create a simple invoker chain (empty here; see 02_invoker_chain.go for the
// recommended order of the resilience invokers)
chain := invoker.NewChain()

// Create the LocalBus
bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	MaxConcurrent: 10,
	// Observer receives every terminal failure once (Observer.Failure). The slog
	// observer is also the default when Observer is nil.
	Observer: event.NewSlogObserver(nil),
})

ctx := context.Background()

// Register handlers
if err := bus.Subscribe(ctx, "user.created", SendWelcomeEmail, event.WithHandlerName("send-welcome-email")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
if err := bus.Subscribe(ctx, "user.created", CreateUserProfile, event.WithHandlerName("create-user-profile")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}
if err := bus.Subscribe(ctx, "order.placed", NotifyWarehouse, event.WithHandlerName("notify-warehouse")); err != nil {
	slog.Error("subscribe failed", "error", err)
	return
}

// Synchronous - waits for all handlers to complete
if _, err := bus.EmitSync(ctx, UserCreated{
	UserID:   "user-123",
	Email:    "john@example.com",
	UserName: "John Doe",
}); err != nil {
	slog.Error("emit sync failed", "error", err)
}

// Asynchronous - fire and forget
bus.Emit(ctx, OrderPlaced{
	OrderID: "order-456",
	UserID:  "user-123",
	Total:   99.99,
})
```

## Reporting failures

The bus has no error callback. Failures are reported through the `Observer`: it receives every terminal failure once, through `Observer.Failure`. The slog observer is the default when `Observer` is nil. See [Failure Hooks]({{ site.baseurl }}/examples/failure-hooks/) to write your own, and [Observability]({{ site.baseurl }}/observability/) for the full reference.
