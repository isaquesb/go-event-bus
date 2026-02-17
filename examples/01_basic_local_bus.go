// Example: Basic LocalBus Usage
//
// This example demonstrates the simplest way to use the LocalBus
// for in-process event-driven communication.
package examples

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// =============================================================================
// Event Definitions
// =============================================================================

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

// =============================================================================
// Handlers
// =============================================================================

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

// NotifyWarehouse handles order.placed events
func NotifyWarehouse(ctx context.Context, evt event.Event) error {
	e := evt.(OrderPlaced)
	slog.Info("notifying warehouse", "order_id", e.OrderID, "total", e.Total)
	// ... notify warehouse logic
	return nil
}

// =============================================================================
// Example Usage
// =============================================================================

func ExampleBasicLocalBus() {
	// Create a simple invoker chain (just metrics for this example)
	chain := invoker.NewChain(
		invoker.NewMetrics(nil), // Uses NoopProvider
	)

	// Create the LocalBus
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 10,
		OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
			slog.Error("handler failed",
				"event", evt.Name(),
				"handler", handler,
				"error", err,
			)
		},
	})

	ctx := context.Background()

	// Register handlers
	bus.Subscribe(ctx, "user.created", SendWelcomeEmail, event.WithHandlerName("send-welcome-email"))
	bus.Subscribe(ctx, "user.created", CreateUserProfile, event.WithHandlerName("create-user-profile"))
	bus.Subscribe(ctx, "order.placed", NotifyWarehouse, event.WithHandlerName("notify-warehouse"))

	// Synchronous - waits for all handlers to complete
	bus.EmitSync(ctx, UserCreated{
		UserID:   "user-123",
		Email:    "john@example.com",
		UserName: "John Doe",
	})

	// Asynchronous - fire and forget
	bus.Emit(ctx, OrderPlaced{
		OrderID: "order-456",
		UserID:  "user-123",
		Total:   99.99,
	})

	fmt.Println("Events emitted successfully")
}
