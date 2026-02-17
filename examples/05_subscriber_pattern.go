// Example: Subscriber Pattern
//
// This example demonstrates how to organize event handlers using
// the Subscriber interface for clean, modular code organization.
package examples

import (
	"context"
	"errors"
	"log/slog"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// =============================================================================
// Domain Events
// =============================================================================

type UserSignedUp struct {
	UserID string
	Email  string
	Plan   string
}

func (e UserSignedUp) Name() string { return "user.signed_up" }

type UserUpgraded struct {
	UserID  string
	OldPlan string
	NewPlan string
}

func (e UserUpgraded) Name() string { return "user.upgraded" }

type UserDeleted struct {
	UserID string
	Reason string
}

func (e UserDeleted) Name() string { return "user.deleted" }

// =============================================================================
// Email Subscriber - Handles all email-related reactions
// =============================================================================

type EmailSubscriber struct {
	smtpHost string
	from     string
}

func NewEmailSubscriber(smtpHost, from string) *EmailSubscriber {
	return &EmailSubscriber{smtpHost: smtpHost, from: from}
}

// Name identifies this subscriber for logging and metrics
func (s *EmailSubscriber) Name() string {
	return "email-subscriber"
}

// Events returns the event handlers this subscriber provides
func (s *EmailSubscriber) Events() map[string]event.HandleFn {
	return map[string]event.HandleFn{
		"user.signed_up": s.handleUserSignedUp,
		"user.upgraded":  s.handleUserUpgraded,
		"user.deleted":   s.handleUserDeleted,
	}
}

func (s *EmailSubscriber) handleUserSignedUp(ctx context.Context, evt event.Event) error {
	e := evt.(UserSignedUp)
	slog.Info("sending welcome email",
		"user_id", e.UserID,
		"email", e.Email,
		"plan", e.Plan,
	)
	// ... send welcome email
	return nil
}

func (s *EmailSubscriber) handleUserUpgraded(ctx context.Context, evt event.Event) error {
	e := evt.(UserUpgraded)
	slog.Info("sending upgrade confirmation",
		"user_id", e.UserID,
		"old_plan", e.OldPlan,
		"new_plan", e.NewPlan,
	)
	// ... send upgrade email
	return nil
}

func (s *EmailSubscriber) handleUserDeleted(ctx context.Context, evt event.Event) error {
	e := evt.(UserDeleted)
	slog.Info("sending goodbye email",
		"user_id", e.UserID,
		"reason", e.Reason,
	)
	// ... send goodbye email
	return nil
}

// =============================================================================
// Analytics Subscriber - Tracks user events for analytics
// =============================================================================

type AnalyticsSubscriber struct {
	analyticsClient interface{} // Your analytics client
}

func NewAnalyticsSubscriber() *AnalyticsSubscriber {
	return &AnalyticsSubscriber{}
}

func (s *AnalyticsSubscriber) Name() string {
	return "analytics-subscriber"
}

func (s *AnalyticsSubscriber) Events() map[string]event.HandleFn {
	return map[string]event.HandleFn{
		"user.signed_up": s.trackSignup,
		"user.upgraded":  s.trackUpgrade,
	}
}

func (s *AnalyticsSubscriber) trackSignup(ctx context.Context, evt event.Event) error {
	e := evt.(UserSignedUp)
	slog.Info("tracking signup",
		"user_id", e.UserID,
		"plan", e.Plan,
	)
	// ... send to analytics
	return nil
}

func (s *AnalyticsSubscriber) trackUpgrade(ctx context.Context, evt event.Event) error {
	e := evt.(UserUpgraded)
	slog.Info("tracking upgrade",
		"user_id", e.UserID,
		"from", e.OldPlan,
		"to", e.NewPlan,
	)
	// ... send to analytics
	return nil
}

// =============================================================================
// Billing Subscriber - Handles billing-related reactions
// =============================================================================

type BillingSubscriber struct {
	stripeKey string
}

func NewBillingSubscriber(stripeKey string) *BillingSubscriber {
	return &BillingSubscriber{stripeKey: stripeKey}
}

func (s *BillingSubscriber) Name() string {
	return "billing-subscriber"
}

func (s *BillingSubscriber) Events() map[string]event.HandleFn {
	return map[string]event.HandleFn{
		"user.signed_up": s.createCustomer,
		"user.upgraded":  s.updateSubscription,
		"user.deleted":   s.cancelSubscription,
	}
}

func (s *BillingSubscriber) createCustomer(ctx context.Context, evt event.Event) error {
	e := evt.(UserSignedUp)
	slog.Info("creating Stripe customer",
		"user_id", e.UserID,
		"email", e.Email,
	)
	// ... create Stripe customer
	return nil
}

func (s *BillingSubscriber) updateSubscription(ctx context.Context, evt event.Event) error {
	e := evt.(UserUpgraded)
	slog.Info("updating subscription",
		"user_id", e.UserID,
		"new_plan", e.NewPlan,
	)
	// ... update Stripe subscription
	return nil
}

func (s *BillingSubscriber) cancelSubscription(ctx context.Context, evt event.Event) error {
	e := evt.(UserDeleted)
	slog.Info("canceling subscription",
		"user_id", e.UserID,
	)
	// ... cancel Stripe subscription
	return nil
}

// =============================================================================
// Event with OnErr Interface
// =============================================================================

// CriticalEvent demonstrates the OnErr interface for event-level error handling
type CriticalEvent struct {
	ID      string
	Payload string
	onErr   func(err error)
}

func (e *CriticalEvent) Name() string { return "critical.event" }

// OnErr is called when any handler fails
func (e *CriticalEvent) OnErr(ctx context.Context, err error, listenerName string) {
	slog.Error("critical event handler failed",
		"event_id", e.ID,
		"handler", listenerName,
		"error", err,
	)
	if e.onErr != nil {
		e.onErr(err)
	}
}

// =============================================================================
// Example Usage
// =============================================================================

func ExampleSubscriberPattern() {
	// Create bus with invoker chain
	chain := invoker.NewChain(
		invoker.NewMetrics(nil),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100,
			MaxDelay:    1000,
		}, nil),
	)

	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:       chain,
		MaxConcurrent: 16,
		OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
			slog.Error("handler failed",
				"event", evt.Name(),
				"handler", handler,
				"error", err,
			)
		},
	})

	ctx := context.Background()

	// Register all subscribers at once
	bus.RegisterSubscribers(ctx,
		NewEmailSubscriber("smtp.example.com", "noreply@example.com"),
		NewAnalyticsSubscriber(),
		NewBillingSubscriber("sk_test_xxx"),
	)

	// Alternative: Register individual handlers
	bus.Subscribe(ctx, "user.signed_up", func(ctx context.Context, evt event.Event) error {
		e := evt.(UserSignedUp)
		slog.Info("audit: user signed up", "user_id", e.UserID)
		return nil
	}, event.WithHandlerName("audit-log"))

	// This will trigger:
	// - EmailSubscriber.handleUserSignedUp
	// - AnalyticsSubscriber.trackSignup
	// - BillingSubscriber.createCustomer
	// - audit-log handler
	bus.EmitSync(ctx, UserSignedUp{
		UserID: "user-123",
		Email:  "john@example.com",
		Plan:   "pro",
	})

	// This will trigger:
	// - EmailSubscriber.handleUserUpgraded
	// - AnalyticsSubscriber.trackUpgrade
	// - BillingSubscriber.updateSubscription
	bus.EmitSync(ctx, UserUpgraded{
		UserID:  "user-123",
		OldPlan: "pro",
		NewPlan: "enterprise",
	})

	// Example with OnErr interface
	errChan := make(chan error, 1)
	bus.Subscribe(ctx, "critical.event", func(ctx context.Context, evt event.Event) error {
		return errors.New("simulated failure")
	}, event.WithHandlerName("failing-handler"))

	bus.EmitSync(ctx, &CriticalEvent{
		ID:      "critical-1",
		Payload: "important data",
		onErr: func(err error) {
			errChan <- err
		},
	})
}
