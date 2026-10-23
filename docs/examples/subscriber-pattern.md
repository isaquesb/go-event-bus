---
layout: default
title: Subscriber Pattern
parent: Examples
nav_order: 5
---

# Subscriber Pattern

Organize handlers into modular subscribers for clean code organization.

Source: [`examples/05_subscriber_pattern.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/05_subscriber_pattern.go)

## The Subscriber Interface

```go
type Subscriber interface {
	Name() string
	Events() map[string]HandleFn
}
```

## Example: Email Subscriber

```go
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
```

## Bulk Registration

```go
chain := invoker.NewChain(
	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   100,
		MaxDelay:    1000,
	}),
)

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	MaxConcurrent: 16,
	// Observer receives every terminal failure once (Observer.Failure). The slog
	// observer is also the default when Observer is nil.
	Observer: event.NewSlogObserver(nil),
})

ctx := context.Background()

// Register all subscribers at once
if err := bus.RegisterSubscribers(ctx,
	NewEmailSubscriber("smtp.example.com", "noreply@example.com"),
	NewAnalyticsSubscriber(),
	NewBillingSubscriber("sk_test_xxx"),
); err != nil {
	slog.Error("register subscribers failed", "error", err)
	return
}
```

Each subscriber's `Name()` is used as the handler name for logs and the `handler` label on metrics.

## Subscribers with transport options

A transport may need options for every handler of a subscriber, for example the JetStream stream name (`WithStream` is required on every JetStream `Subscribe`). A subscriber carries them by implementing the optional `SubscribeOptionsProvider` interface:

```go
// SubscribeOptionsProvider can be implemented by Subscriber to provide
// additional subscribe options (e.g. stream name for JetStream).
type SubscribeOptionsProvider interface {
	SubscribeOptions() []SubscribeOption
}
```

`RegisterSubscribers` detects the interface and merges the returned options with the handler name, identically on the LocalBus, NATS and JetStream buses. A JetStream subscriber that does not implement it fails with `ErrStreamRequired`. See [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/).

## OnErr Interface

Events can implement `OnErr` for event-level error notification:

```go
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
```

The event's own `OnErr` runs after `Observer.Failure`, once per terminal failure, giving the event itself a chance to react. It is optional and independent of the bus `Observer`. See [Failure Hooks]({{ site.baseurl }}/examples/failure-hooks/).
