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

func (s *EmailSubscriber) Name() string { return "email-subscriber" }

func (s *EmailSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{
        "user.signed_up": s.handleUserSignedUp,
        "user.upgraded":  s.handleUserUpgraded,
        "user.deleted":   s.handleUserDeleted,
    }
}

func (s *EmailSubscriber) handleUserSignedUp(ctx context.Context, evt event.Event) error {
    e := evt.(UserSignedUp)
    slog.Info("sending welcome email", "email", e.Email)
    return nil
}
```

## Bulk Registration

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber("smtp.example.com", "noreply@example.com"),
    NewAnalyticsSubscriber(),
    NewBillingSubscriber("sk_test_xxx"),
)
```

Each subscriber's `Name()` is used as the handler name for metrics and logging.

## OnErr Interface

Events can implement `OnErr` for event-level error notification:

```go
type CriticalEvent struct {
    ID      string
    Payload string
}

func (e *CriticalEvent) Name() string { return "critical.event" }

func (e *CriticalEvent) OnErr(ctx context.Context, err error, listenerName string) {
    slog.Error("critical event failed", "event_id", e.ID, "handler", listenerName, "error", err)
    // Alert, notify, etc.
}
```

`OnErr` is called by the bus after the global `OnErr` callback, giving the event itself a chance to react to failures.
