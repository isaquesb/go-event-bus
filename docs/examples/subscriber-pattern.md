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

## JetStream Subscribers with SubscribeOptionsProvider

For JetStream buses, subscribers must provide stream configuration. Implement `SubscribeOptionsProvider` to carry these options:

```go
type ChatSubscriber struct{}

func (s *ChatSubscriber) Name() string { return "chat-subscriber" }

func (s *ChatSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{
        "chat.message.*": s.handleMessage,
    }
}

// SubscribeOptions provides JetStream-specific options for all handlers.
func (s *ChatSubscriber) SubscribeOptions() []event.SubscribeOption {
    return []event.SubscribeOption{
        event.WithStream("CHAT_EVENTS"),
        event.WithConsumer("chat-processor"),
        event.WithMaxDeliver(5),
    }
}

func (s *ChatSubscriber) handleMessage(ctx context.Context, evt event.Event) error {
    msg := evt.(*ChatMessage)
    slog.Info("processing message", "id", msg.MessageID, "room", msg.RoomID)
    return nil
}
```

Registration is identical across all buses:

```go
jsBus.RegisterSubscribers(ctx, &ChatSubscriber{})
```

`RegisterSubscribers` automatically detects `SubscribeOptionsProvider` and merges the provided options with the handler name.

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
