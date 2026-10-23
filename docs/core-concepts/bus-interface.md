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

All transports (LocalBus, NatsBus, JetStreamBus) implement this interface. `Close` is `Shutdown` bounded by the bus `ShutdownTimeout` option (30 seconds by default) and returns its error.

## SyncBus

For buses that support synchronous dispatch:

```go
type SyncBus interface {
    EmitSync(ctx context.Context, e Event) (Event, error)
}
```

`LocalBus` implements `SyncBus` to wait for all handlers to complete before returning. It returns the same event and the `errors.Join` of the terminal failures of the handlers: a dead-lettered message or a duplicate is a handled outcome, not an error, so the error is nil when every failure was handled. The NATS bus returns the decoded reply event, or a `*nats.RemoteError` when the responder failed.

## Shutdowner

A bus that can shut down gracefully also implements `event.Shutdowner`:

```go
type Shutdowner interface {
    Shutdown(ctx context.Context) error
}
```

All three buses implement it, so application code can stop any of them without knowing the transport:

```go
if s, ok := bus.(event.Shutdowner); ok {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    if err := s.Shutdown(ctx); err != nil {
        slog.Error("shutdown did not drain", "error", err)
    }
}
```

`Shutdown(ctx)` stops accepting new work, drains the in-flight handlers bounded by `ctx`, and when `ctx` expires first cancels them with cause `event.ErrShutdownInterrupted` and returns `ctx.Err()`. An interrupted message is reported and never dead-lettered. From the moment `Shutdown` or `Close` begins, `Emit`, `EmitSync` and `Subscribe` return or report `event.ErrBusClosed`; work that was already accepted drains. The transport pages describe the details: [LocalBus]({{ site.baseurl }}/transports/local-bus/#shutdown-and-close), [NATS Bus]({{ site.baseurl }}/transports/nats-bus/#shutdown-and-close) and [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/#shutdown-and-close).

## Observing failures

A bus has no error callback. Every bus takes an `Observer` option, and `Observer.Failure` is called once per terminal outcome of a message, with the handler, outcome, reason, attempts, delivery and failing invoker. The event's own `OnErr` method still runs afterwards (see [Event Interface]({{ site.baseurl }}/core-concepts/event-interface/#onerr)). When `Observer` is nil the bus uses `event.NewSlogObserver(nil)`, so failures are never silent with zero configuration. See [Observability]({{ site.baseurl }}/observability/) for the signals.

This is compiled from [`examples/10_failure_hooks.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/10_failure_hooks.go):

```go
// billingAlerts embeds event.NopObserver, so it implements every Observer
// method and keeps compiling when the interface gains methods. It overrides
// only Failure.
type billingAlerts struct {
	event.NopObserver
}

// Failure is called once per terminal outcome, never on an intermediate retry.
func (a *billingAlerts) Failure(ctx context.Context, f event.FailureContext) {
	slog.Warn("message ended",
		"handler", f.Handler,
		"outcome", f.Outcome,
		"reason", f.Reason,
		"attempts", f.Attempts,
		"delivery", f.Delivery.Count,
		"max_delivery", f.Delivery.Max,
		"breaker", f.Breaker,
	)
}
```

```go
bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker: chain,
	// MultiObserver fans one signal out to several observers and isolates
	// the panic of any of them.
	Observer: event.MultiObserver(
		event.NewSlogObserver(nil, event.WithLogEvery(30*time.Second)),
		&billingAlerts{},
	),
})
```

## RegisterSubscribers

Groups multiple event handlers into a single subscriber. Available on **all buses** (LocalBus, NatsBus, JetStreamBus):

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

Each subscriber's `Name()` is automatically used as the handler name for metrics and logging.

## SubscribeOptionsProvider

Subscribers can implement `SubscribeOptionsProvider` to carry transport-specific options (e.g. JetStream stream name). `RegisterSubscribers` detects this interface automatically:

```go
type SubscribeOptionsProvider interface {
    SubscribeOptions() []SubscribeOption
}
```

Example for JetStream:

```go
type ChatSubscriber struct{}

func (s *ChatSubscriber) Name() string { return "chat-subscriber" }

func (s *ChatSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{"chat.message.*": s.handle}
}

// Provides stream/consumer options for all handlers in this subscriber.
func (s *ChatSubscriber) SubscribeOptions() []event.SubscribeOption {
    return []event.SubscribeOption{
        event.WithStream("CHAT_EVENTS"),
        event.WithConsumer("chat-processor"),
    }
}
```

## LocalBus

The `LocalBus` is the simplest bus implementation for in-process event dispatch:

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,                      // optional: invoker chain
    MaxConcurrent: 16,                         // concurrency limit (default: 16)
    Observer:      event.NewSlogObserver(nil), // optional: receives every failure signal
})
```

It provides:
- `Emit(ctx, event, opts...)` - Asynchronous, fire-and-forget dispatch
- `EmitSync(ctx, event, opts...) (Event, error)` - Synchronous dispatch, waits for handlers and returns the joined terminal failures
- `Subscribe(ctx, subject, handler, opts...)` - Register handlers
- `RegisterSubscribers(ctx, subs...)` - Bulk handler registration
- `Shutdown(ctx)` and `Close()` - Drain the in-flight handlers within a bound

See [LocalBus]({{ site.baseurl }}/transports/local-bus/) for every option.
