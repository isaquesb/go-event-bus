---
layout: default
title: Event Interface
parent: Core Concepts
nav_order: 1
---

# Event Interface

## Base Interface

Every event must implement the `Event` interface:

```go
type Event interface {
    Name() string
}
```

Events are plain Go structs. The `Name()` method returns the event's routing key (e.g., `"user.created"`, `"order.placed"`).

```go
type UserCreated struct {
    UserID string
    Email  string
}

func (e UserCreated) Name() string { return "user.created" }
```

## Optional Interfaces

Behavior is opt-in via additional interfaces:

### WithVersion

```go
type WithVersion interface {
    Version() int
}
```

Used by the Registry for schema evolution. Events with versions can be upcasted from older formats.

### WithId

```go
type WithId interface {
    Id() string
}
```

Provides a unique identifier for the event. Used as a fallback key for idempotency checks when `WithIdempotencyKey` is not implemented.

### OnErr

```go
type OnErr interface {
    OnErr(ctx context.Context, err error, listenerName string)
}
```

Called when a handler fails processing the event. Allows event-level error handling, useful for critical events that need custom failure notification.

### WithIdempotencyKey (invoker package)

```go
type WithIdempotencyKey interface {
    IdempotencyKey() string
}
```

Returns a custom key for idempotency checks. Takes precedence over `WithId`.

### WithRateLimitKey (invoker package)

```go
type WithRateLimitKey interface {
    RateLimitKey() string
}
```

Returns the key used for rate limiting. Enables dynamic rate limit scoping (e.g., per user, per tenant).

## HandlerFunc

The library provides a generic handler type for type-safe event handling:

```go
type HandlerFunc[E Event] func(ctx context.Context, evt E) error
```

The standard handler signature used by `Subscribe` is:

```go
type HandleFn = func(context.Context, Event) error
```
