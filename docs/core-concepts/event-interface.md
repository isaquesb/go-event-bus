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

`Id()` also becomes the **message id** of every envelope emitted for the event (see [Event Metadata](#event-metadata)). It must therefore be unique per message, not per entity: a `UserUpdated` event for user `42` must not return `"42"`, or every update would share one message id. Return a fresh value (for example `event.NewID()`) or leave `WithId` unimplemented and let the bus generate the id. The idempotency invoker reads the same value.

### OnErr

```go
type OnErr interface {
    OnErr(ctx context.Context, err error, listenerName string)
}
```

Called when a message of this event ends in a terminal failure. Allows event-level error handling, useful for critical events that need custom failure notification.

`OnErr` is the **only** error callback left. There is no bus-level error callback option; a bus reports failures to its `Observer` instead (see [Bus Interface]({{ site.baseurl }}/core-concepts/bus-interface/#observing-failures)). The order is fixed:

1. `Observer.Failure` runs first, once per terminal outcome of the message;
2. the event's own `OnErr(ctx, err, listenerName)` runs after it, once per terminal failure, with `listenerName` the handler name.

An intermediate retry or a redelivery that will still happen is not a terminal failure and does not call either. A decode failure has no event, so only `Observer.Failure` is called. A panic inside `OnErr` is isolated and reported as an `InternalError` signal; it never reaches the transport. The `ctx` it receives is the message context, so it carries the event [Metadata](#event-metadata) and trace:

```go
func (e ReportRequested) OnErr(ctx context.Context, err error, listenerName string) {
    slog.Error("report failed", "report_id", e.ReportID, "handler", listenerName, "error", err)
}
```

Like all hooks it runs synchronously on the handler goroutine, so it must be fast and must never block. See [Observability]({{ site.baseurl }}/observability/).

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

## Event Metadata

Every emitted event gets a stable identity and a causal lineage. The bus resolves them at `Emit` and handlers read them from the handler context. Nothing changes in the `Event`, `HandleFn`, `Invoker` or `Bus` signatures.

### Fields

```go
type Metadata struct {
    ID            string            // unique id of this message
    CorrelationID string            // id shared by every event of one causal tree
    CausationID   string            // id of the event that caused this one
    Name          string            // event name
    Version       int               // wire version (0 is read as 1)
    Timestamp     time.Time
    Headers       map[string]string // user headers plus traceparent/tracestate/baggage
}
```

`Metadata` is read-only by contract: do not mutate `Headers`.

### Reading metadata in a handler

```go
func (s *Orders) onPlaced(ctx context.Context, evt event.Event) error {
    md, ok := event.MetadataFromContext(ctx)
    if !ok {
        // the bus did not provide Metadata (for example, a handler called directly in a test)
    }
    log.Printf("message=%s correlation=%s cause=%s tenant=%s",
        md.ID, md.CorrelationID, md.CausationID, md.Headers["tenant"])
    return nil
}
```

On the consumer side of a serialized transport the Metadata is rebuilt from the envelope, so the handler sees the ids the producer wrote, including after a full encode/decode round trip.

### Setting metadata when emitting

```go
bus.Emit(ctx, evt,
    event.WithID("order-placed-7f3a"),
    event.WithCorrelationID(requestID),
    event.WithHeader("tenant", "acme"),
    event.WithHeader("region", "eu"),
)
```

- `event.WithID(id)` sets the message id.
- `event.WithCorrelationID(id)` sets the correlation id.
- `event.WithHeader(key, value)` attaches a user header. It can be repeated; for the same key the last value wins. There is no map variant. An empty key is ignored.
- There is no option to set the causation id. It always comes from the parent event (see below).

### Lineage rules

| Field | Resolved from, in order |
|---|---|
| `ID` | `WithID`, then `Id()` of a `WithId` event, then the bus `IDGenerator`, then a generated UUIDv7 (`event.NewID`) |
| `CorrelationID` | `WithCorrelationID`, then the parent's `CorrelationID`, then the parent's `ID`, then the event's own `ID` (a root event) |
| `CausationID` | The parent's `ID`. Empty for a root event. No option overrides it |

The **parent** is the event being handled: when you emit from inside a handler with the handler's `ctx`, the new event is its child. Emit with a context that carries no Metadata (for example `context.Background()`) and the event is a root: its correlation id is its own id.

```go
func (s *Orders) onPlaced(ctx context.Context, evt event.Event) error {
    // ctx carries the Metadata of the OrderPlaced being handled, so the
    // PaymentRequested below gets causation_id = that message id and
    // inherits its correlation id.
    return bus.Emit(ctx, PaymentRequested{OrderID: "42"})
}
```

### Header inheritance

User headers are **not** inherited by default. A child event only receives the parent's headers whose keys are listed in the bus `InheritHeaders` option; an explicit `WithHeader` overrides an inherited value. Trace context and correlation propagate independently of this list. Headers are producer data visible to every consumer, so do not put secrets in them. See the per-bus options on the transport pages: [LocalBus](../transports/local-bus.md), [NATS](../transports/nats-bus.md) and [JetStream](../transports/jetstream-bus.md).

### Id generation

Ids are RFC 9562 UUIDv7 strings by default (`event.NewID`, standard library only). Ids from one process are strictly increasing. To use another scheme, set `IDGenerator func() string` on the bus options (`LocalBusOptions`, `nats.BusOptions`, `jetstream.BusOptions`) or on the JSON registry with `eventjson.WithIDGenerator`. A single message can override everything with `event.WithID`.

## HandlerFunc

The library provides a generic handler type for type-safe event handling:

```go
type HandlerFunc[E Event] func(ctx context.Context, evt E) error
```

The standard handler signature used by `Subscribe` is:

```go
type HandleFn = func(context.Context, Event) error
```
