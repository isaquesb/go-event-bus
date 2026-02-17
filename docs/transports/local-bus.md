---
layout: default
title: LocalBus
parent: Transports
nav_order: 1
---

# LocalBus

The LocalBus is an in-process event dispatcher for domain events and internal decoupling.

## Creating a LocalBus

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       chain,
    MaxConcurrent: 16,
    OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
        slog.Error("handler failed", "event", evt.Name(), "handler", handler, "error", err)
    },
})
```

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `Invoker` | `event.Invoker` | required | The invoker chain for handler execution |
| `MaxConcurrent` | `int` | 16 | Maximum concurrent async handlers |
| `OnErr` | `func(...)` | nil | Global error callback |

## Dispatch Modes

### Asynchronous (Emit)

```go
bus.Emit(ctx, UserCreated{UserID: "123"})
```

Fire-and-forget. Handlers run in goroutines, limited by `MaxConcurrent`. The semaphore prevents unbounded goroutine growth.

### Synchronous (EmitSync)

```go
bus.EmitSync(ctx, UserCreated{UserID: "123"})
```

Waits for all handlers to complete before returning. Handlers run sequentially in the calling goroutine.

## Concurrency Model

The `MaxConcurrent` setting controls a semaphore channel. When `Emit` is called:

1. For each handler, attempt to acquire a semaphore slot
2. If a slot is available, run the handler in a goroutine
3. If no slot is available, block until one frees up (or context is cancelled)
4. Release the slot when the handler completes

## Error Handling

Errors from handlers are reported via:
1. The `OnErr` callback (if set)
2. The event's `OnErr` method (if the event implements `OnErr`)

Errors do **not** propagate to the caller of `Emit` or `EmitSync`.

## Close

`Close()` is a no-op for LocalBus since there are no external connections.
