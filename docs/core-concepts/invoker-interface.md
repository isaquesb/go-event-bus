---
layout: default
title: Invoker Interface
parent: Core Concepts
nav_order: 3
---

# Invoker Interface

The `Invoker` is the fundamental building block of the execution pipeline:

```go
type Invoker interface {
    Invoke(
        ctx context.Context,
        evt Event,
        handlerName string,
        handleFn func(context.Context) error,
    ) error
}
```

## Semantics

Each invoker receives:
- `ctx` - The context for the current execution
- `evt` - The event being processed
- `handlerName` - Identifier for the handler (used in metrics/logging)
- `handleFn` - The next step in the chain (either another invoker or the final handler)

An invoker can:
- **Execute** `handleFn` to continue the chain
- **Skip** execution (e.g., idempotency check finds duplicate)
- **Retry** execution (e.g., retry invoker)
- **Short-circuit** with an error (e.g., circuit breaker is open)
- **Wrap** execution with observability (e.g., tracing, metrics)

## Example: Custom Invoker

```go
type LoggingInvoker struct{}

func (l *LoggingInvoker) Invoke(
    ctx context.Context,
    evt event.Event,
    handlerName string,
    next func(context.Context) error,
) error {
    slog.Info("invoking handler", "event", evt.Name(), "handler", handlerName)
    err := next(ctx)
    if err != nil {
        slog.Error("handler failed", "event", evt.Name(), "handler", handlerName, "error", err)
    }
    return err
}
```
