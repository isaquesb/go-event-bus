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
- `handlerName` - Identifier for the handler (used in signals and logging)
- `handleFn` - The next step in the chain (either another invoker or the final handler)

An invoker can:
- **Execute** `handleFn` to continue the chain
- **Skip** execution (e.g., idempotency check finds duplicate)
- **Retry** execution (e.g., retry invoker)
- **Short-circuit** with an error (e.g., circuit breaker is open)
- **Wrap** execution with observability (e.g., tracing)

## Contract

- Call `next(ctx)` **at most once per attempt**, and return its error, or a classified error of your own. Only an invoker that retries (Retry) calls it again, and then once per attempt.
- Do not swallow an error silently. Return the error so the invokers outside you and the transport can classify it. Errors are classified by their type (`PermanentError`, `RetryableError`, the flow-control sentinels `ErrCircuitOpen`, `ErrRateLimited`, `ErrInProgress`, `ErrDuplicate`); an error you do not classify is transient.
- Pass the `ctx` you received, or one derived from it, to `next`. It carries the Metadata, the delivery and the bus `Observer`.
- Do not recover panics to hide them. A panic in the handler is already recovered innermost and reaches you as an ordinary returned error (a `*event.PanicError`, which matches `event.ErrHandlerPanic` and is terminal).
- An invoker is shared by every handler and every goroutine: keep per-handler state keyed by `handlerName` and guard it.

## How a transport runs the chain

Every transport calls the chain through one entry point, `event.InvokeHandler`:

```go
err := event.InvokeHandler(ctx, invoker, evt, handlerName, func(c context.Context) error {
    return handler(c, evt)
})
```

`InvokeHandler` runs the handler through the invoker chain, recovers a panic of the handler or of an invoker (the stack travels in the `*event.PanicError`, so nothing escapes the delivery goroutine), and emits `HandlerDone` to the bus `Observer` for every invocation, with the outcome, the duration and the error. A nil invoker calls the handler directly. Latency and outcome therefore come from `HandlerDone`; there is no invoker that measures them. A custom transport must use `InvokeHandler` instead of calling the chain itself.

The order of the built-in invokers matters and is described in [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/). What each error becomes after the chain is in the [Failure Model]({{ site.baseurl }}/architecture/failure-model/).

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
