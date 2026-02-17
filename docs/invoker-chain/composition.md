---
layout: default
title: Composition
parent: Invoker Chain
nav_order: 1
---

# Chain Composition

## NewChain

The `Chain` struct composes multiple invokers into a single execution pipeline:

```go
chain := invoker.NewChain(
    telemetry.NewTracerInvoker(tracer),
    invoker.NewMetrics(metrics),
    invoker.NewRateLimiter(store, cfg, metrics),
    invoker.NewIdempotency(store, cfg, metrics),
    invoker.NewRetry(policy, metrics),
    invoker.NewCircuitBreaker(cfg, metrics),
    invoker.NewDLQ(publisher, metrics),
)
```

## Recursion Model

The chain executes via recursive function calls. Each invoker receives a `next` function that triggers the next invoker in the sequence:

```go
func (c *Chain) Invoke(
    ctx context.Context,
    evt event.Event,
    handlerName string,
    handle func(context.Context) error,
) error {
    var next func(i int, ctx context.Context) error

    next = func(i int, ctx context.Context) error {
        if i == len(c.invokers) {
            return handle(ctx)
        }
        return c.invokers[i].Invoke(ctx, evt, handlerName, func(ctx context.Context) error {
            return next(i+1, ctx)
        })
    }

    return next(0, ctx)
}
```

This means:
- Each invoker controls whether `next` is called
- Context can be enriched at each layer
- Errors propagate back through the chain
- An invoker can call `next` multiple times (e.g., retry)

## Recommended Ordering

1. **Tracing** - Creates spans, must wrap everything
2. **Metrics** - Records latency and outcomes
3. **Rate Limiting** - Cheap rejection, fail fast
4. **Idempotency** - Cheap rejection, prevent duplicates
5. **Retry** - Handle transient failures
6. **Circuit Breaker** - Protect downstream services
7. **DLQ** - Terminal handler for permanent failures

**Why this order?**
- Observability wraps everything for complete visibility
- Cheap rejections happen early to save resources
- Retries occur before circuit breaker health decisions
- DLQ is terminal and isolated as the last resort
