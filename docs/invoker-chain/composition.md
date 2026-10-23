---
layout: default
title: Composition
parent: Invoker Chain
nav_order: 1
---

# Chain Composition

This page is the single home of the recommended chain order and the reason for each position. Every other page links here instead of repeating the reasons.

## Recommended order

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

Read left to right, outermost first. Each invoker wraps the next one, so the leftmost sees every execution and the rightmost sits next to the handler.

| Position | Invoker | Why here |
|---|---|---|
| 1 | Tracer | Outermost, so the span covers everything: rejections, retries, the dead-letter publish. |
| 2 | RateLimit | Before Idempotency, so a rejected message takes no idempotency lock and no store write. |
| 3 | Idempotency | Outside DLQ, so it sees the `ErrDeadLettered` the DLQ invoker returns and records the execution as `dead_lettered`. That record is not a completion: a replay with the same key runs the handler again. |
| 4 | DLQ | Outside Retry, so it sees the final error after the retries ran. It dead-letters a terminal error on any delivery and every other failure only on the final delivery. Inside Retry it would dead-letter a recoverable error on its first failure. |
| 5 | Retry | Outside CircuitBreaker, so an open breaker stops the retries. If the breaker opens in the middle of a retry run, the attempts that really ran and their history are kept (the error still matches `ErrCircuitOpen`, and the DLQ record carries the real attempt count). |
| 6 | CircuitBreaker | Innermost, next to the handler, so it counts only real handler outcomes. |

Latency and outcome are not measured by an invoker. They come from the `HandlerDone` signal that `event.InvokeHandler` emits to the bus Observer for every invocation, so the chain has no metrics invoker. See [Observability]({{ site.baseurl }}/observability/).

## Flow-control errors and the transport backstop

`RateLimit` and `Idempotency` sit outside `DLQ`, so their rejections (`ErrRateLimited`, `ErrInProgress`) never reach the DLQ invoker. The bus does not lose them: on the final delivery the transport dead-letters them with its own publisher (`BusOptions.DLQPublisher` or `LocalBusOptions.DLQPublisher`). Configure the same publisher on the chain and on the bus so every failure has a dead-letter path. Before the final delivery the transport retries the message instead. The per-transport result of each error is in the [failure model]({{ site.baseurl }}/architecture/failure-model/).

An `ErrCircuitOpen` is different: the breaker sits inside `DLQ`, so on the final delivery the chain DLQ dead-letters it and the transport acknowledges the message.

## Retry and MaxDeliver

`Retry` does not look at the delivery: every delivery runs the whole retry policy. On JetStream the redeliveries multiply with it, so a message that keeps failing runs the handler up to `MaxAttempts × MaxDeliver` times before the final delivery dead-letters it. With `MaxAttempts: 5` and `WithMaxDeliver(5)` that is 25 runs.

Prefer a low `MaxAttempts`, or no `Retry` at all, for handlers behind a JetStream consumer, and let the consumer `BackOff` space the deliveries (`event.WithBackOff`). Keep `Retry` for the short in-process hiccup, and let redelivery handle the long outage. A transport without redelivery (LocalBus, NATS Core) has a single delivery, so `Retry` is the only retry there.

## NewChain

The chain below is compiled from [`examples/02_invoker_chain.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/02_invoker_chain.go):

```go
dlq := &mockDLQPublisher{}
chain := invoker.NewChain(
	// 1. Tracing - creates spans for each handler
	telemetry.NewTracerInvoker(tracer),

	// 2. Rate Limiting - prevents abuse
	invoker.NewRateLimiter(
		&mockRateLimitStore{},
		invoker.RateLimitConfig{
			Rate:      10,          // 10 requests
			Period:    time.Minute, // per minute
			Burst:     15,          // allow burst up to 15
			KeyPrefix: "payment:limit:",
			OnLimit: func(ctx context.Context, evt event.Event, key string) {
				slog.Warn("rate limit exceeded", "key", key)
			},
		},
	),

	// 3. Idempotency - prevents duplicate processing
	invoker.NewIdempotency(
		invoker.NewMemoryIdempotencyStore(), // Use Redis in production
		invoker.IdempotencyConfig{
			ProcessingTTL: 5 * time.Minute,
		},
	),

	// 4. DLQ - outside Retry: dead-letters the final outcome once
	invoker.NewDLQ(dlq),

	// 5. Retry - handles transient failures
	invoker.NewRetry(
		invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    5 * time.Second,
		},
	),

	// 6. Circuit Breaker - prevents cascade failures
	invoker.NewCircuitBreaker(
		invoker.CircuitBreakerConfig{
			FailureThreshold: 5,
			SuccessThreshold: 2,
			OpenTimeout:      30 * time.Second,
		},
	),
)

// Create the bus with the chain. The same publisher is the bus backstop
// for failures the chain DLQ did not dead-letter.
bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:       chain,
	DLQPublisher:  dlq,
	MaxConcurrent: 16,
})
```

## Recursion model

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
- An invoker can call `next` multiple times (for example `Retry`)

## Sources

The order and the reasons above are pinned by tests in the `invoker` module: `TestChainOrder_DLQOutsideRetry_TerminalPublishesOnce`, `TestChainOrder_DLQOutsideRetry_ExhaustedNotFinalRetriedByTransport`, `TestChainOrder_LegacyOrderDeadLettersPrematurely`, `TestChainOrder_FlowControlDeadLetteredOnlyOnFinal`, `TestChainOrder_CircuitOpensMidRetryKeepsHistory`, `TestChainOrder_InProgressBypassesChainDLQ` and `TestChainOrder_DeadLetteredThenReplayExecutes` in [`invoker/chain_order_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/invoker/chain_order_test.go). The doc comment of [`invoker/chain.go`](https://github.com/isaquesb/go-event-bus/blob/main/invoker/chain.go) states the same order.
