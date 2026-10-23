---
layout: default
title: Execution Pipeline
parent: Architecture
nav_order: 1
---

# Execution Pipeline

## Pipeline Stages

A transport never calls a handler directly. Every delivery goes through the same three stages, shared by LocalBus, NATS Core and JetStream:

```
Decode → InvokeHandler (chain, HandlerDone) → Settle (Dispose → broker action)
```

1. **Decode** turns the received bytes into an event. A decode failure has no event and no handler run: it goes straight to Settle.
2. **InvokeHandler** (`event.InvokeHandler`) runs the invoker chain around the handler and never lets a panic escape the calling goroutine. A panic in the handler is recovered innermost, so the chain sees it as an ordinary `*event.PanicError`. It emits `HandlerDone` to the Observer with the outcome, the duration and the error.
3. **Settle** (`event.Settle`) is called exactly once per handled message. It maps the result through `event.Dispose` to a disposition (Ack, Retry, RetryDelayed or Terminal), dead-letters a terminal failure through the bus `DLQPublisher` when the chain did not already do so, and reports the terminal outcome once through `Observer.Failure`. The transport then performs the broker action for the disposition. The per-error results are in the [failure model]({{ site.baseurl }}/architecture/failure-model/).

## Invoker Chain Depth

Handlers are never executed directly. Inside `InvokeHandler`, execution flows through a chain of invokers:

```
Invoker → Invoker → ... → Handler
```

Each invoker has a single responsibility and is stateless or externally backed. The chain uses recursive function composition: each invoker receives a `next` function that calls the next invoker, until the final handler is reached.

## Recommended Order

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

The reason for each position is on the [Composition]({{ site.baseurl }}/invoker-chain/composition/) page.

## Context Flow

Context flows through the chain and can be enriched at each layer:
- The transport installs the Observer, the `Delivery` (count, maximum, subject) and the `Execution` on the handler context
- Tracing adds span context
- The handler receives the final enriched context

Handler contexts derive from the context the transport was given (the `Subscribe` context on NATS Core and JetStream, the emit context on LocalBus), never from `context.Background()`, so cancellation and span parenting reach the handler.

## Error Flow

Errors propagate back through the chain:
- Each invoker can inspect, classify, or wrap errors
- `Retry` re-executes on transient errors and gives up with an error that matches `ErrRetriesExhausted`
- `DLQ` publishes the final outcome and returns an error that matches `ErrDeadLettered`, which the transport acknowledges
- What leaves the chain is settled by the transport: a terminal outcome is reported once through `Observer.Failure`, and then through the event's own `OnErr` method when the event implements it
