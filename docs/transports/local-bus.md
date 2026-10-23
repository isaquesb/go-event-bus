---
layout: default
title: LocalBus
parent: Transports
nav_order: 1
---

# LocalBus

The LocalBus is an in-process event dispatcher for domain events and internal decoupling. It has no broker: a delivery never repeats, so every delivery is the **final** one and every failure ends in a dead letter or a drop. See the [Failure Model]({{ site.baseurl }}/architecture/failure-model/) for the complete table.

## Creating a LocalBus

```go
bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:       invoker.NewChain(),
    MaxConcurrent: 10,
    // Observer receives every terminal failure once (Observer.Failure). The slog
    // observer is also the default when Observer is nil.
    Observer: event.NewSlogObserver(nil),
})
```

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `Invoker` | `event.Invoker` | pass-through | The invoker chain for handler execution. Nil calls the handler directly |
| `Observer` | `event.Observer` | `event.NewSlogObserver(nil)` | Receives every signal of the bus, including `Observer.Failure` once per terminal outcome. `event.NopObserver{}` silences everything. See [Observability]({{ site.baseurl }}/observability/) |
| `DLQPublisher` | `event.DLQPublisher` | nil | Per-bus dead-letter backstop. A terminal failure that the chain did not already dead-letter is published here; with nil it is reported as dropped. Pass the same publisher you gave `invoker.NewDLQ` |
| `MaxConcurrent` | `int` | 16 (`DefaultMaxConcurrent`) | Async handlers that run at once. Values at or below 0 mean the default |
| `QueueSize` | `int` | 1024 (`DefaultQueueSize`) | Tasks parked while they wait for an execution slot. Ignored by `OverflowUnbounded` |
| `Overflow` | `event.OverflowPolicy` | `OverflowBlock` | What an external emit does when every slot is busy and the queue is full |
| `HandlerTimeout` | `time.Duration` | 0 (none) | Bounds each async handler execution |
| `ShutdownTimeout` | `time.Duration` | 30s (`DefaultShutdownTimeout`) | The bound `Close` applies to `Shutdown` |
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

There is no bus-level error callback. A failure is observed through `Observer.Failure`, and the event's own `OnErr` method still runs after it (see [Event Interface]({{ site.baseurl }}/core-concepts/event-interface/)).

## Dispatch Modes

### Asynchronous (Emit)

```go
bus.Emit(ctx, OrderPlaced{OrderID: "order-456"})
```

`Emit` returns without waiting for the handlers and has no error return. Handlers run in goroutines under a context that keeps the values of the emitter context (Metadata, trace span, baggage) but not its cancellation or deadline, so a handler outlives the request that emitted it. A drop or a rejection (`ErrQueueFull`, `ErrBusClosed`) is reported through `Observer.Failure` and the event's `OnErr` method, never lost.

### Synchronous (EmitSync)

```go
if _, err := bus.EmitSync(ctx, UserCreated{UserID: "user-123"}); err != nil {
    slog.Error("emit sync failed", "error", err)
}
```

`EmitSync` runs every handler subscribed to the subject, in registration order, on the calling goroutine and under the caller's context. Every handler runs even when an earlier one failed.

It returns `errors.Join` of the terminal failures, each wrapped in a `*event.HandlerError` that names the handler:

- find the failing handler with `errors.As(err, &he)` where `he` is a `*event.HandlerError`;
- match the cause with `errors.Is`: a recovered panic matches `event.ErrHandlerPanic`;
- a message that was dead-lettered (`ErrDeadLettered`) or was a duplicate (`ErrDuplicate`) is a handled outcome and **is not an error**;
- the result is `nil` when every handler succeeded or every failure was handled;
- when the caller context is done, `EmitSync` stops dispatching and includes `ctx.Err()` in the joined error; after shutdown it returns `ErrBusClosed`.

`HandlerTimeout` does not apply to `EmitSync`: its handlers keep the caller's context.

## Metadata

`Emit` and `EmitSync` resolve the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) (message id, correlation id, causation id, headers) and put it in the context of every handler. Handlers read it with `event.MetadataFromContext`:

```go
func handle(ctx context.Context, evt event.Event) error {
    md, _ := event.MetadataFromContext(ctx)
    log.Printf("message=%s correlation=%s", md.ID, md.CorrelationID)
    return nil
}
```

Both methods accept `event.WithID`, `event.WithCorrelationID` and `event.WithHeader`. When a handler emits a new event with its own `ctx`, the new event is a child of the handled one: its `causation_id` is the handled event's id and it inherits the correlation id.

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

LocalBus has no envelope, so there is no `traceparent` header: trace context flows through `ctx`.

## Concurrency Model

`MaxConcurrent` handler goroutines run at once, and `QueueSize` tasks wait for a slot. For each handler of an emit:

1. If an execution slot is free, the handler runs in a new goroutine.
2. If the emit comes from inside a handler of the same bus (it carries the handler context), the handler runs inline on that goroutine. A nested emit never waits, so a handler that emits to its own bus cannot deadlock the slots.
3. Otherwise the `Overflow` policy decides.

Throughput is about `MaxConcurrent` divided by the handler latency. The queue absorbs bursts only; under sustained overload the policy decides who pays:

| Policy | When slots and queue are full | Who pays |
|---|---|---|
| `OverflowBlock` (default) | Parks up to `QueueSize` tasks, then blocks `Emit` until there is queue room. When the emitter context ends first the handler is dropped with `ErrQueueFull` | The emitter, with latency |
| `OverflowUnbounded` | Never blocks and never drops: every task is parked, whatever the count | Memory. Parked tasks are lost if the process dies |
| `OverflowDrop` | Reports `ErrQueueFull` at once and drops the handler | The event |

`ErrQueueFull` is reported, never returned: `Emit` has no error return and `EmitSync` does not queue.

**There is no FIFO guarantee**, neither between events nor between the handlers of one event. Two events emitted in order may run in either order, and handlers must not depend on it. `EmitSync` runs the handlers of one event in registration order, but still gives no order between concurrent emits.

## Timeouts, panics and failures

- `HandlerTimeout` puts a deadline on each async handler context, above the invoker chain, so Retry and the circuit breaker see it. Its expiry is an ordinary transient error that matches `event.ErrHandlerTimeout` and `context.DeadlineExceeded`. Because LocalBus deliveries are final it ends dead-lettered (or dropped without a `DLQPublisher`), never as an interruption. A handler must honor its context for the timeout to take effect: the bus cannot stop a goroutine.
- A handler panic is recovered into `event.ErrHandlerPanic` with the stack. It is a terminal failure: dead-lettered, never retried, and it never crashes the process.
- Every delivery is final, so every failure that is not acknowledged is **dead-lettered** through `DLQPublisher` (outcome `dead_lettered`) or **dropped** when none is configured (outcome `dropped`), and reported once through `Observer.Failure`. An invoker chain that contains a DLQ invoker dead-letters first; the bus backstop never publishes the same message twice.

Configure the same publisher on the chain and on the bus:

```go
dlq := event.NewMemoryDLQ()

bus := event.NewLocalBus(event.LocalBusOptions{
    Invoker:      invoker.NewChain(invoker.NewDLQ(dlq)),
    DLQPublisher: dlq,
})
```

See [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/) for the order of the invokers and [Failure Model]({{ site.baseurl }}/architecture/failure-model/) for what each error becomes.

## RegisterSubscribers

Bulk registration for organized handler grouping:

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber(),
    NewBillingSubscriber(),
)
```

Each subscriber's `Name()` is used as the handler name. Subscribers implementing `SubscribeOptionsProvider` will have their options merged automatically.

## Shutdown and Close

LocalBus implements `event.Shutdowner`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := bus.Shutdown(ctx); err != nil {
    // ctx expired first: the handlers still running were interrupted
    slog.Error("shutdown did not drain", "error", err)
}
```

`Shutdown(ctx)`:

1. stops accepting work: `Emit` drops and reports the event with `ErrBusClosed`, `EmitSync` and `Subscribe` return `ErrBusClosed`;
2. waits for the in-flight handlers and the tasks parked for a slot. A handler may keep emitting to the bus while it drains (a nested emit made with the handler context), and those children are drained too, so the wait ends when the causal chain ends or `ctx` expires;
3. when `ctx` expires first, cancels every handler context with cause `event.ErrShutdownInterrupted`, hands the parked tasks back as interrupted, refuses all new emits and returns `ctx.Err()` without waiting further. An interrupted task is reported once and is **never dead-lettered**: it did not fail, it did not run to the end.

`Shutdown` is safe to call more than once and concurrently. `Close()` is `Shutdown` bounded by `ShutdownTimeout` (30 seconds by default) and returns its error. When `Observer` is nil, `Shutdown` also flushes the default slog observer the bus created, so a pending dedup summary is logged. An observer you supply is never flushed by the bus: it owns its `Flush`.
