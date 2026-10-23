---
layout: default
title: JetStream Bus
parent: Transports
nav_order: 3
---

# JetStream Bus

The JetStream Bus provides durable, distributed event processing with at-least-once delivery. The broker redelivers a message that fails until it succeeds, is dead-lettered or reaches the consumer `MaxDeliver`, and the bus maps every outcome to exactly one broker action.

## Setup

```go
import (
    "github.com/nats-io/nats.go"
    natsjs "github.com/nats-io/nats.go/jetstream"
    "github.com/isaquesb/go-event-bus/jetstream"
    eventjson "github.com/isaquesb/go-event-bus/json"
)

nc, _ := nats.Connect(nats.DefaultURL)
js, _ := natsjs.New(nc)

registry := eventjson.NewRegistry()
// ... register events

dlq := jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)

bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
    Invoker:          chain,
    DLQPublisher:     dlq,
    CircuitOpenDelay: 30 * time.Second,
    RateLimitDelay:   5 * time.Second,
})
```

### BusOptions

| Field | Type | Default | Description |
|---|---|---|---|
| `Invoker` | `event.Invoker` | empty chain | Invoker chain. Nil calls the handler directly |
| `Observer` | `event.Observer` | `event.NewSlogObserver(nil)` | Receives every signal of the bus: `Failure` exactly once per terminal outcome across redeliveries (never on an intermediate `NakWithDelay`), `HandlerDone`, `DecodeFailed`, `DLQPublishFailed` and `InternalError`. `event.NopObserver{}` silences the bus. See [Observability]({{ site.baseurl }}/observability/) |
| `DLQPublisher` | `event.DLQPublisher` | nil (warning) | Per-bus dead-letter backstop. It receives the failures the chain did not dead-letter and every undecodable message. Usually the same `jetstream.NewDLQ` value given to `invoker.NewDLQ`. With nil such messages are terminated with `TermWithReason` and reported as dropped |
| `CircuitOpenDelay` | `time.Duration` | 30s (warning) | `NakWithDelay` delay when the circuit is open. Match it to the breaker `OpenTimeout` |
| `RateLimitDelay` | `time.Duration` | 5s (warning) | `NakWithDelay` delay when rate limited |
| `PublishRetry` | `jetstream.PublishRetryPolicy` | 3 attempts, 100 ms to 1 s jittered backoff, 2 s per attempt | Bounds the internal retry of transient publish failures in `Emit` (see [Publishing](#publishing)) |
| `ShutdownTimeout` | `time.Duration` | 30s (`event.DefaultShutdownTimeout`) | The bound `Close` applies to `Shutdown` |
| `TracerProvider` | `trace.TracerProvider` | the global provider | Creates the producer spans of `Emit` |
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

There is no bus-level error callback: failures reach `Observer.Failure`, and the event's own `OnErr` method still runs after it.

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

## Publishing

```go
// Subject defaults to evt.Name()
err := bus.Emit(ctx, chatMessage)

// Override subject (e.g. for hierarchical NATS subjects)
err = bus.Emit(ctx, chatMessage, event.WithSubject("chat.message.room-general"))
```

The subject defaults to `evt.Name()`. Use `WithSubject` to override it for hierarchical routing.

`Emit` also accepts the [metadata options]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) `event.WithID`, `event.WithCorrelationID` and repeatable `event.WithHeader`. Every envelope carries a message id, the W3C `traceparent` header and, when `ctx` is a handler context, the causal lineage of the handled event.

### Deduplication with Nats-Msg-Id

`Emit` sets the **`Nats-Msg-Id`** header to the resolved message id. The id comes from `event.WithID`, then the event's `Id()` method, then a generated id, in that order. The stream deduplicates messages that carry the same id **inside its duplicate window** (`StreamConfig.Duplicates`, 2 minutes by default). A publish the stream acknowledges as a duplicate is not an error.

Generated ids differ on every call, so emitting the same business event twice is deduplicated only when both calls carry the same id. For an application-level re-emit give the event a stable id:

```go
err := bus.Emit(ctx, invoice, event.WithID("invoice-"+invoice.ID))
// or implement Id() on the event: func (e *Invoice) Id() string { return e.ID }
```

### PublishRetry

`Emit` retries a transient publish failure (a per-attempt timeout or lost acknowledgement while the caller context is alive, no responders, no stream response) with the **same** `Nats-Msg-Id`, so the stream deduplicates a retry whose first attempt was stored but whose acknowledgement was lost. A server API error, a closed connection and anything else are permanent and are not retried. `PublishRetryPolicy` has `MaxAttempts` (the first attempt included; `1` disables the retry), `BaseDelay`, `MaxDelay` and `AttemptTimeout`. No attempt starts once `ctx` is done.

## Subscribing

```go
bus.Subscribe(ctx, "chat.message.*", handler,
    event.WithStream("CHAT_EVENTS"),
    event.WithHandlerName("persist-message"),
    event.WithConsumer("chat-processor"),
    event.WithMaxDeliver(5),
    event.WithBackOff([]time.Duration{1*time.Second, 5*time.Second, 30*time.Second}),
)
```

- **`WithStream` is required.** Without it `Subscribe` returns `jetstream.ErrStreamRequired`.
- The consumer is durable, named by `WithConsumer` (default: the subject), with explicit acks and the subject as its filter. The handler name defaults to the consumer name.
- `MaxDeliver` 0 means unlimited redelivery and logs a warning; a `BackOff` that is not set uses the default schedule 1s, 5s, 30s, 1m and logs a warning.
- When `MaxDeliver` is set it must be **greater than `len(BackOff)`**, because NATS needs it to apply the schedule. Otherwise `Subscribe` returns `jetstream.ErrInvalidConfig`. The example above passes: `MaxDeliver` 5 and three `BackOff` entries.
- Every handler context is derived from the context passed to `Subscribe`, never from `context.Background()`.

See [Subscribe Options]({{ site.baseurl }}/core-concepts/subscribe-options/) for every option.

## Broker actions

`event.Settle` runs once per delivery and maps the outcome through `event.Dispose` to **one** broker action:

| Disposition | Broker action | When |
|---|---|---|
| Ack | `Ack` | Success, a duplicate, or a message the chain already dead-lettered |
| Retry | `Nak` | Only the subscription context ended (an interrupted delivery, a shutdown): redelivered at once to another consumer, never dead-lettered |
| RetryDelayed | `NakWithDelay` | A transient failure before the final delivery, flow control, or a failed dead-letter publish |
| Terminal | `TermWithReason("<outcome>: <reason>")` | **Only after** the dead-letter publish succeeded, or when no `DLQPublisher` is configured (outcome `dropped`) |

The delay of `NakWithDelay` is:

| Error | Delay |
|---|---|
| `ErrCircuitOpen` | `CircuitOpenDelay` |
| `ErrRateLimited` | `RateLimitDelay` |
| anything else | the `BackOff` entry `BackOff[min(Count-1, len-1)]` |

A failed dead-letter publish (`ErrDeadLetterFailed`) follows the `BackOff` schedule even when its cause is flow control, and it is `NakWithDelay` **even on the final delivery**, never `TermWithReason`: the record was not published, so the message must not be ended silently. `Observer.Failure` still fires once, with outcome `dlq_publish_failed`.

A delivery is **final** when `NumDelivered >= MaxDeliver`. `MaxDeliver <= 0` is unlimited and is never final. On the final delivery an error that would be retried takes the terminal path instead: dead-lettered, then `TermWithReason`.

The complete error by transport table, including the reported outcome of each row, is in the [Failure Model]({{ site.baseurl }}/architecture/failure-model/#disposition-table). Do not rely on this page for a row: that page is the single source.

### Retry and MaxDeliver

An in-process `Retry` runs on every delivery, so on JetStream the attempts multiply with the redeliveries (`MaxAttempts` x `MaxDeliver`). Prefer a low `MaxAttempts`, or none, and let `BackOff` space the deliveries. See [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/#retry-and-maxdeliver).

### Decode failures

A message that cannot be decoded has no event and no handler run. It is settled with the context `Registry.Decode` returned, so once the envelope parsed it keeps the Metadata and trace of the message: `FailureContext` is populated and the dead-letter record keeps its event id, event name and traceparent headers. A permanent decode failure (`decode_malformed`, `decode_unknown_event`, `decode_missing_upcaster`, `decode_failed`) is dead-lettered on any delivery and then ended with `TermWithReason`; `ErrUnknownVersion` is retried with `NakWithDelay` until the final delivery. It is never a silent `Term`.

### Panics

A panic in the handler is recovered by the chain and is a terminal error. A panic outside the handler (decode, settle, the DLQ publisher) is recovered and reported as an `InternalError` with `Op` `panic`; the message is handed back with `NakWithDelay`, or ended with `TermWithReason` when it was already dead-lettered or reported. On the final delivery it is reported as `Failure` `dropped` with reason `panic` and ended.

## Metadata

The consumer decodes the envelope and puts the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) in the handler context, together with the producer's trace context. Handlers read it with `event.MetadataFromContext(ctx)`.

Failures are observed through `Observer.Failure`, which receives the **message context** with the event Metadata and trace. Use it to log the message id and correlation id of a failed delivery:

```go
type failureLog struct{ event.NopObserver }

func (failureLog) Failure(ctx context.Context, f event.FailureContext) {
    md, _ := event.MetadataFromContext(ctx)
    slog.Error("message ended", "handler", f.Handler, "outcome", f.Outcome,
        "message", md.ID, "correlation", md.CorrelationID)
}
```

## Stream Configuration

Streams must be created before subscribing. Example:

```go
js.CreateOrUpdateStream(ctx, natsjs.StreamConfig{
    Name:        "CHAT_EVENTS",
    Subjects:    []string{"chat.>"},
    Retention:   natsjs.LimitsPolicy,
    MaxAge:      7 * 24 * time.Hour,
    MaxBytes:    1 << 30,
    Replicas:    3,
    Storage:     natsjs.FileStorage,
})
```

## Dead-letter queue

`jetstream.NewDLQ(js, prefix, opts...)` returns an `event.DLQPublisher` that publishes a dead-lettered message to the stream subject `prefix + original subject`. An empty prefix is `jetstream.DefaultDLQPrefix` (`dlq.`), so `invoice.issued` is stored as `dlq.invoice.issued`. The DLQ stream must exist and cover that prefix.

```go
dlq := jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)

chain := invoker.NewChain(invoker.NewDLQ(dlq) /* , ... */)

bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
    Invoker:      chain,
    DLQPublisher: dlq, // the same publisher: the bus is the backstop
})
```

Configure the **same** publisher on the DLQ invoker and on `BusOptions.DLQPublisher`. The invoker dead-letters what the chain catches; the bus catches what the chain cannot see (an undecodable message, a flow-control failure on the final delivery) and never publishes a message twice.

The record is the original envelope bytes, untouched, plus the original user headers and bounded `Eventbus-Dlq-*` headers that describe the failure. Delivery to the DLQ stream is **at-least-once**: the publish carries no message id, so a record can be duplicated after a crash or a failed acknowledgement, and whatever consumes the DLQ stream must tolerate duplicates. `WithDLQRegistry` gives the DLQ a registry to encode records that come from LocalBus or NATS Core, which carry no envelope bytes. The record layout and headers are described in [DLQ Invoker]({{ site.baseurl }}/invoker-chain/dlq-invoker/).

Never compact or truncate the DLQ stream: it is the only copy of the failed messages.

## Replay

The `Replayer` republishes dead-lettered messages:

```go
replayer := jetstream.NewReplayer(js)

res, err := replayer.Replay(ctx, jetstream.ReplayOptions{
    FromStream:    "INVOICES_DLQ",
    FilterSubject: "dlq.invoice.>",
    Limit:         100,
})
if err != nil {
    slog.Error("replay failed", "error", err, "replayed", res.Replayed, "failed", res.Failed)
}
```

`ReplayOptions`:

| Field | Description |
|---|---|
| `FromStream` | The DLQ stream to read. Required |
| `ToSubject` | When set, overrides the per-record target. Without it each record goes back to its `Eventbus-Dlq-Original-Subject` |
| `Limit` | The most records to replay. At or below 0 means until the stream is drained |
| `Consumer` | The durable consumer name. Default `replay_` plus `FromStream` |
| `FilterSubject` | Restricts the consumer to matching subjects, for a DLQ stream shared with other subjects |

`Replay(ctx, ReplayOptions)` returns a `ReplayResult{Replayed, Failed}` and an error:

- the cursor is a **durable consumer** with explicit acks, so the next `Replay` continues where the last stopped. Records stay in the DLQ stream for audit;
- each record is republished **byte for byte** to its original subject (or `ToSubject`). The envelope is never decoded, the original user headers are kept, and every `Eventbus-Dlq-*`, `Eventbus-Replay-*` and `Nats-*` header is dropped;
- the replayed message carries `Nats-Msg-Id` `EVENT_ID:replay:DLQ_STREAM_SEQ`, so the original id cannot be swallowed by the duplicate window while repeated replays of one record dedup against each other. A publish the stream reports as a duplicate counts as replayed;
- it stops at the first record that fails, Naks that record and the rest of its batch, and returns the error. A record without a valid target subject fails with `jetstream.ErrReplaySubject`;
- it does not block on an empty stream and returns a nil error when the stream is drained or `Limit` is reached. Pass `Limit` while the consumer is still failing, so a run does not chase its own new dead letters.

A replayed message is handled again from scratch, possibly after newer state exists, so handlers must tolerate duplicates and stale state.

The real-server proof of the replay is `TestIntegration_ReplayRoundTrip` in [`jetstream/replay_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/replay_integration_test.go), run by the integration job against a NATS server. The example [`examples/09_dlq_replay.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/09_dlq_replay.go) is compile-only.

## RegisterSubscribers

Bulk registration using the `Subscriber` interface:

```go
bus.RegisterSubscribers(ctx,
    NewChatSubscriber(),
    NewAuditSubscriber(),
)
```

For JetStream, subscribers must provide stream options via the `SubscribeOptionsProvider` interface:

```go
type ChatSubscriber struct{}

func (s *ChatSubscriber) Name() string { return "chat-subscriber" }

func (s *ChatSubscriber) Events() map[string]event.HandleFn {
    return map[string]event.HandleFn{
        "chat.message.*": s.handleMessage,
    }
}

// SubscribeOptions provides JetStream-specific options for all handlers in this subscriber.
func (s *ChatSubscriber) SubscribeOptions() []event.SubscribeOption {
    return []event.SubscribeOption{
        event.WithStream("CHAT_EVENTS"),
        event.WithConsumer("chat-processor"),
    }
}
```

## Shutdown and Close

The JetStream Bus implements `event.Shutdowner`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := bus.Shutdown(ctx); err != nil {
    slog.Error("shutdown did not drain", "error", err)
}
```

`Shutdown(ctx)`:

1. returns `event.ErrBusClosed` from `Emit` and `Subscribe`, except an `Emit` made with the context of a handler whose message is still in flight;
2. **drains** every consumer: messages already buffered are handed back to the server with `Nak` instead of running, and `Shutdown` waits for each consumer to report closed and for the in-flight handlers to return;
3. when `ctx` expires first, stops the consumers, interrupts the running handlers with cause `event.ErrShutdownInterrupted` (their messages are `Nak`ed, **never dead-lettered**) and returns `ctx.Err()`.

Concurrent and repeated calls share one drain. `Close()` is `Shutdown` bounded by `ShutdownTimeout` (30 seconds by default) and returns `context.DeadlineExceeded` on timeout. A message handed back during the drain consumes one delivery. When `Observer` is nil the bus flushes the default slog observer it created; an observer you supply is never flushed by the bus. A failing `Consume` loop (heartbeat loss, a deleted consumer) is reported as an `InternalError` with `Op` `consume`.
