---
layout: default
title: DLQ Invoker
parent: Invoker Chain
nav_order: 6
---

# DLQ (Dead Letter Queue) Invoker

The DLQ invoker moves a message that cannot be handled to a dead-letter queue, keeping the original envelope bytes and describing the failure in headers. It dead-letters a message once, on the delivery that ends it, and returns an error that tells the transport to acknowledge.

## Configuration

```go
invoker.NewDLQ(publisher)
```

`NewDLQ` takes only the publisher. A nil publisher makes the invoker a pass-through, and the transport backstop reports the failure (see [Publisher on the invoker and on the bus](#publisher-on-the-invoker-and-on-the-bus)).

Place the DLQ invoker **outside** `Retry`, so the retries run first and only the final outcome is dead-lettered. The reason for each position is on the [composition page]({{ site.baseurl }}/invoker-chain/composition/).

## DLQPublisher interface

```go
type DLQPublisher interface {
    Publish(ctx context.Context, rec event.DLQRecord) error
}
```

`invoker.DLQPublisher` is an alias of `event.DLQPublisher`. A publisher receives the whole record, not just the event, so it can store the original bytes untouched.

Delivery to the queue is **at-least-once**: a crash between the publish and the acknowledgement, or a failed acknowledgement, can publish the same record again. Whatever consumes the DLQ must tolerate duplicates.

## When a message is dead-lettered

The DLQ invoker asks `event.Dispose` what the transport would do with the error on this delivery. It dead-letters only when the answer is **Terminal**:

| Error | Dead-lettered on |
|---|---|
| Terminal error (`PermanentError`, `ErrSendToDLQ`, a `Terminal() == true` error, a panic) | Any delivery, at once |
| Any other failure, including retries exhausted, `ErrCircuitOpen` and `ErrUnknownVersion` | The final delivery only |
| Context cancellation of the subscription | Never: the message is handed back |

The final delivery is the last one the broker will make (`Delivery.Final`). Before that, the error is returned unchanged and the transport retries with a delay. The full table per transport is in the [failure model]({{ site.baseurl }}/architecture/failure-model/); it is not repeated here.

What the invoker returns:

| Result | Returned error | Transport action |
|---|---|---|
| Publish succeeded | `*event.DeadLetteredError`, matches `event.ErrDeadLettered` and the original cause | Ack, without publishing a second record |
| Publish failed | `*event.DeadLetterFailedError`, matches `event.ErrDeadLetterFailed` | Retry with a delay, never Ack or Term |
| Not a Terminal delivery | the error from the inner chain, unchanged | per the failure model |

The invoker never returns `nil` for a failure. On a final delivery a failing publish is retried inside the call, 3 attempts with 100 ms then 200 ms between them, because no redelivery will give it another chance.

## The record

`event.DLQRecord` carries everything a publisher needs:

| Field | Meaning |
|---|---|
| `Raw` | Original envelope bytes. Empty on LocalBus, where the publisher encodes `Evt` |
| `Headers` | Transport headers of the original message |
| `Evt` | Decoded event, nil for a decode failure (then `Raw` is the complete wire form, possibly empty) |
| `EventID`, `EventName` | Event identity, empty when unknown |
| `Cause`, `Reason` | The failure and its stable reason (`event.ReasonOf`), for example `terminal`, `retries_exhausted`, `circuit_open`, `panic` |
| `Handler`, `Subject` | Failing handler (empty for a decode failure) and the subject received on |
| `Attempts` | In-process handler attempts of the last delivery. 0 means the handler never ran |
| `Delivery`, `Execution` | Delivery and handler context |
| `FailedAt` | When the record was built, UTC |
| `History` | Bounded in-process retry history, oldest first, empty without `Retry` |

`event.NewDLQRecord(ctx, evt, handler, cause)` builds a record from a handler context.

## JetStream record

`jetstream.NewDLQ` publishes the **verbatim envelope** with the original user headers, plus the failure context in `Eventbus-Dlq-*` headers. The envelope is never decoded, re-encoded or truncated. Nothing is decoded on replay either, so the bytes that come back are the bytes that failed.

| Header | Value |
|---|---|
| `Eventbus-Dlq-Cause` | Error text of the cause |
| `Eventbus-Dlq-Reason` | Stable reason |
| `Eventbus-Dlq-Attempts` | In-process attempts, always present |
| `Eventbus-Dlq-Delivery-Count` | Broker delivery count |
| `Eventbus-Dlq-Handler` | Failing handler |
| `Eventbus-Dlq-Original-Subject` | Subject the message was received on |
| `Eventbus-Dlq-Failed-At` | When the record was built (RFC 3339, UTC) |
| `Eventbus-Dlq-First-Attempt-At` | When the handler first started |
| `Eventbus-Dlq-Event-Id`, `Eventbus-Dlq-Event-Name` | Event identity |
| `Eventbus-Dlq-Original-Msg-Id` | The original `Nats-Msg-Id`, kept here because `Nats-*` headers are stripped |
| `Eventbus-Dlq-Original-Stream`, `Eventbus-Dlq-Original-Sequence` | Where the message came from |
| `Eventbus-Dlq-Published-At` | When the original message was published |
| `Eventbus-Dlq-Panic-Stack` | Stack of a recovered panic, only when the cause is a `*event.PanicError` |
| `Eventbus-Dlq-History` | Retry history as compact JSON (below) |
| `Eventbus-Dlq-History-Truncated` | `true` when attempts were dropped to fit |
| `Eventbus-Dlq-Traceparent` | W3C traceparent of the span active at the dead letter, so a replay can link back to it |

Headers with an unknown value are omitted. Every value is bounded at 1024 bytes (`event.DLQHeaderValueLimit`), control characters become spaces and invalid UTF-8 is replaced.

`Eventbus-Dlq-History` is a JSON array of `{"n":1,"err":"…","class":"transient","at":"…","dur_ms":120}` entries, capped at 2048 bytes (`event.HistoryHeaderLimit`). When it does not fit, the oldest attempts are dropped and `Eventbus-Dlq-History-Truncated` is set. The size of the history is `RetryPolicy.HistorySize`, see [Retry]({{ site.baseurl }}/invoker-chain/retry/).

The publisher strips every `Nats-*` header of the original message and any stale `Eventbus-Dlq-*` header, so a record dead-lettered twice does not carry the first failure. If the broker rejects a record for exceeding its maximum payload, `Publish` retries once with a minimal header set (reason, original subject, event id, handler, a shortened cause and the traceparent).

## Publishers

| Publisher | Use |
|---|---|
| `event.NewMemoryDLQ()` | LocalBus, tests and examples. `Records()` and `Len()` read what was stored |
| `jetstream.NewDLQ(js, prefix, opts...)` | JetStream. Publishes to `prefix` plus the original subject |

The default prefix is `dlq.` (`jetstream.DefaultDLQPrefix`, used when `prefix` is empty). A message received on `invoice.issued` is published to `dlq.invoice.issued`, so the DLQ stream must bind `dlq.>`. A DLQ stream that still uses a custom prefix keeps working: pass the prefix explicitly.

A record from LocalBus or NATS Core has no envelope bytes. Give `jetstream.NewDLQ` a registry with `jetstream.WithDLQRegistry(reg)` so it can encode the event; without one `Publish` returns `jetstream.ErrNoEnvelope` and publishes nothing. A decode failure never needs the registry: its bytes are published as they are.

```go
dlq := jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)
```

The publish carries no message id, so the DLQ stream deduplicates nothing: duplicates are possible, as said above.

### Rules for the DLQ stream

- Never set `MaxMsgsPerSubject` on the DLQ stream. Many records share one subject (`dlq.` plus the original subject), and that limit would silently delete the older ones to make room for the newer ones.
- Never compact or truncate the stream. It holds the only copy of the failed messages. Choose retention with `MaxAge` or `MaxBytes`, on purpose.

## Publisher on the invoker and on the bus

Configure the **same publisher** on the DLQ invoker and on the bus:

```go
chain := invoker.NewChain(invoker.NewDLQ(dlq), invoker.NewRetry(policy))

bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
    Invoker:      chain,
    DLQPublisher: dlq, // LocalBusOptions.DLQPublisher on LocalBus
})
```

The invoker only sees what runs inside the chain. Some failures never reach it: an undecodable message, and `ErrRateLimited` or `ErrInProgress` raised by the invokers that sit outside the DLQ. On the final delivery the transport dead-letters those itself, through the publisher set on the bus (the transport backstop). It does not publish a second record for a message the chain already dead-lettered. Without a bus publisher those messages are terminated and reported as dropped, and JetStream logs a warning at start-up.

A complete setup, copied from [`examples/09_dlq_replay.go`](https://github.com/isaquesb/go-event-bus/blob/main/examples/09_dlq_replay.go):

```go
// One publisher, used twice below.
dlq := jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)

// DLQ outside Retry. On JetStream keep MaxAttempts low (or drop Retry and
// leave the delay to the consumer BackOff): the in-process attempts multiply
// with the broker redeliveries, MaxAttempts x MaxDeliver.
chain := invoker.NewChain(
	invoker.NewDLQ(dlq),
	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 2,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    time.Second,
	}),
)

// The same publisher is the bus backstop (DLQPublisher).
bus := jetstream.NewBus(js, registry, jetstream.BusOptions{
	Invoker:      chain,
	DLQPublisher: dlq,
	Observer:     event.NewSlogObserver(nil),
})
defer func() { _ = bus.Close() }()
```

For a LocalBus the same pair uses `event.NewMemoryDLQ()`:

```go
// MemoryDLQ is a concurrency-safe in-memory DLQPublisher for tests and
// examples. It is the same type behind the invoker DLQ and the bus.
dlq := event.NewMemoryDLQ()

chain := invoker.NewChain(
	invoker.NewDLQ(dlq),
	invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
	}),
)

bus := event.NewLocalBus(event.LocalBusOptions{
	Invoker:      chain,
	DLQPublisher: dlq,
	Observer:     event.NewSlogObserver(nil),
})
defer func() { _ = bus.Close() }()
```

## Replay

`jetstream.NewReplayer(js).Replay(ctx, opts)` republishes the records of a DLQ stream, byte for byte, to their original subject (or `ToSubject`). It keeps the user headers and drops the `Eventbus-Dlq-*` ones. See the [JetStream bus]({{ site.baseurl }}/transports/jetstream-bus/) page for the transport.

```go
// Later, once the backend is fixed: replay the DLQ. Each record goes back,
// byte for byte, to its original subject. FilterSubject narrows a DLQ
// stream shared with other subjects; ReplayResult counts the outcome.
res, err := jetstream.NewReplayer(js).Replay(ctx, jetstream.ReplayOptions{
	FromStream:    "INVOICES_DLQ",
	FilterSubject: "dlq.invoice.>",
	Limit:         100,
})
if err != nil {
	slog.Error("replay failed", "error", err, "replayed", res.Replayed, "failed", res.Failed)
	return
}
slog.Info("replay completed", "replayed", res.Replayed, "failed", res.Failed)
```

Two things to remember when replaying:

- A `dead_lettered` idempotency record is not a completion, so a replay with the same key runs the handler again. See [Idempotency]({{ site.baseurl }}/invoker-chain/idempotency/).
- A replayed message is handled from scratch, possibly after newer state exists. Handlers must tolerate duplicates and stale state.

## Signals

Each dead letter emits `DeadLettered` to the bus Observer; a publish that failed emits `DLQPublishFailed` once the bounded retry is over. The terminal failure is reported once through `Observer.Failure` with outcome `dead_lettered`. See [Observability]({{ site.baseurl }}/observability/) for the signals and metrics.
