---
layout: default
title: NATS Bus
parent: Transports
nav_order: 2
---

# NATS Bus

The NATS Bus provides fire-and-forget distributed messaging via NATS Core. NATS Core never redelivers: every delivery is the **final** one, so a failure that is not acknowledged ends in a dead letter or a drop (see [Failure handling](#failure-handling)).

## Setup

```go
import (
    "github.com/nats-io/nats.go"
    eventjson "github.com/isaquesb/go-event-bus/json"
    eventnats "github.com/isaquesb/go-event-bus/nats"
)

nc, _ := nats.Connect(nats.DefaultURL)
registry := eventjson.NewRegistry()
// ... register events

bus := eventnats.NewNatsBus(nc, registry, eventnats.BusOptions{
    RequestTimeout: 5 * time.Second,
    Invoker:        chain,
    DLQPublisher:   dlq,
})
```

### Options

| Field | Type | Default | Description |
|---|---|---|---|
| `RequestTimeout` | `time.Duration` | 5s | Default timeout for `EmitSync` when the context has no deadline |
| `Invoker` | `event.Invoker` | nil | Optional invoker chain |
| `Observer` | `event.Observer` | `event.NewSlogObserver(nil)` | Receives every signal of the bus: `Failure` once per terminal outcome, `HandlerDone`, `DecodeFailed`, `DLQPublishFailed` and `InternalError`. `event.NopObserver{}` silences the bus. See [Observability]({{ site.baseurl }}/observability/) |
| `DLQPublisher` | `event.DLQPublisher` | nil | Per-bus dead-letter backstop. It receives the failures the invoker chain did not dead-letter and **every decode failure**. With nil such failures are only reported and logged |
| `ReplyErrorMessage` | `bool` | false | Makes the `Eventbus-Error` reply of a failed `SubscribeSync` request carry the truncated error text after the reason code |
| `ShutdownTimeout` | `time.Duration` | 30s (`event.DefaultShutdownTimeout`) | The bound `Close` applies to `Shutdown` |
| `TracerProvider` | `trace.TracerProvider` | the global provider | Creates the producer spans of `Emit` and `EmitSync` |
| `IDGenerator` | `func() string` | `event.NewID` (UUIDv7) | Mints the message id of events that carry neither `event.WithID` nor an `Id()` method |
| `InheritHeaders` | `[]string` | empty | Header keys copied from the handled event to events emitted with its context |

There is no bus-level error callback: failures reach `Observer.Failure`, and the event's own `OnErr` method still runs after it.

Header inheritance is opt-in: only the keys listed in `InheritHeaders` are copied from the handled event to events emitted with its context, and an explicit `event.WithHeader` overrides an inherited value. Trace context and correlation propagate regardless of this list. Headers are producer data visible to every consumer, so never carry secrets in them or allowlist secret-bearing keys.

## Publishing

### Fire-and-Forget (Emit)

```go
// Subject defaults to evt.Name()
err := bus.Emit(ctx, myEvent)

// Override subject
err = bus.Emit(ctx, myEvent, event.WithSubject("custom.subject"))
```

Encodes the event via the registry and publishes to `evt.Name()` by default. Use `WithSubject` to override.

`Emit` also accepts the [metadata options]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) `event.WithID`, `event.WithCorrelationID` and repeatable `event.WithHeader`:

```go
err := bus.Emit(ctx, myEvent,
    event.WithCorrelationID(requestID),
    event.WithHeader("tenant", "acme"),
)
```

Every envelope carries a message id, the W3C `traceparent` header and, when `ctx` is a handler context, the causal lineage of the handled event.

### Request/Reply (EmitSync)

```go
response, err := bus.EmitSync(ctx, myEvent)
```

Sends a NATS request and waits for a response. The response is decoded back into an event. Timeout is either from the context deadline or `RequestTimeout`.

`EmitSync` takes no emit options: its signature, `EmitSync(ctx, evt)`, matches `event.SyncBus`. Its Metadata comes from `ctx` only, so emitting from a handler with the handler context makes the request a child of the handled event.

When the responder failed, `EmitSync` returns a `*eventnats.RemoteError` instead of waiting for the request to time out (see [Error replies](#error-replies)).

## Subscribing

```go
bus.Subscribe(ctx, "user.created", handler, event.WithHandlerName("email-sender"))
```

For request/reply handlers:

```go
bus.SubscribeSync(ctx, "user.created", handler, event.WithHandlerName("responder"))
```

`SubscribeSync` decodes the incoming message, runs the handler through the invoker chain, then encodes and responds with the event.

The reply is a child of the request: it gets a **fresh message id** (never the request id, even if the event implements `WithId`), its `causation_id` is the request id and it inherits the request's correlation id.

The handler context is derived from the context passed to `Subscribe`, never from `context.Background()`, so cancelling it cancels the handlers and trace spans parent to the caller.

## Metadata

`Subscribe` and `SubscribeSync` decode the envelope and put the event [Metadata]({{ site.baseurl }}/core-concepts/event-interface/#event-metadata) in the handler context, together with the producer's trace context. Handlers read it with `event.MetadataFromContext(ctx)`.

## Failure handling

NATS Core has one delivery per message, and it is always final. After the handler chain, `event.Settle` classifies the result:

- success, a duplicate or a message the chain already dead-lettered: nothing to do;
- every other failure is a terminal outcome. It is **dead-lettered** through `BusOptions.DLQPublisher` (outcome `dead_lettered`) or **dropped** when none is configured (outcome `dropped`), and reported **once** through `Observer.Failure`, followed by the event's own `OnErr` method;
- a handler panic is recovered into `event.ErrHandlerPanic` and follows the same path. A panic in the code around the handler (decode, settle, the DLQ publisher) is recovered and reported as an `InternalError` with `Op` `panic` and the stack, so it never kills the nats.go delivery goroutine.

The disposition of every error is in the [Failure Model]({{ site.baseurl }}/architecture/failure-model/), and the order of the invokers in [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/).

Configure the same `DLQPublisher` on the DLQ invoker and on the bus: the invoker handles what the chain catches, the bus catches what the chain does not (a failure outside the chain, no DLQ invoker, and every decode failure), and a message is never published twice.

### Decode failures

A message that cannot be decoded never reaches the handler. It is settled as a terminal outcome with no event (`evt == nil`) and dead-lettered through `DLQPublisher` with its raw payload. The context of the settlement is the one `Registry.Decode` returned, so once the envelope parsed it keeps the `Metadata` (event id, correlation id) and the trace context of the undecodable message, and the DLQ record keeps its `Eventbus-Dlq-Event-Id`, `Eventbus-Dlq-Event-Name` and traceparent. See [Registry & Envelope]({{ site.baseurl }}/core-concepts/registry-envelope/).

### Error replies

A failed `SubscribeSync` request is answered at once, with no body, instead of leaving the requester to time out. The reply carries the `Eventbus-Error` header (the constant `nats.HeaderError`):

| Header value | When |
|---|---|
| `<reason>` | `BusOptions.ReplyErrorMessage` is false (default) |
| `<reason>: <message>` | `ReplyErrorMessage` is true; the message is truncated |

The reason is the stable reason code of the failure (for example `terminal`, `retries_exhausted`, `circuit_open`, `decode_malformed`, `panic`, `interrupted`). **Internal error text never reaches remote requesters unless you opt in with `ReplyErrorMessage`.**

`EmitSync` turns the header into a `*eventnats.RemoteError` with `Subject`, `Reason` and `Message`. Match it with `errors.Is`:

```go
_, err := bus.EmitSync(ctx, myEvent)

switch {
case errors.Is(err, eventnats.ErrRemoteDecode):
    // the responder could not decode the request
case errors.Is(err, event.ErrCircuitOpen):
    // the responder's circuit breaker is open
case errors.Is(err, eventnats.ErrRemoteHandler):
    // the responder decoded the request and its handler failed
}
```

`ErrRemoteFailure` matches every `*RemoteError`; `ErrRemoteDecode` matches a decode failure and `ErrRemoteHandler` any other; and the error also matches the root sentinel of its reason (`event.ErrSendToDLQ` for `terminal`, `event.ErrRetriesExhausted`, `event.ErrCircuitOpen`, `event.ErrRateLimited`, `event.ErrInProgress`, `event.ErrUnknownVersion`, `event.ErrHandlerPanic`, `event.ErrShutdownInterrupted`, the decode sentinels, `event.ErrDuplicate`).

## RegisterSubscribers

Bulk registration using the `Subscriber` interface, consistent with LocalBus and JetStreamBus:

```go
bus.RegisterSubscribers(ctx,
    NewEmailSubscriber(),
    NewAuditSubscriber(),
)
```

Subscribers implementing `SubscribeOptionsProvider` will have their options merged automatically.

## Tracing

`Emit` and `EmitSync` create a producer span named `publish <subject>` with the OpenTelemetry messaging attributes (`messaging.system` is `nats`, the destination name, the operation and the message id). The span carries ids and the subject only, never the payload. See [Observability]({{ site.baseurl }}/observability/).

## Shutdown and Close

The NATS Bus implements `event.Shutdowner`.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

if err := bus.Shutdown(ctx); err != nil {
    slog.Error("shutdown did not drain", "error", err)
}
```

`Shutdown(ctx)`:

1. returns `event.ErrBusClosed` from `Emit`, `EmitSync`, `Subscribe` and `SubscribeSync` from the moment it begins, except an `Emit` made with the context of a running handler, which is accepted so the handler can finish;
2. **drains** every tracked subscription (messages already received are still delivered), then waits for the in-flight callbacks, and only then closes the connection, so the replies of `SubscribeSync` requests, including the `Eventbus-Error` reply of a recovered panic, still go out;
3. when `ctx` expires first, cancels the handlers with cause `event.ErrShutdownInterrupted`, closes the connection and returns `ctx.Err()`. Interrupted handlers are reported and **never dead-lettered**. Messages delivered afterwards do not run: they are reported as interrupted and a `SubscribeSync` request gets an `Eventbus-Error` reply with reason `interrupted`.

A drain or unsubscribe that fails is not logged directly: it is reported as an `InternalError` signal with `Op` `drain` or `unsubscribe`, so your Observer sees it. Concurrent and repeated calls share one drain.

`Close()` is `Shutdown` bounded by `ShutdownTimeout` (30 seconds by default) and returns its error. Because the bus closes the NATS connection, do not share the connection with other users that outlive the bus. When `Observer` is nil the bus also flushes the default slog observer it created; an observer you supply is never flushed by the bus.
