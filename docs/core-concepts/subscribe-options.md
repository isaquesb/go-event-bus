---
layout: default
title: Options
parent: Core Concepts
nav_order: 5
---

# Options

## Emit Options

`Emit` accepts functional options:

```go
bus.Emit(ctx, event, opts...)
```

| Option | Sets | LocalBus | NATS Bus | JetStream Bus |
|---|---|---|---|---|
| `WithSubject(subject)` | The subject the event is published or dispatched to | `Emit`, `EmitSync` | `Emit` | `Emit` |
| `WithID(id)` | The message id | `Emit`, `EmitSync` | `Emit` | `Emit` (also the `Nats-Msg-Id`) |
| `WithCorrelationID(id)` | The correlation id | `Emit`, `EmitSync` | `Emit` | `Emit` |
| `WithHeader(key, value)` | A user header | `Emit`, `EmitSync` | `Emit` | `Emit` |

`NatsBus.EmitSync(ctx, evt)` takes no options: its signature matches `event.SyncBus`, and its Metadata comes from `ctx` only.

### EmitOptions Struct

```go
type EmitOptions struct {
    Subject       string            // defaults to evt.Name()
    ID            string            // set by WithID
    CorrelationID string            // set by WithCorrelationID
    Headers       map[string]string // set by WithHeader
    // IDGenerator and InheritHeaders are populated by the bus from its own options
}
```

### WithSubject

```go
bus.Emit(ctx, myEvent, event.WithSubject("chat.message.room-general"))
```

Overrides the publish subject. If not set, the subject defaults to `evt.Name()`. This is useful for buses like JetStream that support hierarchical NATS subjects. On LocalBus the subject selects the handlers.

### WithID

```go
bus.Emit(ctx, myEvent, event.WithID("order-placed-7f3a"))
```

Sets the message id, overriding both the event's `Id()` and a generated id. An empty id leaves it unset. On JetStream the id is also the `Nats-Msg-Id`, so it is the dedup key inside the stream's duplicate window: re-emitting the same business event is deduplicated only when both emits carry the same id (`WithID` or `Id()`). See [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/#deduplication-with-nats-msg-id).

### WithCorrelationID

```go
bus.Emit(ctx, myEvent, event.WithCorrelationID(requestID))
```

Sets the correlation id, overriding the one inherited from the parent event. An empty id leaves it unset. There is no option to set the causation id: it always comes from the parent event.

### WithHeader

```go
bus.Emit(ctx, myEvent,
    event.WithHeader("tenant", "acme"),
    event.WithHeader("region", "eu"),
)
```

Attaches a user header. It can be repeated; for the same key the last value wins, and an empty key is ignored. An explicit header overrides a value inherited from the parent event.

Headers are producer data that every consumer can read, and they count toward the broker `max_payload`. Use headers as an **allowlist of non-sensitive keys** (a tenant, a region, a schema hint) and **never put secrets in them**, and never list a secret-bearing key in `InheritHeaders`. The trace keys `traceparent`, `tracestate` and `baggage` are owned by the transport.

### ApplyEmitOptions Helper

```go
cfg := event.ApplyEmitOptions(evt, opts...)
// cfg.Subject is pre-seeded with evt.Name()
```

---

## Subscribe Options

`Subscribe` accepts functional options:

```go
bus.Subscribe(ctx, subject, handler, opts...)
```

| Option | Sets | LocalBus | NATS Bus | JetStream Bus |
|---|---|---|---|---|
| `WithHandlerName(name)` | The handler name | yes | yes | yes |
| `WithStream(stream)` | The stream | ignored | ignored | **required** |
| `WithConsumer(name)` | The durable consumer | ignored | ignored | yes |
| `WithMaxDeliver(n)` | The redelivery limit | ignored | ignored | yes |
| `WithBackOff(delays)` | The redelivery delays | ignored | ignored | yes |

`RegisterSubscribers` passes the options of a subscriber that implements `SubscribeOptionsProvider` to `Subscribe`, after `WithHandlerName(s.Name())`.

### SubscribeOptions Struct

```go
type SubscribeOptions struct {
    HandlerName string
    Stream      string
    Consumer    string
    MaxDeliver  int
    BackOff     []time.Duration
}
```

### WithHandlerName

```go
event.WithHandlerName("send-welcome-email")
```

Identifies the handler in signals, logs and idempotency key scoping. On LocalBus and NATS it defaults to the subject; on JetStream it defaults to the consumer name.

### WithStream

```go
event.WithStream("CHAT_EVENTS")
```

Sets the JetStream stream name. **Required** for JetStream subscriptions: without it `Subscribe` returns `jetstream.ErrStreamRequired`. The stream must already exist.

### WithConsumer

```go
event.WithConsumer("chat-processor")
```

Sets the durable consumer name for JetStream. Defaults to the subject name if not set.

### WithMaxDeliver

```go
event.WithMaxDeliver(5)
```

Limits delivery attempts per message in JetStream. The delivery that reaches `MaxDeliver` is the **final** one: a failure there is dead-lettered or dropped instead of retried. If zero, messages are redelivered indefinitely, no delivery is ever final, and a warning is logged.

`MaxDeliver` must be greater than `len(BackOff)` when it is set. Otherwise `Subscribe` returns `jetstream.ErrInvalidConfig`, because NATS needs it to apply the backoff schedule. Mind the product with an in-process Retry: see [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/#retry-and-maxdeliver).

### WithBackOff

```go
event.WithBackOff([]time.Duration{
    1 * time.Second,
    5 * time.Second,
    30 * time.Second,
    1 * time.Minute,
})
```

Sets delays between JetStream redelivery attempts: the delay after delivery `n` is entry `n-1`, clamped to the last entry. If nil, the default progressive backoff above is used and a warning is logged. The circuit-open and rate-limit delays are set on the bus (`CircuitOpenDelay`, `RateLimitDelay`), not here.

## ApplyOptions Helper

```go
cfg := event.ApplyOptions(opts...)
```

Applies all options and returns the resulting `SubscribeOptions` struct.
