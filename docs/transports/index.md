---
layout: default
title: Transports
nav_order: 5
has_children: true
permalink: /transports/
---

# Transports

Go Event Bus supports three transport mechanisms: [LocalBus]({{ site.baseurl }}/transports/local-bus/), [NATS Bus]({{ site.baseurl }}/transports/nats-bus/) and [JetStream Bus]({{ site.baseurl }}/transports/jetstream-bus/).

| Feature | LocalBus | NATS Bus | JetStream Bus |
|---|---|---|---|
| Delivery | In-process, once | Fire-and-forget, once | At-least-once, redelivered by the broker |
| Final delivery | Every delivery | Every delivery | The delivery that reaches `MaxDeliver` (never, when `MaxDeliver` is 0) |
| Failure that is not acknowledged | Dead-lettered or dropped | Dead-lettered or dropped | `NakWithDelay` until the final delivery, then dead-lettered or dropped |
| DLQ backstop | `LocalBusOptions.DLQPublisher` | `BusOptions.DLQPublisher` | `BusOptions.DLQPublisher`, with `jetstream.NewDLQ` |
| Persistence | None | None | Durable |
| Ordering | None guaranteed | None | Per consumer |
| Replay | No | No | Yes, from the DLQ stream with the `Replayer` |
| Request/Reply | No | Yes, with an `Eventbus-Error` reply on failure | No |
| `Shutdown(ctx)` | Waits for in-flight and parked handlers, then interrupts them at the deadline | Drains the subscriptions, waits for in-flight handlers, closes the connection | Drains the consumers, hands buffered messages back with `Nak`, waits for in-flight handlers |
| Interrupted by shutdown | Reported, never dead-lettered | Reported, never dead-lettered | `Nak`, never dead-lettered |
| External dependency | None | NATS server | NATS + JetStream |
| Use case | Monolith, decoupling | Ephemeral events | Distributed durable |

All transports implement the same `Bus` interface for `Subscribe` and `Close`, and `event.Shutdowner` for `Shutdown(ctx)`. All of them run the invoker chain through `event.InvokeHandler` and settle a failure with the same `event.Settle` code, so the same error ends the same way everywhere and only the broker action differs. After `Shutdown` began, new work returns `event.ErrBusClosed`.

Each failure is reported once, through `Observer.Failure`. For what every error becomes on every transport, see the [Failure Model]({{ site.baseurl }}/architecture/failure-model/); for the order of the invokers, [Chain Composition]({{ site.baseurl }}/invoker-chain/composition/).
