---
layout: default
title: Transport Layer
parent: Architecture
nav_order: 2
---

# Transport Layer

## Transport Tradeoffs

| | LocalBus | NATS Core | JetStream |
|---|---|---|---|
| **Delivery** | In-process | Fire-and-forget | At-least-once |
| **Persistence** | None | None | Durable |
| **Latency** | Lowest | Low | Medium |
| **Ordering** | Per-handler | None | Per-consumer |
| **Replay** | No | No | Yes |
| **Scalability** | Single process | Multi-pod | Multi-pod |
| **Use case** | Monolith | Ephemeral events | Critical events |

## LocalBus

- In-process only
- Async (`Emit`) or sync (`EmitSync`) dispatch
- No delivery guarantees
- Ideal for monoliths, local domain events, or CQRS read model projection

## NATS Core

- Fire-and-forget across pods
- No persistence - if no subscriber is listening, the message is lost
- Request/Reply pattern for synchronous queries
- Lowest latency distributed option

## JetStream

- At-least-once delivery with ACK/NAK
- Consumer-managed retries with backoff
- Horizontal scalability via consumer groups
- Replay support from any point in the stream

### JetStream Error Handling

The JetStream bus maps every handler result to exactly one broker action (`Ack`, `Nak`, `NakWithDelay` or `TermWithReason`) through `event.Dispose` and `event.Settle`, the same code every transport uses. The complete table of each error and each broker action is in the [Failure Model]({{ site.baseurl }}/architecture/failure-model/#disposition-table), and the rules a new transport has to follow are in the [Adapter Contract]({{ site.baseurl }}/architecture/adapter-contract/).

### Design Decision

JetStream is used **only for transport**. Retry policy, circuit breaking and the dead-letter decision belong to the invoker chain. The bus only applies the shared `event.Settle` policy to what the chain returned, and publishes through its `DLQPublisher` what the chain did not dead-letter, so the transport layer stays thin and every transport behaves the same way.
