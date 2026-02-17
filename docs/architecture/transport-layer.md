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

The JetStream bus classifies invoker errors to decide message acknowledgment:

- **Success or duplicate** → ACK (message processed)
- **Circuit open** → NAK with delay (wait for recovery)
- **Rate limited** → NAK with delay (wait for window)
- **Other errors** → NAK (immediate retry, bounded by MaxDeliver)
- **Decode failure** → Term (malformed, never retry)

### Design Decision

JetStream is used **only for transport**, never for business logic concerns like DLQ or retries. Those are handled by the invoker chain, keeping the transport layer thin and focused.
