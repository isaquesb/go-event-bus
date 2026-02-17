---
layout: default
title: Transports
nav_order: 5
has_children: true
permalink: /transports/
---

# Transports

Go Event Bus supports three transport mechanisms:

| Feature | LocalBus | NatsBus | JetStreamBus |
|---|---|---|---|
| Delivery | In-process | Fire-and-forget | At-least-once |
| Persistence | None | None | Durable |
| Ordering | Per-handler | None | Per-consumer |
| Replay | No | No | Yes |
| Request/Reply | No | Yes | No |
| External dependency | None | NATS server | NATS + JetStream |
| Use case | Monolith, decoupling | Ephemeral events | Distributed durable |

All transports share the same `Bus` interface for `Subscribe` and `Close`, and all support the invoker chain.
