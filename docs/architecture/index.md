---
layout: default
title: Architecture
nav_order: 10
has_children: true
permalink: /architecture/
---

# Architecture

This section describes the internal architecture and design rationale of Go Event Bus.

The library is composed of three orthogonal layers:

1. **Event Definition & Serialization** - Events, Registry, Envelopes, Upcasters
2. **Execution Pipeline (Invoker Chain)** - Cross-cutting concerns as composable middleware
3. **Transport / Delivery Mechanism** - LocalBus, NATS Core, JetStream

These layers are intentionally decoupled. You can change your transport without touching your invoker chain, evolve your schema without affecting handlers, and add middleware without modifying events.

This architecture prioritizes **correctness, observability, and long-term maintainability** over convenience.

## In this section

- [Execution Pipeline]({{ site.baseurl }}/architecture/execution-pipeline/): how the invoker chain wraps a handler.
- [Transport Layer]({{ site.baseurl }}/architecture/transport-layer/): LocalBus, NATS Core and JetStream compared.
- [Distributed Safety]({{ site.baseurl }}/architecture/distributed-safety/): idempotency and rate limiting across processes.
- [Failure Model]({{ site.baseurl }}/architecture/failure-model/): what every error becomes on every transport.
- [Adapter Contract]({{ site.baseurl }}/architecture/adapter-contract/): the normative rules a new transport adapter has to meet.
