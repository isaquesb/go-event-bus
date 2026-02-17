# Architecture

This document describes the internal architecture and design rationale of the Event Bus.

---

## High-Level Overview

The Event Bus is composed of three orthogonal layers:

1. **Event Definition & Serialization**
2. **Execution Pipeline (Invoker Chain)**
3. **Transport / Delivery Mechanism**

These layers are intentionally decoupled.

---

## 1. Event & Serialization Layer

### Envelope

Every event is serialized into an `Envelope` containing:

- Name
- Version
- Timestamp
- Payload
- Trace identifiers

This allows:

- Versioned schema evolution
- Trace correlation across transports
- Safe replay and DLQ handling

---

### Registry

The Registry owns:

- Encoding events into envelopes
- Decoding envelopes back into events
- Upcasting older versions to newer ones

Upcasters are pure functions and deterministic.

---

## 2. Execution Pipeline

### Invoker Chain

Handlers are never executed directly. Instead, execution flows through a chain:

```
Invoker -> Invoker -> ... -> Handler
```

Each Invoker:

- Has a single responsibility
- Is stateless or externally backed
- Emits its own metrics

---

### Ordering Rationale

Recommended order:

1. Tracing
2. Metrics
3. Rate Limiting
4. Idempotency
5. Retry
6. Circuit Breaker
7. DLQ
8. Handler

**Why?**

- Observability must wrap everything
- Cheap rejections happen early
- Retries occur before health decisions
- DLQ is terminal and isolated

---

## 3. Transport Layer

### LocalBus

- In-process
- Async or sync dispatch
- No delivery guarantees
- Ideal for monoliths or local orchestration

---

### NATS Core Bus

- Fire-and-forget delivery
- No persistence
- Request/Reply pattern support
- Lowest latency option

---

### JetStream Bus

- At-least-once delivery
- Consumer-managed retries
- Horizontal scalability
- Replay support

JetStream is used only for **transport**, never for business logic concerns like DLQ or retries.

---

## Distributed Safety

### Idempotency

- Implemented via external store (e.g., Redis)
- Guards against duplicates across pods
- Handler-scoped keys
- Key extraction priority: `WithIdempotencyKey` > `WithId`

**Redis Implementation:**
- Get-then-Put pattern with SET command
- Status-based TTL (processing: 5m, completed: 24h, failed: 1h)
- Stale lock recovery via TTL expiration
- Invoker checks existence via Get before Put

### Rate Limiting

- Distributed token bucket
- Keyed by event-defined identity
- Supports burst control

**Redis Implementation:**
- Lua script for atomic check-and-decrement
- Sliding window with token refill
- Configurable rate, period, and burst

---

## Failure Model

Errors are classified into:

- **Transient**: retried
- **Terminal**: sent to DLQ
- **Policy**: rejected (rate limit, duplicate)

Circuit Breaker reacts only to **transient failures**.

---

## Observability Model

### Metrics

- Counters for attempts, failures, retries
- Histograms for latency
- Low-cardinality labels only

### Tracing

- Spans started per handler
- Context propagated via envelopes
- Transport-agnostic correlation

---

## Key Architectural Decisions

- No hidden retries
- No implicit DLQ
- No shared mutable state
- No transport-specific behavior leakage

---

## Future Extensions

- Replay tooling
- Admin introspection APIs
- Schema registry
- Backpressure signals

---

This architecture prioritizes correctness, observability, and long-term maintainability over convenience.

