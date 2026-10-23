# Architecture

This document describes the internal architecture and design rationale of the Event Bus (v0.4.0).

---

## High-Level Overview

The Event Bus is composed of three orthogonal layers:

1. **Event Definition & Serialization**
2. **Execution Pipeline (Invoker Chain)**
3. **Transport / Delivery Mechanism**

These layers are intentionally decoupled. Two concerns cut across all of them: the
[failure model](https://isaquesb.github.io/go-event-bus/architecture/failure-model/) (what happens to
a message that fails) and the `event.Observer` (how everything that happens is reported).

---

## Modules

The root module defines every core interface and `LocalBus`. Every other capability is a separate
module, so you depend only on what you use.

| Module | Purpose |
|--------|---------|
| root (`github.com/isaquesb/go-event-bus`) | Core interfaces, `LocalBus`, `Observer`, `SlogObserver`, `InvokeHandler`, `Settle` |
| `invoker` | Execution pipeline: retry, circuit breaker, rate limit, idempotency, DLQ |
| `json` | Envelope serialization with trace propagation and the upcaster chain |
| `nats` | NATS Core bus (fire-and-forget) |
| `jetstream` | NATS JetStream bus (at-least-once, durable), DLQ publisher and replayer |
| `redis` | Redis-backed idempotency and rate-limit stores |
| `telemetry` | OpenTelemetry: `TracerInvoker` and an `event.Observer` for span events and metrics |
| `prometheus` | An `event.Observer` with a fixed Prometheus metric set |
| `examples` | Runnable examples, compiled and tested with the repository |

---

## 1. Event & Serialization Layer

### Envelope

Every event is serialized into an `Envelope` containing:

- Name
- Version
- Timestamp
- Identity: id, correlation id and causation id
- Headers
- Payload
- Trace identifiers

This allows:

- Versioned schema evolution
- Causal lineage across events
- Trace correlation across transports
- Safe replay and DLQ handling: a dead letter keeps the original envelope bytes

---

### Registry

The Registry owns:

- Encoding events into envelopes
- Decoding envelopes back into events
- Upcasting older versions to newer ones

Upcasters are pure functions and deterministic. A decode failure is a `*event.DecodeError` with a
sentinel you match with `errors.Is`.

---

## 2. Execution Pipeline

### Pipeline stages

A transport never calls a handler directly. Every delivery goes through the same three stages,
shared by LocalBus, NATS Core and JetStream:

```
Decode → InvokeHandler (chain, HandlerDone) → Settle (Dispose → broker action)
```

- `event.InvokeHandler` runs the invoker chain around the handler and emits `HandlerDone`.
- `event.Settle` runs once per handled message, maps the result through `event.Dispose` to a
  disposition and reports a terminal outcome once.

### Invoker Chain

Handlers are never executed directly. Instead, execution flows through a chain:

```
Invoker -> Invoker -> ... -> Handler
```

Each Invoker:

- Has a single responsibility
- Is stateless or externally backed
- Reports what it does to the Observer (retry attempts, breaker transitions, store errors)

---

### Ordering Rationale

Recommended order, outermost first:

```
Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler
```

**Why?**

- Observability must wrap everything
- Cheap rejections happen early
- DLQ is outside Retry, so only the final outcome is dead-lettered
- An open circuit stops the retries

The reason for each position is on the
[composition page](https://isaquesb.github.io/go-event-bus/invoker-chain/composition/). There is no
metrics invoker: latency and outcome come from the `HandlerDone` signal.

---

## 3. Transport Layer

### LocalBus

- In-process
- Async or sync dispatch
- No delivery guarantees: every delivery is the final one
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
- Consumer-managed redelivery and backoff
- Horizontal scalability
- Replay of dead-lettered messages

JetStream owns the redelivery, the invoker chain owns the in-process policy, and `event.Settle`
decides the broker action (Ack, Nak, NakWithDelay or Term) so the result is the same code on every
transport. Rules for a new transport are in the
[adapter contract](https://isaquesb.github.io/go-event-bus/architecture/adapter-contract/).

---

## Distributed Safety

### Idempotency

- Implemented via external store (e.g., Redis)
- Guards against duplicates across pods
- Handler-scoped keys
- Key extraction priority: `WithIdempotencyKey` > `WithId`

**Redis Implementation:**
- Get-then-Put pattern with SET command
- Status-based TTL (processing: 5m, completed: 24h, failed: 1h, dead_lettered: 1h)
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

- **Transient**: retried, then dead-lettered or dropped on the final delivery
- **Terminal**: dead-lettered at once, never retried
- **Flow control**: the handler did not run (rate limit, circuit open, idempotency in progress);
  retried until the final delivery

A duplicate is acknowledged, and a message the chain already dead-lettered is acknowledged too.
Circuit Breaker reacts only to **transient failures**.

Every terminal outcome is reported to `Observer.Failure` exactly once, then to the event's own
`OnErr` method. Dead-lettering is at-least-once. The disposition of every error on every transport
is in the [failure model](https://isaquesb.github.io/go-event-bus/architecture/failure-model/).

---

## Observability Model

### Observer

One `event.Observer` per bus receives ten typed signals. Invokers and transports only call it; the
backends implement it:

- `event.SlogObserver`: structured logs, the default when a bus has no Observer
- `prometheus.NewObserver`: a fixed set of Prometheus metric families
- `telemetry.NewObserver`: span events and, with a meter provider, OpenTelemetry metrics
- `event.MultiObserver`: several backends side by side, with a panic in one isolated from the rest

### Tracing

- `telemetry.NewTracerInvoker` starts one consumer span per handler invocation
- Context propagated via envelopes
- Transport-agnostic correlation

---

## Key Architectural Decisions

- No hidden retries
- No implicit DLQ: nothing is dead-lettered unless you configure a `DLQPublisher`
- No silent failures: every terminal outcome reaches the Observer once
- No shared mutable state
- No transport-specific behavior leakage

---

## Future Extensions

- Adapters for more brokers, following the adapter contract
- Admin introspection APIs
- Schema registry
- Backpressure signals

---

This architecture prioritizes correctness, observability, and long-term maintainability over convenience.
