---
layout: default
title: Design Goals
parent: Getting Started
nav_order: 3
---

# Design Goals

## Philosophy

Go Event Bus is designed around explicit control and composability. Every behavior is opt-in, every middleware is visible, and there are no hidden side effects.

## Principles

### Explicit Execution Pipeline

The invoker chain makes every cross-cutting concern visible. There are no hidden retries, no implicit DLQ routing, and no magic middleware. You build the chain, you control the order (see [Composition]({{ site.baseurl }}/invoker-chain/composition/)), you see what happens.

### No Framework Lock-in

Events are plain Go structs. Handlers are plain functions. There's no code generation, no annotations, and no required base types beyond the `Event` interface.

### First-Class Observability

One `event.Observer` per bus receives every failure and resilience signal, and backends turn them into logs, Prometheus metrics or OpenTelemetry. Tracing spans are created per handler. Context is propagated across transport boundaries via envelopes. See [Observability]({{ site.baseurl }}/observability/).

### Failures Are Never Silent

Every terminal outcome of a message reaches `Observer.Failure` exactly once, on every transport. A bus with no Observer installs a `SlogObserver`, so a failure is at least logged.

### Lossless Failure Handling

One shared policy (`Dispose` and `Settle`) decides whether a failed message is retried, dead-lettered or dropped. A dead letter keeps the original envelope bytes, so it can be replayed, and JetStream terminates a message only after its dead letter is published. See the [Failure Model]({{ site.baseurl }}/architecture/failure-model/).

### Transport Agnostic

The same event definitions and invoker chain work with LocalBus, NATS Core, and JetStream. Switch transports without changing business logic.

### Transport-Neutral Contract

A transport adapter follows one written contract: how it builds the handler context, runs the chain, settles the result and shuts down. A new adapter is held to the same rules as the shipped ones. See the [Adapter Contract]({{ site.baseurl }}/architecture/adapter-contract/).

### Optional Dependencies

Each transport and integration lives in its own subpackage. You only import (and depend on) what you use:
- Core: zero external dependencies
- NATS/JetStream: `github.com/nats-io/nats.go`
- Redis: `github.com/redis/go-redis/v9`
- Tracing: `go.opentelemetry.io/otel`
- Metrics: `github.com/prometheus/client_golang`

### Production-Grade Resilience

Built-in patterns for retry with backoff, circuit breaking, rate limiting, idempotency, and dead letter queues. All are composable and configurable.

## Key Architectural Decisions

- No hidden retries
- No implicit DLQ: nothing is dead-lettered unless you configure a publisher
- No silent failures
- No shared mutable state
- No transport-specific behavior leakage
- Error classification drives execution flow
