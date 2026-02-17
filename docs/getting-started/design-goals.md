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

The invoker chain makes every cross-cutting concern visible. There are no hidden retries, no implicit DLQ routing, and no magic middleware. You build the chain, you control the order, you see what happens.

### No Framework Lock-in

Events are plain Go structs. Handlers are plain functions. There's no code generation, no annotations, and no required base types beyond the `Event` interface.

### First-Class Observability

Every invoker emits its own metrics. Tracing spans are created per handler. Context is propagated across transport boundaries via envelopes.

### Transport Agnostic

The same event definitions and invoker chain work with LocalBus, NATS Core, and JetStream. Switch transports without changing business logic.

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
- No implicit DLQ
- No shared mutable state
- No transport-specific behavior leakage
- Error classification drives execution flow
