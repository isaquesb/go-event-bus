---
layout: default
title: API Reference
nav_order: 11
permalink: /api-reference/
---

# API Reference

Full API documentation is available on pkg.go.dev:

## Packages

| Package | Description | Link |
|---|---|---|
| `event` | Core interfaces (Event, Bus, Invoker, Registry) | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus) |
| `invoker` | Execution pipeline components | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker) |
| `json` | JSON serialization with envelope format | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json) |
| `jetstream` | NATS JetStream transport | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream) |
| `nats` | NATS Core transport | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats) |
| `redis` | Redis-backed stores | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis) |
| `telemetry` | OpenTelemetry tracing | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry) |
| `prometheus` | Prometheus metrics provider | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus) |

## Key Types

### Core

- [`Event`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Event) - Base event interface
- [`Bus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Bus) - Bus interface (Subscribe, Close)
- [`Invoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Invoker) - Middleware interface
- [`Registry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Registry) - Serialization interface
- [`Envelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Envelope) - Wire format
- [`LocalBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#LocalBus) - In-process bus
- [`Subscriber`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Subscriber) - Grouped handler interface
- [`SubscribeOptionsProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOptionsProvider) - Optional interface for subscriber-level subscribe options
- [`EmitOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EmitOption) - Functional option for Emit
- [`WithSubject`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithSubject) - Overrides the publish subject (defaults to `evt.Name()`)
- [`SubscribeOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOption) - Functional option for Subscribe
- [`WithHandlerName`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithHandlerName) - Sets handler name for metrics/logging
- [`WithStream`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithStream) - JetStream stream name (required for JetStream)
- [`WithConsumer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithConsumer) - JetStream durable consumer name

### Invoker

- [`Chain`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Chain) - Invoker chain composition
- [`Retry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Retry) - Retry with backoff
- [`CircuitBreaker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#CircuitBreaker) - Circuit breaker
- [`RateLimiter`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RateLimiter) - Rate limiting
- [`Idempotency`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Idempotency) - Duplicate detection
- [`DLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#DLQ) - Dead letter queue
- [`Metrics`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Metrics) - Metrics collection
- [`MetricProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#MetricProvider) - Metrics interface

### Error Types

- [`PermanentError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#PermanentError) - Terminal error
- [`RetryableError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RetryableError) - Retryable error
- [`ErrDuplicate`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#ErrDuplicate) - Duplicate detection
- [`ErrRateLimited`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#ErrRateLimited) - Rate limit exceeded
- [`ErrCircuitOpen`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#ErrCircuitOpen) - Circuit breaker open
- [`ErrSendToDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#ErrSendToDLQ) - Terminal error signal
