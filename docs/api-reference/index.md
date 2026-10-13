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
- [`SyncBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SyncBus) - Synchronous dispatch interface (`EmitSync` returns `(Event, error)`)
- [`Invoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Invoker) - Middleware interface
- [`Registry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Registry) - Serialization interface
- [`Envelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Envelope) - Wire format
- [`LocalBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#LocalBus) - In-process bus
- [`Subscriber`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Subscriber) - Grouped handler interface
- [`SubscribeOptionsProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOptionsProvider) - Optional interface for subscriber-level subscribe options
- [`EmitOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EmitOption) - Functional option for Emit
- [`WithSubject`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithSubject) - Overrides the publish subject (defaults to `evt.Name()`)
- [`Metadata`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Metadata) - Identity, causal lineage and headers of one event, read by handlers
- [`WithMetadata`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithMetadata) - Returns a context that carries a `Metadata`
- [`MetadataFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#MetadataFromContext) - Reads the `Metadata` of the event being handled from the handler context
- [`ResolveOutbound`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ResolveOutbound) - Resolves the `Metadata` of an event about to be emitted (id, correlation, causation, headers)
- [`EnvelopeEncoder`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EnvelopeEncoder) - Optional registry interface: `EncodeWith` encodes with already resolved `Metadata`
- [`EncodeEvent`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EncodeEvent) - Encodes with `EncodeWith` when the registry implements `EnvelopeEncoder`, else `Encode`
- [`NewID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewID) - Generates a UUIDv7 message id
- [`WithID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithID) - Sets the message id of an emitted event
- [`WithCorrelationID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithCorrelationID) - Sets the correlation id of an emitted event
- [`WithHeader`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithHeader) - Attaches a user header to an emitted event (repeatable)
- [`SubscribeOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOption) - Functional option for Subscribe
- [`WithHandlerName`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithHandlerName) - Sets handler name for metrics/logging
- [`WithStream`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithStream) - JetStream stream name (required for JetStream)
- [`WithConsumer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithConsumer) - JetStream durable consumer name

### JSON Registry

- [`Option`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#Option) - Functional option for `json.NewRegistry`
- [`WithPropagator`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#WithPropagator) - Replaces the default W3C trace context and baggage propagator
- [`WithIDGenerator`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#WithIDGenerator) - Sets the id generator used for events without an explicit id
- [`EncodeWith`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#Registry.EncodeWith) - Encodes an event with explicit `Metadata`

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
- [`ErrUnknownEvent`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrUnknownEvent) - Decode: the envelope names an event that is not registered
- [`ErrMalformedEnvelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrMalformedEnvelope) - Decode: undecodable bytes, missing name, negative version or a payload that does not fit the event
- [`ErrMissingUpcaster`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrMissingUpcaster) - Decode: no upcaster for an intermediate version
- [`ErrUnknownVersion`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrUnknownVersion) - Decode: the envelope version is newer than the latest registered version
- [`DecodeError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DecodeError) - Wraps a decode failure with the event name and version; `errors.Is` and `errors.As` both work
- [`ErrStreamRequired`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrStreamRequired) - JetStream `Subscribe` called without `WithStream`
- [`ErrInvalidConfig`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrInvalidConfig) - Invalid JetStream consumer configuration (for example `MaxDeliver` inconsistent with the backoff length)
