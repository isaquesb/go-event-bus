# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-02-17

### Added

#### Core (`github.com/isaquesb/go-event-bus`)
- `Event` interface with optional `WithId` and `WithVersion` extensions
- `Bus` and `SyncBus` interfaces for async/sync event dispatch
- `RegisterSubscribers` helper for batch subscription
- `SubscribeOptions` with functional options (`WithHandlerName`, `WithStream`, `WithConsumer`)
- `Registry` interface and `Envelope` struct for serialization contracts
- `LocalBus` in-process event dispatcher with concurrency control

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `Invoker` interface and `NewChain` composition
- `RetryInvoker` with exponential backoff and error classification
- `CircuitBreakerInvoker` with closed/open/half-open states per handler
- `RateLimitInvoker` with distributed store support and dynamic keys
- `IdempotencyInvoker` with pluggable store and `MemoryIdempotencyStore`
- `MetricsInvoker` with `MetricProvider` interface
- `DLQInvoker` with pluggable `DLQPublisher`
- Error classification: `Retryable`, `Terminal`, `ErrSendToDLQ`

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- `JsonRegistry` with envelope encoding/decoding
- Upcaster chain for schema versioning
- OpenTelemetry trace context propagation in envelopes

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- `NatsBus` for fire-and-forget pub/sub over NATS Core
- Request/Reply support via `EmitSync`

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `JetStreamBus` for durable, distributed event dispatch
- Consumer group and fan-out patterns
- ACK/NAK handling with invoker chain integration
- `JetStreamDLQ` publisher for dead letter queue
- `Replayer` for stream replay

#### Redis Stores (`github.com/isaquesb/go-event-bus/redis`)
- `RedisIdempotencyStore` with TTL-based status tracking
- `RedisRateLimitStore` with Lua-based atomic token bucket

#### Observability (`github.com/isaquesb/go-event-bus/telemetry`)
- `TracerInvoker` with OpenTelemetry span-per-handler tracing

#### Prometheus Metrics (`github.com/isaquesb/go-event-bus/prometheus`)
- `PrometheusProvider` implementing `MetricProvider` for histogram and counter metrics

[0.1.0]: https://github.com/isaquesb/go-event-bus/releases/tag/v0.1.0
