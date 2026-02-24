# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-24

### Added

#### Core (`github.com/isaquesb/go-event-bus`)
- `EmitOptions` struct and `EmitOption` functional option type for configuring event publishing
- `WithSubject(string) EmitOption` — overrides the publish subject (defaults to `evt.Name()`)
- `ApplyEmitOptions(evt, opts...) EmitOptions` — applies emit options with subject seeded from `evt.Name()`
- `SubscribeOptionsProvider` interface — subscribers can implement `SubscribeOptions() []SubscribeOption` to carry transport-specific subscribe options through `RegisterSubscribers`

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- `RegisterSubscribers(ctx, subs ...Subscriber) error` — bulk subscriber registration, consistent with LocalBus
- `Emit` signature unified to `Emit(ctx, event, ...EmitOption)` — subject defaults to `evt.Name()`

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `RegisterSubscribers(ctx, subs ...Subscriber) error` — bulk subscriber registration; automatically applies `SubscribeOptionsProvider` options (e.g. stream name) per subscriber
- `Emit` signature unified to `Emit(ctx, event, ...EmitOption)` — subject defaults to `evt.Name()`, use `WithSubject` for hierarchical routing

### Changed

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- **Breaking:** `Emit(ctx, subject, event)` → `Emit(ctx, event, ...EmitOption)`. Replace `bus.Emit(ctx, "my.subject", evt)` with `bus.Emit(ctx, evt, event.WithSubject("my.subject"))`. If subject equals `evt.Name()`, the `WithSubject` option can be omitted.

### Dependencies

- `go.opentelemetry.io/otel` bumped to `v1.40.0` in `/json`, `/nats`, `/telemetry`, `/jetstream`
- `github.com/nats-io/nats.go` bumped to `v1.49.0` in `/nats`, `/jetstream`
- `github.com/redis/go-redis/v9` bumped to `v9.18.0` in `/redis`
- GitHub Actions: `actions/checkout` → v6, `actions/setup-go` → v6, `codecov/codecov-action` → v5, `actions/upload-pages-artifact` → v4

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

[0.2.0]: https://github.com/isaquesb/go-event-bus/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/isaquesb/go-event-bus/releases/tag/v0.1.0
