# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2026-10-13

### Added

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `ErrInvalidConfig` — returned by `Subscribe` when the consumer configuration is invalid
- Integration test harness against a real `nats-server -js` (build tag `integration`) with reusable helpers
- Unit tests for the DLQ publisher

#### Tests and tooling
- Unit tests for `telemetry`, `prometheus`, `redis` and `json`
- Per-module CI: `GOWORK=off` build, `-race` test and golangci-lint v2 matrix over all 9 modules, a workspace job and a real nats-server JetStream integration job
- `RELEASING.md`, `scripts/release.sh`, `scripts/verify-release.sh` and `scripts/ci-local.sh` (layered release procedure and local CI equivalent)

### Changed

#### Core (`github.com/isaquesb/go-event-bus`)
- **Breaking:** `SyncBus.EmitSync` now returns `(Event, error)`. `LocalBus.EmitSync` returns the same event and a nil error after all handlers finish. Update callers: `evt, err := bus.EmitSync(ctx, e)`.
- Minimum Go version is now 1.26: every module declares `go 1.26.0`.

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `go.mod` now declares its requirement on the root module

#### All modules
- All in-repo modules require each other at `v0.3.0`

### Fixed

#### Core (`github.com/isaquesb/go-event-bus`)
- `NewLocalBus` defaults a nil `Invoker` to a passthrough invoker instead of panicking

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- `Registry.Decode` no longer races with `Register` (read lock)

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `DLQ.Publish` returns marshal errors instead of dropping them
- `Subscribe` validates `MaxDeliver` against the backoff length and returns `ErrInvalidConfig`

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- `Subscribe` and `SubscribeSync` pass the subscription context to handlers

#### Redis Stores (`github.com/isaquesb/go-event-bus/redis`)
- `IdempotencyStore.Put` returns marshal errors
- The rate-limit Lua script uses the server-side `TIME`

### Dependencies

- `go.opentelemetry.io/otel` (and `trace`, `sdk`) bumped to `v1.47.0` in `/json`, `/nats`, `/telemetry`, `/jetstream`, `/examples`
- `github.com/nats-io/nats.go` bumped to `v1.54.0` in `/nats`, `/jetstream`, `/examples`
- `github.com/redis/go-redis/v9` bumped to `v9.23.0` in `/redis`, `/examples`
- `github.com/prometheus/client_golang` bumped to `v1.25.0` in `/prometheus`

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
