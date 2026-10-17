# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-10-17

v0.4.0 is a breaking release. It gives every event an identity and a causal lineage, defines what happens to a message that fails (one disposition shared by all transports, a dead-letter record you can replay), makes the bus safe to stop and to overload, and replaces the metrics plumbing with one `Observer`. **This section is the migration guide**: there is no separate migration page. Every signature break has a **Breaking:** entry with a before and an after snippet, and every behavior change has a short note.

> **Read this first: tracing changes without a compile error.** A consumer now honors the producer's W3C sampled flag. v0.3.0 forced every consumed message to be sampled; v0.4.0 does not, and there is no option to bring the old behavior back. See the first entry under Changed.

### Added

#### Core (`github.com/isaquesb/go-event-bus`)

*Observability*
- `Observer`, the single interface through which invokers and transports report what happens, with ten signals: `RetryAttempt`, `BreakerTransition`, `Failure`, `DeadLettered`, `HandlerDone`, `DecodeFailed`, `DLQPublishFailed`, `StoreError`, `IdempotencyStale` and `InternalError`. Embed `NopObserver` to implement only the signals you need; combine observers with `MultiObserver`. `LocalBusOptions.Observer` configures it once per bus.
- `SlogObserver`, `NewSlogObserver` and `WithLogEvery`: structured `log/slog` records with per-window deduplication. A bus with no `Observer` installs one, so failures are never silent.

*Failure semantics and DLQ*
- `DLQRecord`, `NewDLQRecord`, `PublishDeadLetter` and `MemoryDLQ` (an in-memory `DLQPublisher` for tests and examples); `LocalBusOptions.DLQPublisher`, the per-bus dead-letter backstop.
- `Dispose`, `Settle` and `Disposition`: the one mapping from a handler error to Ack, Retry, RetryDelayed or Terminal, shared by every transport.

*Context and metadata*
- `Metadata`, `WithID`, `WithCorrelationID` and `WithHeader`: every event carries an id, a correlation id and a causation id, readable in the handler with `MetadataFromContext`.

*Lifecycle*
- `LocalBus.Shutdown(ctx)`, the `Shutdowner` interface, `ErrBusClosed`, `ErrQueueFull`, `OverflowPolicy` (`OverflowBlock`, `OverflowUnbounded`, `OverflowDrop`) and `LocalBusOptions.HandlerTimeout`.

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `CircuitBreakerConfig.OnStateChange` and `CircuitBreaker.State(handler)`
- `RetryPolicy.HistorySize` (default `DefaultHistorySize`, 5) and `StatusDeadLettered` for idempotency records

### Changed

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- **Breaking:** the consumer honors the producer's sampled flag. v0.3.0 rebuilt the remote span context from the envelope's `trace_id` and `span_id` with the sampled flag always set, so every consumed message was traced whatever the producer decided. v0.4.0 extracts `traceparent`, `tracestate` and `baggage` from the envelope headers, so the consumer span gets the sampling decision of the producer: with a `ParentBased` sampler an unsampled producer gives an unsampled consumer span, and the volume of consumer traces falls to the producer's sampling rate. There is no opt-out. An envelope that carries only the legacy `trace_id` and `span_id` (no `traceparent`) is still read as sampled and remote, so a rolling deploy loses no trace. To keep tracing every message, sample on the consumer side with a sampler that does not follow the remote parent, such as `sdktrace.AlwaysSample()`.

#### Core (`github.com/isaquesb/go-event-bus`)
- **Breaking:** `DLQPublisher.Publish` takes one `event.DLQRecord` instead of an event and a cause. The record carries the original envelope bytes (`Raw`, empty on `LocalBus`, where `Evt` is set), the decoded `Evt` (nil on a decode failure), the `Cause`, a stable `Reason`, `Handler`, `Subject`, `Attempts`, the `History` of attempts, and the `Delivery` and `Execution`. `invoker.DLQPublisher` stays as an alias of `event.DLQPublisher`.

  ```go
  // v0.3.0
  type auditDLQ struct{ log *slog.Logger }

  func (d auditDLQ) Publish(ctx context.Context, evt event.Event, cause error) error {
  	d.log.Error("dead letter", "event", evt.Name(), "cause", cause)
  	return nil
  }
  ```

  ```go
  // v0.4.0
  type auditDLQ struct{ log *slog.Logger }

  func (d auditDLQ) Publish(ctx context.Context, rec event.DLQRecord) error {
  	d.log.Error("dead letter", "event", rec.EventName, "handler", rec.Handler,
  		"reason", rec.Reason, "attempts", rec.Attempts, "cause", rec.Cause)
  	return nil
  }
  ```
- **Breaking:** `LocalBusOptions.OnErr` is replaced by `LocalBusOptions.Observer`. The bus-level hook is now `Observer.Failure`, called once per terminal outcome of a message (dead-lettered, dropped, interrupted, decode failure, retries exhausted with no DLQ) with a `FailureContext`: handler, outcome, reason, attempts, the retry history, delivery count, failing invoker, circuit breaker state, and the event `Metadata`. On a decode failure `FailureContext.Event` is nil, as `evt` was in `OnErr`. The event's own `OnErr` method (`event.OnErr`) stays and still runs, after `Observer.Failure`.

  ```go
  // v0.3.0
  bus := event.NewLocalBus(event.LocalBusOptions{
  	OnErr: func(ctx context.Context, evt event.Event, err error, handler string) {
  		slog.Error("handler failed", "event", evt.Name(), "handler", handler, "err", err)
  	},
  })
  ```

  ```go
  // v0.4.0
  type failureLogger struct{ event.NopObserver }

  func (failureLogger) Failure(ctx context.Context, f event.FailureContext) {
  	slog.ErrorContext(ctx, "handler failed", "handler", f.Handler,
  		"outcome", f.Outcome, "reason", f.Reason, "attempts", f.Attempts, "cause", f.Cause)
  }

  bus := event.NewLocalBus(event.LocalBusOptions{Observer: failureLogger{}})
  ```
- **Breaking:** `LocalBus.EmitSync` returns the `errors.Join` of the terminal failures of its handlers instead of always returning a nil error. Every handler still runs; each error is a `*event.HandlerError` that names its handler (`errors.As`), and `errors.Is` reaches the cause (a recovered panic matches `event.ErrHandlerPanic`). A failure that was dead-lettered and a duplicate are handled outcomes and are not errors. Callers that ignored the error keep compiling, but a failing handler now surfaces.

  ```go
  // v0.3.0: err was always nil, whatever the handlers did
  evt, err := bus.EmitSync(ctx, e)
  ```

  ```go
  // v0.4.0
  evt, err := bus.EmitSync(ctx, e)
  var herr *event.HandlerError
  if errors.As(err, &herr) {
  	slog.Error("handler failed", "handler", herr.Handler, "err", herr.Err)
  }
  ```

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- **Breaking:** the `metrics` argument is gone from `NewRetry`, `NewCircuitBreaker`, `NewRateLimiter`, `NewIdempotency` and `NewDLQ`, and the `Metrics` invoker is removed. Latency and outcome of every handler call come from the `HandlerDone` signal, so the chain has no metrics invoker: configure an `Observer` on the bus (see Prometheus and Observability below). The recommended order changes too: the DLQ invoker moves outside Retry.

  ```go
  // v0.3.0
  chain := invoker.NewChain(
  	invoker.NewMetrics(provider),
  	invoker.NewRateLimiter(rlStore, rlCfg, provider),
  	invoker.NewIdempotency(idStore, idCfg, provider),
  	invoker.NewRetry(policy, provider),
  	invoker.NewCircuitBreaker(cbCfg, provider),
  	invoker.NewDLQ(dlq, provider),
  )
  ```

  ```go
  // v0.4.0
  chain := invoker.NewChain(
  	invoker.NewRateLimiter(rlStore, rlCfg),
  	invoker.NewIdempotency(idStore, idCfg),
  	invoker.NewDLQ(dlq),
  	invoker.NewRetry(policy),
  	invoker.NewCircuitBreaker(cbCfg),
  )
  ```

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- **Breaking:** `NewDQL(js, metrics, prefix)` is removed, with no alias, and replaced by `NewDLQ(js, prefix, opts...)`. The default prefix changes from `dql.` to `dlq.` (`DefaultDLQPrefix`), so a deployment that relied on the default publishes to different subjects after the upgrade: **pass the prefix explicitly to keep `dql.*`**. A dead letter is now published to the prefix plus the subject the message came from (v0.3.0 used the event name).

  ```go
  // v0.3.0
  dlq := jetstream.NewDQL(js, nil, "dql.")
  ```

  ```go
  // v0.4.0, keeping the v0.3.0 subjects
  dlq := jetstream.NewDLQ(js, "dql.")

  // v0.4.0, with the new default prefix "dlq."
  dlq = jetstream.NewDLQ(js, jetstream.DefaultDLQPrefix)
  ```
- **Breaking:** `(*Replayer).Replay` returns `(ReplayResult, error)` instead of `error`, and it no longer swallows publish errors. It stops at the first record it cannot republish, Naks that record, and returns the error with the counts in `ReplayResult{Replayed, Failed}`. `ReplayOptions.ToSubject` is now optional: by default each record goes back to its original subject. The consumer is a durable one (`ReplayOptions.Consumer`, default `replay_<stream>`), so the next call continues where the last one stopped. `NewReplayer(js)` is source compatible.

  ```go
  // v0.3.0
  err := jetstream.NewReplayer(js).Replay(ctx, jetstream.ReplayOptions{
  	FromStream: "EVENTS_DLQ",
  	ToSubject:  "chat.created",
  	Limit:      100,
  })
  ```

  ```go
  // v0.4.0
  res, err := jetstream.NewReplayer(js).Replay(ctx, jetstream.ReplayOptions{
  	FromStream: "EVENTS_DLQ",
  	Limit:      100, // ToSubject is optional: records return to their original subject
  })
  if err != nil {
  	slog.Error("replay stopped", "replayed", res.Replayed, "failed", res.Failed, "err", err)
  }
  ```
- **Breaking:** `BusOptions.OnErr` is replaced by `BusOptions.Observer`, with the same `Observer.Failure` semantics as on `LocalBus`. See the Core entry for the snippet.

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- **Breaking:** `BusOptions.OnErr` is replaced by `BusOptions.Observer`, with the same `Observer.Failure` semantics as on `LocalBus`. See the Core entry for the snippet.

#### Prometheus Metrics (`github.com/isaquesb/go-event-bus/prometheus`)
- **Breaking:** `NewProvider(namespace, WithRegisterer(reg))` is replaced by `NewObserver(reg, WithNamespace(namespace))`. The module no longer implements `invoker.MetricProvider` and no longer requires the `invoker` module. The Observer declares a fixed set of eleven metric families when it is created, so a label mismatch can no longer panic on a handler goroutine, and it is passed as the bus `Observer` instead of through `invoker.NewMetrics`. The metric names change; the old to new table is under Changed in the Observability notes below.

  ```go
  // v0.3.0
  provider := eventbusprom.NewProvider("shop", eventbusprom.WithRegisterer(reg))
  chain := invoker.NewChain(invoker.NewMetrics(provider))
  ```

  ```go
  // v0.4.0
  obs := eventbusprom.NewObserver(reg, eventbusprom.WithNamespace("shop"))
  bus := event.NewLocalBus(event.LocalBusOptions{Observer: obs})
  ```

### Fixed

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- `Registry.Decode` returns a typed `*event.DecodeError` instead of panicking on an unknown event name or on an envelope with version 0 or no version (version 0 is read as version 1)

### Removed

#### Core (`github.com/isaquesb/go-event-bus`)
- `LocalBusOptions.OnErr`: use `LocalBusOptions.Observer` and `Observer.Failure`

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `NewMetrics` and the `Metrics` invoker: latency and outcome come from `Observer.HandlerDone`
- `MetricProvider`, `NoopProvider` (with `IncCounter`, `ObserveHistogram` and `SetGauge`) and `Labels`: implement `event.Observer` (embed `event.NopObserver`) instead
- The `metrics MetricProvider` parameter of `NewRetry`, `NewCircuitBreaker`, `NewRateLimiter`, `NewIdempotency` and `NewDLQ`
- `DLQPublisher.Publish(ctx, evt, cause)`: the method is now `Publish(ctx, rec event.DLQRecord)` (`invoker.DLQPublisher` is an alias of `event.DLQPublisher`)

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `NewDQL`: use `NewDLQ(js, prefix, opts...)`; set the prefix to `dql.` to keep the v0.3.0 subjects
- `BusOptions.OnErr`: use `BusOptions.Observer`

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- `BusOptions.OnErr`: use `BusOptions.Observer`

#### Prometheus Metrics (`github.com/isaquesb/go-event-bus/prometheus`)
- `NewProvider`, `WithRegisterer` and the `Provider` type (with `IncCounter`, `ObserveHistogram` and `SetGauge`): use `NewObserver(reg, opts...)` and `WithNamespace`

### Dependencies

- All in-repo modules require each other at `v0.4.0`
- `prometheus` requires only the root module: it no longer requires `invoker`
- `json` requires `go.opentelemetry.io/otel` directly (`propagation`), at the `v1.47.0` it already used
- `telemetry` requires `go.opentelemetry.io/otel/metric` and `go.opentelemetry.io/otel/sdk/metric` directly (the OpenTelemetry metrics API), at `v1.47.0`
- No third-party version changes: `go.opentelemetry.io/otel` stays at `v1.47.0`, `github.com/nats-io/nats.go` at `v1.54.0`, `github.com/redis/go-redis/v9` at `v9.23.0` and `github.com/prometheus/client_golang` at `v1.25.0`

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
