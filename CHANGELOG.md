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

- `Outcome` and `Reason` vocabularies (`OutcomeDeadLettered`, `OutcomeDropped`, `OutcomeInterrupted`, `ReasonRetriesExhausted`, `ReasonPanic`, ...), the signal structs (`FailureContext`, `HandlerResult`, `RetryAttemptInfo`, `DeadLetterInfo`, `DecodeFailure`, `DLQPublishFailure`, `StoreErrorInfo`, `IdempotencyStaleInfo`, `InternalErrorInfo`) and `BreakerTransition` (with `BreakerID`, which scopes `Seq` to one breaker) with the `BreakerState` and `BreakerReason` values.
- `InvokeHandler`, the single entry point every transport uses to run a handler through the chain, and `FailureScope`, which makes `Observer.Failure` fire exactly once per terminal outcome.

*Failure semantics and DLQ*
- Header constants for the dead-letter record: `HeaderDLQCause`, `HeaderDLQReason`, `HeaderDLQAttempts`, `HeaderDLQDeliveryCount`, `HeaderDLQHandler`, `HeaderDLQOriginalSubject`, `HeaderDLQFailedAt`, `HeaderDLQFirstAttemptAt`, `HeaderDLQEventID`, `HeaderDLQEventName`, `HeaderDLQOriginalMsgID`, `HeaderDLQOriginalStream`, `HeaderDLQOriginalSequence`, `HeaderDLQPublishedAt`, `HeaderDLQPanicStack`, `HeaderDLQHistory`, `HeaderDLQHistoryTruncated` and `HeaderDLQTraceparent`, plus `HeaderReplayTraceparent`, `HeaderReplayFailedTraceparent` and `HeaderReplayDLQSequence` for replayed messages. `DLQRecord.FailureHeaders()` builds them.
- Sentinels and typed errors: `ErrDeadLettered`, `ErrDeadLetterFailed`, `ErrRetriesExhausted`, `ErrInProgress`, `ErrHandlerPanic`, `ErrHandlerTimeout`, `ErrShutdownInterrupted`, `ErrUnknownEvent`, `ErrMalformedEnvelope`, `ErrMissingUpcaster`, `ErrUnknownVersion`, `FailureError`, `PanicError`, `HandlerError`, `DecodeError`, `DeadLetteredError` and `DeadLetterFailedError`. `ErrCircuitOpen`, `ErrRateLimited`, `ErrDuplicate`, `ErrSendToDLQ` and `FailureError` moved to the root module; the `invoker` names remain as aliases, so `errors.Is` and existing imports keep working.
- `Attempt` and the retry history helpers (`ReasonOf`, `AttemptsOf`, `FailingInvokerOf`, `AttemptClassOf`).

*Context and metadata*
- `Delivery` (count, max, subject, raw bytes, headers, stream and consumer) and `Execution` in the handler context, read with `DeliveryFromContext` and `ExecutionFromContext`; `Delivery.Final()` tells the last delivery.
- `LocalBusOptions.IDGenerator` and `LocalBusOptions.InheritHeaders` (an allowlist; no header is inherited by default); `EnvelopeEncoder`, `EncodeEvent` and `ResolveOutbound`; `NewID`, a UUIDv7 generator. `Envelope` gains the optional `id`, `correlation_id`, `causation_id` and `headers` fields, so v0.3.0 consumers ignore them and v0.3.0 bytes still decode.
- A handler that emits through its own context creates a child event: `causation_id` is the parent id and `correlation_id` is inherited (a root event's correlation id is its own id).

*Lifecycle*
- `LocalBus.Shutdown(ctx)`, the `Shutdowner` interface, `ErrBusClosed`, `ErrQueueFull`, `OverflowPolicy` (`OverflowBlock`, `OverflowUnbounded`, `OverflowDrop`) and `LocalBusOptions.HandlerTimeout`
- `LocalBusOptions.ShutdownTimeout`, `DefaultShutdownTimeout` (30 s) and `Lifecycle`, the building block transports share.

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `CircuitBreakerConfig.OnStateChange` and `CircuitBreaker.State(handler)`
- `RetryPolicy.HistorySize` (default `DefaultHistorySize`, 5) and `StatusDeadLettered` for idempotency records

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- `WithIDGenerator` and `WithPropagator` options for `NewRegistry`, and `Registry.EncodeWith`. The registry injects and extracts W3C `traceparent`, `tracestate` and `baggage` with a fixed propagator, not the global one.

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- `BusOptions.Observer`, `DLQPublisher`, `IDGenerator`, `InheritHeaders`, `TracerProvider`, `ShutdownTimeout` and `ReplyErrorMessage`; `Bus.Shutdown`
- `HeaderError` (`Eventbus-Error`), `RemoteError` and the `ErrRemoteFailure`, `ErrRemoteDecode` and `ErrRemoteHandler` sentinels returned by `EmitSync`

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- `BusOptions.Observer`, `DLQPublisher`, `PublishRetry`, `IDGenerator`, `InheritHeaders`, `TracerProvider` and `ShutdownTimeout`; `Bus.Shutdown`
- `PublishRetryPolicy`, `DefaultDLQPrefix`, `WithDLQRegistry`, `WithReplayTracerProvider`, `ReplayResult`, `ReplayOptions.Consumer` and `ReplayOptions.FilterSubject`, `ErrNoEnvelope` and `ErrReplaySubject`
- Integration tests against a real `nats-server -js` for dead-lettering and replay round trip, publish deduplication, shutdown, observability and log compaction

#### Telemetry (`github.com/isaquesb/go-event-bus/telemetry`)
- `NewObserver` with `WithMeterProvider`: the same fixed metric set as the Prometheus module through the OpenTelemetry metrics API (dot names, units in seconds), and span events on the consumer span for retry attempts, breaker transitions and dead letters
- `WithTracerProvider` and `WithSpanDestination` options for `NewTracerInvoker`
- `ScopeName` and the semantic-convention constants: span events (`EventRetryAttempt`, `EventBreakerTransition`, `EventDeadLettered`, `EventDLQPublishFailed`, `EventStoreError`, `EventIdempotencyStale`, `EventInternalError`) and attributes (`AttrHandler`, `AttrCorrelationID`, `AttrDeliveryCount`, `AttrReplayed`, `AttrDLQSequence`, `AttrLinkType`, `AttrRetryAttempt`, `AttrRetryClass`, `AttrRetryDelayMs`, `AttrBreakerFrom`, `AttrBreakerTo`, `AttrBreakerReason`, `AttrBreakerFailures`, `AttrBreakerSeq`, `AttrReason`, `AttrStore`, `AttrOp`, `AttrComponent`)

#### Prometheus Metrics (`github.com/isaquesb/go-event-bus/prometheus`)
- `NewObserver`, `WithNamespace` and `DefaultBuckets`: eleven pre-declared metric families, see the metric table under Changed

#### Tests and tooling
- `scripts/check-docs.sh`: checks the docs for removed symbols, the canonical chain order, links and navigation
- `scripts/release.sh --check-layers` and `scripts/ci-gate.sh`; `scripts/verify-release.sh` probes the v0.4.0 API; `RELEASING.md` documents the pull-request merge gate
- Runnable examples `08_causation_correlation.go`, `09_dlq_replay.go` and `10_failure_hooks.go`, with checked `Example_*` functions, and the guides for log compaction on JetStream and the adapter contract for transports

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

- A nil `Observer` installs a default `SlogObserver`, so a failure is never silent with zero configuration. Pass `event.NopObserver{}` to silence the bus. The bus flushes only the default observer it created, at the end of `Shutdown`; flush your own.
- One disposition for every transport. `event.Dispose` maps the handler error to Ack, Retry, RetryDelayed or Terminal, and `event.Settle` applies it once per message. On `LocalBus` and NATS Core every delivery is the final one (`Delivery{Count: 1, Max: 1}`), so a failure there is dead-lettered or dropped, never retried by the broker. The per-transport table is in the failure model page of the docs.
- A panic in a handler is recovered on every transport (v0.3.0 crashed the process on NATS and JetStream), becomes a `*event.PanicError` (`errors.Is(err, event.ErrHandlerPanic)`), and is terminal: not retried, dead-lettered with reason `panic`.
- `Close` is bounded and waits for in-flight handlers. `Shutdown(ctx)` stops accepting emits, drains the handlers and, when `ctx` expires, cancels them with cause `ErrShutdownInterrupted`; they are reported as interrupted and never dead-lettered. All three buses implement `event.Shutdowner`, so shutdown code does not need to know the transport. After shutdown an emit returns `ErrBusClosed` (`EmitSync`, NATS, JetStream) or is dropped and reported with `ErrBusClosed` (`LocalBus.Emit`). A handler may keep emitting through its own context while the bus drains.
- `LocalBus.Emit` handlers run under a context that keeps the emitter's values (metadata, trace span) but is not cancelled when the emitter's request ends, and is cancelled by the `Shutdown` deadline. The queue is bounded (`QueueSize`, default 1024) and `LocalBusOptions.Overflow` picks what happens under sustained overload: `OverflowBlock` (the default; `Emit` waits for room while its context is alive, then drops with `ErrQueueFull`), `OverflowUnbounded` (never blocks or drops, memory grows, parked work is lost if the process dies) and `OverflowDrop` (drops at once with `ErrQueueFull`). Throughput is `MaxConcurrent` divided by the handler latency; the queue only absorbs bursts. There is no FIFO guarantee: handlers of different emits can run in any order. An emit made from inside a handler of the same bus never waits and runs inline when no slot is free.
- `LocalBusOptions.HandlerTimeout` (default 0, off) bounds each async handler; expiry is a transient failure with cause `ErrHandlerTimeout`.
- `HandlerDone` outcome semantics. `circuit_open`, `rate_limited` and `in_progress` mean "the delivery will be retried". When such a failure happens on the final delivery, the message is lost, and the outcome is reported as `dropped` (no DLQ publisher) or `dead_lettered`, the same one `Observer.Failure` reports. This is what `eventbus_handler_total` counts.
- Trace and message identity: every emitted envelope carries an `id` (`WithID`, then the event's `Id()`, then a generated UUIDv7). `Id()` must be unique per message, not per entity.

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- Recommended chain order: `Tracer → RateLimit → Idempotency → DLQ → Retry → CircuitBreaker → Handler`, with DLQ outside Retry so retries run first and only the final outcome is dead-lettered (v0.3.0 documented Retry, then CircuitBreaker, then DLQ, so the DLQ invoker never saw a terminal failure). See `docs/invoker-chain/composition.md`.
- The DLQ invoker now fires: it dead-letters a terminal error on any delivery, and any other failure on the final delivery, and returns `*event.DeadLetteredError`, which the transports acknowledge. If the publish fails it returns `*event.DeadLetterFailedError`, and JetStream redelivers. A failure raised outside the DLQ invoker (flow control, store errors) is dead-lettered by the transport backstop, so configure the same `DLQPublisher` on the DLQ invoker and on the bus. Nothing is published twice.
- Retry returns a `*event.FailureError` with the cause, the reason, the number of attempts and a bounded history (`RetryPolicy.HistorySize`); `errors.Is` still matches the cause and `ErrRetriesExhausted`. Retry no longer converts a terminal error into `ErrSendToDLQ`; a terminal failure matches `ErrSendToDLQ` through the `FailureError`. When the circuit breaker opens in the middle of a retry run, Retry returns a `FailureError` that keeps the attempts that ran and still matches `ErrCircuitOpen`.
- **Retry multiplies with JetStream redelivery.** In-process attempts run on every delivery, so a message can run `MaxAttempts × MaxDeliver` times. On JetStream use a low `MaxAttempts` or no Retry and leave the delay to the consumer `BackOff`.
- The circuit breaker counts only transient failures (retryable, unclassified and handler timeouts). Terminal errors, panics, context cancellation and flow-control errors are neutral in every state. Half-open admits exactly one probe at a time, with a lease of `OpenTimeout`; other callers get `ErrCircuitOpen`. The transition from open to half_open is lazy: it happens when the first probe is admitted after `OpenTimeout`, so a gauge that says open means "not probed yet". `OnStateChange` and `Observer.BreakerTransition` run on the goroutine of the message that caused the transition, after the lock is released, in that order. They must not block: dispatch slow work elsewhere. Each breaker has an id (`BreakerTransition.BreakerID`); `Seq` increases per breaker.
- Idempotency returns `ErrInProgress` (retried later) instead of `ErrDuplicate` for a record that is still processing, and records `dead_lettered` for a message the DLQ took, so a replay with the same event id runs again. Only `completed` produces `ErrDuplicate`.
- The invoker sentinels (`ErrCircuitOpen`, `ErrRateLimited`, `ErrDuplicate`, `ErrSendToDLQ`) and `FailureError` live in the root module (`event.ErrCircuitOpen`, ...). The `invoker.ErrX` names are aliases of the root values, so `errors.Is` and existing code keep working; new code should use the root names.
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
- Acknowledgement is decided by `event.Settle`: Ack for success, a duplicate and a message the DLQ took; `Nak()` only when the subscription itself ended; `NakWithDelay` for a retry, for flow control (`CircuitOpenDelay`, `RateLimitDelay`) and for a failed dead-letter publish, which is never turned into a Term; `TermWithReason` only after a successful dead-letter, or for a drop when the bus has no `DLQPublisher`. A transient failure on a non-final delivery follows the consumer `BackOff` instead of an immediate `Nak()`. `Observer.Failure` fires once, on the terminal delivery, and never on an intermediate `NakWithDelay`.
- Decode failures are dead-lettered instead of a silent `Term()` with a nil event. A malformed envelope, an unknown event and a missing upcaster are dead-lettered at once; an unknown version (a rolling deploy) is retried until the final delivery. `Observer.Failure` and the DLQ record carry no event in that case (`FailureContext.Event` is nil); the metadata of an envelope that parsed is kept.
- On the final delivery every failure is dead-lettered with its own reason (`terminal`, `retries_exhausted`, `delivery_exhausted`, `circuit_open`, `rate_limited`, `unknown_version`, `in_progress`, `decode_*`, `panic`), whether or not a Retry invoker is in the chain; v0.3.0 stopped redelivering at `MaxDeliver` and lost the message silently. Cancelling the subscription context is never a dead letter: the message is `Nak`ed so another instance takes it.
- Default DLQ prefix `dql.` becomes `dlq.`; **set the prefix explicitly if you depend on `dql.*`**. A dead-letter record is the original envelope byte for byte, the original user headers, and these headers: `Eventbus-Dlq-Cause`, `Eventbus-Dlq-Reason`, `Eventbus-Dlq-Attempts`, `Eventbus-Dlq-Delivery-Count`, `Eventbus-Dlq-Handler`, `Eventbus-Dlq-Original-Subject`, `Eventbus-Dlq-Failed-At`, `Eventbus-Dlq-First-Attempt-At`, `Eventbus-Dlq-Event-Id`, `Eventbus-Dlq-Event-Name`, `Eventbus-Dlq-Original-Msg-Id`, `Eventbus-Dlq-Original-Stream`, `Eventbus-Dlq-Original-Sequence`, `Eventbus-Dlq-Published-At`, `Eventbus-Dlq-Panic-Stack`, `Eventbus-Dlq-Traceparent`, `Eventbus-Dlq-History` and `Eventbus-Dlq-History-Truncated`. Header values are truncated at 1 KiB with the marker `…[truncated]`. `Eventbus-Dlq-History` is one compact JSON array of the last attempts (`n`, `err`, `class`, `at`, `dur_ms`), capped at about 2 KiB; the oldest attempts go first and `Eventbus-Dlq-History-Truncated: true` is set. Broker `Nats-*` headers are stripped from the record (the original `Nats-Msg-Id` is kept in `Eventbus-Dlq-Original-Msg-Id`), because copying `Nats-Msg-Id` would make the DLQ stream drop a second record as a duplicate.
- **DLQ delivery is at-least-once.** If the dead-letter publish succeeds and the acknowledgement of the original fails, the message is redelivered and dead-lettered again. The record carries no message id, so consumers of the DLQ stream must tolerate duplicates.
- **v0.3.0 `dql.*` bodies cannot be replayed by the new Replayer.** They were `{"event":...,"error":...}` documents, not envelopes; the registry rejects them as malformed. Drain or discard them before the upgrade, or republish them yourself.
- Replay is verbatim: the envelope is never decoded, the original headers are kept, every `Eventbus-Dlq-*`, `Eventbus-Replay-*` and `Nats-*` header is dropped, and the publish carries `Nats-Msg-Id` `EVENT_ID:replay:DLQ_STREAM_SEQ`, so the duplicate window cannot swallow it while repeats of one record still deduplicate. The replayed message gets `Eventbus-Replay-Traceparent`, `Eventbus-Replay-Failed-Traceparent` and `Eventbus-Replay-Dlq-Sequence` headers outside the envelope, which the telemetry tracer turns into span links.
- `Emit` sets `Nats-Msg-Id` from the event id and retries transient publish failures (timeout, no responders) with the same id (`BusOptions.PublishRetry`, 3 attempts by default), so the stream deduplicates a retry whose first acknowledgement was lost. **An application-level re-Emit only deduplicates if the event id is stable**: use `event.WithID` or `Event.Id()`; a generated id is new on every call. Deduplication works inside the stream's `Duplicates` window.
- `Close` no longer blocks without bound: it runs `Shutdown` with `BusOptions.ShutdownTimeout`. Messages that were buffered but not started when shutdown begins are handed back with `Nak()` (each consumes one delivery).
- Panics are recovered; a panic outside the handler on the final delivery is reported as `Failure{dropped, panic}` and ended with `TermWithReason`.
- Producer spans are named `publish <subject>` with the OpenTelemetry messaging attributes (`BusOptions.TracerProvider`, global provider by default); v0.3.0 named them `eventbus.emit`.

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- **Breaking:** `BusOptions.OnErr` is replaced by `BusOptions.Observer`, with the same `Observer.Failure` semantics as on `LocalBus`. See the Core entry for the snippet.
- NATS Core handlers run through the same pipeline: panics are recovered, a failure is settled once (dead-lettered through `BusOptions.DLQPublisher` or dropped), and decode failures are reported with a nil event. Every delivery is final.
- `SubscribeSync` replies to a failed request with the `Eventbus-Error` header (a stable reason code such as `terminal` or `decode_unknown_event`) and no body, and `EmitSync` returns a `*nats.RemoteError` immediately instead of waiting for the request to time out (`errors.Is` matches `ErrRemoteFailure`, `ErrRemoteDecode` or `ErrRemoteHandler`, and the root sentinel of the reason). The error text is not sent unless `BusOptions.ReplyErrorMessage` is true, so internal messages do not leak to remote requesters. The reply of `SubscribeSync` is a child event of the request.
- `Close` shuts down with the bounded `Shutdown`; in-flight handlers are waited for (v0.3.0 did not).
- Producer spans are named `publish <subject>`; `Emit` and `EmitSync` return `ErrBusClosed` after shutdown.

#### Telemetry (`github.com/isaquesb/go-event-bus/telemetry`)
- **Breaking:** the consumer span name changes from `event.handle` to `process <subject>` (the subject the message arrived on, or the event name; `WithSpanDestination` overrides it when subjects carry entity ids), and it adopts the OpenTelemetry messaging semantic conventions (pinned to semconv v1.43.0, which is at Development status in the specification and can change). The attributes `event.name` and `handler.name` are replaced by `messaging.system`, `messaging.operation.name`, `messaging.operation.type`, `messaging.destination.name`, `messaging.message.id`, `eventbus.handler`, `eventbus.correlation_id` and `eventbus.delivery_count`. Update trace queries, dashboards and alerts that match the old span or attribute names. A replayed message gets span links to its dead-letter span and to its replay span, with `eventbus.replayed` and `eventbus.dlq.sequence`.
- `NewTracerInvoker(tracer)` is source compatible, and a nil tracer is now built from `WithTracerProvider` or the global provider. Keep the tracer invoker outermost in the chain, so the span events written by `NewObserver` land on its span.

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

##### Prometheus metric names, v0.3.0 to v0.4.0

`NewObserver` declares eleven families once, with fixed labels: `eventbus_handler_duration_seconds` (handler, event, outcome), `eventbus_handler_total` (handler, event, outcome), `eventbus_retry_attempts_total` (handler, event, reason), `eventbus_breaker_transitions_total` (handler, from, to, reason), `eventbus_breaker_state` (handler; 0 closed, 1 open, 2 half_open), `eventbus_dlq_records_total` (handler, event, reason), `eventbus_dlq_publish_errors_total` (handler, event), `eventbus_decode_failures_total` (reason), `eventbus_store_errors_total` (handler, store, op), `eventbus_idempotency_stale_total` (handler) and `eventbus_internal_errors_total` (component, op). `WithNamespace(ns)` prefixes every name with `ns_`. Event ids, subjects and error text are never labels. The histogram buckets are the v0.3.0 ones (`DefaultBuckets`). The OpenTelemetry metrics API of the `telemetry` module exposes the same set with dot names (`eventbus.handler`, `eventbus.handler.duration`, ...). Use one metrics backend, not both.

| v0.3.0 | v0.4.0 |
|--------|--------|
| `eventbus_invoker_calls_total`, `eventbus_invoker_latency_seconds` | `eventbus_handler_total`, `eventbus_handler_duration_seconds` (the label `result` ok/err becomes `outcome`) |
| `eventbus_retry_attempt_total` | `eventbus_retry_attempts_total` |
| `eventbus_retry_success_total`, `eventbus_retry_terminal_total`, `eventbus_retry_exhausted_total` | `eventbus_handler_total` by outcome and `eventbus_dlq_records_total` by reason |
| `eventbus_retry_backoff_ms` | removed; the delay is `eventbus.retry.delay_ms` on the retry span event |
| `eventbus_circuit_state` | `eventbus_breaker_state` |
| `eventbus_circuit_open_total`, `eventbus_circuit_half_open_total` | `eventbus_breaker_transitions_total` by `to` |
| `eventbus_circuit_blocked_total` | `eventbus_handler_total` with outcome `circuit_open` for a delivery that will be retried, and with outcome `dropped` or `dead_lettered` when the delivery was final |
| `eventbus_dlq_published_total`, `eventbus_dlq_publish_error_total` and the unprefixed `dlq_published_total`, `dlq_publish_error_total` | `eventbus_dlq_records_total`, `eventbus_dlq_publish_errors_total` |
| `eventbus_idempotency_duplicate_total` | `eventbus_handler_total` with outcome `duplicate` |
| `eventbus_idempotency_in_progress_total` | `eventbus_handler_total` with outcome `in_progress` for a delivery that will be retried, and with outcome `dropped` or `dead_lettered` when the delivery was final |
| `eventbus_idempotency_error_total`, `eventbus_ratelimit_error_total` | `eventbus_store_errors_total` by `store` |
| `eventbus_idempotency_processing_stale_total` | `eventbus_idempotency_stale_total` |
| `eventbus_ratelimit_blocked_total` | `eventbus_handler_total` with outcome `rate_limited` for a delivery that will be retried, and with outcome `dropped` or `dead_lettered` when the delivery was final (a lost message is counted as lost, never as `rate_limited`) |
| `eventbus_idempotency_lock_acquired_total`, `eventbus_idempotency_executed_total`, `eventbus_ratelimit_allowed_total` | removed; use `eventbus_handler_total` |

Alert rules that sum `eventbus_circuit_blocked_total` or `eventbus_ratelimit_blocked_total` must now also count `dropped` and `dead_lettered`, because a message lost on its final delivery no longer appears as a blocked one.

#### Logs
- `SlogObserver` logs failures, dead letters, decode failures, store errors and breaker transitions with structured attributes such as `handler`, `event`, `outcome`, `reason`, `attempts`, `delivery_count` and the event id. It deduplicates per window: the first record of each (signal, handler, outcome, reason) is logged at once, repeats within the window (`DefaultLogEvery`, 10 s) are counted, and one summary line with the suppressed count closes the window. `WithLogEvery(0)` turns deduplication off. Breaker transitions and retry attempts are never deduplicated, and metrics and traces stay exact. The direct `slog` calls of the transports became `InternalError` signals.

### Fixed

#### Core (`github.com/isaquesb/go-event-bus`)
- A panic in a handler no longer crashes the process on NATS and JetStream, and `LocalBus` recovers it too; it is reported as a terminal failure (`ErrHandlerPanic`)
- `LocalBus.Close` is no longer a no-op: it stops accepting emits and waits, bounded, for in-flight handlers, without deadlocking handlers that emit
- `HandlerDone` reports the real outcome of a message lost on its final delivery to flow control (`circuit_open`, `rate_limited`, `in_progress`): `dropped` or `dead_lettered`, the same outcome `Observer.Failure` reports, so `eventbus_handler_total` counts the loss
- `RetryAttemptInfo.At` is the time the attempt failed, as documented, not the time it started
- The `SlogObserver` the bus creates when `Observer` is nil is flushed at the end of `Shutdown`, so the pending dedup summary with its suppressed count is logged

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- The DLQ invoker dead-letters: in the order documented for v0.3.0 the DLQ sat inside Retry and the circuit breaker and never saw the terminal failure
- When the circuit breaker opens in the middle of a retry run, the attempts that ran and their history are kept in the failure (and in the DLQ record) instead of being lost
- The breaker-state gauge follows a breaker rebuilt with the same handler name: order is kept per breaker (`BreakerTransition.BreakerID`), not per handler name
- Idempotency keeps the start time of an execution in the processing marker and in the final record (a fresh key stored the zero time), and a message that is still processing is no longer acknowledged as a duplicate: it returns `ErrInProgress` and is retried

#### JSON Serialization (`github.com/isaquesb/go-event-bus/json`)
- `Registry.Decode` returns a typed `*event.DecodeError` instead of panicking on an unknown event name or on an envelope with version 0 or no version (version 0 is read as version 1)
- The consumer honors the producer's sampling decision (see the Breaking entry under Changed)

#### JetStream Transport (`github.com/isaquesb/go-event-bus/jetstream`)
- A message that fails to decode is dead-lettered and reported instead of being silently terminated, and the context `Registry.Decode` returns is kept, so the failure carries the event id, correlation id and trace context (`Eventbus-Dlq-Event-Id`, `Eventbus-Dlq-Event-Name`, `Eventbus-Dlq-Traceparent`)
- A message that exhausts `MaxDeliver` is dead-lettered instead of lost without a trace
- `Replay` reports publish errors instead of ignoring them, and continues where it stopped through a durable consumer instead of re-replaying everything
- Draining the consumer on `Close` is bounded, and drain and unsubscribe errors are reported as `InternalError` signals (`OpDrain`, `OpUnsubscribe`) instead of direct log calls

#### NATS Transport (`github.com/isaquesb/go-event-bus/nats`)
- Decode failures keep the context returned by `Registry.Decode`, as on JetStream, and a failed `SubscribeSync` request gets an `Eventbus-Error` reply instead of a timeout
- Drain and unsubscribe errors are reported as `InternalError` signals instead of direct log calls
- `Close` waits for in-flight handlers

### Removed

#### Core (`github.com/isaquesb/go-event-bus`)
- `LocalBusOptions.OnErr`: use `LocalBusOptions.Observer` and `Observer.Failure`

#### Invoker Chain (`github.com/isaquesb/go-event-bus/invoker`)
- `NewMetrics` and the `Metrics` invoker: latency and outcome come from `Observer.HandlerDone`
- `MetricProvider`, `NoopProvider` (with `IncCounter`, `ObserveHistogram` and `SetGauge`) and `Labels`: implement `event.Observer` (embed `event.NopObserver`) instead
- The `metrics MetricProvider` parameter of `NewRetry`, `NewCircuitBreaker`, `NewRateLimiter`, `NewIdempotency` and `NewDLQ`
- `DLQPublisher.Publish(ctx, evt, cause)`: the method is now `Publish(ctx, rec event.DLQRecord)` (`invoker.DLQPublisher` is an alias of `event.DLQPublisher`)
- `IdempotencyConfig.KeyFunc`: the invoker never read it. Delete the assignment; the key comes from the event's `IdempotencyKey()` (`WithIdempotencyKey`) or its id (`WithId`), as before

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

[0.4.0]: https://github.com/isaquesb/go-event-bus/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/isaquesb/go-event-bus/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/isaquesb/go-event-bus/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/isaquesb/go-event-bus/releases/tag/v0.1.0
