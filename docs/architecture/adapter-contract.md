---
layout: default
title: Adapter Contract
parent: Architecture
nav_order: 5
---

# Adapter Contract

This page is the contract a transport adapter has to meet to be a `Bus` of Go Event Bus. It is written for the SQS, RabbitMQ and Kafka adapters planned for v0.5.0, and it is derived from the three shipped transports: [`jetstream/`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) (the reference implementation), [`nats/`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go) and the [LocalBus](https://github.com/isaquesb/go-event-bus/blob/main/local-bus.go).

The key words MUST, MUST NOT, SHOULD and MAY are used as in RFC 2119. Rules are numbered `AC-01`, `AC-02` and so on, and each one is a single statement that a test can check, with a "How to test" line. A reusable conformance test kit is not part of v0.4.0: it ships with the first v0.5.0 adapter. Until then, an adapter proves each rule with its own tests, using the tests named in the reference links as models.

The rules are broker-agnostic. Where a rule needs a broker action, it speaks of a `Disposition` and the [table below](#disposition-to-broker-action) lists the expected action per broker. For what each error becomes on each shipped transport, see the [Failure Model]({{ site.baseurl }}/architecture/failure-model/).

## Rules

### AC-01: Emit resolves Metadata and uses its id for broker deduplication

**MUST.** `Emit` resolves the outbound metadata with `event.ResolveOutbound`, encodes the event with `event.EncodeEvent`, and, when the broker has a deduplication id, sets it from `Metadata.ID`. The subject defaults to `evt.Name()` and is overridden by `event.WithSubject` (`event.ApplyEmitOptions`).

- **How to test:** emit with `event.WithID("x")` and assert the broker message carries the id `x` in its dedup field, and the envelope carries the same id; emit twice and assert the broker saw one message.
- **Reference:** [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `Emit` (sets `Nats-Msg-Id`), [`metadata.go`](https://github.com/isaquesb/go-event-bus/blob/main/metadata.go) (`ResolveOutbound`, `EncodeEvent`), [`jetstream/dedup_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/dedup_integration_test.go). NATS Core has no deduplication id: [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go).

### AC-02: The handler context derives from the subscription context and carries the bus scope

**MUST.** The context passed to decoding, to the handler and to `event.Settle` derives from the context given to `Subscribe`, never from `context.Background()`, so cancellation and span parenting work. Before `Decode` it MUST carry the bus Observer (`event.WithObserver`), a fresh `FailureScope` for the delivery (`event.WithFailureScope`) and the `Delivery` (`event.WithDelivery`). It MUST carry `event.WithExecution` when the handler runs.

- **How to test:** subscribe with a context that carries a value and is later cancelled; assert the handler sees the value, sees the cancellation, and that `event.ObserverFromContext`, `event.FailureScopeFromContext` and `event.DeliveryFromContext` succeed inside the handler and during `Decode`. Two deliveries MUST get two scopes.
- **Reference:** [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `msgHandler` and `msgScope`, [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go) `Subscribe`, [`local-bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/local-bus.go) `execute`.

### AC-03: Delivery reflects the broker's redelivery state

**MUST.** `Delivery.Count` is the 1-based delivery number and `Delivery.Max` the maximum number of deliveries. A broker that redelivers reports its real count and limit. `Max <= 0` means unlimited and is never final. A broker with no redelivery reports `Count 1, Max 1`, so every delivery is final. `Delivery.System` SHOULD carry the OpenTelemetry `messaging.system` value.

- **How to test:** deliver a message at count 1 with a limit of 3 and assert `Final()` is false; at count 3, true; with a limit of 0, never.
- **Reference:** [`delivery.go`](https://github.com/isaquesb/go-event-bus/blob/main/delivery.go), [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `delivery` (count from `NumDelivered`, limit from `MaxDeliver`), [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go) `deliveryOf` (`1/1`).

### AC-04: A decode failure keeps the Decode context and has no event

**MUST.** When `Registry.Decode` returns an error, the adapter settles the message with a nil event, on the context `Decode` returned (it carries the metadata and trace context once the envelope parsed). If `Decode` returns a nil context, the adapter falls back to the handler context. `SettleInput.SubscriptionCtx` stays the handler context, so interruption detection is unchanged. The handler MUST NOT run.

- **How to test:** use a registry that returns a context with `event.WithMetadata` together with an error; assert `FailureContext.Metadata.ID` is set, the DLQ record has the event id and name, and the handler was not called.
- **Reference:** [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `msgHandler`, [`jetstream/decode_ctx_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/decode_ctx_test.go), [`nats/decode_ctx_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/decode_ctx_test.go). LocalBus does not decode.

### AC-05: The handler runs only through event.InvokeHandler, once per delivery

**MUST.** The adapter calls the handler only through `event.InvokeHandler(ctx, invoker, evt, handlerName, fn)`, exactly once per delivery. `InvokeHandler` installs the panic isolation and emits `HandlerDone`; the adapter MUST NOT emit `HandlerDone` itself, and MUST NOT call the invoker chain or the handler directly.

- **How to test:** subscribe a handler that panics and assert the adapter survives, the failure is a terminal error and one `HandlerDone` was observed; assert the handler count equals the delivery count.
- **Reference:** [`invoke.go`](https://github.com/isaquesb/go-event-bus/blob/main/invoke.go) `InvokeHandler`, [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `msgHandler`, [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go) `Subscribe`, [`local-bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/local-bus.go) `execute`.

### AC-06: Settle is called once per delivery and its Disposition maps to one broker action

**MUST.** The adapter calls `event.Settle` exactly once per handled or undecodable message, on the delivery context, and then performs exactly one broker action for the returned `Disposition`: `Ack` acknowledges; `Retry` redelivers at once and is returned only for an interruption; `RetryDelayed` redelivers after a delay; `Terminal` ends the message. An adapter whose broker has no action to take (LocalBus) MAY skip `Settle` for a nil error, because `Settle` returns `Ack` for it.

- **How to test:** drive one message through each error class of the [Failure Model]({{ site.baseurl }}/architecture/failure-model/) and assert exactly one broker call per message with the action of the table.
- **Reference:** [`settle.go`](https://github.com/isaquesb/go-event-bus/blob/main/settle.go) (`Dispose`, `Settle`, `Settlement`), [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `apply`, [`dispose_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/dispose_test.go) `TestDispose_TruthTable`, [`jetstream/observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/observer_test.go) `TestJetStreamObserver_FailureMatrix`.

### AC-07: The transport DLQ is a backstop and never publishes twice

**MUST.** The adapter passes its `event.DLQPublisher` (nil when none is configured) as `SettleInput.Publisher`. `Settle` publishes through it only when the chain did not already dead-letter the message (`ErrDeadLettered`), so a record is never published twice for one failure. The adapter MUST NOT publish a dead-letter record on its own.

- **How to test:** configure the DLQ invoker in the chain and the publisher on the bus; fail a message terminally and assert one record. Remove the invoker, fail again, and assert one record from the backstop.
- **Reference:** [`settle.go`](https://github.com/isaquesb/go-event-bus/blob/main/settle.go) `Settle`, [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `guardedPublisher`, [`jetstream/failure_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/failure_integration_test.go) `TestIntegration_ChainDeadLetterVerbatimRecord`.

### AC-08: A terminal broker action happens only after a successful dead-letter

**MUST.** The adapter performs the terminal broker action (Term, reject without requeue, offset commit past the message) only when `Settle` returns `Terminal`: after a successful dead-letter, or for a drop when no publisher is configured. When the DLQ publish fails, `Settle` returns `RetryDelayed`, and the adapter MUST redeliver after a delay, on the final delivery too. A message that was dead-lettered or reported MUST NOT be redelivered after the fact.

- **How to test:** make the DLQ publisher fail on the final delivery and assert a delayed redelivery request and no terminal action; make it succeed and assert the terminal action came after the publish.
- **Reference:** [`jetstream/failure_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/failure_integration_test.go) `TestIntegration_TermWithReasonOnlyAfterDLQPublish` and `TestIntegration_DLQPublishFailureNakNeverTerm`, [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `apply` and `settleGuard`.

### AC-09: DLQ records are verbatim, with the failure headers and no broker-internal headers

**MUST.** A record the adapter publishes keeps the original envelope bytes untouched and the original user headers. It adds the headers of `DLQRecord.FailureHeaders()`, MUST strip the broker's own headers and any stale `Eventbus-Dlq-*` headers, and keeps the broker message id under `event.HeaderDLQOriginalMsgID`. Header values MUST be bounded (`event.DLQHeaderValueLimit`), and a publish rejected as too large SHOULD be retried once with the minimal headers. The replay path MUST republish the bytes unchanged.

- **How to test:** dead-letter a message with a user header and a broker id; read the record and assert the body is byte-equal, the user header is present, `Eventbus-Dlq-Reason` and the other failure headers are set, no broker header leaked, and the original id is kept.
- **Reference:** [`dlq.go`](https://github.com/isaquesb/go-event-bus/blob/main/dlq.go) (`DLQRecord`, `FailureHeaders`), [`jetstream/dlq.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/dlq.go) (`Publish`, `dlqCopyHeaders`), [`jetstream/replayer.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/replayer.go), [`jetstream/dlq_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/dlq_test.go).

### AC-10: Observer.Failure fires exactly once per terminal outcome

**MUST.** Each terminal outcome reaches `Observer.Failure` exactly once across redeliveries, and an intermediate retry never does. `event.Settle` is the reporter, with the delivery's `FailureScope` making the report once-only. The adapter MUST NOT call `Observer.Failure` on a path `Settle` handles. The one permitted direct call is a panic outside `Settle` on the final delivery, when `Settle` has not reported the message (the JetStream `reportPanicOnFinal`), and it MUST be guarded so the total stays one.

- **How to test:** run every row of the failure matrix on final and non-final deliveries and count `Failure` calls with a recording Observer: one on the final delivery of a failure, none before it.
- **Reference:** [`settle.go`](https://github.com/isaquesb/go-event-bus/blob/main/settle.go) `reportTerminal`, [`scope.go`](https://github.com/isaquesb/go-event-bus/blob/main/scope.go), [`jetstream/observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/observer_test.go), [`nats/observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/observer_test.go), [`local_bus_observer_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/local_bus_observer_test.go).

### AC-11: Runtime errors outside handlers are InternalError signals, and the default observer is flushed

**MUST.** An error inside the adapter that is not a handler failure (a failing ack, nak, term, a consume error, a drain error, a panic in the callback) is reported as an `Observer.InternalError` signal and never logged directly. A nil `BusOptions.Observer` MUST install `event.NewSlogObserver(nil)`, and the bus Observer MUST be wrapped in `event.MultiObserver` so a panicking observer is isolated. The bus MUST flush the default observer it created at the end of `Shutdown`, and MUST NOT flush an observer the caller supplied.

- **How to test:** make the broker client fail an ack and assert an `InternalError` with the matching `Op` and no log output with `event.NopObserver{}`; shut down a bus built without an Observer after a failure storm and assert the summary record.
- **Reference:** [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `internalError`, `apply`, `Shutdown`; [`jetstream/robust_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/robust_test.go) `TestJetStreamBus_ShutdownFlushesDefaultObserver` and `TestJetStreamBus_ShutdownLeavesUserObserver`; [`nats/robust_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/robust_test.go).

### AC-12: The bus implements Shutdowner with Lifecycle semantics

**MUST.** The bus implements `event.Shutdowner` by embedding `event.Lifecycle`: `Shutdown(ctx)` stops admitting new work, waits for the in-flight deliveries, and, if `ctx` expires first, interrupts them (the handler context is cancelled with `event.ErrShutdownInterrupted`) and returns `ctx.Err()`. A delivery interrupted this way MUST NOT be dead-lettered: `Settle` returns `Retry` and the adapter hands the message back. `Close` MUST be `Shutdown` bounded by a timeout (`event.DefaultShutdownTimeout` unless an option overrides it), and `Shutdown` MUST be safe to call concurrently and repeatedly.

- **How to test:** block a handler, call `Shutdown` with a short deadline, assert it returns `context.DeadlineExceeded` in time, the handler context is cancelled, nothing is in the DLQ and the broker got a redelivery request.
- **Reference:** [`lifecycle.go`](https://github.com/isaquesb/go-event-bus/blob/main/lifecycle.go), [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `Shutdown`, `Close`, `handBack`, [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go), [`jetstream/shutdown_integration_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/shutdown_integration_test.go), [`lifecycle_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/lifecycle_test.go).

### AC-13: Emit and Subscribe after shutdown return ErrBusClosed

**MUST.** Once `Shutdown` or `Close` began, `Emit`, `EmitSync` (where offered), `Subscribe` and `SubscribeSync` return `event.ErrBusClosed`. A handler that emits a child event with its own context while its message is still in flight MAY be admitted, so the drain can finish. A delivery that arrives after shutdown began MUST NOT run the handler.

- **How to test:** call `Shutdown`, then each method, and assert `errors.Is(err, event.ErrBusClosed)`; assert a nested emit from an in-flight handler succeeds.
- **Reference:** [`lifecycle.go`](https://github.com/isaquesb/go-event-bus/blob/main/lifecycle.go) `Admit`, [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go) `Emit` and `Subscribe`, [`local-bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/local-bus.go), [`jetstream/robust_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/robust_test.go) `TestJetStreamBus_NestedEmitDuringDrainAccepted`.

### AC-14: The broker client sits behind a narrow unexported interface

**MUST.** The adapter wraps its broker client in a narrow, unexported interface that lists only the calls it makes, so unit tests run against a mock and need no broker. Tests that need a real broker MUST use the `integration` build tag. The adapter ships in its own Go module and registers no global state.

- **How to test:** the module's unit tests (`go test ./...` without tags) build and pass with no broker running, and the integration tests are excluded from that run.
- **Reference:** `natsConn` in [`nats/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/bus.go); `jsBackend` in [`jetstream/bus.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus.go); `dlqBackend` in [`jetstream/dlq.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/dlq.go); `replayBackend` in [`jetstream/replayer.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/replayer.go); [`jetstream/bus_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/bus_test.go).

### AC-15: Producer and consumer spans follow the OpenTelemetry messaging conventions

**SHOULD.** `Emit` starts a producer span named `publish <subject>` and injects the W3C trace context into the envelope headers. The consumer extracts it and honors the producer's sampled flag (it does not force sampling). Span attributes are ids and counts, never payloads. Span names and attribute keys are frozen once released.

- **How to test:** emit with a recording tracer provider and assert the span name, kind `Producer` and the messaging attributes; deliver a message whose `traceparent` is not sampled and assert the handler span is not sampled.
- **Reference:** [`jetstream/producer_span_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/jetstream/producer_span_test.go), [`nats/producer_span_test.go`](https://github.com/isaquesb/go-event-bus/blob/main/nats/producer_span_test.go), [`json/registry.go`](https://github.com/isaquesb/go-event-bus/blob/main/json/registry.go) `extractTrace`, [`telemetry/semconv.go`](https://github.com/isaquesb/go-event-bus/blob/main/telemetry/semconv.go). See [Tracing]({{ site.baseurl }}/observability/tracing/).

## Disposition to broker action

Label of this table: *expected, to be confirmed in v0.5.0*. It guides the adapters and promises no behavior. Each row is a starting point for the adapter's own design, and v0.5.0 confirms or changes it.

| `event.Disposition` to expected broker action | SQS (expected) | RabbitMQ (expected) | Kafka (expected) |
|---|---|---|---|
| `Ack` | `DeleteMessage` | `basic.ack` | commit the offset |
| `Retry` (interruption only) | `ChangeMessageVisibility` to 0 | `basic.nack` with requeue | do not commit; seek back to the message |
| `RetryDelayed` | `ChangeMessageVisibility` with the delay | `basic.nack` to a delay mechanism (for example a TTL queue) | pause the partition or publish to a retry topic |
| `Terminal` (after the dead-letter, or a drop) | `DeleteMessage` after the dead-letter | `basic.reject` without requeue after the dead-letter | commit the offset after the dead-letter |

For comparison, JetStream maps `Ack` to `Ack`, `Retry` to `Nak`, `RetryDelayed` to `NakWithDelay` and `Terminal` to `TermWithReason`. NATS Core and LocalBus take no broker action: they have no redelivery.

## PR checklist for a new adapter

Copy this list into the pull request of a new adapter and tick it.

- [ ] AC-01: `Emit` resolves metadata and sets the broker dedup id from `Metadata.ID`
- [ ] AC-02: the handler context derives from the subscription context and carries the Observer, `FailureScope` and `Delivery`
- [ ] AC-03: `Delivery.Count` and `Max` reflect the broker (or `1/1`)
- [ ] AC-04: a decode failure settles on the Decode context with a nil event
- [ ] AC-05: the handler runs only through `event.InvokeHandler`, once per delivery
- [ ] AC-06: `event.Settle` is called once per delivery and maps to one broker action
- [ ] AC-07: the bus `DLQPublisher` is passed to `Settle` and nothing is published twice
- [ ] AC-08: the terminal broker action follows a successful dead-letter, and a failed publish ends in a delayed redelivery
- [ ] AC-09: DLQ records are verbatim, with `FailureHeaders`, broker headers stripped and bounded values
- [ ] AC-10: `Observer.Failure` fires once per terminal outcome
- [ ] AC-11: adapter errors are `InternalError` signals and the default observer is flushed
- [ ] AC-12: the bus implements `event.Shutdowner` with `event.Lifecycle` semantics, and `Close` is bounded
- [ ] AC-13: `Emit` and `Subscribe` after shutdown return `event.ErrBusClosed`
- [ ] AC-14: the broker client sits behind a narrow unexported interface, with mock unit tests and `integration`-tagged real-broker tests
- [ ] AC-15: producer and consumer spans follow the messaging conventions and honor the sampled flag
- [ ] The adapter is its own Go module with the `go 1.26.0` directive and its in-repo requires on published versions
- [ ] The release layer is checked with `scripts/release.sh --check-layers`
- [ ] A docs page under `docs/transports/` and an entry in `CHANGELOG.md`
- [ ] A row in the [Failure Model]({{ site.baseurl }}/architecture/failure-model/) for the broker action of every disposition
