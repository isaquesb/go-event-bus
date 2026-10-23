---
layout: default
title: API Reference
nav_order: 11
permalink: /api-reference/
---

# API Reference

Full API documentation is available on pkg.go.dev. This page is an index: every exported function and type of the eight library packages, grouped by theme, with a link to its documentation.

## Packages

| Package | Description | Link |
|---|---|---|
| `event` | Core interfaces (Event, Bus, Invoker, Registry, Observer), LocalBus, the failure model and the DLQ record | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus) |
| `invoker` | Execution pipeline components: chain, retry, circuit breaker, rate limiter, idempotency, DLQ | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker) |
| `json` | JSON serialization with envelope format | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json) |
| `jetstream` | NATS JetStream transport, DLQ publisher and replayer | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream) |
| `nats` | NATS Core transport | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats) |
| `redis` | Redis-backed idempotency and rate-limit stores | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis) |
| `telemetry` | OpenTelemetry: tracing, span events and OTel metrics | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry) |
| `prometheus` | `Observer` with a fixed Prometheus metric set | [pkg.go.dev](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus) |

The root package is imported as `event`. Every name removed or renamed in v0.4.0 is listed in the [CHANGELOG](https://github.com/isaquesb/go-event-bus/blob/main/CHANGELOG.md), which is the migration guide.

## Core (`event`)

### Events, buses and subscribers

- [`Event`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Event) - Base event interface: `Name() string`
- [`WithId`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithId) - Optional interface: an event chooses its own message id (`Id() string`)
- [`WithVersion`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithVersion) - Optional interface: the schema version of an event (`Version() int`)
- [`OnErr`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#OnErr) - Optional interface: the event's own failure callback, called after `Observer.Failure`
- [`Bus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Bus) - Bus interface (Subscribe, Close)
- [`SyncBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SyncBus) - Synchronous dispatch interface (`EmitSync` returns `(Event, error)`)
- [`Shutdowner`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Shutdowner) - Optional bus interface: graceful `Shutdown(ctx)` that drains in-flight handlers
- [`RegisterSubscribers`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#RegisterSubscribers) - Bulk registration of `Subscriber` values on a bus
- [`Subscriber`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Subscriber) - Grouped handler interface
- [`SubscribeOptionsProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOptionsProvider) - Optional interface for subscriber-level subscribe options
- [`HandleFn`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#HandleFn) - Handler function type, `func(context.Context, Event) error`
- [`HandlerFunc`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#HandlerFunc) - Generic typed handler, `func(ctx, E) error`
- [`Invoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Invoker) - Middleware interface
- [`LocalBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#LocalBus) - In-process bus
- [`NewLocalBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewLocalBus) - Creates a `LocalBus` from its options
- [`LocalBusOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#LocalBusOptions) - LocalBus configuration: Invoker, Observer, DLQPublisher, MaxConcurrent, Overflow and more
- [`OverflowPolicy`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#OverflowPolicy) - What LocalBus does with an external emit when every slot is busy and the queue is full
- [`Lifecycle`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Lifecycle) - Shutdown state machine shared by every bus: admission, in-flight accounting, bounded drain

### Emit and subscribe options

- [`EmitOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EmitOption) - Functional option for Emit
- [`EmitOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EmitOptions) - Resolved emit configuration
- [`ApplyEmitOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ApplyEmitOptions) - Applies emit options for an event; the subject defaults to `evt.Name()`
- [`WithSubject`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithSubject) - Overrides the publish subject (defaults to `evt.Name()`)
- [`SubscribeOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOption) - Functional option for Subscribe
- [`SubscribeOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SubscribeOptions) - Resolved subscribe configuration
- [`ApplyOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ApplyOptions) - Applies subscribe options
- [`WithHandlerName`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithHandlerName) - Sets handler name for metrics/logging
- [`WithStream`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithStream) - JetStream stream name (required for JetStream)
- [`WithConsumer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithConsumer) - JetStream durable consumer name
- [`WithMaxDeliver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithMaxDeliver) - Limits the number of redelivery attempts per message
- [`WithBackOff`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithBackOff) - Sets the delays between redelivery attempts

### Registry, envelope and decode

- [`Registry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Registry) - Serialization interface
- [`Envelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Envelope) - Wire format
- [`Factory`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Factory) - Creates a new empty event instance for the registry
- [`Upcaster`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Upcaster) - Migrates a payload from one schema version to the next
- [`EnvelopeEncoder`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EnvelopeEncoder) - Optional registry interface: `EncodeWith` encodes with already resolved `Metadata`
- [`EncodeEvent`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EncodeEvent) - Encodes with `EncodeWith` when the registry implements `EnvelopeEncoder`, else `Encode`
- [`DecodeError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DecodeError) - Wraps a decode failure with the event name and version; `errors.Is` and `errors.As` both work
- [`WrapDecodeError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WrapDecodeError) - Wraps an error as a `*DecodeError` unless it already is one

### Context and metadata

- [`Metadata`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Metadata) - Identity, causal lineage and headers of one event, read by handlers
- [`WithMetadata`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithMetadata) - Returns a context that carries a `Metadata`
- [`MetadataFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#MetadataFromContext) - Reads the `Metadata` of the event being handled from the handler context
- [`ResolveOutbound`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ResolveOutbound) - Resolves the `Metadata` of an event about to be emitted (id, correlation, causation, headers)
- [`NewID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewID) - Generates a UUIDv7 message id
- [`WithID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithID) - Sets the message id of an emitted event
- [`WithCorrelationID`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithCorrelationID) - Sets the correlation id of an emitted event
- [`WithHeader`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithHeader) - Attaches a user header to an emitted event (repeatable)
- [`Delivery`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Delivery) - Count, maximum, subject, raw bytes, headers and stream position of one delivery attempt, carried in the handler context
- [`WithDelivery`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithDelivery) - Returns a context that carries a `Delivery`
- [`DeliveryFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DeliveryFromContext) - Reads the `Delivery` of the message being handled
- [`Execution`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Execution) - The handler run in progress: handler name and start time
- [`WithExecution`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithExecution) - Returns a context that carries an `Execution`
- [`ExecutionFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ExecutionFromContext) - Reads the `Execution` of the handler being run

### Failure semantics and DLQ

- [`InvokeHandler`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#InvokeHandler) - The single handler entry point of every transport: runs the chain, recovers panics, emits `HandlerDone`
- [`Settle`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Settle) - The one post-chain policy shared by every transport: dispose, dead-letter backstop, report once
- [`SettleInput`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SettleInput) - What `Settle` needs to decide the fate of one handled message
- [`Settlement`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Settlement) - The decision `Settle` reached
- [`Dispose`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Dispose) - Maps a handler chain error to a `Disposition` for a given `Delivery`
- [`Disposition`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Disposition) - What a transport does with a message: Ack, Retry, RetryDelayed or Terminal
- [`Outcome`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Outcome) - Terminal result `Settle` reports; values are metric label values and never change
- [`OutcomeOf`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#OutcomeOf) - Maps a handler chain error to the `HandlerDone` outcome
- [`ReasonOf`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ReasonOf) - Returns the stable reason code for an error
- [`FailureError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailureError) - Failure of a handler after the in-process retry policy: attempts and history
- [`AttemptsOf`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#AttemptsOf) - How many in-process handler attempts led to an error
- [`Attempt`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Attempt) - One in-process handler attempt of a delivery
- [`NewAttempt`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewAttempt) - Builds an `Attempt`
- [`AttemptClass`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#AttemptClass) - Class of an attempt error: transient, terminal, timeout or panic (closed set)
- [`AttemptClassOf`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#AttemptClassOf) - Classifies the error of one attempt
- [`FailingInvoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailingInvoker) - Names the part of the pipeline that produced a terminal failure (closed set)
- [`FailingInvokerOf`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailingInvokerOf) - Names the part of the pipeline that produced an error
- [`PanicError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#PanicError) - A panic recovered from a handler or an invoker
- [`HandlerError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#HandlerError) - Identifies the failing handler inside the error `LocalBus.EmitSync` returns
- [`DeadLetteredError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DeadLetteredError) - Returned by the DLQ invoker once the message was dead-lettered
- [`DeadLetterFailedError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DeadLetterFailedError) - Returned when publishing to the dead-letter queue failed
- [`IsInterrupted`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#IsInterrupted) - Reports whether an error is the result of the subscription context ending
- [`WrapInterrupted`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WrapInterrupted) - Marks a handler error caused by a shutdown
- [`DLQPublisher`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DLQPublisher) - Publishes a failed message to a dead-letter queue
- [`DLQRecord`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DLQRecord) - Everything a publisher needs to dead-letter one message: original bytes, headers, cause
- [`NewDLQRecord`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewDLQRecord) - Builds the `DLQRecord` for a message that failed with a cause
- [`PublishDeadLetter`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#PublishDeadLetter) - Publishes a record through a publisher and reports `DeadLettered` or `DLQPublishFailed`
- [`MemoryDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#MemoryDLQ) - In-memory `DLQPublisher` for tests and examples
- [`NewMemoryDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewMemoryDLQ) - Returns an empty `MemoryDLQ`
- [`EncodeHistoryHeader`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#EncodeHistoryHeader) - Encodes the retry history as the `Eventbus-Dlq-History` header value
- [`TruncateHeader`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#TruncateHeader) - Makes a string safe to use as a header value of bounded size

### Observability

- [`Observer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#Observer) - The ten typed signals through which invokers and transports report; one per bus
- [`NopObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NopObserver) - Observer whose methods do nothing; embed it to implement only some signals
- [`MultiObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#MultiObserver) - Calls several observers in order and isolates a panic in any of them
- [`WithObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithObserver) - Returns a context that carries an `Observer`
- [`ObserverFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ObserverFromContext) - Reads the `Observer` of the bus from the handler context
- [`SlogObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SlogObserver) - Observer that writes structured `log/slog` records with deduplication; the default
- [`NewSlogObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#NewSlogObserver) - Creates a `SlogObserver`; a nil logger uses `slog.Default()`
- [`SlogOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#SlogOption) - Option for `NewSlogObserver`
- [`WithLogEvery`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithLogEvery) - Sets the log deduplication window
- [`FailureContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailureContext) - Payload of `Observer.Failure`: outcome, reason, attempts, delivery, invoker, breaker state
- [`HandlerResult`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#HandlerResult) - Payload of `Observer.HandlerDone`: outcome, duration and error of one invocation
- [`RetryAttemptInfo`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#RetryAttemptInfo) - Payload of `Observer.RetryAttempt`
- [`DeadLetterInfo`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DeadLetterInfo) - Payload of `Observer.DeadLettered`: a summary, never the record
- [`DecodeFailure`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DecodeFailure) - Payload of `Observer.DecodeFailed`
- [`DLQPublishFailure`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#DLQPublishFailure) - Payload of `Observer.DLQPublishFailed`
- [`StoreErrorInfo`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#StoreErrorInfo) - Payload of `Observer.StoreError`
- [`IdempotencyStaleInfo`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#IdempotencyStaleInfo) - Payload of `Observer.IdempotencyStale`
- [`InternalErrorInfo`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#InternalErrorInfo) - Payload of `Observer.InternalError`
- [`BreakerTransition`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#BreakerTransition) - Payload of `Observer.BreakerTransition`: one circuit breaker state change
- [`BreakerState`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#BreakerState) - State of a circuit breaker: closed, open or half_open
- [`BreakerReason`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#BreakerReason) - Why a circuit breaker changed state (closed set)
- [`FailureScope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailureScope) - Per-delivery recorder that makes `Observer.Failure` fire exactly once
- [`WithFailureScope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#WithFailureScope) - Installs a fresh `FailureScope` in a context
- [`FailureScopeFromContext`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#FailureScopeFromContext) - Reads the `FailureScope` of a context

Constant groups, by their first name: `OutcomeOK` (`Outcome` values), `ReasonTerminal` (`Reason` codes), `ComponentTransport` (`InternalError` components), `OpConsume` (`InternalError` operations), `DispositionAck` (`Disposition` values), `BreakerClosed` and `BreakerReasonFailureThreshold` (breaker values), `AttemptTransient` (`AttemptClass` values), `InvokerRetry` (`FailingInvoker` values), `OverflowBlock` (`OverflowPolicy` values), `HeaderDLQPrefix` and `HeaderDLQTraceparent` (dead-letter and replay header names), and `DefaultMaxConcurrent` (LocalBus and shutdown defaults).

### Error values

- [`ErrUnknownEvent`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrUnknownEvent) - Decode: the envelope names an event that is not registered
- [`ErrMalformedEnvelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrMalformedEnvelope) - Decode: undecodable bytes, missing name, negative version or a payload that does not fit the event
- [`ErrMissingUpcaster`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrMissingUpcaster) - Decode: no upcaster for an intermediate version
- [`ErrUnknownVersion`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrUnknownVersion) - Decode: the envelope version is newer than the latest registered version
- [`ErrDuplicate`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrDuplicate) - Duplicate detection; the transport acknowledges it
- [`ErrCircuitOpen`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrCircuitOpen) - Circuit breaker open; the handler did not run
- [`ErrRateLimited`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrRateLimited) - Rate limit exceeded; the handler did not run
- [`ErrInProgress`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrInProgress) - Another execution of the same event and handler is in progress
- [`ErrSendToDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrSendToDLQ) - Terminal error signal
- [`ErrRetriesExhausted`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrRetriesExhausted) - The in-process retry policy gave up
- [`ErrDeadLettered`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrDeadLettered) - The message was published to the dead-letter queue
- [`ErrDeadLetterFailed`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrDeadLetterFailed) - Publishing to the dead-letter queue failed
- [`ErrHandlerPanic`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrHandlerPanic) - A handler or invoker panicked; matches `PanicError`
- [`ErrHandlerTimeout`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrHandlerTimeout) - The handler timeout elapsed while the subscription context was still alive
- [`ErrBusClosed`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrBusClosed) - Emit or Subscribe after shutdown started
- [`ErrShutdownInterrupted`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrShutdownInterrupted) - A handler was interrupted by a forced shutdown
- [`ErrQueueFull`](https://pkg.go.dev/github.com/isaquesb/go-event-bus#ErrQueueFull) - LocalBus dropped an emit because the queue was full (`OverflowDrop`)

### JSON Registry

- [`Registry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#Registry) - JSON registry: encodes, decodes and upcasts envelopes
- [`NewRegistry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#NewRegistry) - Returns an empty `Registry`; options are optional
- [`Option`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#Option) - Functional option for `json.NewRegistry`
- [`WithPropagator`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#WithPropagator) - Replaces the default W3C trace context and baggage propagator
- [`WithIDGenerator`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#WithIDGenerator) - Sets the id generator used for events without an explicit id
- [`EncodeWith`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/json#Registry.EncodeWith) - Encodes an event with explicit `Metadata`

### Invoker

The names below also have error values re-exported from the root package: `ErrDuplicate`, `ErrRateLimited`, `ErrCircuitOpen` and `ErrSendToDLQ`.

- [`Chain`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Chain) - Invoker chain composition
- [`NewChain`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewChain) - Builds a `Chain` from invokers, outermost first
- [`Retry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Retry) - Retry with backoff
- [`NewRetry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewRetry) - Creates the `Retry` invoker from a policy
- [`RetryPolicy`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RetryPolicy) - Maximum attempts, base and maximum delay, and history size of `Retry`
- [`CircuitBreaker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#CircuitBreaker) - Per-handler circuit breaker
- [`NewCircuitBreaker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewCircuitBreaker) - Creates the `CircuitBreaker` invoker
- [`CircuitBreakerConfig`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#CircuitBreakerConfig) - Failure and success thresholds, open timeout and the `OnStateChange` callback of the breaker
- [`RateLimiter`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RateLimiter) - Rate limiting
- [`NewRateLimiter`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewRateLimiter) - Creates the `RateLimiter` invoker over a store
- [`RateLimitConfig`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RateLimitConfig) - Rate, period, burst and key prefix
- [`RateLimitStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RateLimitStore) - Token-bucket store interface
- [`Idempotency`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Idempotency) - Duplicate detection
- [`NewIdempotency`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewIdempotency) - Creates the `Idempotency` invoker over a store
- [`IdempotencyConfig`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#IdempotencyConfig) - Processing TTL of the invoker (`KeyFunc` is declared but has no effect)
- [`IdempotencyStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#IdempotencyStore) - Idempotency record store interface
- [`IdempotencyRecord`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#IdempotencyRecord) - Status and times of one execution record
- [`IdempotencyStatus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#IdempotencyStatus) - Status of a record: processing, completed, failed or dead_lettered
- [`MemoryIdempotencyStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#MemoryIdempotencyStore) - In-memory idempotency store
- [`NewMemoryIdempotencyStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewMemoryIdempotencyStore) - Creates a `MemoryIdempotencyStore`
- [`DLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#DLQ) - Dead letter queue invoker
- [`NewDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#NewDLQ) - Creates the `DLQ` invoker over a `DLQPublisher`
- [`DLQPublisher`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#DLQPublisher) - Alias of `event.DLQPublisher`
- [`FailureError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#FailureError) - Alias of `event.FailureError`
- [`WithIdempotencyKey`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#WithIdempotencyKey) - Optional event interface: the idempotency key
- [`WithRateLimitKey`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#WithRateLimitKey) - Optional event interface: the rate limit key
- [`PermanentError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#PermanentError) - Terminal error
- [`RetryableError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#RetryableError) - Retryable error
- [`Retryable`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Retryable) - Optional error interface: `Retryable() bool`, false means never retry
- [`Terminal`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/invoker#Terminal) - Optional error interface: `Terminal() bool`, true means never retry

### NATS Core

- [`Bus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats#Bus) - NATS Core bus
- [`NewNatsBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats#NewNatsBus) - Creates a `Bus` over a NATS connection and a registry
- [`BusOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats#BusOptions) - Invoker, Observer, DLQPublisher, `ReplyErrorMessage`, request timeout and shutdown options
- [`RemoteError`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/nats#RemoteError) - What `EmitSync` returns when the responder replied with an `Eventbus-Error` header

### JetStream

- [`Bus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#Bus) - JetStream bus
- [`NewBus`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#NewBus) - Creates a `Bus` over a JetStream backend and a registry
- [`BusOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#BusOptions) - Invoker, Observer, DLQPublisher, publish retry, redelivery delays and shutdown options
- [`PublishRetryPolicy`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#PublishRetryPolicy) - Bounds the internal retry of transient publish failures
- [`DLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#DLQ) - Publishes dead-lettered messages to JetStream with the original bytes
- [`NewDLQ`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#NewDLQ) - Creates a `DLQ` for a prefix (default `dlq.`)
- [`DLQOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#DLQOption) - Option for `NewDLQ`
- [`WithDLQRegistry`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#WithDLQRegistry) - Sets the registry that encodes a record with no original bytes
- [`Replayer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#Replayer) - Republishes dead-lettered messages from a DLQ stream
- [`NewReplayer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#NewReplayer) - Creates a `Replayer`
- [`ReplayerOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ReplayerOption) - Option for `NewReplayer`
- [`WithReplayTracerProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#WithReplayTracerProvider) - Sets the provider of the replay spans
- [`ReplayOptions`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ReplayOptions) - What `Replay` reads and where it publishes
- [`ReplayResult`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ReplayResult) - Counts the records handled by one `Replay` call
- [`ErrStreamRequired`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrStreamRequired) - `Subscribe` called without `WithStream`
- [`ErrInvalidConfig`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrInvalidConfig) - Invalid consumer configuration (for example `MaxDeliver` inconsistent with the backoff length)
- [`ErrNoEnvelope`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrNoEnvelope) - A record with neither original bytes nor a registry cannot be encoded
- [`ErrReplaySubject`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/jetstream#ErrReplaySubject) - A replay record has no valid target subject

### Redis

- [`IdempotencyStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis#IdempotencyStore) - Redis-backed `invoker.IdempotencyStore`
- [`NewIdempotencyStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis#NewIdempotencyStore) - Creates the Redis idempotency store
- [`RateLimitStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis#RateLimitStore) - Redis-backed token bucket
- [`NewRedisRateLimitStore`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/redis#NewRedisRateLimitStore) - Creates the Redis rate limit store

### Telemetry

- [`TracerInvoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#TracerInvoker) - Invoker that creates one consumer span per handler invocation
- [`NewTracerInvoker`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#NewTracerInvoker) - Creates a `TracerInvoker`; it goes first in the chain
- [`TracerOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#TracerOption) - Option for `NewTracerInvoker`
- [`WithTracerProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#WithTracerProvider) - Sets the provider used to build the tracer
- [`WithSpanDestination`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#WithSpanDestination) - Overrides the destination of the consumer span
- [`Observer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#Observer) - `event.Observer` that reports in-chain signals as span events and OpenTelemetry metrics
- [`NewObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#NewObserver) - Creates the telemetry `Observer`
- [`ObserverOption`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#ObserverOption) - Option for `NewObserver`
- [`WithMeterProvider`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/telemetry#WithMeterProvider) - Enables the OpenTelemetry metric set on a meter provider

### Prometheus

- [`Observer`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus#Observer) - `event.Observer` with a fixed set of Prometheus metric families
- [`NewObserver`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus#NewObserver) - Registers the metric set on a registerer and returns the `Observer`
- [`Option`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus#Option) - Option for `NewObserver`
- [`WithNamespace`](https://pkg.go.dev/github.com/isaquesb/go-event-bus/prometheus#WithNamespace) - Prefixes every metric name with a namespace
