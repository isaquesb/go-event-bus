package telemetry

// ScopeName is the instrumentation scope name of every tracer and meter this
// package creates.
//
// The messaging attributes follow the OpenTelemetry messaging semantic
// conventions, which are at Development status in the specification. They are
// pinned to semconv v1.43.0 and may change with future semconv releases.
const ScopeName = "github.com/isaquesb/go-event-bus/telemetry"

// defaultMessagingSystem is the messaging.system value used when the transport
// did not set Delivery.System (LocalBus).
const defaultMessagingSystem = "eventbus"

// Span event names added by Observer to the consumer span.
const (
	// EventRetryAttempt is one failed attempt that the retry policy repeats.
	EventRetryAttempt = "eventbus.retry.attempt"
	// EventBreakerTransition is one circuit breaker state change.
	EventBreakerTransition = "eventbus.breaker.transition"
	// EventDeadLettered is a successful dead-letter publish.
	EventDeadLettered = "eventbus.dead_lettered"
	// EventDLQPublishFailed is a failed dead-letter publish.
	EventDLQPublishFailed = "eventbus.dlq.publish_failed"
	// EventStoreError is a failing idempotency or rate-limit store call.
	EventStoreError = "eventbus.store.error"
	// EventIdempotencyStale is a stale in-progress marker that was taken over.
	EventIdempotencyStale = "eventbus.idempotency.stale"
	// EventInternalError is an error inside the library that no handler sees.
	EventInternalError = "eventbus.internal_error"
)

// Attribute keys set on spans and span events. Metric attributes reuse the
// short names documented on NewObserver.
const (
	// AttrHandler is the handler name.
	AttrHandler = "eventbus.handler"
	// AttrCorrelationID is the event correlation id.
	AttrCorrelationID = "eventbus.correlation_id"
	// AttrDeliveryCount is the 1-based delivery number.
	AttrDeliveryCount = "eventbus.delivery_count"
	// AttrReplayed marks a message that was replayed from the DLQ.
	AttrReplayed = "eventbus.replayed"
	// AttrDLQSequence is the DLQ stream sequence of a replayed record.
	AttrDLQSequence = "eventbus.dlq.sequence"
	// AttrLinkType tells what a span link points at: dead_letter or replay.
	AttrLinkType = "eventbus.link.type"
	// AttrRetryAttempt is the number of the attempt that failed.
	AttrRetryAttempt = "eventbus.retry.attempt"
	// AttrRetryClass is the AttemptClass of the failed attempt.
	AttrRetryClass = "eventbus.retry.class"
	// AttrRetryDelayMs is the wait before the next attempt in milliseconds.
	AttrRetryDelayMs = "eventbus.retry.delay_ms"
	// AttrBreakerFrom is the breaker state before a transition.
	AttrBreakerFrom = "eventbus.breaker.from"
	// AttrBreakerTo is the breaker state after a transition.
	AttrBreakerTo = "eventbus.breaker.to"
	// AttrBreakerReason is why the breaker changed state.
	AttrBreakerReason = "eventbus.breaker.reason"
	// AttrBreakerFailures is the consecutive failure count of a transition.
	AttrBreakerFailures = "eventbus.breaker.failures"
	// AttrBreakerSeq is the per-handler transition sequence number.
	AttrBreakerSeq = "eventbus.breaker.seq"
	// AttrReason is a stable reason string.
	AttrReason = "eventbus.reason"
	// AttrStore is the store kind: idempotency or rate_limit.
	AttrStore = "eventbus.store"
	// AttrOp is the operation that failed.
	AttrOp = "eventbus.op"
	// AttrComponent is the library component that reported an internal error.
	AttrComponent = "eventbus.component"
)

// Link types carried by AttrLinkType on replay span links.
const (
	linkTypeDeadLetter = "dead_letter"
	linkTypeReplay     = "replay"
)
