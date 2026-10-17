package event

import (
	"context"
	"time"
)

// BreakerState is the state of a circuit breaker as reported in signals. The
// set is closed, so backends may use it as a label. The zero value "" means no
// circuit breaker ran for the message.
type BreakerState string

const (
	// BreakerClosed means calls flow through the breaker.
	BreakerClosed BreakerState = "closed"
	// BreakerOpen means the breaker rejects calls with ErrCircuitOpen.
	BreakerOpen BreakerState = "open"
	// BreakerHalfOpen means the breaker admits probe calls.
	BreakerHalfOpen BreakerState = "half_open"
)

// BreakerReason is why a circuit breaker changed state. The set is closed.
type BreakerReason string

const (
	// BreakerReasonFailureThreshold means the consecutive failure threshold was
	// reached.
	BreakerReasonFailureThreshold BreakerReason = "failure_threshold"
	// BreakerReasonOpenTimeoutElapsed means the open timeout elapsed and the
	// first probe was admitted.
	BreakerReasonOpenTimeoutElapsed BreakerReason = "open_timeout_elapsed"
	// BreakerReasonProbeSucceeded means a half-open probe succeeded.
	BreakerReasonProbeSucceeded BreakerReason = "probe_succeeded"
	// BreakerReasonProbeFailed means a half-open probe failed.
	BreakerReasonProbeFailed BreakerReason = "probe_failed"
)

// BreakerTransition is one circuit breaker state change.
//
// It is delivered synchronously after the breaker lock is released, on the
// goroutine of the message that caused it. Observers must not block and must
// not assume an order across goroutines: sort by Seq, which increases per
// handler breaker. The open to half_open transition is emitted lazily, when the
// first probe is admitted after OpenTimeout.
type BreakerTransition struct {
	// Handler is the handler name the breaker protects.
	Handler string
	// From is the state before the change.
	From BreakerState
	// To is the state after the change.
	To BreakerState
	// Reason is why the state changed.
	Reason BreakerReason
	// Failures is the consecutive failure count when the change happened.
	Failures int
	// At is when the change happened.
	At time.Time
	// Seq increases by one on every transition of the same handler breaker.
	Seq uint64
}

// FailingInvoker names the part of the pipeline that produced a terminal
// failure. The set is closed.
type FailingInvoker string

const (
	// InvokerRetry means the in-process retry policy gave up or failed
	// permanently.
	InvokerRetry FailingInvoker = "retry"
	// InvokerCircuitBreaker means the circuit breaker rejected the call.
	InvokerCircuitBreaker FailingInvoker = "circuit_breaker"
	// InvokerDLQ means the dead-letter step ended the message.
	InvokerDLQ FailingInvoker = "dlq"
	// InvokerTransport means the transport itself ended the message.
	InvokerTransport FailingInvoker = "transport"
)

// AttemptClass classifies the error of one handler attempt. The set is closed.
type AttemptClass string

const (
	// AttemptTransient means the error may succeed on a later attempt.
	AttemptTransient AttemptClass = "transient"
	// AttemptTerminal means retrying cannot help.
	AttemptTerminal AttemptClass = "terminal"
	// AttemptTimeout means the handler hit its timeout.
	AttemptTimeout AttemptClass = "timeout"
	// AttemptPanic means the handler panicked.
	AttemptPanic AttemptClass = "panic"
)

// Attempt is one in-process handler attempt of a delivery. Err is the
// truncated error text, and Delay is the backoff waited after the attempt, zero
// for the last one.
type Attempt struct {
	// N is the 1-based attempt number.
	N int
	// Err is the error text, truncated to AttemptErrLimit bytes.
	Err string
	// Class classifies the error.
	Class AttemptClass
	// Started is when the attempt began.
	Started time.Time
	// Duration is how long the attempt ran.
	Duration time.Duration
	// Delay is the backoff waited after this attempt.
	Delay time.Duration
}

// New Outcome values, next to the ones in settle.go. Together they are the full
// bounded vocabulary, all snake_case and never changed once released:
//
//   - HandlerDone uses ok, retry, dead_lettered, dropped, duplicate,
//     in_progress, rate_limited, circuit_open, interrupted and failed.
//   - Failure uses dead_lettered, dropped, interrupted, decode_failed,
//     dlq_publish_failed and rejected.
//
// dropped also covers a message that exhausted its retries with no DLQ
// configured, because Settle reports that case as dropped.
const (
	// OutcomeOK means the handler chain succeeded.
	OutcomeOK Outcome = "ok"
	// OutcomeRetry means the delivery failed and the transport will redeliver.
	OutcomeRetry Outcome = "retry"
	// OutcomeDuplicate means the idempotency store already completed the
	// execution.
	OutcomeDuplicate Outcome = "duplicate"
	// OutcomeInProgress means another execution of the same event is running.
	OutcomeInProgress Outcome = "in_progress"
	// OutcomeRateLimited means the rate limiter rejected the call.
	OutcomeRateLimited Outcome = "rate_limited"
	// OutcomeCircuitOpen means the circuit breaker rejected the call.
	OutcomeCircuitOpen Outcome = "circuit_open"
	// OutcomeFailed means the chain ended in a terminal failure that was not
	// dead-lettered.
	OutcomeFailed Outcome = "failed"
	// OutcomeDecodeFailed means the message could not be decoded, so no handler
	// ran.
	OutcomeDecodeFailed Outcome = "decode_failed"
)

// RetryAttemptInfo is the RetryAttempt signal: one handler attempt failed and
// the retry policy will wait before the next one.
type RetryAttemptInfo struct {
	// Handler is the handler name.
	Handler string
	// Event is the event name.
	Event string
	// Attempt is the number of the attempt that failed.
	Attempt int
	// Class classifies the failure.
	Class AttemptClass
	// Err is the attempt error.
	Err error
	// Delay is the wait before the next attempt.
	Delay time.Duration
	// At is when the attempt failed.
	At time.Time
}

// HandlerResult is the HandlerDone signal: one invocation of a handler chain
// ended.
type HandlerResult struct {
	// Handler is the handler name.
	Handler string
	// Event is the event name.
	Event string
	// Outcome is the bounded result, see OutcomeOf.
	Outcome Outcome
	// Duration is how long the invocation ran.
	Duration time.Duration
	// Err is the chain error, nil on success.
	Err error
	// At is when the invocation ended.
	At time.Time
}

// FailureContext is the Failure signal and the payload of an OnFailure hook:
// the message ended and no redelivery follows. Fields are never removed.
type FailureContext struct {
	// Handler is the failing handler, empty on a decode failure.
	Handler string
	// Event is the decoded event, nil on a decode failure.
	Event Event
	// Metadata is the identity of the event, zero when unknown.
	Metadata Metadata
	// Outcome is how the message ended.
	Outcome Outcome
	// Reason is the stable reason, see ReasonOf.
	Reason string
	// Cause is the reported error. errors.Is reaches the original cause, and
	// ErrDeadLettered when the message was dead-lettered.
	Cause error
	// Invoker names the part of the pipeline that ended the message.
	Invoker FailingInvoker
	// Attempts is the number of in-process attempts made on this delivery.
	Attempts int
	// Delivery describes the delivery. Raw is always nil.
	Delivery Delivery
	// Breaker is the circuit breaker state, empty when none ran.
	Breaker BreakerState
	// History holds the last in-process attempts, oldest first.
	History []Attempt
	// At is when the message ended.
	At time.Time
}

// DeadLetterInfo is the DeadLettered signal. It is a summary and never the
// DLQRecord, so no payload reaches an observer.
type DeadLetterInfo struct {
	// Handler is the failing handler.
	Handler string
	// Event is the event name.
	Event string
	// Reason is the stable dead-letter reason.
	Reason string
	// Subject is the subject the message was received on.
	Subject string
	// Attempts is the number of in-process attempts made.
	Attempts int
	// Delivery describes the delivery. Raw is always nil.
	Delivery Delivery
	// Size is the byte size of the stored envelope, 0 when none.
	Size int
	// At is when the message was dead-lettered.
	At time.Time
}

// DecodeFailure is the DecodeFailed signal: a message could not be decoded.
type DecodeFailure struct {
	// Handler is the handler the message was bound for, empty when unknown.
	Handler string
	// Subject is the subject the message was received on.
	Subject string
	// Reason is the stable decode reason.
	Reason string
	// Err is the decode error.
	Err error
	// Delivery describes the delivery. Raw is always nil.
	Delivery Delivery
	// Final reports that no redelivery follows.
	Final bool
}

// DLQPublishFailure is the DLQPublishFailed signal: publishing to the
// dead-letter queue failed.
type DLQPublishFailure struct {
	// Handler is the failing handler.
	Handler string
	// Event is the event name.
	Event string
	// Reason is the stable dead-letter reason.
	Reason string
	// Subject is the subject the message was received on.
	Subject string
	// Attempts is the number of in-process attempts made.
	Attempts int
	// Err is the publish error.
	Err error
	// Delivery describes the delivery. Raw is always nil.
	Delivery Delivery
	// Final reports that no redelivery follows, so the message is at risk.
	Final bool
}

// StoreErrorInfo is the StoreError signal: an idempotency or rate-limit store
// call failed.
type StoreErrorInfo struct {
	// Handler is the handler name.
	Handler string
	// Event is the event name.
	Event string
	// Store is "idempotency" or "rate_limit".
	Store string
	// Op is the store operation that failed.
	Op string
	// Err is the store error.
	Err error
}

// IdempotencyStaleInfo is the IdempotencyStale signal: a stale in-progress
// marker was taken over.
type IdempotencyStaleInfo struct {
	// Handler is the handler name.
	Handler string
	// Event is the event name.
	Event string
	// Age is how old the stale marker was.
	Age time.Duration
}

// InternalErrorInfo is the InternalError signal. It replaces the per-message
// direct log calls in transports. Component and Op are closed sets, so
// backends may use them as labels.
type InternalErrorInfo struct {
	// Component is one of the Component constants.
	Component string
	// Op is one of the Op constants.
	Op string
	// Handler is the handler name, empty when unknown.
	Handler string
	// Subject is the subject involved, empty when unknown.
	Subject string
	// Err is the internal error.
	Err error
}

// InternalErrorInfo.Component values.
const (
	// ComponentTransport is an error inside a transport adapter.
	ComponentTransport = "transport"
	// ComponentCircuitBreaker is an error inside the circuit breaker.
	ComponentCircuitBreaker = "circuit_breaker"
	// ComponentEvent is an error inside the root module.
	ComponentEvent = "event"
)

// InternalErrorInfo.Op values.
const (
	// OpConsume is receiving or decoding a message.
	OpConsume = "consume"
	// OpAck is acknowledging a message.
	OpAck = "ack"
	// OpNak is negatively acknowledging a message.
	OpNak = "nak"
	// OpTerm is terminating a message.
	OpTerm = "term"
	// OpReply is sending a reply.
	OpReply = "reply"
	// OpEncodeReply is encoding a reply.
	OpEncodeReply = "encode_reply"
	// OpPanic is a recovered panic outside a handler.
	OpPanic = "panic"
)

// redactDelivery returns a copy of d with Raw set to nil. Every root builder of
// a signal uses it, so no payload bytes reach an observer.
func redactDelivery(d Delivery) Delivery {
	d.Raw = nil
	return d
}

// RED stubs: behavior arrives in the GREEN commit.

// AttemptErrLimit is the byte budget of Attempt.Err.
const AttemptErrLimit = 256

// FailingInvokerOf names the failing part of the pipeline for err.
func FailingInvokerOf(err error) FailingInvoker { return "" }

// AttemptClassOf classifies the error of one attempt.
func AttemptClassOf(ctx context.Context, err error) AttemptClass { return "" }

// NewAttempt builds an Attempt.
func NewAttempt(n int, err error, class AttemptClass, started time.Time, duration, delay time.Duration) Attempt {
	return Attempt{N: n}
}
