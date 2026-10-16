package event

import (
	"context"
	"errors"
)

// Flow-control and dead-letter sentinels. The first four keep the exact texts
// they had in v0.3.0, when they lived in the invoker module.
var (
	// ErrDuplicate means the idempotency store already completed this
	// execution. It is a quiet success: the message is acknowledged.
	ErrDuplicate = errors.New("duplicate execution")

	// ErrCircuitOpen means the circuit breaker rejected the call. The handler
	// never ran, so the message is retried later.
	ErrCircuitOpen = errors.New("circuit breaker open")

	// ErrRateLimited means the rate limiter rejected the call. The handler
	// never ran, so the message is retried later.
	ErrRateLimited = errors.New("rate limit exceeded")

	// ErrSendToDLQ marks a terminal failure: retrying cannot help, so the
	// message goes to the dead-letter queue.
	ErrSendToDLQ = errors.New("send to dlq")

	// ErrRetriesExhausted means the in-process retry policy gave up on a
	// retryable failure.
	ErrRetriesExhausted = errors.New("event: retries exhausted")

	// ErrDeadLettered means the message was published to the dead-letter queue.
	// A transport that sees it acknowledges the message without publishing a
	// second record.
	ErrDeadLettered = errors.New("event: dead-lettered")

	// ErrInProgress means another execution of the same event and handler is
	// still running. The message is retried later.
	ErrInProgress = errors.New("event: execution in progress")

	// ErrDeadLetterFailed means publishing to the dead-letter queue failed.
	// The message is never acknowledged or terminated on this error.
	ErrDeadLetterFailed = errors.New("event: dead-letter publish failed")
)

// Stable reason vocabulary. These strings are wire values (DLQ headers) and log
// values, so they never change once released.
const (
	ReasonTerminal              = "terminal"
	ReasonRetriesExhausted      = "retries_exhausted"
	ReasonDeliveryExhausted     = "delivery_exhausted"
	ReasonCircuitOpen           = "circuit_open"
	ReasonRateLimited           = "rate_limited"
	ReasonUnknownVersion        = "unknown_version"
	ReasonInProgress            = "in_progress"
	ReasonDecodeMalformed       = "decode_malformed"
	ReasonDecodeUnknownEvent    = "decode_unknown_event"
	ReasonDecodeMissingUpcaster = "decode_missing_upcaster"
	ReasonDecodeFailed          = "decode_failed"
	ReasonDuplicate             = "duplicate"
	ReasonInterrupted           = "interrupted"
	ReasonPanic                 = "panic"
	ReasonBusClosed             = "bus_closed"
	ReasonQueueFull             = "queue_full"
)

// terminalError marks a permanent error. It has the same shape as
// invoker.Terminal, so the invoker's PermanentError satisfies it.
type terminalError interface {
	Terminal() bool
}

// FailureError is the failure of a handler after the in-process retry policy
// finished. errors.Is matches Cause and, by Reason, ErrSendToDLQ (terminal) or
// ErrRetriesExhausted (retries_exhausted); errors.As recovers Attempts.
//
// Phase 10 (OBS-04) adds the bounded retry history as a new field.
type FailureError struct {
	// Cause is the last handler error.
	Cause error
	// Reason is one of the Reason constants.
	Reason string
	// Attempts is the number of in-process attempts made on this delivery.
	Attempts int
}

// Error implements error. It never panics, including on a nil receiver or a
// nil Cause.
func (e *FailureError) Error() string {
	if e == nil {
		return "event: failure"
	}
	reason := e.Reason
	if reason == "" {
		reason = "failed"
	}
	return "event: " + reason + ": " + causeText(e.Cause)
}

// Unwrap returns the cause and the sentinel selected by Reason, so errors.Is
// traverses both.
func (e *FailureError) Unwrap() []error {
	if e == nil {
		return nil
	}
	var out []error
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	switch e.Reason {
	case ReasonTerminal:
		out = append(out, ErrSendToDLQ)
	case ReasonRetriesExhausted:
		out = append(out, ErrRetriesExhausted)
	}
	return out
}

// DeadLetteredError is returned by the DLQ invoker once the message was
// published to the dead-letter queue. errors.Is matches ErrDeadLettered and
// Cause.
type DeadLetteredError struct {
	// Cause is the failure that was dead-lettered.
	Cause error
	// Reason is one of the Reason constants.
	Reason string
}

// Error implements error. It never panics, including on a nil receiver.
func (e *DeadLetteredError) Error() string {
	if e == nil {
		return ErrDeadLettered.Error()
	}
	return ErrDeadLettered.Error() + " (" + e.Reason + "): " + causeText(e.Cause)
}

// Unwrap returns ErrDeadLettered and the cause.
func (e *DeadLetteredError) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := []error{ErrDeadLettered}
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	return out
}

// DeadLetterFailedError reports that publishing to the dead-letter queue
// failed. errors.Is matches ErrDeadLetterFailed, PublishErr and Cause, and
// never ErrDeadLettered.
type DeadLetterFailedError struct {
	// Cause is the failure that should have been dead-lettered.
	Cause error
	// Reason is one of the Reason constants.
	Reason string
	// PublishErr is the last error returned by the DLQ publisher.
	PublishErr error
}

// Error implements error. It never panics, including on a nil receiver.
func (e *DeadLetterFailedError) Error() string {
	if e == nil {
		return ErrDeadLetterFailed.Error()
	}
	return ErrDeadLetterFailed.Error() + " (" + e.Reason + "): " + causeText(e.PublishErr) +
		"; cause: " + causeText(e.Cause)
}

// Unwrap returns ErrDeadLetterFailed, the publish error and the cause.
func (e *DeadLetterFailedError) Unwrap() []error {
	if e == nil {
		return nil
	}
	out := []error{ErrDeadLetterFailed}
	if e.PublishErr != nil {
		out = append(out, e.PublishErr)
	}
	if e.Cause != nil {
		out = append(out, e.Cause)
	}
	return out
}

func causeText(err error) string {
	if err == nil {
		return "unknown error"
	}
	return err.Error()
}

// ReasonOf returns the stable reason for err, one of the Reason constants, or
// "" for nil. Precedence: panic (an error matching ErrHandlerPanic, even when
// wrapped); the Reason carried by DeadLetteredError,
// DeadLetterFailedError or FailureError; duplicate; bus_closed (ErrBusClosed);
// queue_full (ErrQueueFull); interrupted (ErrShutdownInterrupted); terminal
// (ErrSendToDLQ or an error with Terminal() true); unknown_version; circuit_open; rate_limited;
// in_progress; the decode reasons; retries_exhausted; and delivery_exhausted
// for everything else.
func ReasonOf(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrHandlerPanic) {
		return ReasonPanic
	}
	var dl *DeadLetteredError
	if errors.As(err, &dl) && dl.Reason != "" {
		return dl.Reason
	}
	var df *DeadLetterFailedError
	if errors.As(err, &df) && df.Reason != "" {
		return df.Reason
	}
	var fe *FailureError
	if errors.As(err, &fe) && fe.Reason != "" {
		return fe.Reason
	}
	switch {
	case errors.Is(err, ErrDuplicate):
		return ReasonDuplicate
	case errors.Is(err, ErrBusClosed):
		return ReasonBusClosed
	case errors.Is(err, ErrQueueFull):
		return ReasonQueueFull
	case errors.Is(err, ErrShutdownInterrupted):
		return ReasonInterrupted
	case isTerminal(err):
		return ReasonTerminal
	case errors.Is(err, ErrUnknownVersion):
		return ReasonUnknownVersion
	case errors.Is(err, ErrCircuitOpen):
		return ReasonCircuitOpen
	case errors.Is(err, ErrRateLimited):
		return ReasonRateLimited
	case errors.Is(err, ErrInProgress):
		return ReasonInProgress
	case errors.Is(err, ErrMalformedEnvelope):
		return ReasonDecodeMalformed
	case errors.Is(err, ErrUnknownEvent):
		return ReasonDecodeUnknownEvent
	case errors.Is(err, ErrMissingUpcaster):
		return ReasonDecodeMissingUpcaster
	case isDecodeError(err):
		return ReasonDecodeFailed
	case errors.Is(err, ErrRetriesExhausted):
		return ReasonRetriesExhausted
	}
	return ReasonDeliveryExhausted
}

// AttemptsOf returns how many in-process handler attempts led to err: the
// Attempts of a FailureError, 0 when the handler never ran (circuit_open,
// rate_limited, in_progress, unknown_version, duplicate and the decode
// reasons, or a nil err) and 1 otherwise.
func AttemptsOf(err error) int {
	if err == nil {
		return 0
	}
	var fe *FailureError
	if errors.As(err, &fe) {
		return fe.Attempts
	}
	switch ReasonOf(err) {
	case ReasonCircuitOpen, ReasonRateLimited, ReasonInProgress, ReasonUnknownVersion,
		ReasonDuplicate, ReasonDecodeMalformed, ReasonDecodeUnknownEvent,
		ReasonDecodeMissingUpcaster, ReasonDecodeFailed:
		return 0
	}
	return 1
}

// IsInterrupted reports whether err is the result of the subscription context
// ending: ctx is non-nil and done, and err matches context.Canceled or
// context.DeadlineExceeded. A handler-only timeout, where the subscription
// context is still alive, is not an interruption.
func IsInterrupted(ctx context.Context, err error) bool {
	if ctx == nil || ctx.Err() == nil || err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// WrapDecodeError returns nil for nil, err itself when it already holds a
// *DecodeError, and a *DecodeError wrapping err otherwise.
func WrapDecodeError(err error) error {
	if err == nil {
		return nil
	}
	if isDecodeError(err) {
		return err
	}
	return &DecodeError{Err: err}
}

func isDecodeError(err error) bool {
	var de *DecodeError
	return errors.As(err, &de)
}

func isTerminal(err error) bool {
	if errors.Is(err, ErrSendToDLQ) {
		return true
	}
	var t terminalError
	return errors.As(err, &t) && t.Terminal()
}
