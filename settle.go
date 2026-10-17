package event

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"time"
)

// Disposition is what a transport does with a message after the handler chain
// returned. The zero value is invalid.
type Disposition int

const (
	// DispositionAck acknowledges the message: success, duplicate or already
	// dead-lettered.
	DispositionAck Disposition = iota + 1
	// DispositionRetry redelivers the message as soon as possible. Dispose
	// never returns it: it is the transport's interruption path (Settle, D-07).
	DispositionRetry
	// DispositionRetryDelayed redelivers the message after a backoff.
	DispositionRetryDelayed
	// DispositionTerminal ends the message. Settle only returns it after a
	// successful dead-letter, or for a drop when no DLQPublisher is set.
	DispositionTerminal
)

// String returns ack, retry, retry_delayed, terminal or unknown.
func (d Disposition) String() string {
	switch d {
	case DispositionAck:
		return "ack"
	case DispositionRetry:
		return "retry"
	case DispositionRetryDelayed:
		return "retry_delayed"
	case DispositionTerminal:
		return "terminal"
	}
	return "unknown"
}

// Dispose maps the error a handler chain returned to a Disposition for the
// delivery d. The same function backs every transport. It is evaluated in this
// order, because a wrapped error can match several sentinels:
//
//	nil, ErrDuplicate, ErrDeadLettered            Ack
//	ErrDeadLetterFailed                           RetryDelayed
//	ErrHandlerPanic                               Terminal
//	ErrSendToDLQ or a Terminal() true error       Terminal
//	ErrUnknownVersion, ErrCircuitOpen,
//	ErrRateLimited, ErrInProgress                 RetryDelayed, Terminal when final
//	ErrMalformedEnvelope, ErrUnknownEvent,
//	ErrMissingUpcaster, any other *DecodeError    Terminal
//	anything else                                 RetryDelayed, Terminal when final
//
// ErrDeadLettered is checked before the cause sentinels, so a dead-lettered
// circuit-open failure is acknowledged and never redelivered. Dispose never
// returns DispositionRetry; a subscription context that ended is handled by
// Settle before Dispose is consulted.
func Dispose(err error, d Delivery) Disposition {
	switch {
	case err == nil,
		errors.Is(err, ErrDuplicate),
		errors.Is(err, ErrDeadLettered):
		return DispositionAck
	case errors.Is(err, ErrDeadLetterFailed):
		return DispositionRetryDelayed
	case errors.Is(err, ErrHandlerPanic):
		return DispositionTerminal
	case isTerminal(err):
		return DispositionTerminal
	case errors.Is(err, ErrUnknownVersion),
		errors.Is(err, ErrCircuitOpen),
		errors.Is(err, ErrRateLimited),
		errors.Is(err, ErrInProgress):
		return retryUnlessFinal(d)
	case isDecodeError(err):
		return DispositionTerminal
	}
	return retryUnlessFinal(d)
}

func retryUnlessFinal(d Delivery) Disposition {
	if d.Final() {
		return DispositionTerminal
	}
	return DispositionRetryDelayed
}

// Outcome names a terminal result Settle reports. The values are snake_case
// metric label values and never change once released.
type Outcome string

const (
	// OutcomeDeadLettered means the message was published to the DLQ.
	OutcomeDeadLettered Outcome = "dead_lettered"
	// OutcomeDLQPublishFailed means the DLQ publish failed on the final
	// delivery, so the message is at risk of being lost.
	OutcomeDLQPublishFailed Outcome = "dlq_publish_failed"
	// OutcomeDropped means the message ended with no DLQPublisher configured.
	OutcomeDropped Outcome = "dropped"
	// OutcomeInterrupted means the subscription context ended on the final
	// delivery, so the server will not redeliver the message.
	OutcomeInterrupted Outcome = "interrupted"
	// OutcomeRejected means the event was refused before any handler ran, for
	// example an emit after Shutdown.
	OutcomeRejected Outcome = "rejected"
)

// SettleInput is what Settle needs to decide the fate of one handled message.
type SettleInput struct {
	// SubscriptionCtx is the subscription context. Its cancellation, not the
	// message context's, marks an interruption.
	SubscriptionCtx context.Context
	// Evt is the decoded event, nil on a decode failure.
	Evt Event
	// Handler is the failing handler, empty on a decode failure.
	Handler string
	// Err is what the handler chain (or the decoder) returned.
	Err error
	// Publisher is the per-bus DLQ publisher, nil when none is configured.
	Publisher DLQPublisher
}

// Settlement is the decision Settle reached.
type Settlement struct {
	// Disposition is what the transport must do with the message.
	Disposition Disposition
	// Reason is the stable reason, see ReasonOf; empty on success.
	Reason string
	// Outcome is set when Settle reported a terminal outcome.
	Outcome Outcome
	// Err is the error that was reported or logged, nil on a quiet branch.
	Err error
}

// Settle is the one post-chain policy shared by every transport. A transport
// calls it exactly once per handled message, with the message context, and
// then acts on the returned Disposition.
//
//   - A nil error is Ack.
//   - A cancelled subscription context is Retry, never dead-lettered, and is
//     reported only on the final delivery, when no redelivery follows.
//   - Otherwise Dispose decides. A Terminal disposition that is not already
//     dead-lettered is published through in.Publisher: success is Terminal
//     with outcome dead_lettered, failure is RetryDelayed and never Terminal,
//     and no publisher is Terminal with outcome dropped.
//
// Settle is the only place a terminal outcome is reported. Every terminal
// outcome reaches Observer.Failure once (the OnFailure hook of OBS-03) and then
// the event's own OnErr method; a decode failure has no event, so only the
// observer runs. Settle never logs: SlogObserver turns the signals into log
// records. A transport installs the Observer and a FailureScope on ctx
// (WithObserver, WithFailureScope) before calling it; the scope is also what
// makes the report exactly-once and lets Settle complete a deferred HandlerDone
// signal with the settled outcome.
func Settle(ctx context.Context, in SettleInput) (res Settlement) {
	if in.Err == nil {
		return Settlement{Disposition: DispositionAck}
	}
	scope, _ := FailureScopeFromContext(ctx)
	defer func() { scope.flushHandlerDone(ctx, handlerDoneOutcome(res)) }()

	d, ok := DeliveryFromContext(ctx)
	if !ok {
		d = Delivery{Count: 1, Max: 1}
	}
	if in.Evt == nil && isDecodeError(in.Err) {
		ObserverFromContext(ctx).DecodeFailed(ctx, DecodeFailure{
			Handler:  in.Handler,
			Subject:  d.Subject,
			Reason:   ReasonOf(in.Err),
			Err:      in.Err,
			Delivery: redactDelivery(d),
			Final:    d.Final(),
		})
	}

	if IsInterrupted(in.SubscriptionCtx, in.Err) {
		s := Settlement{Disposition: DispositionRetry, Reason: ReasonInterrupted}
		if d.Final() {
			s.Outcome = OutcomeInterrupted
			s.Err = fmt.Errorf("event: subscription ended on the final delivery, no redelivery follows: %w", in.Err)
			reportTerminal(ctx, in, d, s.Outcome, s.Reason, s.Err)
		}
		return s
	}

	reason := ReasonOf(in.Err)
	switch Dispose(in.Err, d) {
	case DispositionAck:
		if errors.Is(in.Err, ErrDuplicate) || !errors.Is(in.Err, ErrDeadLettered) {
			return Settlement{Disposition: DispositionAck, Reason: reason}
		}
		reportTerminal(ctx, in, d, OutcomeDeadLettered, reason, in.Err)
		return Settlement{Disposition: DispositionAck, Reason: reason, Outcome: OutcomeDeadLettered, Err: in.Err}

	case DispositionRetryDelayed:
		// A non-final retry is reported by the signals that already fired:
		// DLQPublishFailed from PublishDeadLetter and DecodeFailed above.
		s := Settlement{Disposition: DispositionRetryDelayed, Reason: reason}
		if errors.Is(in.Err, ErrDeadLetterFailed) {
			s.Err = in.Err
			if d.Final() {
				s.Outcome = OutcomeDLQPublishFailed
				reportTerminal(ctx, in, d, s.Outcome, reason, in.Err)
			}
		}
		return s

	default: // DispositionTerminal
		if in.Publisher == nil {
			reportTerminal(ctx, in, d, OutcomeDropped, reason, in.Err)
			return Settlement{Disposition: DispositionTerminal, Reason: reason, Outcome: OutcomeDropped, Err: in.Err}
		}
		rec := NewDLQRecord(ctx, in.Evt, in.Handler, in.Err)
		if err := PublishDeadLetter(ctx, in.Publisher, rec); err != nil {
			failed := &DeadLetterFailedError{Cause: in.Err, Reason: reason, PublishErr: err}
			s := Settlement{Disposition: DispositionRetryDelayed, Reason: reason, Err: failed}
			if d.Final() {
				s.Outcome = OutcomeDLQPublishFailed
				reportTerminal(ctx, in, d, s.Outcome, reason, failed)
			}
			return s
		}
		done := &DeadLetteredError{Cause: in.Err, Reason: reason}
		reportTerminal(ctx, in, d, OutcomeDeadLettered, reason, done)
		return Settlement{Disposition: DispositionTerminal, Reason: reason, Outcome: OutcomeDeadLettered, Err: done}
	}
}

// handlerDoneOutcome maps a settlement to the outcome of the HandlerDone signal
// that InvokeHandler deferred: what really happened to the message. A zero
// Settlement, which is what a panicking Settle leaves behind, is failed.
func handlerDoneOutcome(s Settlement) Outcome {
	switch s.Disposition {
	case DispositionAck:
		if s.Outcome == OutcomeDeadLettered {
			return OutcomeDeadLettered
		}
	case DispositionTerminal:
		switch s.Outcome {
		case OutcomeDeadLettered:
			return OutcomeDeadLettered
		case OutcomeDropped:
			return OutcomeDropped
		}
	case DispositionRetryDelayed:
		if s.Outcome != OutcomeDLQPublishFailed {
			return OutcomeRetry
		}
	case DispositionRetry:
		return OutcomeInterrupted
	}
	return OutcomeFailed
}

// reportTerminal is the only place that reports a terminal outcome: one
// Observer.Failure call, then the event's own OnErr method. A FailureScope in
// ctx makes it exactly-once per delivery, even when two code paths report.
func reportTerminal(ctx context.Context, in SettleInput, d Delivery, outcome Outcome, reason string, err error) {
	scope, _ := FailureScopeFromContext(ctx)
	if !scope.markReported() {
		return
	}
	if outcome != OutcomeDLQPublishFailed && in.Evt == nil && isDecodeError(err) {
		outcome = OutcomeDecodeFailed
	}
	// The invoker is read from the error the chain returned, not from the
	// DeadLetteredError or DeadLetterFailedError that Settle wraps around it for
	// its own backstop publish: that failure belongs to the transport.
	origin := in.Err
	if origin == nil {
		origin = err
	}
	fc := FailureContext{
		Handler:  in.Handler,
		Event:    in.Evt,
		Outcome:  outcome,
		Reason:   reason,
		Cause:    err,
		Invoker:  FailingInvokerOf(origin),
		Attempts: AttemptsOf(err),
		Delivery: redactDelivery(d),
		Breaker:  scope.Breaker(),
		At:       time.Now().UTC(),
	}
	if md, ok := MetadataFromContext(ctx); ok {
		fc.Metadata = md
	}
	var fe *FailureError
	if errors.As(err, &fe) && len(fe.History) > 0 {
		fc.History = slices.Clone(fe.History)
	}
	ObserverFromContext(ctx).Failure(ctx, fc)
	if in.Evt != nil {
		if h, ok := in.Evt.(OnErr); ok {
			callEventOnErr(ctx, h, in.Handler, d.Subject, err)
		}
	}
}

// callEventOnErr runs the event's own OnErr method. A panic there is reported as
// an InternalError and never reaches the transport goroutine.
func callEventOnErr(ctx context.Context, h OnErr, handler, subject string, err error) {
	defer func() {
		if r := recover(); r != nil {
			ObserverFromContext(ctx).InternalError(ctx, InternalErrorInfo{
				Component: ComponentEvent,
				Op:        OpPanic,
				Handler:   handler,
				Subject:   subject,
				Err:       recoveredPanic(r),
			})
		}
	}()
	h.OnErr(ctx, err, handler)
}

// reportUnsettled reports a drop or a rejection that never reached a handler,
// so it shares the single report sink with Settle. The delivery is read from
// ctx and defaults to a final first delivery. The reason is derived from err.
func reportUnsettled(ctx context.Context, in SettleInput, outcome Outcome, err error) {
	d, ok := DeliveryFromContext(ctx)
	if !ok {
		d = Delivery{Count: 1, Max: 1}
	}
	reportTerminal(ctx, in, d, outcome, ReasonOf(err), err)
}

// recoveredPanic wraps a recovered panic value and the current stack. It must be
// called from the deferred function that recovered r, so the stack still holds
// the panicking frames.
func recoveredPanic(r any) *PanicError {
	return &PanicError{Value: r, Stack: debug.Stack()}
}
