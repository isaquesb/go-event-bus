package event

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// OnErr is the bus OnErr callback, nil when none is configured.
	OnErr func(ctx context.Context, evt Event, err error, handler string)
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
// Every terminal outcome is logged with slog and reported once to in.OnErr
// and to the event's own OnErr method. A decode failure has no event, so only
// the bus callback runs. Phase 10 replaces this sink.
func Settle(ctx context.Context, in SettleInput) Settlement {
	if in.Err == nil {
		return Settlement{Disposition: DispositionAck}
	}
	d, ok := DeliveryFromContext(ctx)
	if !ok {
		d = Delivery{Count: 1, Max: 1}
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
		s := Settlement{Disposition: DispositionRetryDelayed, Reason: reason}
		switch {
		case errors.Is(in.Err, ErrDeadLetterFailed):
			s.Err = in.Err
			if d.Final() {
				s.Outcome = OutcomeDLQPublishFailed
				reportTerminal(ctx, in, d, s.Outcome, reason, in.Err)
			} else {
				logSettle(ctx, slog.LevelError, "event: dead-letter publish failed, will be redelivered", in, d, reason, in.Err)
			}
		case isDecodeError(in.Err):
			logSettle(ctx, slog.LevelWarn, "event: decode failed, will be redelivered", in, d, reason, in.Err)
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
			} else {
				logSettle(ctx, slog.LevelError, "event: dead-letter publish failed, will be redelivered", in, d, reason, failed)
			}
			return s
		}
		done := &DeadLetteredError{Cause: in.Err, Reason: reason}
		reportTerminal(ctx, in, d, OutcomeDeadLettered, reason, done)
		return Settlement{Disposition: DispositionTerminal, Reason: reason, Outcome: OutcomeDeadLettered, Err: done}
	}
}

// reportTerminal is the only place that reports a terminal outcome: one slog
// record, then the bus OnErr callback, then the event's own OnErr method.
func reportTerminal(ctx context.Context, in SettleInput, d Delivery, outcome Outcome, reason string, err error) {
	level := slog.LevelError
	if outcome == OutcomeDeadLettered {
		level = slog.LevelWarn
	}
	logSettle(ctx, level, "event: terminal outcome", in, d, reason, err, slog.String("outcome", string(outcome)))
	if in.OnErr != nil {
		in.OnErr(ctx, in.Evt, err, in.Handler)
	}
	if in.Evt != nil {
		if h, ok := in.Evt.(OnErr); ok {
			h.OnErr(ctx, err, in.Handler)
		}
	}
}

// logSettle writes one structured record. It never includes the payload.
func logSettle(ctx context.Context, level slog.Level, msg string, in SettleInput, d Delivery, reason string, err error, extra ...slog.Attr) {
	attrs := append(make([]slog.Attr, 0, 9+len(extra)), extra...)
	attrs = append(attrs,
		slog.String("reason", reason),
		slog.String("handler", in.Handler),
		slog.String("subject", d.Subject),
		slog.Int("delivery_count", d.Count),
		slog.Int("max_deliver", d.Max),
	)
	if in.Evt != nil {
		attrs = append(attrs, slog.String("event", in.Evt.Name()))
	}
	if md, ok := MetadataFromContext(ctx); ok {
		attrs = append(attrs, slog.String("event_id", md.ID))
	}
	if err != nil {
		attrs = append(attrs, slog.String("error", err.Error()))
	}
	slog.LogAttrs(ctx, level, msg, attrs...)
}
