package event_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

func TestFailureError_Is(t *testing.T) {
	cause := errors.New("boom")

	terminal := &event.FailureError{Cause: cause, Reason: event.ReasonTerminal, Attempts: 2}
	if !errors.Is(terminal, cause) || !errors.Is(terminal, event.ErrSendToDLQ) {
		t.Error("terminal failure must match its cause and ErrSendToDLQ")
	}
	if errors.Is(terminal, event.ErrRetriesExhausted) {
		t.Error("terminal failure must not match ErrRetriesExhausted")
	}

	exhausted := &event.FailureError{Cause: cause, Reason: event.ReasonRetriesExhausted, Attempts: 4}
	if !errors.Is(exhausted, cause) || !errors.Is(exhausted, event.ErrRetriesExhausted) {
		t.Error("exhausted failure must match its cause and ErrRetriesExhausted")
	}
	if errors.Is(exhausted, event.ErrSendToDLQ) {
		t.Error("exhausted failure must not match ErrSendToDLQ")
	}

	var fe *event.FailureError
	if !errors.As(fmt.Errorf("wrap: %w", exhausted), &fe) || fe.Attempts != 4 {
		t.Errorf("errors.As Attempts = %v, want 4", fe)
	}

	var zero event.FailureError
	if zero.Error() == "" {
		t.Error("zero FailureError.Error() is empty")
	}
	var nilFE *event.FailureError
	_ = nilFE.Error()
}

func TestDeadLetteredError_Is(t *testing.T) {
	cause := errors.New("boom")
	err := &event.DeadLetteredError{Cause: cause, Reason: event.ReasonTerminal}
	if !errors.Is(err, event.ErrDeadLettered) || !errors.Is(err, cause) {
		t.Error("DeadLetteredError must match ErrDeadLettered and its cause")
	}
	var zero event.DeadLetteredError
	_ = zero.Error()
	var nilErr *event.DeadLetteredError
	_ = nilErr.Error()
}

func TestDeadLetterFailedError_Is(t *testing.T) {
	cause := errors.New("boom")
	publish := errors.New("broker down")
	err := &event.DeadLetterFailedError{Cause: cause, Reason: event.ReasonTerminal, PublishErr: publish}
	if !errors.Is(err, event.ErrDeadLetterFailed) || !errors.Is(err, publish) || !errors.Is(err, cause) {
		t.Error("DeadLetterFailedError must match ErrDeadLetterFailed, its publish error and its cause")
	}
	if errors.Is(err, event.ErrDeadLettered) {
		t.Error("DeadLetterFailedError must never match ErrDeadLettered")
	}
	var zero event.DeadLetterFailedError
	_ = zero.Error()
	var nilErr *event.DeadLetterFailedError
	_ = nilErr.Error()
}

func TestReasonOf(t *testing.T) {
	rows := []struct {
		name string
		err  error
		want string
	}{
		{"terminal failure", &event.FailureError{Cause: errors.New("x"), Reason: event.ReasonTerminal}, event.ReasonTerminal},
		{"send to dlq", event.ErrSendToDLQ, event.ReasonTerminal},
		{"permanent", fsPermanent{msg: "x"}, event.ReasonTerminal},
		{"retries exhausted failure", &event.FailureError{Cause: errors.New("x"), Reason: event.ReasonRetriesExhausted}, event.ReasonRetriesExhausted},
		{"retries exhausted sentinel", event.ErrRetriesExhausted, event.ReasonRetriesExhausted},
		{"delivery exhausted", errors.New("flaky"), event.ReasonDeliveryExhausted},
		{"circuit open", event.ErrCircuitOpen, event.ReasonCircuitOpen},
		{"rate limited", event.ErrRateLimited, event.ReasonRateLimited},
		{"unknown version", &event.DecodeError{Err: event.ErrUnknownVersion}, event.ReasonUnknownVersion},
		{"in progress", event.ErrInProgress, event.ReasonInProgress},
		{"decode malformed", &event.DecodeError{Err: event.ErrMalformedEnvelope}, event.ReasonDecodeMalformed},
		{"decode unknown event", &event.DecodeError{Err: event.ErrUnknownEvent}, event.ReasonDecodeUnknownEvent},
		{"decode missing upcaster", &event.DecodeError{Err: event.ErrMissingUpcaster}, event.ReasonDecodeMissingUpcaster},
		{"decode failed", &event.DecodeError{Err: errors.New("bad json")}, event.ReasonDecodeFailed},
		{"duplicate", event.ErrDuplicate, event.ReasonDuplicate},
		{"dead-lettered carries its reason", &event.DeadLetteredError{Cause: event.ErrCircuitOpen, Reason: event.ReasonCircuitOpen}, event.ReasonCircuitOpen},
		{"dead-letter failed carries its reason", &event.DeadLetterFailedError{Cause: errors.New("x"), Reason: event.ReasonTerminal, PublishErr: errors.New("down")}, event.ReasonTerminal},
	}
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			if got := event.ReasonOf(r.err); got != r.want {
				t.Errorf("ReasonOf = %q, want %q", got, r.want)
			}
			if got := event.ReasonOf(fmt.Errorf("wrap: %w", r.err)); got != r.want {
				t.Errorf("ReasonOf(wrapped) = %q, want %q", got, r.want)
			}
		})
	}
	if got := event.ReasonOf(nil); got != "" {
		t.Errorf("ReasonOf(nil) = %q, want empty", got)
	}
}

func TestAttemptsOf(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"failure error", &event.FailureError{Cause: errors.New("x"), Reason: event.ReasonRetriesExhausted, Attempts: 3}, 3},
		{"circuit open", event.ErrCircuitOpen, 0},
		{"decode error", &event.DecodeError{Err: event.ErrMalformedEnvelope}, 0},
		{"plain error", errors.New("boom"), 1},
	}
	for _, c := range cases {
		if got := event.AttemptsOf(c.err); got != c.want {
			t.Errorf("%s: AttemptsOf = %d, want %d", c.name, got, c.want)
		}
	}
}

func TestIsInterrupted(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	live := context.Background()

	if !event.IsInterrupted(cancelled, context.Canceled) {
		t.Error("cancelled ctx with context.Canceled must be interrupted")
	}
	if !event.IsInterrupted(cancelled, fmt.Errorf("wrap: %w", context.DeadlineExceeded)) {
		t.Error("cancelled ctx with context.DeadlineExceeded must be interrupted")
	}
	if event.IsInterrupted(live, context.Canceled) {
		t.Error("live ctx with context.Canceled is a handler timeout, not an interruption")
	}
	if event.IsInterrupted(cancelled, errors.New("boom")) {
		t.Error("cancelled ctx with a plain error must not be interrupted")
	}
	if event.IsInterrupted(nil, context.Canceled) { //nolint:staticcheck // nil ctx is part of the contract
		t.Error("nil ctx must not be interrupted")
	}
}

func TestWrapDecodeError(t *testing.T) {
	if event.WrapDecodeError(nil) != nil {
		t.Error("nil must stay nil")
	}
	plain := errors.New("bad bytes")
	wrapped := event.WrapDecodeError(plain)
	var de *event.DecodeError
	if !errors.As(wrapped, &de) || !errors.Is(wrapped, plain) {
		t.Errorf("plain error must become a *DecodeError matching the original, got %v", wrapped)
	}
	existing := &event.DecodeError{Name: "orders.created", Err: event.ErrUnknownVersion}
	if got := event.WrapDecodeError(existing); got != error(existing) {
		t.Error("an existing *DecodeError must keep its identity")
	}
}

func TestRootSentinelTexts(t *testing.T) {
	texts := map[error]string{
		event.ErrDuplicate:   "duplicate execution",
		event.ErrCircuitOpen: "circuit breaker open",
		event.ErrRateLimited: "rate limit exceeded",
		event.ErrSendToDLQ:   "send to dlq",
	}
	for err, want := range texts {
		if err.Error() != want {
			t.Errorf("sentinel text = %q, want %q (v0.3.0 text)", err.Error(), want)
		}
	}
}
