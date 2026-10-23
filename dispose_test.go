package event_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

func TestDispose_TruthTable(t *testing.T) {
	const (
		ack   = event.DispositionAck
		delay = event.DispositionRetryDelayed
		term  = event.DispositionTerminal
	)
	deliveries := []struct {
		name string
		d    event.Delivery
	}{
		{"non-final 1/3", event.Delivery{Count: 1, Max: 3}},
		{"final 3/3", event.Delivery{Count: 3, Max: 3}},
		{"unlimited 5/0", event.Delivery{Count: 5, Max: 0}},
	}
	terminalFailure := &event.FailureError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal, Attempts: 1}

	rows := []struct {
		name string
		err  error
		want [3]event.Disposition // non-final, final, unlimited
	}{
		{"nil", nil, [3]event.Disposition{ack, ack, ack}},
		{"duplicate wrapped", fmt.Errorf("wrap: %w", event.ErrDuplicate), [3]event.Disposition{ack, ack, ack}},
		{"dead-lettered circuit-open (Pitfall 3)", &event.DeadLetteredError{Cause: event.ErrCircuitOpen, Reason: event.ReasonCircuitOpen}, [3]event.Disposition{ack, ack, ack}},
		{"dead-letter publish failed", &event.DeadLetterFailedError{Cause: terminalFailure, Reason: event.ReasonTerminal, PublishErr: errors.New("down")}, [3]event.Disposition{delay, delay, delay}},
		{"terminal failure", terminalFailure, [3]event.Disposition{term, term, term}},
		{"permanent error", fsPermanent{msg: "bad"}, [3]event.Disposition{term, term, term}},
		{"send to dlq", fmt.Errorf("wrap: %w", event.ErrSendToDLQ), [3]event.Disposition{term, term, term}},
		{"unknown version", &event.DecodeError{Err: event.ErrUnknownVersion}, [3]event.Disposition{delay, term, delay}},
		{"circuit open", event.ErrCircuitOpen, [3]event.Disposition{delay, term, delay}},
		{"rate limited", event.ErrRateLimited, [3]event.Disposition{delay, term, delay}},
		{"in progress", event.ErrInProgress, [3]event.Disposition{delay, term, delay}},
		{"malformed envelope", &event.DecodeError{Err: event.ErrMalformedEnvelope}, [3]event.Disposition{term, term, term}},
		{"unknown event", &event.DecodeError{Err: event.ErrUnknownEvent}, [3]event.Disposition{term, term, term}},
		{"missing upcaster", &event.DecodeError{Err: event.ErrMissingUpcaster}, [3]event.Disposition{term, term, term}},
		{"decode error without sentinel", &event.DecodeError{Err: errors.New("bad json")}, [3]event.Disposition{term, term, term}},
		{"retries exhausted", &event.FailureError{Cause: errors.New("flaky"), Reason: event.ReasonRetriesExhausted, Attempts: 3}, [3]event.Disposition{delay, term, delay}},
		{"plain error", errors.New("boom"), [3]event.Disposition{delay, term, delay}},
		{"handler-only timeout", context.DeadlineExceeded, [3]event.Disposition{delay, term, delay}},
	}

	for _, row := range rows {
		for i, dv := range deliveries {
			t.Run(row.name+"/"+dv.name, func(t *testing.T) {
				if got := event.Dispose(row.err, dv.d); got != row.want[i] {
					t.Fatalf("Dispose = %v, want %v", got, row.want[i])
				}
			})
		}
	}
}

func TestDisposition_String(t *testing.T) {
	cases := map[event.Disposition]string{
		event.DispositionAck:          "ack",
		event.DispositionRetry:        "retry",
		event.DispositionRetryDelayed: "retry_delayed",
		event.DispositionTerminal:     "terminal",
		event.Disposition(0):          "unknown",
	}
	for d, want := range cases {
		if got := d.String(); got != want {
			t.Errorf("Disposition(%d).String() = %q, want %q", int(d), got, want)
		}
	}
}
