package event_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

// fsPermanent is a permanent error: Terminal() reports true, like the
// invoker module's PermanentError, which root tests cannot import.
type fsPermanent struct{ msg string }

func (e fsPermanent) Error() string  { return e.msg }
func (e fsPermanent) Terminal() bool { return true }

// fsEvent is an event with its own OnErr method that records every call.
type fsEvent struct {
	mu   sync.Mutex
	errs []error
}

func (e *fsEvent) Name() string { return "orders.created" }
func (e *fsEvent) OnErr(_ context.Context, err error, _ string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs = append(e.errs, err)
}
func (e *fsEvent) calls() []error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]error(nil), e.errs...)
}

// fsBusErr records every bus OnErr callback.
type fsBusErr struct {
	mu   sync.Mutex
	errs []error
	evts []event.Event
}

func (b *fsBusErr) fn(_ context.Context, evt event.Event, err error, _ string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.errs = append(b.errs, err)
	b.evts = append(b.evts, evt)
}
func (b *fsBusErr) calls() []error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]error(nil), b.errs...)
}

func fsMessageCtx(d event.Delivery) context.Context {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "evt-1", Name: "orders.created"})
	ctx = event.WithDelivery(ctx, d)
	return event.WithExecution(ctx, event.Execution{Handler: "h"})
}

// Tracer: Delivery in ctx -> Dispose -> NewDLQRecord -> PublishDeadLetter ->
// MemoryDLQ -> DeadLetteredError -> OnErr, end to end.
func TestSettle_TerminalDeadLetteredOnce(t *testing.T) {
	raw := []byte(`{"id":"evt-1","name":"orders.created","version":1}`)
	ctx := fsMessageCtx(event.Delivery{Count: 1, Max: 1, Subject: "orders.created", Raw: raw})
	cause := fsPermanent{msg: "bad order"}
	evt := &fsEvent{}
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "h",
		Err:             &event.FailureError{Cause: cause, Reason: event.ReasonTerminal, Attempts: 2},
		Publisher:       dlq,
		OnErr:           bus.fn,
	})

	if got.Disposition != event.DispositionTerminal {
		t.Fatalf("Disposition = %v, want terminal", got.Disposition)
	}
	if got.Outcome != event.OutcomeDeadLettered {
		t.Fatalf("Outcome = %q, want dead_lettered", got.Outcome)
	}
	if got.Reason != event.ReasonTerminal {
		t.Fatalf("Reason = %q, want terminal", got.Reason)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("published %d records, want 1", len(recs))
	}
	rec := recs[0]
	if string(rec.Raw) != string(raw) {
		t.Errorf("Raw = %q, want the input bytes", rec.Raw)
	}
	if rec.EventID != "evt-1" || rec.Handler != "h" || rec.Subject != "orders.created" || rec.Attempts != 2 {
		t.Errorf("record = id %q handler %q subject %q attempts %d", rec.EventID, rec.Handler, rec.Subject, rec.Attempts)
	}

	for name, calls := range map[string][]error{"bus OnErr": bus.calls(), "event OnErr": evt.calls()} {
		if len(calls) != 1 {
			t.Fatalf("%s called %d times, want 1", name, len(calls))
		}
		if !errors.Is(calls[0], event.ErrDeadLettered) || !errors.Is(calls[0], cause) {
			t.Errorf("%s error = %v, want ErrDeadLettered and the cause", name, calls[0])
		}
	}
}

func TestSettle_ChainDeadLetteredIsAcked(t *testing.T) {
	ctx := fsMessageCtx(event.Delivery{Count: 1, Max: 1, Subject: "orders.created"})
	bus := &fsBusErr{}
	dlq := event.NewMemoryDLQ()

	got := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             &fsEvent{},
		Handler:         "h",
		Err:             &event.DeadLetteredError{Cause: fsPermanent{msg: "bad"}, Reason: event.ReasonTerminal},
		Publisher:       dlq,
		OnErr:           bus.fn,
	})

	if got.Disposition != event.DispositionAck {
		t.Fatalf("Disposition = %v, want ack", got.Disposition)
	}
	if dlq.Len() != 0 {
		t.Errorf("published %d records, want 0 (the chain already dead-lettered)", dlq.Len())
	}
	if n := len(bus.calls()); n != 1 {
		t.Errorf("OnErr called %d times, want 1", n)
	}
}
