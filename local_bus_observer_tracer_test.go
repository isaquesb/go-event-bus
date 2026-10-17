package event_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	event "github.com/isaquesb/go-event-bus"
)

// loEvent is the event of the tracer: its own OnErr method counts every call.
type loEvent struct {
	onErr atomic.Int32
}

func (*loEvent) Name() string                           { return "lo.order" }
func (e *loEvent) OnErr(context.Context, error, string) { e.onErr.Add(1) }

// loTerminal is a local error type with Terminal() true.
type loTerminal struct{}

func (loTerminal) Error() string  { return "lo: terminal failure" }
func (loTerminal) Terminal() bool { return true }

// TestLocalBus_FailureReachesObserverOnce is the tracer of plan 10-07: one
// terminal handler failure goes Emit -> execute (Observer and FailureScope on
// the handler ctx) -> InvokeHandler -> Settle -> reportTerminal and reaches the
// bus Observer exactly once with a full FailureContext, and the event's own
// OnErr method once.
func TestLocalBus_FailureReachesObserverOnce(t *testing.T) {
	rec := &obsRecorder{}
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &passthroughInvoker{},
		Observer:     rec,
		DLQPublisher: event.NewMemoryDLQ(),
	})
	if err := bus.Subscribe(context.Background(), "lo.order",
		func(context.Context, event.Event) error { return loTerminal{} },
		event.WithHandlerName("billing")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	evt := &loEvent{}
	if _, err := bus.EmitSync(context.Background(), evt, event.WithID("lo-1")); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a dead-lettered failure", err)
	}

	failures := rec.Failures()
	if len(failures) != 1 {
		t.Fatalf("Failure calls = %d, want exactly 1", len(failures))
	}
	f := failures[0]
	if f.Handler != "billing" {
		t.Errorf("Handler = %q, want billing", f.Handler)
	}
	if f.Event != event.Event(evt) {
		t.Errorf("Event = %v, want the emitted loEvent", f.Event)
	}
	if f.Metadata.ID != "lo-1" {
		t.Errorf("Metadata.ID = %q, want lo-1", f.Metadata.ID)
	}
	if f.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want dead_lettered", f.Outcome)
	}
	if f.Reason != event.ReasonTerminal {
		t.Errorf("Reason = %q, want terminal", f.Reason)
	}
	if !errors.Is(f.Cause, event.ErrDeadLettered) {
		t.Errorf("Cause = %v, want ErrDeadLettered", f.Cause)
	}
	if f.Invoker != event.InvokerTransport {
		t.Errorf("Invoker = %q, want transport", f.Invoker)
	}
	if f.Attempts != 1 {
		t.Errorf("Attempts = %d, want 1", f.Attempts)
	}
	if f.Delivery.Count != 1 || f.Delivery.Max != 1 {
		t.Errorf("Delivery = %+v, want Count 1 Max 1", f.Delivery)
	}
	if f.Delivery.Raw != nil {
		t.Errorf("Delivery.Raw = %q, want nil", f.Delivery.Raw)
	}
	if f.At.IsZero() {
		t.Error("At is zero")
	}

	if got := len(rec.DeadLetters()); got != 1 {
		t.Errorf("DeadLettered signals = %d, want 1", got)
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}
