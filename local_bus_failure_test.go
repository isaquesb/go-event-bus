package event_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

// lfEvent is a named event with an id that counts its own OnErr calls.
type lfEvent struct {
	name  string
	onErr atomic.Int32
}

func (e *lfEvent) Name() string { return e.name }
func (e *lfEvent) OnErr(context.Context, error, string) {
	e.onErr.Add(1)
}

// lfPermanent is a permanent failure: Terminal() true.
type lfPermanent struct{}

func (lfPermanent) Error() string  { return "lf: permanent failure" }
func (lfPermanent) Terminal() bool { return true }

// lfErrCapture records the bus OnErr calls.
type lfErrCapture struct {
	mu   sync.Mutex
	errs []error
	done chan struct{}
}

func newLfErrCapture() *lfErrCapture {
	return &lfErrCapture{done: make(chan struct{}, 16)}
}

func (c *lfErrCapture) fn(_ context.Context, _ event.Event, err error, _ string) {
	c.mu.Lock()
	c.errs = append(c.errs, err)
	c.mu.Unlock()
	c.done <- struct{}{}
}

func (c *lfErrCapture) all() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

func TestLocalBus_DeadLettersTerminalFailure(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &passthroughInvoker{},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})

	cause := lfPermanent{}
	if err := bus.Subscribe(context.Background(), "orders.created",
		func(context.Context, event.Event) error { return cause },
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	evt := &lfEvent{name: "orders.created"}
	if _, err := bus.EmitSync(context.Background(), evt, event.WithID("evt-9")); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Evt != event.Event(evt) {
		t.Errorf("rec.Evt = %v, want the emitted event", rec.Evt)
	}
	if len(rec.Raw) != 0 {
		t.Errorf("rec.Raw = %q, want empty", rec.Raw)
	}
	if rec.Handler != "h" {
		t.Errorf("rec.Handler = %q, want h", rec.Handler)
	}
	if rec.Subject != "orders.created" {
		t.Errorf("rec.Subject = %q, want orders.created", rec.Subject)
	}
	if rec.Reason != event.ReasonTerminal {
		t.Errorf("rec.Reason = %q, want %q", rec.Reason, event.ReasonTerminal)
	}
	if rec.EventID != "evt-9" {
		t.Errorf("rec.EventID = %q, want evt-9", rec.EventID)
	}
	if !rec.Delivery.Final() {
		t.Errorf("rec.Delivery = %+v, want final", rec.Delivery)
	}

	errs := capture.all()
	if len(errs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(errs))
	}
	if !errors.Is(errs[0], event.ErrDeadLettered) {
		t.Errorf("bus OnErr err = %v, want ErrDeadLettered", errs[0])
	}
	if !errors.Is(errs[0], cause) {
		t.Errorf("bus OnErr err = %v, want to wrap the handler error", errs[0])
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}
