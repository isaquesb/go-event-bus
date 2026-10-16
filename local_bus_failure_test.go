package event_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// lfInvoker returns a configured error without calling next, or calls next
// when the error is nil.
type lfInvoker struct{ err error }

func (i *lfInvoker) Invoke(ctx context.Context, _ event.Event, _ string, next func(context.Context) error) error {
	if i.err != nil {
		return i.err
	}
	return next(ctx)
}

// lfFailingPublisher counts calls and always fails.
type lfFailingPublisher struct{ calls atomic.Int32 }

func (p *lfFailingPublisher) Publish(context.Context, event.DLQRecord) error {
	p.calls.Add(1)
	return errors.New("lf: broker down")
}

// lfSubscribeNoop subscribes a handler that returns nil on subject.
func lfSubscribeNoop(t *testing.T, bus *event.LocalBus, subject string) {
	t.Helper()
	if err := bus.Subscribe(context.Background(), subject,
		func(context.Context, event.Event) error { return nil },
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
}

func TestLocalBus_HandlerCtxCarriesDelivery(t *testing.T) {
	bus := event.NewLocalBus(event.LocalBusOptions{Invoker: &passthroughInvoker{}})

	var (
		gotDelivery event.Delivery
		gotExec     event.Execution
		okD, okE    bool
	)
	before := time.Now()
	if err := bus.Subscribe(context.Background(), "orders.created",
		func(ctx context.Context, _ event.Event) error {
			gotDelivery, okD = event.DeliveryFromContext(ctx)
			gotExec, okE = event.ExecutionFromContext(ctx)
			return nil
		},
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, err := bus.EmitSync(context.Background(), &lfEvent{name: "orders.created"}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	if !okD || !okE {
		t.Fatalf("Delivery ok=%v Execution ok=%v, want both true", okD, okE)
	}
	if gotDelivery.Count != 1 || gotDelivery.Max != 1 || !gotDelivery.Final() {
		t.Errorf("Delivery = %+v, want Count 1 Max 1 final", gotDelivery)
	}
	if gotDelivery.Subject != "orders.created" {
		t.Errorf("Delivery.Subject = %q, want orders.created", gotDelivery.Subject)
	}
	if gotExec.Handler != "h" {
		t.Errorf("Execution.Handler = %q, want h", gotExec.Handler)
	}
	if gotExec.StartedAt.Before(before) {
		t.Errorf("Execution.StartedAt = %v, want >= %v", gotExec.StartedAt, before)
	}
}

func TestLocalBus_DroppedKeepsErrorIdentity(t *testing.T) {
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker: &passthroughInvoker{},
		OnErr:   capture.fn,
	})

	boom := errors.New("lf: boom")
	if err := bus.Subscribe(context.Background(), "orders.created",
		func(context.Context, event.Event) error { return boom },
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	evt := &lfEvent{name: "orders.created"}
	if _, err := bus.EmitSync(context.Background(), evt); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	errs := capture.all()
	if len(errs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(errs))
	}
	if errs[0] != boom {
		t.Errorf("bus OnErr err = %v, want the identical handler error value", errs[0])
	}
	if got := evt.onErr.Load(); got != 1 {
		t.Errorf("event OnErr calls = %d, want 1", got)
	}
}

func TestLocalBus_ChainDeadLetteredNotRepublished(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	chainErr := &event.DeadLetteredError{Cause: lfPermanent{}, Reason: event.ReasonTerminal}
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &lfInvoker{err: chainErr},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})
	lfSubscribeNoop(t, bus, "orders.created")

	if _, err := bus.EmitSync(context.Background(), &lfEvent{name: "orders.created"}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	if got := dlq.Len(); got != 0 {
		t.Errorf("bus publisher records = %d, want 0 (already dead-lettered by the chain)", got)
	}
	errs := capture.all()
	if len(errs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(errs))
	}
	if !errors.Is(errs[0], event.ErrDeadLettered) {
		t.Errorf("bus OnErr err = %v, want ErrDeadLettered", errs[0])
	}
}

func TestLocalBus_DeadLetterFailedReported(t *testing.T) {
	pub := &lfFailingPublisher{}
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &lfInvoker{err: lfPermanent{}},
		DLQPublisher: pub,
		OnErr:        capture.fn,
	})
	lfSubscribeNoop(t, bus, "orders.created")

	if _, err := bus.EmitSync(context.Background(), &lfEvent{name: "orders.created"}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	if got := pub.calls.Load(); got != 3 {
		t.Errorf("publisher calls = %d, want 3 (in-process retry on the final delivery)", got)
	}
	errs := capture.all()
	if len(errs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(errs))
	}
	if !errors.Is(errs[0], event.ErrDeadLetterFailed) {
		t.Errorf("bus OnErr err = %v, want ErrDeadLetterFailed", errs[0])
	}
}

func TestLocalBus_DuplicateNotReported(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &lfInvoker{err: event.ErrDuplicate},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})
	lfSubscribeNoop(t, bus, "orders.created")

	evt := &lfEvent{name: "orders.created"}
	if _, err := bus.EmitSync(context.Background(), evt); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	if got := len(capture.all()); got != 0 {
		t.Errorf("bus OnErr calls = %d, want 0 for a duplicate", got)
	}
	if got := evt.onErr.Load(); got != 0 {
		t.Errorf("event OnErr calls = %d, want 0 for a duplicate", got)
	}
	if got := dlq.Len(); got != 0 {
		t.Errorf("records = %d, want 0 for a duplicate", got)
	}
}

func TestLocalBus_InterruptedNeverDeadLettered(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &passthroughInvoker{},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := bus.Subscribe(context.Background(), "orders.created",
		func(hctx context.Context, _ event.Event) error {
			cancel()
			return hctx.Err()
		},
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if _, err := bus.EmitSync(ctx, &lfEvent{name: "orders.created"}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	if got := dlq.Len(); got != 0 {
		t.Errorf("records = %d, want 0 (an interruption is never dead-lettered)", got)
	}
	errs := capture.all()
	if len(errs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(errs))
	}
	if !errors.Is(errs[0], context.Canceled) {
		t.Errorf("bus OnErr err = %v, want context.Canceled", errs[0])
	}
}

func TestLocalBus_InProgressDeadLettered(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &lfInvoker{err: event.ErrInProgress},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})
	lfSubscribeNoop(t, bus, "orders.created")

	if _, err := bus.EmitSync(context.Background(), &lfEvent{name: "orders.created"}); err != nil {
		t.Fatalf("EmitSync: %v", err)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	if recs[0].Reason != event.ReasonInProgress {
		t.Errorf("Reason = %q, want %q", recs[0].Reason, event.ReasonInProgress)
	}
	if recs[0].Attempts != 0 {
		t.Errorf("Attempts = %d, want 0 (the handler never ran)", recs[0].Attempts)
	}
	if got := len(capture.all()); got != 1 {
		t.Errorf("bus OnErr calls = %d, want 1", got)
	}
}

func TestLocalBus_AsyncEmitDeadLetters(t *testing.T) {
	dlq := event.NewMemoryDLQ()
	capture := newLfErrCapture()
	bus := event.NewLocalBus(event.LocalBusOptions{
		Invoker:      &passthroughInvoker{},
		DLQPublisher: dlq,
		OnErr:        capture.fn,
	})
	if err := bus.Subscribe(context.Background(), "orders.created",
		func(context.Context, event.Event) error { return lfPermanent{} },
		event.WithHandlerName("h"),
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	bus.Emit(context.Background(), &lfEvent{name: "orders.created"})

	select {
	case <-capture.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the async failure to be reported")
	}
	if got := dlq.Len(); got != 1 {
		t.Errorf("records = %d, want 1", got)
	}
}
