package event_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	event "github.com/isaquesb/go-event-bus"
)

// hdEvent is the event of the HandlerDone matrix.
type hdEvent struct{}

func (*hdEvent) Name() string { return "hd.evt" }

// hdTerminal is a permanent failure: Terminal() true.
type hdTerminal struct{}

func (hdTerminal) Error() string  { return "hd: terminal failure" }
func (hdTerminal) Terminal() bool { return true }

// hdBus builds a LocalBus with rec as its Observer and one handler "h" on
// "hd.evt" running fn.
func hdBus(t *testing.T, rec *obsRecorder, opts event.LocalBusOptions, fn event.HandleFn) *event.LocalBus {
	t.Helper()
	opts.Observer = rec
	bus := event.NewLocalBus(opts)
	if err := bus.Subscribe(context.Background(), "hd.evt", fn, event.WithHandlerName("h")); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	return bus
}

// hdOneDone fails the test unless exactly one HandlerDone was recorded, and
// returns it.
func hdOneDone(t *testing.T, rec *obsRecorder) event.HandlerResult {
	t.Helper()
	dones := rec.HandlerDones()
	if len(dones) != 1 {
		t.Fatalf("HandlerDone calls = %d (%+v), want exactly 1", len(dones), dones)
	}
	return dones[0]
}

func TestHandlerDone_Success(t *testing.T) {
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{}, func(context.Context, event.Event) error {
		time.Sleep(time.Millisecond)
		return nil
	})

	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil", err)
	}

	d := hdOneDone(t, rec)
	if d.Outcome != event.OutcomeOK || d.Handler != "h" || d.Event != "hd.evt" {
		t.Errorf("HandlerDone = outcome %q handler %q event %q, want ok, h, hd.evt", d.Outcome, d.Handler, d.Event)
	}
	if d.Duration <= 0 {
		t.Errorf("Duration = %v, want > 0", d.Duration)
	}
	if d.Err != nil {
		t.Errorf("Err = %v, want nil", d.Err)
	}
	if d.At.IsZero() {
		t.Error("At is zero")
	}
	if len(rec.Failures()) != 0 {
		t.Errorf("Failure calls = %d, want 0", len(rec.Failures()))
	}
}

func TestHandlerDone_BackstopDeadLettered(t *testing.T) {
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{DLQPublisher: event.NewMemoryDLQ()},
		func(context.Context, event.Event) error { return hdTerminal{} })

	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a dead-lettered failure", err)
	}

	d := hdOneDone(t, rec)
	if d.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want dead_lettered (emitted by Settle after the backstop publish, never failed)", d.Outcome)
	}
	if !errors.Is(d.Err, hdTerminal{}) {
		t.Errorf("Err = %v, want the chain error", d.Err)
	}
	if len(rec.Failures()) != 1 {
		t.Errorf("Failure calls = %d, want 1", len(rec.Failures()))
	}
}

func TestHandlerDone_BackstopDropped(t *testing.T) {
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{},
		func(context.Context, event.Event) error { return hdTerminal{} })

	_, err := bus.EmitSync(context.Background(), &hdEvent{})
	if !errors.Is(err, hdTerminal{}) {
		t.Fatalf("EmitSync = %v, want the handler error", err)
	}

	if d := hdOneDone(t, rec); d.Outcome != event.OutcomeDropped {
		t.Errorf("Outcome = %q, want dropped", d.Outcome)
	}
	if f := rec.Failures(); len(f) != 1 || f[0].Outcome != event.OutcomeDropped {
		t.Errorf("Failures = %+v, want one dropped", f)
	}
}

func TestHandlerDone_BackstopPublishFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &obsRecorder{}
		bus := hdBus(t, rec, event.LocalBusOptions{DLQPublisher: &lfFailingPublisher{}},
			func(context.Context, event.Event) error { return hdTerminal{} })

		_, err := bus.EmitSync(context.Background(), &hdEvent{})
		if !errors.Is(err, event.ErrDeadLetterFailed) {
			t.Fatalf("EmitSync = %v, want ErrDeadLetterFailed", err)
		}

		if d := hdOneDone(t, rec); d.Outcome != event.OutcomeFailed {
			t.Errorf("Outcome = %q, want failed", d.Outcome)
		}
		f := rec.Failures()
		if len(f) != 1 || f[0].Outcome != event.OutcomeDLQPublishFailed {
			t.Fatalf("Failures = %+v, want one dlq_publish_failed", f)
		}
		if got := len(rec.DLQPublishFailures()); got != 1 {
			t.Errorf("DLQPublishFailed signals = %d, want 1", got)
		}
	})
}

func TestHandlerDone_ChainDeadLetteredImmediate(t *testing.T) {
	chainErr := &event.DeadLetteredError{Cause: hdTerminal{}, Reason: event.ReasonTerminal}

	// Through InvokeHandler alone: the signal is already out before any Settle.
	rec := &obsRecorder{}
	ctx, _ := event.WithFailureScope(event.WithObserver(context.Background(), rec))
	err := event.InvokeHandler(ctx, &lfInvoker{err: chainErr}, &hdEvent{}, "h", func(context.Context) error { return nil })
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Fatalf("InvokeHandler = %v, want ErrDeadLettered", err)
	}
	if d := hdOneDone(t, rec); d.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want dead_lettered emitted immediately", d.Outcome)
	}

	// Through a LocalBus: still exactly one.
	busRec := &obsRecorder{}
	bus := hdBus(t, busRec, event.LocalBusOptions{Invoker: &lfInvoker{err: chainErr}, DLQPublisher: event.NewMemoryDLQ()},
		func(context.Context, event.Event) error { return nil })
	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil", err)
	}
	if d := hdOneDone(t, busRec); d.Outcome != event.OutcomeDeadLettered {
		t.Errorf("bus Outcome = %q, want dead_lettered", d.Outcome)
	}
}

func TestHandlerDone_Duplicate(t *testing.T) {
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{Invoker: &lfInvoker{err: event.ErrDuplicate}},
		func(context.Context, event.Event) error { return nil })

	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a duplicate", err)
	}

	if d := hdOneDone(t, rec); d.Outcome != event.OutcomeDuplicate {
		t.Errorf("Outcome = %q, want duplicate", d.Outcome)
	}
	if len(rec.Failures()) != 0 {
		t.Errorf("Failure calls = %d, want 0", len(rec.Failures()))
	}
}

func TestHandlerDone_RateLimitedNotDeferred(t *testing.T) {
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{
		Invoker:      &lfInvoker{err: event.ErrRateLimited},
		DLQPublisher: event.NewMemoryDLQ(),
	}, func(context.Context, event.Event) error { return nil })

	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil", err)
	}

	if d := hdOneDone(t, rec); d.Outcome != event.OutcomeRateLimited {
		t.Errorf("Outcome = %q, want rate_limited (not replaced by the settled outcome)", d.Outcome)
	}
	f := rec.Failures()
	if len(f) != 1 || f[0].Reason != event.ReasonRateLimited {
		t.Errorf("Failures = %+v, want one with reason rate_limited", f)
	}
}

func TestHandlerDone_Panic(t *testing.T) {
	logs := fsCaptureLogs(t)
	rec := &obsRecorder{}
	bus := hdBus(t, rec, event.LocalBusOptions{DLQPublisher: event.NewMemoryDLQ()},
		func(context.Context, event.Event) error { panic("hd: boom") })

	if _, err := bus.EmitSync(context.Background(), &hdEvent{}); err != nil {
		t.Fatalf("EmitSync = %v, want nil for a dead-lettered panic", err)
	}

	d := hdOneDone(t, rec)
	var pe *event.PanicError
	if !errors.As(d.Err, &pe) || len(pe.Stack) == 0 || pe.Value != "hd: boom" {
		t.Fatalf("HandlerDone Err = %v, want a *PanicError with value and stack", d.Err)
	}
	if d.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want dead_lettered", d.Outcome)
	}
	f := rec.Failures()
	if len(f) != 1 {
		t.Fatalf("Failure calls = %d, want 1", len(f))
	}
	var fpe *event.PanicError
	if !errors.As(f[0].Cause, &fpe) || fpe != pe {
		t.Errorf("Failure Cause = %v, want the same *PanicError as HandlerDone", f[0].Cause)
	}
	logs.mu.Lock()
	defer logs.mu.Unlock()
	if len(logs.recs) != 0 {
		t.Errorf("log records = %+v, want none: InvokeHandler no longer logs", logs.recs)
	}
}

func TestHandlerDone_NoScopeImmediateFailed(t *testing.T) {
	rec := &obsRecorder{}
	ctx := event.WithObserver(context.Background(), rec)

	err := event.InvokeHandler(ctx, nil, &hdEvent{}, "h", func(context.Context) error { return hdTerminal{} })
	if !errors.Is(err, hdTerminal{}) {
		t.Fatalf("InvokeHandler = %v, want the handler error", err)
	}

	d := hdOneDone(t, rec)
	if d.Outcome != event.OutcomeFailed || d.Handler != "h" || d.Event != "hd.evt" {
		t.Errorf("HandlerDone = outcome %q handler %q event %q, want failed, h, hd.evt", d.Outcome, d.Handler, d.Event)
	}
}

func TestHandlerDone_FlushOnce(t *testing.T) {
	rec := &obsRecorder{}
	ctx := event.WithDelivery(event.WithObserver(context.Background(), rec), event.Delivery{Count: 1, Max: 1})
	ctx, _ = event.WithFailureScope(ctx)

	err := event.InvokeHandler(ctx, nil, &hdEvent{}, "h", func(context.Context) error { return hdTerminal{} })
	if len(rec.HandlerDones()) != 0 {
		t.Fatalf("HandlerDone before Settle = %d, want 0: a terminal failure is deferred", len(rec.HandlerDones()))
	}

	in := event.SettleInput{SubscriptionCtx: context.Background(), Evt: &hdEvent{}, Handler: "h", Err: err}
	first := event.Settle(ctx, in)
	second := event.Settle(ctx, in)
	if first.Outcome != event.OutcomeDropped || second.Outcome != event.OutcomeDropped {
		t.Fatalf("Settle outcomes = %q and %q, want dropped twice", first.Outcome, second.Outcome)
	}

	if d := hdOneDone(t, rec); d.Outcome != event.OutcomeDropped {
		t.Errorf("Outcome = %q, want dropped", d.Outcome)
	}
	if got := len(rec.Failures()); got != 1 {
		t.Errorf("Failure calls = %d, want 1: the scope guard makes the report exactly-once", got)
	}
}
