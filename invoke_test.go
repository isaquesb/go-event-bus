package event_test

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/isaquesb/go-event-bus"
)

// ihEvent is an event with its own OnErr method that counts every call.
type ihEvent struct {
	mu    sync.Mutex
	calls int
}

func (e *ihEvent) Name() string { return "ih.order" }
func (e *ihEvent) OnErr(context.Context, error, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
}
func (e *ihEvent) onErrCalls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

// ihPanickingInvoker panics before it calls next.
type ihPanickingInvoker struct{}

func (ihPanickingInvoker) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	panic("invoker boom")
}

func TestInvokeHandler_PanicDeadLetteredWithReasonPanic(t *testing.T) {
	ctx := event.WithMetadata(context.Background(), event.Metadata{ID: "evt-p", Name: "ih.order"})
	ctx = event.WithDelivery(ctx, event.Delivery{Count: 1, Max: 1, Subject: "ih.subject"})
	evt := &ihEvent{}

	err := event.InvokeHandler(ctx, nil, evt, "h", func(context.Context) error {
		panic("boom")
	})
	if err == nil || !errors.Is(err, event.ErrHandlerPanic) {
		t.Fatalf("err = %v, want it to match ErrHandlerPanic", err)
	}
	var pe *event.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *PanicError", err)
	}
	if pe.Value != "boom" {
		t.Errorf("Value = %v, want boom", pe.Value)
	}
	if !strings.Contains(string(pe.Stack), "invoke_test.go") {
		t.Errorf("Stack does not mention invoke_test.go:\n%s", pe.Stack)
	}

	dlq := event.NewMemoryDLQ()
	rec := &obsRecorder{}
	s := event.Settle(event.WithObserver(ctx, rec), event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "h",
		Err:             err,
		Publisher:       dlq,
	})
	if s.Disposition != event.DispositionTerminal {
		t.Errorf("Disposition = %v, want terminal", s.Disposition)
	}
	if s.Outcome != event.OutcomeDeadLettered {
		t.Errorf("Outcome = %q, want dead_lettered", s.Outcome)
	}
	if s.Reason != "panic" {
		t.Errorf("Reason = %q, want panic", s.Reason)
	}

	recs := dlq.Records()
	if len(recs) != 1 {
		t.Fatalf("dead letters = %d, want 1", len(recs))
	}
	if recs[0].Reason != "panic" {
		t.Errorf("record Reason = %q, want panic", recs[0].Reason)
	}
	stack := recs[0].FailureHeaders()[event.HeaderDLQPanicStack]
	if stack == "" || len(stack) > 1024 {
		t.Errorf("panic stack header length = %d, want 1..1024", len(stack))
	}

	failures := rec.Failures()
	if len(failures) != 1 {
		t.Fatalf("observer failures = %d, want 1", len(failures))
	}
	if !errors.Is(failures[0].Cause, event.ErrDeadLettered) || !errors.Is(failures[0].Cause, event.ErrHandlerPanic) {
		t.Errorf("observer failure cause = %v, want ErrDeadLettered and ErrHandlerPanic", failures[0].Cause)
	}
	if n := evt.onErrCalls(); n != 1 {
		t.Errorf("event OnErr calls = %d, want 1", n)
	}
}

func TestInvokeHandler_RecoversInvokerPanic(t *testing.T) {
	called := false
	err := event.InvokeHandler(context.Background(), ihPanickingInvoker{}, &ihEvent{}, "h", func(context.Context) error {
		called = true
		return nil
	})
	if !errors.Is(err, event.ErrHandlerPanic) {
		t.Fatalf("err = %v, want it to match ErrHandlerPanic", err)
	}
	if called {
		t.Error("fn was called although the invoker panicked before next")
	}
}

// ihPermanent is a permanent error: Terminal() reports true, like the invoker
// module's PermanentError, which root tests cannot import.
type ihPermanent struct{}

func (ihPermanent) Error() string  { return "ih permanent" }
func (ihPermanent) Terminal() bool { return true }

// ihRecordingInvoker calls next and stores the error it returned.
type ihRecordingInvoker struct {
	mu  sync.Mutex
	got error
}

func (r *ihRecordingInvoker) Invoke(ctx context.Context, _ event.Event, _ string, next func(context.Context) error) error {
	err := next(ctx)
	r.mu.Lock()
	r.got = err
	r.mu.Unlock()
	return err
}

func (r *ihRecordingInvoker) seen() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got
}

func TestInvokeHandler_PanicValueNeverUnwrapped(t *testing.T) {
	values := []struct {
		name string
		v    any
	}{
		{"context canceled", context.Canceled},
		{"duplicate", event.ErrDuplicate},
		{"circuit open", event.ErrCircuitOpen},
		{"dead lettered", event.ErrDeadLettered},
		{"failure error", &event.FailureError{Cause: errors.New("x"), Reason: event.ReasonRetriesExhausted}},
		{"permanent", ihPermanent{}},
		{"string", "s"},
		{"int", 42},
	}
	for _, tc := range values {
		t.Run(tc.name, func(t *testing.T) {
			err := event.InvokeHandler(context.Background(), nil, &ihEvent{}, "h", func(context.Context) error {
				panic(tc.v)
			})
			if !errors.Is(err, event.ErrHandlerPanic) {
				t.Fatalf("err = %v, want it to match ErrHandlerPanic", err)
			}
			if verr, ok := tc.v.(error); ok && errors.Is(err, verr) {
				t.Errorf("err matches the panicked value %v with errors.Is", verr)
			}
			if d := event.Dispose(err, event.Delivery{Count: 1, Max: 3}); d != event.DispositionTerminal {
				t.Errorf("Dispose = %v, want terminal", d)
			}
			if r := event.ReasonOf(err); r != event.ReasonPanic {
				t.Errorf("ReasonOf = %q, want panic", r)
			}
		})
	}
}

func TestInvokeHandler_PanicNil(t *testing.T) {
	err := event.InvokeHandler(context.Background(), nil, &ihEvent{}, "h", func(context.Context) error {
		panic(nil)
	})
	if err == nil || !errors.Is(err, event.ErrHandlerPanic) {
		t.Fatalf("err = %v, want a non-nil error matching ErrHandlerPanic", err)
	}
	var pe *event.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %v, want a *PanicError", err)
	}
	if v, ok := pe.Value.(*runtime.PanicNilError); !ok || v == nil {
		t.Errorf("Value = %T, want *runtime.PanicNilError", pe.Value)
	}
}

func TestInvokeHandler_ChainSeesPanicAsError(t *testing.T) {
	rec := &ihRecordingInvoker{}
	err := event.InvokeHandler(context.Background(), rec, &ihEvent{}, "h", func(context.Context) error {
		panic("boom")
	})
	var pe *event.PanicError
	if !errors.As(rec.seen(), &pe) {
		t.Fatalf("invoker saw %v, want a *PanicError", rec.seen())
	}
	if !errors.Is(err, event.ErrHandlerPanic) {
		t.Errorf("returned err = %v, want it to match ErrHandlerPanic", err)
	}
}

func TestInvokeHandler_NilInvokerPassthrough(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "marker")
	want := errors.New("handler failed")
	var got context.Context
	err := event.InvokeHandler(ctx, nil, &ihEvent{}, "h", func(c context.Context) error {
		got = c
		return want
	})
	if err != want {
		t.Errorf("err = %v, want the handler error unchanged", err)
	}
	if got == nil || got.Value(ctxKey{}) != "marker" {
		t.Errorf("fn did not receive the caller ctx value")
	}
}

func TestInvokeHandler_LogsFullStack(t *testing.T) {
	logs := fsCaptureLogs(t)
	const name = "ih-log-handler"
	_ = event.InvokeHandler(context.Background(), nil, &ihEvent{}, name, func(context.Context) error {
		panic("logged")
	})
	recs := logs.forHandler(name)
	if len(recs) != 1 {
		t.Fatalf("log records = %d, want 1", len(recs))
	}
	if recs[0].level != slog.LevelError {
		t.Errorf("level = %v, want error", recs[0].level)
	}
	if recs[0].attrs["stack"] == "" {
		t.Error("stack attribute is empty")
	}
	if recs[0].attrs["panic"] != "logged" {
		t.Errorf("panic attribute = %q, want logged", recs[0].attrs["panic"])
	}
}

func TestPanicError_ErrorNilSafe(t *testing.T) {
	var nilPE *event.PanicError
	if nilPE.Error() == "" {
		t.Error("nil receiver Error() is empty")
	}
	if (&event.PanicError{}).Error() == "" {
		t.Error("zero PanicError Error() is empty")
	}
}

func TestReasonOf_PanicWrapped(t *testing.T) {
	pe := &event.PanicError{Value: "x"}
	if r := event.ReasonOf(&event.FailureError{Cause: pe, Reason: event.ReasonTerminal}); r != event.ReasonPanic {
		t.Errorf("FailureError: ReasonOf = %q, want panic", r)
	}
	final := event.Delivery{Count: 3, Max: 3}
	dl := &event.DeadLetteredError{Cause: pe, Reason: event.ReasonPanic}
	if r := event.ReasonOf(dl); r != event.ReasonPanic {
		t.Errorf("DeadLetteredError: ReasonOf = %q, want panic", r)
	}
	if d := event.Dispose(dl, final); d != event.DispositionAck {
		t.Errorf("Dispose(dead-lettered panic) = %v, want ack", d)
	}
	failed := &event.DeadLetterFailedError{Cause: pe, Reason: event.ReasonPanic, PublishErr: errors.New("down")}
	if r := event.ReasonOf(failed); r != event.ReasonPanic {
		t.Errorf("DeadLetterFailedError: ReasonOf = %q, want panic", r)
	}
	if d := event.Dispose(failed, final); d != event.DispositionRetryDelayed {
		t.Errorf("Dispose(dead-letter failed panic) = %v, want retry_delayed", d)
	}
}
