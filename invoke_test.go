package event_test

import (
	"context"
	"errors"
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
	var mu sync.Mutex
	var busErrs []error
	s := event.Settle(ctx, event.SettleInput{
		SubscriptionCtx: context.Background(),
		Evt:             evt,
		Handler:         "h",
		Err:             err,
		Publisher:       dlq,
		OnErr: func(_ context.Context, _ event.Event, e error, _ string) {
			mu.Lock()
			defer mu.Unlock()
			busErrs = append(busErrs, e)
		},
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

	mu.Lock()
	defer mu.Unlock()
	if len(busErrs) != 1 {
		t.Fatalf("bus OnErr calls = %d, want 1", len(busErrs))
	}
	if !errors.Is(busErrs[0], event.ErrDeadLettered) || !errors.Is(busErrs[0], event.ErrHandlerPanic) {
		t.Errorf("bus OnErr error = %v, want ErrDeadLettered and ErrHandlerPanic", busErrs[0])
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
