package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// btEvent is the event the breaker observer tests feed through the breaker.
type btEvent struct{}

func (btEvent) Name() string { return "bt.order" }

// btCtxKey carries a unique value so a test can prove which context the
// Observer and the callback received.
type btCtxKey struct{}

// TestCircuitBreaker_TransitionToObserverAndCallback is the tracer: one trip
// travels Invoke, record under the lock, unlock, Observer.BreakerTransition,
// OnStateChange and back into State without deadlocking.
func TestCircuitBreaker_TransitionToObserverAndCallback(t *testing.T) {
	rec := &invRecorder{}
	done := make(chan struct{})

	var (
		cb          *invoker.CircuitBreaker
		cbSeen      event.BreakerTransition
		cbState     event.BreakerState
		cbCtxValue  any
		callbackRun int
	)
	cb = invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
		OnStateChange: func(ctx context.Context, tr event.BreakerTransition) {
			callbackRun++
			cbSeen = tr
			cbState = cb.State(tr.Handler)
			cbCtxValue = ctx.Value(btCtxKey{})
			close(done)
		},
	})

	ctx := context.WithValue(event.WithObserver(context.Background(), rec), btCtxKey{}, "bt-unique")

	invokeErr := errors.New("transient")
	got := cb.Invoke(ctx, btEvent{}, "h", func(context.Context) error { return invokeErr })
	if !errors.Is(got, invokeErr) {
		t.Fatalf("the breaker must return the handler error, got %v", got)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("callback deadlocked")
	}

	want := event.BreakerTransition{
		Handler:  "h",
		From:     event.BreakerClosed,
		To:       event.BreakerOpen,
		Reason:   event.BreakerReasonFailureThreshold,
		Failures: 1,
		Seq:      1,
	}
	same := func(label string, got event.BreakerTransition) {
		t.Helper()
		if got.At.IsZero() {
			t.Errorf("%s: At must be set", label)
		}
		got.At = time.Time{}
		if got != want {
			t.Errorf("%s: got %+v, want %+v", label, got, want)
		}
	}

	observed := rec.Transitions()
	if len(observed) != 1 {
		t.Fatalf("observer saw %d transitions, want 1", len(observed))
	}
	same("observer", observed[0].Val)
	if v := observed[0].Ctx.Value(btCtxKey{}); v != "bt-unique" {
		t.Errorf("observer ctx value = %v, want bt-unique", v)
	}

	if callbackRun != 1 {
		t.Fatalf("callback ran %d times, want 1", callbackRun)
	}
	same("callback", cbSeen)
	if cbCtxValue != "bt-unique" {
		t.Errorf("callback ctx value = %v, want bt-unique", cbCtxValue)
	}
	if cbState != event.BreakerOpen {
		t.Errorf("State inside the callback = %q, want open", cbState)
	}

	next := cb.Invoke(ctx, btEvent{}, "h", func(context.Context) error { return nil })
	if !errors.Is(next, event.ErrCircuitOpen) {
		t.Errorf("a call on the open breaker must match ErrCircuitOpen, got %v", next)
	}
}
