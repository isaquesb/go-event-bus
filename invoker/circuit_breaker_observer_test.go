package invoker_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"testing/synctest"
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
		if got.BreakerID == 0 {
			t.Errorf("%s: BreakerID must be set", label)
		}
		got.BreakerID = 0
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

// TestCircuitBreaker_BreakerIDUniquePerInstance proves that every breaker stamps
// a non-zero id on its transitions, that the id differs between instances and
// that all transitions of one breaker share it.
func TestCircuitBreaker_BreakerIDUniquePerInstance(t *testing.T) {
	trip := func(handlers ...string) []event.BreakerTransition {
		rec := &invRecorder{}
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Hour,
		})
		ctx := event.WithObserver(context.Background(), rec)
		for _, h := range handlers {
			_ = cb.Invoke(ctx, btEvent{}, h, func(context.Context) error { return errors.New("transient") })
		}
		var out []event.BreakerTransition
		for _, c := range rec.Transitions() {
			out = append(out, c.Val)
		}
		return out
	}

	a := trip("h", "other")
	b := trip("h")
	if len(a) != 2 || len(b) != 1 {
		t.Fatalf("transitions = %d and %d, want 2 and 1", len(a), len(b))
	}
	if a[0].BreakerID == 0 || b[0].BreakerID == 0 {
		t.Fatalf("BreakerID must be non-zero, got %d and %d", a[0].BreakerID, b[0].BreakerID)
	}
	if a[0].BreakerID == b[0].BreakerID {
		t.Errorf("two breakers share BreakerID %d", a[0].BreakerID)
	}
	if a[0].BreakerID != a[1].BreakerID {
		t.Errorf("transitions of one breaker carry ids %d and %d, want the same", a[0].BreakerID, a[1].BreakerID)
	}
}

// btTransition is the shape the matrix asserts: reason and endpoints, in order.
type btTransition struct {
	from, to event.BreakerState
	reason   event.BreakerReason
	seq      uint64
}

func btShapes(rec *invRecorder) []btTransition {
	var out []btTransition
	for _, c := range rec.Transitions() {
		out = append(out, btTransition{c.Val.From, c.Val.To, c.Val.Reason, c.Val.Seq})
	}
	return out
}

func btCheckShapes(t *testing.T, label string, got, want []btTransition) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %d transitions %+v, want %d %+v", label, len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s: transition %d = %+v, want %+v", label, i, got[i], want[i])
		}
	}
}

// TestCircuitBreaker_TransitionReasons walks the closed, open, half_open path
// to either outcome of the probe and checks every reason, the Seq chain and
// that open to half_open is emitted before the probe runs.
func TestCircuitBreaker_TransitionReasons(t *testing.T) {
	rows := []struct {
		name     string
		probeErr error
		last     btTransition
	}{
		{
			name: "probe succeeds",
			last: btTransition{event.BreakerHalfOpen, event.BreakerClosed, event.BreakerReasonProbeSucceeded, 3},
		},
		{
			name:     "probe fails",
			probeErr: errors.New("bt: probe failed"),
			last:     btTransition{event.BreakerHalfOpen, event.BreakerOpen, event.BreakerReasonProbeFailed, 3},
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rec := &invRecorder{}
				ctx := event.WithObserver(context.Background(), rec)
				cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
					FailureThreshold: 2,
					SuccessThreshold: 1,
					OpenTimeout:      time.Second,
				})
				fail := errors.New("bt: transient")
				run := func(next func(context.Context) error) error {
					return cb.Invoke(ctx, btEvent{}, "h", next)
				}

				_ = run(func(context.Context) error { return fail })
				_ = run(func(context.Context) error { return fail })
				opened := btTransition{event.BreakerClosed, event.BreakerOpen, event.BreakerReasonFailureThreshold, 1}
				btCheckShapes(t, "after two failures", btShapes(rec), []btTransition{opened})
				if f := rec.Transitions()[0].Val.Failures; f != 2 {
					t.Errorf("Failures = %d, want 2", f)
				}

				ran := false
				err := run(func(context.Context) error { ran = true; return nil })
				if !errors.Is(err, event.ErrCircuitOpen) || ran {
					t.Fatalf("blocked call: err=%v ran=%v", err, ran)
				}
				btCheckShapes(t, "after a blocked call", btShapes(rec), []btTransition{opened})

				time.Sleep(time.Second)

				halfOpen := btTransition{event.BreakerOpen, event.BreakerHalfOpen, event.BreakerReasonOpenTimeoutElapsed, 2}
				_ = run(func(context.Context) error {
					// The lazy transition is already delivered when the probe runs.
					btCheckShapes(t, "before the probe runs", btShapes(rec), []btTransition{opened, halfOpen})
					return row.probeErr
				})
				btCheckShapes(t, "after the probe", btShapes(rec), []btTransition{opened, halfOpen, row.last})
			})
		})
	}
}

// TestCircuitBreaker_LazyHalfOpenNoTimers: the breaker owns no timers, so time
// alone never produces a transition.
func TestCircuitBreaker_LazyHalfOpenNoTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &invRecorder{}
		ctx := event.WithObserver(context.Background(), rec)
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
		})

		_ = cb.Invoke(ctx, btEvent{}, "h", func(context.Context) error { return errors.New("bt: boom") })
		time.Sleep(time.Hour)
		synctest.Wait()

		btCheckShapes(t, "after one hour without calls", btShapes(rec), []btTransition{
			{event.BreakerClosed, event.BreakerOpen, event.BreakerReasonFailureThreshold, 1},
		})
		if st := cb.State("h"); st != event.BreakerOpen {
			t.Errorf("State after an idle hour = %q, want open (half_open is lazy)", st)
		}
	})
}

// TestCircuitBreaker_CallbackReentersInvoke: OnStateChange may call Invoke for
// the handler whose breaker just tripped.
func TestCircuitBreaker_CallbackReentersInvoke(t *testing.T) {
	var (
		cb       *invoker.CircuitBreaker
		reentry  = make(chan error, 1)
		innerRan = false
	)
	cb = invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
		OnStateChange: func(ctx context.Context, tr event.BreakerTransition) {
			reentry <- cb.Invoke(ctx, btEvent{}, tr.Handler, func(context.Context) error {
				innerRan = true
				return nil
			})
		},
	})

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = cb.Invoke(context.Background(), btEvent{}, "h", func(context.Context) error { return errors.New("bt: boom") })
	}()

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("callback deadlocked")
	}
	if err := <-reentry; !errors.Is(err, event.ErrCircuitOpen) {
		t.Errorf("re-entrant Invoke on the open breaker returned %v, want ErrCircuitOpen", err)
	}
	if innerRan {
		t.Error("the re-entrant call must not reach next while the breaker is open")
	}
}

// TestCircuitBreaker_SeqOrderingConcurrent: under -race, 50 goroutines drive two
// handlers through many cycles; per handler the transitions sorted by Seq form
// a gap-free chain in which every From is the previous To.
func TestCircuitBreaker_SeqOrderingConcurrent(t *testing.T) {
	const (
		goroutines = 50
		perG       = 40
	)
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 1,
		OpenTimeout:      time.Millisecond,
	})
	fail := errors.New("bt: transient")

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			handler := fmt.Sprintf("h%d", g%2)
			for i := 0; i < perG; i++ {
				bad := (i+g)%5 != 0
				_ = cb.Invoke(ctx, btEvent{}, handler, func(context.Context) error {
					if bad {
						return fail
					}
					return nil
				})
				if i%4 == 0 {
					time.Sleep(300 * time.Microsecond)
				}
			}
		}()
	}
	wg.Wait()

	byHandler := map[string][]event.BreakerTransition{}
	for _, c := range rec.Transitions() {
		byHandler[c.Val.Handler] = append(byHandler[c.Val.Handler], c.Val)
	}
	for _, handler := range []string{"h0", "h1"} {
		ts := byHandler[handler]
		if len(ts) < 4 {
			t.Fatalf("%s: only %d transitions, the test is not exercising the cycle", handler, len(ts))
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i].Seq < ts[j].Seq })
		prev := event.BreakerClosed
		for i, tr := range ts {
			if tr.Seq != uint64(i+1) {
				t.Fatalf("%s: transition %d has Seq %d, want %d (gap or duplicate)", handler, i, tr.Seq, i+1)
			}
			if tr.From != prev {
				t.Fatalf("%s: Seq %d starts at %q, the previous transition ended at %q", handler, tr.Seq, tr.From, prev)
			}
			prev = tr.To
		}
	}
}

// TestCircuitBreaker_CallbackPanicIsolated: a panicking OnStateChange neither
// fails the message nor stops the breaker; it surfaces as an InternalError.
func TestCircuitBreaker_CallbackPanicIsolated(t *testing.T) {
	rec := &invRecorder{}
	ctx := event.WithObserver(context.Background(), rec)
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
		OnStateChange: func(context.Context, event.BreakerTransition) {
			panic("bt: callback exploded")
		},
	})

	boom := errors.New("bt: handler failure")
	err := cb.Invoke(ctx, btEvent{}, "h", func(context.Context) error { return boom })

	if !errors.Is(err, boom) {
		t.Fatalf("Invoke returned %v, want the handler's own error", err)
	}
	var pe *event.PanicError
	if errors.As(err, &pe) || errors.Is(err, event.ErrHandlerPanic) {
		t.Errorf("a callback panic must not become a handler panic: %v", err)
	}
	if st := cb.State("h"); st != event.BreakerOpen {
		t.Errorf("State = %q, want open: the transition must still be applied", st)
	}
	if n := len(rec.Transitions()); n != 1 {
		t.Errorf("observer saw %d transitions, want 1", n)
	}

	internals := rec.InternalErrors()
	if len(internals) != 1 {
		t.Fatalf("got %d InternalError signals, want 1", len(internals))
	}
	got := internals[0].Val
	if got.Component != event.ComponentCircuitBreaker || got.Op != event.OpPanic || got.Handler != "h" || got.Err == nil {
		t.Errorf("InternalError = %+v, want circuit_breaker/panic for handler h with an error", got)
	}
}

// TestCircuitBreaker_ScopeBreakerState: the breaker reports its state into the
// FailureScope carried by ctx, and works without one.
func TestCircuitBreaker_ScopeBreakerState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
		})
		call := func(next func(context.Context) error) event.BreakerState {
			ctx, scope := event.WithFailureScope(context.Background())
			_ = cb.Invoke(ctx, btEvent{}, "h", next)
			return scope.Breaker()
		}
		ok := func(context.Context) error { return nil }

		if got := call(ok); got != event.BreakerClosed {
			t.Errorf("after a success: scope = %q, want closed", got)
		}
		if got := call(func(context.Context) error { return errors.New("bt: boom") }); got != event.BreakerOpen {
			t.Errorf("after the failure that opens: scope = %q, want open", got)
		}
		if got := call(ok); got != event.BreakerOpen {
			t.Errorf("blocked call: scope = %q, want open", got)
		}

		time.Sleep(time.Second)
		if got := call(ok); got != event.BreakerClosed {
			t.Errorf("after the successful half-open probe: scope = %q, want closed", got)
		}

		if err := cb.Invoke(context.Background(), btEvent{}, "h", ok); err != nil {
			t.Errorf("without a scope: %v", err)
		}
	})
}

// TestCircuitBreaker_StateUnknownHandler: a handler that never ran is closed.
func TestCircuitBreaker_StateUnknownHandler(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{FailureThreshold: 1})

	if st := cb.State("never-invoked"); st != event.BreakerClosed {
		t.Errorf("State of an unknown handler = %q, want closed", st)
	}
}

// TestCircuitBreaker_CallbackSeesLazyHalfOpen: the open to half_open callback
// runs on the probe's goroutine, before next, and sees the new state.
func TestCircuitBreaker_CallbackSeesLazyHalfOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			cb     *invoker.CircuitBreaker
			states []event.BreakerState
		)
		cb = invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
			OpenTimeout:      time.Second,
			OnStateChange: func(_ context.Context, tr event.BreakerTransition) {
				if tr.To == event.BreakerHalfOpen {
					states = append(states, cb.State(tr.Handler))
				}
			},
		})

		_ = cb.Invoke(context.Background(), btEvent{}, "h", func(context.Context) error { return errors.New("bt: boom") })
		time.Sleep(time.Second)

		probeRan := false
		_ = cb.Invoke(context.Background(), btEvent{}, "h", func(context.Context) error {
			probeRan = true
			if len(states) != 1 {
				t.Errorf("half_open callback ran %d times before the probe, want 1", len(states))
			}
			return nil
		})

		if !probeRan {
			t.Fatal("the probe did not run")
		}
		if len(states) != 1 || states[0] != event.BreakerHalfOpen {
			t.Errorf("State inside the half_open callback = %v, want [half_open]", states)
		}
	})
}
