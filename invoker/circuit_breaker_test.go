package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus/invoker"
)

func TestCircuitBreaker_StartsInClosedState(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      time.Second,
	}, nil)

	called := false
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected handler to be called in closed state")
	}
}

func TestCircuitBreaker_OpensAfterFailures(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      time.Second,
	}, nil)

	handlerErr := errors.New("handler failed")

	// Trigger failures to open the circuit
	for i := 0; i < 3; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// Next call should fail with circuit open
	called := false
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if !errors.Is(err, invoker.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got '%v'", err)
	}
	if called {
		t.Error("handler should not be called when circuit is open")
	}
}

func TestCircuitBreaker_ResetFailuresOnSuccess(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      time.Second,
	}, nil)

	handlerErr := errors.New("handler failed")

	// 2 failures
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// 1 success resets the counter
	_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	// 2 more failures (not 3 total since reset)
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// Circuit should still be closed
	called := false
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should be called, circuit should be closed")
	}
}

func TestCircuitBreaker_HalfOpenAfterTimeout(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}, nil)

	handlerErr := errors.New("handler failed")

	// Open the circuit
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// Verify circuit is open
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, invoker.ErrCircuitOpen) {
		t.Error("expected circuit to be open")
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Circuit should now be half-open, allowing one request through
	called := false
	err = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error in half-open state: %v", err)
	}
	if !called {
		t.Error("handler should be called in half-open state")
	}
}

func TestCircuitBreaker_ClosesAfterSuccessesInHalfOpen(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}, nil)

	handlerErr := errors.New("handler failed")

	// Open the circuit
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	// 2 successful calls should close the circuit
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return nil
		})
	}

	// Circuit should now be closed, even after many calls
	for i := 0; i < 5; i++ {
		called := false
		err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			called = true
			return nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !called {
			t.Error("handler should be called in closed state")
		}
	}
}

func TestCircuitBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}, nil)

	handlerErr := errors.New("handler failed")

	// Open the circuit
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	// Fail in half-open state
	_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return handlerErr
	})

	// Circuit should be open again
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	if !errors.Is(err, invoker.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen after half-open failure, got '%v'", err)
	}
}

func TestCircuitBreaker_PerHandlerState(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      time.Second,
	}, nil)

	handlerErr := errors.New("handler failed")

	// Open circuit for handler1
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler1", func(ctx context.Context) error {
			return handlerErr
		})
	}

	// handler1 should be blocked
	err := cb.Invoke(context.Background(), &testEvent{}, "handler1", func(ctx context.Context) error {
		return nil
	})
	if !errors.Is(err, invoker.ErrCircuitOpen) {
		t.Error("expected handler1 circuit to be open")
	}

	// handler2 should still work
	called := false
	err = cb.Invoke(context.Background(), &testEvent{}, "handler2", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error for handler2: %v", err)
	}
	if !called {
		t.Error("handler2 should be called, its circuit is independent")
	}
}

func TestCircuitBreaker_DLQErrorNotCountedAsFailure(t *testing.T) {
	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      time.Second,
	}, nil)

	// ErrSendToDLQ should not count as failure
	for i := 0; i < 5; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return invoker.ErrSendToDLQ
		})
	}

	// Circuit should still be closed
	called := false
	err := cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called = true
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler should be called, ErrSendToDLQ should not trip circuit")
	}
}

func TestCircuitBreaker_MetricsEmitted(t *testing.T) {
	metrics := &testMetricProvider{}

	cb := invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	}, metrics)

	handlerErr := errors.New("handler failed")

	// Open the circuit
	for i := 0; i < 2; i++ {
		_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
			return handlerErr
		})
	}

	if metrics.counters["eventbus_circuit_open_total"] != 1 {
		t.Errorf("expected 1 circuit_open_total, got %d", metrics.counters["eventbus_circuit_open_total"])
	}

	// Try while open
	_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	if metrics.counters["eventbus_circuit_blocked_total"] != 1 {
		t.Errorf("expected 1 circuit_blocked_total, got %d", metrics.counters["eventbus_circuit_blocked_total"])
	}

	// Wait for half-open
	time.Sleep(60 * time.Millisecond)

	_ = cb.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		return nil
	})

	if metrics.counters["eventbus_circuit_half_open_total"] != 1 {
		t.Errorf("expected 1 circuit_half_open_total, got %d", metrics.counters["eventbus_circuit_half_open_total"])
	}
}
