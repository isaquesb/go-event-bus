package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

// coChain builds the documented failure-handling order: DLQ outside Retry,
// Retry outside CircuitBreaker.
func coChain(pub invoker.DLQPublisher) *invoker.Chain {
	return invoker.NewChain(
		invoker.NewDLQ(pub),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}),
		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 100,
			SuccessThreshold: 1,
			OpenTimeout:      time.Minute,
		}),
	)
}

// coHandler returns a handler that fails with err on its first failUntil calls
// and succeeds afterwards. A negative failUntil fails forever. calls counts
// every call.
func coHandler(calls *int, failUntil int, err error) func(context.Context) error {
	return func(context.Context) error {
		*calls++
		if failUntil < 0 || *calls <= failUntil {
			return err
		}
		return nil
	}
}

func TestChainOrder_DLQOutsideRetry_TerminalPublishesOnce(t *testing.T) {
	pub := &dqPublisher{}
	inner := errors.New("email is required")
	calls := 0

	err := coChain(pub).Invoke(dqDelivery(1, 3), &testEvent{name: "test.event"}, "handler",
		coHandler(&calls, -1, invoker.PermanentError{Err: inner}))

	if calls != 1 {
		t.Errorf("handler calls = %d, want 1 (terminal errors are not retried)", calls)
	}
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("expected ErrDeadLettered, got %v", err)
	}
	if !errors.Is(err, inner) {
		t.Errorf("expected the original cause to be preserved, got %v", err)
	}
	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("expected exactly 1 DLQ record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonTerminal || recs[0].Attempts != 1 {
		t.Errorf("record reason=%q attempts=%d, want terminal and 1", recs[0].Reason, recs[0].Attempts)
	}
}

func TestChainOrder_DLQOutsideRetry_TransientRecovers(t *testing.T) {
	pub := &dqPublisher{}
	calls := 0

	err := coChain(pub).Invoke(dqDelivery(1, 1), &testEvent{name: "test.event"}, "handler",
		coHandler(&calls, 1, errors.New("transient")))

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Errorf("handler calls = %d, want 2", calls)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("a recovered transient error must not be dead-lettered, got %d records", n)
	}
}

func TestChainOrder_DLQOutsideRetry_ExhaustedOnFinal(t *testing.T) {
	pub := &dqPublisher{}
	calls := 0
	last := errors.New("always fails")

	err := coChain(pub).Invoke(dqDelivery(1, 1), &testEvent{name: "test.event"}, "handler",
		coHandler(&calls, -1, last))

	if calls != 3 {
		t.Errorf("handler calls = %d, want 3", calls)
	}
	if !errors.Is(err, event.ErrDeadLettered) || !errors.Is(err, last) {
		t.Errorf("expected ErrDeadLettered wrapping the last error, got %v", err)
	}
	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("expected 1 DLQ record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonRetriesExhausted || recs[0].Attempts != 3 {
		t.Errorf("record reason=%q attempts=%d, want retries_exhausted and 3", recs[0].Reason, recs[0].Attempts)
	}
}

func TestChainOrder_DLQOutsideRetry_ExhaustedNotFinalRetriedByTransport(t *testing.T) {
	pub := &dqPublisher{}
	calls := 0

	err := coChain(pub).Invoke(dqDelivery(1, 3), &testEvent{name: "test.event"}, "handler",
		coHandler(&calls, -1, errors.New("always fails")))

	if !errors.Is(err, event.ErrRetriesExhausted) {
		t.Errorf("expected ErrRetriesExhausted so the transport retries, got %v", err)
	}
	if errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("must not be dead-lettered before the final delivery: %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("expected no DLQ record before the final delivery, got %d", n)
	}
}

// The legacy order put DLQ inside Retry. This test documents why it is wrong:
// the first failure of a recoverable error is dead-lettered and Retry never
// gets to run again.
func TestChainOrder_LegacyOrderDeadLettersPrematurely(t *testing.T) {
	pub := &dqPublisher{}
	calls := 0

	legacy := invoker.NewChain(
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}),
		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 100,
			SuccessThreshold: 1,
			OpenTimeout:      time.Minute,
		}),
		invoker.NewDLQ(pub),
	)

	err := legacy.Invoke(dqDelivery(1, 1), &testEvent{name: "test.event"}, "handler",
		coHandler(&calls, 1, errors.New("transient")))

	if calls != 1 {
		t.Errorf("handler calls = %d, want 1: the legacy order never retries", calls)
	}
	if n := len(pub.records()); n != 1 {
		t.Errorf("expected the premature DLQ record, got %d", n)
	}
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("expected ErrDeadLettered, got %v", err)
	}
}

// coStub is an invoker that returns err without calling next, the way a rate
// limiter or an open circuit breaker does.
type coStub struct{ err error }

func (s coStub) Invoke(context.Context, event.Event, string, func(context.Context) error) error {
	return s.err
}

func TestChainOrder_FlowControlDeadLetteredOnlyOnFinal(t *testing.T) {
	pub := &dqPublisher{}
	chain := invoker.NewChain(
		invoker.NewDLQ(pub),
		invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}),
		coStub{err: event.ErrCircuitOpen},
	)
	evt := &testEvent{name: "test.event"}

	err := chain.Invoke(dqDelivery(1, 3), evt, "handler", func(context.Context) error { return nil })
	if err != event.ErrCircuitOpen {
		t.Errorf("non-final delivery: expected the raw ErrCircuitOpen, got %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Fatalf("non-final delivery: expected no DLQ record, got %d", n)
	}

	err = chain.Invoke(dqDelivery(3, 3), evt, "handler", func(context.Context) error { return nil })
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Errorf("final delivery: expected ErrDeadLettered, got %v", err)
	}
	recs := pub.records()
	if len(recs) != 1 {
		t.Fatalf("final delivery: expected 1 DLQ record, got %d", len(recs))
	}
	if recs[0].Reason != event.ReasonCircuitOpen || recs[0].Attempts != 0 {
		t.Errorf("record reason=%q attempts=%d, want circuit_open and 0", recs[0].Reason, recs[0].Attempts)
	}
}

// The breaker opens after two real attempts of the same delivery. On the final
// delivery the DLQ record keeps those attempts and their history; before the
// final delivery nothing is dead-lettered and the error still matches
// ErrCircuitOpen.
func TestChainOrder_CircuitOpensMidRetryKeepsHistory(t *testing.T) {
	newChain := func(pub invoker.DLQPublisher) *invoker.Chain {
		return invoker.NewChain(
			invoker.NewDLQ(pub),
			invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 4, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}),
			invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{FailureThreshold: 2, OpenTimeout: time.Minute}),
		)
	}
	evt := &testEvent{name: "test.event"}
	transient := errors.New("db down")

	t.Run("final delivery", func(t *testing.T) {
		pub := &dqPublisher{}
		calls := 0

		err := newChain(pub).Invoke(dqDelivery(3, 3), evt, "handler", coHandler(&calls, -1, transient))

		if calls != 2 {
			t.Errorf("handler calls = %d, want 2 (the open circuit stops the third)", calls)
		}
		if !errors.Is(err, event.ErrDeadLettered) {
			t.Errorf("expected ErrDeadLettered, got %v", err)
		}
		recs := pub.records()
		if len(recs) != 1 {
			t.Fatalf("expected 1 DLQ record, got %d", len(recs))
		}
		rec := recs[0]
		if rec.Reason != event.ReasonCircuitOpen || rec.Attempts != 2 || len(rec.History) != 2 {
			t.Fatalf("record reason=%q attempts=%d history=%d, want circuit_open, 2 and 2",
				rec.Reason, rec.Attempts, len(rec.History))
		}
		h := rec.FailureHeaders()
		if h[event.HeaderDLQAttempts] != "2" {
			t.Errorf("%s = %q, want 2", event.HeaderDLQAttempts, h[event.HeaderDLQAttempts])
		}
		if h[event.HeaderDLQHistory] == "" {
			t.Errorf("%s is empty, want the two attempts", event.HeaderDLQHistory)
		}
	})

	t.Run("not final delivery", func(t *testing.T) {
		pub := &dqPublisher{}
		calls := 0

		err := newChain(pub).Invoke(dqDelivery(1, 3), evt, "handler", coHandler(&calls, -1, transient))

		if !errors.Is(err, event.ErrCircuitOpen) {
			t.Errorf("expected ErrCircuitOpen, got %v", err)
		}
		if n := len(pub.records()); n != 0 {
			t.Errorf("expected no DLQ record before the final delivery, got %d", n)
		}
	})
}

// Idempotency sits outside DLQ, so its ErrInProgress never reaches the DLQ
// invoker. The transport backstop (event.Settle) must dead-letter it on the
// final delivery.
func TestChainOrder_InProgressBypassesChainDLQ(t *testing.T) {
	pub := &dqPublisher{}
	store := invoker.NewMemoryIdempotencyStore()
	_ = store.Put(context.Background(), "evt-1:handler", invoker.IdempotencyRecord{
		Status:    invoker.StatusProcessing,
		StartedAt: time.Now(),
	})
	chain := invoker.NewChain(
		invoker.NewIdempotency(store, invoker.IdempotencyConfig{ProcessingTTL: time.Hour}),
		invoker.NewDLQ(pub),
		invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}),
	)

	called := false
	err := chain.Invoke(dqDelivery(3, 3), &eventWithId{id: "evt-1", name: "test"}, "handler", func(context.Context) error {
		called = true
		return nil
	})

	if called {
		t.Error("the handler must not run while another execution is in progress")
	}
	if !errors.Is(err, event.ErrInProgress) {
		t.Errorf("expected ErrInProgress, got %v", err)
	}
	if n := len(pub.records()); n != 0 {
		t.Errorf("the chain DLQ must not see ErrInProgress, got %d records: the transport backstop is required", n)
	}
}

func TestChainOrder_DeadLetteredThenReplayExecutes(t *testing.T) {
	pub := &dqPublisher{}
	store := invoker.NewMemoryIdempotencyStore()
	chain := invoker.NewChain(
		invoker.NewIdempotency(store, invoker.IdempotencyConfig{}),
		invoker.NewDLQ(pub),
		invoker.NewRetry(invoker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}),
	)
	evt := &eventWithId{id: "evt-1", name: "test"}

	err := chain.Invoke(dqDelivery(1, 3), evt, "handler", func(context.Context) error {
		return invoker.PermanentError{Err: errors.New("bad input")}
	})
	if !errors.Is(err, event.ErrDeadLettered) {
		t.Fatalf("first run: expected ErrDeadLettered, got %v", err)
	}
	if n := len(pub.records()); n != 1 {
		t.Fatalf("first run: expected 1 DLQ record, got %d", n)
	}
	rec, _, _ := store.Get(context.Background(), "evt-1:handler")
	if rec.Status != invoker.StatusDeadLettered {
		t.Fatalf("first run: stored status = %q, want dead_lettered", rec.Status)
	}

	called := false
	err = chain.Invoke(dqDelivery(1, 3), evt, "handler", func(context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Errorf("replay: unexpected error %v", err)
	}
	if !called {
		t.Error("replay: a dead-lettered event must execute again")
	}
	rec, _, _ = store.Get(context.Background(), "evt-1:handler")
	if rec.Status != invoker.StatusCompleted {
		t.Errorf("replay: stored status = %q, want completed", rec.Status)
	}
	if n := len(pub.records()); n != 1 {
		t.Errorf("replay: expected still 1 DLQ record, got %d", n)
	}
}
