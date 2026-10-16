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
		invoker.NewDLQ(pub, nil),
		invoker.NewRetry(invoker.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   time.Millisecond,
			MaxDelay:    2 * time.Millisecond,
		}, nil),
		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 100,
			SuccessThreshold: 1,
			OpenTimeout:      time.Minute,
		}, nil),
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
		}, nil),
		invoker.NewCircuitBreaker(invoker.CircuitBreakerConfig{
			FailureThreshold: 100,
			SuccessThreshold: 1,
			OpenTimeout:      time.Minute,
		}, nil),
		invoker.NewDLQ(pub, nil),
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
