package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus"
	"github.com/isaquesb/go-event-bus/invoker"
)

func TestRetry_Success(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestRetry_RetryOnFailure(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		if called < 3 {
			return errors.New("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called != 3 {
		t.Errorf("expected 3 calls, got %d", called)
	}
}

func TestRetry_ExhaustedRetries(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	persistentErr := errors.New("persistent error")

	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return persistentErr
	})

	if !errors.Is(err, event.ErrRetriesExhausted) {
		t.Errorf("expected ErrRetriesExhausted, got '%v'", err)
	}
	if !errors.Is(err, persistentErr) {
		t.Errorf("expected the last error to be preserved, got '%v'", err)
	}
	var fe *invoker.FailureError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FailureError, got %T", err)
	}
	if fe.Reason != event.ReasonRetriesExhausted || fe.Attempts != 3 {
		t.Errorf("reason=%q attempts=%d, want retries_exhausted and 3", fe.Reason, fe.Attempts)
	}
	if called != 3 {
		t.Errorf("expected 3 calls, got %d", called)
	}
}

func TestRetry_PermanentErrorSendsToDLQ(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	inner := errors.New("permanent failure")
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return invoker.PermanentError{Err: inner}
	})

	if !errors.Is(err, invoker.ErrSendToDLQ) {
		t.Errorf("expected ErrSendToDLQ, got '%v'", err)
	}
	if !errors.Is(err, inner) {
		t.Errorf("expected the permanent error to be preserved, got '%v'", err)
	}
	var fe *invoker.FailureError
	if !errors.As(err, &fe) {
		t.Fatalf("expected *FailureError, got %T", err)
	}
	if fe.Reason != event.ReasonTerminal || fe.Attempts != 1 {
		t.Errorf("reason=%q attempts=%d, want terminal and 1", fe.Reason, fe.Attempts)
	}
	if called != 1 {
		t.Errorf("expected 1 call (no retries for terminal errors), got %d", called)
	}
}

func TestRetry_RetryableError(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		if called < 2 {
			return invoker.RetryableError{Err: errors.New("retry me")}
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called != 2 {
		t.Errorf("expected 2 calls, got %d", called)
	}
}

func TestRetry_NoRetryForDuplicate(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return invoker.ErrDuplicate
	})

	if !errors.Is(err, invoker.ErrDuplicate) {
		t.Errorf("expected ErrDuplicate, got '%v'", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call (no retries for duplicates), got %d", called)
	}
}

func TestRetry_NoRetryForRateLimited(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return invoker.ErrRateLimited
	})

	if !errors.Is(err, invoker.ErrRateLimited) {
		t.Errorf("expected ErrRateLimited, got '%v'", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call (no retries for rate limited), got %d", called)
	}
}

func TestRetry_NoRetryForCircuitOpen(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return invoker.ErrCircuitOpen
	})

	if !errors.Is(err, invoker.ErrCircuitOpen) {
		t.Errorf("expected ErrCircuitOpen, got '%v'", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call (no retries for circuit open), got %d", called)
	}
}

func TestRetry_NoRetryForContextCanceled(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return context.Canceled
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got '%v'", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call (no retries for context canceled), got %d", called)
	}
}

func TestRetry_ContextCancellationDuringBackoff(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Second, // Long delay
		MaxDelay:    time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())

	called := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := retry.Invoke(ctx, &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return errors.New("fail")
	})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got '%v'", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call before cancellation, got %d", called)
	}
}

func TestRetry_BackoffCapped(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 10,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    200 * time.Millisecond, // Cap at 200ms
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		if called < 5 {
			return errors.New("fail")
		}
		return nil
	})

	if err != nil || called != 5 {
		t.Errorf("expected success on the 5th attempt with a capped backoff, got called=%d err=%v", called, err)
	}
}

func retryNoRetryFor(t *testing.T, sentinel error) {
	t.Helper()
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	})

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return sentinel
	})

	if err != sentinel {
		t.Errorf("expected %v returned unwrapped, got '%v'", sentinel, err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestRetry_NoRetryForInProgress(t *testing.T) {
	retryNoRetryFor(t, event.ErrInProgress)
}

func TestRetry_NoRetryForDeadLettered(t *testing.T) {
	retryNoRetryFor(t, event.ErrDeadLettered)
}

func TestRetry_NoRetryForDeadLetterFailed(t *testing.T) {
	retryNoRetryFor(t, event.ErrDeadLetterFailed)
}

func TestRetry_ZeroMaxAttemptsRunsOnce(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{})

	called := 0
	cause := errors.New("fails")
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return cause
	})

	if called != 1 {
		t.Errorf("expected the handler to run once, got %d", called)
	}
	if !errors.Is(err, cause) {
		t.Errorf("expected the cause to be preserved, got '%v'", err)
	}
	var fe *invoker.FailureError
	if !errors.As(err, &fe) || fe.Cause == nil || fe.Attempts != 1 {
		t.Errorf("expected a FailureError with a cause and 1 attempt, got %#v", err)
	}
}

func TestSentinelAliases_Identity(t *testing.T) {
	if invoker.ErrDuplicate != event.ErrDuplicate {
		t.Error("invoker.ErrDuplicate must be event.ErrDuplicate")
	}
	if invoker.ErrCircuitOpen != event.ErrCircuitOpen {
		t.Error("invoker.ErrCircuitOpen must be event.ErrCircuitOpen")
	}
	if invoker.ErrRateLimited != event.ErrRateLimited {
		t.Error("invoker.ErrRateLimited must be event.ErrRateLimited")
	}
	if invoker.ErrSendToDLQ != event.ErrSendToDLQ {
		t.Error("invoker.ErrSendToDLQ must be event.ErrSendToDLQ")
	}
}
