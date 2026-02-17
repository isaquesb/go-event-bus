package invoker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/isaquesb/go-event-bus/invoker"
)

func TestRetry_Success(t *testing.T) {
	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}, nil)

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
	}, nil)

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
	}, nil)

	called := 0
	persistentErr := errors.New("persistent error")

	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return persistentErr
	})

	if !errors.Is(err, persistentErr) {
		t.Errorf("expected error '%v', got '%v'", persistentErr, err)
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
	}, nil)

	called := 0
	err := retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		return invoker.PermanentError{Err: errors.New("permanent failure")}
	})

	if !errors.Is(err, invoker.ErrSendToDLQ) {
		t.Errorf("expected ErrSendToDLQ, got '%v'", err)
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
	}, nil)

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
	}, nil)

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
	}, nil)

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
	}, nil)

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
	}, nil)

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
	}, nil)

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
	metrics := &testMetricProvider{}

	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 10,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    200 * time.Millisecond, // Cap at 200ms
	}, metrics)

	called := 0
	_ = retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		if called < 5 {
			return errors.New("fail")
		}
		return nil
	})

	// Check that backoff was capped (all recorded backoffs should be <= 200ms)
	for _, val := range metrics.histograms["eventbus_retry_backoff_ms"] {
		if val > 200 {
			t.Errorf("backoff exceeded max delay: %v", val)
		}
	}
}

func TestRetry_MetricsTracked(t *testing.T) {
	metrics := &testMetricProvider{}

	retry := invoker.NewRetry(invoker.RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		MaxDelay:    10 * time.Millisecond,
	}, metrics)

	called := 0
	_ = retry.Invoke(context.Background(), &testEvent{}, "handler", func(ctx context.Context) error {
		called++
		if called < 3 {
			return errors.New("fail")
		}
		return nil
	})

	if metrics.counters["eventbus_retry_attempt_total"] != 3 {
		t.Errorf("expected 3 retry attempts, got %d", metrics.counters["eventbus_retry_attempt_total"])
	}

	if metrics.counters["eventbus_retry_success_total"] != 1 {
		t.Errorf("expected 1 retry success, got %d", metrics.counters["eventbus_retry_success_total"])
	}
}

// testMetricProvider collects metrics for testing
type testMetricProvider struct {
	counters   map[string]int64
	histograms map[string][]float64
	gauges     map[string]float64
}

func (m *testMetricProvider) IncCounter(name string, n int64, labels invoker.Labels) {
	if m.counters == nil {
		m.counters = make(map[string]int64)
	}
	m.counters[name] += n
}

func (m *testMetricProvider) ObserveHistogram(name string, value float64, labels invoker.Labels) {
	if m.histograms == nil {
		m.histograms = make(map[string][]float64)
	}
	m.histograms[name] = append(m.histograms[name], value)
}

func (m *testMetricProvider) SetGauge(name string, value float64, labels invoker.Labels) {
	if m.gauges == nil {
		m.gauges = make(map[string]float64)
	}
	m.gauges[name] = value
}
