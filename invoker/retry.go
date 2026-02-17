package invoker

import (
	"context"
	"errors"
	"github.com/isaquesb/go-event-bus"
	"time"
)

// Retryable false never retry
type Retryable interface {
	Retryable() bool
}

// Terminal true never retry
type Terminal interface {
	Terminal() bool
}
type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }
func (e PermanentError) Retryable() bool {
	return false
}
func (e PermanentError) Terminal() bool {
	return true
}

type RetryableError struct {
	Err error
}

func (e RetryableError) Error() string { return e.Err.Error() }
func (e RetryableError) Unwrap() error { return e.Err }
func (e RetryableError) Retryable() bool {
	return true
}

type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func classify(err error) (retry bool, terminal bool) {
	if err == nil {
		return false, false
	}

	if errors.Is(err, ErrDuplicate) {
		return false, false
	}

	if errors.Is(err, ErrRateLimited) {
		return false, false
	}

	if errors.Is(err, ErrCircuitOpen) {
		return false, false
	}

	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}

	var t Terminal
	if errors.As(err, &t) && t.Terminal() {
		return false, true
	}

	var r Retryable
	if errors.As(err, &r) {
		return r.Retryable(), false
	}

	// default: retryable
	return true, false
}

func backoff(attempt int, base, max time.Duration) time.Duration {
	d := time.Duration(1<<attempt) * base
	if d > max {
		return max
	}
	return d
}

func NewRetry(p RetryPolicy, metrics MetricProvider) *Retry {
	if metrics == nil {
		metrics = &NoopProvider{}
	}

	return &Retry{
		policy:  p,
		metrics: metrics,
	}
}

type Retry struct {
	policy  RetryPolicy
	metrics MetricProvider
}

func (r *Retry) Invoke(
	ctx context.Context,
	_ event.Event,
	handler string,
	next func(context.Context) error,
) error {
	var lastErr error

	for attempt := 1; attempt <= r.policy.MaxAttempts; attempt++ {
		r.metrics.IncCounter(
			"eventbus_retry_attempt_total",
			1,
			Labels{"handler": handler},
		)

		err := next(ctx)
		if err == nil {
			if attempt > 1 {
				r.metrics.IncCounter(
					"eventbus_retry_success_total",
					1,
					Labels{"handler": handler},
				)
			}
			return nil
		}

		retry, terminal := classify(err)

		if terminal {
			r.metrics.IncCounter(
				"eventbus_retry_terminal_total",
				1,
				Labels{"handler": handler},
			)
			return ErrSendToDLQ
		}

		if !retry {
			return err
		}

		lastErr = err

		if attempt == r.policy.MaxAttempts {
			break
		}

		delay := backoff(attempt-1, r.policy.BaseDelay, r.policy.MaxDelay)

		r.metrics.ObserveHistogram(
			"eventbus_retry_backoff_ms",
			float64(delay.Milliseconds()),
			Labels{"handler": handler},
		)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	r.metrics.IncCounter(
		"eventbus_retry_exhausted_total",
		1,
		Labels{"handler": handler},
	)

	return lastErr
}

/*
// Example Usage
func (h *CreateUserHandler) Handle(
	ctx context.Context,
	evt event.Event,
) error {
	e := evt.(*UserCreated)

	if e.Email == "" {
		return event.PermanentError{
			Err: errors.New("email is required"),
		}
	}

	if err := h.repo.Save(ctx, e); err != nil {
		return event.RetryableError{Err: err}
	}

	return nil
}
*/
